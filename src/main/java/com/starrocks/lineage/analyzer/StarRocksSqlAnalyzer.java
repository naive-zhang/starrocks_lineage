package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.metadata.MetadataAccessException;
import com.starrocks.lineage.metadata.MetadataProvider;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.ColumnUsageStat;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.model.UnresolvedNameException;
import com.starrocks.lineage.scope.ScopeStack;
import com.starrocks.lineage.scope.SemanticAnalysisException;
import com.starrocks.sql.parser.SqlModeHelper;
import com.starrocks.sql.parser.SqlParsingException;
import com.starrocks.sql.parser.SqlSanitizer;
import com.starrocks.sql.parser.StarRocksParser;
import com.starrocks.sql.parser.StarRocksSqlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Facade: audit record → {@link SqlParseResult}.
 *
 * <ol>
 *   <li>Pre-filter: failed statements ({@code state = 'ERR'} / non-zero {@code errorCode}) are skipped.</li>
 *   <li>{@link SqlSanitizer}: driver comments removed; empty and management statements skipped without parsing.</li>
 *   <li>{@link StarRocksSqlParser}: official grammar, SLL then LL.</li>
 *   <li>Every statement of the text is analysed by a fresh {@link StarRocksAuditAstVisitor} on a fresh
 *       {@link ScopeStack}; {@code USE db} / {@code USE catalog.db} / {@code SET CATALOG c} between statements update
 *       the namespace of the following ones. Results of several in-scope statements are merged.</li>
 * </ol>
 *
 * <p>Error classification: syntax errors and pathological nesting give {@link ParseStatus#PARSE_ERROR}; semantic
 * failures {@link ParseStatus#ANALYSIS_ERROR}; a {@link MetadataAccessException} (infrastructure failure) is
 * <b>re-thrown</b> so that the caller can retry the record later instead of storing a wrong result.</p>
 *
 * <p>Thread-safe: holds only immutable configuration and thread-safe collaborators; every call allocates its own
 * parser, scope stack and visitor.</p>
 */
public final class StarRocksSqlAnalyzer {

    private final MetadataProvider metadata;
    private final StarRocksSqlParser parser;
    private final boolean strictColumnResolution;

    /** Default sql_mode, lenient column resolution. */
    public StarRocksSqlAnalyzer(MetadataProvider metadata) {
        this(metadata, SqlModeHelper.MODE_DEFAULT, false);
    }

    /**
     * @param metadata               schema source
     * @param sqlMode                sql_mode bit mask used to lex statements (e.g. {@code PIPES_AS_CONCAT})
     * @param strictColumnResolution fail on columns missing from metadata instead of attributing them
     */
    public StarRocksSqlAnalyzer(MetadataProvider metadata, long sqlMode, boolean strictColumnResolution) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.parser = new StarRocksSqlParser(sqlMode);
        this.strictColumnResolution = strictColumnResolution;
    }

    /**
     * Analyses one audit record.
     *
     * @throws MetadataAccessException when metadata could not be read (retryable)
     */
    public SqlParseResult analyze(AuditLogRecord record) {
        Objects.requireNonNull(record, "record");
        if (record.isFailed()) {
            return SqlParseResult.of(record.getQueryId(), ParseStatus.SKIPPED_FAILED_QUERY,
                    "state=" + record.getState() + ", errorCode=" + record.getErrorCode());
        }
        return analyze(record.getQueryId(), record.getStmt(), record.namespace());
    }

    /**
     * Analyses SQL text executed in the given namespace.
     *
     * @throws MetadataAccessException when metadata could not be read (retryable)
     */
    public SqlParseResult analyze(String queryId, String sql, NamespaceContext namespace) {
        Objects.requireNonNull(namespace, "namespace");
        try {
            return doAnalyze(queryId, sql, namespace);
        } catch (MetadataAccessException e) {
            throw e;
        } catch (RuntimeException e) {
            // an analyzer defect on one statement must not fail the batch: it becomes a dead letter and can be
            // repaired once the analyzer is fixed
            return SqlParseResult.of(queryId, ParseStatus.ANALYSIS_ERROR, "internal analyzer error: " + e);
        }
    }

    private SqlParseResult doAnalyze(String queryId, String sql, NamespaceContext namespace) {
        SqlSanitizer.SanitizedSql sanitized = SqlSanitizer.sanitize(sql);
        if (sanitized.isEmpty()) {
            return SqlParseResult.of(queryId, ParseStatus.SKIPPED_EMPTY, "empty statement");
        }
        if (sanitized.isManagementStatement()) {
            return SqlParseResult.of(queryId, ParseStatus.SKIPPED_MANAGEMENT, sanitized.getLeadingKeyword());
        }
        StarRocksSqlParser.ParsedSql parsed;
        try {
            parsed = parser.parse(sanitized.getSql());
        } catch (SqlParsingException e) {
            return SqlParseResult.of(queryId, ParseStatus.PARSE_ERROR, e.getMessage());
        } catch (StackOverflowError e) {
            return SqlParseResult.of(queryId, ParseStatus.PARSE_ERROR, "statement nesting too deep to parse");
        }

        List<SqlParseResult> analysed = new ArrayList<>();
        NamespaceContext ns = namespace;
        for (StarRocksParser.StatementContext statement : parsed.getStatements()) {
            Optional<NamespaceContext> switched = namespaceSwitch(statement, ns);
            if (switched.isPresent()) {
                ns = switched.get();
                continue;
            }
            SqlParseResult.Builder builder = SqlParseResult.builder(queryId)
                    .predictionMode(parsed.getPredictionMode().name());
            try {
                StarRocksAuditAstVisitor visitor = new StarRocksAuditAstVisitor(new ScopeStack(ns, metadata), builder,
                        strictColumnResolution);
                if (visitor.analyze(statement).isPresent()) {
                    analysed.add(builder.build());
                }
            } catch (SemanticAnalysisException | UnresolvedNameException e) {
                return SqlParseResult.of(queryId, ParseStatus.ANALYSIS_ERROR, e.getMessage());
            } catch (StackOverflowError e) {
                return SqlParseResult.of(queryId, ParseStatus.ANALYSIS_ERROR, "statement nesting too deep");
            }
        }
        if (analysed.isEmpty()) {
            return SqlParseResult.of(queryId, ParseStatus.SKIPPED_UNSUPPORTED,
                    "no lineage / usage producing statement (" + sanitized.getLeadingKeyword() + ")");
        }
        if (analysed.size() == 1) {
            return analysed.get(0);
        }
        return merge(queryId, parsed, analysed);
    }

    /**
     * Merges the results of a multi-statement text: the first statement's type and target, every edge, usage and
     * warning.
     */
    private static SqlParseResult merge(String queryId, StarRocksSqlParser.ParsedSql parsed,
                                        List<SqlParseResult> results) {
        SqlParseResult first = results.get(0);
        SqlParseResult.Builder b = SqlParseResult.builder(queryId)
                .statementType(first.getStatementType().orElse(StatementType.OTHER))
                .predictionMode(parsed.getPredictionMode().name())
                .addWarning(results.size() + " statements in one audit record were merged");
        first.getTargetTable().ifPresent(b::targetTable);
        for (SqlParseResult r : results) {
            for (TableIdentifier t : r.getSourceTables()) {
                b.addSourceTable(t);
            }
            for (ColumnLineageEdge e : r.getLineageEdges()) {
                b.addEdge(e);
            }
            for (ColumnUsageStat u : r.getColumnUsages().values()) {
                b.addUsage(u);
            }
            for (String w : r.getWarnings()) {
                b.addWarning(w);
            }
        }
        return b.build();
    }

    /**
     * {@code USE db}, {@code USE catalog.db}, {@code USE 'catalog c'} and {@code SET CATALOG c} change the namespace
     * of the following statements of the same text.
     */
    private static Optional<NamespaceContext> namespaceSwitch(StarRocksParser.StatementContext statement,
                                                             NamespaceContext current) {
        if (statement.useDatabaseStatement() != null) {
            List<String> parts = StarRocksAuditAstVisitor.qualifiedNameParts(
                    statement.useDatabaseStatement().qualifiedName());
            if (parts.size() == 2) {
                return Optional.of(current.withCatalog(parts.get(0)).withDatabase(parts.get(1)));
            }
            return Optional.of(current.withDatabase(parts.get(parts.size() - 1)));
        }
        if (statement.useCatalogStatement() != null) {
            // USE 'catalog hive_catalog'
            String value = unquoteString(statement.useCatalogStatement().string().getText()).trim();
            String lower = value.toLowerCase(java.util.Locale.ROOT);
            if (lower.startsWith("catalog")) {
                value = value.substring("catalog".length()).trim();
            }
            return Optional.of(current.withCatalog(value));
        }
        if (statement.setCatalogStatement() != null) {
            StarRocksParser.IdentifierOrStringContext c = statement.setCatalogStatement().identifierOrString();
            String value = c.identifier() != null ? StarRocksAuditAstVisitor.identifierText(c.identifier())
                    : unquoteString(c.string().getText());
            return Optional.of(current.withCatalog(value));
        }
        return Optional.empty();
    }

    private static String unquoteString(String quoted) {
        if (quoted.length() >= 2) {
            char q = quoted.charAt(0);
            if ((q == '\'' || q == '"') && quoted.charAt(quoted.length() - 1) == q) {
                return quoted.substring(1, quoted.length() - 1).replace(String.valueOf(q) + q, String.valueOf(q));
            }
        }
        return quoted;
    }
}
