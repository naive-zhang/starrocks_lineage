package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.metadata.MetadataAccessException;
import com.starrocks.lineage.metadata.MetadataProvider;
import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.scope.SemanticAnalysisException;
import com.starrocks.sql.parser.SqlModeHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.Optional;

import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.SALES;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.analyze;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.assertClauses;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.assertDirect;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.c;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.catalog;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.success;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StarRocksSqlAnalyzerFacadeTest {

    private static AuditLogRecord.Builder record(String stmt) {
        return AuditLogRecord.builder().queryId("qid-1").timestamp(LocalDateTime.of(2024, 5, 1, 10, 30))
                .user("alice").catalog("default_catalog").db("sales").state("EOF").errorCode("").stmt(stmt);
    }

    @Test
    void auditRecordPathUsesRecordNamespaceAndCleansDriverComments() {
        SqlParseResult r = new StarRocksSqlAnalyzer(catalog()).analyze(record(
                "/* ApplicationName=DBeaver 23.1 - Main */ SELECT city FROM users -- trailing\n;").build());
        assertEquals(ParseStatus.SUCCESS, r.getStatus());
        assertEquals("qid-1", r.getQueryId());
        assertClauses(r, "users.city", ClauseType.SELECT);
        assertEquals("SLL", r.getPredictionMode().get());
    }

    @Test
    void failedEmptyManagementAndUnsupportedStatementsAreSkipped() {
        StarRocksSqlAnalyzer a = new StarRocksSqlAnalyzer(catalog());
        assertEquals(ParseStatus.SKIPPED_FAILED_QUERY,
                a.analyze(record("SELECT 1").state("ERR").build()).getStatus());
        assertEquals(ParseStatus.SKIPPED_FAILED_QUERY,
                a.analyze(record("SELECT 1").errorCode("1064").build()).getStatus());
        assertEquals(ParseStatus.SKIPPED_EMPTY, a.analyze(record("/* nothing */ ;").build()).getStatus());
        assertEquals(ParseStatus.SKIPPED_MANAGEMENT, a.analyze(record("SHOW TABLES").build()).getStatus());
        assertEquals(ParseStatus.SKIPPED_UNSUPPORTED,
                a.analyze(record("CREATE TABLE t (a INT) DISTRIBUTED BY HASH(a)").build()).getStatus());
        assertEquals(ParseStatus.SKIPPED_UNSUPPORTED,
                a.analyze(record("DROP TABLE IF EXISTS t").build()).getStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = {"EXPLAIN SELECT 1", "DESC SELECT city FROM users",
        "EXPLAIN INSERT INTO dw.target SELECT 1, 2, 3", "TRACE OPTIMIZER SELECT city FROM users"})
    void explainedStatementsProduceNothing(String sql) {
        SqlParseResult r = analyze(sql);
        assertTrue(r.getStatus().isSkipped(), () -> r.toString());
    }

    @Test
    void parseAndAnalysisErrorsAreClassified() {
        SqlParseResult parse = analyze("SELECT FROM WHERE");
        assertEquals(ParseStatus.PARSE_ERROR, parse.getStatus());
        assertTrue(parse.getMessage().get().contains("Syntax error"));

        SqlParseResult ambiguous = analyze("SELECT user_id FROM orders JOIN users ON orders.user_id = users.user_id");
        assertEquals(ParseStatus.ANALYSIS_ERROR, ambiguous.getStatus());
        assertTrue(ambiguous.getMessage().get().contains("AMBIGUOUS_COLUMN"));

        SqlParseResult noDb = new StarRocksSqlAnalyzer(catalog()).analyze("q", "SELECT a FROM t",
                NamespaceContext.of("default_catalog", null));
        assertEquals(ParseStatus.ANALYSIS_ERROR, noDb.getStatus());
        assertTrue(noDb.getMessage().get().contains("No database selected"));
    }

    @Test
    void multiStatementTextSwitchesNamespace() {
        SqlParseResult r = analyze("USE dw; INSERT INTO target (uid) SELECT user_id FROM sales.orders");
        assertEquals(ParseStatus.SUCCESS, r.getStatus());
        assertEquals(c("dw.target.x").getTable(), r.getTargetTable().get());
        assertDirect(r, "dw.target.uid", "orders.user_id", TransformType.IDENTITY);

        SqlParseResult catalogSwitch = analyze("SET CATALOG hive; USE ods; SELECT payload FROM events");
        assertClauses(catalogSwitch, "hive.ods.events.payload", ClauseType.SELECT);

        SqlParseResult useCatalog = analyze("USE 'catalog hive'; SELECT payload FROM ods.events");
        assertClauses(useCatalog, "hive.ods.events.payload", ClauseType.SELECT);

        SqlParseResult two = analyze("SELECT city FROM users; SELECT amount FROM orders WHERE dt > '2024'");
        assertEquals(StatementType.QUERY, two.getStatementType().get());
        assertEquals(3, two.getColumnUsages().size());
        assertTrue(two.getWarnings().get(0).contains("2 statements"));
    }

    @Test
    void metadataInfrastructureFailureIsRethrownForRetry() {
        MetadataProvider broken = new MetadataProvider() {
            @Override
            public Optional<TableSchema> getTableSchema(TableIdentifier table) {
                throw new MetadataAccessException("FE unreachable", null);
            }
        };
        assertThrows(MetadataAccessException.class,
                () -> new StarRocksSqlAnalyzer(broken).analyze("q", "SELECT a FROM t", SALES));
    }

    @Test
    void lenientAndStrictResolutionOfStaleMetadata() {
        SqlParseResult lenient = analyze("SELECT o.new_col FROM orders o");
        assertEquals(ParseStatus.SUCCESS, lenient.getStatus());
        assertTrue(lenient.isDegraded());
        assertClauses(lenient, "orders.new_col", ClauseType.SELECT);

        SqlParseResult strict = new StarRocksSqlAnalyzer(catalog(), SqlModeHelper.MODE_DEFAULT, true)
                .analyze("q", "SELECT o.new_col FROM orders o", SALES);
        assertEquals(ParseStatus.ANALYSIS_ERROR, strict.getStatus());
        assertTrue(strict.getMessage().get().contains(SemanticAnalysisException.ErrorCode.COLUMN_NOT_FOUND.name()));
    }

    @Test
    void unknownTablesDegradeGracefully() {
        SqlParseResult q = success("SELECT g.a, g.b FROM ghost g WHERE g.c = 1");
        assertTrue(q.isDegraded());
        assertClauses(q, "ghost.a", ClauseType.SELECT);
        assertClauses(q, "ghost.c", ClauseType.WHERE);

        SqlParseResult star = success("INSERT INTO dw.target SELECT * FROM ghost");
        assertTrue(star.getLineageEdges().isEmpty(), "positions behind an unexpanded * are never guessed");
        assertTrue(star.getWarnings().stream().anyMatch(w -> w.contains("could be aligned")));
    }

    @Test
    void pipesAsConcatFollowsSqlMode() {
        SqlParseResult concat = new StarRocksSqlAnalyzer(catalog(), SqlModeHelper.MODE_PIPES_AS_CONCAT, false)
                .analyze("q", "INSERT INTO dw.target (uid) SELECT name || city FROM users", SALES);
        assertDirect(concat, "dw.target.uid", "users.name", TransformType.EXPRESSION);
        assertDirect(concat, "dw.target.uid", "users.city", TransformType.EXPRESSION);
        assertFalse(concat.isDegraded());
    }
}
