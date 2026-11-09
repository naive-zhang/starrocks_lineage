package com.starrocks.lineage.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable analysis result of one audit statement.
 *
 * <ul>
 *   <li>Write statements fill {@link #getLineageEdges()} and {@link #getTargetTable()}.</li>
 *   <li>Query statements fill {@link #getColumnUsages()}.</li>
 *   <li>Skipped / failed statements carry only {@link #getStatus()} and {@link #getMessage()}.</li>
 * </ul>
 * Edges with the same identity are folded with {@link ColumnLineageEdge#merge(ColumnLineageEdge)} and usages of
 * the same column with {@link ColumnUsageStat#mergeFrom(ColumnUsageStat)} while building, so consumers never see
 * duplicates.
 */
public final class SqlParseResult {

    private final String queryId;
    private final ParseStatus status;
    private final StatementType statementType;
    private final String message;
    private final String predictionMode;
    private final TableIdentifier targetTable;
    private final Set<TableIdentifier> sourceTables;
    private final List<ColumnLineageEdge> lineageEdges;
    private final Map<ColumnIdentifier, ColumnUsageStat> columnUsages;
    private final List<String> warnings;

    private SqlParseResult(Builder b) {
        this.queryId = b.queryId;
        this.status = Objects.requireNonNull(b.status, "status");
        this.statementType = b.statementType;
        this.message = b.message;
        this.predictionMode = b.predictionMode;
        this.targetTable = b.targetTable;
        this.sourceTables = Collections.unmodifiableSet(new LinkedHashSet<>(b.sourceTables));
        this.lineageEdges = Collections.unmodifiableList(new ArrayList<>(b.edges.values()));
        Map<ColumnIdentifier, ColumnUsageStat> usages = new LinkedHashMap<>();
        for (Map.Entry<ColumnIdentifier, ColumnUsageStat> e : b.usages.entrySet()) {
            usages.put(e.getKey(), new ColumnUsageStat(e.getKey()).mergeFrom(e.getValue()));
        }
        this.columnUsages = Collections.unmodifiableMap(usages);
        this.warnings = Collections.unmodifiableList(new ArrayList<>(b.warnings));
        if (status == ParseStatus.SUCCESS && statementType == null) {
            throw new IllegalStateException("a successful result must carry a statement type");
        }
    }

    public static Builder builder(String queryId) {
        return new Builder(queryId);
    }

    /** Shortcut for a skipped or failed record. */
    public static SqlParseResult of(String queryId, ParseStatus status, String message) {
        if (status == ParseStatus.SUCCESS) {
            throw new IllegalArgumentException("use builder() for successful results");
        }
        return new Builder(queryId).status(status).message(message).build();
    }

    public String getQueryId() {
        return queryId;
    }

    public ParseStatus getStatus() {
        return status;
    }

    public boolean isSuccess() {
        return status == ParseStatus.SUCCESS;
    }

    public Optional<StatementType> getStatementType() {
        return Optional.ofNullable(statementType);
    }

    /** @return skip reason or error message */
    public Optional<String> getMessage() {
        return Optional.ofNullable(message);
    }

    /** @return {@code SLL} / {@code LL} when the statement was parsed */
    public Optional<String> getPredictionMode() {
        return Optional.ofNullable(predictionMode);
    }

    public Optional<TableIdentifier> getTargetTable() {
        return Optional.ofNullable(targetTable);
    }

    public Set<TableIdentifier> getSourceTables() {
        return sourceTables;
    }

    public List<ColumnLineageEdge> getLineageEdges() {
        return lineageEdges;
    }

    public Map<ColumnIdentifier, ColumnUsageStat> getColumnUsages() {
        return columnUsages;
    }

    /**
     * @return diagnostics of a successful but imperfect analysis (relation without metadata, unexpanded
     *         {@code *}, column count mismatch ...), de-duplicated, in discovery order
     */
    public List<String> getWarnings() {
        return warnings;
    }

    /** @return {@code true} when the result is usable but may be incomplete, see {@link #getWarnings()} */
    public boolean isDegraded() {
        return !warnings.isEmpty();
    }

    /** @return usage of one column, if the query accessed it */
    public Optional<ColumnUsageStat> usageOf(ColumnIdentifier column) {
        return Optional.ofNullable(columnUsages.get(column));
    }

    @Override
    public String toString() {
        return "SqlParseResult{queryId=" + queryId + ", status=" + status + ", type=" + statementType
                + ", edges=" + lineageEdges.size() + ", usages=" + columnUsages.size()
                + (warnings.isEmpty() ? "" : ", warnings=" + warnings)
                + (message == null ? "" : ", message=" + message) + '}';
    }

    /**
     * Mutable builder; not thread-safe.
     */
    public static final class Builder {
        private final String queryId;
        private ParseStatus status = ParseStatus.SUCCESS;
        private StatementType statementType;
        private String message;
        private String predictionMode;
        private TableIdentifier targetTable;
        private final Set<TableIdentifier> sourceTables = new LinkedHashSet<>();
        private final Map<ColumnLineageEdge, ColumnLineageEdge> edges = new LinkedHashMap<>();
        private final Map<ColumnIdentifier, ColumnUsageStat> usages = new LinkedHashMap<>();
        private final Set<String> warnings = new LinkedHashSet<>();

        private Builder(String queryId) {
            this.queryId = queryId;
        }

        public Builder status(ParseStatus v) {
            this.status = v;
            return this;
        }

        public Builder statementType(StatementType v) {
            this.statementType = v;
            return this;
        }

        public Builder message(String v) {
            this.message = v;
            return this;
        }

        public Builder predictionMode(String v) {
            this.predictionMode = v;
            return this;
        }

        public Builder targetTable(TableIdentifier v) {
            this.targetTable = v;
            return this;
        }

        public Builder addSourceTable(TableIdentifier table) {
            sourceTables.add(Objects.requireNonNull(table, "table"));
            return this;
        }

        public Builder addEdge(ColumnLineageEdge edge) {
            Objects.requireNonNull(edge, "edge");
            edges.merge(edge, edge, ColumnLineageEdge::merge);
            return this;
        }

        public Builder addEdges(Collection<ColumnLineageEdge> all) {
            for (ColumnLineageEdge e : all) {
                addEdge(e);
            }
            return this;
        }

        /** Records one occurrence of {@code column} in {@code clause}. */
        public Builder recordUsage(ColumnIdentifier column, ClauseType clause) {
            usages.computeIfAbsent(column, ColumnUsageStat::new).record(clause);
            return this;
        }

        public Builder addUsage(ColumnUsageStat stat) {
            usages.computeIfAbsent(stat.getColumn(), ColumnUsageStat::new).mergeFrom(stat);
            return this;
        }

        /** Adds a diagnostic; duplicates are ignored. */
        public Builder addWarning(String warning) {
            warnings.add(Objects.requireNonNull(warning, "warning"));
            return this;
        }

        public StatementType getStatementType() {
            return statementType;
        }

        public SqlParseResult build() {
            return new SqlParseResult(this);
        }
    }
}
