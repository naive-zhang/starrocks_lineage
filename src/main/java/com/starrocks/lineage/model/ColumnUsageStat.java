package com.starrocks.lineage.model;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Usage of one physical column inside <b>one</b> query statement.
 *
 * <p>Counting semantics (single source of truth for the hourly table):</p>
 * <ul>
 *   <li>{@link #getAccessQueryCnt()} is always {@code 1}: however many times the column appears, the query
 *       accessed it once.</li>
 *   <li>Each clause metric ({@code select_clause_cnt}, {@code where_clause_cnt}, {@code join_clause_cnt},
 *       {@code group_by_cnt}, {@code order_by_cnt}) is <b>de-duplicated per query</b>: {@code 1} if the column
 *       occurs at least once in a clause mapped to that metric, else {@code 0}. Hence after hourly SUM each
 *       metric reads "number of queries that used the column in this clause" and is directly comparable with
 *       {@code access_query_cnt}.</li>
 *   <li>Raw occurrence counts per {@link ClauseType} are kept for diagnostics and tests.</li>
 * </ul>
 * <p>Instances are mutable while a single statement is analysed (single-threaded) and must not be shared
 * across threads before the analysis is finished.</p>
 */
public final class ColumnUsageStat {

    private final ColumnIdentifier column;
    private final EnumMap<ClauseType, Integer> occurrences = new EnumMap<>(ClauseType.class);

    public ColumnUsageStat(ColumnIdentifier column) {
        this.column = Objects.requireNonNull(column, "column");
        if (column.isConstant()) {
            throw new IllegalArgumentException("usage cannot be recorded for the constant sentinel");
        }
    }

    /**
     * Records one occurrence of the column in the given clause.
     */
    public ColumnUsageStat record(ClauseType clause) {
        occurrences.merge(Objects.requireNonNull(clause, "clause"), 1, Integer::sum);
        return this;
    }

    /**
     * Adds every occurrence of {@code other} (same column) into this instance.
     */
    public ColumnUsageStat mergeFrom(ColumnUsageStat other) {
        if (!column.equals(other.column)) {
            throw new IllegalArgumentException("cannot merge usage of " + other.column + " into " + column);
        }
        for (Map.Entry<ClauseType, Integer> e : other.occurrences.entrySet()) {
            occurrences.merge(e.getKey(), e.getValue(), Integer::sum);
        }
        return this;
    }

    public ColumnIdentifier getColumn() {
        return column;
    }

    /** @return {@code true} if the column was referenced at all */
    public boolean isAccessed() {
        return !occurrences.isEmpty();
    }

    /** @return 1 if accessed, else 0 */
    public long getAccessQueryCnt() {
        return isAccessed() ? 1L : 0L;
    }

    /** @return 1 if the column occurs in any clause mapped to {@code metric}, else 0 */
    public long metricCount(UsageMetric metric) {
        for (ClauseType clause : occurrences.keySet()) {
            if (clause.getMetric() == metric) {
                return 1L;
            }
        }
        return 0L;
    }

    public long getSelectClauseCnt() {
        return metricCount(UsageMetric.SELECT);
    }

    public long getWhereClauseCnt() {
        return metricCount(UsageMetric.WHERE);
    }

    public long getJoinClauseCnt() {
        return metricCount(UsageMetric.JOIN);
    }

    public long getGroupByCnt() {
        return metricCount(UsageMetric.GROUP_BY);
    }

    public long getOrderByCnt() {
        return metricCount(UsageMetric.ORDER_BY);
    }

    /** @return raw number of occurrences in the clause (diagnostics only) */
    public int occurrences(ClauseType clause) {
        return occurrences.getOrDefault(clause, 0);
    }

    /** @return clauses the column appeared in */
    public Set<ClauseType> getClauses() {
        return occurrences.isEmpty() ? Collections.emptySet()
                : Collections.unmodifiableSet(EnumSet.copyOf(occurrences.keySet()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnUsageStat)) {
            return false;
        }
        ColumnUsageStat that = (ColumnUsageStat) o;
        return column.equals(that.column) && occurrences.equals(that.occurrences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, occurrences);
    }

    @Override
    public String toString() {
        return "ColumnUsageStat{" + column + ", " + occurrences + '}';
    }
}
