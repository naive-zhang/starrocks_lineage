package com.starrocks.lineage.model;

/**
 * Syntactic clause in which a column reference occurs. The AST visitor keeps a stack of these; the top of the
 * stack is attributed to every resolved physical column. Each clause maps to exactly one {@link UsageMetric}
 * and to the {@link LineageType} it implies for write statements.
 */
public enum ClauseType {
    /** Projection list (including expressions and function arguments inside it). */
    SELECT(UsageMetric.SELECT, LineageType.DIRECT),
    /** {@code WHERE} of a query, {@code UPDATE} or {@code DELETE}. */
    WHERE(UsageMetric.WHERE, LineageType.INDIRECT),
    /** {@code JOIN ... ON}. */
    JOIN_ON(UsageMetric.JOIN, LineageType.INDIRECT),
    /** {@code JOIN ... USING (...)}. */
    JOIN_USING(UsageMetric.JOIN, LineageType.INDIRECT),
    /** {@code GROUP BY}, {@code ROLLUP}, {@code CUBE}, {@code GROUPING SETS}. */
    GROUP_BY(UsageMetric.GROUP_BY, LineageType.INDIRECT),
    /** {@code HAVING} - counted in {@code group_by_cnt} per specification. */
    HAVING(UsageMetric.GROUP_BY, LineageType.INDIRECT),
    /** Top level / sub-query {@code ORDER BY}. */
    ORDER_BY(UsageMetric.ORDER_BY, LineageType.INDIRECT),
    /** {@code OVER (PARTITION BY ...)} - grouping semantics, counted in {@code group_by_cnt}. */
    WINDOW_PARTITION_BY(UsageMetric.GROUP_BY, LineageType.INDIRECT),
    /** {@code OVER (ORDER BY ...)} - counted in {@code order_by_cnt} per specification. */
    WINDOW_ORDER_BY(UsageMetric.ORDER_BY, LineageType.INDIRECT),
    /** {@code QUALIFY} - post-window row filter, counted in {@code where_clause_cnt}. */
    QUALIFY(UsageMetric.WHERE, LineageType.INDIRECT),
    /** Arguments of a table function in FROM, e.g. {@code UNNEST(t.arr)}. */
    TABLE_FUNCTION(UsageMetric.OTHER, LineageType.DIRECT),
    /** {@code SET col = expr} of an {@code UPDATE}. */
    UPDATE_SET(UsageMetric.OTHER, LineageType.DIRECT);

    private final UsageMetric metric;
    private final LineageType lineageType;

    ClauseType(UsageMetric metric, LineageType lineageType) {
        this.metric = metric;
        this.lineageType = lineageType;
    }

    public UsageMetric getMetric() {
        return metric;
    }

    /** @return lineage type implied for write statements when a source column is seen in this clause */
    public LineageType getLineageType() {
        return lineageType;
    }
}
