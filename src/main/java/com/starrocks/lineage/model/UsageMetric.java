package com.starrocks.lineage.model;

/**
 * Metric columns of {@code audit_column_hourly_usage} that a {@link ClauseType} contributes to.
 */
public enum UsageMetric {
    SELECT("select_clause_cnt"),
    WHERE("where_clause_cnt"),
    JOIN("join_clause_cnt"),
    GROUP_BY("group_by_cnt"),
    ORDER_BY("order_by_cnt"),
    /** Access that is not attributable to one of the five counted clauses (still counts in access_query_cnt). */
    OTHER(null);

    private final String columnName;

    UsageMetric(String columnName) {
        this.columnName = columnName;
    }

    /** @return storage column name, {@code null} for {@link #OTHER} */
    public String getColumnName() {
        return columnName;
    }
}
