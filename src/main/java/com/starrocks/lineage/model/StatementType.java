package com.starrocks.lineage.model;

/**
 * Statement classification, stored in {@code audit_column_lineage.stmt_type}.
 */
public enum StatementType {
    QUERY(false),
    INSERT_INTO(true),
    INSERT_OVERWRITE(true),
    CREATE_TABLE_AS_SELECT(true),
    CREATE_VIEW(true),
    ALTER_VIEW(true),
    CREATE_MATERIALIZED_VIEW(true),
    UPDATE(true),
    DELETE(true),
    SUBMIT_TASK(true),
    /** Recognised statement that produces neither lineage nor usage (DDL without query, etc.). */
    OTHER(false);

    private final boolean write;

    StatementType(boolean write) {
        this.write = write;
    }

    /** @return {@code true} for statements feeding {@code audit_column_lineage} */
    public boolean isWrite() {
        return write;
    }

    /** @return {@code true} for statements feeding {@code audit_column_hourly_usage} */
    public boolean isQuery() {
        return this == QUERY;
    }
}
