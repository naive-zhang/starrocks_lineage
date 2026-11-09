package com.starrocks.lineage.model;

/**
 * How a source column influences a target column.
 */
public enum LineageType {
    /** The source value flows into the target value (projection / SET expression). */
    DIRECT,
    /**
     * The source only controls which rows / groups reach the target (WHERE, JOIN ON/USING, GROUP BY, HAVING,
     * QUALIFY, ORDER BY, window PARTITION BY / ORDER BY). Note that the condition of a {@code CASE WHEN} inside a
     * projection is DIRECT: it is part of the value expression itself.
     */
    INDIRECT
}
