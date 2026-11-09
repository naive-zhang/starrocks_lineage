package com.starrocks.lineage.scope;

/**
 * Origin of a {@link RelationDescriptor}.
 */
public enum RelationKind {
    /** Physical table, view or materialized view resolved through the catalog. */
    TABLE,
    /** Reference to a common table expression. */
    CTE,
    /** Parenthesised query in FROM ({@code (SELECT ...) t}). */
    SUBQUERY,
    /** {@code UNNEST(...)} / lateral table function. */
    TABLE_FUNCTION,
    /** {@code (VALUES (...), (...)) t(a, b)}. */
    VALUES,
    /** Result of {@code UNION / INTERSECT / EXCEPT}. */
    SET_OPERATION,
    /** Output of a query block, used to resolve the {@code ORDER BY} of a set operation. */
    QUERY_OUTPUT
}
