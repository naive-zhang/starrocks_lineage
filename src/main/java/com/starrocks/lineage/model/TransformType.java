package com.starrocks.lineage.model;

/**
 * Nature of the transformation applied on a DIRECT lineage path.
 *
 * <p>Declaration order is the <b>dominance order</b> used by {@link #combine(TransformType)}: when a value
 * passes through several layers (CTE, sub-query, view-like nesting) or an expression mixes several kinds
 * of operations, the strongest kind wins. Example: {@code SUM(a) + 1} is {@link #AGGREGATION};
 * {@code SUM(a) OVER (...)} is {@link #WINDOW}; an IDENTITY projection of a column that was an EXPRESSION in an
 * inner CTE stays {@link #EXPRESSION}.</p>
 */
public enum TransformType {
    /** Literal only, no physical source. */
    CONSTANT,
    /** Column copied unchanged (possibly renamed / cast-free). */
    IDENTITY,
    /** Scalar expression or function (including CAST, CASE, arithmetic, lambda). */
    EXPRESSION,
    /** Aggregate function. */
    AGGREGATION,
    /** Window (analytic) function. */
    WINDOW;

    /**
     * @param other the other transformation on the same path, may be {@code null}
     * @return the dominant transformation
     */
    public TransformType combine(TransformType other) {
        if (other == null) {
            return this;
        }
        return this.ordinal() >= other.ordinal() ? this : other;
    }
}
