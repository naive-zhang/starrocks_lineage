package com.starrocks.lineage.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * One column-level lineage edge {@code source -> target}.
 *
 * <p><b>Identity</b> ({@link #equals(Object)}, {@link #hashCode()}, {@link #edgeId()}) is the triple
 * {@code (target, source, lineageType)}: this is exactly the logical primary key of
 * {@code audit_column_lineage}. The same source may legitimately reach the same target both DIRECT and
 * INDIRECT (e.g. {@code INSERT INTO t2 SELECT a FROM t1 WHERE a > 0}), which yields two rows.</p>
 *
 * <p>{@code transformType} and {@code transformExpr} are attributes; when two edges with the same identity are
 * produced for one statement they are folded with {@link #merge(ColumnLineageEdge)}.</p>
 */
public final class ColumnLineageEdge {

    /** Upper bound of the stored expression text (column is VARCHAR(2048) in storage). */
    public static final int MAX_EXPR_LENGTH = 2048;

    private static final char FIELD_SEPARATOR = '\u0001';

    private final ColumnIdentifier target;
    private final ColumnIdentifier source;
    private final LineageType lineageType;
    private final TransformType transformType;
    private final String transformExpr;
    /** Lazily computed SHA-256 identity; benign race - every thread computes the same value. */
    private volatile String edgeId;

    private ColumnLineageEdge(ColumnIdentifier target, ColumnIdentifier source, LineageType lineageType,
                              TransformType transformType, String transformExpr) {
        this.target = Objects.requireNonNull(target, "target");
        this.source = Objects.requireNonNull(source, "source");
        this.lineageType = Objects.requireNonNull(lineageType, "lineageType");
        this.transformType = Objects.requireNonNull(transformType, "transformType");
        if (target.isConstant()) {
            throw new IllegalArgumentException("target column cannot be the constant sentinel");
        }
        if (source.isConstant() && transformType != TransformType.CONSTANT) {
            throw new IllegalArgumentException("constant source requires TransformType.CONSTANT");
        }
        if (!source.isConstant() && transformType == TransformType.CONSTANT) {
            throw new IllegalArgumentException("TransformType.CONSTANT requires the constant source");
        }
        if (lineageType == LineageType.INDIRECT && source.isConstant()) {
            throw new IllegalArgumentException("an INDIRECT edge must have a physical source");
        }
        this.transformExpr = truncate(transformExpr);
    }

    public static ColumnLineageEdge of(ColumnIdentifier target, ColumnIdentifier source, LineageType lineageType,
                                       TransformType transformType, String transformExpr) {
        return new ColumnLineageEdge(target, source, lineageType, transformType, transformExpr);
    }

    /**
     * Edge for a target column fed only by a literal.
     */
    public static ColumnLineageEdge constant(ColumnIdentifier target, String literalExpr) {
        return new ColumnLineageEdge(target, ColumnIdentifier.CONSTANT, LineageType.DIRECT, TransformType.CONSTANT,
                literalExpr);
    }

    private static String truncate(String expr) {
        if (expr == null) {
            return null;
        }
        String e = expr.trim();
        if (e.length() <= MAX_EXPR_LENGTH) {
            return e;
        }
        int cut = MAX_EXPR_LENGTH - 3;
        if (Character.isHighSurrogate(e.charAt(cut - 1))) {
            cut--;
        }
        return e.substring(0, cut) + "...";
    }

    /**
     * Folds another edge with the same identity into this one.
     *
     * @param other edge with equal identity
     * @return merged edge: dominant transform type; the expression of the dominant side (first one on a tie)
     * @throws IllegalArgumentException when identities differ
     */
    public ColumnLineageEdge merge(ColumnLineageEdge other) {
        if (!this.equals(other)) {
            throw new IllegalArgumentException("cannot merge edges with different identity: " + this + " / " + other);
        }
        TransformType merged = transformType.combine(other.transformType);
        String expr = merged == transformType ? (transformExpr != null ? transformExpr : other.transformExpr)
                : (other.transformExpr != null ? other.transformExpr : transformExpr);
        return new ColumnLineageEdge(target, source, lineageType, merged, expr);
    }

    /**
     * Deterministic 64 hex char SHA-256 of the edge identity. Used as the physical primary key of
     * {@code audit_column_lineage} because the natural 9-column key easily exceeds the 128 byte primary key
     * length limit of StarRocks 3.1 Primary Key tables.
     */
    public String edgeId() {
        String id = edgeId;
        if (id == null) {
            id = computeEdgeId();
            edgeId = id;
        }
        return id;
    }

    private String computeEdgeId() {
        String raw = target.getCatalog() + FIELD_SEPARATOR + target.getDatabase() + FIELD_SEPARATOR
                + target.getTableName() + FIELD_SEPARATOR + target.getColumn() + FIELD_SEPARATOR
                + source.getCatalog() + FIELD_SEPARATOR + source.getDatabase() + FIELD_SEPARATOR
                + source.getTableName() + FIELD_SEPARATOR + source.getColumn() + FIELD_SEPARATOR
                + lineageType.name();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is mandatory on every Java platform", e);
        }
    }

    public ColumnIdentifier getTarget() {
        return target;
    }

    public ColumnIdentifier getSource() {
        return source;
    }

    public LineageType getLineageType() {
        return lineageType;
    }

    public TransformType getTransformType() {
        return transformType;
    }

    public String getTransformExpr() {
        return transformExpr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnLineageEdge)) {
            return false;
        }
        ColumnLineageEdge that = (ColumnLineageEdge) o;
        return target.equals(that.target) && source.equals(that.source) && lineageType == that.lineageType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, source, lineageType);
    }

    @Override
    public String toString() {
        return source + " -[" + lineageType + "/" + transformType + "]-> " + target;
    }
}
