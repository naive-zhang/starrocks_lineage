package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.TransformType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Result of resolving a column reference against the {@link ScopeStack}.
 */
public final class ResolvedColumn {

    /**
     * What the reference designates.
     */
    public enum Kind {
        /** A field of a relation of some FROM clause (physical, CTE, sub-query, UNNEST ...). */
        RELATION_FIELD,
        /** A formal parameter of an enclosing lambda ({@code x} in {@code x -> x + 1}). */
        LAMBDA_PARAMETER,
        /** An output alias of the current SELECT list (GROUP BY / HAVING / ORDER BY only). */
        SELECT_ALIAS
    }

    private final Kind kind;
    private final RelationDescriptor relation;
    private final RelationField field;
    private final List<String> subfieldPath;
    private final int outerDepth;
    private final boolean onDemand;

    ResolvedColumn(Kind kind, RelationDescriptor relation, RelationField field, List<String> subfieldPath,
                   int outerDepth, boolean onDemand) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.relation = relation;
        this.field = Objects.requireNonNull(field, "field");
        this.subfieldPath = subfieldPath == null ? Collections.emptyList()
                : Collections.unmodifiableList(subfieldPath);
        this.outerDepth = outerDepth;
        this.onDemand = onDemand;
        if (kind == Kind.RELATION_FIELD && relation == null) {
            throw new IllegalArgumentException("a relation field needs its relation");
        }
    }

    ResolvedColumn atDepth(int depth) {
        return depth == outerDepth ? this : new ResolvedColumn(kind, relation, field, subfieldPath, depth, onDemand);
    }

    public Kind getKind() {
        return kind;
    }

    /** @return the relation for {@link Kind#RELATION_FIELD} */
    public Optional<RelationDescriptor> getRelation() {
        return Optional.ofNullable(relation);
    }

    public RelationField getField() {
        return field;
    }

    /** @return struct sub-field path after the column ({@code c.f1.f2} gives {@code [f1, f2]}) */
    public List<String> getSubfieldPath() {
        return subfieldPath;
    }

    /**
     * @return number of query blocks crossed outwards: 0 for a local reference, &gt; 0 for a correlated
     *         (outer) reference
     */
    public int getOuterDepth() {
        return outerDepth;
    }

    public boolean isCorrelated() {
        return outerDepth > 0;
    }

    /** @return {@code true} when the field was created on demand on a relation with unknown schema */
    public boolean isOnDemand() {
        return onDemand;
    }

    /**
     * @return lineage of the referenced value; reading a struct sub-field is a transformation of the column, so
     *         a non-empty sub-field path elevates the lineage to {@link TransformType#EXPRESSION}
     */
    public FieldLineage lineage() {
        FieldLineage l = field.getLineage();
        return subfieldPath.isEmpty() ? l : l.elevate(TransformType.EXPRESSION);
    }

    @Override
    public String toString() {
        return kind + ":" + (relation == null ? "" : relation.displayName() + ".") + field.getName()
                + (subfieldPath.isEmpty() ? "" : "." + String.join(".", subfieldPath))
                + (outerDepth > 0 ? "@outer" + outerDepth : "");
    }
}
