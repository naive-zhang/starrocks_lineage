package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.TransformType;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable lineage of one value (a relation field or an expression), expressed in <b>physical</b> columns only.
 *
 * <ul>
 *   <li>{@link #getDirect()}: physical columns whose values flow into this value, each with the dominant
 *       {@link TransformType} along its path (e.g. {@code SUM(a) + b}: {@code a -> AGGREGATION},
 *       {@code b -> EXPRESSION}).</li>
 *   <li>{@link #getIndirect()}: physical columns that influence this value without flowing into it and that are
 *       specific to this value, e.g. the {@code PARTITION BY} / {@code ORDER BY} columns of a window function.
 *       Row-level control (WHERE, JOIN, GROUP BY ...) is kept on {@link RelationDescriptor#getControlSources()}
 *       instead, because it applies to every field of a relation.</li>
 * </ul>
 * <p>Because every derived field is expressed in physical columns, lineage through any number of CTE / sub-query
 * layers is obtained by simple composition ({@link #elevate(TransformType)}) - no layer ever needs to be
 * re-visited.</p>
 */
public final class FieldLineage {

    private static final FieldLineage CONSTANT = new FieldLineage(Collections.emptyMap(), Collections.emptySet());

    private final Map<ColumnIdentifier, TransformType> direct;
    private final Set<ColumnIdentifier> indirect;

    private FieldLineage(Map<ColumnIdentifier, TransformType> direct, Set<ColumnIdentifier> indirect) {
        this.direct = direct;
        this.indirect = indirect;
    }

    /** @return lineage of a value without physical source (literal, {@code COUNT(*)}, {@code NOW()}) */
    public static FieldLineage constant() {
        return CONSTANT;
    }

    /** @return lineage of a physical column read as is */
    public static FieldLineage physical(ColumnIdentifier column) {
        Objects.requireNonNull(column, "column");
        if (column.isConstant()) {
            return CONSTANT;
        }
        Map<ColumnIdentifier, TransformType> d = new LinkedHashMap<>();
        d.put(column, TransformType.IDENTITY);
        return new FieldLineage(Collections.unmodifiableMap(d), Collections.emptySet());
    }

    /**
     * @param direct   direct sources with transform types (the constant sentinel is ignored)
     * @param indirect indirect sources (the constant sentinel is ignored)
     */
    public static FieldLineage of(Map<ColumnIdentifier, TransformType> direct, Collection<ColumnIdentifier> indirect) {
        Map<ColumnIdentifier, TransformType> d = new LinkedHashMap<>();
        for (Map.Entry<ColumnIdentifier, TransformType> e : direct.entrySet()) {
            if (!e.getKey().isConstant()) {
                TransformType t = Objects.requireNonNull(e.getValue(), "transform type");
                if (t == TransformType.CONSTANT) {
                    throw new IllegalArgumentException("a physical source cannot carry TransformType.CONSTANT");
                }
                d.put(e.getKey(), t);
            }
        }
        Set<ColumnIdentifier> i = new LinkedHashSet<>();
        for (ColumnIdentifier c : indirect) {
            if (!c.isConstant()) {
                i.add(c);
            }
        }
        if (d.isEmpty() && i.isEmpty()) {
            return CONSTANT;
        }
        return new FieldLineage(Collections.unmodifiableMap(d), Collections.unmodifiableSet(i));
    }

    public Map<ColumnIdentifier, TransformType> getDirect() {
        return direct;
    }

    public Set<ColumnIdentifier> getIndirect() {
        return indirect;
    }

    /** @return {@code true} when no physical column flows into the value */
    public boolean isConstant() {
        return direct.isEmpty();
    }

    /** @return {@code true} when neither direct nor indirect sources exist */
    public boolean isEmpty() {
        return direct.isEmpty() && indirect.isEmpty();
    }

    /** @return strongest transform over all direct sources, {@link TransformType#CONSTANT} when there is none */
    public TransformType dominantTransform() {
        TransformType result = TransformType.CONSTANT;
        for (TransformType t : direct.values()) {
            result = result.combine(t);
        }
        return result;
    }

    /** @return every direct and indirect physical source */
    public Set<ColumnIdentifier> allSources() {
        Set<ColumnIdentifier> all = new LinkedHashSet<>(direct.keySet());
        all.addAll(indirect);
        return Collections.unmodifiableSet(all);
    }

    /**
     * Composes this lineage with an outer transformation, e.g. a CTE field that is {@code EXPRESSION} read inside
     * an outer {@code SUM(...)} becomes {@code AGGREGATION}; read as a plain column it stays {@code EXPRESSION}.
     */
    public FieldLineage elevate(TransformType outer) {
        Objects.requireNonNull(outer, "outer");
        if (direct.isEmpty() || outer == TransformType.CONSTANT || outer == TransformType.IDENTITY) {
            return this;
        }
        Map<ColumnIdentifier, TransformType> d = new LinkedHashMap<>();
        for (Map.Entry<ColumnIdentifier, TransformType> e : direct.entrySet()) {
            d.put(e.getKey(), e.getValue().combine(outer));
        }
        return new FieldLineage(Collections.unmodifiableMap(d), indirect);
    }

    /**
     * Union used for expressions over several inputs and for positional merge of set-operation branches.
     * A source present in both keeps the dominant transform.
     */
    public FieldLineage union(FieldLineage other) {
        Objects.requireNonNull(other, "other");
        if (other.isEmpty()) {
            return this;
        }
        if (this.isEmpty()) {
            return other;
        }
        Map<ColumnIdentifier, TransformType> d = new LinkedHashMap<>(direct);
        for (Map.Entry<ColumnIdentifier, TransformType> e : other.direct.entrySet()) {
            d.merge(e.getKey(), e.getValue(), TransformType::combine);
        }
        Set<ColumnIdentifier> i = new LinkedHashSet<>(indirect);
        i.addAll(other.indirect);
        return new FieldLineage(Collections.unmodifiableMap(d), Collections.unmodifiableSet(i));
    }

    /** @return a copy with additional indirect sources */
    public FieldLineage withIndirect(Collection<ColumnIdentifier> extra) {
        if (extra.isEmpty()) {
            return this;
        }
        Set<ColumnIdentifier> i = new LinkedHashSet<>(indirect);
        for (ColumnIdentifier c : extra) {
            if (!c.isConstant()) {
                i.add(c);
            }
        }
        return new FieldLineage(direct, Collections.unmodifiableSet(i));
    }

    /** @return a copy in which every source (direct and indirect) is indirect */
    public FieldLineage asIndirect() {
        return of(Collections.emptyMap(), allSources());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldLineage)) {
            return false;
        }
        FieldLineage that = (FieldLineage) o;
        return direct.equals(that.direct) && indirect.equals(that.indirect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(direct, indirect);
    }

    @Override
    public String toString() {
        return "FieldLineage{direct=" + direct + ", indirect=" + indirect + '}';
    }
}
