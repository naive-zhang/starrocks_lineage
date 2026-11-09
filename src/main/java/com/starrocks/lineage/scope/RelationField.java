package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.Identifiers;

import java.util.Objects;

/**
 * One output column of a relation (physical table, CTE, sub-query, table function, VALUES, set operation).
 * Immutable. The name is normalised to lower case because StarRocks column names are case-insensitive;
 * duplicates are allowed inside derived relations ({@code SELECT a, a FROM t}) and are detected on lookup.
 */
public final class RelationField {

    private final String name;
    private final int index;
    private final FieldLineage lineage;
    private final String expression;

    /**
     * @param name       output column name (alias, column name or StarRocks' generated name)
     * @param index      0-based position inside the relation
     * @param lineage    physical lineage of the value
     * @param expression source text producing the value, used as {@code transform_expr}; may be {@code null}
     */
    public RelationField(String name, int index, FieldLineage lineage, String expression) {
        String n = Identifiers.normalizeColumn(name);
        if (n == null || n.isEmpty()) {
            throw new IllegalArgumentException("field name must not be empty");
        }
        if (index < 0) {
            throw new IllegalArgumentException("field index must be >= 0");
        }
        this.name = n;
        this.index = index;
        this.lineage = Objects.requireNonNull(lineage, "lineage");
        this.expression = expression;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public FieldLineage getLineage() {
        return lineage;
    }

    public String getExpression() {
        return expression;
    }

    /** @return {@code true} when {@code other} designates this field (case-insensitive, backquote tolerant) */
    public boolean hasName(String other) {
        String n = Identifiers.normalizeColumn(other);
        return name.equals(n);
    }

    RelationField withIndex(int newIndex) {
        return newIndex == index ? this : new RelationField(name, newIndex, lineage, expression);
    }

    RelationField withName(String newName) {
        return new RelationField(newName, index, lineage, expression);
    }

    RelationField withLineage(FieldLineage newLineage) {
        return new RelationField(name, index, newLineage, expression);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RelationField)) {
            return false;
        }
        RelationField that = (RelationField) o;
        return index == that.index && name.equals(that.name) && lineage.equals(that.lineage)
                && Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, index, lineage, expression);
    }

    @Override
    public String toString() {
        return name + "#" + index + lineage.getDirect().keySet();
    }
}
