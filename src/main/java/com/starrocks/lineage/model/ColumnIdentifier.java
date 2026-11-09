package com.starrocks.lineage.model;

import java.util.Comparator;
import java.util.Objects;

/**
 * Physical column {@code catalog.db.table.column}. Column names are lower-cased (StarRocks column names are
 * case-insensitive). Immutable, usable as a hash key.
 *
 * <p>The special value {@link #CONSTANT} stands for "no physical source": it is the source of a lineage edge
 * whose target column is fed by a literal ({@code TransformType.CONSTANT}). Using a sentinel instead of
 * {@code null} keeps the storage primary key NOT NULL and the edge equality total.</p>
 */
public final class ColumnIdentifier implements Comparable<ColumnIdentifier> {

    /** Sentinel catalog/db/table/column used for literal sources. */
    public static final String CONSTANT_NAME = "__constant__";

    /** Source used by constant lineage edges. */
    public static final ColumnIdentifier CONSTANT =
            new ColumnIdentifier(TableIdentifier.of(CONSTANT_NAME, CONSTANT_NAME, CONSTANT_NAME), CONSTANT_NAME);

    private static final Comparator<ColumnIdentifier> ORDER = Comparator
            .comparing(ColumnIdentifier::getTable)
            .thenComparing(ColumnIdentifier::getColumn);

    private final TableIdentifier table;
    private final String column;

    private ColumnIdentifier(TableIdentifier table, String column) {
        this.table = Objects.requireNonNull(table, "table");
        String c = Identifiers.normalizeColumn(column);
        if (c == null || c.isEmpty()) {
            throw new UnresolvedNameException("column name must not be empty");
        }
        this.column = c;
    }

    public static ColumnIdentifier of(TableIdentifier table, String column) {
        return new ColumnIdentifier(table, column);
    }

    public static ColumnIdentifier of(String catalog, String database, String table, String column) {
        return new ColumnIdentifier(TableIdentifier.of(catalog, database, table), column);
    }

    /**
     * Parses {@code catalog.db.table.column}.
     */
    public static ColumnIdentifier parse(String qualified) {
        java.util.List<String> parts = Identifiers.splitQualifiedName(qualified);
        if (parts.size() != 4) {
            throw new UnresolvedNameException("Expected catalog.db.table.column but got: " + qualified);
        }
        return of(parts.get(0), parts.get(1), parts.get(2), parts.get(3));
    }

    public TableIdentifier getTable() {
        return table;
    }

    public String getCatalog() {
        return table.getCatalog();
    }

    public String getDatabase() {
        return table.getDatabase();
    }

    public String getTableName() {
        return table.getTable();
    }

    public String getColumn() {
        return column;
    }

    public boolean isConstant() {
        return this.equals(CONSTANT);
    }

    public String toQualifiedString() {
        return table.toQualifiedString() + '.' + column;
    }

    @Override
    public int compareTo(ColumnIdentifier o) {
        return ORDER.compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnIdentifier)) {
            return false;
        }
        ColumnIdentifier that = (ColumnIdentifier) o;
        return table.equals(that.table) && column.equals(that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, column);
    }

    @Override
    public String toString() {
        return toQualifiedString();
    }
}
