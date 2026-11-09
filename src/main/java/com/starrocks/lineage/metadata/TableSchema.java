package com.starrocks.lineage.metadata;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.Identifiers;
import com.starrocks.lineage.model.TableIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable, ordered schema of a table or view.
 */
public final class TableSchema {

    private final TableIdentifier table;
    private final List<ColumnMetadata> columns;
    private final Map<String, ColumnMetadata> byName;

    /**
     * @param table   table identity
     * @param columns columns; they are re-numbered 0..n-1 in list order so callers cannot create gaps
     * @throws IllegalArgumentException on an empty column list or duplicate (case-insensitive) names
     */
    public TableSchema(TableIdentifier table, List<ColumnMetadata> columns) {
        this.table = Objects.requireNonNull(table, "table");
        Objects.requireNonNull(columns, "columns");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("table " + table + " must have at least one column");
        }
        List<ColumnMetadata> ordered = new ArrayList<>(columns.size());
        Map<String, ColumnMetadata> index = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata c = columns.get(i);
            ColumnMetadata renumbered = c.getOrdinal() == i ? c
                    : new ColumnMetadata(c.getName(), i, c.getDataType(), c.isNullable());
            if (index.put(renumbered.getName(), renumbered) != null) {
                throw new IllegalArgumentException("duplicate column '" + renumbered.getName() + "' in " + table);
            }
            ordered.add(renumbered);
        }
        this.columns = Collections.unmodifiableList(ordered);
        this.byName = Collections.unmodifiableMap(index);
    }

    /** Convenience factory: column names only, types unknown, all nullable. */
    public static TableSchema of(TableIdentifier table, List<String> columnNames) {
        List<ColumnMetadata> cols = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            cols.add(new ColumnMetadata(columnNames.get(i), i, null, true));
        }
        return new TableSchema(table, cols);
    }

    public TableIdentifier getTable() {
        return table;
    }

    /** @return columns in declaration order */
    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    /** @return normalised column names in declaration order */
    public List<String> getColumnNames() {
        List<String> names = new ArrayList<>(columns.size());
        for (ColumnMetadata c : columns) {
            names.add(c.getName());
        }
        return Collections.unmodifiableList(names);
    }

    /** Case-insensitive, backquote-tolerant lookup. */
    public Optional<ColumnMetadata> findColumn(String name) {
        String n = Identifiers.normalizeColumn(name);
        return n == null ? Optional.empty() : Optional.ofNullable(byName.get(n));
    }

    public boolean hasColumn(String name) {
        return findColumn(name).isPresent();
    }

    /** @return the physical identifier of one column of this table */
    public Optional<ColumnIdentifier> columnIdentifier(String name) {
        return findColumn(name).map(c -> ColumnIdentifier.of(table, c.getName()));
    }

    public int size() {
        return columns.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableSchema)) {
            return false;
        }
        TableSchema that = (TableSchema) o;
        return table.equals(that.table) && columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, columns);
    }

    @Override
    public String toString() {
        return table + columns.toString();
    }
}
