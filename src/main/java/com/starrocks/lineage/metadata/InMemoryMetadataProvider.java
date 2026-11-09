package com.starrocks.lineage.metadata;

import com.starrocks.lineage.model.TableIdentifier;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe in-memory {@link MetadataProvider}, used by unit tests and as an overlay for tables created in the
 * same audit batch (CTAS / CREATE VIEW targets that the analyzer learns before they are visible through JDBC).
 */
public final class InMemoryMetadataProvider implements MetadataProvider {

    private final ConcurrentMap<TableIdentifier, TableSchema> schemas = new ConcurrentHashMap<>();

    /** Registers or replaces a schema. */
    public InMemoryMetadataProvider register(TableSchema schema) {
        Objects.requireNonNull(schema, "schema");
        schemas.put(schema.getTable(), schema);
        return this;
    }

    /** Registers or replaces a table with the given ordered column names. */
    public InMemoryMetadataProvider register(TableIdentifier table, List<String> columns) {
        return register(TableSchema.of(table, columns));
    }

    /**
     * Registers or replaces a table given as {@code catalog.db.table}.
     *
     * @param qualifiedName fully qualified name, e.g. {@code default_catalog.sales.orders}
     * @param columns       ordered column names
     */
    public InMemoryMetadataProvider register(String qualifiedName, String... columns) {
        return register(TableIdentifier.parse(qualifiedName), Arrays.asList(columns));
    }

    /** Removes a table; returns whether it was present. */
    public boolean remove(TableIdentifier table) {
        return schemas.remove(table) != null;
    }

    public int size() {
        return schemas.size();
    }

    @Override
    public Optional<TableSchema> getTableSchema(TableIdentifier table) {
        Objects.requireNonNull(table, "table");
        return Optional.ofNullable(schemas.get(table));
    }
}
