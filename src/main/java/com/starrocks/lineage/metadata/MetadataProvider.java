package com.starrocks.lineage.metadata;

import com.starrocks.lineage.model.TableIdentifier;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Source of table schemas for column resolution.
 *
 * <p><b>Contract</b></p>
 * <ul>
 *   <li>Implementations are thread-safe: the pipeline analyses statements on many threads concurrently.</li>
 *   <li>The table identifier passed in is always fully qualified and normalised ({@link TableIdentifier}).</li>
 *   <li>A table or view that does not exist yields {@link Optional#empty()} - never an exception and never
 *       {@code null}.</li>
 *   <li>Infrastructure failures are raised as {@link MetadataAccessException} so that the caller can decide
 *       between retry and degradation; they are never cached.</li>
 *   <li>The returned schema lists columns in table declaration order (needed for {@code SELECT *} and
 *       {@code INSERT INTO t SELECT ...} without a column list).</li>
 * </ul>
 */
public interface MetadataProvider {

    /**
     * @param table fully qualified table or view
     * @return the schema, or empty when the table does not exist
     * @throws MetadataAccessException on infrastructure failure
     */
    Optional<TableSchema> getTableSchema(TableIdentifier table);

    /** @return whether the table or view exists */
    default boolean tableExists(TableIdentifier table) {
        return getTableSchema(table).isPresent();
    }

    /** @return ordered column names, empty list when the table does not exist */
    default List<String> getColumnNames(TableIdentifier table) {
        return getTableSchema(table).map(TableSchema::getColumnNames).orElse(Collections.emptyList());
    }

    /**
     * @return whether the table exists and has the column (case-insensitive)
     */
    default boolean hasColumn(TableIdentifier table, String column) {
        return getTableSchema(table).map(s -> s.hasColumn(column)).orElse(false);
    }

    /**
     * Drops any cached knowledge about the table (e.g. after a DDL on it was seen in the audit log).
     * The default implementation has no cache and does nothing.
     */
    default void invalidate(TableIdentifier table) {
    }

    /** Drops every cached entry. The default implementation does nothing. */
    default void invalidateAll() {
    }
}
