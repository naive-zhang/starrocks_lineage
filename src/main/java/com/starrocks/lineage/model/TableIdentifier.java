package com.starrocks.lineage.model;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Fully qualified, normalised table name {@code catalog.db.table}. Immutable, usable as a hash key.
 */
public final class TableIdentifier implements Comparable<TableIdentifier> {

    private static final Comparator<TableIdentifier> ORDER = Comparator
            .comparing(TableIdentifier::getCatalog)
            .thenComparing(TableIdentifier::getDatabase)
            .thenComparing(TableIdentifier::getTable);

    private final String catalog;
    private final String database;
    private final String table;

    private TableIdentifier(String catalog, String database, String table) {
        this.catalog = Identifiers.normalizeCatalog(catalog);
        this.database = requireName(Identifiers.normalizeDatabase(database), "database");
        this.table = requireName(Identifiers.normalizeTable(table), "table");
    }

    /**
     * @param catalog catalog, blank means {@value Identifiers#DEFAULT_CATALOG}
     * @param database database, must not be blank
     * @param table table, must not be blank
     */
    public static TableIdentifier of(String catalog, String database, String table) {
        return new TableIdentifier(catalog, database, table);
    }

    /**
     * Completes a 1, 2 or 3 part name against the session namespace:
     * <ul>
     *   <li>{@code t} becomes {@code ctx.catalog . ctx.db . t} (fails if no current database)</li>
     *   <li>{@code db.t} becomes {@code ctx.catalog . db . t}</li>
     *   <li>{@code c.db.t} is kept</li>
     * </ul>
     *
     * @param parts unquoted (or still backquoted) name parts
     * @param ctx session namespace
     * @throws UnresolvedNameException when the name cannot be completed
     */
    public static TableIdentifier resolve(List<String> parts, NamespaceContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        if (parts == null || parts.isEmpty()) {
            throw new UnresolvedNameException("empty table name");
        }
        switch (parts.size()) {
            case 1:
                String db = ctx.getDefaultDatabase().orElseThrow(() -> new UnresolvedNameException(
                        "No database selected while resolving table '" + parts.get(0) + "'"));
                return new TableIdentifier(ctx.getDefaultCatalog(), db, parts.get(0));
            case 2:
                return new TableIdentifier(ctx.getDefaultCatalog(), parts.get(0), parts.get(1));
            case 3:
                return new TableIdentifier(parts.get(0), parts.get(1), parts.get(2));
            default:
                throw new UnresolvedNameException("Too many name parts for a table: " + String.join(".", parts));
        }
    }

    /**
     * Parses a dotted text name and completes it, see {@link #resolve(List, NamespaceContext)}.
     */
    public static TableIdentifier parse(String qualifiedName, NamespaceContext ctx) {
        return resolve(Identifiers.splitQualifiedName(qualifiedName), ctx);
    }

    /**
     * Parses a fully qualified {@code catalog.db.table} text.
     */
    public static TableIdentifier parse(String qualifiedName) {
        List<String> parts = Identifiers.splitQualifiedName(qualifiedName);
        if (parts.size() != 3) {
            throw new UnresolvedNameException("Expected catalog.db.table but got: " + qualifiedName);
        }
        return new TableIdentifier(parts.get(0), parts.get(1), parts.get(2));
    }

    private static String requireName(String value, String what) {
        if (value == null || value.isEmpty()) {
            throw new UnresolvedNameException(what + " name must not be empty");
        }
        return value;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    /** @return {@code catalog.db.table} without quoting (for logs / keys) */
    public String toQualifiedString() {
        return catalog + '.' + database + '.' + table;
    }

    /** @return {@code `catalog`.`db`.`table`} safe for SQL embedding */
    public String toQuotedSql() {
        return Identifiers.quote(catalog) + '.' + Identifiers.quote(database) + '.' + Identifiers.quote(table);
    }

    @Override
    public int compareTo(TableIdentifier o) {
        return ORDER.compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableIdentifier)) {
            return false;
        }
        TableIdentifier that = (TableIdentifier) o;
        return catalog.equals(that.catalog) && database.equals(that.database) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, database, table);
    }

    @Override
    public String toString() {
        return toQualifiedString();
    }
}
