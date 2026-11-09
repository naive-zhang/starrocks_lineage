package com.starrocks.lineage.model;

import java.util.Objects;
import java.util.Optional;

/**
 * Session namespace of an audited statement: the catalog and database that were current when the statement
 * ran. Used to complete partially qualified names ({@code t}, {@code db.t}) to the three-part form
 * {@code catalog.db.table}.
 */
public final class NamespaceContext {

    /** Prefix written by StarRocks versions that still had cluster namespaces (e.g. {@code default_cluster:db1}). */
    static final String LEGACY_CLUSTER_PREFIX = "default_cluster:";

    private final String defaultCatalog;
    private final String defaultDatabase;

    private NamespaceContext(String defaultCatalog, String defaultDatabase) {
        this.defaultCatalog = Identifiers.normalizeCatalog(defaultCatalog);
        String db = Identifiers.normalizeDatabase(defaultDatabase);
        this.defaultDatabase = db == null || db.isEmpty() ? null : db;
    }

    /**
     * @param catalog current catalog, blank means {@value Identifiers#DEFAULT_CATALOG}
     * @param database current database, blank means "no database selected"
     */
    public static NamespaceContext of(String catalog, String database) {
        return new NamespaceContext(catalog, database);
    }

    /**
     * Builds the context from the {@code catalog} and {@code db} columns of the audit table. Some StarRocks
     * versions/clients record the database as {@code catalog.db} while leaving {@code catalog} empty; since
     * StarRocks database names cannot contain a dot, such a value is split safely. Audit rows written by clusters
     * upgraded from versions that still had cluster namespaces carry a {@code default_cluster:} prefix, which is
     * removed.
     *
     * @param auditCatalog value of the {@code catalog} column
     * @param auditDb value of the {@code db} column
     */
    public static NamespaceContext fromAudit(String auditCatalog, String auditDb) {
        String catalog = auditCatalog == null ? "" : auditCatalog.trim();
        String db = auditDb == null ? "" : auditDb.trim();
        if (db.startsWith(LEGACY_CLUSTER_PREFIX)) {
            db = db.substring(LEGACY_CLUSTER_PREFIX.length());
        }
        int dot = db.indexOf('.');
        if (dot > 0 && dot < db.length() - 1 && !db.startsWith("`")) {
            String prefix = db.substring(0, dot);
            String rest = db.substring(dot + 1);
            if (catalog.isEmpty() || catalog.equalsIgnoreCase(prefix)) {
                catalog = prefix;
                db = rest;
            }
        }
        return new NamespaceContext(catalog, db);
    }

    public String getDefaultCatalog() {
        return defaultCatalog;
    }

    public Optional<String> getDefaultDatabase() {
        return Optional.ofNullable(defaultDatabase);
    }

    /**
     * @return a copy whose current database is replaced (used for {@code USE} inside multi-statement text)
     */
    public NamespaceContext withDatabase(String database) {
        return new NamespaceContext(defaultCatalog, database);
    }

    /**
     * @return a copy whose current catalog is replaced; the database is cleared like StarRocks
     *         {@code SET CATALOG} does
     */
    public NamespaceContext withCatalog(String catalog) {
        return new NamespaceContext(catalog, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NamespaceContext)) {
            return false;
        }
        NamespaceContext that = (NamespaceContext) o;
        return defaultCatalog.equals(that.defaultCatalog) && Objects.equals(defaultDatabase, that.defaultDatabase);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultCatalog, defaultDatabase);
    }

    @Override
    public String toString() {
        return defaultCatalog + "." + (defaultDatabase == null ? "<none>" : defaultDatabase);
    }
}
