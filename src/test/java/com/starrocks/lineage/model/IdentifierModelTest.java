package com.starrocks.lineage.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdentifierModelTest {

    private final NamespaceContext ctx = NamespaceContext.of("", "sales");

    @Test
    void onePartNameIsCompletedWithSessionCatalogAndDatabase() {
        TableIdentifier t = TableIdentifier.resolve(Collections.singletonList("Orders"), ctx);
        assertEquals("default_catalog", t.getCatalog());
        assertEquals("sales", t.getDatabase());
        assertEquals("Orders", t.getTable(), "table names are case-sensitive in StarRocks");
    }

    @Test
    void twoAndThreePartNames() {
        assertEquals(TableIdentifier.of("default_catalog", "ods", "t"),
                TableIdentifier.resolve(Arrays.asList("ods", "t"), ctx));
        assertEquals(TableIdentifier.of("hive", "ods", "t"),
                TableIdentifier.resolve(Arrays.asList("HIVE", "ods", "t"), ctx), "catalog is case-insensitive");
    }

    @Test
    void sessionCatalogIsUsedForTwoPartNames() {
        TableIdentifier t = TableIdentifier.resolve(Arrays.asList("ods", "t"), NamespaceContext.of("Hive", "x"));
        assertEquals(TableIdentifier.of("hive", "ods", "t"), t);
    }

    @Test
    void unresolvableNamesFail() {
        assertThrows(UnresolvedNameException.class,
                () -> TableIdentifier.resolve(Collections.singletonList("t"), NamespaceContext.of(null, null)));
        assertThrows(UnresolvedNameException.class,
                () -> TableIdentifier.resolve(Arrays.asList("a", "b", "c", "d"), ctx));
        assertThrows(UnresolvedNameException.class, () -> TableIdentifier.resolve(Collections.emptyList(), ctx));
        assertThrows(UnresolvedNameException.class, () -> TableIdentifier.parse("db.t"));
    }

    @Test
    void backquotedNamesAreUnquotedAndMayContainDots() {
        assertEquals(Arrays.asList("hive", "my.db", "a`b"), Identifiers.splitQualifiedName("hive.`my.db`.`a``b`"));
        TableIdentifier t = TableIdentifier.parse("`hive`.`my.db`.`T1`");
        assertEquals("my.db", t.getDatabase());
        assertEquals("`hive`.`my.db`.`T1`", t.toQuotedSql());
        assertThrows(IllegalArgumentException.class, () -> Identifiers.splitQualifiedName("a..b"));
        assertThrows(IllegalArgumentException.class, () -> Identifiers.splitQualifiedName("`a.b"));
    }

    @Test
    void columnNamesAreCaseInsensitive() {
        ColumnIdentifier a = ColumnIdentifier.of("default_catalog", "db", "t", "UserId");
        ColumnIdentifier b = ColumnIdentifier.parse("DEFAULT_CATALOG.db.t.`userid`");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals("default_catalog.db.t.userid", a.toQualifiedString());
        assertFalse(a.isConstant());
        assertTrue(ColumnIdentifier.CONSTANT.isConstant());
    }

    @Test
    void namespaceFromAuditSplitsCatalogQualifiedDb() {
        assertEquals(NamespaceContext.of("hive", "ods"), NamespaceContext.fromAudit("", "hive.ods"));
        assertEquals(NamespaceContext.of("hive", "ods"), NamespaceContext.fromAudit("hive", "hive.ods"));
        assertEquals(NamespaceContext.of("default_catalog", "ods"), NamespaceContext.fromAudit(null, "ods"));
        assertEquals(NamespaceContext.of("default_catalog", "ods"),
                NamespaceContext.fromAudit("", "default_cluster:ods"));
        assertFalse(NamespaceContext.fromAudit("default_catalog", "").getDefaultDatabase().isPresent());
        assertEquals(NamespaceContext.of("iceberg", null), NamespaceContext.of("hive", "x").withCatalog("ICEBERG"));
        assertEquals(NamespaceContext.of("hive", "y"), NamespaceContext.of("hive", "x").withDatabase("y"));
    }
}
