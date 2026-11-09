package com.starrocks.lineage.metadata;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.TableIdentifier;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryMetadataProviderTest {

    @Test
    void registeredSchemaKeepsDeclarationOrder() {
        InMemoryMetadataProvider p = new InMemoryMetadataProvider()
                .register("default_catalog.ods.orders", "order_id", "User_Id", "amount");
        TableIdentifier t = TableIdentifier.parse("default_catalog.ods.orders");
        assertEquals(Arrays.asList("order_id", "user_id", "amount"), p.getColumnNames(t));
        assertTrue(p.hasColumn(t, "USER_ID"));
        assertTrue(p.hasColumn(t, "`amount`"));
        assertFalse(p.hasColumn(t, "missing"));
        TableSchema schema = p.getTableSchema(t).orElseThrow();
        assertEquals(1, schema.findColumn("user_id").orElseThrow().getOrdinal());
        assertEquals(ColumnIdentifier.parse("default_catalog.ods.orders.amount"),
                schema.columnIdentifier("AMOUNT").orElseThrow());
    }

    @Test
    void missingTableIsEmptyNotException() {
        InMemoryMetadataProvider p = new InMemoryMetadataProvider();
        TableIdentifier t = TableIdentifier.parse("hive.db.none");
        assertFalse(p.tableExists(t));
        assertEquals(Collections.emptyList(), p.getColumnNames(t));
        assertFalse(p.hasColumn(t, "a"));
    }

    @Test
    void schemaValidation() {
        TableIdentifier t = TableIdentifier.parse("default_catalog.db.t");
        assertThrows(IllegalArgumentException.class, () -> TableSchema.of(t, Arrays.asList("a", "A")));
        assertThrows(IllegalArgumentException.class, () -> TableSchema.of(t, Collections.emptyList()));
        TableSchema renumbered = new TableSchema(t, Arrays.asList(
                new ColumnMetadata("x", 7, "INT", false), new ColumnMetadata("y", 3, null, true)));
        assertEquals(0, renumbered.getColumns().get(0).getOrdinal());
        assertEquals(1, renumbered.getColumns().get(1).getOrdinal());
    }

    @Test
    void replaceAndRemove() {
        InMemoryMetadataProvider p = new InMemoryMetadataProvider().register("c.d.t", "a");
        p.register("c.d.t", "a", "b");
        TableIdentifier t = TableIdentifier.parse("c.d.t");
        assertEquals(2, p.getColumnNames(t).size());
        assertTrue(p.remove(t));
        assertEquals(0, p.size());
    }
}
