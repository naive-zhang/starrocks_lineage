package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static com.starrocks.lineage.scope.ScopeTestSupport.columnItem;
import static com.starrocks.lineage.scope.ScopeTestSupport.control;
import static com.starrocks.lineage.scope.ScopeTestSupport.from;
import static com.starrocks.lineage.scope.ScopeTestSupport.newStack;
import static com.starrocks.lineage.scope.ScopeTestSupport.none;
import static com.starrocks.lineage.scope.ScopeTestSupport.parts;
import static com.starrocks.lineage.scope.ScopeTestSupport.resolve;
import static com.starrocks.lineage.scope.ScopeTestSupport.sales;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Architect iron rules #3 (star expansion), #4 (positional set operations) and #5 (UNNEST).
 */
class RelationAlgebraTest {

    // ------------------------------------------------------------------------------------------------ rule 3

    @Test
    void starFollowsFromOrderThenSchemaOrderAndKeepsBothUsingColumns() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            from(st, "users", "u");
            st.current().addUsingColumns(Collections.singletonList("user_id"));
            StarExpansion all = st.expandStar(none());
            assertTrue(all.isComplete());
            assertEquals(Arrays.asList(
                    sales("orders.order_id"), sales("orders.user_id"), sales("orders.amount"), sales("orders.dt"),
                    sales("orders.tags"), sales("users.user_id"), sales("users.name"), sales("users.city"),
                    sales("users.info")), sourcesOf(all));
            assertEquals(9, all.getReliablePrefix());

            StarExpansion users = st.expandStar(parts("u"));
            assertEquals(Arrays.asList(sales("users.user_id"), sales("users.name"), sales("users.city"),
                    sales("users.info")), sourcesOf(users));
            assertEquals(ErrorCode.RELATION_NOT_FOUND, assertThrows(SemanticAnalysisException.class,
                    () -> st.expandStar(parts("users"))).getErrorCode(), "aliased: only u.* works");
        }
    }

    @Test
    void starOverUnknownSchemaIsIncompleteWithReliablePrefix() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "users", "u");
            from(st, "ghost", "g");
            from(st, "orders", "o");
            StarExpansion all = st.expandStar(none());
            assertFalse(all.isComplete());
            assertEquals(4, all.getReliablePrefix(), "only the users.* positions precede the unknown relation");
            assertEquals(9, all.getColumns().size(), "known columns after the gap are still listed");
            assertEquals(Collections.singleton("default_catalog.sales.ghost"), all.getUnexpandedTables());

            // SELECT * as a derived relation: positional consumers must not trust positions >= 4
            List<RelationField> outs = new ArrayList<>();
            for (ResolvedColumn rc : all.getColumns()) {
                outs.add(rc.getField());
            }
            st.current().setSelectOutputs(outs, all.getReliablePrefix(), all.getUnexpandedTables());
            RelationDescriptor derived = st.current().toDerivedRelation(RelationKind.SUBQUERY, "d");
            assertFalse(derived.isComplete());
            assertEquals(4, derived.getReliablePrefix());
            assertEquals(ErrorCode.ORDINAL_OUT_OF_RANGE, assertThrows(SemanticAnalysisException.class,
                    () -> st.current().selectOutputByOrdinal(5)).getErrorCode());
        }
    }

    @Test
    void insertWithoutColumnListAlignsStarPositionallyWithTargetSchema() {
        // INSERT INTO sales.orders SELECT t1.* FROM sales.orders t1 -- target columns from the same metadata
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "t1");
            StarExpansion star = st.expandStar(parts("t1"));
            List<String> targetColumns = st.getMetadata().getColumnNames(sales("orders.x").getTable());
            assertEquals(targetColumns.size(), star.getColumns().size());
            for (int i = 0; i < targetColumns.size(); i++) {
                assertEquals(sales("orders." + targetColumns.get(i)),
                        star.getColumns().get(i).lineage().getDirect().keySet().iterator().next(),
                        "position " + i);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ rule 4

    /** SELECT order_id, amount FROM orders UNION ALL SELECT user_id, city FROM users UNION ALL SELECT 1, price ... */
    @Test
    void setOperationMergesBranchesByPositionNotByName() {
        RelationDescriptor b1 = branch(sales("orders.order_id"), sales("orders.amount"));
        RelationDescriptor b2 = branch(sales("users.user_id"), sales("users.city"))
                .withControlSources(Collections.singleton(sales("users.name")));
        RelationDescriptor b3 = RelationDescriptor.derived(RelationKind.SUBQUERY, null, Arrays.asList(
                new RelationField("one", 0, FieldLineage.constant(), "1"),
                new RelationField("price", 1, FieldLineage.physical(sales("items.price")), "price")),
                Collections.emptySet());
        RelationDescriptor u = RelationDescriptor.setOperation(Arrays.asList(b1, b2, b3));
        assertEquals(Arrays.asList("order_id", "amount"), Arrays.asList(u.field(0).getName(), u.field(1).getName()),
                "names come from the first branch");
        assertEquals(new LinkedHashSet<>(Arrays.asList(sales("orders.order_id"), sales("users.user_id"))),
                u.field(0).getLineage().getDirect().keySet());
        assertEquals(new LinkedHashSet<>(Arrays.asList(sales("orders.amount"), sales("users.city"),
                sales("items.price"))), u.field(1).getLineage().getDirect().keySet());
        assertEquals(Collections.singleton(sales("users.name")), u.getControlSources());
        assertTrue(u.isComplete());
        assertEquals(RelationKind.SET_OPERATION, u.getKind());
    }

    @Test
    void setOperationArityMismatchAndIncompleteBranches() {
        RelationDescriptor two = branch(sales("orders.order_id"), sales("orders.amount"));
        RelationDescriptor three = branch(sales("users.user_id"), sales("users.city"), sales("users.name"));
        assertEquals(ErrorCode.SET_OPERATION_ARITY_MISMATCH, assertThrows(SemanticAnalysisException.class,
                () -> RelationDescriptor.setOperation(Arrays.asList(two, three))).getErrorCode());

        // branch 2 = SELECT u.user_id, g.* FROM users u, ghost g  -> reliable prefix 1
        RelationDescriptor incomplete = RelationDescriptor.derived(RelationKind.SUBQUERY, null,
                Collections.singletonList(new RelationField("user_id", 0,
                        FieldLineage.physical(sales("users.user_id")), null)),
                Collections.emptySet(), 1, Collections.singleton("default_catalog.sales.ghost"));
        RelationDescriptor merged = RelationDescriptor.setOperation(Arrays.asList(two, incomplete));
        assertEquals(2, merged.size(), "arity from the complete branch");
        assertEquals(2, merged.getReliablePrefix(), "positions are known thanks to the complete branch");
        assertFalse(merged.isComplete(), "but lineage of position 1 is missing the ghost column");
        assertEquals(new LinkedHashSet<>(Arrays.asList(sales("orders.order_id"), sales("users.user_id"))),
                merged.field(0).getLineage().getDirect().keySet());
        assertEquals(Collections.singleton(sales("orders.amount")), merged.field(1).getLineage().getDirect().keySet());

        RelationDescriptor onlyIncomplete = RelationDescriptor.setOperation(Arrays.asList(incomplete, incomplete));
        assertEquals(1, onlyIncomplete.size());
        assertFalse(onlyIncomplete.isComplete());

        RelationDescriptor tooWide = RelationDescriptor.derived(RelationKind.SUBQUERY, null, Arrays.asList(
                new RelationField("a", 0, FieldLineage.constant(), null),
                new RelationField("b", 1, FieldLineage.constant(), null),
                new RelationField("c", 2, FieldLineage.constant(), null)),
                Collections.emptySet(), 3, Collections.singleton("x"));
        assertEquals(ErrorCode.SET_OPERATION_ARITY_MISMATCH, assertThrows(SemanticAnalysisException.class,
                () -> RelationDescriptor.setOperation(Arrays.asList(two, tooWide))).getErrorCode());
        assertThrows(IllegalArgumentException.class,
                () -> RelationDescriptor.setOperation(Collections.singletonList(two)));
    }

    @Test
    void orderByOfSetOperationResolvesAgainstItsOutput() {
        ScopeStack st = newStack();
        RelationDescriptor u = RelationDescriptor.setOperation(Arrays.asList(
                branch(sales("orders.order_id")), branch(sales("users.user_id"))));
        try (ScopeStack.Frame out = st.pushOutput(u)) {
            ResolvedColumn rc = resolve(st, "order_id");
            assertEquals(2, rc.lineage().getDirect().size());
            assertEquals(ErrorCode.COLUMN_NOT_FOUND, assertThrows(SemanticAnalysisException.class,
                    () -> resolve(st, "user_id")).getErrorCode(), "output is named after the first branch");
        }
    }

    // ------------------------------------------------------------------------------------------------ rule 5

    /** FROM items, UNNEST(arr1, arr2) AS t(c1, c2). */
    @Test
    void unnestVirtualColumnsMapBackToPhysicalArrays() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "items", null);
            FieldLineage a1 = resolve(st, "arr1").lineage();
            FieldLineage a2 = resolve(st, "items.arr2").lineage();
            st.current().addRelation(RelationDescriptor.tableFunction("UNNEST", "t", Arrays.asList("c1", "c2"),
                    Arrays.asList(a1, a2)));
            ResolvedColumn c1 = resolve(st, "c1");
            assertEquals(Collections.singletonMap(sales("items.arr1"), TransformType.EXPRESSION),
                    c1.lineage().getDirect());
            assertEquals(Collections.singletonMap(sales("items.arr2"), TransformType.EXPRESSION),
                    resolve(st, "t.c2").lineage().getDirect());
            assertEquals(Arrays.asList(sales("items.order_id"), sales("items.sku"), sales("items.price"),
                    sales("items.arr1"), sales("items.arr2"), sales("items.x"), sales("items.arr1"),
                    sales("items.arr2")), sourcesOf(st.expandStar(none())));
        }
    }

    @Test
    void unnestDefaultNamesAndAliasFollowStarRocks() {
        RelationDescriptor u = RelationDescriptor.tableFunction("unnest", null, null, Arrays.asList(
                FieldLineage.physical(sales("items.arr1")), FieldLineage.physical(sales("items.arr2"))));
        assertEquals("table_function_unnest", u.getAlias().get());
        assertEquals(Arrays.asList("unnest", "unnest"), Arrays.asList(u.field(0).getName(), u.field(1).getName()));
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            st.current().addRelation(u);
            assertEquals(ErrorCode.AMBIGUOUS_COLUMN, assertThrows(SemanticAnalysisException.class,
                    () -> resolve(st, "unnest")).getErrorCode(), "two 'unnest' columns from different arrays");
        }
        RelationDescriptor single = RelationDescriptor.tableFunction("unnest", null, null,
                Collections.singletonList(FieldLineage.physical(sales("orders.tags"))));
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            st.current().addRelation(single);
            assertEquals(sales("orders.tags"), resolve(st, "table_function_unnest.unnest").lineage().getDirect()
                    .keySet().iterator().next());
        }
        assertEquals(ErrorCode.COLUMN_ALIAS_MISMATCH, assertThrows(SemanticAnalysisException.class,
                () -> RelationDescriptor.tableFunction("unnest", "t", Arrays.asList("a", "b"),
                        Collections.singletonList(FieldLineage.constant()))).getErrorCode());
    }

    @Test
    void otherTableFunctionsAreOpenUnlessAliased() {
        FieldLineage payload = FieldLineage.physical(ColumnIdentifier.parse("hive.ods.events.payload"));
        RelationDescriptor open = RelationDescriptor.tableFunction("json_each", "j", null,
                Collections.singletonList(payload));
        assertFalse(open.isSchemaKnown());
        RelationField key = open.onDemandField("key").get();
        assertEquals(Collections.singletonMap(ColumnIdentifier.parse("hive.ods.events.payload"),
                TransformType.EXPRESSION), key.getLineage().getDirect());
        assertEquals(Collections.singleton("j"), open.getUnexpandedStarSources());

        RelationDescriptor aliased = RelationDescriptor.tableFunction("json_each", "j", Arrays.asList("k", "v"),
                Collections.singletonList(payload));
        assertTrue(aliased.isComplete());
        assertEquals(2, aliased.size());
        assertFalse(aliased.onDemandField("zzz").isPresent());
    }

    @Test
    void valuesDefaultsAndAliases() {
        RelationDescriptor v = RelationDescriptor.values("v", null,
                Arrays.asList(FieldLineage.constant(), FieldLineage.constant()));
        assertEquals(Arrays.asList("column_0", "column_1"), Arrays.asList(v.field(0).getName(),
                v.field(1).getName()));
        assertTrue(v.field(0).getLineage().isConstant());
        RelationDescriptor named = RelationDescriptor.values("v", Arrays.asList("a", "b"),
                Arrays.asList(FieldLineage.constant(), FieldLineage.constant()));
        assertEquals("b", named.field(1).getName());
        assertEquals(ErrorCode.COLUMN_ALIAS_MISMATCH, assertThrows(SemanticAnalysisException.class,
                () -> RelationDescriptor.values("v", Collections.singletonList("a"),
                        Arrays.asList(FieldLineage.constant(), FieldLineage.constant()))).getErrorCode());
    }

    @Test
    void columnAliasesOnIncompleteRelation() {
        RelationDescriptor incomplete = RelationDescriptor.derived(RelationKind.SUBQUERY, "s",
                Collections.singletonList(new RelationField("user_id", 0,
                        FieldLineage.physical(sales("users.user_id")), null)),
                Collections.emptySet(), 1, Collections.singleton("default_catalog.sales.ghost"));
        RelationDescriptor renamed = incomplete.withColumnAliases(Arrays.asList("id", "g1", "g2"));
        assertEquals(3, renamed.size());
        assertEquals(3, renamed.getReliablePrefix());
        assertTrue(renamed.field(2).getLineage().isEmpty(), "source of an unexpanded position is unknown");
        assertFalse(renamed.isComplete());
        assertEquals(ErrorCode.COLUMN_ALIAS_MISMATCH, assertThrows(SemanticAnalysisException.class,
                () -> RelationDescriptor.table(sales("ghost.x").getTable(), "g", java.util.Optional.empty())
                        .withColumnAliases(Collections.singletonList("a"))).getErrorCode());
        assertEquals(ErrorCode.COLUMN_ALIAS_MISMATCH, assertThrows(SemanticAnalysisException.class,
                () -> renamed.withColumnAliases(Collections.emptyList()).withColumnAliases(
                        Collections.singletonList("a")).size()).getErrorCode());
    }

    @Test
    void derivedRelationCarriesBlockControlIncludingHiddenRelations() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            RelationDescriptor u = from(st, "users", "u");
            control(st, "u.city");
            st.current().hideRelations(Collections.singletonList(u));
            control(st, "o.dt");
            st.current().setSelectOutputs(Collections.singletonList(columnItem(st, "o.amount")));
            RelationDescriptor d = st.current().toDerivedRelation(RelationKind.SUBQUERY, "d");
            assertEquals(new LinkedHashSet<>(Arrays.asList(sales("users.city"), sales("orders.dt"))),
                    d.getControlSources());
            assertTrue(d.isComplete());
        }
    }

    /** Review R2-01: a relation whose arity is fixed by column aliases does not break positions after it. */
    @Test
    void positionallyCompleteButLineageIncompleteRelationKeepsLaterPositionsReliable() {
        RelationDescriptor aliased = RelationDescriptor.derived(RelationKind.SUBQUERY, "s",
                Collections.singletonList(new RelationField("user_id", 0,
                        FieldLineage.physical(sales("users.user_id")), null)),
                Collections.emptySet(), 1, Collections.singleton("default_catalog.sales.ghost"))
                .withColumnAliases(Arrays.asList("id", "g1"));
        assertTrue(aliased.isPositionallyComplete());
        assertFalse(aliased.isComplete());
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            st.current().addRelation(aliased);
            from(st, "orders", "o");
            StarExpansion all = st.expandStar(none());
            assertEquals(7, all.getColumns().size());
            assertEquals(7, all.getReliablePrefix(), "every position is known");
            assertFalse(all.isComplete(), "lineage of s.g1 is still unknown");
        }
        RelationDescriptor three = branch(sales("orders.order_id"), sales("orders.amount"), sales("orders.dt"));
        assertEquals(ErrorCode.SET_OPERATION_ARITY_MISMATCH, assertThrows(SemanticAnalysisException.class,
                () -> RelationDescriptor.setOperation(Arrays.asList(three, aliased))).getErrorCode(),
                "an aliased branch takes part in the arity check");
        RelationDescriptor ok = RelationDescriptor.setOperation(Arrays.asList(
                branch(sales("orders.order_id"), sales("orders.amount")), aliased));
        assertEquals(2, ok.getReliablePrefix());
        assertEquals(Collections.singleton("default_catalog.sales.ghost"), ok.getUnexpandedStarSources());
    }

    private static RelationDescriptor branch(ColumnIdentifier... columns) {
        List<RelationField> fields = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            fields.add(new RelationField(columns[i].getColumn(), i, FieldLineage.physical(columns[i]),
                    columns[i].getColumn()));
        }
        return RelationDescriptor.derived(RelationKind.SUBQUERY, null, fields, Collections.emptySet());
    }

    private static List<ColumnIdentifier> sourcesOf(StarExpansion e) {
        List<ColumnIdentifier> result = new ArrayList<>();
        for (ResolvedColumn rc : e.getColumns()) {
            result.addAll(rc.lineage().getDirect().keySet());
        }
        return result;
    }
}
