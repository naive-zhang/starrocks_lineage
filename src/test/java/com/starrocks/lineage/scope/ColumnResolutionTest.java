package com.starrocks.lineage.scope;

import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.starrocks.lineage.scope.ScopeTestSupport.c;
import static com.starrocks.lineage.scope.ScopeTestSupport.from;
import static com.starrocks.lineage.scope.ScopeTestSupport.newStack;
import static com.starrocks.lineage.scope.ScopeTestSupport.parts;
import static com.starrocks.lineage.scope.ScopeTestSupport.resolve;
import static com.starrocks.lineage.scope.ScopeTestSupport.sales;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnResolutionTest {

    private static SemanticAnalysisException.ErrorCode codeOf(Runnable r) {
        return assertThrows(SemanticAnalysisException.class, r::run).getErrorCode();
    }

    @Test
    void unqualifiedAndEveryQualifiedFormOfAnUnaliasedTable() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", null);
            assertEquals(sales("orders.amount"), single(resolve(st, "amount")));
            assertEquals(sales("orders.amount"), single(resolve(st, "orders.amount")));
            assertEquals(sales("orders.amount"), single(resolve(st, "sales.orders.amount")));
            assertEquals(sales("orders.amount"), single(resolve(st, "default_catalog.sales.orders.AMOUNT")));
            assertEquals(sales("orders.amount"), single(resolve(st, "`ORDERS`.`amount`")));
            assertEquals(ErrorCode.COLUMN_NOT_FOUND, codeOf(() -> resolve(st, "ods.orders.amount")));
            assertEquals(ErrorCode.COLUMN_NOT_FOUND, codeOf(() -> resolve(st, "nope")));
        }
        st.assertEmpty();
    }

    @Test
    void aliasedRelationIsOnlyReachableThroughItsAlias() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            assertEquals(sales("orders.amount"), single(resolve(st, "o.amount")));
            assertEquals(sales("orders.amount"), single(resolve(st, "O.amount")), "aliases are case-insensitive");
            assertEquals(ErrorCode.COLUMN_NOT_FOUND, codeOf(() -> resolve(st, "orders.amount")));
        }
    }

    @Test
    void sameTableNameInTwoDatabasesIsDisambiguatedByDatabase() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "sales.orders", null);
            from(st, "ods.orders", null);
            assertEquals(c("default_catalog.ods.orders.raw_amount"), single(resolve(st, "raw_amount")));
            assertEquals(sales("orders.order_id"), single(resolve(st, "sales.orders.order_id")));
            assertEquals(c("default_catalog.ods.orders.order_id"), single(resolve(st, "ods.orders.order_id")));
            assertEquals(ErrorCode.AMBIGUOUS_COLUMN, codeOf(() -> resolve(st, "orders.order_id")));
            assertEquals(ErrorCode.AMBIGUOUS_COLUMN, codeOf(() -> resolve(st, "order_id")));
        }
    }

    @Test
    void ambiguousJoinColumnUnlessUsing() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            from(st, "users", "u");
            assertEquals(ErrorCode.AMBIGUOUS_COLUMN, codeOf(() -> resolve(st, "user_id")));
            assertEquals(sales("users.user_id"), single(resolve(st, "u.user_id")));
        }
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            from(st, "users", "u");
            st.current().addUsingColumns(Collections.singletonList("USER_ID"));
            assertEquals(sales("orders.user_id"), single(resolve(st, "user_id")), "USING: left-most relation wins");
        }
        st.assertEmpty();
    }

    @Test
    void duplicateAliasIsRejected() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "t");
            assertEquals(ErrorCode.DUPLICATE_RELATION, codeOf(() -> from(st, "users", "T")));
            assertEquals(ErrorCode.DUPLICATE_RELATION, codeOf(() -> from(st, "orders", "t")));
            from(st, "orders", "t2");
        }
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", null);
            assertEquals(ErrorCode.DUPLICATE_RELATION, codeOf(() -> from(st, "sales.orders", null)));
        }
    }

    @Test
    void structSubfieldPathsAreSplitFromTheColumn() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "users", "u");
            ResolvedColumn qualified = resolve(st, "u.info.city");
            assertEquals(sales("users.info"), single(qualified));
            assertEquals(Collections.singletonList("city"), qualified.getSubfieldPath());
            assertEquals(TransformType.EXPRESSION, qualified.lineage().getDirect().get(sales("users.info")));

            ResolvedColumn unqualified = resolve(st, "info.zip.code");
            assertEquals(sales("users.info"), single(unqualified));
            assertEquals(Arrays.asList("zip", "code"), unqualified.getSubfieldPath());

            ResolvedColumn plain = resolve(st, "u.city");
            assertTrue(plain.getSubfieldPath().isEmpty());
            assertEquals(TransformType.IDENTITY, plain.lineage().getDirect().get(sales("users.city")));
        }
    }

    @Test
    void unknownSchemaRelationsResolveOnDemand() {
        InMemoryMetadataProvider meta = ScopeTestSupport.catalog();
        ScopeStack st = new ScopeStack(NamespaceContext.of("default_catalog", "sales"), meta);
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            RelationDescriptor ghost = from(st, "ghost", "g");
            from(st, "users", "u");
            assertFalse(ghost.isSchemaKnown());
            assertEquals(sales("users.city"), single(resolve(st, "city")), "a known column wins");
            ResolvedColumn x = resolve(st, "whatever");
            assertTrue(x.isOnDemand());
            assertEquals(sales("ghost.whatever"), single(x));
            assertEquals(x.getField(), resolve(st, "g.WHATEVER").getField(), "on-demand fields are memoised");
            assertEquals(sales("ghost.zzz"), single(resolve(st, "g.zzz")));
        }
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "ghost", "g1");
            from(st, "ghost2", "g2");
            assertEquals(ErrorCode.AMBIGUOUS_UNKNOWN_SCHEMA, codeOf(() -> resolve(st, "col")));
            assertEquals(sales("ghost2.col"), single(resolve(st, "g2.col")));
        }
        assertEquals(Arrays.asList(sales("ghost.x").getTable(), sales("users.x").getTable(),
                sales("ghost2.x").getTable()), Arrays.asList(st.getReferencedTables().toArray()));
    }

    @Test
    void selectAliasPolicies() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            // SELECT o.amount * 2 AS user_id, o.dt AS d
            RelationField doubled = ScopeTestSupport.item("user_id", t -> ScopeTestSupport.inContext(t,
                    TransformType.EXPRESSION, () -> t.addReference(resolve(st, "o.amount"))));
            RelationField d = ScopeTestSupport.columnItem(st, "o.dt").withName("d");
            st.current().setSelectOutputs(Arrays.asList(doubled, d));

            ResolvedColumn orderBy = st.resolveColumn(parts("user_id"), AliasPolicy.PREFER_ALIAS);
            assertEquals(ResolvedColumn.Kind.SELECT_ALIAS, orderBy.getKind());
            assertEquals(sales("orders.amount"), single(orderBy));

            ResolvedColumn groupBy = st.resolveColumn(parts("user_id"), AliasPolicy.PREFER_INPUT);
            assertEquals(ResolvedColumn.Kind.RELATION_FIELD, groupBy.getKind());
            assertEquals(sales("orders.user_id"), single(groupBy));

            assertEquals(sales("orders.dt"), single(st.resolveColumn(parts("d"), AliasPolicy.PREFER_INPUT)));
            assertEquals(ErrorCode.COLUMN_NOT_FOUND, codeOf(() -> st.resolveColumn(parts("d"), AliasPolicy.NONE)));
            assertEquals(ErrorCode.COLUMN_NOT_FOUND,
                    codeOf(() -> st.resolveColumn(parts("o.d"), AliasPolicy.PREFER_ALIAS)),
                    "aliases are never qualified");

            assertEquals("d", st.current().selectOutputByOrdinal(2).getName());
            assertEquals(ErrorCode.ORDINAL_OUT_OF_RANGE, codeOf(() -> st.current().selectOutputByOrdinal(3)));
            assertEquals(ErrorCode.ORDINAL_OUT_OF_RANGE, codeOf(() -> st.current().selectOutputByOrdinal(0)));

            try (ScopeStack.Frame inner = st.pushQueryBlock()) {
                from(st, "users", null);
                assertEquals(ErrorCode.COLUMN_NOT_FOUND,
                        codeOf(() -> st.resolveColumn(parts("d"), AliasPolicy.PREFER_ALIAS)),
                        "aliases of an outer block are invisible to sub-queries");
            }
        }
    }

    @Test
    void lambdaParametersShadowColumnsOnlyInsideTheLambda() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "items", "i");
            // array_map((x, y) -> x + y, arr1, arr2)   -- items also has a physical column named x
            FieldLineage arr1 = resolve(st, "arr1").lineage().elevate(TransformType.EXPRESSION);
            FieldLineage arr2 = resolve(st, "arr2").lineage().elevate(TransformType.EXPRESSION);
            try (ScopeStack.Frame lambda = st.pushLambda(Arrays.asList("x", "Y"), Arrays.asList(arr1, arr2))) {
                ResolvedColumn x = resolve(st, "x");
                assertEquals(ResolvedColumn.Kind.LAMBDA_PARAMETER, x.getKind());
                assertEquals(sales("items.arr1"), single(x));
                assertEquals(sales("items.arr2"), single(resolve(st, "y")));
                assertEquals(sales("items.price"), single(resolve(st, "price")), "non-parameters still resolve");
                assertEquals(sales("items.x"), single(resolve(st, "i.x")), "a qualified name is not a parameter");
                try (ScopeStack.Frame nested = st.pushLambda(Collections.singletonList("x"),
                        Collections.emptyList())) {
                    ResolvedColumn inner = resolve(st, "x");
                    assertEquals(ResolvedColumn.Kind.LAMBDA_PARAMETER, inner.getKind());
                    assertTrue(inner.lineage().isEmpty(), "innermost lambda wins");
                }
            }
            ResolvedColumn after = resolve(st, "x");
            assertEquals(ResolvedColumn.Kind.RELATION_FIELD, after.getKind());
            assertEquals(sales("items.x"), single(after));
        }
        assertThrows(IllegalArgumentException.class,
                () -> st.pushLambda(Arrays.asList("a", "b"), Collections.singletonList(FieldLineage.constant())));
        st.assertEmpty();
    }

    @Test
    void semiJoinHidesTheRightSideButKeepsItsControl() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            RelationDescriptor u = from(st, "users", "u");
            // ON o.user_id = u.user_id  (analysed while both sides are visible)
            ScopeTestSupport.control(st, "o.user_id");
            ScopeTestSupport.control(st, "u.user_id");
            st.current().hideRelations(Collections.singletonList(u));
            assertEquals(ErrorCode.COLUMN_NOT_FOUND, codeOf(() -> resolve(st, "u.city")));
            assertEquals(sales("orders.user_id"), single(resolve(st, "user_id")), "no longer ambiguous");
            assertTrue(st.current().getControlSources().contains(sales("users.user_id")));
            assertEquals(ErrorCode.SCOPE_STATE, codeOf(() -> st.current().hideRelations(Collections.singletonList(
                    RelationDescriptor.values(null, null, Collections.singletonList(FieldLineage.constant()))))));
        }
    }

    /** Review R2-02: external catalogs are case-insensitive; the metadata spelling becomes the storage key. */
    @Test
    void externalCatalogNamesAreCanonicalisedThroughMetadata() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            RelationDescriptor ev = from(st, "HIVE.ODS.Events", "e");
            assertTrue(ev.isSchemaKnown());
            assertEquals(c("hive.ods.events.payload"), single(resolve(st, "e.PAYLOAD")));
            RelationDescriptor exact = from(st, "Sales.Orders", "o");
            assertFalse(exact.isSchemaKnown(), "default_catalog names are case-sensitive: no lower-case fallback");
            assertEquals(c("default_catalog.Sales.Orders.amount"), single(resolve(st, "o.amount")));
        }
        assertTrue(st.getReferencedTables().contains(c("hive.ods.events.x").getTable()));
        assertFalse(st.getReferencedTables().contains(c("hive.ODS.Events.x").getTable()));
    }

    private static Object single(ResolvedColumn rc) {
        List<?> keys = Arrays.asList(rc.lineage().getDirect().keySet().toArray());
        assertEquals(1, keys.size(), () -> "expected exactly one direct source for " + rc + " but got " + keys);
        return keys.get(0);
    }
}
