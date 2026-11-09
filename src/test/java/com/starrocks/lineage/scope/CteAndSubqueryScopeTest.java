package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.starrocks.lineage.scope.ScopeTestSupport.columnItem;
import static com.starrocks.lineage.scope.ScopeTestSupport.control;
import static com.starrocks.lineage.scope.ScopeTestSupport.from;
import static com.starrocks.lineage.scope.ScopeTestSupport.inContext;
import static com.starrocks.lineage.scope.ScopeTestSupport.item;
import static com.starrocks.lineage.scope.ScopeTestSupport.newStack;
import static com.starrocks.lineage.scope.ScopeTestSupport.none;
import static com.starrocks.lineage.scope.ScopeTestSupport.resolve;
import static com.starrocks.lineage.scope.ScopeTestSupport.sales;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Architect iron rule #2: multi-level CTE chains, shadowing and correlated sub-queries.
 */
class CteAndSubqueryScopeTest {

    /**
     * <pre>
     * WITH c1 AS (SELECT user_id, amount * 2 AS amt FROM orders WHERE dt &gt; '2024-01-01'),
     *      c2 (uid, total) AS (SELECT user_id, SUM(amt) FROM c1 GROUP BY user_id)
     * SELECT c2.total FROM c2
     * </pre>
     */
    @Test
    void cteChainComposesLineageAndControlThroughEveryLayer() {
        ScopeStack st = newStack();
        RelationField total;
        Set<ColumnIdentifier> outerControl;
        try (ScopeStack.Frame with = st.pushCte()) {
            RelationDescriptor c1Body;
            try (ScopeStack.Frame body = st.pushQueryBlock()) {
                from(st, "orders", null);
                RelationField uid = columnItem(st, "user_id");
                RelationField amt = item("amt", t -> inContext(t, TransformType.EXPRESSION,
                        () -> t.addReference(resolve(st, "amount"))));
                control(st, "dt");
                st.current().setSelectOutputs(Arrays.asList(uid, amt));
                c1Body = st.current().toDerivedRelation(RelationKind.SUBQUERY, null);
                assertEquals(ErrorCode.SCOPE_STATE, assertThrows(SemanticAnalysisException.class,
                        () -> st.registerCte("c1", none(), st.current().toDerivedRelation(RelationKind.SUBQUERY,
                                null))).getErrorCode(), "a CTE cannot be registered while its body is open");
            }
            st.registerCte("c1", none(), c1Body);
            RelationDescriptor c2Body;
            try (ScopeStack.Frame body = st.pushQueryBlock()) {
                from(st, "c1", null);
                RelationField uid = columnItem(st, "user_id");
                RelationField sum = item("sum(amt)", t -> inContext(t, TransformType.AGGREGATION,
                        () -> t.addReference(resolve(st, "amt"))));
                control(st, "user_id");
                st.current().setSelectOutputs(Arrays.asList(uid, sum));
                c2Body = st.current().toDerivedRelation(RelationKind.SUBQUERY, null);
            }
            RelationDescriptor c2 = st.registerCte("c2", Arrays.asList("uid", "total"), c2Body);
            assertEquals(Arrays.asList("uid", "total"), Arrays.asList(c2.field(0).getName(),
                    c2.field(1).getName()));
            try (ScopeStack.Frame main = st.pushQueryBlock()) {
                from(st, "c2", null);
                total = columnItem(st, "c2.total");
                assertEquals(ErrorCode.COLUMN_NOT_FOUND, assertThrows(SemanticAnalysisException.class,
                        () -> resolve(st, "amt")).getErrorCode(), "c1 columns are not visible through c2");
                outerControl = st.current().getControlSources();
            }
        }
        st.assertEmpty();

        Map<ColumnIdentifier, TransformType> direct = total.getLineage().getDirect();
        assertEquals(Collections.singletonMap(sales("orders.amount"), TransformType.AGGREGATION), direct);
        assertEquals(new LinkedHashSet<>(Arrays.asList(sales("orders.dt"), sales("orders.user_id"))), outerControl,
                "WHERE of c1 and GROUP BY of c2 control every row of the final result");
        assertEquals(Collections.singleton(sales("orders.user_id").getTable()), st.getReferencedTables(),
                "CTE references are not physical tables");
    }

    @Test
    void cteNameShadowsTableButNotInsideItsOwnBodyAndDuplicatesAreRejected() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame with = st.pushCte()) {
            RelationDescriptor bodyRelation;
            try (ScopeStack.Frame body = st.pushQueryBlock()) {
                // WITH orders AS (SELECT order_id, amount FROM orders) -- inner 'orders' is the physical table
                RelationDescriptor physical = from(st, "orders", null);
                assertEquals(RelationKind.TABLE, physical.getKind());
                st.current().setSelectOutputs(Arrays.asList(columnItem(st, "order_id"), columnItem(st, "amount")));
                bodyRelation = st.current().toDerivedRelation(RelationKind.SUBQUERY, null);
            }
            st.registerCte("orders", none(), bodyRelation);
            assertEquals(ErrorCode.DUPLICATE_CTE, assertThrows(SemanticAnalysisException.class,
                    () -> st.registerCte("ORDERS", none(), RelationDescriptor.values(null, null,
                            Collections.singletonList(FieldLineage.constant())))).getErrorCode());
            try (ScopeStack.Frame main = st.pushQueryBlock()) {
                RelationDescriptor cte = from(st, "orders", "o");
                assertEquals(RelationKind.CTE, cte.getKind());
                assertEquals(2, cte.size(), "the CTE (2 columns) shadows the 5-column table");
                assertEquals(sales("orders.amount"), resolve(st, "o.amount").lineage().getDirect().keySet()
                        .iterator().next());
                assertEquals(RelationKind.TABLE, from(st, "sales.orders", null).getKind(),
                        "a qualified name never designates a CTE");
            }
        }
        assertEquals(ErrorCode.SCOPE_STATE, assertThrows(SemanticAnalysisException.class,
                () -> st.registerCte("x", none(), RelationDescriptor.values(null, null,
                        Collections.singletonList(FieldLineage.constant())))).getErrorCode());
    }

    @Test
    void innerWithClauseShadowsOuterCteOnlyWhileOpen() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame outerWith = st.pushCte()) {
            st.registerCte("c", none(), cteOver(st, "orders", "amount"));
            try (ScopeStack.Frame block = st.pushQueryBlock()) {
                try (ScopeStack.Frame innerWith = st.pushCte()) {
                    st.registerCte("C", none(), cteOver(st, "users", "city"));
                    assertEquals(sales("users.city"), onlySource(st.findCte("c").get().field(0)));
                }
                assertEquals(sales("orders.amount"), onlySource(st.findCte("c").get().field(0)),
                        "outer CTE visible again after the inner WITH is closed");
            }
        }
        assertFalse(st.findCte("c").isPresent());
    }

    /**
     * <pre>
     * SELECT t.order_id FROM orders t
     * WHERE EXISTS (SELECT 1 FROM users t WHERE t.user_id = user_id AND amount &gt; 0
     *               AND EXISTS (SELECT 1 FROM items i WHERE i.order_id = order_id AND i.price &lt; amount))
     * </pre>
     */
    @Test
    void shadowedAliasAndMultiLevelCorrelatedReferences() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame outer = st.pushQueryBlock()) {
            from(st, "orders", "t");
            try (ScopeStack.Frame sub = st.pushQueryBlock()) {
                from(st, "users", "t");
                ResolvedColumn innerT = resolve(st, "t.user_id");
                assertEquals(sales("users.user_id"), onlySource(innerT.getField()));
                assertFalse(innerT.isCorrelated(), "inner alias t shadows the outer t");

                ResolvedColumn bare = resolve(st, "user_id");
                assertEquals(sales("users.user_id"), onlySource(bare.getField()), "local scope wins");

                ResolvedColumn amount = resolve(st, "amount");
                assertTrue(amount.isCorrelated());
                assertEquals(1, amount.getOuterDepth());
                assertEquals(sales("orders.amount"), onlySource(amount.getField()));
                try (ScopeStack.Frame sub2 = st.pushQueryBlock()) {
                    from(st, "items", "i");
                    assertEquals(0, resolve(st, "order_id").getOuterDepth(), "items.order_id is local");
                    ResolvedColumn deep = resolve(st, "amount");
                    assertEquals(2, deep.getOuterDepth());
                    assertEquals(1, resolve(st, "city").getOuterDepth());
                    assertEquals(sales("users.user_id"), onlySource(resolve(st, "t.user_id").getField()),
                            "nearest t wins");
                }
            }
            assertEquals(sales("orders.user_id"), onlySource(resolve(st, "t.user_id").getField()),
                    "after the sub-query the outer t is visible again");
        }
        st.assertEmpty();
    }

    @Test
    void derivedTableAliasAndColumnAliases() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame outer = st.pushQueryBlock()) {
            RelationDescriptor sub;
            try (ScopeStack.Frame inner = st.pushQueryBlock()) {
                from(st, "orders", null);
                st.current().setSelectOutputs(Arrays.asList(columnItem(st, "order_id"), columnItem(st, "amount")));
                control(st, "dt");
                sub = st.current().toDerivedRelation(RelationKind.SUBQUERY, "s").withColumnAliases(
                        Arrays.asList("id", "amt"));
            }
            st.current().addRelation(sub);
            assertEquals(sales("orders.amount"), onlySource(resolve(st, "s.amt").getField()));
            assertEquals(ErrorCode.COLUMN_NOT_FOUND, assertThrows(SemanticAnalysisException.class,
                    () -> resolve(st, "s.amount")).getErrorCode(), "renamed by the column alias list");
            assertTrue(st.current().getControlSources().contains(sales("orders.dt")),
                    "control of the derived table propagates to the outer block");
            assertEquals(ErrorCode.COLUMN_ALIAS_MISMATCH, assertThrows(SemanticAnalysisException.class,
                    () -> sub.withColumnAliases(Collections.singletonList("only_one"))).getErrorCode());
        }
    }

    @Test
    void frameDisciplineDetectsPollutionAndLeaks() {
        ScopeStack st = newStack();
        ScopeStack.Frame outer = st.pushQueryBlock();
        ScopeStack.Frame inner = st.pushQueryBlock();
        SemanticAnalysisException e = assertThrows(SemanticAnalysisException.class, outer::close);
        assertEquals(ErrorCode.SCOPE_STATE, e.getErrorCode());
        assertEquals(2, st.depth(), "an out-of-order close must not pop anything");
        assertEquals(ErrorCode.SCOPE_STATE,
                assertThrows(SemanticAnalysisException.class, st::assertEmpty).getErrorCode());
        inner.close();
        inner.close();
        outer.close();
        st.assertEmpty();

        // an exception thrown inside nested try-with-resources unwinds the stack cleanly
        assertThrows(SemanticAnalysisException.class, () -> {
            try (ScopeStack.Frame a = st.pushQueryBlock()) {
                try (ScopeStack.Frame b = st.pushCte()) {
                    try (ScopeStack.Frame c = st.pushQueryBlock()) {
                        resolve(st, "does_not_exist");
                    }
                }
            }
        });
        st.assertEmpty();
        assertEquals(ErrorCode.SCOPE_STATE,
                assertThrows(SemanticAnalysisException.class, st::current).getErrorCode());
        assertEquals(ErrorCode.SCOPE_STATE,
                assertThrows(SemanticAnalysisException.class, st::currentQueryBlock).getErrorCode());
    }

    @Test
    void nestingDepthIsBounded() {
        ScopeStack st = new ScopeStack(com.starrocks.lineage.model.NamespaceContext.of(null, "sales"),
                ScopeTestSupport.catalog(), 3);
        ScopeStack.Frame a = st.pushQueryBlock();
        ScopeStack.Frame b = st.pushQueryBlock();
        ScopeStack.Frame c = st.pushQueryBlock();
        assertEquals(ErrorCode.SCOPE_STATE,
                assertThrows(SemanticAnalysisException.class, st::pushQueryBlock).getErrorCode());
        c.close();
        b.close();
        a.close();
        st.assertEmpty();
        assertThrows(IllegalArgumentException.class,
                () -> new ScopeStack(com.starrocks.lineage.model.NamespaceContext.of(null, null),
                        ScopeTestSupport.catalog(), 0));
    }

    @Test
    void scopeKindGuards() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame with = st.pushCte()) {
            assertEquals(ErrorCode.SCOPE_STATE, assertThrows(SemanticAnalysisException.class,
                    () -> st.current().addRelation(RelationDescriptor.values(null, null,
                            Collections.singletonList(FieldLineage.constant())))).getErrorCode());
            assertEquals(ErrorCode.SCOPE_STATE, assertThrows(SemanticAnalysisException.class,
                    () -> st.expandStar(none())).getErrorCode());
        }
        try (ScopeStack.Frame block = st.pushQueryBlock()) {
            Scope s = st.current();
            assertSame(s, st.currentQueryBlock());
            try (ScopeStack.Frame lambda = st.pushLambda(Collections.singletonList("x"), Collections.emptyList())) {
                assertSame(s, st.currentQueryBlock(), "lambda scopes are skipped");
                assertEquals(s, st.current().getParent());
                assertEquals(1, st.current().getLevel());
            }
        }
    }

    private static RelationDescriptor cteOver(ScopeStack st, String table, String column) {
        try (ScopeStack.Frame body = st.pushQueryBlock()) {
            from(st, table, null);
            st.current().setSelectOutputs(Collections.singletonList(columnItem(st, column)));
            return st.current().toDerivedRelation(RelationKind.SUBQUERY, null);
        }
    }

    private static ColumnIdentifier onlySource(RelationField f) {
        assertEquals(1, f.getLineage().getDirect().size(), () -> "one source expected: " + f);
        return f.getLineage().getDirect().keySet().iterator().next();
    }
}
