package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import static com.starrocks.lineage.scope.ScopeTestSupport.from;
import static com.starrocks.lineage.scope.ScopeTestSupport.inContext;
import static com.starrocks.lineage.scope.ScopeTestSupport.newStack;
import static com.starrocks.lineage.scope.ScopeTestSupport.resolve;
import static com.starrocks.lineage.scope.ScopeTestSupport.sales;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpressionSourceTrackerTest {

    /** SUM(o.amount) OVER (PARTITION BY o.user_id ORDER BY o.dt). */
    @Test
    void windowFunctionArgumentIsWindowAndPartitionOrderAreIndirect() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            ExpressionSourceTracker t = new ExpressionSourceTracker();
            try (ExpressionSourceTracker.Context w = t.enter(TransformType.WINDOW)) {
                try (ExpressionSourceTracker.Context agg = t.enter(TransformType.AGGREGATION)) {
                    assertEquals(TransformType.WINDOW, t.currentTransform(), "dominance inside nesting");
                    t.addReference(resolve(st, "o.amount"));
                }
                try (ExpressionSourceTracker.Context over = t.enterIndirect()) {
                    assertTrue(t.isIndirect());
                    t.addReference(resolve(st, "o.user_id"));
                    t.addReference(resolve(st, "o.dt"));
                }
            }
            FieldLineage l = t.build();
            assertEquals(Collections.singletonMap(sales("orders.amount"), TransformType.WINDOW), l.getDirect());
            assertEquals(new LinkedHashSet<>(Arrays.asList(sales("orders.user_id"), sales("orders.dt"))),
                    l.getIndirect());
            assertEquals(TransformType.WINDOW, l.dominantTransform());
            assertEquals(3, t.getReferences().size());
            assertFalse(t.isBareColumn());
        }
    }

    /** CASE WHEN u.city = 'x' THEN SUM(o.amount) ELSE o.order_id END. */
    @Test
    void caseWhenConditionsAndBranchesAreDirectWithPerSourceTransform() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            from(st, "users", "u");
            ExpressionSourceTracker t = new ExpressionSourceTracker();
            inContext(t, TransformType.EXPRESSION, () -> {
                t.addReference(resolve(st, "u.city"));
                inContext(t, TransformType.AGGREGATION, () -> t.addReference(resolve(st, "o.amount")));
                t.addReference(resolve(st, "o.order_id"));
            });
            Map<ColumnIdentifier, TransformType> expected = new LinkedHashMap<>();
            expected.put(sales("users.city"), TransformType.EXPRESSION);
            expected.put(sales("orders.amount"), TransformType.AGGREGATION);
            expected.put(sales("orders.order_id"), TransformType.EXPRESSION);
            assertEquals(expected, t.build().getDirect());
        }
    }

    @Test
    void bareColumnIsIdentityAndLiteralsAreConstant() {
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            from(st, "orders", "o");
            ExpressionSourceTracker bare = new ExpressionSourceTracker();
            bare.addReference(resolve(st, "amount"));
            assertTrue(bare.isBareColumn());
            assertEquals(TransformType.IDENTITY, bare.build().dominantTransform());

            ExpressionSourceTracker literal = new ExpressionSourceTracker();
            literal.markNonColumnNode();
            assertTrue(literal.build().isConstant());
            assertEquals(TransformType.CONSTANT, literal.build().dominantTransform());

            ExpressionSourceTracker countStar = new ExpressionSourceTracker();
            inContext(countStar, TransformType.AGGREGATION, countStar::markNonColumnNode);
            assertTrue(countStar.build().isConstant());
            assertEquals(TransformType.AGGREGATION, countStar.getStrongestTransformSeen(),
                    "COUNT(*) has no source but is still an aggregation");
        }
    }

    /** Derived column (EXPRESSION in a CTE) read plainly stays EXPRESSION, read in MAX() becomes AGGREGATION. */
    @Test
    void derivedLineageIsComposedNotReanalysed() {
        FieldLineage inner = FieldLineage.physical(sales("orders.amount")).elevate(TransformType.EXPRESSION)
                .withIndirect(Collections.singleton(sales("orders.dt")));
        RelationDescriptor cte = RelationDescriptor.derived(RelationKind.CTE, "c",
                Collections.singletonList(new RelationField("amt", 0, inner, "amount * 2")), Collections.emptySet());
        ScopeStack st = newStack();
        try (ScopeStack.Frame f = st.pushQueryBlock()) {
            st.current().addRelation(cte);
            ExpressionSourceTracker plain = new ExpressionSourceTracker();
            plain.addReference(resolve(st, "amt"));
            assertEquals(TransformType.EXPRESSION, plain.build().getDirect().get(sales("orders.amount")));
            assertEquals(Collections.singleton(sales("orders.dt")), plain.build().getIndirect());

            ExpressionSourceTracker max = new ExpressionSourceTracker();
            inContext(max, TransformType.AGGREGATION, () -> max.addReference(resolve(st, "c.amt")));
            assertEquals(TransformType.AGGREGATION, max.build().getDirect().get(sales("orders.amount")));

            ExpressionSourceTracker inIndirect = new ExpressionSourceTracker();
            try (ExpressionSourceTracker.Context c = inIndirect.enterIndirect()) {
                inIndirect.addReference(resolve(st, "amt"));
            }
            assertTrue(inIndirect.build().isConstant());
            assertEquals(new LinkedHashSet<>(Arrays.asList(sales("orders.amount"), sales("orders.dt"))),
                    inIndirect.build().getIndirect());
        }
    }

    /** (SELECT MAX(i.price) FROM items i WHERE i.order_id = o.order_id) as a select item. */
    @Test
    void scalarSubqueryOutputAndControlAreAdded() {
        ExpressionSourceTracker t = new ExpressionSourceTracker();
        t.addLineage(FieldLineage.of(Collections.singletonMap(sales("items.price"), TransformType.AGGREGATION),
                Collections.emptySet()));
        t.addIndirect(Arrays.asList(sales("items.order_id"), sales("orders.order_id"), ColumnIdentifier.CONSTANT));
        FieldLineage l = t.build();
        assertEquals(Collections.singletonMap(sales("items.price"), TransformType.AGGREGATION), l.getDirect());
        assertEquals(new LinkedHashSet<>(Arrays.asList(sales("items.order_id"), sales("orders.order_id"))),
                l.getIndirect());
    }

    @Test
    void contextsMustBeClosedInOrder() {
        ExpressionSourceTracker t = new ExpressionSourceTracker();
        ExpressionSourceTracker.Context a = t.enter(TransformType.EXPRESSION);
        ExpressionSourceTracker.Context b = t.enterIndirect();
        assertEquals(ErrorCode.SCOPE_STATE, assertThrows(SemanticAnalysisException.class, a::close).getErrorCode());
        assertEquals(ErrorCode.SCOPE_STATE, assertThrows(SemanticAnalysisException.class, t::build).getErrorCode());
        b.close();
        b.close();
        a.close();
        assertTrue(t.build().isEmpty());
        assertEquals(TransformType.IDENTITY, t.currentTransform());
        assertThrows(IllegalArgumentException.class, () -> t.enter(TransformType.CONSTANT));
        assertThrows(IllegalArgumentException.class, () -> t.enter(null));
    }

    @Test
    void fieldLineageAlgebra() {
        FieldLineage a = FieldLineage.physical(sales("orders.amount"));
        FieldLineage b = FieldLineage.physical(sales("orders.amount")).elevate(TransformType.AGGREGATION);
        assertEquals(TransformType.AGGREGATION, a.union(b).getDirect().get(sales("orders.amount")));
        assertEquals(a, a.union(FieldLineage.constant()));
        assertEquals(a, FieldLineage.constant().union(a));
        assertEquals(a, a.elevate(TransformType.IDENTITY));
        assertTrue(FieldLineage.physical(ColumnIdentifier.CONSTANT).isEmpty());
        assertTrue(a.asIndirect().isConstant());
        assertEquals(Collections.singleton(sales("orders.amount")), a.asIndirect().getIndirect());
        assertThrows(IllegalArgumentException.class, () -> FieldLineage.of(
                Collections.singletonMap(sales("orders.amount"), TransformType.CONSTANT), Collections.emptySet()));
        assertEquals(a, a.withIndirect(Collections.emptySet()));
    }
}
