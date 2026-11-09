package com.starrocks.lineage.scope;

import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.TransformType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Shared fixtures: an in-memory catalog and small helpers that drive the scope API the same way the Stage 3
 * visitor does.
 */
final class ScopeTestSupport {

    private ScopeTestSupport() {
    }

    static InMemoryMetadataProvider catalog() {
        return new InMemoryMetadataProvider()
                .register("default_catalog.sales.orders", "order_id", "user_id", "amount", "dt", "tags")
                .register("default_catalog.sales.users", "user_id", "name", "city", "info")
                .register("default_catalog.sales.items", "order_id", "sku", "price", "arr1", "arr2", "x")
                .register("default_catalog.ods.orders", "order_id", "raw_amount")
                .register("hive.ods.events", "event_id", "user_id", "payload");
    }

    static ScopeStack newStack() {
        return new ScopeStack(NamespaceContext.of("default_catalog", "sales"), catalog());
    }

    static List<String> parts(String dotted) {
        return Arrays.asList(dotted.split("\\."));
    }

    static ColumnIdentifier c(String qualified) {
        return ColumnIdentifier.parse(qualified);
    }

    static ColumnIdentifier sales(String tableDotColumn) {
        return ColumnIdentifier.parse("default_catalog.sales." + tableDotColumn);
    }

    static ResolvedColumn resolve(ScopeStack stack, String dotted) {
        return stack.resolveColumn(parts(dotted));
    }

    /** Adds {@code FROM name [alias]} to the current query block and returns the relation. */
    static RelationDescriptor from(ScopeStack stack, String name, String alias) {
        RelationDescriptor r = stack.resolveRelation(parts(name), alias);
        stack.current().addRelation(r);
        return r;
    }

    /** Builds one select item by running {@code body} against a fresh tracker. */
    static RelationField item(String name, Consumer<ExpressionSourceTracker> body) {
        ExpressionSourceTracker t = new ExpressionSourceTracker();
        body.accept(t);
        return new RelationField(name, 0, t.build(), name);
    }

    /** {@code SELECT col} item (bare column). */
    static RelationField columnItem(ScopeStack stack, String dotted) {
        ResolvedColumn rc = resolve(stack, dotted);
        ExpressionSourceTracker t = new ExpressionSourceTracker();
        t.addReference(rc);
        String[] p = dotted.split("\\.");
        return new RelationField(p[p.length - 1], 0, t.build(), dotted);
    }

    /** Resolves {@code dotted} as a control source of the current block (WHERE, JOIN ON, GROUP BY ...). */
    static void control(ScopeStack stack, String dotted) {
        stack.currentQueryBlock().addControlLineage(resolve(stack, dotted).lineage());
    }

    static void inContext(ExpressionSourceTracker t, TransformType type, Runnable body) {
        try (ExpressionSourceTracker.Context ignored = t.enter(type)) {
            body.run();
        }
    }

    static List<String> none() {
        return Collections.emptyList();
    }
}
