package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.ColumnUsageStat;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.TransformType;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AnalyzerTestSupport {

    static final NamespaceContext SALES = NamespaceContext.of("default_catalog", "sales");

    private AnalyzerTestSupport() {
    }

    static InMemoryMetadataProvider catalog() {
        return new InMemoryMetadataProvider()
                .register("default_catalog.sales.orders", "order_id", "user_id", "amount", "dt", "tags")
                .register("default_catalog.sales.users", "user_id", "name", "city", "info")
                .register("default_catalog.sales.items", "order_id", "sku", "price", "arr1", "arr2", "x")
                .register("default_catalog.dw.target", "uid", "total", "rk")
                .register("hive.ods.events", "event_id", "user_id", "payload");
    }

    static SqlParseResult analyze(String sql) {
        return new StarRocksSqlAnalyzer(catalog()).analyze("q1", sql, SALES);
    }

    static SqlParseResult success(String sql) {
        SqlParseResult r = analyze(sql);
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> "unexpected result: " + r);
        return r;
    }

    static ColumnIdentifier c(String qualified) {
        if (qualified.split("\\.").length == 2) {
            return ColumnIdentifier.parse("default_catalog.sales." + qualified);
        }
        if (qualified.split("\\.").length == 3) {
            return ColumnIdentifier.parse("default_catalog." + qualified);
        }
        return ColumnIdentifier.parse(qualified);
    }

    static Optional<ColumnLineageEdge> edge(SqlParseResult r, String target, String source, LineageType type) {
        ColumnIdentifier t = c(target);
        ColumnIdentifier s = "CONSTANT".equals(source) ? ColumnIdentifier.CONSTANT : c(source);
        return r.getLineageEdges().stream()
                .filter(e -> e.getTarget().equals(t) && e.getSource().equals(s) && e.getLineageType() == type)
                .findFirst();
    }

    static void assertDirect(SqlParseResult r, String target, String source, TransformType transform) {
        ColumnLineageEdge e = edge(r, target, source, LineageType.DIRECT)
                .orElseThrow(() -> new AssertionError("missing DIRECT " + source + " -> " + target + " in "
                        + r.getLineageEdges()));
        assertEquals(transform, e.getTransformType(), () -> "transform of " + e);
    }

    static void assertIndirect(SqlParseResult r, String target, String source) {
        assertTrue(edge(r, target, source, LineageType.INDIRECT).isPresent(),
                () -> "missing INDIRECT " + source + " -> " + target + " in " + r.getLineageEdges());
    }

    static void assertNoEdge(SqlParseResult r, String target, String source) {
        assertFalse(edge(r, target, source, LineageType.DIRECT).isPresent()
                        || edge(r, target, source, LineageType.INDIRECT).isPresent(),
                () -> "unexpected edge " + source + " -> " + target);
    }

    static Set<ColumnIdentifier> targets(SqlParseResult r) {
        Set<ColumnIdentifier> t = new LinkedHashSet<>();
        r.getLineageEdges().forEach(e -> t.add(e.getTarget()));
        return t;
    }

    static ColumnUsageStat usage(SqlParseResult r, String column) {
        return r.usageOf(c(column)).orElseThrow(() -> new AssertionError("no usage of " + column + " in "
                + r.getColumnUsages().values()));
    }

    static void assertClauses(SqlParseResult r, String column, ClauseType... expected) {
        Set<ClauseType> e = expected.length == 0 ? EnumSet.noneOf(ClauseType.class)
                : EnumSet.of(expected[0], expected);
        assertEquals(e, usage(r, column).getClauses(), () -> "clauses of " + column);
    }
}
