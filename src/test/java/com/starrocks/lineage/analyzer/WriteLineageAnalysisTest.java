package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TransformType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.assertDirect;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.assertIndirect;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.assertNoEdge;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.c;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.edge;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.success;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.targets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Column lineage of every write statement type.
 */
class WriteLineageAnalysisTest {

    /**
     * Multi-level CTE + window + positional INSERT without column list.
     */
    @Test
    void insertWithCteChainAndWindow() {
        SqlParseResult r = success("INSERT INTO dw.target "
                + "WITH c1 AS (SELECT user_id, amount * 2 amt FROM orders WHERE dt > '2024-01-01'), "
                + "     c2 AS (SELECT user_id, SUM(amt) total FROM c1 GROUP BY user_id) "
                + "SELECT c2.user_id, c2.total, RANK() OVER (PARTITION BY c2.user_id ORDER BY c2.total) FROM c2");
        assertEquals(StatementType.INSERT_INTO, r.getStatementType().get());
        assertEquals(c("dw.target.x").getTable(), r.getTargetTable().get());
        assertDirect(r, "dw.target.uid", "orders.user_id", TransformType.IDENTITY);
        assertDirect(r, "dw.target.total", "orders.amount", TransformType.AGGREGATION);
        assertDirect(r, "dw.target.rk", "CONSTANT", TransformType.CONSTANT);
        assertIndirect(r, "dw.target.rk", "orders.user_id");
        assertIndirect(r, "dw.target.rk", "orders.amount");
        for (String t : Arrays.asList("dw.target.uid", "dw.target.total", "dw.target.rk")) {
            assertIndirect(r, t, "orders.dt");
            assertIndirect(r, t, "orders.user_id");
        }
        assertEquals("c2.total", edge(r, "dw.target.total", "orders.amount", LineageType.DIRECT).get()
                .getTransformExpr());
        assertEquals(new LinkedHashSet<>(Arrays.asList(c("orders.x").getTable(), c("dw.target.x").getTable())),
                r.getSourceTables(), "target is resolved too; CTE names are not tables");
        assertTrue(r.getColumnUsages().isEmpty(), "write statements produce no usage");
    }

    @Test
    void insertOverwriteWithColumnListAndQualifiedStar() {
        SqlParseResult r = success("INSERT OVERWRITE dw.target (total, uid) "
                + "SELECT t1.* FROM (SELECT amount, user_id FROM orders) t1");
        assertEquals(StatementType.INSERT_OVERWRITE, r.getStatementType().get());
        assertDirect(r, "dw.target.total", "orders.amount", TransformType.IDENTITY);
        assertDirect(r, "dw.target.uid", "orders.user_id", TransformType.IDENTITY);
        assertFalse(targets(r).contains(c("dw.target.rk")), "column not listed is not written");
    }

    @Test
    void insertWithoutColumnListUsesTargetSchemaOrder() {
        SqlParseResult r = success("INSERT INTO dw.target SELECT o.user_id, o.amount + 1, o.dt FROM orders o "
                + "ORDER BY o.dt");
        assertDirect(r, "dw.target.uid", "orders.user_id", TransformType.IDENTITY);
        assertDirect(r, "dw.target.total", "orders.amount", TransformType.EXPRESSION);
        assertDirect(r, "dw.target.rk", "orders.dt", TransformType.IDENTITY);
        assertNoEdge(r, "dw.target.uid", "orders.dt");
        assertFalse(r.isDegraded());
    }

    @Test
    void orderByWithLimitControlsRows() {
        SqlParseResult r = success("INSERT INTO dw.target (uid) SELECT user_id FROM orders ORDER BY dt DESC LIMIT 10");
        assertIndirect(r, "dw.target.uid", "orders.dt");
    }

    @Test
    void insertValuesWithScalarSubquery() {
        SqlParseResult r = success("INSERT INTO dw.target (uid, total) VALUES (1, 2), "
                + "(3, (SELECT MAX(amount) FROM orders WHERE dt > '2024-01-01'))");
        assertDirect(r, "dw.target.uid", "CONSTANT", TransformType.CONSTANT);
        assertDirect(r, "dw.target.total", "orders.amount", TransformType.AGGREGATION);
        assertIndirect(r, "dw.target.total", "orders.dt");
        assertEquals("(SELECT MAX(amount) FROM orders WHERE dt > '2024-01-01')",
                edge(r, "dw.target.total", "orders.amount", LineageType.DIRECT).get().getTransformExpr());
    }

    @Test
    void ctasWithAndWithoutColumnList() {
        SqlParseResult named = success("CREATE TABLE dw.c2 (k, v) AS SELECT user_id, amount FROM orders");
        assertEquals(StatementType.CREATE_TABLE_AS_SELECT, named.getStatementType().get());
        assertDirect(named, "dw.c2.k", "orders.user_id", TransformType.IDENTITY);
        assertDirect(named, "dw.c2.v", "orders.amount", TransformType.IDENTITY);

        SqlParseResult implicit = success("CREATE TABLE IF NOT EXISTS dw.c3 DISTRIBUTED BY HASH(city) BUCKETS 4 "
                + "AS SELECT city, COUNT(*) AS cnt FROM users GROUP BY city");
        assertDirect(implicit, "dw.c3.city", "users.city", TransformType.IDENTITY);
        assertDirect(implicit, "dw.c3.cnt", "CONSTANT", TransformType.CONSTANT);
        assertIndirect(implicit, "dw.c3.cnt", "users.city");
        assertIndirect(implicit, "dw.c3.city", "users.city");
        assertFalse(implicit.isDegraded(), "aliased / plain column names need no engine naming");

        SqlParseResult unaliased = success("CREATE VIEW dw.v9 AS SELECT city, COUNT(*) FROM users GROUP BY city");
        assertTrue(unaliased.getWarnings().stream().anyMatch(w -> w.contains("derived from the query text")),
                "no metadata for the new view yet: unaliased expression names are a best effort");
    }

    @Test
    void viewsWithColumnListStructAndUnion() {
        SqlParseResult v = success("CREATE OR REPLACE VIEW dw.v1 (a, b) AS "
                + "SELECT u.info.zip, upper(u.name) FROM users u");
        assertEquals(StatementType.CREATE_VIEW, v.getStatementType().get());
        assertDirect(v, "dw.v1.a", "users.info", TransformType.EXPRESSION);
        assertDirect(v, "dw.v1.b", "users.name", TransformType.EXPRESSION);

        SqlParseResult alter = success("ALTER VIEW dw.v1 AS SELECT order_id AS id FROM orders "
                + "UNION ALL SELECT user_id FROM users");
        assertEquals(StatementType.ALTER_VIEW, alter.getStatementType().get());
        assertDirect(alter, "dw.v1.id", "orders.order_id", TransformType.IDENTITY);
        assertDirect(alter, "dw.v1.id", "users.user_id", TransformType.IDENTITY);
    }

    @Test
    void materializedViewWithJoinAndAggregation() {
        SqlParseResult r = success("CREATE MATERIALIZED VIEW dw.mv1 DISTRIBUTED BY HASH(city) REFRESH ASYNC "
                + "AS SELECT u.city, SUM(o.amount) AS gmv, COUNT(DISTINCT o.user_id) AS buyers "
                + "FROM orders o JOIN users u ON o.user_id = u.user_id WHERE o.dt >= '2024-01-01' GROUP BY u.city");
        assertEquals(StatementType.CREATE_MATERIALIZED_VIEW, r.getStatementType().get());
        assertDirect(r, "dw.mv1.city", "users.city", TransformType.IDENTITY);
        assertDirect(r, "dw.mv1.gmv", "orders.amount", TransformType.AGGREGATION);
        assertDirect(r, "dw.mv1.buyers", "orders.user_id", TransformType.AGGREGATION);
        for (String t : Arrays.asList("dw.mv1.city", "dw.mv1.gmv", "dw.mv1.buyers")) {
            assertIndirect(r, t, "orders.user_id");
            assertIndirect(r, t, "users.user_id");
            assertIndirect(r, t, "orders.dt");
            assertIndirect(r, t, "users.city");
        }
    }

    @Test
    void updateWithFromJoin() {
        SqlParseResult r = success("UPDATE orders SET amount = u.city, tags = DEFAULT FROM users u "
                + "WHERE orders.user_id = u.user_id");
        assertEquals(StatementType.UPDATE, r.getStatementType().get());
        assertDirect(r, "orders.amount", "users.city", TransformType.IDENTITY);
        assertIndirect(r, "orders.amount", "orders.user_id");
        assertIndirect(r, "orders.amount", "users.user_id");
        assertDirect(r, "orders.tags", "CONSTANT", TransformType.CONSTANT);
        assertFalse(targets(r).contains(c("orders.dt")), "only assigned columns are targets");
    }

    @Test
    void deleteWithUsingAffectsEveryColumnIndirectly() {
        SqlParseResult r = success("DELETE FROM orders USING users u WHERE orders.user_id = u.user_id "
                + "AND u.city = 'x'");
        assertEquals(StatementType.DELETE, r.getStatementType().get());
        assertEquals(5, targets(r).size());
        for (ColumnLineageEdge e : r.getLineageEdges()) {
            assertEquals(LineageType.INDIRECT, e.getLineageType());
        }
        assertIndirect(r, "orders.tags", "users.city");
        assertIndirect(r, "orders.order_id", "orders.user_id");
    }

    @Test
    void submitTaskWrapsCtasAndInsert() {
        SqlParseResult ctas = success("SUBMIT TASK t1 AS CREATE TABLE dw.t2 AS SELECT city FROM users");
        assertEquals(StatementType.SUBMIT_TASK, ctas.getStatementType().get());
        assertDirect(ctas, "dw.t2.city", "users.city", TransformType.IDENTITY);
        SqlParseResult insert = success("SUBMIT TASK AS INSERT INTO dw.target (uid) SELECT user_id FROM orders");
        assertEquals(StatementType.SUBMIT_TASK, insert.getStatementType().get());
        assertDirect(insert, "dw.target.uid", "orders.user_id", TransformType.IDENTITY);
    }

    @Test
    void unnestAndLambdaLineage() {
        SqlParseResult r = success("INSERT INTO dw.target (uid, total) "
                + "SELECT t.e, array_sum(array_map((x, y) -> x * y, arr1, arr2)) FROM items, UNNEST(arr1) AS t(e)");
        assertDirect(r, "dw.target.uid", "items.arr1", TransformType.EXPRESSION);
        assertDirect(r, "dw.target.total", "items.arr1", TransformType.EXPRESSION);
        assertDirect(r, "dw.target.total", "items.arr2", TransformType.EXPRESSION);
        assertNoEdge(r, "dw.target.total", "items.x");
    }

    @Test
    void semiJoinAndCorrelatedExistsAreIndirect() {
        SqlParseResult r = success("INSERT INTO dw.target (uid) SELECT o.user_id FROM orders o "
                + "LEFT ANTI JOIN users u ON o.user_id = u.user_id "
                + "WHERE EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.order_id AND i.price > 0)");
        assertDirect(r, "dw.target.uid", "orders.user_id", TransformType.IDENTITY);
        assertIndirect(r, "dw.target.uid", "users.user_id");
        assertIndirect(r, "dw.target.uid", "items.order_id");
        assertIndirect(r, "dw.target.uid", "items.price");
        assertIndirect(r, "dw.target.uid", "orders.order_id");
        assertEquals(1, r.getLineageEdges().stream()
                .filter(e -> e.getLineageType() == LineageType.DIRECT).count());
    }

    @Test
    void sameSourceDirectAndIndirectAreTwoEdges() {
        SqlParseResult r = success("INSERT INTO dw.target (total) SELECT amount FROM orders WHERE amount > 0");
        assertDirect(r, "dw.target.total", "orders.amount", TransformType.IDENTITY);
        assertIndirect(r, "dw.target.total", "orders.amount");
        assertEquals(2, r.getLineageEdges().size());
    }
}
