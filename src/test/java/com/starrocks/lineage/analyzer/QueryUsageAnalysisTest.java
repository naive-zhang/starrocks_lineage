package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.ColumnUsageStat;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import org.junit.jupiter.api.Test;

import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.assertClauses;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.c;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.success;
import static com.starrocks.lineage.analyzer.AnalyzerTestSupport.usage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clause attribution and per-query de-duplication of column usage (iron rules #6 and #7).
 */
class QueryUsageAnalysisTest {

    @Test
    void ruleSevenSameColumnTwiceInSelectOnceInWhere() {
        SqlParseResult r = success("SELECT a.amount, a.amount + 1 FROM orders a WHERE a.amount > 10");
        assertEquals(StatementType.QUERY, r.getStatementType().get());
        assertTrue(r.getLineageEdges().isEmpty(), "queries produce no lineage");
        assertEquals(1, r.getColumnUsages().size());
        ColumnUsageStat a = usage(r, "orders.amount");
        assertEquals(1, a.getAccessQueryCnt());
        assertEquals(1, a.getSelectClauseCnt());
        assertEquals(1, a.getWhereClauseCnt());
        assertEquals(0, a.getJoinClauseCnt() + a.getGroupByCnt() + a.getOrderByCnt());
        assertEquals(2, a.occurrences(ClauseType.SELECT));
    }

    @Test
    void ruleSixExistsSubqueryClausesAreItsOwn() {
        SqlParseResult r = success("SELECT o.order_id FROM orders o "
                + "WHERE EXISTS (SELECT u.name FROM users u WHERE u.user_id = o.user_id)");
        assertClauses(r, "orders.order_id", ClauseType.SELECT);
        assertClauses(r, "users.name", ClauseType.SELECT);
        assertClauses(r, "users.user_id", ClauseType.WHERE);
        assertClauses(r, "orders.user_id", ClauseType.WHERE);
        assertEquals(4, r.getColumnUsages().size());
    }

    @Test
    void joinOnUsingGroupByHavingOrderByAndOrdinals() {
        SqlParseResult r = success("SELECT u.city, SUM(o.amount) total FROM orders o JOIN users u "
                + "ON o.user_id = u.user_id GROUP BY 1 HAVING total > 100 ORDER BY total DESC, 1 LIMIT 10");
        assertClauses(r, "orders.user_id", ClauseType.JOIN_ON);
        assertClauses(r, "users.user_id", ClauseType.JOIN_ON);
        assertClauses(r, "users.city", ClauseType.SELECT, ClauseType.GROUP_BY, ClauseType.ORDER_BY);
        assertClauses(r, "orders.amount", ClauseType.SELECT, ClauseType.HAVING, ClauseType.ORDER_BY);
        ColumnUsageStat amount = usage(r, "orders.amount");
        assertEquals(1, amount.getGroupByCnt(), "HAVING counts in group_by_cnt");
        assertEquals(1, amount.getOrderByCnt());
        assertEquals(1, usage(r, "users.user_id").getJoinClauseCnt());

        SqlParseResult using = success("SELECT name FROM orders JOIN users USING (user_id)");
        assertClauses(using, "orders.user_id", ClauseType.JOIN_USING);
        assertClauses(using, "users.user_id", ClauseType.JOIN_USING);
    }

    @Test
    void groupByPrefersInputColumnOrderByPrefersAlias() {
        // output alias 'user_id' hides nothing in GROUP BY (input wins) but wins in ORDER BY
        SqlParseResult r = success("SELECT amount AS user_id FROM orders GROUP BY user_id, amount ORDER BY user_id");
        assertClauses(r, "orders.user_id", ClauseType.GROUP_BY);
        assertClauses(r, "orders.amount", ClauseType.SELECT, ClauseType.GROUP_BY, ClauseType.ORDER_BY);
    }

    @Test
    void windowPartitionAndOrderAndQualify() {
        SqlParseResult r = success("SELECT user_id, SUM(amount) OVER (PARTITION BY user_id ORDER BY dt "
                + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s, "
                + "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) rn FROM orders QUALIFY rn = 1");
        assertClauses(r, "orders.user_id", ClauseType.SELECT, ClauseType.WINDOW_PARTITION_BY, ClauseType.QUALIFY);
        assertClauses(r, "orders.dt", ClauseType.WINDOW_ORDER_BY, ClauseType.QUALIFY);
        assertClauses(r, "orders.amount", ClauseType.SELECT);
        ColumnUsageStat dt = usage(r, "orders.dt");
        assertEquals(1, dt.getOrderByCnt(), "window ORDER BY counts in order_by_cnt");
        assertEquals(1, dt.getWhereClauseCnt(), "QUALIFY counts in where_clause_cnt");
        assertEquals(1, usage(r, "orders.user_id").getGroupByCnt(), "PARTITION BY counts in group_by_cnt");
    }

    @Test
    void unnestArgumentsAndVirtualColumnsMapToPhysicalArrays() {
        SqlParseResult r = success("SELECT i.order_id, t.c1 FROM items i, LATERAL UNNEST(i.arr1, i.arr2) AS t(c1, c2) "
                + "WHERE t.c2 > 0");
        assertClauses(r, "items.arr1", ClauseType.TABLE_FUNCTION, ClauseType.SELECT);
        assertClauses(r, "items.arr2", ClauseType.TABLE_FUNCTION, ClauseType.WHERE);
        ColumnUsageStat arr2 = usage(r, "items.arr2");
        assertEquals(0, arr2.getSelectClauseCnt());
        assertEquals(1, arr2.getWhereClauseCnt());
        assertFalse(r.usageOf(c("items.c1")).isPresent(), "virtual column is never physical");
    }

    @Test
    void lambdaParametersAreNotPhysicalColumns() {
        SqlParseResult r = success("SELECT array_map((x, y) -> x + y, arr1, arr2), x FROM items");
        assertClauses(r, "items.arr1", ClauseType.SELECT);
        assertClauses(r, "items.arr2", ClauseType.SELECT);
        assertEquals(1, usage(r, "items.x").occurrences(ClauseType.SELECT),
                "only the projection 'x' outside the lambda reads items.x");
    }

    @Test
    void correlatedScalarAndInSubqueries() {
        SqlParseResult r = success("SELECT o.order_id, (SELECT MAX(i.price) FROM items i WHERE i.order_id = o.order_id)"
                + " mp FROM orders o WHERE o.user_id IN (SELECT user_id FROM users WHERE city = 'x')");
        assertClauses(r, "orders.order_id", ClauseType.SELECT, ClauseType.WHERE);
        assertClauses(r, "items.price", ClauseType.SELECT);
        assertClauses(r, "items.order_id", ClauseType.WHERE);
        assertClauses(r, "orders.user_id", ClauseType.WHERE);
        assertClauses(r, "users.user_id", ClauseType.SELECT);
        assertClauses(r, "users.city", ClauseType.WHERE);
    }

    @Test
    void derivedColumnsAttributeOuterClausesToPhysicalSources() {
        SqlParseResult r = success("WITH c AS (SELECT user_id, amount * 2 AS amt FROM orders) "
                + "SELECT user_id FROM c WHERE amt > 3 ORDER BY amt");
        assertClauses(r, "orders.amount", ClauseType.SELECT, ClauseType.WHERE, ClauseType.ORDER_BY);
        assertClauses(r, "orders.user_id", ClauseType.SELECT);
    }

    @Test
    void setOperationBranchesAndOuterOrderBy() {
        SqlParseResult r = success("(SELECT order_id k FROM orders WHERE amount > 0) UNION ALL "
                + "(SELECT user_id FROM users) ORDER BY k LIMIT 5");
        assertClauses(r, "orders.order_id", ClauseType.SELECT, ClauseType.ORDER_BY);
        assertClauses(r, "users.user_id", ClauseType.SELECT, ClauseType.ORDER_BY);
        assertClauses(r, "orders.amount", ClauseType.WHERE);
    }

    @Test
    void starAndSemiJoinVisibility() {
        SqlParseResult r = success("SELECT * FROM orders o LEFT SEMI JOIN users u ON o.user_id = u.user_id");
        assertEquals(6, r.getColumnUsages().size(), "5 orders columns + users.user_id from ON");
        assertClauses(r, "users.user_id", ClauseType.JOIN_ON);
        assertClauses(r, "orders.tags", ClauseType.SELECT);
        assertFalse(r.usageOf(c("users.city")).isPresent(), "right side of a LEFT SEMI JOIN is not in *");
    }

    @Test
    void structSubfieldCaseWhenAndAggregatesParsedAsSimpleFunctions() {
        SqlParseResult r = success("SELECT u.info.zip, CASE WHEN city = 'a' THEN name ELSE 'z' END, "
                + "approx_count_distinct(user_id) FROM users u");
        assertClauses(r, "users.info", ClauseType.SELECT);
        assertClauses(r, "users.city", ClauseType.SELECT);
        assertClauses(r, "users.name", ClauseType.SELECT);
        assertClauses(r, "users.user_id", ClauseType.SELECT);
    }

    /** Iron rule #2: inner alias t shadows outer alias t; nested WITH shadows the outer CTE of the same name. */
    @Test
    void shadowedAliasesAndCtesEndToEnd() {
        SqlParseResult r = success("WITH c AS (SELECT order_id AS a FROM orders) "
                + "SELECT t.a FROM c t WHERE EXISTS ("
                + "  WITH c AS (SELECT user_id AS a FROM users) SELECT 1 FROM c t WHERE t.a = 1)");
        assertClauses(r, "orders.order_id", ClauseType.SELECT);
        assertClauses(r, "users.user_id", ClauseType.SELECT, ClauseType.WHERE);
        assertEquals(2, r.getColumnUsages().size());
    }

    @Test
    void explainInsideMultiStatementTextIsSkippedByTheVisitor() {
        SqlParseResult r = success("EXPLAIN SELECT amount FROM orders; SELECT city FROM users");
        assertEquals(1, r.getColumnUsages().size());
        assertClauses(r, "users.city", ClauseType.SELECT);
    }

    @Test
    void externalCatalogThreePartNames() {
        SqlParseResult r = success("SELECT e.payload FROM HIVE.ODS.EVENTS e WHERE e.user_id = 1");
        assertClauses(r, "hive.ods.events.payload", ClauseType.SELECT);
        assertClauses(r, "hive.ods.events.user_id", ClauseType.WHERE);
        assertTrue(r.getSourceTables().contains(c("hive.ods.events.x").getTable()));
    }
}
