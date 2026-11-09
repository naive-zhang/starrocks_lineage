package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.ColumnUsageStat;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.pipeline.HourlyUsageAggregator;
import com.starrocks.lineage.pipeline.HourlyUsageRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage 4 acceptance suite: eight high-difficulty SQL cases with exact assertions on every lineage edge and every
 * clause counter.
 */
class StarRocksSqlAnalyzerTest {

    private static final InMemoryMetadataProvider CATALOG = new InMemoryMetadataProvider()
            .register("default_catalog.dw.ods_orders", "order_id", "user_id", "shop_id", "amount", "status", "dt")
            .register("default_catalog.dw.ods_users", "user_id", "user_name", "city", "tags", "scores", "weights",
                    "x")
            .register("default_catalog.dw.ods_shops", "shop_id", "shop_name", "region")
            .register("default_catalog.dw.ads_user_rank", "user_id", "region", "total_amount", "rk", "src")
            .register("default_catalog.dw.ads_orders_copy", "oid", "uid", "sid", "amt", "st", "d")
            .register("default_catalog.dw.ads_user_tags", "user_id", "tag", "score")
            .register("default_catalog.dw.ads_user_score", "user_id", "weighted");

    private final StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(CATALOG);

    // ---------------------------------------------------------------------------------------------------- helpers

    private static AuditLogRecord.Builder audit(String stmt) {
        return AuditLogRecord.builder().queryId("q-" + Math.abs(stmt.hashCode()))
                .timestamp(LocalDateTime.of(2024, 6, 18, 9, 41, 7)).user("analyst").catalog("default_catalog")
                .db("dw").state("EOF").errorCode("").queryTime(1200).scanBytes(4096).stmt(stmt);
    }

    private SqlParseResult run(String sql) {
        SqlParseResult r = analyzer.analyze(audit(sql).build());
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> "unexpected: " + r);
        assertFalse(r.isDegraded(), () -> "unexpected warnings: " + r.getWarnings());
        return r;
    }

    private static ColumnIdentifier col(String tableDotColumn) {
        return ColumnIdentifier.parse("default_catalog.dw." + tableDotColumn);
    }

    /** "target <- source : TRANSFORM" for every DIRECT edge. */
    private static Set<String> directEdges(SqlParseResult r) {
        return r.getLineageEdges().stream().filter(e -> e.getLineageType() == LineageType.DIRECT)
                .map(StarRocksSqlAnalyzerTest::render).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static String render(ColumnLineageEdge e) {
        String src = e.getSource().isConstant() ? "CONSTANT"
                : e.getSource().getTableName() + "." + e.getSource().getColumn();
        return e.getTarget().getTableName() + "." + e.getTarget().getColumn() + " <- " + src + " : "
                + e.getTransformType();
    }

    /** Sources of every INDIRECT edge reaching {@code target}. */
    private static Set<String> indirectSources(SqlParseResult r, String target) {
        ColumnIdentifier t = col(target);
        return r.getLineageEdges().stream()
                .filter(e -> e.getLineageType() == LineageType.INDIRECT && e.getTarget().equals(t))
                .map(e -> e.getSource().getTableName() + "." + e.getSource().getColumn())
                .collect(Collectors.toCollection(HashSet::new));
    }

    private static Set<String> set(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    private static void assertUsage(SqlParseResult r, String column, long select, long where, long join,
                                    long groupBy, long orderBy) {
        ColumnUsageStat s = r.usageOf(col(column))
                .orElseThrow(() -> new AssertionError("no usage of " + column + ": " + r.getColumnUsages()));
        assertEquals(1, s.getAccessQueryCnt(), column + " access_query_cnt");
        assertEquals(select, s.getSelectClauseCnt(), column + " select_clause_cnt");
        assertEquals(where, s.getWhereClauseCnt(), column + " where_clause_cnt");
        assertEquals(join, s.getJoinClauseCnt(), column + " join_clause_cnt");
        assertEquals(groupBy, s.getGroupByCnt(), column + " group_by_cnt");
        assertEquals(orderBy, s.getOrderByCnt(), column + " order_by_cnt");
    }

    // ---------------------------------------------------------------------------------------------------- case 1

    @Test
    @DisplayName("1. multi-level CTE + window + UNION ALL into INSERT without column list")
    void case1MultiLevelCteWindowUnionAll() {
        SqlParseResult r = run("INSERT INTO dw.ads_user_rank\n"
                + "WITH paid AS (\n"
                + "  SELECT o.user_id, s.region, o.amount FROM ods_orders o JOIN ods_shops s ON o.shop_id = s.shop_id\n"
                + "  WHERE o.status = 'PAID'),\n"
                + "agg AS (SELECT user_id, region, SUM(amount) AS total FROM paid GROUP BY user_id, region),\n"
                + "ranked AS (SELECT user_id, region, total,\n"
                + "           RANK() OVER (PARTITION BY region ORDER BY total DESC) AS rk FROM agg)\n"
                + "SELECT user_id, region, total, rk, 'online' AS src FROM ranked WHERE rk <= 10\n"
                + "UNION ALL\n"
                + "SELECT u.user_id, u.city, 0, 0, 'offline' FROM ods_users u");

        assertEquals(StatementType.INSERT_INTO, r.getStatementType().get());
        assertEquals(col("ads_user_rank.x").getTable(), r.getTargetTable().get());
        assertEquals(set(
                "ads_user_rank.user_id <- ods_orders.user_id : IDENTITY",
                "ads_user_rank.user_id <- ods_users.user_id : IDENTITY",
                "ads_user_rank.region <- ods_shops.region : IDENTITY",
                "ads_user_rank.region <- ods_users.city : IDENTITY",
                "ads_user_rank.total_amount <- ods_orders.amount : AGGREGATION",
                "ads_user_rank.rk <- CONSTANT : CONSTANT",
                "ads_user_rank.src <- CONSTANT : CONSTANT"), directEdges(r),
                "positional UNION ALL merge across differently named branches");

        Set<String> rowControl = set("ods_orders.status", "ods_orders.shop_id", "ods_shops.shop_id",
                "ods_orders.user_id", "ods_shops.region", "ods_orders.amount");
        for (String target : Arrays.asList("ads_user_rank.user_id", "ads_user_rank.region",
                "ads_user_rank.total_amount", "ads_user_rank.src")) {
            assertEquals(rowControl, indirectSources(r, target), target);
        }
        assertEquals(rowControl, indirectSources(r, "ads_user_rank.rk"),
                "rk: window PARTITION BY region / ORDER BY total plus the row control");
        assertEquals(new HashSet<>(Arrays.asList(col("ods_orders.x").getTable(), col("ods_shops.x").getTable(),
                col("ods_users.x").getTable(), col("ads_user_rank.x").getTable())),
                new HashSet<>(r.getSourceTables()), "CTE names paid / agg / ranked are not tables");
        assertTrue(r.getColumnUsages().isEmpty());
    }

    // ---------------------------------------------------------------------------------------------------- case 2

    @Test
    @DisplayName("2. INSERT without column list from SELECT t1.* aligns by position with the target schema")
    void case2InsertWithoutColumnListFromQualifiedStar() {
        SqlParseResult r = run("INSERT INTO ads_orders_copy SELECT t1.* FROM ods_orders t1 "
                + "JOIN ods_users u ON t1.user_id = u.user_id WHERE u.city = 'Hangzhou'");
        assertEquals(set(
                "ads_orders_copy.oid <- ods_orders.order_id : IDENTITY",
                "ads_orders_copy.uid <- ods_orders.user_id : IDENTITY",
                "ads_orders_copy.sid <- ods_orders.shop_id : IDENTITY",
                "ads_orders_copy.amt <- ods_orders.amount : IDENTITY",
                "ads_orders_copy.st <- ods_orders.status : IDENTITY",
                "ads_orders_copy.d <- ods_orders.dt : IDENTITY"), directEdges(r),
                "t1.* expands in ods_orders schema order and the target names are irrelevant");
        for (String t : Arrays.asList("oid", "uid", "sid", "amt", "st", "d")) {
            assertEquals(set("ods_orders.user_id", "ods_users.user_id", "ods_users.city"),
                    indirectSources(r, "ads_orders_copy." + t), t);
        }
        assertEquals(6 + 18, r.getLineageEdges().size());
    }

    // ---------------------------------------------------------------------------------------------------- case 3

    @Test
    @DisplayName("3. LATERAL UNNEST virtual columns resolve to the physical arrays")
    void case3LateralUnnest() {
        SqlParseResult r = run("INSERT INTO ads_user_tags (user_id, tag, score) "
                + "SELECT u.user_id, t.tag, t.score FROM ods_users u, "
                + "LATERAL UNNEST(u.tags, u.scores) AS t(tag, score) "
                + "WHERE t.score > 0.5");
        assertEquals(set(
                "ads_user_tags.user_id <- ods_users.user_id : IDENTITY",
                "ads_user_tags.tag <- ods_users.tags : EXPRESSION",
                "ads_user_tags.score <- ods_users.scores : EXPRESSION"), directEdges(r));
        for (String t : Arrays.asList("user_id", "tag", "score")) {
            assertEquals(set("ods_users.scores"), indirectSources(r, "ads_user_tags." + t), t);
        }

        SqlParseResult q = run("SELECT t.unnest FROM ods_users u, UNNEST(u.tags) AS t WHERE u.city = 'x'");
        assertUsage(q, "ods_users.tags", 1, 0, 0, 0, 0);
        assertEquals(EnumSet.of(ClauseType.TABLE_FUNCTION, ClauseType.SELECT),
                q.usageOf(col("ods_users.tags")).get().getClauses());
        assertUsage(q, "ods_users.city", 0, 1, 0, 0, 0);
        assertEquals(2, q.getColumnUsages().size(), "no phantom column for the virtual 'unnest'");
    }

    // ---------------------------------------------------------------------------------------------------- case 4

    @Test
    @DisplayName("4. lambda parameters of array_map shadow the physical column x and bind to the array arguments")
    void case4LambdaArrayMap() {
        SqlParseResult r = run("INSERT INTO ads_user_score "
                + "SELECT u.user_id, array_sum(array_map((x, y) -> x * y, u.scores, u.weights)) FROM ods_users u");
        assertEquals(set(
                "ads_user_score.user_id <- ods_users.user_id : IDENTITY",
                "ads_user_score.weighted <- ods_users.scores : EXPRESSION",
                "ads_user_score.weighted <- ods_users.weights : EXPRESSION"), directEdges(r),
                "ods_users.x must never appear: x is the lambda parameter");
        assertEquals(3, r.getLineageEdges().size());

        SqlParseResult q = run("SELECT array_map(x -> x + 1, scores), array_filter(weights, x -> x > 0), x "
                + "FROM ods_users");
        assertEquals(1, q.usageOf(col("ods_users.x")).get().occurrences(ClauseType.SELECT),
                "only the trailing projection reads the physical x");
        assertUsage(q, "ods_users.scores", 1, 0, 0, 0, 0);
        assertUsage(q, "ods_users.weights", 1, 0, 0, 0, 0);
        assertEquals(3, q.getColumnUsages().size());
    }

    // ---------------------------------------------------------------------------------------------------- case 5

    @Test
    @DisplayName("5. WHERE [NOT] EXISTS correlated sub-queries count inner SELECT / WHERE per clause")
    void case5CorrelatedExists() {
        SqlParseResult r = run("SELECT o.order_id, o.amount FROM ods_orders o\n"
                + "WHERE o.status = 'PAID'\n"
                + "  AND EXISTS (SELECT s.shop_name FROM ods_shops s "
                + "WHERE s.shop_id = o.shop_id AND s.region = 'EAST')\n"
                + "  AND NOT EXISTS (SELECT 1 FROM ods_users u WHERE u.user_id = o.user_id AND u.city IN ('A', 'B'))");
        assertEquals(StatementType.QUERY, r.getStatementType().get());
        assertTrue(r.getLineageEdges().isEmpty());
        assertEquals(10, r.getColumnUsages().size());
        assertUsage(r, "ods_orders.order_id", 1, 0, 0, 0, 0);
        assertUsage(r, "ods_orders.amount", 1, 0, 0, 0, 0);
        assertUsage(r, "ods_orders.status", 0, 1, 0, 0, 0);
        assertUsage(r, "ods_shops.shop_name", 1, 0, 0, 0, 0);
        assertUsage(r, "ods_shops.shop_id", 0, 1, 0, 0, 0);
        assertUsage(r, "ods_orders.shop_id", 0, 1, 0, 0, 0);
        assertUsage(r, "ods_shops.region", 0, 1, 0, 0, 0);
        assertUsage(r, "ods_users.user_id", 0, 1, 0, 0, 0);
        assertUsage(r, "ods_orders.user_id", 0, 1, 0, 0, 0);
        assertUsage(r, "ods_users.city", 0, 1, 0, 0, 0);
    }

    // ---------------------------------------------------------------------------------------------------- case 6

    @Test
    @DisplayName("6. CREATE MATERIALIZED VIEW with column list, join, aggregation and refresh clause")
    void case6CreateMaterializedView() {
        SqlParseResult r = run("CREATE MATERIALIZED VIEW IF NOT EXISTS dw.mv_region_daily "
                + "(region, dt, gmv, buyers, avg_amt) "
                + "DISTRIBUTED BY HASH(region) BUCKETS 4 REFRESH ASYNC EVERY (INTERVAL 1 HOUR) "
                + "PROPERTIES (\"replication_num\" = \"1\") AS "
                + "SELECT s.region, o.dt, SUM(o.amount), COUNT(DISTINCT o.user_id), SUM(o.amount) / COUNT(*) "
                + "FROM ods_orders o JOIN ods_shops s ON o.shop_id = s.shop_id "
                + "WHERE o.status <> 'CANCELLED' GROUP BY s.region, o.dt");
        assertEquals(StatementType.CREATE_MATERIALIZED_VIEW, r.getStatementType().get());
        assertEquals(col("mv_region_daily.x").getTable(), r.getTargetTable().get());
        assertEquals(set(
                "mv_region_daily.region <- ods_shops.region : IDENTITY",
                "mv_region_daily.dt <- ods_orders.dt : IDENTITY",
                "mv_region_daily.gmv <- ods_orders.amount : AGGREGATION",
                "mv_region_daily.buyers <- ods_orders.user_id : AGGREGATION",
                "mv_region_daily.avg_amt <- ods_orders.amount : AGGREGATION"), directEdges(r));
        for (String t : Arrays.asList("region", "dt", "gmv", "buyers", "avg_amt")) {
            assertEquals(set("ods_orders.shop_id", "ods_shops.shop_id", "ods_orders.status", "ods_shops.region",
                    "ods_orders.dt"), indirectSources(r, "mv_region_daily." + t), t);
        }
        assertEquals("SUM(o.amount) / COUNT(*)", r.getLineageEdges().stream()
                .filter(e -> e.getTarget().getColumn().equals("avg_amt") && e.getLineageType() == LineageType.DIRECT)
                .findFirst().get().getTransformExpr());
    }

    // ---------------------------------------------------------------------------------------------------- case 7

    @Test
    @DisplayName("7. one query referencing the same column in many clauses counts it once per query and clause")
    void case7PerQueryDeduplication() {
        String sql = "SELECT o.amount, o.amount + 1, ROUND(o.amount * 0.9, 2) AS discounted\n"
                + "FROM ods_orders o JOIN ods_shops s ON o.shop_id = s.shop_id AND o.amount > 0\n"
                + "WHERE o.amount > 10 AND o.amount BETWEEN 10 AND 100\n"
                + "ORDER BY o.amount DESC, discounted";
        SqlParseResult r = run(sql);
        assertUsage(r, "ods_orders.amount", 1, 1, 1, 0, 1);
        ColumnUsageStat amount = r.usageOf(col("ods_orders.amount")).get();
        assertEquals(3, amount.occurrences(ClauseType.SELECT));
        assertEquals(2, amount.occurrences(ClauseType.WHERE), "o.amount > 10 AND o.amount BETWEEN ...");
        assertEquals(1, amount.occurrences(ClauseType.JOIN_ON));
        assertEquals(2, amount.occurrences(ClauseType.ORDER_BY), "o.amount and the alias 'discounted'");
        assertUsage(r, "ods_orders.shop_id", 0, 0, 1, 0, 0);
        assertUsage(r, "ods_shops.shop_id", 0, 0, 1, 0, 0);
        assertEquals(3, r.getColumnUsages().size());

        // hourly pre-aggregation: two executions by two users in the same hour, one by alice an hour later
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        agg.add(audit(sql).user("alice").queryTime(100).scanBytes(1000).build(), r);
        agg.add(audit(sql).user("bob").queryTime(300).scanBytes(2000)
                .timestamp(LocalDateTime.of(2024, 6, 18, 9, 59, 59)).build(), r);
        agg.add(audit(sql).user("alice").queryTime(50).scanBytes(10)
                .timestamp(LocalDateTime.of(2024, 6, 18, 10, 0, 0)).build(), r);
        List<HourlyUsageRow> rows = agg.drain().stream()
                .filter(u -> u.getColumn().equals(col("ods_orders.amount"))).collect(Collectors.toList());
        assertEquals(3, rows.size(), "one row per (hour, column, user)");
        HourlyUsageRow alice9 = rows.get(0);
        assertEquals(LocalDateTime.of(2024, 6, 18, 9, 0), alice9.getStatHour());
        assertEquals("alice", alice9.getUserName());
        assertEquals(1, alice9.getAccessQueryCnt());
        assertEquals(1, alice9.getSelectClauseCnt());
        assertEquals(1, alice9.getWhereClauseCnt());
        assertEquals(1, alice9.getJoinClauseCnt());
        assertEquals(0, alice9.getGroupByCnt());
        assertEquals(1, alice9.getOrderByCnt());
        assertEquals(1000, alice9.getTotalScanBytes());
        HourlyUsageRow bob9 = rows.get(1);
        assertEquals("bob", bob9.getUserName());
        assertEquals(300, bob9.getMaxQueryTimeMs());
        assertEquals(LocalDateTime.of(2024, 6, 18, 10, 0), rows.get(2).getStatHour());
        assertEquals(2, rows.stream().filter(u -> u.getStatHour().getHour() == 9)
                .mapToLong(HourlyUsageRow::getAccessQueryCnt).sum(), "hour 9: 2 queries");
    }

    // ---------------------------------------------------------------------------------------------------- case 8

    @Test
    @DisplayName("8. dirty driver comments are cleaned and failed / management statements are filtered")
    void case8DirtyCommentsAndFilters() {
        SqlParseResult r = run("/* ApplicationName=DBeaver 23.1.0 - SQLEditor <Script-1.sql> */ /* trace=7 */\n"
                + "-- generated by BI tool\n"
                + "SELECT /*+ SET_VAR(query_timeout = 600) */ city # trailing mysql comment\n"
                + "FROM ods_users WHERE user_name = 'a--b/*c*/#d' ;;");
        assertEquals(2, r.getColumnUsages().size());
        assertUsage(r, "ods_users.city", 1, 0, 0, 0, 0);
        assertUsage(r, "ods_users.user_name", 0, 1, 0, 0, 0);

        assertEquals(ParseStatus.SKIPPED_FAILED_QUERY,
                analyzer.analyze(audit("SELECT city FROM ods_users").state("ERR").build()).getStatus());
        assertEquals(ParseStatus.SKIPPED_FAILED_QUERY,
                analyzer.analyze(audit("SELECT city FROM ods_users").errorCode("1064").build()).getStatus());
        assertEquals(ParseStatus.SKIPPED_EMPTY,
                analyzer.analyze(audit("/* ApplicationName=DataGrip */ -- nothing\n ;").build()).getStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SHOW TABLES", "/* ApplicationName=DBeaver */ show databases", "DESC ods_orders", "DESCRIBE ods_users",
        "SET query_timeout = 300", "/* JDBC */ SET NAMES utf8mb4", "USE dw", "ADMIN SHOW FRONTEND CONFIG",
        "EXPLAIN SELECT city FROM ods_users", "explain costs insert into ads_user_score select 1, 2", "KILL 42",
        "KILL QUERY 'abc'"
    })
    @DisplayName("8b. management statements are skipped without parsing")
    void case8ManagementStatements(String stmt) {
        SqlParseResult r = analyzer.analyze(audit(stmt).build());
        assertEquals(ParseStatus.SKIPPED_MANAGEMENT, r.getStatus(), stmt);
        assertTrue(r.getLineageEdges().isEmpty() && r.getColumnUsages().isEmpty());
    }
}
