package com.starrocks.lineage.it;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.JdbcStarRocksMetadataProvider;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.scope.RelationDescriptor;
import com.starrocks.lineage.scope.ResolvedColumn;
import com.starrocks.lineage.scope.ScopeStack;
import com.starrocks.lineage.scope.StarExpansion;
import com.starrocks.sql.parser.SqlSanitizer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential tests: the real StarRocks 3.1 engine is the oracle for the analyzer.
 * <ol>
 *   <li>every acceptance SQL of {@code StarRocksSqlAnalyzerTest} is valid StarRocks 3.1 (it is executed) and is
 *       analysed with the <b>real</b> metadata ({@link JdbcStarRocksMetadataProvider});</li>
 *   <li>output column names / counts derived by the analyzer equal those StarRocks gives to a view over the same
 *       query, and {@code SELECT *} expansion equals the result set metadata of the engine.</li>
 * </ol>
 */
@Testcontainers(disabledWithoutDocker = true)
class AnalyzerDifferentialIT {

    private static final AtomicInteger VIEW_SEQ = new AtomicInteger();
    private static StarRocksCluster sr;
    private static JdbcStarRocksMetadataProvider metadata;
    private static StarRocksSqlAnalyzer analyzer;
    private static final NamespaceContext DW = NamespaceContext.of("default_catalog", "dw");

    @BeforeAll
    static void schema() {
        sr = StarRocksCluster.get();
        String props = " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")";
        sr.execute("DROP DATABASE IF EXISTS dw FORCE", "CREATE DATABASE dw",
                "CREATE TABLE dw.ods_orders (order_id BIGINT, user_id BIGINT, shop_id BIGINT, amount DECIMAL(18, 2), "
                        + "status VARCHAR(16), dt DATE)" + props.replace("(k)", "(order_id)"),
                "CREATE TABLE dw.ods_users (user_id BIGINT, user_name VARCHAR(32), city VARCHAR(32), "
                        + "tags ARRAY<VARCHAR(16)>, scores ARRAY<DOUBLE>, weights ARRAY<DOUBLE>, x INT)"
                        + props.replace("(k)", "(user_id)"),
                "CREATE TABLE dw.ods_shops (shop_id BIGINT, shop_name VARCHAR(32), region VARCHAR(16))"
                        + props.replace("(k)", "(shop_id)"),
                "CREATE TABLE dw.ads_user_rank (user_id BIGINT, region VARCHAR(32), total_amount DECIMAL(38, 2), "
                        + "rk BIGINT, src VARCHAR(16))" + props.replace("(k)", "(user_id)"),
                "CREATE TABLE dw.ads_orders_copy (oid BIGINT, uid BIGINT, sid BIGINT, amt DECIMAL(18, 2), "
                        + "st VARCHAR(16), d DATE)" + props.replace("(k)", "(oid)"),
                "CREATE TABLE dw.ads_user_tags (user_id BIGINT, tag VARCHAR(16), score DOUBLE)"
                        + props.replace("(k)", "(user_id)"),
                "CREATE TABLE dw.ads_user_score (user_id BIGINT, weighted DOUBLE)" + props.replace("(k)", "(user_id)"),
                "INSERT INTO dw.ods_orders VALUES (1, 10, 100, 25.00, 'PAID', '2024-06-01'), "
                        + "(2, 11, 101, 500.00, 'PAID', '2024-06-02'), (3, 10, 100, 7.50, 'CANCELLED', '2024-06-02')",
                "INSERT INTO dw.ods_users VALUES (10, 'a', 'Hangzhou', ['t1', 't2'], [0.9, 0.2], [1.0, 2.0], 1), "
                        + "(11, 'b', 'A', ['t3'], [0.7], [3.0], 2)",
                "INSERT INTO dw.ods_shops VALUES (100, 's1', 'EAST'), (101, 's2', 'WEST')");
        metadata = new JdbcStarRocksMetadataProvider(sr.dataSource());
        analyzer = new StarRocksSqlAnalyzer(metadata);
    }

    private static SqlParseResult analyze(String sql) {
        SqlParseResult r = analyzer.analyze("it", sql, DW);
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> "analyzer: " + r);
        assertTrue(r.getWarnings().isEmpty(), () -> "real metadata must be complete: " + r.getWarnings());
        return r;
    }

    private static Set<String> direct(SqlParseResult r) {
        return r.getLineageEdges().stream().filter(e -> e.getLineageType() == LineageType.DIRECT)
                .map(e -> e.getTarget().getTableName() + "." + e.getTarget().getColumn() + " <- "
                        + (e.getSource().isConstant() ? "CONSTANT" : e.getSource().getTableName() + "."
                        + e.getSource().getColumn()) + " : " + e.getTransformType())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Set<String> set(String... s) {
        return new LinkedHashSet<>(Arrays.asList(s));
    }

    // ------------------------------------------------------------------------------------------------ 1. corpus

    @Test
    void acceptanceCase1CteWindowUnionAllRunsAndMatches() {
        String sql = "INSERT INTO dw.ads_user_rank\n"
                + "WITH paid AS (\n"
                + "  SELECT o.user_id, s.region, o.amount FROM ods_orders o JOIN ods_shops s ON o.shop_id = s.shop_id\n"
                + "  WHERE o.status = 'PAID'),\n"
                + "agg AS (SELECT user_id, region, SUM(amount) AS total FROM paid GROUP BY user_id, region),\n"
                + "ranked AS (SELECT user_id, region, total,\n"
                + "           RANK() OVER (PARTITION BY region ORDER BY total DESC) AS rk FROM agg)\n"
                + "SELECT user_id, region, total, rk, 'online' AS src FROM ranked WHERE rk <= 10\n"
                + "UNION ALL\n"
                + "SELECT u.user_id, u.city, 0, 0, 'offline' FROM ods_users u";
        executeInDw(sql);
        assertEquals(4L, count("dw.ads_user_rank"), "2 paid user/region groups + 2 users");
        assertEquals(set(
                "ads_user_rank.user_id <- ods_orders.user_id : IDENTITY",
                "ads_user_rank.user_id <- ods_users.user_id : IDENTITY",
                "ads_user_rank.region <- ods_shops.region : IDENTITY",
                "ads_user_rank.region <- ods_users.city : IDENTITY",
                "ads_user_rank.total_amount <- ods_orders.amount : AGGREGATION",
                "ads_user_rank.rk <- CONSTANT : CONSTANT",
                "ads_user_rank.src <- CONSTANT : CONSTANT"), direct(analyze(sql)));
    }

    @Test
    void acceptanceCase2QualifiedStarIntoDifferentlyNamedTargetRunsAndMatches() {
        String sql = "INSERT INTO ads_orders_copy SELECT t1.* FROM ods_orders t1 "
                + "JOIN ods_users u ON t1.user_id = u.user_id WHERE u.city = 'Hangzhou'";
        executeInDw(sql);
        assertEquals(2L, count("dw.ads_orders_copy"));
        assertEquals(set(
                "ads_orders_copy.oid <- ods_orders.order_id : IDENTITY",
                "ads_orders_copy.uid <- ods_orders.user_id : IDENTITY",
                "ads_orders_copy.sid <- ods_orders.shop_id : IDENTITY",
                "ads_orders_copy.amt <- ods_orders.amount : IDENTITY",
                "ads_orders_copy.st <- ods_orders.status : IDENTITY",
                "ads_orders_copy.d <- ods_orders.dt : IDENTITY"), direct(analyze(sql)));
    }

    @Test
    void acceptanceCase3And4UnnestAndLambdaRunAndMatch() {
        String unnest = "INSERT INTO ads_user_tags (user_id, tag, score) "
                + "SELECT u.user_id, t.tag, t.score FROM ods_users u, "
                + "LATERAL UNNEST(u.tags, u.scores) AS t(tag, score) WHERE t.score > 0.5";
        executeInDw(unnest);
        assertEquals(2L, count("dw.ads_user_tags"), "(a, t1, 0.9) and (b, t3, 0.7)");
        assertEquals(set(
                "ads_user_tags.user_id <- ods_users.user_id : IDENTITY",
                "ads_user_tags.tag <- ods_users.tags : EXPRESSION",
                "ads_user_tags.score <- ods_users.scores : EXPRESSION"), direct(analyze(unnest)));

        String lambda = "INSERT INTO ads_user_score "
                + "SELECT u.user_id, array_sum(array_map((x, y) -> x * y, u.scores, u.weights)) FROM ods_users u";
        executeInDw(lambda);
        List<Map<String, Object>> rows = sr.query("SELECT weighted FROM dw.ads_user_score ORDER BY user_id");
        assertEquals(1.3, ((Number) rows.get(0).get("weighted")).doubleValue(), 1e-9, "0.9*1 + 0.2*2");
        assertEquals(set(
                "ads_user_score.user_id <- ods_users.user_id : IDENTITY",
                "ads_user_score.weighted <- ods_users.scores : EXPRESSION",
                "ads_user_score.weighted <- ods_users.weights : EXPRESSION"), direct(analyze(lambda)));
    }

    @Test
    void acceptanceCase5And7QueriesRunAndCountUsage() {
        String exists = "SELECT o.order_id, o.amount FROM ods_orders o\n"
                + "WHERE o.status = 'PAID'\n"
                + "  AND EXISTS (SELECT s.shop_name FROM ods_shops s "
                + "WHERE s.shop_id = o.shop_id AND s.region = 'EAST')\n"
                + "  AND NOT EXISTS (SELECT 1 FROM ods_users u WHERE u.user_id = o.user_id AND u.city IN ('A', 'B'))";
        assertEquals(1, queryInDw(exists).size(), "order 1 only");
        assertEquals(10, analyze(exists).getColumnUsages().size());

        String dedup = "SELECT o.amount, o.amount + 1, ROUND(o.amount * 0.9, 2) AS discounted\n"
                + "FROM ods_orders o JOIN ods_shops s ON o.shop_id = s.shop_id AND o.amount > 0\n"
                + "WHERE o.amount > 10 AND o.amount BETWEEN 10 AND 100\n"
                + "ORDER BY o.amount DESC, discounted";
        assertEquals(1, queryInDw(dedup).size());
        SqlParseResult r = analyze(dedup);
        assertEquals(1, r.usageOf(ColumnIdentifier.parse("default_catalog.dw.ods_orders.amount")).get()
                .getAccessQueryCnt());
    }

    @Test
    void acceptanceCase6MaterializedViewIsCreatedAndMatches() {
        String sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS dw.mv_region_daily "
                + "(region, dt, gmv, buyers, avg_amt) "
                + "DISTRIBUTED BY HASH(region) BUCKETS 4 REFRESH ASYNC EVERY (INTERVAL 1 HOUR) "
                + "PROPERTIES (\"replication_num\" = \"1\") AS "
                + "SELECT s.region, o.dt, SUM(o.amount), COUNT(DISTINCT o.user_id), SUM(o.amount) / COUNT(*) "
                + "FROM ods_orders o JOIN ods_shops s ON o.shop_id = s.shop_id "
                + "WHERE o.status <> 'CANCELLED' GROUP BY s.region, o.dt";
        executeInDw(sql);
        assertEquals(Arrays.asList("region", "dt", "gmv", "buyers", "avg_amt"), realColumns("mv_region_daily"));
        assertEquals(set(
                "mv_region_daily.region <- ods_shops.region : IDENTITY",
                "mv_region_daily.dt <- ods_orders.dt : IDENTITY",
                "mv_region_daily.gmv <- ods_orders.amount : AGGREGATION",
                "mv_region_daily.buyers <- ods_orders.user_id : AGGREGATION",
                "mv_region_daily.avg_amt <- ods_orders.amount : AGGREGATION"), direct(analyze(sql)));
    }

    @Test
    void acceptanceCase8SanitizedStatementRunsWhileRawMysqlCommentIsRejectedByStarRocks() {
        String raw = "/* ApplicationName=DBeaver 23.1.0 - SQLEditor <Script-1.sql> */ /* trace=7 */\n"
                + "-- generated by BI tool\n"
                + "SELECT /*+ SET_VAR(query_timeout = 600) */ city # trailing mysql comment\n"
                + "FROM ods_users WHERE user_name = 'a--b/*c*/#d'";
        String sanitized = SqlSanitizer.sanitize(raw).getSql();
        assertEquals(0, queryInDw(sanitized).size());
        assertEquals(2, analyze(raw).getColumnUsages().size());
    }

    // ------------------------------------------------------------------------------------------------ 2. naming

    /**
     * The analyzer's target columns of {@code CREATE VIEW v AS <query>} equal, in order, the columns StarRocks
     * gives to that view.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "unaliased aggregates | SELECT city, COUNT(*), SUM(user_id), MAX(x) FROM ods_users GROUP BY city",
        "column refs and struct-free exprs | SELECT u.city, u.user_id + 1, upper(u.user_name) FROM ods_users u",
        "default unnest column | SELECT u.user_id, t.unnest FROM ods_users u, UNNEST(u.tags) AS t",
        "unnest star | SELECT * FROM ods_users u, UNNEST(u.tags)",
        "set operation names | SELECT order_id AS k, amount FROM ods_orders UNION ALL SELECT user_id, x FROM ods_users",
        "cte column aliases | WITH c (a, b) AS (SELECT order_id, amount FROM ods_orders) SELECT * FROM c",
        "derived column aliases | SELECT * FROM (SELECT order_id, amount FROM ods_orders) t (a, b)",
        "values default names | SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS v",
        "qualified star plus column | SELECT o.*, u.city FROM ods_orders o JOIN ods_users u ON o.user_id = u.user_id",
        "left semi join star | SELECT * FROM ods_orders o LEFT SEMI JOIN ods_users u ON o.user_id = u.user_id",
        "right anti join star | SELECT * FROM ods_users u RIGHT ANTI JOIN ods_orders o ON o.user_id = u.user_id",
        "window and case | SELECT user_id, ROW_NUMBER() OVER (ORDER BY amount), "
            + "CASE WHEN amount > 1 THEN 1 END FROM ods_orders"
    })
    void viewColumnsDerivedByTheAnalyzerEqualStarRocks(String name, String query) {
        String view = "v_diff_" + VIEW_SEQ.incrementAndGet();
        String ddl = "CREATE VIEW dw." + view + " AS " + query;
        executeInDw(ddl);
        List<String> real = realColumns(view);
        SqlParseResult r = analyze(ddl);
        List<String> ours = new ArrayList<>();
        for (ColumnLineageEdge e : r.getLineageEdges()) {
            String c = e.getTarget().getColumn();
            if (!ours.contains(c)) {
                ours.add(c);
            }
        }
        assertEquals(real, ours, name);
    }

    /** {@code SELECT *} with JOIN USING: StarRocks 3.1 does not coalesce, both join columns are output. */
    @Test
    void starOverUsingJoinMatchesTheEngineResultSet() throws SQLException {
        String sql = "SELECT * FROM dw.ods_orders o JOIN dw.ods_users u USING (user_id)";
        List<String> real = resultColumns(sql + " LIMIT 0");
        ScopeStack stack = new ScopeStack(DW, metadata);
        try (ScopeStack.Frame f = stack.pushQueryBlock()) {
            RelationDescriptor o = stack.resolveRelation(Arrays.asList("dw", "ods_orders"), "o");
            RelationDescriptor u = stack.resolveRelation(Arrays.asList("dw", "ods_users"), "u");
            f.scope().addRelation(o);
            f.scope().addRelation(u);
            f.scope().addUsingColumns(Collections.singletonList("user_id"));
            StarExpansion star = stack.expandStar(Collections.emptyList());
            List<String> ours = star.getColumns().stream().map(ResolvedColumn::getField)
                    .map(fld -> fld.getName()).collect(Collectors.toList());
            assertEquals(real, ours);
            assertEquals(13, ours.size(), "6 + 7 columns, user_id twice");
        }
    }

    // ------------------------------------------------------------------------------------------------ 3. metadata

    @Test
    void metadataProviderReadsTheRealInformationSchema() {
        TableIdentifier orders = TableIdentifier.of("default_catalog", "dw", "ods_orders");
        assertEquals(Arrays.asList("order_id", "user_id", "shop_id", "amount", "status", "dt"),
                metadata.getColumnNames(orders));
        assertTrue(metadata.getTableSchema(orders).get().getColumns().get(3).getDataType().toLowerCase(Locale.ROOT)
                .contains("decimal"));
        assertTrue(!metadata.tableExists(TableIdentifier.of("default_catalog", "dw", "missing")));
        assertTrue(!metadata.tableExists(TableIdentifier.of("default_catalog", "no_db", "missing")));

        sr.execute("CREATE TABLE IF NOT EXISTS dw.evolving (a INT, b INT) DISTRIBUTED BY HASH(a) BUCKETS 1 "
                + "PROPERTIES (\"replication_num\" = \"1\")");
        TableIdentifier evolving = TableIdentifier.of("default_catalog", "dw", "evolving");
        assertEquals(Arrays.asList("a", "b"), metadata.getColumnNames(evolving));
        sr.execute("ALTER TABLE dw.evolving ADD COLUMN c INT");
        awaitSchemaChange("evolving", 3);
        assertEquals(Arrays.asList("a", "b"), metadata.getColumnNames(evolving), "cached");
        metadata.invalidate(evolving);
        assertEquals(Arrays.asList("a", "b", "c"), metadata.getColumnNames(evolving));
    }

    /** Table-name case: the provider must agree with what the engine accepts. */
    @Test
    void tableNameCaseSensitivityAgreesWithTheEngine() {
        sr.execute("CREATE TABLE IF NOT EXISTS dw.MixedCase (a INT) DISTRIBUTED BY HASH(a) BUCKETS 1 "
                + "PROPERTIES (\"replication_num\" = \"1\")");
        boolean engineAcceptsLower;
        try {
            sr.query("SELECT a FROM dw.mixedcase");
            engineAcceptsLower = true;
        } catch (IllegalStateException e) {
            engineAcceptsLower = false;
        }
        assertTrue(metadata.tableExists(TableIdentifier.of("default_catalog", "dw", "MixedCase")));
        assertEquals(engineAcceptsLower,
                metadata.tableExists(TableIdentifier.of("default_catalog", "dw", "mixedcase")),
                "engine accepts lower-case spelling: " + engineAcceptsLower);
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private static void awaitSchemaChange(String table, int columns) {
        for (int i = 0; i < 60; i++) {
            if (realColumns(table).size() == columns) {
                return;
            }
            StarRocksCluster.sleep(1_000);
        }
        throw new AssertionError("schema change of " + table + " did not finish");
    }

    private static void executeInDw(String sql) {
        try (Connection c = sr.dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("USE dw");
            st.execute(sql);
        } catch (SQLException e) {
            throw new AssertionError("StarRocks rejected: " + sql + "\n" + e.getMessage(), e);
        }
    }

    private static List<Map<String, Object>> queryInDw(String sql) {
        try (Connection c = sr.dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("USE dw");
            try (ResultSet rs = st.executeQuery(sql)) {
                return StarRocksCluster.rows(rs);
            }
        } catch (SQLException e) {
            throw new AssertionError("StarRocks rejected: " + sql + "\n" + e.getMessage(), e);
        }
    }

    private static List<String> resultColumns(String sql) throws SQLException {
        try (Connection c = sr.dataSource().getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> names = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                names.add(md.getColumnLabel(i).toLowerCase(Locale.ROOT));
            }
            return names;
        }
    }

    private static List<String> realColumns(String table) {
        return sr.query("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = 'dw' "
                + "AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", table).stream()
                .map(r -> String.valueOf(r.get("COLUMN_NAME")).toLowerCase(Locale.ROOT)).collect(Collectors.toList());
    }

    private static long count(String table) {
        return ((Number) sr.query("SELECT COUNT(*) c FROM " + table).get(0).get("c")).longValue();
    }
}
