package com.starrocks.lineage.it;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.JdbcStarRocksMetadataProvider;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.pipeline.AuditLineageSink;
import com.starrocks.lineage.pipeline.AuditLogSyncPipeline;
import com.starrocks.lineage.pipeline.CheckpointStore;
import com.starrocks.lineage.pipeline.CycleReport;
import com.starrocks.lineage.pipeline.HourlyUsageRow;
import com.starrocks.lineage.pipeline.JdbcAuditLogSource;
import com.starrocks.lineage.pipeline.JdbcCheckpointStore;
import com.starrocks.lineage.pipeline.LineageRow;
import com.starrocks.lineage.pipeline.PipelineConfig;
import com.starrocks.lineage.pipeline.PipelineException;
import com.starrocks.lineage.pipeline.StarRocksSink;
import com.starrocks.lineage.pipeline.StreamLoadClient;
import com.starrocks.lineage.pipeline.UserDictionary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole pipeline on a real StarRocks 3.1: real metadata, real audit table, real checkpoint, real Stream Load and
 * merge; results read back with the business queries of {@code sql/query/business_queries.sql}.
 */
@Testcontainers(disabledWithoutDocker = true)
class PipelineEndToEndIT {

    private static StarRocksCluster sr;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private LocalDateTime base;

    @BeforeAll
    static void schema() {
        sr = StarRocksCluster.get();
        String props = " BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")";
        sr.execute("DROP DATABASE IF EXISTS e2e_sales FORCE", "DROP DATABASE IF EXISTS e2e_dw FORCE",
                "CREATE DATABASE e2e_sales", "CREATE DATABASE e2e_dw",
                "CREATE TABLE e2e_sales.orders (order_id BIGINT, user_id BIGINT, amount DECIMAL(18, 2), dt DATE) "
                        + "DISTRIBUTED BY HASH(order_id)" + props,
                "CREATE TABLE e2e_sales.users (user_id BIGINT, name VARCHAR(32), city VARCHAR(32)) "
                        + "DISTRIBUTED BY HASH(user_id)" + props,
                "CREATE TABLE e2e_dw.user_gmv (uid BIGINT, gmv DECIMAL(38, 2)) DISTRIBUTED BY HASH(uid)" + props,
                "INSERT INTO e2e_sales.orders VALUES (1, 10, 25.00, '2024-06-01'), (2, 11, 5.00, '2024-06-01')",
                "INSERT INTO e2e_sales.users VALUES (10, 'a', 'HZ'), (11, 'b', 'SH')");
    }

    @BeforeEach
    void reset() {
        sr.resetPipelineTables();
        sr.execute("DROP VIEW IF EXISTS e2e_dw.v_city");
        base = StarRocksCluster.todayMinutesAgo(20, 11);
    }

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
        closeables.clear();
    }

    /** Executes {@code stmt} on the engine (when it is valid) and records it in the audit table. */
    private void audited(String queryId, int minute, String user, String state, String errorCode, String stmt,
                         boolean execute) {
        if (execute) {
            sr.execute(stmt);
        }
        sr.query("SELECT 1");
        try (java.sql.Connection c = sr.dataSource().getConnection();
             java.sql.PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO starrocks_audit_db__.starrocks_audit_tbl__ "
                     + "(queryId, `timestamp`, queryType, clientIp, `user`, authorizedUser, resourceGroup, `catalog`, "
                     + "db, state, errorCode, queryTime, scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, "
                     + "stmtId, isQuery, feIp, stmt, digest, planCpuCosts, planMemCosts) "
                     + "VALUES (?, ?, 'Query', '10.0.0.1', ?, ?, 'default_wg', 'default_catalog', 'e2e_sales', ?, ?, "
                     + "?, 4096, 2, 1, 0, 0, 1, 1, 'fe', ?, ?, 0, 0)")) {
            ps.setString(1, queryId);
            ps.setObject(2, base.plusMinutes(minute));
            ps.setString(3, user);
            ps.setString(4, "'" + user + "'@'%'");
            ps.setString(5, state);
            ps.setString(6, errorCode);
            ps.setLong(7, 100L * minute + 50);
            ps.setString(8, stmt);
            ps.setString(9, "dg_" + queryId);
            ps.executeUpdate();
        } catch (java.sql.SQLException e) {
            String tracking = "";
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("job_id=(\\d+)").matcher(e.getMessage());
            if (m.find()) {
                tracking = String.valueOf(sr.query("select tracking_log from information_schema.load_tracking_logs "
                        + "where job_id=" + m.group(1)));
            }
            throw new IllegalStateException(e.getMessage() + " " + tracking, e);
        }
    }

    private void seedScenario() {
        audited("e01", 0, "alice", "EOF", "", "/* ApplicationName=DBeaver */ "
                + "SELECT amount FROM e2e_sales.orders WHERE amount > 1", true);
        audited("e02", 1, "bob", "EOF", "", "SELECT o.amount, u.city FROM e2e_sales.orders o "
                + "JOIN e2e_sales.users u ON o.user_id = u.user_id", true);
        audited("e03", 2, "etl", "EOF", "", "INSERT INTO e2e_dw.user_gmv "
                + "SELECT user_id, SUM(amount) FROM e2e_sales.orders GROUP BY user_id", true);
        audited("e04", 3, "etl", "EOF", "", "CREATE VIEW e2e_dw.v_city AS "
                + "SELECT city, COUNT(*) FROM e2e_sales.users GROUP BY city", true);
        audited("e05", 4, "alice", "ERR", "1064", "SELECT broken FROM", false);
        audited("e06", 5, "alice", "EOF", "", "SHOW TABLES", false);
        audited("e07", 6, "carol", "EOF", "", "SELECT FROM WHERE", false);
        audited("e08", 7, "alice", "EOF", "", "SELECT amount FROM e2e_sales.orders ORDER BY amount", true);
    }

    private AuditLogSyncPipeline pipeline(AuditLineageSink sink) {
        JdbcStarRocksMetadataProvider metadata = new JdbcStarRocksMetadataProvider(sr.dataSource());
        PipelineConfig config = PipelineConfig.builder("e2e", "it-node").pageSize(3).maxRecordsPerCycle(1_000)
                .lateArrival(Duration.ofMinutes(1)).analyzerThreads(4).initialStart(base.minusMinutes(5)).build();
        AuditLogSyncPipeline p = new AuditLogSyncPipeline(config, new JdbcAuditLogSource(sr.dataSource()),
                new JdbcCheckpointStore(sr.dataSource()), sink, new StarRocksSqlAnalyzer(metadata), metadata,
                Clock.systemUTC());
        closeables.add(p);
        return p;
    }

    private StarRocksSink realSink() {
        StreamLoadClient client = sr.streamLoadClient();
        closeables.add(client);
        return new StarRocksSink(sr.dataSource(), client, "audit_lineage_db");
    }

    private static long num(Object o) {
        return o instanceof BigDecimal ? ((BigDecimal) o).longValue() : ((Number) o).longValue();
    }

    /** Business query Q1 aggregated over the whole window. */
    private Map<String, Object> usageOf(String table, String column) {
        List<Map<String, Object>> rows = sr.query("SELECT SUM(access_query_cnt) access, SUM(select_clause_cnt) sel, "
                + "SUM(where_clause_cnt) wh, SUM(join_clause_cnt) jn, SUM(group_by_cnt) gb, SUM(order_by_cnt) ob, "
                + "BITMAP_UNION_COUNT(user_bitmap) uv, HLL_UNION_AGG(user_hll) uv_hll, MAX(max_query_time_ms) mx "
                + "FROM audit_lineage_db.audit_column_hourly_usage WHERE catalog_name = 'default_catalog' "
                + "AND db_name = 'e2e_sales' AND table_name = ? AND column_name = ?", table, column);
        return rows.get(0);
    }

    /** Business query Q6: upstream of one target column, as "source.column DIRECT/TRANSFORM". */
    private Set<String> upstream(String table, String column) {
        return sr.query("SELECT source_table, source_column, lineage_type, transform_type "
                + "FROM audit_lineage_db.audit_column_lineage WHERE target_catalog = 'default_catalog' "
                + "AND target_db = 'e2e_dw' AND target_table = ? AND target_column = ?", table, column).stream()
                .map(r -> r.get("source_table") + "." + r.get("source_column") + " " + r.get("lineage_type") + "/"
                        + r.get("transform_type")).collect(Collectors.toCollection(HashSet::new));
    }

    private Map<String, Object> snapshot() {
        Map<String, Object> s = new java.util.TreeMap<>();
        s.put("amount", usageOf("orders", "amount"));
        s.put("user_id", usageOf("orders", "user_id"));
        s.put("city", usageOf("users", "city"));
        s.put("lineage", sr.query("SELECT edge_id, exec_count FROM audit_lineage_db.audit_column_lineage "
                + "ORDER BY edge_id"));
        s.put("dict", sr.query("SELECT user_name, user_id FROM audit_lineage_db.audit_user_dict ORDER BY user_id"));
        return s;
    }

    @Test
    void fullCycleOnARealCluster() {
        seedScenario();
        AuditLogSyncPipeline p = pipeline(realSink());
        CycleReport report = p.runOnce();
        assertEquals(CycleReport.Outcome.COMMITTED, report.getOutcome(), report.toString());
        assertEquals(8, report.getRecords());
        assertEquals(5, report.statusCount(ParseStatus.SUCCESS), report.toString());
        assertEquals(1, report.statusCount(ParseStatus.SKIPPED_FAILED_QUERY));
        assertEquals(1, report.statusCount(ParseStatus.SKIPPED_MANAGEMENT));
        assertEquals(1, report.statusCount(ParseStatus.PARSE_ERROR));
        assertEquals(0, p.getMetrics().getDegraded(), "real metadata: nothing degraded");

        Map<String, Object> amount = usageOf("orders", "amount");
        assertEquals(3, num(amount.get("access")), "e01, e02, e08");
        assertEquals(3, num(amount.get("sel")));
        assertEquals(1, num(amount.get("wh")));
        assertEquals(0, num(amount.get("jn")));
        assertEquals(1, num(amount.get("ob")));
        assertEquals(2, num(amount.get("uv")), "alice and bob");
        assertEquals(2, num(amount.get("uv_hll")));
        assertEquals(750, num(amount.get("mx")), "queryTime of e08 = 100 * 7 + 50");
        Map<String, Object> userId = usageOf("orders", "user_id");
        assertEquals(1, num(userId.get("access")));
        assertEquals(1, num(userId.get("jn")));
        assertEquals(1, num(usageOf("users", "city").get("sel")));

        List<String> readers = sr.query("SELECT d.user_name FROM (SELECT BITMAP_UNION(user_bitmap) bm FROM "
                + "audit_lineage_db.audit_column_hourly_usage WHERE db_name = 'e2e_sales' AND table_name = 'orders' "
                + "AND column_name = 'amount') b CROSS JOIN LATERAL UNNEST(BITMAP_TO_ARRAY(b.bm)) AS t(user_id) "
                + "JOIN audit_lineage_db.audit_user_dict d ON d.user_id = t.user_id ORDER BY d.user_name").stream()
                .map(r -> String.valueOf(r.get("user_name"))).collect(Collectors.toList());
        assertEquals(Arrays.asList("alice", "bob"), readers, "business query Q5");

        assertEquals(new HashSet<>(Arrays.asList("orders.amount DIRECT/AGGREGATION",
                        "orders.user_id INDIRECT/IDENTITY")),
                upstream("user_gmv", "gmv"));
        assertEquals(new HashSet<>(Arrays.asList("orders.user_id DIRECT/IDENTITY", "orders.user_id INDIRECT/IDENTITY")),
                upstream("user_gmv", "uid"));
        List<String> viewColumns = sr.query("SELECT COLUMN_NAME FROM information_schema.columns WHERE "
                + "TABLE_SCHEMA = 'e2e_dw' AND TABLE_NAME = 'v_city' ORDER BY ORDINAL_POSITION").stream()
                .map(r -> String.valueOf(r.get("COLUMN_NAME"))).collect(Collectors.toList());
        Set<String> lineageTargets = sr.query("SELECT DISTINCT target_column "
                + "FROM audit_lineage_db.audit_column_lineage "
                + "WHERE target_table = 'v_city'").stream().map(r -> String.valueOf(r.get("target_column")))
                .collect(Collectors.toSet());
        assertEquals(new HashSet<>(viewColumns), lineageTargets, "view lineage uses the engine's column names");
        assertEquals(new HashSet<>(Arrays.asList("users.city DIRECT/IDENTITY", "users.city INDIRECT/IDENTITY")),
                upstream("v_city", "city"));

        Set<String> downstream = sr.query("SELECT target_table, target_column "
                + "FROM audit_lineage_db.audit_column_lineage "
                + "WHERE source_db = 'e2e_sales' AND source_table = 'orders' AND source_column = 'amount'").stream()
                .map(r -> r.get("target_table") + "." + r.get("target_column")).collect(Collectors.toSet());
        assertEquals(new HashSet<>(Arrays.asList("user_gmv.gmv")), downstream, "business query Q7");

        Map<String, Object> gmv = sr.query("SELECT exec_count, last_query_id, last_exec_user, stmt_type, sql_digest "
                + "FROM audit_lineage_db.audit_column_lineage "
                + "WHERE target_table = 'user_gmv' AND target_column = 'gmv' "
                + "AND lineage_type = 'DIRECT'").get(0);
        assertEquals(1, num(gmv.get("exec_count")));
        assertEquals("e03", gmv.get("last_query_id"));
        assertEquals("etl", gmv.get("last_exec_user"));
        assertEquals("INSERT_INTO", gmv.get("stmt_type"));
        assertEquals("dg_e03", gmv.get("sql_digest"));

        CheckpointStore.Checkpoint cp = new JdbcCheckpointStore(sr.dataSource()).load("e2e").get();
        assertEquals("e08", cp.getWatermark().getQueryId());
        assertFalse(cp.getPending().isPresent());
        assertEquals(CycleReport.Outcome.IDLE, p.runOnce().getOutcome());
    }

    @Test
    void crashAfterMergeIsReplayedExactlyOnceOnARealCluster() {
        seedScenario();
        StarRocksSink real = realSink();
        AtomicBoolean crash = new AtomicBoolean(true);
        AuditLineageSink crashing = new AuditLineageSink() {
            @Override
            public Map<String, Long> loadUserDictionary() {
                return real.loadUserDictionary();
            }

            @Override
            public void writeUserDictionary(String b, List<UserDictionary.Entry> e, LocalDateTime t) {
                real.writeUserDictionary(b, e, t);
            }

            @Override
            public void writeUsage(String b, List<HourlyUsageRow> rows, UserDictionary d) {
                real.writeUsage(b, rows, d);
            }

            @Override
            public void writeLineage(String b, List<LineageRow> rows) {
                real.writeLineage(b, rows);
                if (crash.getAndSet(false)) {
                    throw new IllegalStateException("killed after the merge, before the checkpoint");
                }
            }
        };
        AuditLogSyncPipeline p = pipeline(crashing);
        assertThrows(PipelineException.class, p::runOnce);
        Map<String, Object> afterCrash = snapshot();
        assertTrue(new JdbcCheckpointStore(sr.dataSource()).load("e2e").get().getPending().isPresent());

        // a statement arriving meanwhile must go to the next batch, not into the replay
        audited("e09", 8, "dave", "EOF", "", "SELECT amount FROM e2e_sales.orders", true);

        CycleReport replay = p.runOnce();
        assertTrue(replay.isReplay());
        assertEquals(8, replay.getRecords());
        assertEquals(afterCrash, snapshot(), "the replay changed nothing: labels and the merge guard held");

        CycleReport next = p.runOnce();
        assertEquals(1, next.getRecords());
        assertEquals(4, num(usageOf("orders", "amount").get("access")));
        assertEquals(3, num(usageOf("orders", "amount").get("uv")), "dave added");
        assertEquals(1, num(sr.query("SELECT exec_count FROM audit_lineage_db.audit_column_lineage "
                + "WHERE target_table = 'user_gmv' AND target_column = 'gmv' AND lineage_type = 'DIRECT'")
                .get(0).get("exec_count")), "never doubled");
    }
}
