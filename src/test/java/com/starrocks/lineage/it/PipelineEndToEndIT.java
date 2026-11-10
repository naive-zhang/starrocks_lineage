package com.starrocks.lineage.it;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.JdbcStarRocksMetadataProvider;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.pipeline.AuditLineageSink;
import com.starrocks.lineage.pipeline.AuditLogSyncPipeline;
import com.starrocks.lineage.pipeline.BatchKind;
import com.starrocks.lineage.pipeline.CheckpointStore;
import com.starrocks.lineage.pipeline.CycleReport;
import com.starrocks.lineage.pipeline.JdbcAuditLogSource;
import com.starrocks.lineage.pipeline.JdbcCheckpointStore;
import com.starrocks.lineage.pipeline.LeaseLostException;
import com.starrocks.lineage.pipeline.PipelineConfig;
import com.starrocks.lineage.pipeline.PipelineException;
import com.starrocks.lineage.pipeline.StarRocksSink;
import com.starrocks.lineage.pipeline.StreamLoadClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole pipeline on a real StarRocks 3.1: real metadata, audit table, checkpoint, Stream Load, labelled INSERTs
 * and refresh; results read back with the business queries of {@code sql/query/business_queries.sql}. One test per
 * Stage 5 gap, failures injected by a proxy around the real sink.
 */
@Testcontainers(disabledWithoutDocker = true)
class PipelineEndToEndIT {

    private static StarRocksCluster sr;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private LocalDateTime yesterday;
    private LocalDateTime base;
    private int seq;

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
        sr.execute("DROP VIEW IF EXISTS e2e_dw.v_city", "DROP TABLE IF EXISTS e2e_sales.arrives_later FORCE");
        yesterday = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).minusDays(1);
        base = yesterday.plusHours(10);
    }

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
        closeables.clear();
    }

    /** Executes {@code stmt} on the engine (when it is valid) and records it in the audit table at {@code at}. */
    private String audited(LocalDateTime at, String user, String state, String errorCode, String stmt,
                           boolean execute) {
        if (execute) {
            sr.execute(stmt);
        }
        String queryId = String.format("e%02d", ++seq);
        try (java.sql.Connection c = sr.dataSource().getConnection();
             java.sql.PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO starrocks_audit_db__.starrocks_audit_tbl__ "
                     + "(queryId, `timestamp`, queryType, clientIp, `user`, authorizedUser, resourceGroup, `catalog`, "
                     + "db, state, errorCode, queryTime, scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, "
                     + "stmtId, isQuery, feIp, stmt, digest, planCpuCosts, planMemCosts) "
                     + "VALUES (?, ?, 'Query', '10.0.0.1', ?, ?, 'default_wg', 'default_catalog', 'e2e_sales', ?, ?, "
                     + "?, 4096, 2, 1, 0, 0, 1, ?, 'fe', ?, ?, 0, 0)")) {
            ps.setString(1, queryId);
            ps.setObject(2, at);
            ps.setString(3, user);
            ps.setString(4, "'" + user + "'@'%'");
            ps.setString(5, state);
            ps.setString(6, errorCode);
            ps.setLong(7, 100L * seq + 50);
            ps.setInt(8, stmt.trim().toUpperCase().contains("SELECT") && !stmt.contains("INSERT")
                    && !stmt.contains("CREATE") ? 1 : 0);
            ps.setString(9, stmt);
            ps.setString(10, "dg_" + queryId);
            ps.executeUpdate();
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return queryId;
    }

    /** Eight statements yesterday 10:00 - 10:07, one today. */
    private void seedScenario() {
        audited(base, "alice", "EOF", "", "/* ApplicationName=DBeaver */ "
                + "SELECT amount FROM e2e_sales.orders WHERE amount > 1", true);
        audited(base.plusMinutes(1), "bob", "EOF", "", "SELECT o.amount, u.city FROM e2e_sales.orders o "
                + "JOIN e2e_sales.users u ON o.user_id = u.user_id", true);
        audited(base.plusMinutes(2), "etl", "EOF", "", "INSERT INTO e2e_dw.user_gmv "
                + "SELECT user_id, SUM(amount) FROM e2e_sales.orders GROUP BY user_id", true);
        audited(base.plusMinutes(3), "etl", "EOF", "", "CREATE VIEW e2e_dw.v_city AS "
                + "SELECT city, COUNT(*) FROM e2e_sales.users GROUP BY city", true);
        audited(base.plusMinutes(4), "alice", "ERR", "1064", "SELECT broken FROM", false);
        audited(base.plusMinutes(5), "alice", "EOF", "", "SHOW TABLES", false);
        audited(base.plusMinutes(6), "carol", "EOF", "", "SELECT FROM WHERE", false);
        audited(base.plusMinutes(7), "alice", "EOF", "", "SELECT amount FROM e2e_sales.orders ORDER BY amount", true);
        audited(LocalDateTime.now(ZoneOffset.UTC).withNano(0).minusMinutes(20), "etl", "EOF", "",
                "INSERT INTO e2e_dw.user_gmv SELECT user_id, SUM(amount) FROM e2e_sales.orders GROUP BY user_id",
                true);
    }

    private PipelineConfig.Builder config(String owner) {
        return PipelineConfig.builder("e2e", owner).pageSize(3).lateArrival(Duration.ofMinutes(1))
                .analyzerThreads(4).initialStart(yesterday);
    }

    private AuditLogSyncPipeline pipeline(PipelineConfig config, AuditLineageSink sink, boolean strict) {
        JdbcStarRocksMetadataProvider metadata = new JdbcStarRocksMetadataProvider(sr.dataSource());
        AuditLogSyncPipeline p = new AuditLogSyncPipeline(config, new JdbcAuditLogSource(sr.dataSource()),
                new JdbcCheckpointStore(sr.dataSource(), "audit_lineage_db.audit_lineage_sync_checkpoint",
                        Clock.systemUTC(), "INSERT"), sink,
                new StarRocksSqlAnalyzer(metadata, com.starrocks.sql.parser.SqlModeHelper.MODE_DEFAULT, strict), metadata, Clock.systemUTC());
        closeables.add(p);
        return p;
    }

    private AuditLogSyncPipeline pipeline(AuditLineageSink sink) {
        return pipeline(config("it-node").build(), sink, false);
    }

    private StarRocksSink realSink() {
        StreamLoadClient client = sr.streamLoadClient();
        closeables.add(client);
        return new StarRocksSink(sr.dataSource(), client, "audit_lineage_db", "e2e", Clock.systemUTC());
    }

    /** A sink that runs {@code hooks} (by method name, once each) before or after delegating to the real one. */
    private static final class Interceptor {
        final Map<String, Runnable> before = new ConcurrentHashMap<>();
        final Map<String, Runnable> after = new ConcurrentHashMap<>();

        AuditLineageSink wrap(AuditLineageSink real) {
            return (AuditLineageSink) Proxy.newProxyInstance(AuditLineageSink.class.getClassLoader(),
                    new Class<?>[]{AuditLineageSink.class}, (proxy, method, args) -> {
                        Runnable b = before.remove(method.getName());
                        if (b != null) {
                            b.run();
                        }
                        Object result;
                        try {
                            result = method.invoke(real, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                        Runnable a = after.remove(method.getName());
                        if (a != null) {
                            a.run();
                        }
                        return result;
                    });
        }
    }

    private static Runnable kill(String what) {
        return () -> {
            throw new IllegalStateException("process killed " + what);
        };
    }

    private static List<CycleReport> drain(AuditLogSyncPipeline p) {
        List<CycleReport> committed = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            CycleReport r = p.runOnce();
            if (r.getOutcome() != CycleReport.Outcome.COMMITTED) {
                return committed;
            }
            committed.add(r);
        }
        throw new AssertionError("pipeline does not settle");
    }

    private static long num(Object o) {
        return o == null ? 0 : o instanceof BigDecimal ? ((BigDecimal) o).longValue() : ((Number) o).longValue();
    }

    /** Business query Q1 aggregated over the whole window. */
    private Map<String, Object> usageOf(String table, String column) {
        return sr.query("SELECT SUM(access_query_cnt) access, SUM(select_clause_cnt) sel, "
                + "SUM(where_clause_cnt) wh, SUM(join_clause_cnt) jn, SUM(group_by_cnt) gb, SUM(order_by_cnt) ob, "
                + "BITMAP_UNION_COUNT(user_bitmap) uv, HLL_UNION_AGG(user_hll) uv_hll, MAX(max_query_time_ms) mx "
                + "FROM audit_lineage_db.audit_column_hourly_usage WHERE catalog_name = 'default_catalog' "
                + "AND db_name = 'e2e_sales' AND table_name = ? AND column_name = ?", table, column).get(0);
    }

    /** Business query Q6: upstream of one target column. */
    private Set<String> upstream(String table, String column) {
        return sr.query("SELECT source_table, source_column, lineage_type, transform_type "
                + "FROM audit_lineage_db.audit_column_lineage WHERE target_catalog = 'default_catalog' "
                + "AND target_db = 'e2e_dw' AND target_table = ? AND target_column = ?", table, column).stream()
                .map(r -> r.get("source_table") + "." + r.get("source_column") + " " + r.get("lineage_type") + "/"
                        + r.get("transform_type")).collect(Collectors.toCollection(HashSet::new));
    }

    private long gmvExecutions() {
        return num(sr.query("SELECT exec_count FROM audit_lineage_db.audit_column_lineage "
                + "WHERE target_table = 'user_gmv' AND target_column = 'gmv' AND lineage_type = 'DIRECT'")
                .get(0).get("exec_count"));
    }

    private Map<String, Object> snapshot() {
        Map<String, Object> s = new TreeMap<>();
        s.put("usage", sr.query("SELECT stat_hour, table_name, column_name, access_query_cnt, select_clause_cnt, "
                + "where_clause_cnt, join_clause_cnt, order_by_cnt, total_scan_bytes, max_query_time_ms, "
                + "BITMAP_TO_STRING(user_bitmap) users FROM audit_lineage_db.audit_column_hourly_usage "
                + "ORDER BY stat_hour, table_name, column_name"));
        s.put("lineage", sr.query("SELECT edge_id, exec_count, first_seen_time, last_seen_time, last_query_id "
                + "FROM audit_lineage_db.audit_column_lineage ORDER BY edge_id"));
        s.put("daily", sr.query("SELECT stat_day, edge_id, exec_count FROM audit_lineage_db"
                + ".audit_column_lineage_daily ORDER BY stat_day, edge_id"));
        s.put("dict", sr.query("SELECT user_name, user_id FROM audit_lineage_db.audit_user_dict ORDER BY user_id"));
        return s;
    }

    private CheckpointStore.Checkpoint checkpoint() {
        return new JdbcCheckpointStore(sr.dataSource()).load("e2e").get();
    }

    // ================================================================================================ gap 1 + base

    @Test
    void firstRunCoversYesterdayAndTodayOnARealCluster() {
        seedScenario();
        AuditLogSyncPipeline p = pipeline(realSink());
        List<CycleReport> batches = drain(p);
        assertEquals(9, batches.stream().mapToInt(CycleReport::getRecords).sum(), batches.toString());
        assertEquals(6, batches.stream().mapToInt(b -> b.statusCount(ParseStatus.SUCCESS)).sum());
        assertEquals(1, batches.stream().mapToInt(b -> b.statusCount(ParseStatus.SKIPPED_FAILED_QUERY)).sum());
        assertEquals(1, batches.stream().mapToInt(b -> b.statusCount(ParseStatus.SKIPPED_MANAGEMENT)).sum());
        assertEquals(1, batches.stream().mapToInt(b -> b.statusCount(ParseStatus.PARSE_ERROR)).sum());
        assertEquals(0, p.getMetrics().getDegraded(), "real metadata: nothing degraded");

        Map<String, Object> amount = usageOf("orders", "amount");
        assertEquals(3, num(amount.get("access")), "e01, e02, e08 - all of yesterday");
        assertEquals(3, num(amount.get("sel")));
        assertEquals(1, num(amount.get("wh")));
        assertEquals(0, num(amount.get("jn")));
        assertEquals(1, num(amount.get("ob")));
        assertEquals(2, num(amount.get("uv")), "alice and bob");
        assertEquals(2, num(amount.get("uv_hll")));
        assertEquals(1, num(usageOf("orders", "user_id").get("jn")));
        assertEquals(1, num(usageOf("users", "city").get("sel")));
        assertEquals(1L, num(sr.query("SELECT COUNT(DISTINCT DATE(stat_hour)) d FROM audit_lineage_db"
                + ".audit_column_hourly_usage").get(0).get("d")), "every query was yesterday");

        List<String> readers = sr.query("SELECT d.user_name FROM (SELECT BITMAP_UNION(user_bitmap) bm FROM "
                + "audit_lineage_db.audit_column_hourly_usage WHERE db_name = 'e2e_sales' AND table_name = 'orders' "
                + "AND column_name = 'amount') b CROSS JOIN LATERAL UNNEST(BITMAP_TO_ARRAY(b.bm)) AS t(user_id) "
                + "JOIN audit_lineage_db.audit_user_dict d ON d.user_id = t.user_id ORDER BY d.user_name").stream()
                .map(r -> String.valueOf(r.get("user_name"))).collect(Collectors.toList());
        assertEquals(Arrays.asList("alice", "bob"), readers, "business query Q5 with 63-bit hash ids");

        assertEquals(new HashSet<>(Arrays.asList("orders.amount DIRECT/AGGREGATION",
                "orders.user_id INDIRECT/IDENTITY")), upstream("user_gmv", "gmv"));
        assertEquals(new HashSet<>(Arrays.asList("users.city DIRECT/IDENTITY", "users.city INDIRECT/IDENTITY")),
                upstream("v_city", "city"));
        assertEquals(2, gmvExecutions(), "yesterday + today");
        assertEquals(2L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_column_lineage_daily d "
                + "JOIN audit_lineage_db.audit_column_lineage l ON l.edge_id = d.edge_id "
                + "WHERE l.target_table = 'user_gmv' AND l.target_column = 'gmv' AND l.lineage_type = 'DIRECT'")
                .get(0).get("c")) , "one fact per day (business query Q9)");

        Map<String, Object> parse = sr.query("SELECT kind, status, stmt, user_name FROM audit_lineage_db"
                + ".audit_lineage_dead_letter WHERE query_id = 'e07'").get(0);
        assertEquals("PARSE_ERROR", parse.get("kind"));
        assertEquals("OPEN", parse.get("status"));
        assertEquals("SELECT FROM WHERE", parse.get("stmt"));
        assertEquals("carol", parse.get("user_name"));

        CheckpointStore.Checkpoint cp = checkpoint();
        assertFalse(cp.getPending().isPresent());
        assertTrue(cp.getCommittedUntil().isAfter(LocalDateTime.now(ZoneOffset.UTC).minusMinutes(7)));
        assertEquals(9, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_batch_member")
                .get(0).get("c")));
        assertTrue(sr.query("SELECT * FROM audit_lineage_db.audit_lineage_batch_log").size() >= batches.stream()
                .filter(b -> b.getRecords() > 0).count());
    }

    // ================================================================================================ gap 4 + 5

    @Test
    void crashesAfterEachAdditiveStepAreReplayedExactlyOnce() {
        seedScenario();
        AuditLogSyncPipeline reference = pipeline(realSink());
        drain(reference);
        Map<String, Object> expected = snapshot();
        reference.close();

        for (String method : Arrays.asList("writeMembers", "writeUsage", "writeBatchLineage", "insertDaily",
                "refreshLineage")) {
            sr.execute(StarRocksCluster.PIPELINE_TABLES.stream().map(t -> "TRUNCATE TABLE audit_lineage_db." + t)
                    .toArray(String[]::new));
            Interceptor crash = new Interceptor();
            crash.after.put(method, kill("after " + method));
            AuditLogSyncPipeline p = pipeline(crash.wrap(realSink()));
            assertThrows(PipelineException.class, () -> drain(p), method);
            assertTrue(checkpoint().getPending().isPresent());
            drain(p);
            assertEquals(expected, snapshot(), "crash after " + method + ": the replay changed nothing");
            p.close();
        }
    }

    @Test
    void unknownLabelBeyondRetentionBlocksUntilTheOperatorDecides() {
        audited(base, "alice", "EOF", "", "SELECT amount FROM e2e_sales.orders", true);
        Interceptor crash = new Interceptor();
        crash.before.put("writeUsage", kill("before the usage load"));
        AuditLogSyncPipeline p = pipeline(config("it-node").labelRetention(Duration.ofSeconds(1)).build(),
                crash.wrap(realSink()), false);
        assertThrows(PipelineException.class, () -> drain(p));
        StarRocksCluster.sleep(1_500);

        CycleReport blocked = p.runOnce();
        assertEquals(CycleReport.Outcome.BLOCKED, blocked.getOutcome(), "real get_load_state answered UNKNOWN");
        assertTrue(p.getMetrics().isBlocked());
        assertEquals(0, num(usageOf("orders", "amount").get("access")));

        p.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.REDO);
        drain(p);
        assertEquals(1, num(usageOf("orders", "amount").get("access")));
    }

    @Test
    void auditRowsArrivingDuringAnInterruptedBatchGoToReconciliation() {
        audited(base, "alice", "EOF", "", "SELECT amount FROM e2e_sales.orders", true);
        Interceptor crash = new Interceptor();
        crash.after.put("writeMembers", kill("after the membership load"));
        AuditLogSyncPipeline p = pipeline(config("it-node").reconcileInterval(Duration.ofSeconds(1)).build(),
                crash.wrap(realSink()), false);
        assertThrows(PipelineException.class, () -> drain(p));
        // a record of the same slice reaches the audit table while the batch is interrupted
        audited(base.plusSeconds(30), "bob", "EOF", "", "SELECT amount FROM e2e_sales.orders", true);

        List<CycleReport> batches = new ArrayList<>(drain(p));
        assertEquals(1, batches.get(0).getRecords(), "gap 5: the resumed batch is exactly its membership");
        StarRocksCluster.sleep(1_100);
        batches.addAll(drain(p));
        CycleReport late = batches.stream().filter(b -> b.getKind().get() == BatchKind.LATE).findFirst()
                .orElseThrow(() -> new AssertionError(batches.toString()));
        assertEquals(1, late.getRecords(), "gap 6: the late record is found by the anti join");
        assertEquals(2, num(usageOf("orders", "amount").get("access")));
        assertEquals(2, num(usageOf("orders", "amount").get("uv")));
    }

    // ================================================================================================ gap 3

    @Test
    void aPausedInstanceIsFencedOnARealCheckpoint() {
        seedScenario();
        PipelineConfig cfgA = config("node-a").leaseTtl(Duration.ofSeconds(2)).build();
        PipelineConfig cfgB = config("node-b").leaseTtl(Duration.ofSeconds(2)).build();
        AuditLogSyncPipeline b = pipeline(cfgB, realSink(), false);
        AtomicReference<List<CycleReport>> byB = new AtomicReference<>();
        Interceptor pause = new Interceptor();
        pause.before.put("writeBatchLineage", () -> {
            StarRocksCluster.sleep(2_500);
            byB.set(drain(b));
        });
        AuditLogSyncPipeline a = pipeline(cfgA, pause.wrap(realSink()), false);
        assertThrows(LeaseLostException.class, () -> drain(a));
        assertFalse(byB.get().isEmpty());
        assertTrue(byB.get().get(0).isReplay(), "node-b finished node-a's batch");
        drain(b);
        assertEquals("node-b", checkpoint().getOwner());
        assertEquals(2, gmvExecutions(), "node-a's in-flight write reused the batch label: counted once");
        assertEquals(3, num(usageOf("orders", "amount").get("access")));
        assertEquals(CycleReport.Outcome.LEASE_NOT_ACQUIRED, a.runOnce().getOutcome());
    }

    // ================================================================================================ gap 2 + 7

    /**
     * A record that failed analysis (strict resolution: the column was dropped between the query and its analysis)
     * is a dead letter with its full record; once the column exists again a repair - with fresh metadata - adds it.
     */
    @Test
    void deadLettersAreRepairedOnceTheMissingObjectExists() {
        sr.execute("CREATE TABLE e2e_sales.arrives_later (k BIGINT) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")");
        String q = audited(base, "alice", "EOF", "", "SELECT k, v FROM e2e_sales.arrives_later", false);
        audited(base.plusMinutes(1), "alice", "EOF", "", "SELECT amount FROM e2e_sales.orders", true);
        AuditLogSyncPipeline p = pipeline(config("it-node").build(), realSink(), true);
        drain(p);
        assertEquals("ANALYSIS_ERROR", sr.query("SELECT kind FROM audit_lineage_db.audit_lineage_dead_letter "
                + "WHERE query_id = ?", q).get(0).get("kind"));
        assertEquals(1, num(usageOf("orders", "amount").get("access")), "the good record was not held back");

        sr.execute("DROP TABLE e2e_sales.arrives_later FORCE",
                "CREATE TABLE e2e_sales.arrives_later (k BIGINT, v VARCHAR(8)) DISTRIBUTED BY HASH(k) "
                        + "BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")");
        CycleReport repair = p.repairDeadLetters(yesterday, yesterday.plusDays(1), 100);
        assertEquals(BatchKind.REPAIR, repair.getKind().get(), repair.toString());
        assertEquals("RESOLVED", sr.query("SELECT status FROM audit_lineage_db.audit_lineage_dead_letter "
                + "WHERE query_id = ?", q).get(0).get("status"));
        assertEquals(1, num(usageOf("arrives_later", "k").get("access")));
        assertEquals(1, num(usageOf("arrives_later", "v").get("access")));
    }

    @Test
    void rebuildRestoresCorruptedDaysExactly() {
        seedScenario();
        AuditLogSyncPipeline p = pipeline(realSink());
        drain(p);
        Map<String, Object> expected = snapshot();

        sr.execute("INSERT INTO audit_lineage_db.audit_column_hourly_usage (stat_hour, catalog_name, db_name, "
                        + "table_name, column_name, access_query_cnt, user_bitmap, user_hll, last_access_time) "
                        + "VALUES ('" + base.withMinute(0).toString().replace('T', ' ') + "', 'default_catalog', "
                        + "'e2e_sales', 'orders', 'amount', 40, to_bitmap(1), hll_hash('x'), NOW())",
                "UPDATE audit_lineage_db.audit_column_lineage SET exec_count = 99 WHERE target_table = 'user_gmv'");
        Interceptor crash = new Interceptor();
        crash.after.put("purgeUsage", kill("after the rebuild purge"));
        AuditLogSyncPipeline rebuilding = pipeline(config("it-node").build(), crash.wrap(realSink()), false);
        p.close();
        assertThrows(PipelineException.class, () -> rebuilding.rebuild(yesterday, yesterday.plusDays(1)));
        CycleReport resumed = rebuilding.runOnce();
        assertEquals(BatchKind.REBUILD, resumed.getKind().get());
        assertEquals(8, resumed.getRecords());
        assertEquals(expected, snapshot(), "usage, daily facts and lineage recomputed exactly");

        rebuilding.rebuild(yesterday, yesterday.plusDays(1));
        assertEquals(expected, snapshot(), "a rebuild is repeatable");
    }
}
