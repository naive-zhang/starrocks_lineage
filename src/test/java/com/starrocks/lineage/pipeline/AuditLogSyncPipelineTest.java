package com.starrocks.lineage.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.metadata.MetadataAccessException;
import com.starrocks.lineage.metadata.MetadataProvider;
import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.model.TransformType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end: H2 audit table → {@link JdbcAuditLogSource} → analyzer threads → aggregators →
 * {@link StarRocksSink} (stream load to {@link FakeStarRocks}, merge on H2) → {@link JdbcCheckpointStore}.
 */
class AuditLogSyncPipelineTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2024, 6, 18, 12, 0);
    private static final Clock CLOCK = Clock.fixed(NOW.toInstant(ZoneOffset.UTC), ZoneId.of("UTC"));

    private FakeStarRocks sr;
    private StreamLoadClient client;
    private InMemoryMetadataProvider metadata;
    private final List<AuditLogSyncPipeline> pipelines = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        sr = new FakeStarRocks();
        client = sr.client();
        metadata = new InMemoryMetadataProvider()
                .register("default_catalog.sales.orders", "order_id", "user_id", "amount", "dt")
                .register("default_catalog.sales.users", "user_id", "name", "city")
                .register("default_catalog.dw.user_gmv", "user_id", "gmv");
        try (Connection c = sr.dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA starrocks_audit_db__");
            st.execute("CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (queryId VARCHAR(48), "
                    + "`timestamp` DATETIME, queryType VARCHAR(12), clientIp VARCHAR(32), `user` VARCHAR(64), "
                    + "authorizedUser VARCHAR(64), `catalog` VARCHAR(32), db VARCHAR(96), state VARCHAR(8), "
                    + "errorCode VARCHAR(96), queryTime BIGINT, scanBytes BIGINT, scanRows BIGINT, "
                    + "returnRows BIGINT, cpuCostNs BIGINT, memCostBytes BIGINT, isQuery TINYINT, "
                    + "stmt VARCHAR(65533), digest VARCHAR(32))");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AuditLogSyncPipeline p : pipelines) {
            p.close();
        }
        client.close();
        sr.close();
    }

    private void audit(String queryId, LocalDateTime ts, String user, String state, String errorCode, String stmt)
            throws Exception {
        try (Connection c = sr.dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO starrocks_audit_db__.starrocks_audit_tbl__ VALUES "
                        + "(?, ?, 'query', '10.0.0.1', ?, ?, 'default_catalog', 'sales', ?, ?, 120, 4096, 10, 1, "
                        + "0, 0, 1, ?, ?)")) {
            ps.setString(1, queryId);
            ps.setTimestamp(2, Timestamp.valueOf(ts));
            ps.setString(3, user);
            ps.setString(4, "'" + user + "'@'%'");
            ps.setString(5, state);
            ps.setString(6, errorCode);
            ps.setString(7, stmt);
            ps.setString(8, "dg" + queryId);
            ps.executeUpdate();
        }
    }

    private AuditLogSyncPipeline pipeline(String owner, int pageSize, int maxRecords, AuditLineageSink sink,
                                          MetadataProvider meta) {
        PipelineConfig config = PipelineConfig.builder("audit_lineage", owner).pageSize(pageSize)
                .maxRecordsPerCycle(maxRecords).lateArrival(Duration.ofMinutes(5)).analyzerThreads(4)
                .leaseTtl(Duration.ofMinutes(10)).initialStart(NOW.minusDays(1)).build();
        AuditLogSyncPipeline p = new AuditLogSyncPipeline(config, new JdbcAuditLogSource(sr.dataSource),
                new JdbcCheckpointStore(sr.dataSource, "audit_lineage_db.audit_lineage_sync_checkpoint", CLOCK,
                        "MERGE"),
                sink, new StarRocksSqlAnalyzer(meta), meta, CLOCK);
        pipelines.add(p);
        return p;
    }

    private StarRocksSink sink() {
        return new StarRocksSink(sr.dataSource, client, "audit_lineage_db", 1_000, "MERGE", CLOCK);
    }

    private CheckpointStore.Checkpoint checkpoint() {
        return new JdbcCheckpointStore(sr.dataSource, "audit_lineage_db.audit_lineage_sync_checkpoint", CLOCK,
                        "MERGE")
                .load("audit_lineage").get();
    }

    private void seedFirstBatch() throws Exception {
        LocalDateTime t = NOW.minusHours(2);
        audit("q01", t, "alice", "EOF", "", "/* ApplicationName=DBeaver */ SELECT amount FROM orders WHERE amount > 1");
        audit("q02", t, "bob", "EOF", "",
                "SELECT o.amount, u.city FROM orders o JOIN users u ON o.user_id = u.user_id");
        audit("q03", t.plusMinutes(1), "etl", "EOF", "",
                "INSERT INTO dw.user_gmv SELECT user_id, SUM(amount) FROM orders GROUP BY user_id");
        audit("q04", t.plusMinutes(2), "alice", "ERR", "1064", "SELECT broken FROM");
        audit("q05", t.plusMinutes(3), "alice", "EOF", "", "SHOW TABLES");
        audit("q06", t.plusMinutes(4), "carol", "EOF", "", "SELECT FROM WHERE");
        audit("q07", t.plusMinutes(5), "alice", "EOF", "0", "SELECT amount FROM orders ORDER BY amount");
        audit("q99", NOW.minusMinutes(2), "late", "EOF", "", "SELECT city FROM users");
    }

    @Test
    void endToEndCycleWritesUsageLineageDictionaryAndCheckpoint() throws Exception {
        seedFirstBatch();
        AuditLogSyncPipeline p = pipeline("node-1", 3, 1_000, sink(), metadata);

        CycleReport report = p.runOnce();
        assertEquals(CycleReport.Outcome.COMMITTED, report.getOutcome());
        assertEquals(7, report.getRecords(), "q99 is inside the 5 minute late-arrival window");
        assertEquals(4, report.statusCount(ParseStatus.SUCCESS));
        assertEquals(1, report.statusCount(ParseStatus.SKIPPED_FAILED_QUERY));
        assertEquals(1, report.statusCount(ParseStatus.SKIPPED_MANAGEMENT));
        assertEquals(1, report.statusCount(ParseStatus.PARSE_ERROR));
        assertEquals(new Watermark(NOW.minusHours(2).plusMinutes(5), "q07"), report.getWatermark().get());

        // usage: one row per (hour, column, user)
        List<FakeStarRocks.Load> usage = sr.loadsInto("audit_column_hourly_usage");
        assertEquals(1, usage.size());
        assertEquals(report.getBatchId().get() + "_usage_0", usage.get(0).label);
        JsonNode rows = usage.get(0).rows;
        Map<String, Long> users = sink().loadUserDictionary();
        assertEquals(2, users.size(), "alice and bob (etl wrote lineage only)");
        long aliceAmountAccess = 0;
        long aliceAmountWhere = 0;
        long aliceAmountOrder = 0;
        for (JsonNode r : rows) {
            if (r.get("user_name").asText().equals("alice") && r.get("column_name").asText().equals("amount")) {
                aliceAmountAccess += r.get("access_query_cnt").asLong();
                aliceAmountWhere += r.get("where_clause_cnt").asLong();
                aliceAmountOrder += r.get("order_by_cnt").asLong();
                assertEquals(users.get("alice").longValue(), r.get("user_id").asLong());
                assertEquals("2024-06-18 10:00:00", r.get("stat_hour").asText());
                assertEquals(8192, r.get("total_scan_bytes").asLong());
            }
        }
        assertEquals(2, aliceAmountAccess, "q01 + q07");
        assertEquals(1, aliceAmountWhere);
        assertEquals(1, aliceAmountOrder);
        assertEquals(5, rows.size(), "alice.amount, bob.amount, bob.user_id x2 (orders/users), bob.city");

        // lineage merged
        ColumnLineageEdge gmv = ColumnLineageEdge.of(ColumnIdentifier.parse("default_catalog.dw.user_gmv.gmv"),
                ColumnIdentifier.parse("default_catalog.sales.orders.amount"), LineageType.DIRECT,
                TransformType.AGGREGATION, null);
        Map<String, Object> row = sr.lineageRow(gmv.edgeId());
        assertEquals("AGGREGATION", row.get("transform_type"));
        assertEquals(1L, row.get("exec_count"));
        assertEquals("q03", row.get("last_query_id"));
        assertEquals("etl", row.get("last_exec_user"));
        assertEquals("INSERT_INTO", row.get("stmt_type"));
        assertEquals("dgq03", row.get("sql_digest"));
        assertEquals(4, sr.count("audit_column_lineage"), "uid/gmv direct + group-by indirect on both");

        CheckpointStore.Checkpoint cp = checkpoint();
        assertEquals(report.getWatermark().get(), cp.getWatermark());
        assertFalse(cp.getPending().isPresent());
        assertEquals(7, cp.getProcessedRows());

        assertEquals(CycleReport.Outcome.IDLE, p.runOnce().getOutcome());
        assertEquals(1, p.getMetrics().getCommittedCycles());
        assertEquals(4, p.getMetrics().statusCount(ParseStatus.SUCCESS));
    }

    /** Review R4-02: crash after the lineage merge, before the commit → exact replay, nothing counted twice. */
    @Test
    void failedCycleIsReplayedExactlyOnce() throws Exception {
        seedFirstBatch();
        AtomicBoolean crash = new AtomicBoolean(true);
        StarRocksSink real = sink();
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
                    throw new IllegalStateException("process killed after merge");
                }
            }
        };
        AuditLogSyncPipeline p = pipeline("node-1", 100, 1_000, crashing, metadata);
        assertThrows(PipelineException.class, p::runOnce);
        CheckpointStore.Checkpoint afterCrash = checkpoint();
        assertTrue(afterCrash.getPending().isPresent(), "intent survives the crash");
        assertEquals(Watermark.startingAt(NOW.minusDays(1)), afterCrash.getWatermark());
        String batchId = afterCrash.getPending().get().getBatchId();

        // a new record arrives meanwhile: it must NOT join the replayed batch
        audit("q08", NOW.minusHours(1), "dave", "EOF", "", "SELECT name FROM users");

        CycleReport replay = p.runOnce();
        assertTrue(replay.isReplay());
        assertEquals(batchId, replay.getBatchId().get());
        assertEquals(7, replay.getRecords());
        assertEquals(1, sr.loadsInto("audit_column_hourly_usage").size(), "usage label de-duplicated the replay");
        assertEquals(1, sr.loadsInto("audit_column_lineage_stage").size());
        ColumnLineageEdge gmv = ColumnLineageEdge.of(ColumnIdentifier.parse("default_catalog.dw.user_gmv.gmv"),
                ColumnIdentifier.parse("default_catalog.sales.orders.amount"), LineageType.DIRECT,
                TransformType.AGGREGATION, null);
        assertEquals(1L, sr.lineageRow(gmv.edgeId()).get("exec_count"), "merge guard: not doubled");
        assertFalse(checkpoint().getPending().isPresent());

        CycleReport next = p.runOnce();
        assertFalse(next.isReplay());
        assertEquals(1, next.getRecords(), "q08 in its own batch");
        assertEquals(1, p.getMetrics().getFailedCycles());
        assertEquals(1, p.getMetrics().getReplayedCycles());
        assertEquals(3, sink().loadUserDictionary().size(), "dave got the next id");
    }

    @Test
    void metadataOutageAbortsWithoutCommittingAndRecovers() throws Exception {
        seedFirstBatch();
        AtomicBoolean down = new AtomicBoolean(true);
        MetadataProvider flaky = new MetadataProvider() {
            @Override
            public Optional<TableSchema> getTableSchema(TableIdentifier table) {
                if (down.get()) {
                    throw new MetadataAccessException("FE unreachable", null);
                }
                return metadata.getTableSchema(table);
            }
        };
        AuditLogSyncPipeline p = pipeline("node-1", 100, 1_000, sink(), flaky);
        PipelineException e = assertThrows(PipelineException.class, p::runOnce);
        assertTrue(e.getMessage().contains("metadata unavailable"));
        assertTrue(sr.loadsInto("audit_column_hourly_usage").isEmpty(), "nothing written");
        assertTrue(checkpoint().getPending().isPresent());
        down.set(false);
        CycleReport ok = p.runOnce();
        assertTrue(ok.isReplay());
        assertEquals(4, ok.statusCount(ParseStatus.SUCCESS));
    }

    @Test
    void keysetPaginationAcrossEqualTimestampsAndCycleLimit() throws Exception {
        LocalDateTime t = NOW.minusHours(3);
        for (int i = 0; i < 10; i++) {
            audit(String.format("k%02d", i), i < 6 ? t : t.plusSeconds(1), "u" + i, "EOF", "",
                    "SELECT city FROM users");
        }
        AuditLogSyncPipeline p = pipeline("node-1", 2, 4, sink(), metadata);
        List<String> batches = new ArrayList<>();
        int total = 0;
        CycleReport r;
        while ((r = p.runOnce()).getOutcome() == CycleReport.Outcome.COMMITTED) {
            total += r.getRecords();
            batches.add(r.getBatchId().get());
            assertTrue(r.getRecords() <= 4);
        }
        assertEquals(10, total, "no record skipped or repeated across pages sharing a timestamp");
        assertEquals(3, batches.size());
        assertEquals(3, batches.stream().distinct().count());
        long access = 0;
        for (FakeStarRocks.Load l : sr.loadsInto("audit_column_hourly_usage")) {
            for (JsonNode row : l.rows) {
                access += row.get("access_query_cnt").asLong();
            }
        }
        assertEquals(10, access);
        assertEquals(10, sink().loadUserDictionary().size());
    }

    @Test
    void secondInstanceCannotTakeALiveLease() throws Exception {
        seedFirstBatch();
        AuditLogSyncPipeline first = pipeline("node-1", 100, 1_000, sink(), metadata);
        AuditLogSyncPipeline second = pipeline("node-2", 100, 1_000, sink(), metadata);
        assertEquals(CycleReport.Outcome.COMMITTED, first.runOnce().getOutcome());
        assertEquals(CycleReport.Outcome.LEASE_NOT_ACQUIRED, second.runOnce().getOutcome());
        assertEquals(CycleReport.Outcome.IDLE, first.runOnce().getOutcome());
    }

    @Test
    void createdObjectsAreInvalidatedInTheMetadataCache() throws Exception {
        audit("c1", NOW.minusHours(1), "etl", "EOF", "", "CREATE TABLE dw.t_new AS SELECT city FROM users");
        AtomicInteger invalidations = new AtomicInteger();
        MetadataProvider tracking = new MetadataProvider() {
            @Override
            public Optional<TableSchema> getTableSchema(TableIdentifier table) {
                return metadata.getTableSchema(table);
            }

            @Override
            public void invalidate(TableIdentifier table) {
                assertEquals(TableIdentifier.parse("default_catalog.dw.t_new"), table);
                invalidations.incrementAndGet();
            }
        };
        pipeline("node-1", 100, 1_000, sink(), tracking).runOnce();
        assertEquals(1, invalidations.get());
    }

    @Test
    void scheduledRunnerProcessesInTheBackground() throws Exception {
        seedFirstBatch();
        AuditLogSyncPipeline p = pipeline("node-1", 100, 1_000, sink(), metadata);
        p.start(Duration.ofMillis(50));
        assertThrows(IllegalStateException.class, () -> p.start(Duration.ofMillis(50)));
        long deadline = System.currentTimeMillis() + 10_000;
        while (p.getMetrics().getCommittedCycles() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        p.close();
        assertEquals(1, p.getMetrics().getCommittedCycles());
        assertFalse(checkpoint().getPending().isPresent());
    }

    /** Review R4-05: a checkpoint store that does not persist the commit must fail, never loop. */
    @Test
    void unpersistedCommitFailsInsteadOfLoopingForever() throws Exception {
        try (Connection c = sr.dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE audit_lineage_db.cp_without_pk (pipeline_name VARCHAR(64), "
                    + "watermark_time TIMESTAMP, watermark_query_id VARCHAR(64), owner VARCHAR(128), "
                    + "lease_expire_time TIMESTAMP, processed_rows BIGINT, pending_batch_id VARCHAR(64), "
                    + "pending_end_time TIMESTAMP, pending_end_query_id VARCHAR(64), update_time TIMESTAMP)");
        }
        JdbcCheckpointStore appending = new JdbcCheckpointStore(sr.dataSource, "audit_lineage_db.cp_without_pk",
                CLOCK, "INSERT");
        Watermark start = Watermark.startingAt(NOW.minusDays(1));
        assertTrue(appending.tryAcquireLease("p", "o", Duration.ofMinutes(1), start));
        appending.savePending("p", "o", Duration.ofMinutes(1),
                new CheckpointStore.PendingBatch("b", new Watermark(NOW, "q")));
        assertThrows(PipelineException.class, () -> appending.commit("p", "o", Duration.ofMinutes(1),
                new Watermark(NOW, "q"), 1));
    }

    @Test
    void configValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> PipelineConfig.builder("", "o").build());
        assertThrows(IllegalArgumentException.class,
                () -> PipelineConfig.builder("p", "o").pageSize(10).maxRecordsPerCycle(5).build());
        assertThrows(IllegalArgumentException.class,
                () -> PipelineConfig.builder("p", "o").analyzerThreads(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> PipelineConfig.builder("p", "o").leaseTtl(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcAuditLogSource(sr.dataSource, "t; drop table x", 1));
        assertEquals(Instant.parse("2024-06-18T12:00:00Z"), CLOCK.instant());
    }
}
