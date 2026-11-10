package com.starrocks.lineage.it;

import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.pipeline.BatchKind;
import com.starrocks.lineage.pipeline.BatchLogEntry;
import com.starrocks.lineage.pipeline.BatchStep;
import com.starrocks.lineage.pipeline.CheckpointStore;
import com.starrocks.lineage.pipeline.DeadLetter;
import com.starrocks.lineage.pipeline.HourlyUsageAggregator;
import com.starrocks.lineage.pipeline.HourlyUsageRow;
import com.starrocks.lineage.pipeline.JdbcAuditLogSource;
import com.starrocks.lineage.pipeline.JdbcCheckpointStore;
import com.starrocks.lineage.pipeline.LeaseLostException;
import com.starrocks.lineage.pipeline.LineageAggregator;
import com.starrocks.lineage.pipeline.LineageRow;
import com.starrocks.lineage.pipeline.PipelineException;
import com.starrocks.lineage.pipeline.StarRocksSink;
import com.starrocks.lineage.pipeline.StreamLoadClient;
import com.starrocks.lineage.pipeline.UserIds;
import com.starrocks.lineage.pipeline.Watermark;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage layer on a real StarRocks 3.1: every SQL statement and load of {@link StarRocksSink},
 * {@link JdbcAuditLogSource} and {@link JdbcCheckpointStore}.
 */
@Testcontainers(disabledWithoutDocker = true)
class StarRocksStorageIT {

    private static final String P = "it_pipe";
    private static StarRocksCluster sr;
    private StreamLoadClient client;
    private StarRocksSink sink;
    private final LocalDateTime hour = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS);
    private final LocalDateTime today = hour.truncatedTo(ChronoUnit.DAYS);

    @BeforeAll
    static void start() {
        sr = StarRocksCluster.get();
    }

    @BeforeEach
    void reset() {
        sr.resetPipelineTables();
        client = sr.streamLoadClient();
        sink = new StarRocksSink(sr.dataSource(), client, "audit_lineage_db", P, Clock.systemUTC());
    }

    @AfterEach
    void close() throws Exception {
        client.close();
    }

    private static long num(Object o) {
        return o instanceof BigDecimal ? ((BigDecimal) o).longValue() : ((Number) o).longValue();
    }

    private static long count(String sql, Object... params) {
        return num(sr.query(sql, params).get(0).values().iterator().next());
    }

    // ------------------------------------------------------------------------------------------------ stream load

    @Test
    void streamLoadThroughFeRedirectIsIdempotentPerLabelAndHasALoadState() {
        byte[] body = ("[{\"user_name\":\"it_a\",\"user_id\":1,\"first_seen_time\":\"2024-06-18 09:00:00\"}]")
                .getBytes(StandardCharsets.UTF_8);
        assertEquals(StreamLoadClient.LoadState.UNKNOWN, client.loadState("audit_lineage_db", "it_dict_label"));
        StreamLoadClient.Result first = client.load("audit_lineage_db", "audit_user_dict", "it_dict_label",
                StarRocksSink.DICT_COLUMNS, body);
        assertFalse(first.isAlreadyLoaded());
        assertEquals(1, first.getLoadedRows());
        StreamLoadClient.Result again = client.load("audit_lineage_db", "audit_user_dict", "it_dict_label",
                StarRocksSink.DICT_COLUMNS, body);
        assertTrue(again.isAlreadyLoaded(), "real 'Label Already Exists' + ExistingJobStatus FINISHED");
        assertEquals(1L, count("SELECT COUNT(*) c FROM audit_lineage_db.audit_user_dict"));
        assertEquals(StreamLoadClient.LoadState.COMMITTED, client.loadState("audit_lineage_db", "it_dict_label"),
                "get_load_state answers VISIBLE for a used label");
    }

    @Test
    void realLoadFailuresAreReported() {
        PipelineException e = assertThrows(PipelineException.class, () -> client.load("audit_lineage_db",
                "audit_user_dict", "it_bad_json", StarRocksSink.DICT_COLUMNS,
                "not json".getBytes(StandardCharsets.UTF_8)));
        assertTrue(e.getMessage().contains("failed"), e.getMessage());
        PipelineException unknownTable = assertThrows(PipelineException.class, () -> client.load(
                "audit_lineage_db", "no_such_table", "it_no_table", "a", "[]".getBytes(StandardCharsets.UTF_8)));
        assertTrue(unknownTable.getMessage().contains("no_such_table"), unknownTable.getMessage());
    }

    /** Gap 2: a rejected row fails the strict load, leaves the label free, and the tolerant retry keeps the rest. */
    @Test
    void rejectedRowsAreIsolatedUnderTheSameLabel() {
        byte[] body = ("[{\"user_name\":\"it_ok1\",\"user_id\":1,\"first_seen_time\":\"2024-06-18 09:00:00\"},"
                + "{\"user_name\":\"it_bad\",\"user_id\":2,\"first_seen_time\":\"not a time\"},"
                + "{\"user_name\":\"it_ok2\",\"user_id\":3,\"first_seen_time\":\"2024-06-18 09:00:00\"}]")
                .getBytes(StandardCharsets.UTF_8);
        StreamLoadClient.DataQualityException rejected = assertThrows(StreamLoadClient.DataQualityException.class,
                () -> client.load("audit_lineage_db", "audit_user_dict", "it_dq", StarRocksSink.DICT_COLUMNS, body,
                        false));
        assertEquals(1, rejected.getFilteredRows());
        assertFalse(rejected.getErrorUrl().isEmpty(), "StarRocks error-log URL is kept");
        assertEquals(0L, count("SELECT COUNT(*) c FROM audit_lineage_db.audit_user_dict"));
        assertTrue(client.loadState("audit_lineage_db", "it_dq") != StreamLoadClient.LoadState.COMMITTED);

        StreamLoadClient.Result tolerant = client.load("audit_lineage_db", "audit_user_dict", "it_dq",
                StarRocksSink.DICT_COLUMNS, body, true);
        assertEquals(2, tolerant.getLoadedRows(), "the aborted label is reusable");
        assertEquals(1, tolerant.getFilteredRows());
        assertEquals(2L, count("SELECT COUNT(*) c FROM audit_lineage_db.audit_user_dict"));
    }

    /** Gap 2 through the sink: rows rejected in a usage load leave a LOAD_FILTERED dead letter. */
    @Test
    void sinkWritesLoadFilteredEvidenceForRejectedUsageRows() {
        sr.execute("DROP DATABASE IF EXISTS it_strict FORCE", "CREATE DATABASE it_strict");
        for (String ddl : Arrays.asList("03_audit_column_hourly_usage.sql", "08_audit_lineage_dead_letter.sql")) {
            sr.executeScript(StarRocksCluster.read(StarRocksCluster.projectDir().resolve("sql/ddl/" + ddl))
                    .replace("audit_lineage_db.", "it_strict.")
                    .replace("\"replication_num\" = \"3\"", "\"replication_num\" = \"1\"")
                    // a stricter column so StarRocks itself rejects long names
                    .replace("column_name        VARCHAR(256)", "column_name        VARCHAR(4)"));
        }
        StarRocksSink strict = new StarRocksSink(sr.dataSource(), client, "it_strict", P, Clock.systemUTC());
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        agg.add(record("q1", hour.plusMinutes(1), "alice", 1, 1), query("ok", "much_too_long"));
        StreamLoadClient.Result r = strict.writeUsage("it_strict1", agg.drain());
        assertEquals(1, r.getLoadedRows());
        assertEquals(1, r.getFilteredRows());
        List<Map<String, Object>> letters = sr.query("SELECT query_id, kind, batch_id, detail "
                + "FROM it_strict.audit_lineage_dead_letter");
        assertEquals(1, letters.size());
        assertEquals("it_strict1_usage", letters.get(0).get("query_id"));
        assertEquals("LOAD_FILTERED", letters.get(0).get("kind"));
        assertEquals("it_strict1", letters.get(0).get("batch_id"));
        assertTrue(String.valueOf(letters.get(0).get("detail")).startsWith("http"));
        sr.execute("DROP DATABASE it_strict FORCE");
    }

    // ------------------------------------------------------------------------------------------------ usage

    @Test
    void usageRowsGiveExactBitmapUvAndApproximateHllAcrossHours() {
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        String[] users = {"alice", "bob", "carol", "alice"};
        for (int i = 0; i < users.length; i++) {
            agg.add(record("q" + i, hour.plusMinutes(5 + i), users[i], 100L * (i + 1), 10), query("amount"));
        }
        agg.add(record("q9", hour.minusHours(1).plusMinutes(1), "dave", 7, 1), query("amount"));
        List<HourlyUsageRow> rows = agg.drain();
        sink.writeUserDictionary("it_b1", rows);
        sink.writeUsage("it_b1", rows);
        sink.writeUsage("it_b1", rows);
        sink.writeUserDictionary("it_b2", rows);
        assertEquals(StreamLoadClient.LoadState.COMMITTED, sink.loadState(sink.label("it_b1", "usage")));

        Map<String, Object> thisHour = sr.query("SELECT SUM(access_query_cnt) access, SUM(select_clause_cnt) sel, "
                + "SUM(where_clause_cnt) wh, BITMAP_UNION_COUNT(user_bitmap) uv, HLL_UNION_AGG(user_hll) uv_hll, "
                + "MAX(max_query_time_ms) mx, SUM(total_scan_bytes) scan "
                + "FROM audit_lineage_db.audit_column_hourly_usage "
                + "WHERE stat_hour = ? AND column_name = 'amount'", hour).get(0);
        assertEquals(4, num(thisHour.get("access")), "the replayed load (same label) added nothing");
        assertEquals(4, num(thisHour.get("sel")));
        assertEquals(4, num(thisHour.get("wh")));
        assertEquals(3, num(thisHour.get("uv")), "alice twice, bob, carol");
        assertEquals(3, num(thisHour.get("uv_hll")));
        assertEquals(400, num(thisHour.get("mx")));
        assertEquals(40, num(thisHour.get("scan")));

        Map<String, Object> twoHours = sr.query("SELECT SUM(access_query_cnt) access, "
                + "BITMAP_UNION_COUNT(user_bitmap) uv FROM audit_lineage_db.audit_column_hourly_usage "
                + "WHERE column_name = 'amount'").get(0);
        assertEquals(5, num(twoHours.get("access")));
        assertEquals(4, num(twoHours.get("uv")), "exact UV rolls up across hours");

        // business query Q5 shape: decode the 63-bit ids of the bitmap through the dictionary
        List<Map<String, Object>> names = sr.query("SELECT d.user_name, d.user_id FROM (SELECT "
                + "BITMAP_UNION(user_bitmap) bm FROM audit_lineage_db.audit_column_hourly_usage WHERE stat_hour = ?) b "
                + "CROSS JOIN LATERAL UNNEST(BITMAP_TO_ARRAY(b.bm)) AS t(user_id) "
                + "JOIN audit_lineage_db.audit_user_dict d ON d.user_id = t.user_id ORDER BY d.user_name", hour);
        assertEquals(3, names.size());
        assertEquals("alice", names.get(0).get("user_name"));
        assertEquals(UserIds.idOf("alice"), num(names.get(0).get("user_id")));
        assertEquals("carol", names.get(2).get("user_name"));
        assertEquals(4L, count("SELECT COUNT(*) c FROM audit_lineage_db.audit_user_dict"),
                "dictionary rewrites aggregate into one row per user");
        assertTrue(sink.userIdCollisions().isEmpty());

        sink.purgeUsage(hour.minusHours(1), hour);
        assertEquals(4L, count("SELECT SUM(access_query_cnt) FROM audit_lineage_db.audit_column_hourly_usage"),
                "rebuild purge is a key-range delete of whole hours");
    }

    @Test
    void usageOfYesterdayAndLastMonthIsAccepted() {
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        agg.add(record("q1", hour.minusDays(1), "alice", 1, 1), query("amount"));
        agg.add(record("q2", hour.minusDays(35), "alice", 1, 1), query("amount"));
        sink.writeUsage("it_past", agg.drain());
        assertEquals(2L, count("SELECT SUM(access_query_cnt) FROM audit_lineage_db.audit_column_hourly_usage"),
                "gap 1: expression partitions are created by the load");
    }

    // ------------------------------------------------------------------------------------------------ lineage

    @Test
    void dailyFactsAndLineageRefreshOnTheRealEngine() {
        ColumnIdentifier tgt = ColumnIdentifier.parse("default_catalog.dw.t.a");
        ColumnIdentifier src = ColumnIdentifier.parse("default_catalog.ods.s.a");
        SqlParseResult insert = SqlParseResult.builder("x").statementType(StatementType.INSERT_INTO)
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.IDENTITY, "a"))
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.INDIRECT, TransformType.IDENTITY, null)).build();
        String direct = ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.IDENTITY, null).edgeId();
        String indirect = ColumnLineageEdge.of(tgt, src, LineageType.INDIRECT, TransformType.IDENTITY, null).edgeId();

        LineageAggregator first = new LineageAggregator();
        first.add(record("q1", hour.minusDays(2), "etl", 1, 1), insert);
        first.add(record("q2", hour.minusDays(1), "etl", 1, 1), insert);
        first.add(record("q3", hour.minusDays(1).plusMinutes(1), "etl", 1, 1), insert);
        writeBatch("it_l1", first.drain());
        Map<String, Object> row = lineage(direct);
        assertEquals(3, num(row.get("exec_count")));
        assertEquals("q3", row.get("last_query_id"));
        assertEquals(hour.minusDays(2), row.get("first_seen_time"));
        assertEquals("IDENTITY", row.get("transform_type"));
        assertEquals("ods", row.get("source_db"));
        assertEquals("dw", row.get("target_db"));

        // replay of every step: labels and the idempotent refresh keep it exact
        sink.insertDaily("it_l1");
        sink.refreshLineage("it_l1");
        sink.refreshLineage("it_l1");
        assertEquals(3, num(lineage(direct).get("exec_count")));
        assertEquals(StreamLoadClient.LoadState.COMMITTED, sink.loadState(sink.label("it_l1", "daily")),
                "a labelled INSERT has a load state too");

        LineageAggregator second = new LineageAggregator();
        second.add(record("q4", hour, "bob", 1, 1), SqlParseResult.builder("y")
                .statementType(StatementType.INSERT_OVERWRITE)
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.EXPRESSION, "a + 1"))
                .build());
        writeBatch("it_l2", second.drain());
        Map<String, Object> merged = lineage(direct);
        assertEquals(4, num(merged.get("exec_count")));
        assertEquals("EXPRESSION", merged.get("transform_type"), "attributes of the latest day");
        assertEquals("INSERT_OVERWRITE", merged.get("stmt_type"));
        assertEquals("bob", merged.get("last_exec_user"));
        assertEquals(3, num(lineage(indirect).get("exec_count")), "an edge the batch did not touch is unchanged");
        assertEquals(3L, count("SELECT COUNT(*) FROM audit_lineage_db.audit_column_lineage_daily "
                + "WHERE edge_id = ?", direct), "one fact row per (day, edge)");

        // rebuild of the two older days: record the edges, purge the facts, recompute (here: nothing left)
        LocalDateTime from = today.minusDays(2);
        sink.recordEdgesForRefresh("it_rb", hour, from, today);
        sink.purgeLineageDaily(from, today);
        sink.insertDaily("it_rb");
        sink.refreshLineage("it_rb");
        assertEquals(1, num(lineage(direct).get("exec_count")), "only today's fact remains");
        assertTrue(sr.query("SELECT * FROM audit_lineage_db.audit_column_lineage WHERE edge_id = ?", indirect)
                .isEmpty(), "an edge without any fact is deleted");
        assertEquals(0L, count("SELECT COUNT(*) FROM audit_lineage_db.audit_column_lineage_daily "
                + "WHERE stat_day < ?", today), "refresh-only rows are never added to the facts");
    }

    private void writeBatch(String batchId, List<LineageRow> rows) {
        sink.writeBatchLineage(batchId, hour, rows);
        sink.writeBatchLineage(batchId, hour, rows);
        sink.insertDaily(batchId);
        sink.refreshLineage(batchId);
    }

    private Map<String, Object> lineage(String edgeId) {
        return sr.query("SELECT * FROM audit_lineage_db.audit_column_lineage WHERE edge_id = ?", edgeId).get(0);
    }

    // ------------------------------------------------------------------------------------------------ dead letters

    @Test
    void deadLettersRoundTripWithTheirAuditRecord() {
        AuditLogRecord r = AuditLogRecord.builder().queryId("dl1").timestamp(hour.plusMinutes(3)).user("名字")
                .authorizedUser("'名字'@'%'").state("EOF").errorCode("").catalog("default_catalog").db("sales")
                .stmt("SELEC broken -- " + new String(new char[3000]).replace('\0', 'x')).queryType("query")
                .clientIp("10.0.0.1").queryTime(12).scanBytes(34).scanRows(5).returnRows(6).digest("dg")
                .isQuery(true).build();
        DeadLetter open = DeadLetter.ofRecord(r, DeadLetter.Kind.PARSE_ERROR, "b1", "mismatched input");
        sink.writeDeadLetters("b1", "failed_x", Collections.singletonList(open));
        sink.writeDeadLetters("b1", "skipped", Collections.singletonList(new DeadLetter("b1",
                DeadLetter.Kind.BATCH_SKIPPED, DeadLetter.Status.OPEN, "b1", null, hour, "operator", "SLICE b1", null)));

        List<DeadLetter> read = sink.deadLetters(EnumSet.of(DeadLetter.Status.OPEN),
                EnumSet.of(DeadLetter.Kind.PARSE_ERROR, DeadLetter.Kind.ANALYSIS_ERROR), hour, hour.plusHours(1),
                null, 10);
        assertEquals(1, read.size());
        AuditLogRecord back = read.get(0).getRecord();
        assertEquals(r.getStmt(), back.getStmt(), "statements up to 1 MB are kept whole");
        assertEquals("名字", back.getUser());
        assertEquals("sales", back.getDb());
        assertEquals(34, back.getScanBytes());
        assertTrue(back.isQuery());
        assertEquals("mismatched input", read.get(0).getErrorMessage());
        assertEquals(2, sink.deadLettersOfBatch("b1", hour.minusDays(1), hour.plusDays(1)).size());

        sink.writeDeadLetters("rep", "reserve", Collections.singletonList(open.with(DeadLetter.Status.REPAIRING,
                "rep", null)));
        assertTrue(sink.deadLetters(EnumSet.of(DeadLetter.Status.OPEN), EnumSet.of(DeadLetter.Kind.PARSE_ERROR),
                hour, hour.plusHours(1), null, 10).isEmpty(), "upsert on (pipeline, query_id, kind)");
        List<DeadLetter> reserved = sink.deadLetters(EnumSet.of(DeadLetter.Status.REPAIRING),
                EnumSet.of(DeadLetter.Kind.PARSE_ERROR), hour, hour.plusHours(1), "rep", 10);
        assertEquals(1, reserved.size());
        assertEquals("b1", reserved.get(0).getBatchId());
        sink.writeDeadLetters("rep", "resolved", Collections.singletonList(reserved.get(0)
                .with(DeadLetter.Status.RESOLVED, "rep", "repaired")));
        assertEquals(1L, count("SELECT COUNT(*) FROM audit_lineage_db.audit_lineage_dead_letter "
                + "WHERE status = 'RESOLVED' AND repair_batch_id = 'rep'"));
        assertTrue(sink.deadLetters(EnumSet.noneOf(DeadLetter.Status.class), EnumSet.of(DeadLetter.Kind.PARSE_ERROR),
                hour, hour.plusHours(1), null, 10).isEmpty());
    }

    // ------------------------------------------------------------------------------------------------ batch log

    @Test
    void batchLogIsUpserted() {
        BatchLogEntry e = new BatchLogEntry("bl1", BatchKind.SLICE, hour, hour.plusMinutes(5), 10, 7, 1, 2, 0, 3, 4,
                2, 1, 1, hour, hour.plusMinutes(6));
        sink.writeBatchLog(e);
        sink.writeBatchLog(e);
        Map<String, Object> row = sr.query("SELECT * FROM audit_lineage_db.audit_lineage_batch_log").get(0);
        assertEquals("SLICE", row.get("kind"));
        assertEquals(10, num(row.get("records")));
        assertEquals(P, row.get("pipeline_name"));
        assertEquals(1L, count("SELECT COUNT(*) FROM audit_lineage_db.audit_lineage_batch_log"));
    }

    // ------------------------------------------------------------------------------------------------ checkpoint

    @Test
    void checkpointStepsLeaseAndCommitOnTheRealPrimaryKeyTable() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).withNano(0);
        LocalDateTime start = today.minusDays(1);
        JdbcCheckpointStore a = store(now);
        assertTrue(a.tryAcquireLease("it_p", "node-a", Duration.ofMinutes(10), start));
        CheckpointStore.PendingBatch pending = CheckpointStore.PendingBatch.declare("it_batch", BatchKind.SLICE,
                start, start.plusMinutes(5), now);
        a.savePending("it_p", "node-a", Duration.ofMinutes(10), pending);
        a.advanceStep("it_p", "node-a", Duration.ofMinutes(10), "it_batch", BatchStep.USAGE);
        a.recordFailure("it_p", "node-a", Duration.ofMinutes(10), "it_batch", "boom");
        CheckpointStore.PendingBatch stored = a.load("it_p").get().getPending().get();
        assertEquals(BatchStep.USAGE, stored.getStep());
        assertEquals(1, stored.getAttempts());
        assertEquals("boom", stored.getLastError().get());

        assertFalse(store(now.plusMinutes(5)).tryAcquireLease("it_p", "node-b", Duration.ofMinutes(10), start));
        JdbcCheckpointStore b = store(now.plusMinutes(11));
        assertTrue(b.tryAcquireLease("it_p", "node-b", Duration.ofMinutes(10), start));
        assertThrows(LeaseLostException.class, () -> store(now.plusMinutes(12)).advanceStep("it_p", "node-a",
                Duration.ofMinutes(10), "it_batch", BatchStep.REFRESH), "the paused node-a is fenced");
        b.commit("it_p", "node-b", Duration.ofMinutes(10), "it_batch", start.plusMinutes(5), 9);
        b.markReconciled("it_p", "node-b", Duration.ofMinutes(10), now);
        CheckpointStore.Checkpoint cp = b.load("it_p").get();
        assertEquals(start.plusMinutes(5), cp.getCommittedUntil());
        assertEquals(start, cp.getCoverageStart());
        assertFalse(cp.getPending().isPresent());
        assertEquals(9, cp.getProcessedRows());
        assertEquals(now, cp.getLastReconcile().get());
        assertEquals(1L, count("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_sync_checkpoint "
                + "WHERE pipeline_name = 'it_p'"), "PRIMARY KEY upsert, never appended");
    }

    private JdbcCheckpointStore store(LocalDateTime at) {
        return new JdbcCheckpointStore(sr.dataSource(), "audit_lineage_db.audit_lineage_sync_checkpoint",
                Clock.fixed(at.toInstant(ZoneOffset.UTC), ZoneId.of("UTC")), "INSERT");
    }

    // ------------------------------------------------------------------------------------------------ audit source

    @Test
    void auditSourcePagesMembershipJoinsAndAntiJoins() {
        LocalDateTime t = today.minusDays(1).plusHours(10);
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < 7; i++) {
            if (values.length() > 0) {
                values.append(',');
            }
            values.append(String.format("('k%02d', '%s', 'Query', '10.0.0.1', 'u%d', 'u', 'rg', 'default_catalog', "
                    + "'sales', '%s', '%s', 12, 34, 5, 1, 0, 0, 1, 1, 'fe', 'SELECT %d', 'dg', 0, 0)", i,
                    (i < 4 ? t : t.plusSeconds(1)).toString().replace('T', ' '), i, i == 6 ? "ERR" : "EOF",
                    i == 6 ? "1064" : "", i));
        }
        sr.execute("INSERT INTO starrocks_audit_db__.starrocks_audit_tbl__ VALUES " + values);
        JdbcAuditLogSource source = new JdbcAuditLogSource(sr.dataSource());
        LocalDateTime from = t.minusMinutes(1);
        LocalDateTime to = t.plusMinutes(4);

        List<AuditLogRecord> all = new ArrayList<>();
        Watermark cursor = Watermark.startingAt(from);
        while (true) {
            List<AuditLogRecord> page = source.fetchRange(from, to, cursor, 3);
            all.addAll(page);
            if (page.size() < 3) {
                break;
            }
            AuditLogRecord last = page.get(page.size() - 1);
            cursor = new Watermark(last.getTimestamp(), last.getQueryId());
        }
        assertEquals(7, all.size(), "yesterday's audit rows, equal timestamps paged exactly once");
        for (int i = 0; i < 7; i++) {
            assertEquals(String.format("k%02d", i), all.get(i).getQueryId());
        }
        AuditLogRecord failed = all.get(6);
        assertTrue(failed.isFailed());
        assertEquals("default_catalog", failed.getCatalog());
        assertEquals(12, all.get(0).getQueryTime());
        assertEquals(34, all.get(0).getScanBytes());
        assertTrue(all.get(0).isQuery());
        assertEquals("SELECT 0", all.get(0).getStmt());
        assertTrue(source.fetchRange(from, t, Watermark.startingAt(from), 10).isEmpty(), "upper bound exclusive");

        sink.writeMembers("m1", hour, all.subList(0, 3));
        sink.writeMembers("m2", hour, all.subList(3, 5));
        assertEquals(Arrays.asList("k00", "k01", "k02"), ids(source.fetchMembers(P, "m1", from, to,
                Watermark.startingAt(from), 10)));
        assertEquals(Arrays.asList("k03", "k04"), ids(source.fetchMembers(P, "m2", from, to,
                Watermark.startingAt(from), 10)));
        assertEquals(Arrays.asList("k00", "k01", "k02", "k03", "k04"), ids(source.fetchProcessed(P, from, to,
                Watermark.startingAt(from), 10)));
        assertEquals(Arrays.asList("k05", "k06"), ids(source.fetchUnprocessed(P, from, to, 10)),
                "gap 6: late records are the anti join with the membership");
        assertEquals(7, source.fetchUnprocessed("other_pipeline", from, to, 10).size(),
                "membership is per pipeline");
        assertEquals(5, source.countMembers(P, from, to));
        assertEquals(5, source.countProcessed(P, from, to));
        sr.execute("DELETE FROM starrocks_audit_db__.starrocks_audit_tbl__ WHERE queryId = 'k00' AND `timestamp` = '"
                + t.toString().replace('T', ' ') + "'");
        assertEquals(4, source.countProcessed(P, from, to), "gap 7: rebuild detects audit records that are gone");
    }

    private static List<String> ids(List<AuditLogRecord> records) {
        return records.stream().map(AuditLogRecord::getQueryId).collect(Collectors.toList());
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private static SqlParseResult query(String... columns) {
        SqlParseResult.Builder b = SqlParseResult.builder("q").statementType(StatementType.QUERY);
        for (String c : columns) {
            ColumnIdentifier col = ColumnIdentifier.parse("default_catalog.sales.orders." + c);
            b.recordUsage(col, ClauseType.SELECT).recordUsage(col, ClauseType.WHERE);
        }
        return b.build();
    }

    private static AuditLogRecord record(String id, LocalDateTime ts, String user, long queryTime, long scanBytes) {
        return AuditLogRecord.builder().queryId(id).timestamp(ts).user(user).state("EOF").errorCode("")
                .queryTime(queryTime).scanBytes(scanBytes).catalog("default_catalog").db("sales").stmt("x")
                .digest("dg").build();
    }
}
