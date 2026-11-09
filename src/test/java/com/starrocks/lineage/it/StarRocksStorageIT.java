package com.starrocks.lineage.it;

import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.pipeline.CheckpointStore;
import com.starrocks.lineage.pipeline.HourlyUsageAggregator;
import com.starrocks.lineage.pipeline.JdbcAuditLogSource;
import com.starrocks.lineage.pipeline.JdbcCheckpointStore;
import com.starrocks.lineage.pipeline.LineageAggregator;
import com.starrocks.lineage.pipeline.LineageRow;
import com.starrocks.lineage.pipeline.PipelineException;
import com.starrocks.lineage.pipeline.StarRocksSink;
import com.starrocks.lineage.pipeline.StreamLoadClient;
import com.starrocks.lineage.pipeline.UserDictionary;
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage layer on a real StarRocks 3.1: Stream Load, usage bitmap / HLL, PRIMARY KEY merge, checkpoint, audit read.
 */
@Testcontainers(disabledWithoutDocker = true)
class StarRocksStorageIT {

    private static StarRocksCluster sr;
    private StreamLoadClient client;
    private StarRocksSink sink;
    private final LocalDateTime hour = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS);

    @BeforeAll
    static void start() {
        sr = StarRocksCluster.get();
    }

    @BeforeEach
    void reset() {
        sr.resetPipelineTables();
        client = sr.streamLoadClient();
        sink = new StarRocksSink(sr.dataSource(), client, "audit_lineage_db", 2, "INSERT", Clock.systemUTC());
    }

    @AfterEach
    void close() throws Exception {
        client.close();
    }

    private static long num(Object o) {
        return o instanceof BigDecimal ? ((BigDecimal) o).longValue() : ((Number) o).longValue();
    }

    // ------------------------------------------------------------------------------------------------ stream load

    @Test
    void streamLoadThroughFeRedirectIsIdempotentPerLabel() {
        byte[] body = ("[{\"user_name\":\"it_a\",\"user_id\":1,\"first_seen_time\":\"2024-06-18 09:00:00\"}]")
                .getBytes(StandardCharsets.UTF_8);
        StreamLoadClient.Result first = client.load("audit_lineage_db", "audit_user_dict", "it_dict_label",
                "user_name,user_id,first_seen_time", body);
        assertFalse(first.isAlreadyLoaded());
        assertEquals(1, first.getLoadedRows());
        StreamLoadClient.Result again = client.load("audit_lineage_db", "audit_user_dict", "it_dict_label",
                "user_name,user_id,first_seen_time", body);
        assertTrue(again.isAlreadyLoaded(), "real 'Label Already Exists' + ExistingJobStatus FINISHED");
        assertEquals(1L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_user_dict").get(0).get("c")));
    }

    @Test
    void realLoadFailuresAreReported() {
        PipelineException e = assertThrows(PipelineException.class, () -> client.load("audit_lineage_db",
                "audit_user_dict", "it_bad_json", "user_name,user_id,first_seen_time",
                "not json".getBytes(StandardCharsets.UTF_8)));
        assertTrue(e.getMessage().contains("failed"), e.getMessage());
        PipelineException unknownTable = assertThrows(PipelineException.class, () -> client.load(
                "audit_lineage_db", "no_such_table", "it_no_table", "a", "[]".getBytes(StandardCharsets.UTF_8)));
        assertTrue(unknownTable.getMessage().contains("no_such_table"), unknownTable.getMessage());
    }

    // ------------------------------------------------------------------------------------------------ usage

    @Test
    void usageRowsGiveExactBitmapUvAndApproximateHllAcrossHours() {
        ColumnIdentifier amount = ColumnIdentifier.parse("default_catalog.sales.orders.amount");
        SqlParseResult q = SqlParseResult.builder("q").statementType(StatementType.QUERY)
                .recordUsage(amount, ClauseType.SELECT).recordUsage(amount, ClauseType.WHERE).build();
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        String[] users = {"alice", "bob", "carol", "alice"};
        for (int i = 0; i < users.length; i++) {
            agg.add(record("q" + i, hour.plusMinutes(5 + i), users[i], 100L * (i + 1), 10), q);
        }
        agg.add(record("q9", hour.minusHours(1).plusMinutes(1), "dave", 7, 1), q);
        UserDictionary dict = new UserDictionary(sink.loadUserDictionary());
        sink.writeUsage("it_b1", agg.drain(), dict);
        sink.writeUserDictionary("it_b1", dict.drainPending(), hour);
        // retried batch: labels it_b1_usage_0.. already exist -> nothing doubled
        HourlyUsageAggregator again = new HourlyUsageAggregator();
        for (int i = 0; i < users.length; i++) {
            again.add(record("q" + i, hour.plusMinutes(5 + i), users[i], 100L * (i + 1), 10), q);
        }
        again.add(record("q9", hour.minusHours(1).plusMinutes(1), "dave", 7, 1), q);
        sink.writeUsage("it_b1", again.drain(), dict);

        Map<String, Object> thisHour = sr.query("SELECT SUM(access_query_cnt) access, SUM(select_clause_cnt) sel, "
                + "SUM(where_clause_cnt) wh, BITMAP_UNION_COUNT(user_bitmap) uv, HLL_UNION_AGG(user_hll) uv_hll, "
                + "MAX(max_query_time_ms) mx, SUM(total_scan_bytes) scan "
                + "FROM audit_lineage_db.audit_column_hourly_usage "
                + "WHERE stat_hour = ? AND column_name = 'amount'", hour).get(0);
        assertEquals(4, num(thisHour.get("access")));
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

        // business query Q5 shape: decode the bitmap through the dictionary
        List<Map<String, Object>> names = sr.query("SELECT d.user_name FROM (SELECT BITMAP_UNION(user_bitmap) bm "
                + "FROM audit_lineage_db.audit_column_hourly_usage WHERE stat_hour = ?) b "
                + "CROSS JOIN LATERAL UNNEST(BITMAP_TO_ARRAY(b.bm)) AS t(user_id) "
                + "JOIN audit_lineage_db.audit_user_dict d ON d.user_id = t.user_id ORDER BY d.user_name", hour);
        assertEquals(3, names.size());
        assertEquals("alice", names.get(0).get("user_name"));
        assertEquals("carol", names.get(2).get("user_name"));
    }

    // ------------------------------------------------------------------------------------------------ lineage

    @Test
    void lineageMergeOnRealPrimaryKeyTableAccumulatesAndIgnoresReplays() {
        ColumnIdentifier tgt = ColumnIdentifier.parse("default_catalog.dw.t.a");
        ColumnIdentifier src = ColumnIdentifier.parse("default_catalog.ods.s.a");
        LineageAggregator first = new LineageAggregator();
        SqlParseResult insert = SqlParseResult.builder("x").statementType(StatementType.INSERT_INTO)
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.IDENTITY, "a"))
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.INDIRECT, TransformType.IDENTITY, null)).build();
        first.add(record("q1", hour.minusDays(2), "etl", 1, 1), insert);
        first.add(record("q2", hour.minusDays(1), "etl", 1, 1), insert);
        List<LineageRow> b1 = first.drain();
        sink.writeLineage("it_l1", b1);
        String edgeId = ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.IDENTITY, null).edgeId();
        Map<String, Object> row = lineage(edgeId);
        assertEquals(2, num(row.get("exec_count")));
        assertEquals("q2", row.get("last_query_id"));

        sink.writeLineage("it_l1", b1);
        assertEquals(0, mergeAgain("it_l1"), "replayed batch merges nothing");
        assertEquals(2, num(lineage(edgeId).get("exec_count")));

        LineageAggregator second = new LineageAggregator();
        second.add(record("q3", hour, "bob", 1, 1), SqlParseResult.builder("y")
                .statementType(StatementType.INSERT_OVERWRITE)
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.EXPRESSION, "a + 1"))
                .build());
        sink.writeLineage("it_l2", second.drain());
        Map<String, Object> merged = lineage(edgeId);
        assertEquals(3, num(merged.get("exec_count")));
        assertEquals("EXPRESSION", merged.get("transform_type"));
        assertEquals("INSERT_OVERWRITE", merged.get("stmt_type"));
        assertEquals("bob", merged.get("last_exec_user"));
        assertEquals("it_l2", merged.get("last_batch_id"));
        assertEquals(2L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_column_lineage").get(0).get("c")),
                "one row per edge id (DIRECT + INDIRECT), upserted - never duplicated");
        assertEquals(2L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_merge_log")
                .get(0).get("c")));
    }

    private int mergeAgain(String batchId) {
        try {
            java.lang.reflect.Method m = StarRocksSink.class.getDeclaredMethod("mergeLineage", String.class);
            m.setAccessible(true);
            return (Integer) m.invoke(sink, batchId);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<String, Object> lineage(String edgeId) {
        return sr.query("SELECT * FROM audit_lineage_db.audit_column_lineage WHERE edge_id = ?", edgeId).get(0);
    }

    // ------------------------------------------------------------------------------------------------ checkpoint

    @Test
    void checkpointUpsertsLeaseAndIntentOnTheRealPrimaryKeyTable() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).withNano(0);
        JdbcCheckpointStore a = store(now);
        Watermark start = Watermark.startingAt(now.minusDays(1));
        assertTrue(a.tryAcquireLease("it_p", "node-a", Duration.ofMinutes(10), start));
        CheckpointStore.PendingBatch pending = new CheckpointStore.PendingBatch("it_batch",
                new Watermark(now.minusMinutes(30), "q-end"));
        a.savePending("it_p", "node-a", Duration.ofMinutes(10), pending);
        assertEquals(pending, a.load("it_p").get().getPending().get());
        assertFalse(store(now.plusMinutes(5)).tryAcquireLease("it_p", "node-b", Duration.ofMinutes(10), start));
        JdbcCheckpointStore b = store(now.plusMinutes(11));
        assertTrue(b.tryAcquireLease("it_p", "node-b", Duration.ofMinutes(10), start));
        b.commit("it_p", "node-b", Duration.ofMinutes(10), pending.getEnd(), 9);
        CheckpointStore.Checkpoint cp = b.load("it_p").get();
        assertEquals(pending.getEnd(), cp.getWatermark());
        assertFalse(cp.getPending().isPresent());
        assertEquals(9, cp.getProcessedRows());
        assertEquals(1L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_sync_checkpoint "
                + "WHERE pipeline_name = 'it_p'").get(0).get("c")), "PRIMARY KEY upsert, never appended");
    }

    private JdbcCheckpointStore store(LocalDateTime at) {
        return new JdbcCheckpointStore(sr.dataSource(), "audit_lineage_db.audit_lineage_sync_checkpoint",
                Clock.fixed(at.toInstant(ZoneOffset.UTC), ZoneId.of("UTC")), "INSERT");
    }

    // ------------------------------------------------------------------------------------------------ audit source

    @Test
    void auditSourcePagesThroughEqualTimestampsExactlyOnce() {
        LocalDateTime t = StarRocksCluster.todayMinutesAgo(30, 2);
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
        List<AuditLogRecord> all = new ArrayList<>();
        Watermark cursor = Watermark.startingAt(t.minusMinutes(1));
        LocalDateTime upper = LocalDateTime.now(ZoneOffset.UTC);
        while (true) {
            List<AuditLogRecord> page = source.fetch(cursor, upper, 3);
            all.addAll(page);
            if (page.size() < 3) {
                break;
            }
            AuditLogRecord last = page.get(page.size() - 1);
            cursor = new Watermark(last.getTimestamp(), last.getQueryId());
        }
        assertEquals(7, all.size());
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
        assertTrue(source.fetch(Watermark.startingAt(t.minusMinutes(1)), t, 10).isEmpty(), "upper bound respected");
    }

    private static AuditLogRecord record(String id, LocalDateTime ts, String user, long queryTime, long scanBytes) {
        return AuditLogRecord.builder().queryId(id).timestamp(ts).user(user).state("EOF").errorCode("")
                .queryTime(queryTime).scanBytes(scanBytes).catalog("default_catalog").db("sales").stmt("x")
                .digest("dg").build();
    }
}
