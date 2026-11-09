package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TransformType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregatorsTest {

    private static final LocalDateTime T0 = LocalDateTime.of(2024, 6, 18, 9, 15);

    static AuditLogRecord record(String id, LocalDateTime ts, String user, long queryTime, long scanBytes) {
        return AuditLogRecord.builder().queryId(id).timestamp(ts).user(user).state("EOF").errorCode("")
                .queryTime(queryTime).scanBytes(scanBytes).digest("d-" + id).catalog("default_catalog").db("db")
                .stmt("SELECT 1").build();
    }

    static SqlParseResult query(Map<ColumnIdentifier, ClauseType[]> usage) {
        SqlParseResult.Builder b = SqlParseResult.builder("q").statementType(StatementType.QUERY);
        usage.forEach((c, clauses) -> {
            for (ClauseType t : clauses) {
                b.recordUsage(c, t);
            }
        });
        return b.build();
    }

    static ColumnIdentifier col(String c) {
        return ColumnIdentifier.parse("default_catalog.db.t." + c);
    }

    /** "Aa" and "BB" have the same String.hashCode(): the keys must still be separate rows (review R4-04). */
    @Test
    void hashCollidingKeysStaySeparate() {
        assertEquals("Aa".hashCode(), "BB".hashCode());
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        Map<ColumnIdentifier, ClauseType[]> u = new HashMap<>();
        u.put(col("Aa"), new ClauseType[]{ClauseType.SELECT});
        u.put(col("BB"), new ClauseType[]{ClauseType.WHERE});
        agg.add(record("1", T0, "Aa", 10, 1), query(u));
        agg.add(record("2", T0, "BB", 10, 1), query(u));
        List<HourlyUsageRow> rows = agg.drain();
        assertEquals(4, rows.size(), "2 columns x 2 users, despite identical hash codes");
        for (HourlyUsageRow r : rows) {
            assertEquals(1, r.getAccessQueryCnt());
            assertEquals(r.getColumn().getColumn().equals("aa") ? 1 : 0, r.getSelectClauseCnt());
        }
    }

    @Test
    void countersMaxAndBucketing() {
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        Map<ColumnIdentifier, ClauseType[]> u = Collections.singletonMap(col("a"),
                new ClauseType[]{ClauseType.SELECT, ClauseType.SELECT, ClauseType.HAVING});
        agg.add(record("1", T0, "u", 100, 10), query(u));
        agg.add(record("2", T0.plusMinutes(44), "u", 700, 5), query(u));
        agg.add(record("3", T0.plusMinutes(45), "u", 1, 1), query(u));
        assertEquals(2, agg.size());
        List<HourlyUsageRow> rows = agg.drain();
        HourlyUsageRow h9 = rows.get(0);
        assertEquals(LocalDateTime.of(2024, 6, 18, 9, 0), h9.getStatHour());
        assertEquals(2, h9.getAccessQueryCnt());
        assertEquals(2, h9.getSelectClauseCnt(), "per query de-duplicated, then summed");
        assertEquals(2, h9.getGroupByCnt(), "HAVING feeds group_by_cnt");
        assertEquals(15, h9.getTotalScanBytes());
        assertEquals(800, h9.getTotalQueryTimeMs());
        assertEquals(700, h9.getMaxQueryTimeMs());
        assertEquals(T0.plusMinutes(44), h9.getLastAccessTime());
        assertEquals(LocalDateTime.of(2024, 6, 18, 10, 0), rows.get(1).getStatHour());
        assertEquals(0, agg.size(), "drain resets");
        assertEquals(0, agg.add(record("4", T0, "u", 1, 1), SqlParseResult.of("x", ParseStatus.PARSE_ERROR, "e")));
    }

    /** 16 threads x 2 000 queries while another thread drains repeatedly: no update is lost or split. */
    @Test
    void concurrentAddsAndDrainsLoseNothing() throws Exception {
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        Map<ColumnIdentifier, ClauseType[]> u = new HashMap<>();
        for (int i = 0; i < 8; i++) {
            u.put(col("c" + i), new ClauseType[]{ClauseType.SELECT, ClauseType.WHERE});
        }
        SqlParseResult r = query(u);
        int threads = 16;
        int perThread = 2_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        List<HourlyUsageRow> drained = Collections.synchronizedList(new ArrayList<>());
        try {
            List<Future<?>> writers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                writers.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        agg.add(record(tid + "-" + i, T0.plusSeconds(i % 3600), "user" + (i % 5), 10, 2), r);
                    }
                    return null;
                }));
            }
            Future<?> drainer = pool.submit(() -> {
                start.await();
                while (running.get()) {
                    drained.addAll(agg.drain());
                }
                return null;
            });
            start.countDown();
            for (Future<?> w : writers) {
                w.get(60, TimeUnit.SECONDS);
            }
            running.set(false);
            drainer.get(60, TimeUnit.SECONDS);
            drained.addAll(agg.drain());
        } finally {
            pool.shutdownNow();
        }
        long expected = (long) threads * perThread;
        for (int i = 0; i < 8; i++) {
            ColumnIdentifier c = col("c" + i);
            long access = 0;
            long select = 0;
            long where = 0;
            long scan = 0;
            for (HourlyUsageRow row : drained) {
                if (row.getColumn().equals(c)) {
                    access += row.getAccessQueryCnt();
                    select += row.getSelectClauseCnt();
                    where += row.getWhereClauseCnt();
                    scan += row.getTotalScanBytes();
                }
            }
            assertEquals(expected, access, "access of " + c);
            assertEquals(expected, select);
            assertEquals(expected, where);
            assertEquals(expected * 2, scan);
        }
    }

    @Test
    void lineageAggregationFoldsByEdgeAndKeepsNewestAttributes() {
        ColumnIdentifier tgt = ColumnIdentifier.parse("default_catalog.db.t2.x");
        ColumnIdentifier src = col("a");
        LineageAggregator agg = new LineageAggregator();
        SqlParseResult first = SqlParseResult.builder("q1").statementType(StatementType.INSERT_INTO)
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.IDENTITY, "a"))
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.INDIRECT, TransformType.IDENTITY, null))
                .build();
        SqlParseResult second = SqlParseResult.builder("q2").statementType(StatementType.INSERT_OVERWRITE)
                .addEdge(ColumnLineageEdge.of(tgt, src, LineageType.DIRECT, TransformType.EXPRESSION, "a + 1"))
                .build();
        agg.add(record("q2", T0.plusHours(2), "bob", 1, 1), second);
        agg.add(record("q1", T0, "alice", 1, 1), first);
        agg.add(record("q3", T0.plusHours(1), "carol", 1, 1), first);
        List<LineageRow> rows = agg.drain();
        assertEquals(2, rows.size());
        LineageRow direct = rows.stream().filter(x -> x.getEdge().getLineageType() == LineageType.DIRECT)
                .findFirst().get();
        assertEquals(3, direct.getExecCount());
        assertEquals(T0, direct.getFirstSeenTime());
        assertEquals(T0.plusHours(2), direct.getLastSeenTime());
        assertEquals("q2", direct.getLastQueryId());
        assertEquals("bob", direct.getLastExecUser());
        assertEquals(StatementType.INSERT_OVERWRITE, direct.getStmtType());
        assertEquals(TransformType.EXPRESSION, direct.getEdge().getTransformType(), "newest statement wins");
        LineageRow indirect = rows.stream().filter(x -> x.getEdge().getLineageType() == LineageType.INDIRECT)
                .findFirst().get();
        assertEquals(2, indirect.getExecCount());
        assertEquals("q3", indirect.getLastQueryId());
        assertEquals(0, agg.size());
    }

    @Test
    void lineageMergeTieBreakIsOrderIndependent() {
        ColumnLineageEdge e = ColumnLineageEdge.of(ColumnIdentifier.parse("c.d.t.x"), col("a"), LineageType.DIRECT,
                TransformType.IDENTITY, null);
        LineageRow a = new LineageRow(e, StatementType.INSERT_INTO, null, "qa", "u", T0, T0, 1);
        LineageRow b = new LineageRow(e, StatementType.UPDATE, null, "qb", "u", T0, T0, 1);
        assertEquals("qb", a.merge(b).getLastQueryId());
        assertEquals("qb", b.merge(a).getLastQueryId());
        assertThrows(IllegalArgumentException.class, () -> a.merge(new LineageRow(ColumnLineageEdge.of(
                ColumnIdentifier.parse("c.d.t.y"), col("a"), LineageType.DIRECT, TransformType.IDENTITY, null),
                StatementType.INSERT_INTO, null, "q", "u", T0, T0, 1)));
    }

    @Test
    void userDictionaryAssignsDenseIdsAfterThePersistedMaximum() throws Exception {
        Map<String, Long> persisted = new HashMap<>();
        persisted.put("alice", 1L);
        persisted.put("bob", 7L);
        UserDictionary dict = new UserDictionary(persisted);
        assertEquals(7L, dict.idOf("bob"));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Long>> ids = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                final String user = "u" + (i % 50);
                ids.add(pool.submit(() -> dict.idOf(user)));
            }
            Map<String, Long> seen = new HashMap<>();
            for (int i = 0; i < 200; i++) {
                long id = ids.get(i).get();
                Long previous = seen.putIfAbsent("u" + (i % 50), id);
                if (previous != null) {
                    assertEquals(previous.longValue(), id, "stable id per user");
                }
            }
            assertEquals(50, new java.util.HashSet<>(seen.values()).size(), "unique ids");
            assertTrue(seen.values().stream().allMatch(id -> id >= 8 && id <= 57), "dense after max(7)");
        } finally {
            pool.shutdownNow();
        }
        List<UserDictionary.Entry> pending = dict.drainPending();
        assertEquals(50, pending.size());
        assertTrue(dict.drainPending().isEmpty());
        assertEquals(52, dict.size());
        assertThrows(IllegalArgumentException.class,
                () -> new UserDictionary(Collections.singletonMap("x", 0L)));
        Map<String, Long> dup = new HashMap<>();
        dup.put("x", 3L);
        dup.put("y", 3L);
        assertThrows(IllegalArgumentException.class, () -> new UserDictionary(dup));
        assertNotEquals(dict.idOf("brand-new"), dict.idOf("alice"));
    }

    @Test
    void watermarkOrderingAndBatchIdDeterminism() {
        Watermark a = new Watermark(T0, "b");
        Watermark b = new Watermark(T0, "c");
        Watermark c = new Watermark(T0.plusSeconds(1), "a");
        List<Watermark> list = new ArrayList<>(Arrays.asList(c, b, a));
        Collections.sort(list);
        assertEquals(Arrays.asList(a, b, c), list);
        assertTrue(Watermark.startingAt(T0).compareTo(new Watermark(T0, "")) < 0);
        String id = AuditLogSyncPipeline.batchId("p", a, c);
        assertEquals(id, AuditLogSyncPipeline.batchId("p", a, c));
        assertNotEquals(id, AuditLogSyncPipeline.batchId("p", a, b));
        assertNotEquals(id, AuditLogSyncPipeline.batchId("p2", a, c));
        assertTrue(id.matches("[0-9a-f]{32}"));
    }
}
