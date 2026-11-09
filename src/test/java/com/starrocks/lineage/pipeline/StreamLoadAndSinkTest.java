package com.starrocks.lineage.pipeline;

import com.sun.net.httpserver.HttpServer;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TransformType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamLoadAndSinkTest {

    private static final LocalDateTime T0 = LocalDateTime.of(2024, 6, 18, 9, 0);
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2024-06-18T12:00:00Z"), ZoneId.of("UTC"));

    private FakeStarRocks sr;
    private StreamLoadClient client;
    private StarRocksSink sink;

    @BeforeEach
    void setUp() throws Exception {
        sr = new FakeStarRocks();
        client = sr.client();
        sink = new StarRocksSink(sr.dataSource, client, "audit_lineage_db", 2, "MERGE", CLOCK);
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        sr.close();
    }

    // ------------------------------------------------------------------------------------------- stream load

    @Test
    void streamLoadFollowsRedirectWithCredentialsAndIsIdempotentPerLabel() {
        byte[] body = "[{\"user_name\":\"a\",\"user_id\":1,\"first_seen_time\":\"2024-06-18 09:00:00\"}]"
                .getBytes(StandardCharsets.UTF_8);
        StreamLoadClient.Result first = client.load("audit_lineage_db", "audit_user_dict", "b1_dict_0",
                StarRocksSink.DICT_COLUMNS, body);
        assertFalse(first.isAlreadyLoaded());
        assertEquals(1, first.getLoadedRows());
        FakeStarRocks.Load load = sr.loads.get(0);
        assertEquals("100-continue", load.expect);
        assertEquals(StarRocksSink.DICT_COLUMNS, load.columns);

        StreamLoadClient.Result retry = client.load("audit_lineage_db", "audit_user_dict", "b1_dict_0",
                StarRocksSink.DICT_COLUMNS, body);
        assertTrue(retry.isAlreadyLoaded(), "same label: StarRocks de-duplicates the retry");
        assertEquals(1, sr.loads.size());
    }

    @Test
    void streamLoadFailuresAreReported() throws Exception {
        sr.failNextLoads.set(1);
        PipelineException e = assertThrows(PipelineException.class, () -> client.load("audit_lineage_db",
                "audit_column_hourly_usage", "x_usage_0", StarRocksSink.USAGE_COLUMNS, "[]".getBytes()));
        assertTrue(e.getMessage().contains("too many versions"));
        assertTrue(e.getMessage().contains("http://be/err"));
        assertThrows(IllegalArgumentException.class,
                () -> client.load("db", "t", "bad label!", "c", new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> client.load("db;drop", "t", "ok", "c", new byte[0]));

        try (StreamLoadClient wrongPassword = new StreamLoadClient(sr.feUrl(), FakeStarRocks.USER, "nope",
                Duration.ofSeconds(5))) {
            assertTrue(assertThrows(PipelineException.class, () -> wrongPassword.load("audit_lineage_db",
                    "audit_user_dict", "l1", "c", "[]".getBytes())).getMessage().contains("HTTP 401"));
        }
    }

    @Test
    void runningJobWithSameLabelIsRetryable() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            byte[] b = "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"RUNNING\"}"
                    .getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        });
        server.start();
        try (StreamLoadClient c = new StreamLoadClient(URI.create("http://127.0.0.1:"
                + server.getAddress().getPort()), "u", "p", Duration.ofSeconds(5))) {
            assertTrue(assertThrows(PipelineException.class, () -> c.load("d", "t", "l", "c", "[]".getBytes()))
                    .getMessage().contains("RUNNING"));
        } finally {
            server.stop(0);
        }
    }

    // ------------------------------------------------------------------------------------------- sink

    @Test
    void usageRowsMapUserIdsAndChunkWithDeterministicLabels() {
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        com.starrocks.lineage.model.SqlParseResult q = AggregatorsTest.query(Collections.singletonMap(
                AggregatorsTest.col("a"), new com.starrocks.lineage.model.ClauseType[]{
                    com.starrocks.lineage.model.ClauseType.SELECT}));
        for (String user : Arrays.asList("alice", "bob", "carol")) {
            agg.add(AggregatorsTest.record(user, T0, user, 10, 100), q);
        }
        Map<String, Long> persisted = new HashMap<>();
        persisted.put("bob", 41L);
        UserDictionary dict = new UserDictionary(persisted);
        sink.writeUsage("b7", agg.drain(), dict);
        List<FakeStarRocks.Load> loads = sr.loadsInto("audit_column_hourly_usage");
        assertEquals(2, loads.size(), "chunkRows = 2");
        assertEquals("b7_usage_0", loads.get(0).label);
        assertEquals("b7_usage_1", loads.get(1).label);
        assertTrue(loads.get(0).columns.endsWith("user_bitmap=to_bitmap(user_id),user_hll=hll_hash(user_name)"));
        assertEquals("alice", loads.get(0).rows.get(0).get("user_name").asText());
        assertEquals(42, loads.get(0).rows.get(0).get("user_id").asLong(), "new user gets max + 1");
        assertEquals(41, loads.get(0).rows.get(1).get("user_id").asLong());
        assertEquals("2024-06-18 09:00:00", loads.get(0).rows.get(0).get("stat_hour").asText());
        assertEquals("default_catalog", loads.get(1).rows.get(0).get("catalog_name").asText());
    }

    @Test
    void userDictionaryRoundTrip() {
        UserDictionary dict = new UserDictionary(Collections.emptyMap());
        dict.idOf("alice");
        dict.idOf("bob");
        sink.writeUserDictionary("b1", dict.drainPending(), T0);
        Map<String, Long> loaded = sink.loadUserDictionary();
        assertEquals(1L, loaded.get("alice").longValue());
        assertEquals(2L, loaded.get("bob").longValue());
    }

    private static LineageRow row(String target, String source, LineageType type, TransformType transform,
                                  String queryId, LocalDateTime first, LocalDateTime last, long count) {
        ColumnLineageEdge e = ColumnLineageEdge.of(ColumnIdentifier.parse(target), ColumnIdentifier.parse(source),
                type, transform, transform.name().toLowerCase());
        return new LineageRow(e, StatementType.INSERT_INTO, "digest", queryId, "etl", first, last, count);
    }

    /** Review R4-01: the merge accumulates, keeps the newest attributes and is a no-op when replayed. */
    @Test
    void lineageMergeAccumulatesAndReplayIsANoOp() throws Exception {
        LineageRow existing = row("default_catalog.dw.t.a", "default_catalog.ods.s.a", LineageType.DIRECT,
                TransformType.IDENTITY, "q-old", T0.minusDays(3), T0.minusDays(1), 5);
        sink.writeLineage("b0", Collections.singletonList(existing));
        assertEquals(5L, sr.lineageRow(existing.getEdgeId()).get("exec_count"));

        LineageRow again = row("default_catalog.dw.t.a", "default_catalog.ods.s.a", LineageType.DIRECT,
                TransformType.EXPRESSION, "q-new", T0, T0.plusHours(1), 2);
        LineageRow fresh = row("default_catalog.dw.t.b", "default_catalog.ods.s.b", LineageType.INDIRECT,
                TransformType.IDENTITY, "q-new", T0, T0, 1);
        sink.writeLineage("b1", Arrays.asList(again, fresh));

        Map<String, Object> merged = sr.lineageRow(again.getEdgeId());
        assertEquals(7L, merged.get("exec_count"));
        assertEquals(Timestamp.valueOf(T0.minusDays(3)), merged.get("first_seen_time"));
        assertEquals(Timestamp.valueOf(T0.plusHours(1)), merged.get("last_seen_time"));
        assertEquals("q-new", merged.get("last_query_id"));
        assertEquals("EXPRESSION", merged.get("transform_type"));
        assertEquals("b1", merged.get("last_batch_id"));
        assertEquals(1L, sr.lineageRow(fresh.getEdgeId()).get("exec_count"));

        // crash after the merge, before the checkpoint: the batch is replayed with the same id
        sink.writeLineage("b1", Arrays.asList(again, fresh));
        assertEquals(0, sink.mergeLineage("b1"), "guard: nothing is merged twice");
        assertEquals(7L, sr.lineageRow(again.getEdgeId()).get("exec_count"));
        assertEquals(3, sr.count("audit_column_lineage_stage"), "stage label de-duplicated the replayed load");

        // an older observation of the same edge in a later batch: counts add, newest attributes are kept
        LineageRow older = row("default_catalog.dw.t.a", "default_catalog.ods.s.a", LineageType.DIRECT,
                TransformType.AGGREGATION, "q-older", T0.minusDays(10), T0.minusDays(2), 1);
        sink.writeLineage("b2", Collections.singletonList(older));
        Map<String, Object> afterOlder = sr.lineageRow(again.getEdgeId());
        assertEquals(8L, afterOlder.get("exec_count"));
        assertEquals(Timestamp.valueOf(T0.minusDays(10)), afterOlder.get("first_seen_time"));
        assertEquals("q-new", afterOlder.get("last_query_id"));
        assertEquals("EXPRESSION", afterOlder.get("transform_type"));
        assertEquals(3, sr.count("audit_lineage_merge_log"));
        assertNull(sr.lineageRow("does-not-exist"));
    }

    @Test
    void sinkValidatesConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> new StarRocksSink(sr.dataSource, client, "db; drop", 1, "INSERT", CLOCK));
        assertThrows(IllegalArgumentException.class,
                () -> new StarRocksSink(sr.dataSource, client, "db", 0, "INSERT", CLOCK));
        assertThrows(IllegalArgumentException.class,
                () -> new StarRocksSink(sr.dataSource, client, "db", 1, "insert;", CLOCK));
        StarRocksSink noStreamLoad = new StarRocksSink(sr.dataSource, null, "audit_lineage_db", 1, "MERGE", CLOCK);
        noStreamLoad.writeLineage("b", Collections.emptyList());
        assertThrows(PipelineException.class, () -> noStreamLoad.writeUserDictionary("b",
                new UserDictionary(Collections.emptyMap()).drainPending().isEmpty()
                        ? Collections.singletonList(firstEntry()) : Collections.emptyList(), T0));
    }

    private static UserDictionary.Entry firstEntry() {
        UserDictionary d = new UserDictionary(Collections.emptyMap());
        d.idOf("x");
        return d.drainPending().get(0);
    }
}
