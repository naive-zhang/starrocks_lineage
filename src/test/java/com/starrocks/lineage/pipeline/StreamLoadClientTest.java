package com.starrocks.lineage.pipeline;

import com.sun.net.httpserver.HttpServer;
import com.starrocks.lineage.model.ColumnIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link StreamLoadClient} against {@link FakeStarRocks}, plus the row mapping of {@link StarRocksSink} (its SQL
 * steps run against a real StarRocks in {@code StarRocksStorageIT}).
 */
class StreamLoadClientTest {

    private FakeStarRocks sr;
    private StreamLoadClient client;

    @BeforeEach
    void setUp() throws Exception {
        sr = new FakeStarRocks();
        client = sr.client();
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        sr.close();
    }

    private static byte[] rows(String... userNames) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < userNames.length; i++) {
            sb.append(i == 0 ? "" : ",").append("{\"user_name\":\"").append(userNames[i])
                    .append("\",\"user_id\":1,\"first_seen_time\":\"2024-06-18 09:00:00\"}");
        }
        return sb.append(']').toString().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void redirectCredentialsAndLabelIdempotency() {
        StreamLoadClient.Result first = client.load("audit_lineage_db", "audit_user_dict", "b1_dict",
                StarRocksSink.DICT_COLUMNS, rows("a"));
        assertFalse(first.isAlreadyLoaded());
        assertEquals(1, first.getLoadedRows());
        assertEquals(StarRocksSink.DICT_COLUMNS, sr.loads.get(0).columns);
        assertFalse(sr.loads.get(0).tolerant, "strict by default");

        StreamLoadClient.Result retry = client.load("audit_lineage_db", "audit_user_dict", "b1_dict",
                StarRocksSink.DICT_COLUMNS, rows("a"));
        assertTrue(retry.isAlreadyLoaded(), "same label: StarRocks de-duplicates the retry");
        assertEquals(1, sr.loads.size());
    }

    @Test
    void failuresAreReportedAndClassified() throws Exception {
        sr.failNextLoads.set(1);
        PipelineException e = assertThrows(PipelineException.class, () -> client.load("audit_lineage_db",
                "audit_column_hourly_usage", "x_usage", StarRocksSink.USAGE_COLUMNS, "[]".getBytes()));
        assertTrue(e.getMessage().contains("too many versions"));
        assertTrue(e.getMessage().contains("http://be/err"));
        assertTrue(e.isRetryable(), "an infrastructure failure is retried");
        assertThrows(IllegalArgumentException.class, () -> client.load("db", "t", "bad label!", "c", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> client.load("db;drop", "t", "ok", "c", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> client.loadState("db", "bad label!"));

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
            PipelineException e = assertThrows(PipelineException.class, () -> c.load("d", "t", "l", "c",
                    "[]".getBytes()));
            assertTrue(e.getMessage().contains("RUNNING"));
            assertTrue(e.isRetryable());
        } finally {
            server.stop(0);
        }
    }

    /** Gap 2: rejected rows fail the strict load without consuming the label; the tolerant retry keeps the rest. */
    @Test
    void rejectedRowsAreReportedAndTheSameLabelCanLoadTheRest() {
        sr.rejectRow = r -> r.path("user_name").asText().startsWith("bad");
        StreamLoadClient.DataQualityException rejected = assertThrows(StreamLoadClient.DataQualityException.class,
                () -> client.load("audit_lineage_db", "audit_user_dict", "b2_dict", StarRocksSink.DICT_COLUMNS,
                        rows("good1", "bad", "good2"), false));
        assertFalse(rejected.isRetryable(), "the same data fails the same way");
        assertEquals(1, rejected.getFilteredRows());
        assertEquals("b2_dict", rejected.getLabel());
        assertTrue(rejected.getErrorUrl().contains("_load_error_log"));
        assertTrue(sr.loads.isEmpty());

        StreamLoadClient.Result tolerant = client.load("audit_lineage_db", "audit_user_dict", "b2_dict",
                StarRocksSink.DICT_COLUMNS, rows("good1", "bad", "good2"), true);
        assertEquals(2, tolerant.getLoadedRows());
        assertEquals(1, tolerant.getFilteredRows());
        assertTrue(sr.loads.get(0).tolerant);
        assertEquals(2, sr.loads.get(0).rows.size());
    }

    /** Gap 4: the label state of a load, as the FE reports it. */
    @Test
    void loadStateMapsTheFeAnswer() {
        assertEquals(StreamLoadClient.LoadState.UNKNOWN, client.loadState("audit_lineage_db", "never_used"));
        client.load("audit_lineage_db", "audit_user_dict", "b3_dict", StarRocksSink.DICT_COLUMNS, rows("a"));
        assertEquals(StreamLoadClient.LoadState.COMMITTED, client.loadState("audit_lineage_db", "b3_dict"));
        sr.labelStates.put("l_prepared", "PREPARE");
        sr.labelStates.put("l_committed", "COMMITTED");
        sr.labelStates.put("l_aborted", "ABORTED");
        sr.labelStates.put("l_weird", "SOMETHING");
        assertEquals(StreamLoadClient.LoadState.IN_PROGRESS, client.loadState("audit_lineage_db", "l_prepared"));
        assertEquals(StreamLoadClient.LoadState.COMMITTED, client.loadState("audit_lineage_db", "l_committed"));
        assertEquals(StreamLoadClient.LoadState.ABORTED, client.loadState("audit_lineage_db", "l_aborted"));
        assertThrows(PipelineException.class, () -> client.loadState("audit_lineage_db", "l_weird"));
        sr.expireLabels();
        assertEquals(StreamLoadClient.LoadState.UNKNOWN, client.loadState("audit_lineage_db", "b3_dict"),
                "after label_keep_max_second the FE no longer knows the label");
    }

    @Test
    void usageRowsCarryStatelessUserIdsAndTruncatedStrings() {
        String longName = "名" + new String(new char[300]).replace('\0', 'x');
        HourlyUsageAggregator agg = new HourlyUsageAggregator();
        agg.add(AggregatorsTest.record("q1", LocalDateTime.of(2024, 6, 18, 9, 15), "alice", 7, 100),
                AggregatorsTest.query(Collections.singletonMap(ColumnIdentifier.parse("default_catalog.db.t."
                        + "c"), new com.starrocks.lineage.model.ClauseType[]{
                        com.starrocks.lineage.model.ClauseType.SELECT})));
        List<Map<String, Object>> rows = StarRocksSink.usageRows(agg.drain());
        assertEquals(1, rows.size());
        assertEquals(UserIds.idOf("alice"), rows.get(0).get("user_id"));
        assertEquals("2024-06-18 09:00:00", rows.get(0).get("stat_hour"));
        assertTrue(StarRocksSink.USAGE_COLUMNS.contains("user_bitmap=to_bitmap(user_id)"));
        String cut = StarRocksSink.Text.truncate(longName, 256);
        assertTrue(cut.getBytes(StandardCharsets.UTF_8).length <= 256);
        assertTrue(longName.startsWith(cut));
        assertEquals("名", StarRocksSink.Text.truncate("名字", 4), "never splits a UTF-8 character");
        assertEquals(null, StarRocksSink.Text.truncate(null, 4));
    }

    @Test
    void sinkValidatesConfiguration() {
        javax.sql.DataSource ds = new org.h2.jdbcx.JdbcDataSource();
        assertThrows(IllegalArgumentException.class, () -> new StarRocksSink(ds, client, "bad db", "p"));
        assertThrows(IllegalArgumentException.class, () -> new StarRocksSink(ds, client, "db", ""));
        StarRocksSink sink = new StarRocksSink(ds, client, "audit_lineage_db", "p");
        assertEquals("0123abcd_usage", sink.label("0123abcd", "usage"));
    }

    // ------------------------------------------------------------------------------------------------ routing

    /**
     * Found with the production statements on 3.1.17: the FE answers 307 without reading the body and closes the
     * connection, so a 13 MB lineage load failed with "Broken pipe". The body must never travel to an FE.
     */
    @Test
    void theBodyNeverTravelsToTheFe() {
        StreamLoadClient.Result r = client.load("audit_lineage_db", "audit_user_dict", "big_dict",
                StarRocksSink.DICT_COLUMNS, rows("a", "b", "c"));
        assertEquals(3, r.getLoadedRows());
        assertEquals(java.util.Collections.singletonList("big_dict_route:0"), sr.feRequests,
                "one body-less probe under its own label; the real label and the body went to the BE only");
        assertEquals("big_dict", sr.loads.get(0).label);
        assertEquals(StreamLoadClient.LoadState.UNKNOWN, client.loadState("audit_lineage_db", "big_dict_route"),
                "the FE redirect starts no transaction");
    }

    /** A follower FE redirects to the leader, which hangs up on the body: probe the leader, then load on the BE. */
    @Test
    void aFollowerFeChainIsResolvedWithoutSendingTheBodyTwice() throws Exception {
        try (StreamLoadClient viaFollower = new StreamLoadClient(sr.followerUrl(), FakeStarRocks.USER,
                FakeStarRocks.PASSWORD, Duration.ofSeconds(30))) {
            StreamLoadClient.Result r = viaFollower.load("audit_lineage_db", "audit_user_dict", "chain_dict",
                    StarRocksSink.DICT_COLUMNS, rows("a"));
            assertEquals(1, r.getLoadedRows());
        }
        assertEquals(1, sr.loads.size());
        assertTrue(sr.feRequests.contains("chain_dict_route:0"), "the leader was probed: " + sr.feRequests);
        sr.feHangsUpOnBody = false;
        try (StreamLoadClient viaFollower = new StreamLoadClient(sr.followerUrl(), FakeStarRocks.USER,
                FakeStarRocks.PASSWORD, Duration.ofSeconds(30))) {
            assertEquals(1, viaFollower.load("audit_lineage_db", "audit_user_dict", "chain_dict2",
                    StarRocksSink.DICT_COLUMNS, rows("b")).getLoadedRows(), "an FE that does answer 307 is followed");
        }
    }

    /** A URL that points at a BE directly: the probe runs there as an empty load under its own label only. */
    @Test
    void aDirectBeUrlNeverConsumesTheRealLabelWithTheProbe() throws Exception {
        for (boolean emptyLoadAsError : new boolean[]{true, false}) {
            sr.emptyLoadAsError = emptyLoadAsError;
            String label = "direct_" + emptyLoadAsError;
            try (StreamLoadClient direct = new StreamLoadClient(sr.beUrl(), FakeStarRocks.USER,
                    FakeStarRocks.PASSWORD, Duration.ofSeconds(30))) {
                StreamLoadClient.Result r = direct.load("audit_lineage_db", "audit_user_dict", label,
                        StarRocksSink.DICT_COLUMNS, rows("a", "b"));
                assertFalse(r.isAlreadyLoaded(), "empty_load_as_error=" + emptyLoadAsError);
                assertEquals(2, r.getLoadedRows());
            }
            assertEquals(StreamLoadClient.LoadState.COMMITTED, client.loadState("audit_lineage_db", label));
        }
        assertEquals(StreamLoadClient.LoadState.ABORTED, client.loadState("audit_lineage_db", "direct_true_route"));
    }

    @Test
    void anUnreachableFeIsARetryableFailure() {
        try (StreamLoadClient nowhere = new StreamLoadClient(URI.create("http://127.0.0.1:1"), "u", "p",
                Duration.ofSeconds(5))) {
            PipelineException e = assertThrows(PipelineException.class, () -> nowhere.load("db", "t", "l", "c",
                    "[]".getBytes(StandardCharsets.UTF_8)));
            assertTrue(e.isRetryable());
            assertTrue(e.getMessage().contains("unreachable"), e.getMessage());
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Found on the production cluster (3 FE): the configured FE was a follower, which answers get_load_state with a
     * 307 to the leader - the client read the empty redirect body. The state must be read from the leader.
     */
    @Test
    void loadStateIsReadFromTheLeaderThroughAFollower() throws Exception {
        try (StreamLoadClient viaFollower = new StreamLoadClient(sr.followerUrl(), FakeStarRocks.USER,
                FakeStarRocks.PASSWORD, Duration.ofSeconds(30))) {
            viaFollower.load("audit_lineage_db", "audit_user_dict", "f_label", StarRocksSink.DICT_COLUMNS, rows("a"));
            assertEquals(StreamLoadClient.LoadState.COMMITTED, viaFollower.loadState("audit_lineage_db", "f_label"));
            assertEquals(StreamLoadClient.LoadState.UNKNOWN, viaFollower.loadState("audit_lineage_db", "nope"));
        }
    }

    /** An answer that is not a load state says what it was: HTTP code and body, never an empty message. */
    @Test
    void anUnusableLoadStateAnswerNamesTheCodeAndBody() throws Exception {
        try (StreamLoadClient wrongPassword = new StreamLoadClient(sr.feUrl(), FakeStarRocks.USER, "nope",
                Duration.ofSeconds(5))) {
            PipelineException e = assertThrows(PipelineException.class,
                    () -> wrongPassword.loadState("audit_lineage_db", "x"));
            assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
        }
        HttpServer empty = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        empty.createContext("/", ex -> {
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        empty.start();
        try (StreamLoadClient c = new StreamLoadClient(URI.create("http://127.0.0.1:" + empty.getAddress().getPort()),
                "u", "p", Duration.ofSeconds(5))) {
            PipelineException e = assertThrows(PipelineException.class, () -> c.loadState("db", "x"));
            assertTrue(e.getMessage().contains("(empty body)"), e.getMessage());
        } finally {
            empty.stop(0);
        }
    }
}
