package com.starrocks.lineage.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * HTTP test double of a StarRocks 3.1 FE / BE pair, with the behaviour verified on a real 3.1.17 container:
 * <ul>
 *   <li>the FE (its own port) requires {@code Expect: 100-continue}, answers a Stream Load with {@code 307} to the BE
 *       (another port) and hangs up when a body arrives instead of reading it ({@link #feHangsUpOnBody}); a follower
 *       FE redirects to the leader;</li>
 *   <li>an empty load fails without consuming its label ({@link #emptyLoadAsError});</li>
 *   <li>the BE enforces Basic auth and {@code Expect: 100-continue}, and label uniqueness ("Label Already Exists");</li>
 *   <li>rows matching {@link #rejectRow} are data-quality errors: without {@code max_filter_ratio} the load fails
 *       ({@code NumberFilteredRows > 0}) and the label stays free; with {@code max_filter_ratio: 1} the other rows
 *       are loaded;</li>
 *   <li>{@code GET /api/{db}/get_load_state?label=} answers VISIBLE for used labels, the configured state for
 *       {@link #labelStates}, UNKNOWN otherwise.</li>
 * </ul>
 */
final class FakeStarRocks implements AutoCloseable {

    static final String USER = "loader";
    static final String PASSWORD = "secret";

    final HttpServer server;
    final HttpServer beServer;
    final HttpServer followerServer;
    /** Every request the FE received for a stream load: {@code label} and body bytes declared. */
    final List<String> feRequests = Collections.synchronizedList(new ArrayList<>());
    /**
     * The real 3.1 FE answers 307 without reading the body and closes the connection; a large body then fails with
     * "Broken pipe". The fake hangs up without any response as soon as a body arrives, which a client must survive.
     */
    volatile boolean feHangsUpOnBody = true;
    /** FE config {@code empty_load_as_error} (true by default): an empty load fails and its label stays free. */
    volatile boolean emptyLoadAsError = true;
    private final ObjectMapper json = new ObjectMapper();
    private final Set<String> labels = Collections.synchronizedSet(new HashSet<>());
    final java.util.Map<String, String> labelStates = Collections.synchronizedMap(new java.util.HashMap<>());
    final List<Load> loads = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger failNextLoads = new AtomicInteger();
    volatile Predicate<JsonNode> rejectRow = r -> false;

    FakeStarRocks() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/", this::fe);
        server.start();
        beServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        beServer.createContext("/api/", this::be);
        beServer.start();
        followerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        followerServer.createContext("/api/", ex -> {
            if (ex.getRequestURI().getPath().endsWith("/get_load_state")) {
                // GetStreamLoadState#redirectToLeader: 307 with the full URI, empty body
                drain(ex.getRequestBody());
                ex.getResponseHeaders().add("Location", feUrl().resolve(ex.getRequestURI().toString()).toString());
                ex.sendResponseHeaders(307, -1);
                ex.close();
                return;
            }
            redirect(ex, feUrl());
        });
        followerServer.start();
    }

    URI feUrl() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    URI beUrl() {
        return URI.create("http://127.0.0.1:" + beServer.getAddress().getPort());
    }

    /** A follower FE: redirects stream loads to the leader ({@link #feUrl()}), like a 3.1 follower. */
    URI followerUrl() {
        return URI.create("http://127.0.0.1:" + followerServer.getAddress().getPort());
    }

    StreamLoadClient client() {
        return new StreamLoadClient(feUrl(), USER, PASSWORD, java.time.Duration.ofSeconds(30));
    }

    /** Forgets every label, like the FE after {@code label_keep_max_second}. */
    void expireLabels() {
        labels.clear();
    }

    private void fe(HttpExchange ex) throws IOException {
        drain(ex.getRequestBody());
        if (ex.getRequestURI().getPath().endsWith("/get_load_state")) {
            if (!authorized(ex)) {
                reply(ex, 401, "{}");
                return;
            }
            String label = ex.getRequestURI().getQuery().replaceFirst("^label=", "");
            String state = labelStates.getOrDefault(label, labels.contains(label) ? "VISIBLE" : "UNKNOWN");
            reply(ex, 200, "{\"state\":\"" + state + "\",\"status\":\"OK\",\"msg\":\"Success\"}");
            return;
        }
        redirect(ex, beUrl());
    }

    /** FE behaviour of a stream load request: 100-continue required, redirect, never reads a body. */
    private void redirect(HttpExchange ex, URI to) throws IOException {
        String length = ex.getRequestHeaders().getFirst("Content-Length");
        long bodyBytes = length == null ? -1 : Long.parseLong(length);
        if (ex.getHttpContext().getServer() == server) {
            feRequests.add(ex.getRequestHeaders().getFirst("label") + ":" + bodyBytes);
        }
        if (!"100-continue".equalsIgnoreCase(ex.getRequestHeaders().getFirst("Expect"))) {
            drain(ex.getRequestBody());
            reply(ex, 200, "{\"Status\":\"FAILED\",\"Message\":\"There is no 100-continue header\"}");
            return;
        }
        if (bodyBytes > 0 && feHangsUpOnBody) {
            ex.close();
            return;
        }
        drain(ex.getRequestBody());
        ex.getResponseHeaders().add("Location", to.resolve(ex.getRequestURI().getPath()).toString());
        ex.sendResponseHeaders(307, -1);
        ex.close();
    }

    private boolean authorized(HttpExchange ex) {
        String expected = "Basic " + Base64.getEncoder()
                .encodeToString((USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
        return expected.equals(ex.getRequestHeaders().getFirst("Authorization"));
    }

    private void be(HttpExchange ex) throws IOException {
        byte[] body = drain(ex.getRequestBody());
        if (!authorized(ex)) {
            reply(ex, 401, "{\"Status\":\"Fail\",\"Message\":\"no auth\"}");
            return;
        }
        if (!"100-continue".equalsIgnoreCase(ex.getRequestHeaders().getFirst("Expect"))) {
            reply(ex, 200, "{\"Status\":\"Fail\",\"Message\":\"There is no 100-continue header\"}");
            return;
        }
        String[] path = ex.getRequestURI().getPath().split("/");
        String db = path[3];
        String table = path[4];
        String label = ex.getRequestHeaders().getFirst("label");
        if (failNextLoads.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            reply(ex, 200, "{\"Status\":\"Fail\",\"Message\":\"too many versions\",\"ErrorURL\":\"http://be/err\"}");
            return;
        }
        if (labels.contains(label)) {
            reply(ex, 200, "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"FINISHED\"}");
            return;
        }
        if (body.length == 0) {
            if (emptyLoadAsError) {
                labelStates.put(label, "ABORTED");
                reply(ex, 200, "{\"Status\":\"Fail\",\"Message\":\"No partitions have data available for "
                        + "loading\"}");
            } else {
                labels.add(label);
                reply(ex, 200, "{\"Status\":\"Success\",\"NumberLoadedRows\":0}");
            }
            return;
        }
        JsonNode rows = json.readTree(body);
        List<JsonNode> accepted = new ArrayList<>();
        int rejected = 0;
        for (JsonNode row : rows) {
            if (rejectRow.test(row)) {
                rejected++;
            } else {
                accepted.add(row);
            }
        }
        boolean tolerant = "1".equals(ex.getRequestHeaders().getFirst("max_filter_ratio"));
        if (rejected > 0 && !tolerant) {
            reply(ex, 200, "{\"Status\":\"Fail\",\"Message\":\"too many filtered rows\",\"NumberLoadedRows\":0,"
                    + "\"NumberFilteredRows\":" + rejected + ",\"ErrorURL\":\"http://be/_load_error_log?file=x\"}");
            return;
        }
        labels.add(label);
        loads.add(new Load(db, table, label, ex.getRequestHeaders().getFirst("columns"), tolerant, accepted));
        reply(ex, 200, "{\"Status\":\"Success\",\"NumberLoadedRows\":" + accepted.size() + ",\"NumberFilteredRows\":"
                + rejected + (rejected > 0 ? ",\"ErrorURL\":\"http://be/_load_error_log?file=x\"" : "") + "}");
    }

    private static byte[] drain(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static void reply(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        beServer.stop(0);
        followerServer.stop(0);
    }

    /** One accepted stream load. */
    static final class Load {
        final String db;
        final String table;
        final String label;
        final String columns;
        final boolean tolerant;
        final List<JsonNode> rows;

        Load(String db, String table, String label, String columns, boolean tolerant, List<JsonNode> rows) {
            this.db = db;
            this.table = table;
            this.label = label;
            this.columns = columns;
            this.tolerant = tolerant;
            this.rows = rows;
        }
    }
}
