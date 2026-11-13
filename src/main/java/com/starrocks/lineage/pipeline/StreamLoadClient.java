package com.starrocks.lineage.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal StarRocks Stream Load client ({@code PUT /api/{db}/{table}/_stream_load}, JSON array body).
 *
 * <ul>
 *   <li>The FE answers {@code 307 Temporary Redirect} to a BE. Redirects are followed <b>manually</b> so the
 *       {@code Authorization} header and the (repeatable) body are re-sent to the BE; generic HTTP clients drop
 *       credentials on cross-host redirects.</li>
 *   <li>The {@code Location} of the redirect is the address the BE registered in the cluster (e.g.
 *       {@code http://127.0.0.1:8040} in an all-in-one container, a pod IP in Kubernetes). When the client reaches
 *       the cluster through NAT / port mapping, a {@code redirectRewriter} maps it to a reachable address.</li>
 *   <li>{@code Expect: 100-continue} is sent: the StarRocks 3.1 FE rejects a stream load without it
 *       ("There is no 100-continue header", verified on 3.1.17).</li>
 *   <li>Connections are never reused ({@code Connection: close}): the 3.1 BE replies {@code 400 Bad Request} to a
 *       second stream load on a kept-alive connection (verified on 3.1.17).</li>
 *   <li>Idempotency: every load carries a caller supplied label. {@code Label Already Exists} with an existing job
 *       that is {@code FINISHED} / {@code VISIBLE} / {@code COMMITTED} is reported as already loaded; a job still
 *       running is a retryable {@link PipelineException}.</li>
 *   <li>{@code Success} and {@code Publish Timeout} (committed, visible shortly) are successes.</li>
 * </ul>
 * Thread-safe; close it to release the connection pool.
 */
public final class StreamLoadClient implements Closeable {

    private static final Pattern LABEL = Pattern.compile("[-_A-Za-z0-9:]{1,128}");
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_]+");
    private static final int MAX_REDIRECTS = 3;
    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadClient.class);
    /**
     * Warning threshold: a JSON stream load is limited by the BE config {@code streaming_load_max_batch_size_mb}
     * (100 MB by default in 3.1). Measured with the production statements: 17,756 lineage edges = 13.4 MB.
     */
    static final long LARGE_BODY_BYTES = 64L * 1024 * 1024;

    private final URI feHttpUrl;
    private final String authorization;
    private final CloseableHttpClient http;
    private final Duration loadTimeout;
    private final UnaryOperator<URI> redirectRewriter;
    private final ObjectMapper json = new ObjectMapper();

    /**
     * @param feHttpUrl   e.g. {@code http://fe-host:8030}
     * @param loadTimeout StarRocks {@code timeout} header and HTTP response timeout
     */
    public StreamLoadClient(URI feHttpUrl, String user, String password, Duration loadTimeout) {
        this(feHttpUrl, user, password, loadTimeout, UnaryOperator.identity());
    }

    /**
     * @param redirectRewriter maps the BE address of a redirect to an address reachable from this client
     */
    public StreamLoadClient(URI feHttpUrl, String user, String password, Duration loadTimeout,
                            UnaryOperator<URI> redirectRewriter) {
        this.redirectRewriter = Objects.requireNonNull(redirectRewriter, "redirectRewriter");
        this.feHttpUrl = Objects.requireNonNull(feHttpUrl, "feHttpUrl");
        this.authorization = "Basic " + Base64.getEncoder().encodeToString(
                (user + ":" + (password == null ? "" : password)).getBytes(StandardCharsets.UTF_8));
        this.loadTimeout = Objects.requireNonNull(loadTimeout, "loadTimeout");
        this.http = HttpClients.custom()
                .setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
                        .setDefaultConnectionConfig(ConnectionConfig.custom()
                                .setConnectTimeout(Timeout.ofSeconds(10))
                                .build())
                        .build())
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(false)
                        .setResponseTimeout(Timeout.ofMilliseconds(loadTimeout.toMillis() + 10_000))
                        .build())
                .disableRedirectHandling()
                .disableAutomaticRetries()
                // StarRocks 3.1 BE answers "400 Bad Request" to a stream load sent on a reused keep-alive connection
                // (verified on 3.1.17): every load uses a fresh connection.
                .setConnectionReuseStrategy((request, response, context) -> false)
                .build();
    }

    /**
     * Loads a JSON array into {@code db.table}; any filtered row fails the whole load.
     *
     * @param columns the {@code columns} header (column list with optional {@code col=expr} mappings)
     * @param body    UTF-8 JSON array of objects
     * @return the outcome
     * @throws DataQualityException when StarRocks filtered rows (non-retryable, nothing was loaded)
     * @throws PipelineException    on any other failure
     */
    public Result load(String db, String table, String label, String columns, byte[] body) {
        return load(db, table, label, columns, body, false);
    }

    /**
     * Loads a JSON array; with {@code tolerateFilteredRows} the rows StarRocks rejects (too long, out of range ...)
     * are dropped instead of failing the load ({@code max_filter_ratio = 1}). Callers first try a strict load, record
     * the rejection ({@link DataQualityException}), then re-send with the <b>same label</b>: StarRocks 3.1 allows a
     * label to be reused after its load aborted (verified on 3.1.17), so exactly-once is preserved.
     */
    public Result load(String db, String table, String label, String columns, byte[] body,
                       boolean tolerateFilteredRows) {
        if (!NAME.matcher(db).matches() || !NAME.matcher(table).matches()) {
            throw new IllegalArgumentException("invalid database / table name: " + db + "." + table);
        }
        if (!LABEL.matcher(label).matches()) {
            throw new IllegalArgumentException("invalid stream load label: " + label);
        }
        if (body.length > LARGE_BODY_BYTES) {
            LOG.warn("stream load {} to {}.{} is {} MB: JSON loads are limited by streaming_load_max_batch_size_mb "
                    + "(100 MB by default)", label, db, table, body.length / (1024 * 1024));
        }
        URI fe = feHttpUrl.resolve("/api/" + db + "/" + table + "/_stream_load");
        String probeLabel = (label.length() <= 122 ? label : label.substring(0, 122)) + "_route";
        // the body never travels to an FE: the FE answers 307 without reading it and closes the connection, so a
        // body larger than the socket buffers fails with "Broken pipe" (verified on 3.1.17 with a 13 MB load)
        Response first;
        try {
            first = send(fe, label, probeLabel, columns, null, tolerateFilteredRows);
        } catch (IOException e) {
            throw new PipelineException("stream load " + label + " to " + db + "." + table + ": FE " + fe
                    + " unreachable: " + e.getMessage(), e);
        }
        URI target = first.isRedirect() ? redirect(fe, first) : fe;
        for (int hop = 0; hop <= MAX_REDIRECTS; hop++) {
            Response response;
            try {
                response = send(target, label, label, columns, body, tolerateFilteredRows);
            } catch (IOException e) {
                // either an FE that hung up on the body (a follower redirecting to the leader), or a real failure
                Response probe;
                try {
                    probe = send(target, label, probeLabel, columns, null, tolerateFilteredRows);
                } catch (IOException probeFailure) {
                    e.addSuppressed(probeFailure);
                    throw new PipelineException("stream load " + label + " to " + db + "." + table + " failed: "
                            + e.getMessage(), e);
                }
                if (!probe.isRedirect()) {
                    throw new PipelineException("stream load " + label + " to " + db + "." + table + " failed: "
                            + e.getMessage(), e);
                }
                target = redirect(target, probe);
                continue;
            }
            if (response.isRedirect()) {
                target = redirect(target, response);
                continue;
            }
            if (response.code != 200) {
                throw new PipelineException("stream load " + label + " HTTP " + response.code + ": "
                        + response.body);
            }
            return interpret(label, response.body);
        }
        throw new PipelineException("stream load " + label + ": too many redirects");
    }

    /**
     * One request of a stream load. Without {@code body} it is a <b>route probe</b>: it carries
     * {@code probeLabel}, never the real label, so even when it reaches a BE (which executes it as an empty load) it
     * cannot consume the label of the data.
     */
    private Response send(URI target, String label, String requestLabel, String columns, byte[] body,
                          boolean tolerateFilteredRows) throws IOException {
        HttpPut put = new HttpPut(target);
        put.setHeader("Authorization", authorization);
        put.setHeader("Connection", "close");
        put.setHeader("label", requestLabel);
        put.setHeader("format", "json");
        put.setHeader("strip_outer_array", "true");
        put.setHeader("columns", columns);
        put.setHeader("timeout", String.valueOf(Math.max(1, loadTimeout.getSeconds())));
        if (tolerateFilteredRows) {
            put.setHeader("max_filter_ratio", "1");
        }
        // the 3.1 FE refuses any stream load request without it, the body-less probe included
        put.setHeader("Expect", "100-continue");
        put.setEntity(new ByteArrayEntity(body == null ? new byte[0] : body, ContentType.APPLICATION_JSON));
        return http.execute(put, r -> {
            Header location = r.getFirstHeader("Location");
            String text = r.getEntity() == null ? "" : EntityUtils.toString(r.getEntity(), StandardCharsets.UTF_8);
            return new Response(r.getCode(), location == null ? null : location.getValue(), text);
        });
    }

    private URI redirect(URI from, Response response) {
        if (response.location == null) {
            throw new PipelineException("stream load redirect without Location from " + from);
        }
        return redirectRewriter.apply(from.resolve(response.location));
    }

    private Result interpret(String label, String body) {
        JsonNode node;
        try {
            node = json.readTree(body);
        } catch (IOException e) {
            throw new PipelineException("stream load " + label + ": unreadable response " + body, e);
        }
        String status = node.path("Status").asText("");
        long rows = node.path("NumberLoadedRows").asLong(0);
        long filtered = node.path("NumberFilteredRows").asLong(0);
        String errorUrl = node.path("ErrorURL").asText("");
        switch (status) {
            case "Success":
            case "Publish Timeout":
                return new Result(label, false, rows, filtered, errorUrl);
            case "Label Already Exists":
                String existing = node.path("ExistingJobStatus").asText("").toUpperCase(Locale.ROOT);
                if ("FINISHED".equals(existing) || "VISIBLE".equals(existing) || "COMMITTED".equals(existing)) {
                    return new Result(label, true, 0L, 0L, "");
                }
                throw new PipelineException("stream load label " + label + " exists with job status '" + existing
                        + "', retry later");
            default:
                String message = node.path("Message").asText("");
                if (filtered > 0) {
                    throw new DataQualityException("stream load " + label + " rejected " + filtered + " row(s): "
                            + message + ", errorUrl=" + errorUrl, label, filtered, errorUrl);
                }
                throw new PipelineException("stream load " + label + " failed: status=" + status + ", message="
                        + message + ", errorUrl=" + errorUrl);
        }
    }

    /**
     * State of a load label ({@code GET /api/{db}/get_load_state?label=}), for Stream Load and labelled
     * {@code INSERT} alike. The FE forgets labels after {@code label_keep_max_second} (3 days by default in 3.1):
     * {@link LoadState#UNKNOWN} then means "never existed <i>or</i> expired".
     */
    public LoadState loadState(String db, String label) {
        if (!NAME.matcher(db).matches() || !LABEL.matcher(label).matches()) {
            throw new IllegalArgumentException("invalid database / label: " + db + " / " + label);
        }
        // a follower FE answers 307 to the leader (GetStreamLoadState#redirectToLeader in 3.1): follow it, otherwise
        // the empty body of the redirect is all there is to read (found on a multi-FE production cluster)
        URI target = feHttpUrl.resolve("/api/" + db + "/get_load_state?label=" + label);
        Response response = null;
        for (int hop = 0; hop <= MAX_REDIRECTS; hop++) {
            HttpGet get = new HttpGet(target);
            get.setHeader("Authorization", authorization);
            get.setHeader("Connection", "close");
            try {
                response = http.execute(get, r -> {
                    Header location = r.getFirstHeader("Location");
                    String text = r.getEntity() == null ? "" : EntityUtils.toString(r.getEntity(),
                            StandardCharsets.UTF_8);
                    return new Response(r.getCode(), location == null ? null : location.getValue(), text);
                });
            } catch (IOException e) {
                throw new PipelineException("get_load_state " + label + " at " + target + " failed: "
                        + e.getMessage(), e);
            }
            if (!response.isRedirect()) {
                break;
            }
            target = redirect(target, response);
        }
        if (response.isRedirect()) {
            throw new PipelineException("get_load_state " + label + ": too many redirects (last " + target + ")");
        }
        String body = response.body;
        if (response.code != 200) {
            throw new PipelineException("get_load_state " + label + " at " + target + ": HTTP " + response.code
                    + " " + excerpt(body));
        }
        try {
            String state = json.readTree(body).path("state").asText("").toUpperCase(Locale.ROOT);
            switch (state) {
                case "VISIBLE":
                case "COMMITTED":
                case "FINISHED":
                    return LoadState.COMMITTED;
                case "PREPARE":
                case "PREPARED":
                case "LOADING":
                    return LoadState.IN_PROGRESS;
                case "ABORTED":
                case "CANCELLED":
                    return LoadState.ABORTED;
                case "UNKNOWN":
                    return LoadState.UNKNOWN;
                default:
                    throw new PipelineException("get_load_state " + label + " at " + target + ": unexpected answer "
                            + excerpt(body));
            }
        } catch (IOException e) {
            throw new PipelineException("get_load_state " + label + " at " + target + ": unreadable answer "
                    + excerpt(body), e);
        }
    }

    private static String excerpt(String body) {
        if (body == null || body.trim().isEmpty()) {
            return "(empty body)";
        }
        String flat = body.replaceAll("\\s+", " ").trim();
        return flat.length() > 300 ? flat.substring(0, 300) + "..." : flat;
    }

    /**
     * Label state as seen by the FE.
     */
    public enum LoadState {
        /** Committed (visible now or after publish): the step is done. */
        COMMITTED,
        /** Still running: retry later. */
        IN_PROGRESS,
        /** Known and aborted: nothing was loaded, the label may be reused. */
        ABORTED,
        /** Never used, or forgotten after the label retention. */
        UNKNOWN
    }

    /**
     * Rows rejected by StarRocks (data quality); retrying the same data fails identically.
     */
    public static final class DataQualityException extends PipelineException {
        private static final long serialVersionUID = 1L;
        private final String label;
        private final long filteredRows;
        private final String errorUrl;

        DataQualityException(String message, String label, long filteredRows, String errorUrl) {
            super(message, null, false);
            this.label = label;
            this.filteredRows = filteredRows;
            this.errorUrl = errorUrl;
        }

        public String getLabel() {
            return label;
        }

        public long getFilteredRows() {
            return filteredRows;
        }

        public String getErrorUrl() {
            return errorUrl;
        }
    }

    @Override
    public void close() throws IOException {
        http.close();
    }

    private static final class Response {
        private final int code;
        private final String location;
        private final String body;

        Response(int code, String location, String body) {
            this.code = code;
            this.location = location;
            this.body = body;
        }

        boolean isRedirect() {
            return code == 301 || code == 302 || code == 307 || code == 308;
        }
    }

    /**
     * Outcome of a load.
     */
    public static final class Result {
        private final String label;
        private final boolean alreadyLoaded;
        private final long loadedRows;
        private final long filteredRows;
        private final String errorUrl;

        Result(String label, boolean alreadyLoaded, long loadedRows, long filteredRows, String errorUrl) {
            this.label = label;
            this.alreadyLoaded = alreadyLoaded;
            this.loadedRows = loadedRows;
            this.filteredRows = filteredRows;
            this.errorUrl = errorUrl;
        }

        public String getLabel() {
            return label;
        }

        /** @return {@code true} when the label had already been loaded (retry of a committed load) */
        public boolean isAlreadyLoaded() {
            return alreadyLoaded;
        }

        public long getLoadedRows() {
            return loadedRows;
        }

        /** @return rows dropped by a tolerant load */
        public long getFilteredRows() {
            return filteredRows;
        }

        public String getErrorUrl() {
            return errorUrl;
        }
    }
}
