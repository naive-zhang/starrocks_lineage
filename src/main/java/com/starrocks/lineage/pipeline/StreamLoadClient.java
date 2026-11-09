package com.starrocks.lineage.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
     * Loads a JSON array into {@code db.table}.
     *
     * @param columns the {@code columns} header (column list with optional {@code col=expr} mappings)
     * @param body    UTF-8 JSON array of objects
     * @return the outcome
     * @throws PipelineException on any failure
     */
    public Result load(String db, String table, String label, String columns, byte[] body) {
        if (!NAME.matcher(db).matches() || !NAME.matcher(table).matches()) {
            throw new IllegalArgumentException("invalid database / table name: " + db + "." + table);
        }
        if (!LABEL.matcher(label).matches()) {
            throw new IllegalArgumentException("invalid stream load label: " + label);
        }
        URI target = feHttpUrl.resolve("/api/" + db + "/" + table + "/_stream_load");
        for (int hop = 0; hop <= MAX_REDIRECTS; hop++) {
            HttpPut put = new HttpPut(target);
            put.setHeader("Authorization", authorization);
            put.setHeader("Expect", "100-continue");
            put.setHeader("Connection", "close");
            put.setHeader("label", label);
            put.setHeader("format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("columns", columns);
            put.setHeader("timeout", String.valueOf(Math.max(1, loadTimeout.getSeconds())));
            put.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
            Response response;
            try {
                response = http.execute(put, r -> {
                    Header location = r.getFirstHeader("Location");
                    String text = r.getEntity() == null ? "" : EntityUtils.toString(r.getEntity(),
                            StandardCharsets.UTF_8);
                    return new Response(r.getCode(), location == null ? null : location.getValue(), text);
                });
            } catch (IOException e) {
                throw new PipelineException("stream load " + label + " to " + db + "." + table + " failed: "
                        + e.getMessage(), e);
            }
            if (response.code == 307 || response.code == 302 || response.code == 301 || response.code == 308) {
                if (response.location == null) {
                    throw new PipelineException("stream load redirect without Location from " + target);
                }
                target = redirectRewriter.apply(target.resolve(response.location));
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

    private Result interpret(String label, String body) {
        JsonNode node;
        try {
            node = json.readTree(body);
        } catch (IOException e) {
            throw new PipelineException("stream load " + label + ": unreadable response " + body, e);
        }
        String status = node.path("Status").asText("");
        long rows = node.path("NumberLoadedRows").asLong(0);
        switch (status) {
            case "Success":
            case "Publish Timeout":
                return new Result(label, false, rows);
            case "Label Already Exists":
                String existing = node.path("ExistingJobStatus").asText("").toUpperCase(Locale.ROOT);
                if ("FINISHED".equals(existing) || "VISIBLE".equals(existing) || "COMMITTED".equals(existing)) {
                    return new Result(label, true, 0L);
                }
                throw new PipelineException("stream load label " + label + " exists with job status '" + existing
                        + "', retry later");
            default:
                throw new PipelineException("stream load " + label + " failed: status=" + status + ", message="
                        + node.path("Message").asText("") + ", errorUrl=" + node.path("ErrorURL").asText(""));
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
    }

    /**
     * Outcome of a load.
     */
    public static final class Result {
        private final String label;
        private final boolean alreadyLoaded;
        private final long loadedRows;

        Result(String label, boolean alreadyLoaded, long loadedRows) {
            this.label = label;
            this.alreadyLoaded = alreadyLoaded;
            this.loadedRows = loadedRows;
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
    }
}
