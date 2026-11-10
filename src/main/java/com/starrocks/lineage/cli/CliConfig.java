package com.starrocks.lineage.cli;

import com.starrocks.lineage.pipeline.PipelineConfig;
import com.starrocks.lineage.pipeline.StreamLoadClient;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

/**
 * Configuration of the standalone program: built-in defaults, overridden by a properties file ({@code --config}),
 * overridden by environment variables. Keys are the same in the file and the environment.
 */
public final class CliConfig {

    /** Every key with its default ({@code null}: required). */
    static final Map<String, String> DEFAULTS;

    static {
        Map<String, String> d = new LinkedHashMap<>();
        d.put("SR_FE_HOST", null);
        d.put("SR_QUERY_PORT", "9030");
        d.put("SR_HTTP_PORT", "8030");
        d.put("SR_USER", "lineage_etl");
        d.put("SR_PASSWORD", "");
        d.put("SR_BE_REWRITE", "");
        d.put("LINEAGE_DB", "audit_lineage_db");
        d.put("AUDIT_TABLE", "starrocks_audit_db__.starrocks_audit_tbl__");
        d.put("PIPELINE", "audit_lineage");
        d.put("OWNER", "");
        d.put("TIME_ZONE", "");
        d.put("INITIAL_LOOKBACK_HOURS", "24");
        d.put("SLICE_SECONDS", "300");
        d.put("LATE_ARRIVAL_SECONDS", "300");
        d.put("RECONCILE_SECONDS", "900");
        d.put("RECONCILE_WINDOW_HOURS", "24");
        d.put("LABEL_RETENTION_HOURS", "48");
        d.put("ALERT_AFTER_ATTEMPTS", "5");
        d.put("LEASE_TTL_SECONDS", "600");
        d.put("THREADS", String.valueOf(Runtime.getRuntime().availableProcessors()));
        d.put("INTERVAL_SECONDS", "60");
        d.put("LOAD_TIMEOUT_SECONDS", "600");
        DEFAULTS = Collections.unmodifiableMap(d);
    }

    private final Map<String, String> values;

    private CliConfig(Map<String, String> values) {
        this.values = values;
    }

    /**
     * @param env  environment variables (only known keys are read)
     * @param file optional properties file, may be {@code null}
     */
    public static CliConfig load(Map<String, String> env, Path file) {
        Map<String, String> v = new LinkedHashMap<>(DEFAULTS);
        if (file != null) {
            Properties p = new Properties();
            try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                p.load(r);
            } catch (IOException e) {
                throw new IllegalArgumentException("cannot read config file " + file + ": " + e.getMessage(), e);
            }
            for (String key : p.stringPropertyNames()) {
                if (!DEFAULTS.containsKey(key)) {
                    throw new IllegalArgumentException("unknown key " + key + " in " + file);
                }
                v.put(key, p.getProperty(key).trim());
            }
        }
        for (String key : DEFAULTS.keySet()) {
            String e = env.get(key);
            if (e != null && !e.isEmpty()) {
                v.put(key, e.trim());
            }
        }
        if (v.get("SR_FE_HOST") == null || v.get("SR_FE_HOST").isEmpty()) {
            throw new IllegalArgumentException("SR_FE_HOST is required");
        }
        if (v.get("OWNER").isEmpty()) {
            v.put("OWNER", hostname());
        }
        CliConfig c = new CliConfig(v);
        c.validate();
        return c;
    }

    private void validate() {
        for (String key : DEFAULTS.keySet()) {
            if (key.endsWith("_PORT") || key.endsWith("_SECONDS") || key.endsWith("_HOURS") || key.equals("THREADS")
                    || key.equals("ALERT_AFTER_ATTEMPTS")) {
                if (intValue(key) <= 0) {
                    throw new IllegalArgumentException(key + " must be > 0: " + values.get(key));
                }
            }
        }
        if (!lineageDb().matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("invalid LINEAGE_DB " + lineageDb());
        }
        if (!auditTable().matches("[A-Za-z0-9_]+\\.[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("AUDIT_TABLE must be db.table: " + auditTable());
        }
        beRewrite();
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "lineage-" + ProcessHandle.current().pid();
        }
    }

    public String get(String key) {
        if (!DEFAULTS.containsKey(key)) {
            throw new IllegalArgumentException("unknown key " + key);
        }
        return values.get(key);
    }

    int intValue(String key) {
        try {
            return Integer.parseInt(get(key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " is not a number: " + get(key));
        }
    }

    public String lineageDb() {
        return get("LINEAGE_DB");
    }

    public String auditTable() {
        return get("AUDIT_TABLE");
    }

    public String pipeline() {
        return get("PIPELINE");
    }

    public String owner() {
        return get("OWNER");
    }

    public Duration interval() {
        return Duration.ofSeconds(intValue("INTERVAL_SECONDS"));
    }

    /** @return the effective configuration with the password masked, for logs */
    public Map<String, String> describe() {
        Map<String, String> m = new TreeMap<>(values);
        m.put("SR_PASSWORD", values.get("SR_PASSWORD").isEmpty() ? "(empty)" : "******");
        return m;
    }

    /** The pipeline configuration; {@code now} anchors the start of a first run. */
    public PipelineConfig pipelineConfig(LocalDateTime now) {
        return PipelineConfig.builder(pipeline(), owner())
                .initialStart(now.minusHours(intValue("INITIAL_LOOKBACK_HOURS")))
                .sliceSize(Duration.ofSeconds(intValue("SLICE_SECONDS")))
                .lateArrival(Duration.ofSeconds(intValue("LATE_ARRIVAL_SECONDS")))
                .reconcileInterval(Duration.ofSeconds(intValue("RECONCILE_SECONDS")))
                .reconcileWindow(Duration.ofHours(intValue("RECONCILE_WINDOW_HOURS")))
                .labelRetention(Duration.ofHours(intValue("LABEL_RETENTION_HOURS")))
                .alertAfterAttempts(intValue("ALERT_AFTER_ATTEMPTS"))
                .leaseTtl(Duration.ofSeconds(intValue("LEASE_TTL_SECONDS")))
                .analyzerThreads(intValue("THREADS"))
                .build();
    }

    public HikariDataSource dataSource() {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl("jdbc:mysql://" + get("SR_FE_HOST") + ":" + get("SR_QUERY_PORT")
                + "/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=600000");
        hc.setUsername(get("SR_USER"));
        hc.setPassword(get("SR_PASSWORD"));
        hc.setMaximumPoolSize(Math.max(4, intValue("THREADS") + 2));
        hc.setPoolName("audit-lineage");
        return new HikariDataSource(hc);
    }

    public StreamLoadClient streamLoad() {
        return new StreamLoadClient(feHttpUrl(), get("SR_USER"), get("SR_PASSWORD"),
                Duration.ofSeconds(intValue("LOAD_TIMEOUT_SECONDS")), beRewrite());
    }

    public URI feHttpUrl() {
        return URI.create("http://" + get("SR_FE_HOST") + ":" + get("SR_HTTP_PORT"));
    }

    /**
     * {@code SR_BE_REWRITE="be-host:8040=reachable-host:port[,more...]"}: the BE address a Stream Load is redirected
     * to, mapped to an address this machine can reach (NAT, Kubernetes, containers).
     */
    UnaryOperator<URI> beRewrite() {
        String spec = get("SR_BE_REWRITE");
        if (spec.isEmpty()) {
            return UnaryOperator.identity();
        }
        Map<String, String[]> rules = new LinkedHashMap<>();
        for (String rule : spec.split(",")) {
            String[] fromTo = rule.trim().split("=");
            if (fromTo.length != 2 || !fromTo[0].contains(":") || !fromTo[1].contains(":")) {
                throw new IllegalArgumentException("SR_BE_REWRITE rule must be host:port=host:port: " + rule);
            }
            String[] to = fromTo[1].split(":");
            Integer.parseInt(to[1]);
            rules.put(fromTo[0].trim(), to);
        }
        return uri -> {
            String[] to = rules.get(uri.getHost() + ":" + uri.getPort());
            if (to == null) {
                return uri;
            }
            try {
                return new URI(uri.getScheme(), uri.getUserInfo(), to[0], Integer.parseInt(to[1]), uri.getPath(),
                        uri.getQuery(), uri.getFragment());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        };
    }

    @Override
    public String toString() {
        return describe().toString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CliConfig && values.equals(((CliConfig) o).values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
}
