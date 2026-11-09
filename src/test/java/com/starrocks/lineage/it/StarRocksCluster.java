package com.starrocks.lineage.it;

import com.starrocks.lineage.pipeline.StreamLoadClient;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * One real StarRocks 3.1 (all-in-one FE + BE) per test JVM, started with Testcontainers and shared by every
 * {@code *IT} class.
 *
 * <ul>
 *   <li>{@code be.conf} of the image plus {@code min_file_descriptor_number = 10000}: sandboxed Docker hosts often
 *       cap {@code nofile} below the 60 000 the BE requires, and {@code --ulimit} cannot raise a hard limit.</li>
 *   <li>The FE redirects stream loads to the BE's registered address ({@code 127.0.0.1:8040} inside the container);
 *       {@link #streamLoadClient()} rewrites it to the mapped host port.</li>
 *   <li>{@link #applyProjectDdl()} runs {@code sql/ddl/00..05} of this repository (with {@code replication_num = 1}
 *       for the single BE), so the DDL that ships is the DDL that is tested.</li>
 * </ul>
 */
public final class StarRocksCluster {

    public static final String IMAGE = System.getProperty("starrocks.image", "starrocks/allin1-ubuntu:3.1.17");
    private static final int QUERY_PORT = 9030;
    private static final int FE_HTTP_PORT = 8030;
    private static final int BE_HTTP_PORT = 8040;

    private static StarRocksCluster instance;

    private final GenericContainer<?> container;
    private final HikariDataSource dataSource;
    private boolean ddlApplied;

    private StarRocksCluster() {
        container = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withExposedPorts(QUERY_PORT, FE_HTTP_PORT, BE_HTTP_PORT)
                .withCopyFileToContainer(MountableFile.forClasspathResource("starrocks/be.conf"),
                        "/data/deploy/starrocks/be/conf/be.conf")
                .waitingFor(Wait.forListeningPorts(QUERY_PORT).withStartupTimeout(Duration.ofMinutes(5)));
        container.start();
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl("jdbc:mysql://" + container.getHost() + ":" + container.getMappedPort(QUERY_PORT)
                + "/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=300000");
        hc.setUsername("root");
        hc.setPassword("");
        hc.setMaximumPoolSize(12);
        hc.setInitializationFailTimeout(-1);
        dataSource = new HikariDataSource(hc);
        awaitBackendAlive(Duration.ofMinutes(5));
    }

    /** @return the shared cluster, started on first use */
    public static synchronized StarRocksCluster get() {
        if (instance == null) {
            instance = new StarRocksCluster();
            Runtime.getRuntime().addShutdownHook(new Thread(instance::stop, "starrocks-container-stop"));
        }
        return instance;
    }

    private void stop() {
        dataSource.close();
        container.stop();
    }

    private void awaitBackendAlive(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        String last = "no answer";
        while (System.currentTimeMillis() < deadline) {
            try {
                List<Map<String, Object>> backends = query("SHOW BACKENDS");
                if (!backends.isEmpty() && "true".equalsIgnoreCase(String.valueOf(backends.get(0).get("Alive")))) {
                    // the BE is alive; make sure it accepts a tablet and a load
                    execute("CREATE DATABASE IF NOT EXISTS it_probe",
                            "CREATE TABLE IF NOT EXISTS it_probe.p (a INT) DISTRIBUTED BY HASH(a) BUCKETS 1 "
                                    + "PROPERTIES (\"replication_num\" = \"1\")",
                            "INSERT INTO it_probe.p VALUES (1)");
                    return;
                }
                last = String.valueOf(backends);
            } catch (RuntimeException e) {
                last = e.getMessage();
            }
            sleep(3_000);
        }
        throw new IllegalStateException("StarRocks BE not alive after " + timeout + ": " + last + "\n"
                + container.getLogs());
    }

    public DataSource dataSource() {
        return dataSource;
    }

    /** A pooled data source of another account. */
    public HikariDataSource dataSource(String user, String password) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(dataSource.getJdbcUrl());
        hc.setUsername(user);
        hc.setPassword(password);
        hc.setMaximumPoolSize(4);
        return new HikariDataSource(hc);
    }

    public int beHttpPort() {
        return container.getMappedPort(BE_HTTP_PORT);
    }

    public int feHttpPort() {
        return container.getMappedPort(FE_HTTP_PORT);
    }

    public String host() {
        return container.getHost();
    }

    public int queryPort() {
        return container.getMappedPort(QUERY_PORT);
    }

    public URI feHttpUrl() {
        return URI.create("http://" + container.getHost() + ":" + container.getMappedPort(FE_HTTP_PORT));
    }

    /** Stream Load client whose BE redirects are rewritten to the mapped BE port. */
    public StreamLoadClient streamLoadClient() {
        String host = container.getHost();
        int bePort = container.getMappedPort(BE_HTTP_PORT);
        return new StreamLoadClient(feHttpUrl(), "root", "", Duration.ofMinutes(2), uri -> {
            if (uri.getPort() != BE_HTTP_PORT) {
                return uri;
            }
            try {
                return new URI(uri.getScheme(), uri.getUserInfo(), host, bePort, uri.getPath(), uri.getQuery(),
                        uri.getFragment());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        });
    }

    public GenericContainer<?> container() {
        return container;
    }

    // ------------------------------------------------------------------------------------------------ SQL helpers

    public void execute(String... statements) {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                try {
                    st.execute(sql);
                } catch (SQLException e) {
                    throw new IllegalStateException("failed: " + sql + "\n" + e.getMessage(), e);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<Map<String, Object>> query(String sql, Object... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rows(rs);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed: " + sql + "\n" + e.getMessage(), e);
        }
    }

    static List<Map<String, Object>> rows(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                row.put(md.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Runs a SQL script (full-line {@code --} comments removed, statements separated by {@code ;} at a line end)
     * and returns the rows of every statement that produced a result set, in order.
     */
    public List<List<Map<String, Object>>> executeScript(String script) {
        return executeScript(dataSource, script);
    }

    /** {@link #executeScript(String)} with another account's data source. */
    public static List<List<Map<String, Object>>> executeScript(DataSource ds, String script) {
        List<List<Map<String, Object>>> results = new ArrayList<>();
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            for (String sql : splitScript(script)) {
                try {
                    if (st.execute(sql)) {
                        try (ResultSet rs = st.getResultSet()) {
                            results.add(rows(rs));
                        }
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException("failed: " + sql + "\n" + e.getMessage(), e);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return results;
    }

    public static List<String> splitScript(String script) {
        String body = Stream.of(script.split("\n"))
                .filter(line -> !line.trim().startsWith("--"))
                .collect(Collectors.joining("\n"));
        List<String> statements = new ArrayList<>();
        for (String s : body.split(";\\s*(\\n|$)")) {
            if (!s.trim().isEmpty()) {
                statements.add(s.trim());
            }
        }
        return statements;
    }

    public static Path projectDir() {
        return Paths.get(System.getProperty("project.basedir", System.getProperty("user.dir")));
    }

    public static String read(Path p) {
        try {
            return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Runs {@code sql/ddl/00..05} of the repository once, with {@code replication_num = 1}. */
    public synchronized void applyProjectDdl() {
        if (ddlApplied) {
            return;
        }
        try (Stream<Path> files = Files.list(projectDir().resolve("sql/ddl"))) {
            for (Path f : files.sorted().collect(Collectors.toList())) {
                executeScript(read(f).replace("\"replication_num\" = \"3\"", "\"replication_num\" = \"1\""));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        createAuditTable();
        ddlApplied = true;
    }

    /**
     * The audit table of the StarRocks 3.1 audit loader plugin (official DDL, single replica). The image only ships
     * the built-in AuditLogBuilder, so the integration tests write audit rows themselves.
     */
    private void createAuditTable() {
        execute("CREATE DATABASE IF NOT EXISTS starrocks_audit_db__",
                "CREATE TABLE IF NOT EXISTS starrocks_audit_db__.starrocks_audit_tbl__ (\n"
                        + "  `queryId` VARCHAR(64), `timestamp` DATETIME NOT NULL, `queryType` VARCHAR(12),\n"
                        + "  `clientIp` VARCHAR(32), `user` VARCHAR(64), `authorizedUser` VARCHAR(64),\n"
                        + "  `resourceGroup` VARCHAR(64), `catalog` VARCHAR(32), `db` VARCHAR(96),\n"
                        + "  `state` VARCHAR(8), `errorCode` VARCHAR(96), `queryTime` BIGINT, `scanBytes` BIGINT,\n"
                        + "  `scanRows` BIGINT, `returnRows` BIGINT, `cpuCostNs` BIGINT, `memCostBytes` BIGINT,\n"
                        + "  `stmtId` INT, `isQuery` TINYINT, `feIp` VARCHAR(128), `stmt` VARCHAR(1048576),\n"
                        + "  `digest` VARCHAR(32), `planCpuCosts` DOUBLE, `planMemCosts` DOUBLE\n"
                        + ") ENGINE = OLAP DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)\n"
                        + "PARTITION BY RANGE (`timestamp`) ()\n"
                        + "DISTRIBUTED BY HASH (`queryId`) BUCKETS 3\n"
                        + "PROPERTIES (\"dynamic_partition.time_unit\" = \"DAY\","
                        + " \"dynamic_partition.start\" = \"-30\","
                        + " \"dynamic_partition.end\" = \"3\", \"dynamic_partition.prefix\" = \"p\","
                        + " \"dynamic_partition.buckets\" = \"3\", \"dynamic_partition.enable\" = \"true\","
                        + " \"replication_num\" = \"1\")");
    }

    /** Empties every table written by the pipeline and the audit table. */
    public void resetPipelineTables() {
        applyProjectDdl();
        execute("TRUNCATE TABLE audit_lineage_db.audit_column_lineage",
                "TRUNCATE TABLE audit_lineage_db.audit_column_lineage_stage",
                "TRUNCATE TABLE audit_lineage_db.audit_column_hourly_usage",
                "TRUNCATE TABLE audit_lineage_db.audit_user_dict",
                "TRUNCATE TABLE audit_lineage_db.audit_lineage_sync_checkpoint",
                "TRUNCATE TABLE audit_lineage_db.audit_lineage_merge_log",
                "TRUNCATE TABLE starrocks_audit_db__.starrocks_audit_tbl__");
    }

    /**
     * A recent audit timestamp that is guaranteed to fall into today's (UTC) audit partition: dynamic partitioning
     * pre-creates today and the next days only.
     *
     * @param minutesBack how far before now
     * @param minimumSpan minutes that must remain between the returned instant and now (the test writes records up
     *                    to that span); the test is skipped when it is too close to midnight
     */
    public static java.time.LocalDateTime todayMinutesAgo(int minutesBack, int minimumSpan) {
        java.time.LocalDateTime now = java.time.LocalDateTime.now(java.time.ZoneOffset.UTC).withNano(0);
        java.time.LocalDateTime start = now.minusMinutes(minutesBack);
        if (!start.toLocalDate().equals(now.toLocalDate())) {
            start = now.toLocalDate().atStartOfDay().plusSeconds(1);
        }
        org.junit.jupiter.api.Assumptions.assumeTrue(
                java.time.Duration.between(start, now).toMinutes() >= minimumSpan,
                "too close to midnight UTC for today's audit partition, re-run in a few minutes");
        return start;
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
