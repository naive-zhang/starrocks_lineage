package com.starrocks.lineage.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The production artifact itself: {@code target/*-all.jar} started as a separate process with nothing but
 * {@code java -jar} - no JDK tools, no Maven, no test classpath - against StarRocks 3.1.17 with the low-privilege
 * account of {@code scripts/smoke/01_grants.sql}. This is {@code docs/production-deployment.md} step by step.
 */
@Testcontainers(disabledWithoutDocker = true)
class StandaloneJarIT {

    private static final String PASSWORD = "CHANGE_ME_ETL";
    private static StarRocksCluster sr;
    private static Path jar;

    @BeforeAll
    static void start() throws IOException {
        sr = StarRocksCluster.get();
        sr.resetPipelineTables();
        sr.executeScript(StarRocksCluster.read(StarRocksCluster.projectDir().resolve("scripts/smoke/01_grants.sql")));
        try (Stream<Path> files = Files.list(StarRocksCluster.projectDir().resolve("target"))) {
            List<Path> jars = files.filter(p -> p.getFileName().toString().endsWith("-all.jar"))
                    .collect(Collectors.toList());
            assertEquals(1, jars.size(), "run with mvn verify: the package phase builds the -all jar " + jars);
            jar = jars.get(0);
        }
        String props = " BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")";
        sr.execute("DROP DATABASE IF EXISTS jar_shop FORCE", "CREATE DATABASE jar_shop",
                "CREATE TABLE jar_shop.orders (order_id BIGINT, user_id BIGINT, amount DECIMAL(18, 2)) "
                        + "DISTRIBUTED BY HASH(order_id)" + props,
                "CREATE TABLE jar_shop.gmv (uid BIGINT, gmv DECIMAL(38, 2)) DISTRIBUTED BY HASH(uid)" + props,
                "CREATE VIEW jar_shop.v_orders AS SELECT order_id, user_id FROM jar_shop.orders");
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).withNano(0);
        audit("j1", now.minusMinutes(50), "alice", "SELECT amount FROM orders WHERE user_id = 1");
        audit("j2", now.minusMinutes(40), "etl", "INSERT INTO gmv SELECT user_id, SUM(amount) FROM orders "
                + "GROUP BY user_id");
        audit("j3", now.minusMinutes(30), "bob", "SELEC broken");
        // views are separate privilege objects: without the view grant of 01_grants.sql this is analysed blind
        audit("j4", now.minusMinutes(20), "carol", "SELECT * FROM v_orders");
    }

    private static void audit(String id, LocalDateTime at, String user, String stmt) {
        sr.execute("INSERT INTO starrocks_audit_db__.starrocks_audit_tbl__ (queryId, `timestamp`, queryType, "
                + "clientIp, `user`, authorizedUser, resourceGroup, `catalog`, db, state, errorCode, queryTime, "
                + "scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, stmtId, isQuery, feIp, stmt, digest, "
                + "planCpuCosts, planMemCosts) VALUES ('" + id + "', '" + at.toString().replace('T', ' ') + "', "
                + "'Query', 'h', '" + user + "', '" + user + "', 'wg', 'default_catalog', 'jar_shop', 'EOF', '', 10, "
                + "100, 1, 1, 0, 0, 1, " + (stmt.startsWith("INSERT") ? 0 : 1) + ", 'fe', '" + stmt + "', 'd', 0, 0)");
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("SR_FE_HOST", sr.host());
        env.put("SR_QUERY_PORT", String.valueOf(sr.queryPort()));
        env.put("SR_HTTP_PORT", String.valueOf(sr.feHttpPort()));
        env.put("SR_BE_REWRITE", "127.0.0.1:8040=" + sr.host() + ":" + sr.beHttpPort());
        env.put("SR_USER", "lineage_etl");
        env.put("SR_PASSWORD", PASSWORD);
        env.put("TIME_ZONE", "UTC");
        env.put("PIPELINE", "jar_it");
        env.put("OWNER", "jar-it-node");
        env.put("LATE_ARRIVAL_SECONDS", "60");
        return env;
    }

    private static ProcessBuilder jar(Map<String, String> env, String... args) {
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        String[] command = new String[args.length + 3];
        command[0] = java;
        command[1] = "-jar";
        command[2] = jar.toString();
        System.arraycopy(args, 0, command, 3, args.length);
        ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
        pb.environment().keySet().removeIf(k -> k.startsWith("SR_") || CliKeys.contains(k));
        pb.environment().putAll(env);
        return pb;
    }

    /** Keys of the program, cleared from the inherited environment so only {@link #env()} applies. */
    private static final class CliKeys {
        static boolean contains(String k) {
            return Stream.of("LINEAGE_DB", "AUDIT_TABLE", "PIPELINE", "OWNER", "TIME_ZONE", "THREADS")
                    .anyMatch(k::equals);
        }
    }

    private static final class Run {
        final int exit;
        final String out;

        Run(int exit, String out) {
            this.exit = exit;
            this.out = out;
        }
    }

    private static Run exec(Map<String, String> env, String... args) throws Exception {
        Process p = jar(env, args).start();
        String out = read(p.getInputStream());
        assertTrue(p.waitFor(5, TimeUnit.MINUTES));
        return new Run(p.exitValue(), out);
    }

    private static String read(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] b = new byte[8192];
        int n;
        while ((n = in.read(b)) > 0) {
            buf.write(b, 0, n);
        }
        return new String(buf.toByteArray(), StandardCharsets.UTF_8);
    }

    private static long num(Object o) {
        return o instanceof BigDecimal ? ((BigDecimal) o).longValue() : ((Number) o).longValue();
    }

    @Test
    void checkAnalyzeRunStatusAndDaemonWithOnlyAJavaRuntime() throws Exception {
        Run check = exec(env(), "check");
        assertEquals(0, check.exit, check.out);
        assertTrue(check.out.contains("CHECK RESULT PASSED"), check.out);
        for (String name : new String[]{"connection", "clock", "audit_table", "target_tables", "metadata_visibility",
                "sql_write", "stream_load", "pipeline_state"}) {
            assertTrue(check.out.contains("CHECK " + name + " OK"), name + "\n" + check.out);
        }
        assertTrue(check.out.contains("SR_PASSWORD=******") && !check.out.contains(PASSWORD), "password masked");
        assertEquals(0L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_dead_letter "
                + "WHERE pipeline_name = '__preflight__'").get(0).get("c")), "check cleans up after itself");

        Run analyze = exec(env(), "analyze", "2");
        assertEquals(0, analyze.exit, analyze.out);
        assertTrue(analyze.out.contains("ANALYZE total=4 {SUCCESS=3, PARSE_ERROR=1} degraded=0"), analyze.out);
        assertFalse(analyze.out.contains("without metadata"), "the view is visible to lineage_etl\n" + analyze.out);
        assertTrue(analyze.out.contains("FAIL PARSE_ERROR j3"), analyze.out);
        assertEquals(0L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_batch_member")
                .get(0).get("c")), "analyze writes nothing");

        Run run = exec(env(), "run");
        assertEquals(0, run.exit, run.out);
        assertTrue(run.out.contains("SUMMARY") && run.out.contains("records=4 last_outcome=IDLE"), run.out);
        assertTrue(num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_column_lineage WHERE "
                + "target_table = 'gmv'").get(0).get("c")) > 0, "lineage written");
        assertEquals(1L, num(sr.query("SELECT SUM(access_query_cnt) c FROM audit_lineage_db"
                + ".audit_column_hourly_usage WHERE table_name = 'orders' AND column_name = 'amount'")
                .get(0).get("c")));

        Run status = exec(env(), "status");
        assertEquals(0, status.exit, status.out);
        assertTrue(status.out.contains("STATUS pipeline=jar_it") && status.out.contains("pending=-"), status.out);
        assertTrue(status.out.contains("dead_letters kind=PARSE_ERROR status=OPEN count=1"), status.out);
        Run again = exec(env(), "run");
        assertTrue(again.out.matches("(?s).*SUMMARY committed=\\d+ records=0 last_outcome=IDLE.*"),
                "nothing new to process (a slice that closed meanwhile is committed empty)\n" + again.out);

        // daemon: runs until SIGTERM, then releases the lease for a standby
        // Process.destroy() closes the pipes right after SIGTERM: the output goes to a file to keep the shutdown log
        Path daemonLog = Files.createTempFile("lineage-daemon", ".log");
        Process daemon = jar(env(), "daemon").redirectOutput(daemonLog.toFile()).start();
        long deadline = System.currentTimeMillis() + 120_000;
        boolean leased = false;
        while (!leased && System.currentTimeMillis() < deadline) {
            StarRocksCluster.sleep(1_000);
            List<Map<String, Object>> cp = sr.query("SELECT owner, lease_expire_time FROM audit_lineage_db"
                    + ".audit_lineage_sync_checkpoint WHERE pipeline_name = 'jar_it'");
            leased = ((LocalDateTime) cp.get(0).get("lease_expire_time"))
                    .isAfter(LocalDateTime.now(ZoneOffset.UTC).plusMinutes(5));
        }
        assertTrue(leased, "the daemon holds the lease");
        daemon.destroy();
        assertTrue(daemon.waitFor(60, TimeUnit.SECONDS));
        String out = new String(Files.readAllBytes(daemonLog), StandardCharsets.UTF_8);
        Files.delete(daemonLog);
        assertTrue(out.contains("DAEMON started") && out.contains("DAEMON stopping"), out);
        LocalDateTime expire = (LocalDateTime) sr.query("SELECT lease_expire_time FROM audit_lineage_db"
                + ".audit_lineage_sync_checkpoint WHERE pipeline_name = 'jar_it'").get(0).get("lease_expire_time");
        assertTrue(!expire.isAfter(LocalDateTime.now(ZoneOffset.UTC).plusSeconds(5)),
                "SIGTERM released the lease: a standby takes over at once (" + expire + ")\n" + out);
    }

    @Test
    void checkFailsOnTablesOfTheOldSchema() throws Exception {
        sr.execute("DROP DATABASE IF EXISTS jar_old_lineage FORCE", "CREATE DATABASE jar_old_lineage",
                "CREATE TABLE jar_old_lineage.audit_column_lineage_stage (batch_id VARCHAR(64)) "
                        + "DISTRIBUTED BY HASH(batch_id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")");
        Map<String, String> env = env();
        env.put("LINEAGE_DB", "jar_old_lineage");
        Run check = exec(env, "check");
        assertEquals(1, check.exit, check.out);
        assertTrue(check.out.contains("CHECK target_tables FAIL"), check.out);
        assertTrue(check.out.contains("old (Stage 4) schema"), check.out);
        assertTrue(check.out.contains("CHECK RESULT FAILED"), check.out);
    }
}
