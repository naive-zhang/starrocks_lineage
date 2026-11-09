package com.starrocks.lineage.it;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rehearses {@code docs/staging-smoke-test.md} step by step on a real StarRocks 3.1, with the very scripts of
 * {@code scripts/smoke} - so the runbook given to operators is known to work and its expected values are right.
 * The only difference to a staging cluster: there is no audit loader plugin in the image, so the test writes the
 * audit rows of the scenario statements it executes.
 */
@Testcontainers(disabledWithoutDocker = true)
class SmokeRunbookIT {

    private static final Path SMOKE = StarRocksCluster.projectDir().resolve("scripts/smoke");
    private static final String ETL_PASSWORD = "CHANGE_ME_ETL";
    private static final String SMOKE_PASSWORD = "CHANGE_ME_SMOKE";
    private static StarRocksCluster sr;
    private static HikariDataSource etl;

    @BeforeAll
    static void start() {
        sr = StarRocksCluster.get();
        sr.resetPipelineTables();
        sr.execute("DROP DATABASE IF EXISTS lineage_smoke_db FORCE", "DROP USER IF EXISTS 'lineage_smoke_user'@'%'",
                "DROP USER IF EXISTS 'lineage_etl'@'%'");
    }

    @AfterAll
    static void close() {
        if (etl != null) {
            etl.close();
        }
    }

    private static String script(String name) {
        return StarRocksCluster.read(SMOKE.resolve(name));
    }

    private static long num(Object o) {
        return o instanceof BigDecimal ? ((BigDecimal) o).longValue() : ((Number) o).longValue();
    }

    @Test
    void runbookEndToEnd() throws Exception {
        // ---- step 2: grants and fixtures (admin)
        List<List<Map<String, Object>>> grants = sr.executeScript(script("01_grants.sql"));
        assertTrue(grants.get(grants.size() - 1).toString().contains("lineage_etl"));
        sr.executeScript(script("02_smoke_fixtures.sql"));
        etl = sr.dataSource("lineage_etl", ETL_PASSWORD);

        // ---- step 3: manual checks as lineage_etl, around the stream load script
        String checks = script("03_manual_checks.sql");
        int split = checks.indexOf("-- 3.5 ");
        List<List<Map<String, Object>>> partA = StarRocksCluster.executeScript(etl, checks.substring(0, split));
        assertManualChecksPartA(partA);
        streamLoadCheckInsideTheContainer();
        List<List<Map<String, Object>>> partB = StarRocksCluster.executeScript(etl, checks.substring(split));
        Map<String, Object> usage = partB.get(0).get(0);
        assertEquals(2, num(usage.get("access")));
        assertEquals(2, num(usage.get("uv_exact")));
        assertEquals(2, num(usage.get("uv_approx")));
        assertEquals("1,2", usage.get("ids_expect_1_2"));
        assertEquals(2, partB.get(1).size(), "two dictionary rows");
        assertEquals(0L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_column_hourly_usage")
                .get(0).get("c")), "3.6 reset the synthetic rows");

        // ---- step 5: first cycle on an empty audit window, then W0
        Map<String, String> first = runJshell("smoke-run-cycle.jsh", "0");
        assertEquals("IDLE", first.get("outcome"));
        Map<String, Object> w0 = verify().get(0).get(0);

        // ---- step 6: scenario as lineage_smoke_user, audit rows like the audit loader would write them
        runScenario();
        Map<String, String> cycle = runJshell("smoke-run-cycle.jsh", "1");
        assertEquals("COMMITTED", cycle.get("outcome"), cycle.toString());
        assertEquals("8", cycle.get("records"));
        assertEquals("false", cycle.get("replay"));
        List<List<Map<String, Object>>> v = verify();
        assertScenarioResults(v);

        // ---- step 7: exactly-once drill
        String drill = script("07_replay_drill.sql")
                .replace("${W0_TIME}", String.valueOf(w0.get("watermark_time")).replace(".0", ""))
                .replace("${W0_QUERY_ID}", String.valueOf(w0.get("watermark_query_id")))
                .replace("${BATCH_ID}", cycle.get("batch"))
                .replace("${END_TIME}", cycle.get("end_time"))
                .replace("${END_QUERY_ID}", cycle.get("end_query_id"));
        List<List<Map<String, Object>>> rewound = StarRocksCluster.executeScript(etl, drill);
        assertEquals(cycle.get("batch"), rewound.get(0).get(0).get("pending_batch_id"));
        Map<String, String> replay = runJshell("smoke-run-cycle.jsh", "1");
        assertEquals("true", replay.get("replay"), replay.toString());
        assertEquals(cycle.get("batch"), replay.get("batch"));
        assertEquals("8", replay.get("records"), "w0=" + w0 + " rewound=" + rewound + "\n" + replay.get("out"));
        List<List<Map<String, Object>>> afterReplay = verify();
        assertEquals(v.subList(1, 4), afterReplay.subList(1, 4), "usage and lineage unchanged by the replay");

        // ---- step 4 (read-only analysis) on the scenario window
        Map<String, String> analyze = runJshell("smoke-analyze.jsh", "0");
        assertTrue(analyze.get("line").contains("SUCCESS=6"), analyze.toString());
        assertTrue(analyze.get("line").contains("SKIPPED_FAILED_QUERY=1"), analyze.toString());
        assertTrue(analyze.get("line").contains("error_rate_percent=0.0"), analyze.toString());

        // ---- step 9: cleanup
        sr.executeScript(script("06_cleanup.sql"));
        assertTrue(sr.query("SHOW DATABASES").stream().noneMatch(r -> r.containsValue("lineage_smoke_db")));
        assertTrue(sr.query("SELECT * FROM audit_lineage_db.audit_lineage_sync_checkpoint "
                + "WHERE pipeline_name = 'audit_lineage_smoke'").isEmpty());
    }

    private static void assertManualChecksPartA(List<List<Map<String, Object>>> results) {
        List<Map<String, Object>> rows = results.stream().flatMap(List::stream).collect(Collectors.toList());
        Map<String, Object> cp = rows.stream().filter(r -> r.containsKey("expect_q2")).findFirst().get();
        assertEquals(1, num(cp.get("rows_expect_1")));
        assertEquals("q2", cp.get("expect_q2"));
        assertEquals("b", cp.get("expect_b"));
        List<Map<String, Object>> merges = rows.stream().filter(r -> r.containsKey("last_batch_id"))
                .collect(Collectors.toList());
        assertEquals(3, merges.size(), "3.4 (a), (b), (c)");
        assertEquals(Arrays.asList(2L, 2L, 5L), merges.stream().map(r -> num(r.get("exec_count")))
                .collect(Collectors.toList()));
        assertEquals(Arrays.asList("IDENTITY", "IDENTITY", "EXPRESSION"), merges.stream()
                .map(r -> r.get("transform_type")).collect(Collectors.toList()));
        assertEquals(Arrays.asList("smoke_b1", "smoke_b1", "smoke_b2"), merges.stream()
                .map(r -> r.get("last_batch_id")).collect(Collectors.toList()));
        assertEquals(1, rows.stream().filter(r -> r.containsKey("rows_expect_1") && !r.containsKey("expect_q2"))
                .mapToLong(r -> num(r.get("rows_expect_1"))).sum());
        List<Map<String, Object>> metadata = results.stream().filter(r -> !r.isEmpty()
                && r.get(0).containsKey("ORDINAL_POSITION")).findFirst().get();
        assertEquals(Arrays.asList("order_id", "user_id", "amount", "dt"), metadata.stream()
                .map(r -> r.get("COLUMN_NAME")).collect(Collectors.toList()));
    }

    private static void streamLoadCheckInsideTheContainer() throws Exception {
        sr.container().copyFileToContainer(MountableFile.forHostPath(SMOKE.resolve("stream_load_check.sh")),
                "/tmp/stream_load_check.sh");
        Container.ExecResult r = sr.container().execInContainer("bash", "-c", "SR_FE_HOST=127.0.0.1 "
                + "SR_USER=lineage_etl SR_PASSWORD=" + ETL_PASSWORD + " bash /tmp/stream_load_check.sh");
        String out = r.getStdout();
        assertEquals(0, r.getExitCode(), out + r.getStderr());
        assertEquals(2, count(out, "\"Status\": \"Success\""), out);
        assertEquals(1, count(out, "\"Status\": \"Label Already Exists\""), out);
        assertTrue(out.contains("\"ExistingJobStatus\": \"FINISHED\""), out);
    }

    private static int count(String text, String needle) {
        int n = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + 1)) {
            n++;
        }
        return n;
    }

    /** Executes 04_scenario.sql as lineage_smoke_user and writes the audit rows the audit loader would write. */
    private static void runScenario() throws SQLException {
        List<String> statements = StarRocksCluster.splitScript(script("04_scenario.sql"));
        assertEquals(8, statements.size());
        LocalDateTime base = StarRocksCluster.todayMinutesAgo(5, 3);
        try (HikariDataSource smoke = sr.dataSource("lineage_smoke_user", SMOKE_PASSWORD);
             Connection c = smoke.getConnection(); Statement st = c.createStatement()) {
            for (int i = 0; i < statements.size(); i++) {
                String state = "EOF";
                String errorCode = "";
                try {
                    st.execute(statements.get(i));
                } catch (SQLException e) {
                    state = "ERR";
                    errorCode = String.valueOf(e.getErrorCode());
                }
                audit(String.format("smoke-%02d", i), base.plusSeconds(i), state, errorCode, statements.get(i));
            }
        }
        assertEquals(1L, num(sr.query("SELECT COUNT(*) c FROM starrocks_audit_db__.starrocks_audit_tbl__ "
                + "WHERE state = 'ERR'").get(0).get("c")), "exactly F1 failed");
    }

    private static void audit(String queryId, LocalDateTime ts, String state, String errorCode, String stmt) {
        try (Connection c = sr.dataSource().getConnection(); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO starrocks_audit_db__.starrocks_audit_tbl__ (queryId, `timestamp`, queryType, clientIp, "
                        + "`user`, authorizedUser, resourceGroup, `catalog`, db, state, errorCode, queryTime, "
                        + "scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, stmtId, isQuery, feIp, stmt, "
                        + "digest, planCpuCosts, planMemCosts) VALUES (?, ?, 'Query', '10.0.0.9', "
                        + "'lineage_smoke_user', '''lineage_smoke_user''@''%''', 'default_wg', 'default_catalog', '', "
                        + "?, ?, 20, 1024, 2, 1, 0, 0, 1, 1, 'fe', ?, ?, 0, 0)")) {
            ps.setString(1, queryId);
            ps.setObject(2, ts);
            ps.setString(3, state);
            ps.setString(4, errorCode);
            ps.setString(5, stmt);
            ps.setString(6, "dg-" + queryId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<List<Map<String, Object>>> verify() {
        return StarRocksCluster.executeScript(etl, script("05_verify.sql"));
    }

    private static void assertScenarioResults(List<List<Map<String, Object>>> v) {
        assertNull(v.get(0).get(0).get("pending_batch_id"), "V1: no pending batch");
        List<String> usage = v.get(1).stream().map(r -> r.get("table_name") + "." + r.get("column_name") + " "
                + num(r.get("access")) + "/" + num(r.get("sel")) + "/" + num(r.get("wh")) + "/" + num(r.get("jn"))
                + "/" + num(r.get("gb")) + "/" + num(r.get("ob")) + " uv=" + num(r.get("uv")) + " hll="
                + num(r.get("uv_approx"))).collect(Collectors.toList());
        assertEquals(Arrays.asList(
                "orders.amount 2/2/2/0/0/0 uv=1 hll=1",
                "orders.order_id 1/1/0/0/0/0 uv=1 hll=1",
                "orders.user_id 1/0/1/0/0/0 uv=1 hll=1",
                "users.city 2/1/1/0/1/1 uv=1 hll=1",
                "users.user_id 1/0/1/0/0/0 uv=1 hll=1"), usage, "V2 (docs section 6, expected usage)");
        assertEquals(Arrays.asList("lineage_smoke_user"), v.get(2).stream().map(r -> r.get("user_name"))
                .collect(Collectors.toList()), "V3");
        List<String> lineage = v.get(3).stream().map(r -> r.get("target_table") + "." + r.get("target_column")
                + " <- " + r.get("source_table") + "." + r.get("source_column") + " " + r.get("lineage_type") + "/"
                + r.get("transform_type") + " " + r.get("stmt_type") + " x" + num(r.get("exec_count")) + " "
                + r.get("last_exec_user")).collect(Collectors.toList());
        assertEquals(Arrays.asList(
                "user_gmv.gmv <- orders.amount DIRECT/AGGREGATION INSERT_INTO x1 lineage_smoke_user",
                "user_gmv.gmv <- orders.user_id INDIRECT/IDENTITY INSERT_INTO x1 lineage_smoke_user",
                "user_gmv.uid <- orders.user_id DIRECT/IDENTITY INSERT_INTO x1 lineage_smoke_user",
                "user_gmv.uid <- orders.user_id INDIRECT/IDENTITY INSERT_INTO x1 lineage_smoke_user",
                "v_orders.amt2 <- orders.amount DIRECT/EXPRESSION CREATE_VIEW x1 lineage_smoke_user",
                "v_orders.amt2 <- orders.dt INDIRECT/IDENTITY CREATE_VIEW x1 lineage_smoke_user",
                "v_orders.order_id <- orders.order_id DIRECT/IDENTITY CREATE_VIEW x1 lineage_smoke_user",
                "v_orders.order_id <- orders.dt INDIRECT/IDENTITY CREATE_VIEW x1 lineage_smoke_user"), lineage,
                "V4 (docs section 6, expected lineage)");
    }

    /** Runs a smoke jshell script exactly as the runbook says, and returns its SMOKE_REPORT / SMOKE_ANALYZE line. */
    private static Map<String, String> runJshell(String scriptName, String cycles) throws Exception {
        String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Path jshell = Paths.get(System.getProperty("java.home"), "bin", "jshell");
        ProcessBuilder pb = new ProcessBuilder(jshell.toString(), "-J-Duser.timezone=UTC", "-R-Duser.timezone=UTC",
                "--class-path", classPath, SMOKE.resolve("smoke-common.jsh").toString(),
                SMOKE.resolve(scriptName).toString());
        Map<String, String> env = pb.environment();
        env.put("SR_FE_HOST", sr.host());
        env.put("SR_QUERY_PORT", String.valueOf(sr.queryPort()));
        env.put("SR_HTTP_PORT", String.valueOf(sr.feHttpPort()));
        env.put("SR_BE_REWRITE", "127.0.0.1:8040=" + sr.host() + ":" + sr.beHttpPort());
        env.put("SR_USER", "lineage_etl");
        env.put("SR_PASSWORD", ETL_PASSWORD);
        env.put("SMOKE_CYCLES", cycles);
        env.put("SMOKE_LATE_ARRIVAL_SECONDS", "1");
        env.put("SMOKE_LOOKBACK_HOURS", "1");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out;
        try (InputStream in = p.getInputStream()) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] b = new byte[8192];
            int n;
            while ((n = in.read(b)) > 0) {
                buf.write(b, 0, n);
            }
            out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        }
        assertTrue(p.waitFor(5, TimeUnit.MINUTES), out);
        assertEquals(0, p.exitValue(), scriptName + " failed:\n" + out);
        assertTrue(out.contains("SMOKE_CLOCK"), out);
        Map<String, String> fields = new HashMap<>();
        List<String> lines = new ArrayList<>(Arrays.asList(out.split("\n")));
        for (String line : lines) {
            if (line.startsWith("SMOKE_REPORT") || line.startsWith("SMOKE_ANALYZE")) {
                fields.clear();
                fields.put("line", line);
                Matcher m = Pattern.compile("(\\w+)=(\"[^\"]*\"|\\S+)").matcher(line);
                while (m.find()) {
                    fields.put(m.group(1), m.group(2).replace("\"", ""));
                }
            }
        }
        assertTrue(fields.containsKey("line"), "no report line in:\n" + out);
        fields.put("out", out);
        return fields;
    }
}
