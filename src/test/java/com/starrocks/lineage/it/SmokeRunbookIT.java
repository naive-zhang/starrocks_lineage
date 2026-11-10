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
import java.time.ZoneOffset;
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
    private static final int LATE_ARRIVAL_SECONDS = 2;
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

        // ---- step 5: first run on an empty audit window: the hour of look-back is committed as empty slices
        Run first = runJshell("smoke-run-cycle.jsh");
        assertEquals("IDLE", first.summary.get("last_outcome"), first.out);
        assertEquals("0", first.summary.get("records"));

        // ---- step 6: scenario as lineage_smoke_user, audit rows like the audit loader would write them; then the
        //      cycles once the slice of the scenario has closed
        LocalDateTime lastRecord = runScenario();
        LocalDateTime closes = lastRecord.withSecond(0).plusMinutes(1).plusSeconds(LATE_ARRIVAL_SECONDS + 1);
        StarRocksCluster.sleep(Math.max(0, java.time.Duration.between(LocalDateTime.now(ZoneOffset.UTC), closes)
                .toMillis()));
        Run cycle = runJshell("smoke-run-cycle.jsh");
        Map<String, String> scenario = cycle.withRecords();
        assertEquals("COMMITTED", scenario.get("outcome"), cycle.out);
        assertEquals("SLICE", scenario.get("kind"));
        assertEquals("8", scenario.get("records"), cycle.out);
        assertEquals("false", scenario.get("replay"));
        assertEquals("0", scenario.get("dead_letters"));
        assertEquals("8", cycle.summary.get("records"), "exactly one batch held the scenario");
        List<List<Map<String, Object>>> v = verify();
        assertScenarioResults(v);
        assertEquals(8, num(v.get(5).get(0).get("records")), "V6");

        // ---- step 7: exactly-once drill
        String drill = script("07_replay_drill.sql")
                .replace("${BATCH_ID}", scenario.get("batch"))
                .replace("${RANGE_START}", scenario.get("range_start"))
                .replace("${RANGE_END}", scenario.get("range_end"));
        List<List<Map<String, Object>>> rewound = StarRocksCluster.executeScript(etl, drill);
        assertEquals(scenario.get("batch"), rewound.get(0).get(0).get("pending_batch_id"));
        assertEquals("INTENT", rewound.get(0).get(0).get("pending_step"));
        Run replay = runJshell("smoke-run-cycle.jsh");
        Map<String, String> replayed = replay.reports.get(0);
        assertEquals("true", replayed.get("replay"), replay.out);
        assertEquals(scenario.get("batch"), replayed.get("batch"));
        assertEquals("8", replayed.get("records"), replay.out);
        List<List<Map<String, Object>>> afterReplay = verify();
        assertEquals(v.subList(1, 5), afterReplay.subList(1, 5),
                "usage, readers, lineage and dead letters unchanged by the replay");

        // ---- step 8: operator actions (status, and a repair that has nothing to repair)
        Run status = runJshell("smoke-ops.jsh");
        assertEquals("-", status.summary.get("pending"), status.out);
        assertTrue(status.summary.get("committed_until").compareTo(scenario.get("range_end")) >= 0, status.out);
        Map<String, String> repairEnv = new HashMap<>();
        repairEnv.put("SMOKE_OP", "repair");
        repairEnv.put("SMOKE_FROM", scenario.get("range_start"));
        repairEnv.put("SMOKE_TO", scenario.get("range_end"));
        Run repair = runJshell("smoke-ops.jsh", repairEnv);
        assertEquals("IDLE", repair.summary.get("outcome"), repair.out);

        // ---- step 4 (read-only analysis) on the scenario window
        Run analyze = runJshell("smoke-analyze.jsh");
        assertTrue(analyze.summary.get("line").contains("SUCCESS=6"), analyze.out);
        assertTrue(analyze.summary.get("line").contains("SKIPPED_FAILED_QUERY=1"), analyze.out);
        assertTrue(analyze.summary.get("line").contains("error_rate_percent=0.0"), analyze.out);

        // ---- step 9: cleanup
        sr.executeScript(script("06_cleanup.sql"));
        assertTrue(sr.query("SHOW DATABASES").stream().noneMatch(r -> r.containsValue("lineage_smoke_db")));
        assertTrue(sr.query("SELECT * FROM audit_lineage_db.audit_lineage_sync_checkpoint "
                + "WHERE pipeline_name = 'audit_lineage_smoke'").isEmpty());
        assertTrue(sr.query("SELECT * FROM audit_lineage_db.audit_lineage_batch_member "
                + "WHERE pipeline_name = 'audit_lineage_smoke'").isEmpty());
    }

    private static void assertManualChecksPartA(List<List<Map<String, Object>>> results) {
        List<Map<String, Object>> rows = results.stream().flatMap(List::stream).collect(Collectors.toList());
        Map<String, Object> cp = rows.stream().filter(r -> r.containsKey("expect_b")).findFirst().get();
        assertEquals(1, num(cp.get("rows_expect_1")));
        assertEquals(LocalDateTime.of(2024, 1, 1, 0, 5), cp.get("expect_0005"));
        assertEquals("b", cp.get("expect_b"));
        assertEquals("USAGE", cp.get("expect_usage"));
        List<Map<String, Object>> refreshes = rows.stream().filter(r -> r.containsKey("transform_type"))
                .collect(Collectors.toList());
        assertEquals(3, refreshes.size(), "3.4 (a), (b), (c)");
        assertEquals(Arrays.asList(2L, 2L, 5L), refreshes.stream().map(r -> num(r.get("exec_count")))
                .collect(Collectors.toList()));
        assertEquals(Arrays.asList("IDENTITY", "IDENTITY", "EXPRESSION"), refreshes.stream()
                .map(r -> r.get("transform_type")).collect(Collectors.toList()));
        assertEquals(Arrays.asList("q-b1", "q-b1", "q-b2"), refreshes.stream()
                .map(r -> r.get("last_query_id")).collect(Collectors.toList()));
        assertEquals(LocalDateTime.of(2024, 1, 1, 10, 0), refreshes.get(2).get("first_seen_time"));
        assertEquals(LocalDateTime.of(2024, 1, 2, 10, 0), refreshes.get(2).get("last_seen_time"));
        assertEquals(1, rows.stream().filter(r -> r.containsKey("rows_expect_1") && !r.containsKey("expect_b"))
                .mapToLong(r -> num(r.get("rows_expect_1"))).sum());
        assertEquals(2, rows.stream().filter(r -> r.containsKey("fact_rows_expect_2"))
                .mapToLong(r -> num(r.get("fact_rows_expect_2"))).sum());
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
        assertTrue(out.contains("\"state\":\"VISIBLE\""), "the pipeline account may ask get_load_state: " + out);
        assertTrue(out.contains("\"state\":\"UNKNOWN\""), out);
    }

    private static int count(String text, String needle) {
        int n = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + 1)) {
            n++;
        }
        return n;
    }

    /**
     * Executes 04_scenario.sql as lineage_smoke_user and writes the audit rows the audit loader would write, all in
     * one minute (one slice of the smoke configuration).
     *
     * @return the audit time of the last statement
     */
    private static LocalDateTime runScenario() throws SQLException {
        List<String> statements = StarRocksCluster.splitScript(script("04_scenario.sql"));
        assertEquals(8, statements.size());
        if (LocalDateTime.now(ZoneOffset.UTC).getSecond() > 45) {
            StarRocksCluster.sleep((61 - LocalDateTime.now(ZoneOffset.UTC).getSecond()) * 1000L);
        }
        LocalDateTime base = LocalDateTime.now(ZoneOffset.UTC).withNano(0);
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
        return base.plusSeconds(statements.size() - 1);
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
        assertNull(v.get(0).get(0).get("last_error"), "V1: no failed attempt");
        assertTrue(v.get(4).isEmpty(), "V5: no dead letters " + v.get(4));
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

    /** Output of one smoke jshell run. */
    private static final class Run {
        final List<Map<String, String>> reports = new ArrayList<>();
        final Map<String, String> summary = new HashMap<>();
        String out;

        /** @return the report of the (single) batch that had records */
        Map<String, String> withRecords() {
            List<Map<String, String>> withRecords = reports.stream().filter(r -> !"0".equals(r.get("records")))
                    .collect(Collectors.toList());
            assertEquals(1, withRecords.size(), out);
            return withRecords.get(0);
        }
    }

    private static Map<String, String> fields(String line) {
        Map<String, String> fields = new HashMap<>();
        fields.put("line", line);
        Matcher m = Pattern.compile("(\\w+)=(\"[^\"]*\"|\\S+)").matcher(line);
        while (m.find()) {
            fields.put(m.group(1), m.group(2).replace("\"", ""));
        }
        return fields;
    }

    /** Runs a smoke jshell script exactly as the runbook says and collects its SMOKE_* lines. */
    private static Run runJshell(String scriptName) throws Exception {
        return runJshell(scriptName, new HashMap<>());
    }

    private static Run runJshell(String scriptName, Map<String, String> extraEnv) throws Exception {
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
        env.put("SMOKE_SLICE_SECONDS", "60");
        env.put("SMOKE_LATE_ARRIVAL_SECONDS", String.valueOf(LATE_ARRIVAL_SECONDS));
        env.put("SMOKE_LOOKBACK_HOURS", "1");
        env.putAll(extraEnv);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        Run run = new Run();
        try (InputStream in = p.getInputStream()) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] b = new byte[8192];
            int n;
            while ((n = in.read(b)) > 0) {
                buf.write(b, 0, n);
            }
            run.out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        }
        assertTrue(p.waitFor(5, TimeUnit.MINUTES), run.out);
        assertEquals(0, p.exitValue(), scriptName + " failed:\n" + run.out);
        assertTrue(run.out.contains("SMOKE_CLOCK"), run.out);
        for (String line : run.out.split("\n")) {
            if (line.startsWith("SMOKE_REPORT")) {
                run.reports.add(fields(line));
            } else if (line.startsWith("SMOKE_SUMMARY") || line.startsWith("SMOKE_ANALYZE")
                    || line.startsWith("SMOKE_STATUS") || line.startsWith("SMOKE_OP ")) {
                run.summary.putAll(fields(line));
            }
        }
        assertTrue(run.summary.containsKey("line"), "no summary line in:\n" + run.out);
        return run;
    }
}
