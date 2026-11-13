package com.starrocks.lineage.cli;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditLineageCliTest {

    private static String[] out = new String[1];

    private static int run(Map<String, String> env, String... args) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int code = AuditLineageCli.run(args, env, new PrintStream(buf, true));
        out[0] = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        return code;
    }

    @Test
    void helpAndUsageErrors() {
        assertEquals(AuditLineageCli.OK, run(Collections.emptyMap(), "--help"));
        assertTrue(out[0].contains("check") && out[0].contains("SR_FE_HOST (required)"), out[0]);
        assertEquals(AuditLineageCli.USAGE, run(Collections.emptyMap()));
        assertEquals(AuditLineageCli.USAGE, run(Collections.emptyMap(), "check"));
        assertTrue(out[0].startsWith("CONFIG ERROR SR_FE_HOST"), out[0]);

        Map<String, String> env = new HashMap<>();
        env.put("SR_FE_HOST", "127.0.0.1");
        env.put("SR_QUERY_PORT", "1");
        assertEquals(AuditLineageCli.USAGE, run(env, "deploy"));
        assertTrue(out[0].contains("unknown command 'deploy'"), out[0]);
        assertEquals(AuditLineageCli.USAGE, run(env, "--config", "/no/such/file", "check"));
        assertEquals(AuditLineageCli.USAGE, run(env, "--threads", "0", "rebuild", "2026-11-01", "2026-11-02"));
        assertTrue(out[0].contains("THREADS must be > 0"), "--threads overrides THREADS: " + out[0]);
        assertEquals(AuditLineageCli.USAGE, run(env, "rebuild", "2026-11-01", "2026-11-02", "--threads=abc"));
        assertTrue(out[0].contains("THREADS is not a number"), out[0]);
    }

    @Test
    void timesAcceptDaysAndSeconds() {
        assertEquals(LocalDateTime.of(2026, 11, 9, 0, 0), AuditLineageCli.time("2026-11-09"));
        assertEquals(LocalDateTime.of(2026, 11, 9, 8, 5, 3), AuditLineageCli.time("2026-11-09 08:05:03"));
        assertEquals(LocalDateTime.of(2026, 11, 9, 8, 5), AuditLineageCli.time("2026-11-09 08:05"));
        assertThrows(IllegalArgumentException.class, () -> AuditLineageCli.time("yesterday"));
    }

    @Test
    void preflightCoversEveryTableOfTheDdl() {
        assertEquals(9, Preflight.TARGET_COLUMNS.size());
        assertTrue(Preflight.TARGET_COLUMNS.get("audit_lineage_sync_checkpoint").contains("pipeline_epoch"),
                "a checkpoint table of the old DDL fails the check");
        assertTrue(Preflight.TARGET_COLUMNS.get("audit_column_hourly_usage").contains("user_bitmap"));
        assertEquals(19, Preflight.AUDIT_COLUMNS.size());
    }
}
