package com.starrocks.lineage.cli;

import com.starrocks.lineage.pipeline.PipelineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliConfigTest {

    private static Map<String, String> env(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void defaultsGiveTheProductionPipeline() {
        CliConfig c = CliConfig.load(env("SR_FE_HOST", "fe1"), null);
        assertEquals("audit_lineage", c.pipeline());
        assertEquals("audit_lineage_db", c.lineageDb());
        assertFalse(c.owner().isEmpty(), "owner defaults to the host name");
        assertEquals(URI.create("http://fe1:8030"), c.feHttpUrl());
        LocalDateTime now = LocalDateTime.of(2026, 11, 10, 9, 7, 30);
        PipelineConfig p = c.pipelineConfig(now);
        assertEquals(LocalDateTime.of(2026, 11, 9, 9, 5), p.getInitialStart(), "24 h back, slice aligned");
        assertEquals(Duration.ofMinutes(5), p.getSliceSize());
        assertEquals(Duration.ofMinutes(5), p.getLateArrival());
        assertEquals(Duration.ofHours(48), p.getLabelRetention());
        assertEquals(Duration.ofMinutes(10), p.getLeaseTtl());
        assertEquals(Duration.ofSeconds(60), c.interval());
    }

    @Test
    void environmentOverridesTheFileWhichOverridesDefaults(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("lineage.properties");
        Files.write(file, ("SR_FE_HOST=fe-from-file\nSR_PASSWORD=secret\nPIPELINE=from_file\nSLICE_SECONDS=600\n"
                + "OWNER=node-a\n").getBytes(StandardCharsets.UTF_8));
        CliConfig c = CliConfig.load(env("PIPELINE", "from_env"), file);
        assertEquals("fe-from-file", c.get("SR_FE_HOST"));
        assertEquals("from_env", c.pipeline());
        assertEquals("node-a", c.owner());
        assertEquals(Duration.ofMinutes(10), c.pipelineConfig(LocalDateTime.now()).getSliceSize());
        assertEquals("******", c.describe().get("SR_PASSWORD"), "the password is never printed");
        assertFalse(c.toString().contains("secret"));
    }

    @Test
    void invalidConfigurationIsRejectedWithItsKey(@TempDir Path dir) throws Exception {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> CliConfig.load(Collections.emptyMap(), null))
                .getMessage().contains("SR_FE_HOST"));
        Path typo = dir.resolve("typo.properties");
        Files.write(typo, "SR_FE_HOST=x\nSR_PASWORD=y\n".getBytes(StandardCharsets.UTF_8));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> CliConfig.load(Collections.emptyMap(), typo))
                .getMessage().contains("SR_PASWORD"), "a misspelt key is an error, not silently ignored");
        assertThrows(IllegalArgumentException.class, () -> CliConfig.load(env("SR_FE_HOST", "x",
                "SLICE_SECONDS", "abc"), null));
        assertThrows(IllegalArgumentException.class, () -> CliConfig.load(env("SR_FE_HOST", "x", "THREADS", "0"),
                null));
        assertThrows(IllegalArgumentException.class, () -> CliConfig.load(env("SR_FE_HOST", "x",
                "LINEAGE_DB", "x; drop"), null));
        assertThrows(IllegalArgumentException.class, () -> CliConfig.load(env("SR_FE_HOST", "x",
                "SR_BE_REWRITE", "nonsense"), null));
        assertThrows(IllegalArgumentException.class, () -> CliConfig.load(env("SR_FE_HOST", "x",
                "SLICE_SECONDS", "420"), null).pipelineConfig(LocalDateTime.now()), "must divide the hour");
        assertThrows(IllegalArgumentException.class, () -> CliConfig.load(Collections.emptyMap(),
                dir.resolve("missing.properties")));
    }

    @Test
    void beAddressesAreRewritten() {
        CliConfig c = CliConfig.load(env("SR_FE_HOST", "fe", "SR_BE_REWRITE",
                "10.0.0.1:8040=be1.example.com:18040, 10.0.0.2:8040=be2.example.com:18040"), null);
        assertEquals(URI.create("http://be1.example.com:18040/api/db/t/_stream_load"),
                c.beRewrite().apply(URI.create("http://10.0.0.1:8040/api/db/t/_stream_load")));
        assertEquals(URI.create("http://be2.example.com:18040/x"),
                c.beRewrite().apply(URI.create("http://10.0.0.2:8040/x")));
        assertEquals(URI.create("http://10.0.0.3:8040/x"), c.beRewrite().apply(URI.create("http://10.0.0.3:8040/x")));
    }
}
