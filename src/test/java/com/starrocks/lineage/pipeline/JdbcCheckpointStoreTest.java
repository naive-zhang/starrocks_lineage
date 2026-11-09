package com.starrocks.lineage.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcCheckpointStoreTest {

    private static final String TABLE = "audit_lineage_db.audit_lineage_sync_checkpoint";
    private FakeStarRocks sr;

    @BeforeEach
    void setUp() throws Exception {
        sr = new FakeStarRocks();
    }

    @AfterEach
    void tearDown() throws Exception {
        sr.close();
    }

    private JdbcCheckpointStore at(String instant) {
        return new JdbcCheckpointStore(sr.dataSource, TABLE, Clock.fixed(Instant.parse(instant), ZoneId.of("UTC")),
                "MERGE");
    }

    @Test
    void leaseLifecycleIntentAndTakeoverAfterExpiry() {
        Watermark start = Watermark.startingAt(LocalDateTime.of(2024, 6, 1, 0, 0));
        JdbcCheckpointStore a = at("2024-06-18T12:00:00Z");
        assertFalse(a.load("p").isPresent());
        assertTrue(a.tryAcquireLease("p", "node-a", Duration.ofMinutes(10), start));
        assertEquals(start, a.load("p").get().getWatermark());

        CheckpointStore.PendingBatch pending = new CheckpointStore.PendingBatch("b1",
                new Watermark(LocalDateTime.of(2024, 6, 18, 11, 0), "q9"));
        a.savePending("p", "node-a", Duration.ofMinutes(10), pending);
        assertEquals(pending, a.load("p").get().getPending().get());

        JdbcCheckpointStore bSoon = at("2024-06-18T12:05:00Z");
        assertFalse(bSoon.tryAcquireLease("p", "node-b", Duration.ofMinutes(10), start), "lease still alive");
        assertThrows(PipelineException.class, () -> bSoon.commit("p", "node-b", Duration.ofMinutes(10),
                pending.getEnd(), 1));

        JdbcCheckpointStore bLater = at("2024-06-18T12:11:00Z");
        assertTrue(bLater.tryAcquireLease("p", "node-b", Duration.ofMinutes(10), start), "expired lease taken over");
        CheckpointStore.Checkpoint taken = bLater.load("p").get();
        assertEquals(pending, taken.getPending().get(), "the new owner inherits the intent and replays it");
        bLater.commit("p", "node-b", Duration.ofMinutes(10), pending.getEnd(), 42);
        CheckpointStore.Checkpoint committed = bLater.load("p").get();
        assertEquals(pending.getEnd(), committed.getWatermark());
        assertFalse(committed.getPending().isPresent());
        assertEquals(42, committed.getProcessedRows());
        assertFalse(at("2024-06-18T12:12:00Z").tryAcquireLease("p", "node-a", Duration.ofMinutes(10), start));
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcCheckpointStore(sr.dataSource, "x; drop", Clock.systemUTC(), "INSERT"));
    }
}
