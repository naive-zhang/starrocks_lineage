package com.starrocks.lineage.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcCheckpointStoreTest {

    private static final Duration TTL = Duration.ofMinutes(10);
    private static final LocalDateTime START = LocalDateTime.of(2024, 6, 17, 0, 0);
    private static final LocalDateTime NOW = LocalDateTime.of(2024, 6, 18, 12, 0);

    private H2Checkpoints h2;
    private MutableClock clock;
    private JdbcCheckpointStore store;

    @BeforeEach
    void setUp() throws Exception {
        h2 = new H2Checkpoints();
        clock = new MutableClock(NOW);
        store = h2.store(clock);
    }

    @AfterEach
    void tearDown() throws Exception {
        h2.close();
    }

    @Test
    void firstLeaseCreatesTheCheckpointAtTheInitialStart() {
        assertFalse(store.load("p").isPresent());
        assertTrue(store.tryAcquireLease("p", "a", TTL, START));
        CheckpointStore.Checkpoint cp = store.load("p").get();
        assertEquals(START, cp.getCommittedUntil());
        assertEquals(START, cp.getCoverageStart());
        assertEquals("a", cp.getOwner());
        assertEquals(NOW.plus(TTL), cp.getLeaseExpire());
        assertFalse(cp.getPending().isPresent());
        assertFalse(cp.getLastReconcile().isPresent());
        assertTrue(store.tryAcquireLease("p", "a", TTL, START.minusDays(5)), "renewal");
        assertEquals(START, store.load("p").get().getCoverageStart(), "initial start only applies once");
        assertEquals(cp.getEpoch(), store.load("p").get().getEpoch(), "the epoch is created once");
        assertEquals(36, cp.getEpoch().length());
    }

    @Test
    void pendingBatchStepsFailuresAndCommit() {
        store.tryAcquireLease("p", "a", TTL, START);
        CheckpointStore.PendingBatch b = CheckpointStore.PendingBatch.declare("b1", BatchKind.SLICE, START,
                START.plusMinutes(5), NOW);
        store.savePending("p", "a", TTL, b);
        assertEquals(b, store.load("p").get().getPending().get());
        assertThrows(PipelineException.class, () -> store.savePending("p", "a", TTL,
                CheckpointStore.PendingBatch.declare("b2", BatchKind.SLICE, START, START, NOW)),
                "one pending batch at a time");

        store.advanceStep("p", "a", TTL, "b1", BatchStep.PREPARE);
        store.advanceStep("p", "a", TTL, "b1", BatchStep.USAGE);
        store.advanceStep("p", "a", TTL, "b1", BatchStep.DICTIONARY);
        assertEquals(BatchStep.USAGE, store.load("p").get().getPending().get().getStep(), "steps never go back");
        assertThrows(PipelineException.class, () -> store.advanceStep("p", "a", TTL, "other", BatchStep.REFRESH));

        store.recordFailure("p", "a", TTL, "b1", "boom");
        store.recordFailure("p", "a", TTL, "b1", new String(new char[5000]).replace('\0', 'x'));
        CheckpointStore.PendingBatch failed = store.load("p").get().getPending().get();
        assertEquals(2, failed.getAttempts());
        assertEquals(1000, failed.getLastError().get().length());

        clock.advance(Duration.ofHours(1));
        store.restartPendingClock("p", "a", TTL, "b1");
        assertEquals(NOW.plusHours(1), store.load("p").get().getPending().get().getSince());

        store.commit("p", "a", TTL, "b1", START.plusMinutes(5), 42);
        CheckpointStore.Checkpoint cp = store.load("p").get();
        assertEquals(START.plusMinutes(5), cp.getCommittedUntil());
        assertFalse(cp.getPending().isPresent());
        assertEquals(42, cp.getProcessedRows());

        store.savePending("p", "a", TTL, CheckpointStore.PendingBatch.declare("late", BatchKind.LATE, START,
                START.plusMinutes(5), NOW));
        store.commit("p", "a", TTL, "late", null, 3);
        assertEquals(START.plusMinutes(5), store.load("p").get().getCommittedUntil(), "LATE does not move it");
        assertEquals(45, store.load("p").get().getProcessedRows());

        store.savePending("p", "a", TTL, CheckpointStore.PendingBatch.declare("back", BatchKind.SLICE, START,
                START, NOW));
        assertThrows(PipelineException.class, () -> store.commit("p", "a", TTL, "back", START, 0),
                "the watermark never moves backwards");
        store.markReconciled("p", "a", TTL, NOW);
        assertEquals(NOW, store.load("p").get().getLastReconcile().get());
    }

    /** The hours of a rebuilt day: commit one and declare the next in a single write. */
    @Test
    void commitAndDeclareChainsBatchesAtomically() {
        store.tryAcquireLease("p", "a", TTL, START);
        store.savePending("p", "a", TTL, CheckpointStore.PendingBatch.declare("h0", BatchKind.REBUILD, START,
                START.plusHours(1), NOW));
        store.advanceStep("p", "a", TTL, "h0", BatchStep.REFRESH);
        CheckpointStore.PendingBatch next = CheckpointStore.PendingBatch.declare("h1", BatchKind.REBUILD,
                START.plusHours(1), START.plusHours(2), NOW);

        store.commitAndDeclare("p", "a", TTL, "h0", 0, next);

        CheckpointStore.Checkpoint cp = store.load("p").get();
        assertEquals(next, cp.getPending().get());
        assertEquals(START, cp.getCommittedUntil(), "a rebuild does not move the watermark");
        assertThrows(PipelineException.class, () -> store.commitAndDeclare("p", "a", TTL, "h0", 0, next),
                "h0 is no longer pending");
        store.commit("p", "a", TTL, "h1", null, 0);
        assertFalse(store.load("p").get().getPending().isPresent());
        assertThrows(LeaseLostException.class, () -> store.commitAndDeclare("p", "b", TTL, "h1", 0, next));
    }

    /** Gap 3: fencing - a paused owner cannot write after a take-over, and a released lease is free at once. */
    @Test
    void leaseFencingTakeoverAndRelease() {
        store.tryAcquireLease("p", "a", TTL, START);
        store.savePending("p", "a", TTL, CheckpointStore.PendingBatch.declare("b1", BatchKind.SLICE, START,
                START.plusMinutes(5), NOW));
        JdbcCheckpointStore b = h2.store(clock);
        assertFalse(b.tryAcquireLease("p", "b", TTL, START), "lease still alive");
        assertThrows(LeaseLostException.class, () -> b.verifyLease("p", "b", TTL));

        clock.advance(TTL.plusSeconds(1));
        assertTrue(b.tryAcquireLease("p", "b", TTL, START), "expired lease is taken over");
        assertEquals("b1", b.load("p").get().getPending().get().getBatchId(), "the new owner inherits the batch");

        LeaseLostException lost = assertThrows(LeaseLostException.class,
                () -> store.verifyLease("p", "a", TTL));
        assertFalse(lost.isRetryable());
        assertThrows(LeaseLostException.class, () -> store.advanceStep("p", "a", TTL, "b1", BatchStep.PREPARE));
        assertThrows(LeaseLostException.class, () -> store.commit("p", "a", TTL, "b1", START.plusMinutes(5), 1));
        assertEquals(BatchStep.INTENT, b.load("p").get().getPending().get().getStep(), "zombie wrote nothing");

        b.releaseLease("p", "b");
        assertTrue(store.tryAcquireLease("p", "a", TTL, START), "released lease is free immediately");
        store.releaseLease("p", "someone-else");
        assertEquals("a", store.load("p").get().getOwner(), "only the owner can release");
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcCheckpointStore(h2.dataSource, "x; drop", Clock.systemUTC(), "INSERT"));
    }
}
