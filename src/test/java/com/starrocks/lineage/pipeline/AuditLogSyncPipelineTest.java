package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.metadata.MetadataAccessException;
import com.starrocks.lineage.metadata.MetadataProvider;
import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.TableIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AuditLogSyncPipeline} against {@link FakeWarehouse} (audit table + targets with StarRocks semantics and
 * label de-duplication) and {@link JdbcCheckpointStore} on H2. One nested class per Stage 5 gap.
 */
class AuditLogSyncPipelineTest {

    static final String PIPELINE = "audit_lineage";
    static final LocalDateTime YESTERDAY = LocalDateTime.of(2024, 6, 17, 0, 0);
    static final LocalDateTime TODAY = YESTERDAY.plusDays(1);
    static final LocalDateTime NOW = TODAY.plusHours(12);
    static final String QUERY = "SELECT amount FROM sales.orders WHERE user_id = 1";
    static final String INSERT = "INSERT INTO dw.user_gmv SELECT user_id, SUM(amount) FROM sales.orders "
            + "GROUP BY user_id";

    FakeWarehouse wh;
    H2Checkpoints h2;
    MutableClock clock;
    JdbcCheckpointStore store;
    FlakyMetadata metadata;
    final List<AuditLogSyncPipeline> pipelines = new ArrayList<>();
    int ids;

    @BeforeEach
    void setUp() throws Exception {
        wh = new FakeWarehouse(PIPELINE);
        h2 = new H2Checkpoints();
        clock = new MutableClock(NOW);
        store = h2.store(clock);
        metadata = new FlakyMetadata(new InMemoryMetadataProvider()
                .register("default_catalog.sales.orders", "order_id", "user_id", "amount", "dt")
                .register("default_catalog.sales.flaky", "f")
                .register("default_catalog.dw.user_gmv", "user_id", "gmv"));
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AuditLogSyncPipeline p : pipelines) {
            p.close();
        }
        h2.close();
    }

    PipelineConfig.Builder config(String owner) {
        return PipelineConfig.builder(PIPELINE, owner).initialStart(YESTERDAY).analyzerThreads(4).pageSize(2)
                .maxRecordsPerBatch(1000).alertAfterAttempts(2);
    }

    AuditLogSyncPipeline pipeline(PipelineConfig cfg) {
        AuditLogSyncPipeline p = new AuditLogSyncPipeline(cfg, wh, store, wh, new StarRocksSqlAnalyzer(metadata),
                metadata, clock);
        pipelines.add(p);
        return p;
    }

    AuditLogSyncPipeline pipeline() {
        return pipeline(config("node-a").build());
    }

    AuditLogRecord audit(LocalDateTime ts, String user, String sql) {
        AuditLogRecord r = AuditLogRecord.builder().queryId(String.format("q%04d", ++ids)).timestamp(ts).user(user)
                .state("EOF").errorCode("").catalog("default_catalog").db("sales").queryTime(10).scanBytes(100)
                .digest("d").isQuery(sql.trim().toUpperCase().startsWith("SELECT")).stmt(sql).build();
        wh.addAudit(r);
        return r;
    }

    /** Runs cycles until one does not commit; returns the committed reports. */
    static List<CycleReport> drain(AuditLogSyncPipeline p) {
        List<CycleReport> committed = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            CycleReport r = p.runOnce();
            if (r.getOutcome() != CycleReport.Outcome.COMMITTED) {
                return committed;
            }
            committed.add(r);
        }
        throw new AssertionError("pipeline does not settle");
    }

    CheckpointStore.Checkpoint checkpoint() {
        return store.load(PIPELINE).get();
    }

    static LocalDateTime hour(LocalDateTime t) {
        return t.withMinute(0).withSecond(0).withNano(0);
    }

    // ================================================================================================ gap 1

    @Nested
    class Gap1FirstRunAcrossDays {

        @Test
        void firstRunStartingYesterdayProcessesEveryDayInClosedSlices() {
            AuditLogRecord y1 = audit(YESTERDAY.plusHours(10).plusSeconds(5), "alice", QUERY);
            audit(TODAY.minusSeconds(1), "bob", QUERY);
            audit(TODAY, "carol", QUERY);
            audit(NOW.minusMinutes(6), "dave", QUERY);
            AuditLogRecord open = audit(NOW.minusMinutes(4), "erin", QUERY);

            List<CycleReport> batches = drain(pipeline());

            assertEquals(4, batches.size(), "runs of empty slices end at the first slice with records");
            assertTrue(batches.stream().allMatch(b -> b.getKind().get() == BatchKind.SLICE));
            assertEquals(YESTERDAY, batches.get(0).getRangeStart().get());
            assertEquals(YESTERDAY.plusHours(10).plusMinutes(5), batches.get(0).getRangeEnd().get());
            assertEquals(1, wh.accessCount(hour(y1.getTimestamp()), "amount"), "yesterday is counted");
            assertEquals(1, wh.accessCount(hour(TODAY.minusSeconds(1)), "amount"));
            assertEquals(1, wh.accessCount(TODAY, "amount"));
            assertEquals(NOW.minusMinutes(5), checkpoint().getCommittedUntil(),
                    "watermark = last closed slice boundary (now - lateArrival)");
            assertEquals(1, wh.accessCount(hour(open.getTimestamp()), "amount"),
                    "the open slice is not processed yet: only dave's query counts in that hour");
            assertEquals(4, checkpoint().getProcessedRows());
            assertEquals(4, wh.members.size());
        }

        @Test
        void quietPeriodsAreCommittedTogetherAndSlicesAreHourAligned() {
            PipelineConfig cfg = config("node-a").initialStart(YESTERDAY.plusMinutes(7)).maxEmptySlicesPerBatch(12)
                    .build();
            assertEquals(YESTERDAY.plusMinutes(5), cfg.getInitialStart(), "aligned down to a slice boundary");
            List<CycleReport> batches = drain(pipeline(cfg));
            assertTrue(batches.stream().allMatch(b -> b.getRecords() == 0));
            assertEquals(Duration.ofHours(1), Duration.between(batches.get(0).getRangeStart().get(),
                    batches.get(0).getRangeEnd().get()), "12 empty slices per checkpoint write");
            assertEquals(NOW.minusMinutes(5), checkpoint().getCommittedUntil());
            assertThrows(IllegalArgumentException.class,
                    () -> config("x").sliceSize(Duration.ofMinutes(7)).build(), "must divide the hour");
        }
    }

    // ================================================================================================ gap 2

    @Nested
    class Gap2PoisonRecordsAndBatches {

        @Test
        void badRecordsBecomeDeadLettersAndTheBatchCommits() {
            AuditLogRecord good = audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            AuditLogRecord unparsable = audit(YESTERDAY.plusMinutes(2), "bob", "SELEC amount FROM");
            AuditLogRecord crashing = audit(YESTERDAY.plusMinutes(3), "carol", "SELECT f FROM sales.flaky");
            audit(YESTERDAY.plusMinutes(4), "dave", "SELECT dt FROM sales.orders");
            metadata.fail("flaky", new IllegalStateException("analyzer bug"));
            wh.rejectUsage = r -> r.getColumn().getColumn().equals("dt");

            CycleReport r = pipeline().runOnce();

            assertEquals(CycleReport.Outcome.COMMITTED, r.getOutcome());
            assertEquals(2, r.getDeadLetters());
            assertEquals(1, r.getFilteredRows());
            assertEquals(1, r.statusCount(ParseStatus.PARSE_ERROR));
            assertEquals(1, r.statusCount(ParseStatus.ANALYSIS_ERROR));
            DeadLetter parse = wh.letter(unparsable.getQueryId(), DeadLetter.Kind.PARSE_ERROR);
            assertEquals(DeadLetter.Status.OPEN, parse.getStatus());
            assertEquals("SELEC amount FROM", parse.getRecord().getStmt(), "the full record is kept for repair");
            assertEquals("bob", parse.getRecord().getUser());
            assertTrue(wh.letter(crashing.getQueryId(), DeadLetter.Kind.ANALYSIS_ERROR).getErrorMessage()
                    .contains("internal analyzer error"));
            assertEquals(1, wh.letters(DeadLetter.Kind.LOAD_FILTERED).size(), "rejected rows are recorded");
            assertEquals(1, wh.accessCount(hour(good.getTimestamp()), "amount"), "good rows are kept");
            assertEquals(0, wh.accessCount(hour(good.getTimestamp()), "dt"));
            BatchLogEntry log = wh.batchLog.get(r.getBatchId().get());
            assertEquals(4, log.getRecords());
            assertEquals(2, log.getFailed());
            assertEquals(1, log.getFilteredRows());
        }

        @Test
        void nonRetryableFailureBlocksAndIsCountedThenRecovers() {
            audit(YESTERDAY.plusMinutes(1), "alice", INSERT);
            wh.failBefore.put("lineage", 1);
            wh.failNonRetryable.put("lineage", true);
            AuditLogSyncPipeline p = pipeline();

            CycleReport blocked = p.runOnce();
            assertEquals(CycleReport.Outcome.BLOCKED, blocked.getOutcome());
            assertTrue(blocked.getMessage().get().contains("injected failure before lineage"));
            assertTrue(p.getMetrics().isBlocked());
            CheckpointStore.PendingBatch pending = checkpoint().getPending().get();
            assertEquals(1, pending.getAttempts());
            assertEquals(BatchStep.USAGE, pending.getStep());
            assertTrue(pending.getLastError().get().startsWith("[non-retryable]"));

            CycleReport ok = p.runOnce();
            assertEquals(CycleReport.Outcome.COMMITTED, ok.getOutcome());
            assertTrue(ok.isReplay());
            assertFalse(p.getMetrics().isBlocked());
            assertEquals(0, p.getMetrics().getConsecutiveFailures());
            assertEquals(1, wh.lineage.values().stream().filter(l -> l.target.getColumn().equals("gmv"))
                    .findFirst().get().execCount);
        }

        @Test
        void permanentlyBlockedBatchIsSkippedByTheOperatorAndNotReprocessed() {
            AuditLogRecord write = audit(YESTERDAY.plusMinutes(1), "alice", INSERT);
            audit(YESTERDAY.plusMinutes(2), "bob", QUERY);
            wh.failBefore.put("lineage", 100);
            wh.failNonRetryable.put("lineage", true);
            AuditLogSyncPipeline p = pipeline();
            assertEquals(CycleReport.Outcome.BLOCKED, p.runOnce().getOutcome());
            assertEquals(CycleReport.Outcome.BLOCKED, p.runOnce().getOutcome());
            assertEquals(2, checkpoint().getPending().get().getAttempts(), "alert threshold reached");
            String batchId = checkpoint().getPending().get().getBatchId();

            CycleReport skipped = p.skipPendingBatch("lineage stage rejects batch, ticket OPS-1");
            assertEquals(CycleReport.Outcome.COMMITTED, skipped.getOutcome());
            assertFalse(checkpoint().getPending().isPresent());
            assertEquals(YESTERDAY.plusMinutes(5), checkpoint().getCommittedUntil());
            DeadLetter letter = wh.letter(batchId, DeadLetter.Kind.BATCH_SKIPPED);
            assertTrue(letter.getErrorMessage().contains("OPS-1"));
            assertTrue(letter.getDetail().contains("step=USAGE"));
            assertEquals(1, p.getMetrics().getSkippedBatches());

            wh.failBefore.clear();
            clock.advance(Duration.ofMinutes(20));
            drain(p);
            assertEquals(batchId, wh.members.get(write.getQueryId() + "@" + write.getTimestamp()));
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"), "usage of the skipped batch kept once");
            assertTrue(wh.batchLog.values().stream().noneMatch(b -> b.getKind() == BatchKind.LATE),
                    "reconciliation does not pick the skipped records up again");
            assertThrows(IllegalStateException.class, () -> p.skipPendingBatch("nothing pending"));
        }

        @Test
        void metadataOutageAbortsTheBatchAndIsRetried() {
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            metadata.fail("orders", new MetadataAccessException("FE down", null));
            AuditLogSyncPipeline p = pipeline();
            PipelineException e = assertThrows(PipelineException.class, p::runOnce);
            assertTrue(e.isRetryable());
            assertEquals(0, wh.ops("usage"));
            assertEquals(1, p.getMetrics().getConsecutiveFailures());
            metadata.heal();
            assertEquals(CycleReport.Outcome.COMMITTED, p.runOnce().getOutcome());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
        }
    }

    // ================================================================================================ gap 3

    @Nested
    class Gap3Fencing {

        @Test
        void pausedInstanceIsFencedAndTheTakeOverWritesExactlyOnce() {
            audit(YESTERDAY.plusMinutes(1), "alice", INSERT);
            audit(YESTERDAY.plusMinutes(2), "bob", QUERY);
            AuditLogSyncPipeline a = pipeline(config("node-a").build());
            AuditLogSyncPipeline b = pipeline(config("node-b").build());
            AtomicReference<CycleReport> byB = new AtomicReference<>();
            // node-a pauses (GC, network partition) right before its lineage load; its lease expires and node-b
            // takes the batch over and finishes it
            wh.beforeOp.put("lineage", () -> {
                clock.advance(Duration.ofMinutes(11));
                byB.set(b.runOnce());
            });

            LeaseLostException fenced = assertThrows(LeaseLostException.class, a::runOnce);
            assertTrue(fenced.getMessage().contains("node-b"));
            assertEquals(CycleReport.Outcome.COMMITTED, byB.get().getOutcome());
            assertTrue(byB.get().isReplay(), "node-b resumed node-a's declared batch");
            assertEquals("node-b", checkpoint().getOwner());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
            assertEquals(1, wh.lineage.values().stream().filter(l -> l.target.getColumn().equals("gmv"))
                    .findFirst().get().execCount, "node-a's late stage write is an idempotent upsert and it "
                    + "is fenced before its daily insert");
            assertEquals(CycleReport.Outcome.LEASE_NOT_ACQUIRED, a.runOnce().getOutcome());
        }

        @Test
        void closeReleasesTheLeaseForAStandby() {
            AuditLogSyncPipeline a = pipeline(config("node-a").build());
            AuditLogSyncPipeline b = pipeline(config("node-b").build());
            a.runOnce();
            assertEquals(CycleReport.Outcome.LEASE_NOT_ACQUIRED, b.runOnce().getOutcome());
            a.close();
            assertNotEquals(CycleReport.Outcome.LEASE_NOT_ACQUIRED, b.runOnce().getOutcome());
        }
    }

    // ================================================================================================ gap 4

    @Nested
    class Gap4LabelRetention {

        @Test
        void crashAfterUsageInsideRetentionIsSkippedByLabelState() {
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            wh.crashAfter.put("usage", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            assertEquals(BatchStep.DICTIONARY, checkpoint().getPending().get().getStep());
            assertEquals(CycleReport.Outcome.COMMITTED, p.runOnce().getOutcome());
            assertEquals(1, wh.ops("usage"), "label COMMITTED: the load is not repeated");
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
        }

        @Test
        void crashAfterUsageBeyondRetentionBlocksInsteadOfDoubleCounting() {
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            wh.crashAfter.put("usage", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            wh.expireLabels();
            clock.advance(Duration.ofDays(3));

            CycleReport blocked = p.runOnce();
            assertEquals(CycleReport.Outcome.BLOCKED, blocked.getOutcome());
            assertTrue(blocked.getMessage().get().contains("label retention"));
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"), "nothing added while blocked");

            p.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.MARK_DONE);
            assertEquals(CycleReport.Outcome.COMMITTED, p.runOnce().getOutcome());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
        }

        @Test
        void failureBeforeUsageBeyondRetentionIsRedoneByTheOperator() {
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            wh.failBefore.put("usage", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            wh.expireLabels();
            clock.advance(Duration.ofDays(3));
            assertEquals(CycleReport.Outcome.BLOCKED, p.runOnce().getOutcome());
            assertThrows(IllegalArgumentException.class, () -> AuditLogSyncPipeline.StepResolution.valueOf("x"));
            p.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.REDO);
            assertEquals(CycleReport.Outcome.COMMITTED, p.runOnce().getOutcome());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
        }

        @Test
        void loadStillInFlightIsRetriedLaterAndLaterStepsAreNotGuarded() {
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            audit(YESTERDAY.plusMinutes(2), "bob", INSERT);
            wh.crashAfter.put("usage", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            String batch = checkpoint().getPending().get().getBatchId();
            wh.labelStates.put(batch + "_usage", StreamLoadClient.LoadState.IN_PROGRESS);
            PipelineException inFlight = assertThrows(PipelineException.class, p::runOnce);
            assertTrue(inFlight.isRetryable());
            assertTrue(inFlight.getMessage().contains("in flight"));
            wh.labelStates.clear();
            assertEquals(CycleReport.Outcome.COMMITTED, p.runOnce().getOutcome());
            assertEquals(1, wh.ops("usage"), "the committed usage label is not loaded again");
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
            assertEquals(1, wh.ops("daily"));
        }

        @Test
        void crashBetweenLineageChunksIsClearedAndRewrittenOnResume() {
            audit(YESTERDAY.plusMinutes(1), "alice", INSERT);
            wh.crashAfter.put("lineage", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            String batch = checkpoint().getPending().get().getBatchId();
            assertTrue(wh.batchLineage.values().stream().anyMatch(c -> c.batchId.equals(batch)),
                    "a part of the stage was loaded before the crash");
            // the stage of an earlier attempt may differ from what the resume derives (e.g. a chunk of an edge the
            // fixed metadata no longer produces): it must not survive
            LineageRow stale = wh.batchLineage.values().iterator().next().row;
            wh.batchLineage.put("stale", new FakeWarehouse.LineageRowCell(batch, stale, false));

            assertEquals(CycleReport.Outcome.COMMITTED, p.runOnce().getOutcome());
            assertEquals(1, wh.ops("clear_lineage"));
            assertFalse(wh.batchLineage.containsKey("stale"));
            assertEquals(1, wh.lineage.values().stream().filter(l -> l.target.getColumn().equals("gmv"))
                    .findFirst().get().execCount, "every edge counted once");
            assertEquals(1, wh.ops("daily"));
        }

        @Test
        void resolveRefusesWhenNotBlockedOnALoad() {
            AuditLogSyncPipeline p = pipeline();
            assertThrows(IllegalStateException.class,
                    () -> p.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.REDO));
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            wh.failBefore.put("members", 1);
            assertThrows(PipelineException.class, p::runOnce);
            assertThrows(IllegalStateException.class,
                    () -> p.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.MARK_DONE));
        }
    }

    // ================================================================================================ gap 5

    @Nested
    class Gap5DeterministicReplay {

        @Test
        void aResumedBatchProcessesExactlyItsMembers() {
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            wh.crashAfter.put("members", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            AuditLogRecord late = audit(YESTERDAY.plusMinutes(2), "bob", QUERY);

            CycleReport resumed = p.runOnce();
            assertEquals(1, resumed.getRecords(), "the record that arrived after PREPARE is not in the batch");
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
            assertFalse(wh.members.containsKey(late.getQueryId() + "@" + late.getTimestamp()));
        }

        /** Found on the real engine: a reset pipeline must not reproduce labels StarRocks still remembers. */
        @Test
        void aPipelineResetUnderTheSameNameGetsNewBatchIdsAndLabels() throws Exception {
            audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
            AuditLogSyncPipeline first = pipeline();
            String firstId = first.runOnce().getBatchId().get();
            String epoch = checkpoint().getEpoch();
            first.close();
            // operator reset: checkpoint deleted, tables emptied; the FE still knows the labels (3 days)
            try (java.sql.Connection c = h2.dataSource.getConnection(); java.sql.Statement st = c.createStatement()) {
                st.execute("DELETE FROM " + H2Checkpoints.TABLE);
            }
            wh.usage.clear();
            wh.members.clear();

            CycleReport again = pipeline().runOnce();
            assertNotEquals(epoch, checkpoint().getEpoch());
            assertNotEquals(firstId, again.getBatchId().get(), "same pipeline, same slice, new generation");
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"), "the loads were not dropped as duplicates");
            assertEquals(1, wh.members.size());
        }

        @Test
        void recordsDeadLetteredByAnEarlierAttemptAreNotAnalysedAgain() {
            AuditLogRecord flaky = audit(YESTERDAY.plusMinutes(1), "alice", "SELECT f FROM sales.flaky");
            audit(YESTERDAY.plusMinutes(2), "bob", QUERY);
            metadata.fail("flaky", new IllegalStateException("transient analyzer state"));
            wh.crashAfter.put("dict", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            metadata.heal();

            CycleReport resumed = p.runOnce();
            assertEquals(CycleReport.Outcome.COMMITTED, resumed.getOutcome());
            assertEquals(0, wh.accessCount(hour(YESTERDAY), "f"),
                    "attempt 2 would succeed, but the record is already a dead letter: no double contribution");
            assertEquals(DeadLetter.Status.OPEN,
                    wh.letter(flaky.getQueryId(), DeadLetter.Kind.ANALYSIS_ERROR).getStatus());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
        }

        @Test
        void aQueryFailingOnlyOnReplayAfterUsageIsNotDeadLettered() {
            AuditLogRecord q = audit(YESTERDAY.plusMinutes(1), "alice", "SELECT f FROM sales.flaky");
            wh.crashAfter.put("usage", 1);
            AuditLogSyncPipeline p = pipeline();
            assertThrows(PipelineException.class, p::runOnce);
            metadata.fail("flaky", new IllegalStateException("regression"));
            assertEquals(CycleReport.Outcome.COMMITTED, p.runOnce().getOutcome());
            assertNull(wh.letter(q.getQueryId(), DeadLetter.Kind.ANALYSIS_ERROR),
                    "its usage is already loaded: a dead letter would be repaired into a double count");
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "f"));
        }
    }

    // ================================================================================================ gap 6

    @Nested
    class Gap6LateArrivals {

        @Test
        void reconciliationProcessesRecordsThatArrivedAfterTheirSliceWasCommitted() {
            audit(TODAY.plusHours(1), "alice", QUERY);
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            LocalDateTime committed = checkpoint().getCommittedUntil();
            AuditLogRecord late = audit(TODAY.plusHours(1).plusMinutes(1), "bob", QUERY);
            audit(committed.minusHours(25), "zed", QUERY);

            clock.advance(Duration.ofMinutes(16));
            List<CycleReport> batches = drain(p);
            CycleReport lateBatch = batches.stream().filter(b -> b.getKind().get() == BatchKind.LATE).findFirst()
                    .orElseThrow(AssertionError::new);
            assertEquals(1, lateBatch.getRecords(), "the record older than the reconcile window is not taken");
            assertEquals(2, wh.accessCount(hour(late.getTimestamp()), "amount"));
            assertEquals(2, wh.users(hour(late.getTimestamp()), "amount").size());
            assertEquals(lateBatch.getBatchId().get(), wh.members.get(late.getQueryId() + "@" + late.getTimestamp()));
            assertEquals(1, p.getMetrics().getLateRecords());
            assertTrue(checkpoint().getLastReconcile().isPresent(), "the following reconciliation found nothing");

            clock.advance(Duration.ofMinutes(16));
            drain(p);
            assertEquals(1, wh.batchLog.values().stream().filter(b -> b.getKind() == BatchKind.LATE).count(),
                    "no new LATE batch");
            assertEquals(2, wh.accessCount(hour(late.getTimestamp()), "amount"));
        }
    }

    // ================================================================================================ gap 7

    @Nested
    class Gap7RepairAndRebuild {

        @Test
        void deadLettersAreRepairedAfterTheFix() {
            AuditLogRecord a = audit(YESTERDAY.plusMinutes(1), "alice", "SELECT f FROM sales.flaky");
            AuditLogRecord b = audit(YESTERDAY.plusMinutes(2), "bob", "SELEC broken");
            metadata.fail("flaky", new IllegalStateException("analyzer bug"));
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            assertEquals(2, wh.deadLetters.size());
            metadata.heal();

            CycleReport repair = p.repairDeadLetters(YESTERDAY, TODAY, 100);
            assertTrue(metadata.invalidated.contains("*"), "a repair never uses cached schemas");
            assertEquals(BatchKind.REPAIR, repair.getKind().get());
            assertEquals(2, repair.getRecords());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "f"));
            assertEquals(DeadLetter.Status.RESOLVED,
                    wh.letter(a.getQueryId(), DeadLetter.Kind.ANALYSIS_ERROR).getStatus());
            DeadLetter stillBroken = wh.letter(b.getQueryId(), DeadLetter.Kind.PARSE_ERROR);
            assertEquals(DeadLetter.Status.OPEN, stillBroken.getStatus(), "fails again: back to OPEN");
            assertNull(stillBroken.getRepairBatchId());

            assertEquals(CycleReport.Outcome.IDLE, p.repairDeadLetters(TODAY, NOW, 100).getOutcome());
            CycleReport again = p.repairDeadLetters(YESTERDAY, TODAY, 100);
            assertEquals(1, again.getRecords());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "f"), "a resolved record is never repaired twice");
        }

        @Test
        void repairInterruptedAfterReservationResumes() {
            AuditLogRecord a = audit(YESTERDAY.plusMinutes(1), "alice", "SELECT f FROM sales.flaky");
            metadata.fail("flaky", new IllegalStateException("analyzer bug"));
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            metadata.heal();
            wh.crashAfter.put("usage", 1);
            assertThrows(PipelineException.class, () -> p.repairDeadLetters(YESTERDAY, TODAY, 100));
            assertEquals(DeadLetter.Status.REPAIRING,
                    wh.letter(a.getQueryId(), DeadLetter.Kind.ANALYSIS_ERROR).getStatus());
            assertThrows(IllegalStateException.class, () -> p.repairDeadLetters(YESTERDAY, TODAY, 100),
                    "one batch at a time");
            CycleReport resumed = p.runOnce();
            assertEquals(BatchKind.REPAIR, resumed.getKind().get());
            assertEquals(1, wh.accessCount(hour(YESTERDAY), "f"));
            assertEquals(DeadLetter.Status.RESOLVED,
                    wh.letter(a.getQueryId(), DeadLetter.Kind.ANALYSIS_ERROR).getStatus());
        }

        @Test
        void rebuildRecomputesDaysExactlyAndSurvivesACrash() {
            audit(YESTERDAY.plusHours(1), "alice", QUERY);
            audit(YESTERDAY.plusHours(2), "bob", QUERY);
            audit(YESTERDAY.plusHours(3), "alice", INSERT);
            audit(TODAY.plusHours(1), "carol", INSERT);
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            Map<String, String> usage = wh.usageSnapshot();
            Map<String, String> lineage = wh.lineageSnapshot();
            assertTrue(lineage.values().stream().anyMatch(v -> v.contains("count=2")), "two days of facts");

            // corruption a rebuild has to repair, e.g. counts of an analyzer bug
            wh.usage.get(hour(YESTERDAY.plusHours(1)) + "|amount").access += 40;
            wh.crashAfter.put("purge_usage", 1);
            assertThrows(PipelineException.class, () -> p.rebuild(YESTERDAY, TODAY));
            assertEquals(YESTERDAY, checkpoint().getPending().get().getRangeStart());
            // run / the daemon resume the pending hour and complete its day (the chain of hours)
            List<CycleReport> resumed = drain(p);
            assertEquals(24, resumed.stream().filter(r -> r.getKind().orElse(null) == BatchKind.REBUILD).count());
            assertEquals(3, resumed.stream().filter(r -> r.getKind().orElse(null) == BatchKind.REBUILD)
                    .mapToInt(CycleReport::getRecords).sum());
            assertEquals(usage, wh.usageSnapshot());
            assertEquals(lineage, wh.lineageSnapshot(), "lineage recomputed from both days of facts");

            CycleReport twice = p.rebuild(YESTERDAY, TODAY);
            assertEquals(CycleReport.Outcome.COMMITTED, twice.getOutcome());
            assertEquals(YESTERDAY.plusHours(23), twice.getRangeStart().get(), "the last hour of the day");
            assertEquals(usage, wh.usageSnapshot(), "a rebuild is repeatable");
            assertEquals(lineage, wh.lineageSnapshot());
            assertFalse(checkpoint().getPending().isPresent());
        }

        @Test
        void rebuildRunsOneBatchPerHourAndReportsEachHour() {
            audit(YESTERDAY.plusHours(1), "alice", QUERY);
            audit(YESTERDAY.plusHours(3), "alice", INSERT);
            audit(TODAY.plusHours(1), "carol", INSERT);
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            clock.advance(Duration.ofDays(1));
            drain(p);
            Map<String, String> usage = wh.usageSnapshot();
            Map<String, String> lineage = wh.lineageSnapshot();
            int purges = wh.ops("purge_usage");
            List<String> hours = new ArrayList<>();

            CycleReport last = p.rebuild(YESTERDAY, TODAY.plusDays(1), (r, done, total) -> {
                assertEquals(48, total);
                assertEquals(hours.size() + 1, done);
                hours.add(r.getRangeStart().get() + "/" + r.getRangeEnd().get());
            });

            assertEquals(48, hours.size());
            assertEquals(YESTERDAY + "/" + YESTERDAY.plusHours(1), hours.get(0));
            assertEquals(TODAY.plusHours(23) + "/" + TODAY.plusDays(1), hours.get(47));
            assertEquals(TODAY.plusHours(23), last.getRangeStart().get());
            assertEquals(purges + 2, wh.ops("purge_usage"), "one purge per day, by its first hour");
            assertEquals(usage, wh.usageSnapshot());
            assertEquals(lineage, wh.lineageSnapshot());
            assertTrue(wh.batchLog.values().stream().filter(b -> b.getKind() == BatchKind.REBUILD)
                    .allMatch(b -> Duration.between(b.getRangeStart(), b.getRangeEnd()).equals(Duration.ofHours(1))));
        }

        @Test
        void interruptedRebuildResumesFromTheInterruptedHour() {
            audit(YESTERDAY.plusHours(1), "alice", QUERY);
            audit(YESTERDAY.plusHours(5), "bob", QUERY);
            audit(YESTERDAY.plusHours(7), "alice", INSERT);
            audit(TODAY.plusHours(1), "carol", INSERT);
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            clock.advance(Duration.ofDays(1));
            drain(p);
            Map<String, String> usage = wh.usageSnapshot();
            Map<String, String> lineage = wh.lineageSnapshot();
            // hour 01 writes its usage, hour 05 crashes right after its usage load committed
            wh.beforeOp.put("usage", () -> wh.beforeOp.put("usage", () -> wh.crashAfter.put("usage", 1)));
            assertThrows(PipelineException.class, () -> p.rebuild(YESTERDAY, TODAY.plusDays(1)));
            CheckpointStore.PendingBatch interrupted = checkpoint().getPending().get();
            assertEquals(BatchKind.REBUILD, interrupted.getKind());
            assertEquals(YESTERDAY.plusHours(5), interrupted.getRangeStart());
            assertEquals(BatchStep.DICTIONARY, interrupted.getStep());
            int hours = (int) wh.batchLog.values().stream().filter(b -> b.getKind() == BatchKind.REBUILD).count();
            int purges = wh.ops("purge_usage");

            List<String> resumed = new ArrayList<>();
            p.rebuild(YESTERDAY, TODAY.plusDays(1), (r, done, total) -> {
                assertEquals(43, total, "19 hours left of the first day, then the second day");
                resumed.add(r.getRangeStart().get().toString());
            });

            assertEquals(YESTERDAY.plusHours(5).toString(), resumed.get(0), "the interrupted hour first");
            assertEquals(43, resumed.size());
            assertEquals(purges + 1, wh.ops("purge_usage"), "the first day is not purged again");
            assertEquals(5, hours, "hours 00..04 were committed before the crash");
            assertEquals(usage, wh.usageSnapshot(), "the usage label of the interrupted hour is not loaded twice");
            assertEquals(lineage, wh.lineageSnapshot());
        }

        @Test
        void rebuildOutsideTheInterruptedDayCompletesThatDayFirst() {
            audit(YESTERDAY.plusHours(1), "alice", QUERY);
            audit(YESTERDAY.plusHours(5), "alice", INSERT);
            audit(TODAY.plusHours(1), "carol", QUERY);
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            clock.advance(Duration.ofDays(1));
            drain(p);
            Map<String, String> usage = wh.usageSnapshot();
            Map<String, String> lineage = wh.lineageSnapshot();
            wh.crashAfter.put("lineage", 1);
            assertThrows(PipelineException.class, () -> p.rebuild(YESTERDAY, TODAY));
            assertEquals(YESTERDAY.plusHours(5), checkpoint().getPending().get().getRangeStart());

            List<LocalDateTime> starts = new ArrayList<>();
            p.rebuild(TODAY, TODAY.plusDays(1), (r, done, total) -> starts.add(r.getRangeStart().get()));

            assertEquals(19 + 24, starts.size(), "the rest of the interrupted day, then the requested one");
            assertEquals(YESTERDAY.plusHours(5), starts.get(0));
            assertEquals(TODAY, starts.get(19));
            assertEquals(usage, wh.usageSnapshot());
            assertEquals(lineage, wh.lineageSnapshot());
        }

        @Test
        void rebuildReadsWithSeveralThreadsAndReportsProgress() {
            for (int i = 0; i < 40; i++) {
                audit(YESTERDAY.plusHours(2).plusMinutes(i), "u" + (i % 3), i % 4 == 0 ? INSERT : QUERY);
            }
            AuditLogSyncPipeline single = pipeline();
            drain(single);
            Map<String, String> usage = wh.usageSnapshot();
            Map<String, String> lineage = wh.lineageSnapshot();
            single.close();

            AuditLogSyncPipeline p = pipeline(config("node-a").readThreads(4).progressInterval(Duration.ofNanos(1))
                    .build());
            List<AuditLogSyncPipeline.Progress> progress = new java.util.concurrent.CopyOnWriteArrayList<>();
            p.setProgressListener(progress::add);
            p.rebuild(YESTERDAY, TODAY);

            assertEquals(usage, wh.usageSnapshot(), "parallel readers analyse every record exactly once");
            assertEquals(lineage, wh.lineageSnapshot());
            List<AuditLogSyncPipeline.Progress> hour2 = progress.stream()
                    .filter(x -> x.getBatch().getRangeStart().equals(YESTERDAY.plusHours(2)))
                    .collect(Collectors.toList());
            AuditLogSyncPipeline.Progress done = hour2.get(hour2.size() - 1);
            assertTrue(done.isFinished());
            assertEquals(40, done.getAnalysed());
            assertEquals(40, done.getTotal());
            assertTrue(done.toString().startsWith("PROGRESS REBUILD [" + YESTERDAY.plusHours(2)));
            assertTrue(done.toString().contains("analysed=40/40 (100.0%)"));
            assertTrue(hour2.size() > 1, "intermediate progress was reported");
        }

        @Test
        void aLongRebuildHourRenewsItsLeaseAndStopsWhenItIsLost() {
            for (int i = 0; i < 20; i++) {
                audit(YESTERDAY.plusHours(2).plusMinutes(i), "u", QUERY);
            }
            AuditLogSyncPipeline p = pipeline(config("node-a").progressInterval(Duration.ofNanos(1)).build());
            drain(p);
            AuditLogSyncPipeline standby = pipeline(config("node-b").build());
            // the analysis of hour 02 outlives the lease TTL; the standby can only take over when node-a stopped
            // renewing
            p.setProgressListener(progress -> {
                if (progress.getBatch().getRangeStart().equals(YESTERDAY.plusHours(2)) && !progress.isFinished()) {
                    clock.advance(Duration.ofMinutes(6));
                    assertEquals(CycleReport.Outcome.LEASE_NOT_ACQUIRED, standby.runOnce().getOutcome());
                }
            });
            p.rebuild(YESTERDAY, TODAY);
            assertEquals("node-a", checkpoint().getOwner());

            p.setProgressListener(progress -> {
                if (progress.getBatch().getRangeStart().equals(YESTERDAY.plusHours(2)) && !progress.isFinished()) {
                    clock.advance(Duration.ofMinutes(11));
                    standby.runOnce();
                }
            });
            assertThrows(LeaseLostException.class, () -> p.rebuild(YESTERDAY, TODAY));
            assertEquals("node-b", checkpoint().getOwner());
        }

        @Test
        void readerFailureFailsTheHourRetryably() {
            audit(YESTERDAY.plusHours(1), "alice", QUERY);
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            Map<String, String> usage = wh.usageSnapshot();
            wh.failFetchProcessed = 1;
            PipelineException e = assertThrows(PipelineException.class, () -> p.rebuild(YESTERDAY, TODAY));
            assertTrue(e.isRetryable());
            assertEquals(YESTERDAY, checkpoint().getPending().get().getRangeStart());
            p.rebuild(YESTERDAY, TODAY);
            assertEquals(usage, wh.usageSnapshot());
        }

        @Test
        void rebuildAfterAnAnalyzerChangeReplacesEdgesAndResolvesDeadLetters() {
            AuditLogRecord flaky = audit(YESTERDAY.plusHours(1), "alice", "SELECT f FROM sales.flaky");
            audit(YESTERDAY.plusHours(2), "bob", INSERT);
            metadata.fail("flaky", new IllegalStateException("analyzer bug"));
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            assertTrue(wh.lineage.values().stream().anyMatch(l -> l.target.getColumn().equals("gmv")));

            metadata.heal();
            metadata.delegate.register("default_catalog.dw.user_gmv", "uid", "total");
            p.rebuild(YESTERDAY, TODAY);

            assertTrue(wh.lineage.values().stream().noneMatch(l -> l.target.getColumn().equals("gmv")),
                    "the edge without facts any more is deleted");
            assertTrue(wh.lineage.values().stream().anyMatch(l -> l.target.getColumn().equals("total")));
            assertEquals(DeadLetter.Status.RESOLVED,
                    wh.letter(flaky.getQueryId(), DeadLetter.Kind.ANALYSIS_ERROR).getStatus());
            assertEquals(1, wh.accessCount(hour(YESTERDAY.plusHours(1)), "f"));
        }

        @Test
        void rebuildRefusesWhatItCannotRecompute() {
            audit(YESTERDAY.plusHours(1), "alice", QUERY);
            AuditLogSyncPipeline p = pipeline();
            drain(p);
            assertThrows(IllegalArgumentException.class, () -> p.rebuild(TODAY, TODAY.plusDays(1)),
                    "today is not committed yet");
            assertThrows(IllegalArgumentException.class, () -> p.rebuild(YESTERDAY.plusHours(1), TODAY));
            wh.deleteAudit(r -> true);
            IllegalStateException gone = assertThrows(IllegalStateException.class,
                    () -> p.rebuild(YESTERDAY, TODAY));
            assertTrue(gone.getMessage().contains("audit retention"));
            assertFalse(checkpoint().getPending().isPresent());
        }
    }

    // ================================================================================================ misc

    @Test
    void createdObjectsAreInvalidatedInTheMetadataCache() {
        audit(YESTERDAY.plusMinutes(1), "alice", "CREATE TABLE dw.t2 AS SELECT user_id FROM sales.orders");
        pipeline().runOnce();
        assertTrue(metadata.invalidated.contains("default_catalog.dw.t2"));
    }

    @Test
    void scheduledRunnerCatchesUp() throws Exception {
        audit(YESTERDAY.plusMinutes(1), "alice", QUERY);
        AuditLogSyncPipeline p = pipeline();
        p.start(Duration.ofMillis(50));
        long deadline = System.currentTimeMillis() + 10_000;
        while (store.load(PIPELINE).map(c -> c.getCommittedUntil().isBefore(NOW.minusMinutes(5))).orElse(true)
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(NOW.minusMinutes(5), checkpoint().getCommittedUntil());
        assertThrows(IllegalStateException.class, () -> p.start(Duration.ofSeconds(1)));
        assertEquals(1, wh.accessCount(hour(YESTERDAY), "amount"));
    }

    @Test
    void configValidation() {
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.builder("", "o").build());
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.builder("p", "o").pageSize(10)
                .maxRecordsPerBatch(5).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.builder("p", "o")
                .labelRetention(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.builder("p", "o")
                .lateArrival(Duration.ofSeconds(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.builder("p", "o")
                .alertAfterAttempts(0).build());
        PipelineConfig d = PipelineConfig.builder("p", "o").build();
        assertEquals(Duration.ofMinutes(5), d.getSliceSize());
        assertEquals(Duration.ofDays(2), d.getLabelRetention());
        assertEquals(d.getInitialStart(), d.getInitialStart().toLocalDate().atStartOfDay(),
                "default start: yesterday 00:00");
        assertNotNull(d.getReconcileWindow());
    }

    /**
     * Metadata whose answers for one table can be broken on demand.
     */
    static final class FlakyMetadata implements MetadataProvider {
        final InMemoryMetadataProvider delegate;
        final List<String> invalidated = new ArrayList<>();
        volatile String brokenTable;
        volatile RuntimeException failure;

        FlakyMetadata(InMemoryMetadataProvider delegate) {
            this.delegate = delegate;
        }

        void fail(String table, RuntimeException e) {
            brokenTable = table;
            failure = e;
        }

        void heal() {
            brokenTable = null;
        }

        @Override
        public Optional<TableSchema> getTableSchema(TableIdentifier table) {
            String broken = brokenTable;
            if (broken != null && table.getTable().equalsIgnoreCase(broken)) {
                throw failure;
            }
            return delegate.getTableSchema(table);
        }

        @Override
        public synchronized void invalidate(TableIdentifier table) {
            invalidated.add(table.toString());
        }

        @Override
        public synchronized void invalidateAll() {
            invalidated.add("*");
        }
    }
}
