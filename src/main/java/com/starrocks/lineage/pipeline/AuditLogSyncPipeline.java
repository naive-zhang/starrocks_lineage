package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.MetadataAccessException;
import com.starrocks.lineage.metadata.MetadataProvider;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Incremental audit → lineage / usage pipeline (Stage 5 reliability design, {@code docs/stage5-reliability.md}).
 *
 * <p><b>Batches.</b> Work is done in batches of four kinds ({@link BatchKind}). A cycle ({@link #runOnce()}) takes
 * the lease, then either resumes the pending batch, or - when reconciliation is due - declares a LATE batch of
 * audit records of committed time that are members of no batch, or declares the next SLICE: the records of fixed,
 * hour-aligned time slices {@code [start, start + sliceSize)} that are older than {@code lateArrival}.</p>
 *
 * <p><b>Steps.</b> A declared batch is persisted in the checkpoint before anything is written; its steps
 * ({@link BatchStep}) run in order, each one a single atomic write named by a label derived from the batch id, and
 * the checkpoint records each completed step. A resumed batch continues after its last completed step:</p>
 * <ul>
 *   <li>its records come from the membership table (exactly the set of the first attempt), not from a new range
 *       read;</li>
 *   <li>records that already have a dead letter from this batch are not analysed again, so the dead-letter set and
 *       the usage / lineage written by different attempts never overlap;</li>
 *   <li>the additive steps (USAGE, LINEAGE_DAILY) ask the FE for their label state first: committed →
 *       skipped; in flight → retried later; aborted or never seen → executed; unknown after the label retention →
 *       {@link BatchBlockedException} (operator: {@link #resolveBlockedStep}); the lineage stage (a PRIMARY KEY
 *       upsert written in chunks) is cleared and written again;</li>
 *   <li>once LINEAGE_STAGE is complete nothing is analysed any more: the daily facts and the lineage table derive
 *       from the persisted stage.</li>
 * </ul>
 *
 * <p><b>Failures.</b> A record that fails to parse or analyse becomes a dead letter (the batch continues). A
 * metadata outage aborts the batch (retryable). Rows StarRocks rejects are isolated by the sink. Any other failure is
 * counted on the pending batch; a non-retryable one reports {@link CycleReport.Outcome#BLOCKED} and is logged as an
 * alert after {@code alertAfterAttempts}. Nothing is ever skipped automatically ({@link #skipPendingBatch}).</p>
 *
 * <p><b>Fencing.</b> The lease is verified (and renewed) before every write step and every step completion is
 * written under the owner check, so an instance that was paused past its lease stops at its next step. Batch ids
 * are deterministic, so a write that slipped through the remaining window reuses the label of the same step of the
 * same batch and StarRocks drops it.</p>
 */
public final class AuditLogSyncPipeline implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AuditLogSyncPipeline.class);
    private static final Set<StatementType> SCHEMA_CHANGING = EnumSet.of(StatementType.CREATE_TABLE_AS_SELECT,
            StatementType.CREATE_VIEW, StatementType.ALTER_VIEW, StatementType.CREATE_MATERIALIZED_VIEW);
    private static final Set<DeadLetter.Kind> REPAIRABLE = EnumSet.of(DeadLetter.Kind.PARSE_ERROR,
            DeadLetter.Kind.ANALYSIS_ERROR);
    /**
     * Additive steps whose replay must ask the FE for the label state. LINEAGE_STAGE is not one: it is a PRIMARY KEY
     * upsert loaded in chunks, and a resumed batch clears it before writing it again.
     */
    private static final Set<BatchStep> GUARDED_STEPS = EnumSet.of(BatchStep.USAGE, BatchStep.LINEAGE_DAILY);
    private static final int DEAD_LETTER_CHUNK = 1_000;

    /**
     * Operator decision for a batch blocked on a load whose outcome the FE no longer knows.
     */
    public enum StepResolution {
        /** The data of the step is present (checked by the operator): continue after it. */
        MARK_DONE,
        /** The data of the step is absent: execute it again. */
        REDO
    }

    private final PipelineConfig config;
    private final AuditLogSource source;
    private final CheckpointStore checkpoints;
    private final AuditLineageSink sink;
    private final StarRocksSqlAnalyzer analyzer;
    private final MetadataProvider metadata;
    private final Clock clock;
    private final ExecutorService analysisPool;
    private final PipelineMetrics metrics = new PipelineMetrics();
    private volatile Consumer<Progress> progressListener = p -> { };
    private final Object cycleLock = new Object();
    private ScheduledExecutorService scheduler;

    public AuditLogSyncPipeline(PipelineConfig config, AuditLogSource source, CheckpointStore checkpoints,
                                AuditLineageSink sink, StarRocksSqlAnalyzer analyzer, MetadataProvider metadata,
                                Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.source = Objects.requireNonNull(source, "source");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.analyzer = Objects.requireNonNull(analyzer, "analyzer");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.analysisPool = Executors.newFixedThreadPool(config.getAnalyzerThreads(),
                namedThreads("audit-analyzer"));
    }

    public PipelineMetrics getMetrics() {
        return metrics;
    }

    /** Receives the periodic progress of long batches (a rebuild hour) in addition to the log line. */
    public void setProgressListener(Consumer<Progress> listener) {
        this.progressListener = Objects.requireNonNull(listener, "listener");
    }

    /**
     * Receives every batch a {@link #rebuild} commits.
     */
    @FunctionalInterface
    public interface RebuildListener {
        /**
         * @param report     the committed (or BLOCKED) hour
         * @param hoursDone  hours of this rebuild call completed so far
         * @param hoursTotal hours this rebuild call has to process
         */
        void hourDone(CycleReport report, int hoursDone, int hoursTotal);
    }

    // ============================================================================================ cycle

    /**
     * Runs one cycle. Cycles and operations never overlap (also when called concurrently with the scheduler).
     *
     * @return what happened; {@link CycleReport.Outcome#BLOCKED} when the pending batch needs an operator
     * @throws PipelineException on a retryable failure (the pending batch is resumed by the next cycle) or when the
     *                           lease was lost ({@link LeaseLostException})
     */
    public CycleReport runOnce() {
        synchronized (cycleLock) {
            try {
                CycleReport report = doRunOnce();
                if (report.getOutcome() == CycleReport.Outcome.IDLE) {
                    metrics.recordIdle();
                }
                return report;
            } catch (RuntimeException e) {
                metrics.recordFailure(false);
                throw e instanceof PipelineException ? e : new PipelineException("cycle failed: " + e, e);
            }
        }
    }

    private CycleReport doRunOnce() {
        if (!acquireLease()) {
            return CycleReport.leaseNotAcquired();
        }
        CheckpointStore.Checkpoint cp = checkpoint();
        if (cp.getPending().isPresent()) {
            return execute(cp.getPending().get(), true, Declared.NONE);
        }
        LocalDateTime now = now();
        if (reconcileDue(cp, now)) {
            Optional<Declared> late = declareLate(cp, now);
            if (late.isPresent()) {
                return execute(late.get().batch, false, late.get());
            }
            checkpoints.markReconciled(name(), owner(), ttl(), now);
        }
        return nextSlice(cp, now);
    }

    private boolean reconcileDue(CheckpointStore.Checkpoint cp, LocalDateTime now) {
        if (!cp.getCommittedUntil().isAfter(cp.getCoverageStart())) {
            return false;
        }
        return cp.getLastReconcile().map(t -> !now.isBefore(t.plus(config.getReconcileInterval()))).orElse(true);
    }

    /** Reconciliation: committed audit time whose records are members of no batch (they arrived late). */
    private Optional<Declared> declareLate(CheckpointStore.Checkpoint cp, LocalDateTime now) {
        LocalDateTime to = cp.getCommittedUntil();
        LocalDateTime from = max(cp.getCoverageStart(), to.minus(config.getReconcileWindow()));
        if (!from.isBefore(to)) {
            return Optional.empty();
        }
        List<AuditLogRecord> late = source.fetchUnprocessed(name(), from, to, config.getMaxRecordsPerBatch());
        if (late.isEmpty()) {
            return Optional.empty();
        }
        List<String> ids = late.stream().map(r -> r.getQueryId() + '@' + r.getTimestamp()).sorted()
                .collect(Collectors.toList());
        CheckpointStore.PendingBatch batch = CheckpointStore.PendingBatch.declare(
                batchId(name(), BatchKind.LATE.name(), nonce(), String.join(",", ids)), BatchKind.LATE, from, to,
                now);
        checkpoints.savePending(name(), owner(), ttl(), batch);
        LOG.info("reconciliation found {} late records in [{}, {}): batch {}", late.size(), from, to,
                batch.getBatchId());
        return Optional.of(new Declared(batch, late, null));
    }

    /**
     * The next slice. Consecutive empty slices are taken together (one checkpoint write for a quiet night); the run
     * stops at the first slice with records, which ends the batch.
     */
    private CycleReport nextSlice(CheckpointStore.Checkpoint cp, LocalDateTime now) {
        LocalDateTime start = cp.getCommittedUntil();
        LocalDateTime closed = now.minus(config.getLateArrival());
        LocalDateTime end = start;
        List<AuditLogRecord> records = Collections.emptyList();
        int slices = 0;
        while (slices < config.getMaxEmptySlicesPerBatch()) {
            LocalDateTime next = end.plus(config.getSliceSize());
            if (next.isAfter(closed)) {
                break;
            }
            records = readRange(end, next);
            end = next;
            slices++;
            if (!records.isEmpty()) {
                break;
            }
        }
        if (slices == 0) {
            return CycleReport.idle("slice starting at " + start + " is not closed before " + closed);
        }
        CheckpointStore.PendingBatch batch = CheckpointStore.PendingBatch.declare(
                batchId(name(), cp.getEpoch(), BatchKind.SLICE.name(), start.toString(), end.toString()),
                BatchKind.SLICE, start,
                end, now);
        checkpoints.savePending(name(), owner(), ttl(), batch);
        if (records.isEmpty()) {
            checkpoints.commit(name(), owner(), ttl(), batch.getBatchId(), end, 0);
            metrics.recordCommit(BatchKind.SLICE, 0, 0, 0, 0, 0, false);
            return CycleReport.committed(batch, 0, Collections.emptyMap(), 0, 0, 0, 0, false);
        }
        if (records.size() > config.getMaxRecordsPerBatch()) {
            LOG.warn("slice [{}, {}) holds {} records (> maxRecordsPerBatch {}): consider a smaller sliceSize",
                    end.minus(config.getSliceSize()), end, records.size(), config.getMaxRecordsPerBatch());
        }
        return execute(batch, false, new Declared(batch, records, null));
    }

    // ============================================================================================ operations

    /**
     * Operator: gives up the pending batch. A BATCH_SKIPPED dead letter records it; a SLICE advances the watermark.
     * The records of a SLICE / LATE batch become members (so reconciliation does not pick them up again) and can be
     * recomputed later with {@link #rebuild}. A REPAIR batch returns its dead letters to OPEN.
     */
    public CycleReport skipPendingBatch(String reason) {
        Objects.requireNonNull(reason, "reason");
        synchronized (cycleLock) {
            requireLease();
            CheckpointStore.PendingBatch p = checkpoint().getPending()
                    .orElseThrow(() -> new IllegalStateException("no pending batch"));
            String id = p.getBatchId();
            if ((p.getKind() == BatchKind.SLICE || p.getKind() == BatchKind.LATE)
                    && !p.getStep().isAfter(BatchStep.INTENT)) {
                List<AuditLogRecord> records = readBatchRecords(p);
                if (!records.isEmpty()) {
                    checkpoints.verifyLease(name(), owner(), ttl());
                    sink.writeMembers(id, p.getSince(), records);
                }
            }
            if (p.getKind() == BatchKind.REPAIR && p.getStep().isAfter(BatchStep.INTENT)) {
                List<DeadLetter> reserved = sink.deadLetters(EnumSet.of(DeadLetter.Status.REPAIRING), REPAIRABLE,
                        p.getRangeStart(), p.getRangeEnd(), id, Integer.MAX_VALUE);
                writeLetters(id, "released", reserved.stream()
                        .map(d -> d.with(DeadLetter.Status.OPEN, null, null)).collect(Collectors.toList()));
            }
            DeadLetter skipped = new DeadLetter(id, DeadLetter.Kind.BATCH_SKIPPED, DeadLetter.Status.OPEN, id, null,
                    p.getRangeStart(), reason, p.toString(), null);
            checkpoints.verifyLease(name(), owner(), ttl());
            sink.writeDeadLetters(id, "skipped", Collections.singletonList(skipped));
            checkpoints.commit(name(), owner(), ttl(), id, p.getKind().advancesWatermark() ? p.getRangeEnd() : null,
                    0);
            metrics.recordSkip();
            LOG.warn("batch {} skipped by operator: {}", p, reason);
            if (p.getKind() == BatchKind.REBUILD && !(isDayStart(p.getRangeStart())
                    && !p.getStep().isAfter(BatchStep.INTENT))) {
                // the purge of the day already happened (by this batch or by the first hour of its day)
                LocalDateTime day = p.getRangeStart().truncatedTo(ChronoUnit.DAYS);
                LOG.warn("ALERT the rebuild stopped at {}: usage and daily lineage of [{}, {}) are missing until "
                        + "these days are rebuilt again (rebuild {} {})", p.getRangeStart(), p.getRangeStart(),
                        ceilDay(p.getRangeEnd()), day.toLocalDate(), ceilDay(p.getRangeEnd()).toLocalDate());
            }
            return CycleReport.committed(p, 0, Collections.emptyMap(), 0, 0, 1, 0, true);
        }
    }

    /**
     * Operator: resolves a batch blocked on a step whose load outcome the FE no longer knows (label retention
     * expired). Check the data first (see {@code docs/stage5-reliability.md}).
     */
    public void resolveBlockedStep(StepResolution resolution) {
        Objects.requireNonNull(resolution, "resolution");
        synchronized (cycleLock) {
            requireLease();
            CheckpointStore.PendingBatch p = checkpoint().getPending()
                    .orElseThrow(() -> new IllegalStateException("no pending batch"));
            BatchStep next = BatchStep.values()[p.getStep().ordinal() + 1];
            if (!GUARDED_STEPS.contains(next)) {
                throw new IllegalStateException("batch " + p.getBatchId() + " is not blocked on a load step (next "
                        + "step " + next + ")");
            }
            if (resolution == StepResolution.MARK_DONE) {
                checkpoints.advanceStep(name(), owner(), ttl(), p.getBatchId(), next);
            } else {
                checkpoints.restartPendingClock(name(), owner(), ttl(), p.getBatchId());
            }
            LOG.warn("batch {} step {} resolved by operator: {}", p.getBatchId(), next, resolution);
        }
    }

    /**
     * Operator: re-analyses OPEN parse / analysis dead letters with audit time in {@code [from, to)} (after an
     * analyzer fix or once the missing metadata exists). Records that succeed are added to usage / lineage and their
     * dead letters RESOLVED; the others return to OPEN with the new error.
     */
    public CycleReport repairDeadLetters(LocalDateTime from, LocalDateTime to, int limit) {
        synchronized (cycleLock) {
            requireLease();
            requireNoPending();
            List<DeadLetter> open = sink.deadLetters(EnumSet.of(DeadLetter.Status.OPEN), REPAIRABLE, from, to, null,
                    Math.min(limit, config.getMaxRecordsPerBatch()));
            if (open.isEmpty()) {
                return CycleReport.idle("no open dead letters in [" + from + ", " + to + ")");
            }
            // a repair follows a fix - often of the metadata itself: never re-analyse with cached schemas
            metadata.invalidateAll();
            String ids = open.stream().map(d -> d.getQueryId() + '/' + d.getKind()).sorted()
                    .collect(Collectors.joining(","));
            CheckpointStore.PendingBatch batch = CheckpointStore.PendingBatch.declare(
                    batchId(name(), BatchKind.REPAIR.name(), nonce(), ids), BatchKind.REPAIR, from, to, now());
            checkpoints.savePending(name(), owner(), ttl(), batch);
            return execute(batch, false, new Declared(batch, null, open));
        }
    }

    /** {@link #rebuild(LocalDateTime, LocalDateTime, RebuildListener)} without a listener. */
    public CycleReport rebuild(LocalDateTime fromDay, LocalDateTime toDay) {
        return rebuild(fromDay, toDay, (r, done, total) -> { });
    }

    /**
     * Operator: recomputes whole days {@code [fromDay, toDay)} from the batch membership (after an analyzer change),
     * one REBUILD batch per hour.
     *
     * <p>The first hour of a day purges the usage and the daily lineage of the whole day and records every edge with
     * facts that day for a refresh; every hour then adds its own usage and lineage and refreshes the edges it
     * touched. The hours of a day form a chain: each one commits and declares the next in one checkpoint write, so
     * an interrupted day is completed by whoever resumes the pending batch (this method, {@code run} or the daemon)
     * and a day is never left half rebuilt. While a day is being rebuilt its usage and lineage are partial.</p>
     *
     * <p>Interrupted rebuilds resume: when a REBUILD batch is pending its day is completed first; when that day lies
     * in {@code [fromDay, toDay)} the rebuild continues with the next day, otherwise it starts at {@code fromDay}.
     * Refused when audit records of members are gone (audit retention), since the rebuild would lose data.</p>
     *
     * @return the report of the last hour (BLOCKED stops the rebuild), or IDLE when there was nothing to do
     */
    public CycleReport rebuild(LocalDateTime fromDay, LocalDateTime toDay, RebuildListener listener) {
        Objects.requireNonNull(listener, "listener");
        synchronized (cycleLock) {
            requireLease();
            if (!isDayStart(fromDay) || !isDayStart(toDay) || !fromDay.isBefore(toDay)) {
                throw new IllegalArgumentException("rebuild needs whole days fromDay < toDay");
            }
            CheckpointStore.Checkpoint cp = checkpoint();
            if (toDay.isAfter(cp.getCommittedUntil())) {
                throw new IllegalArgumentException("days up to " + toDay + " are not committed yet (watermark "
                        + cp.getCommittedUntil() + ")");
            }
            LocalDateTime first = fromDay;
            CheckpointStore.PendingBatch interrupted = cp.getPending().orElse(null);
            if (interrupted != null && interrupted.getKind() != BatchKind.REBUILD) {
                throw new IllegalStateException("batch " + interrupted + " is pending: let it finish (or skip it) "
                        + "first");
            }
            if (interrupted != null && !interrupted.getRangeStart().isBefore(fromDay)
                    && interrupted.getRangeStart().isBefore(toDay)) {
                first = ceilDay(interrupted.getRangeEnd());
            }
            for (LocalDateTime day = first; day.isBefore(toDay); day = day.plusDays(1)) {
                requireAuditComplete(day, day.plusDays(1));
            }
            int total = (int) (ChronoUnit.HOURS.between(first, max(first, toDay))
                    + (interrupted == null ? 0 : remainingHours(interrupted)));
            int[] done = {0};
            CycleReport last = CycleReport.idle("nothing to rebuild in [" + fromDay + ", " + toDay + ")");
            if (interrupted != null) {
                LOG.info("completing the interrupted rebuild of day {} first, from {}",
                        interrupted.getRangeStart().toLocalDate(), interrupted.getRangeStart());
                last = completeRebuildChain(listener, done, total);
                if (last.getOutcome() == CycleReport.Outcome.BLOCKED) {
                    return last;
                }
                if (first.isAfter(fromDay)) {
                    LOG.info("days [{}, {}) were rebuilt by the interrupted run: continuing at {}", fromDay, first,
                            first);
                }
            }
            // a rebuild follows an analyzer or metadata fix: never re-analyse with cached schemas
            metadata.invalidateAll();
            for (LocalDateTime day = first; day.isBefore(toDay); day = day.plusDays(1)) {
                CheckpointStore.PendingBatch batch = declareRebuildHour(day);
                checkpoints.savePending(name(), owner(), ttl(), batch);
                last = execute(batch, false, Declared.NONE);
                listener.hourDone(last, ++done[0], total);
                if (last.getOutcome() == CycleReport.Outcome.BLOCKED) {
                    return last;
                }
                last = completeRebuildChain(listener, done, total);
                if (last.getOutcome() == CycleReport.Outcome.BLOCKED) {
                    return last;
                }
                LOG.info("REBUILD day {} complete ({}/{} hours); to resume after an interruption from here: rebuild "
                        + "{} {}", day.toLocalDate(), done[0], total, day.plusDays(1).toLocalDate(),
                        toDay.toLocalDate());
            }
            return last;
        }
    }

    /** Executes pending REBUILD hours until the chain of the day ends. */
    private CycleReport completeRebuildChain(RebuildListener listener, int[] done, int total) {
        CycleReport last = null;
        Optional<CheckpointStore.PendingBatch> pending;
        while ((pending = checkpoint().getPending()).isPresent() && pending.get().getKind() == BatchKind.REBUILD) {
            CheckpointStore.PendingBatch p = pending.get();
            // a chained hour that has not started is a first attempt; anything else may have written already
            last = execute(p, p.getStep().isAfter(BatchStep.INTENT) || p.getAttempts() > 0, Declared.NONE);
            listener.hourDone(last, ++done[0], total);
            if (last.getOutcome() == CycleReport.Outcome.BLOCKED) {
                return last;
            }
        }
        return last == null ? CycleReport.idle("no rebuild hour pending") : last;
    }

    private CheckpointStore.PendingBatch declareRebuildHour(LocalDateTime start) {
        LocalDateTime end = min(start.truncatedTo(ChronoUnit.HOURS).plusHours(1),
                start.truncatedTo(ChronoUnit.DAYS).plusDays(1));
        return CheckpointStore.PendingBatch.declare(batchId(name(), BatchKind.REBUILD.name(), nonce(),
                start.toString(), end.toString()), BatchKind.REBUILD, start, end, now());
    }

    /** @return batches left in the chain of an interrupted REBUILD (one for a multi-day batch of an older version) */
    private static int remainingHours(CheckpointStore.PendingBatch p) {
        if (ChronoUnit.HOURS.between(p.getRangeStart(), p.getRangeEnd()) > 1) {
            return 1;
        }
        return (int) Math.max(1, ChronoUnit.HOURS.between(p.getRangeStart().truncatedTo(ChronoUnit.HOURS),
                ceilDay(p.getRangeEnd())));
    }

    static boolean isDayStart(LocalDateTime t) {
        return t.equals(t.truncatedTo(ChronoUnit.DAYS));
    }

    /** @return {@code t} if it starts a day, else the start of the next day */
    static LocalDateTime ceilDay(LocalDateTime t) {
        return isDayStart(t) ? t : t.truncatedTo(ChronoUnit.DAYS).plusDays(1);
    }

    private void requireAuditComplete(LocalDateTime from, LocalDateTime to) {
        long members = source.countMembers(name(), from, to);
        long present = source.countProcessed(name(), from, to);
        if (members != present) {
            throw new IllegalStateException((members - present) + " of " + members + " audit records of ["
                    + from + ", " + to + ") are gone (audit retention): a rebuild would lose their contribution");
        }
    }

    // ============================================================================================ batch execution

    private CycleReport execute(CheckpointStore.PendingBatch p, boolean replay, Declared declared) {
        try {
            return doExecute(p, replay, declared);
        } catch (LeaseLostException e) {
            LOG.warn("lease of {} lost during batch {}: stopping ({})", name(), p.getBatchId(), e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            // an unexpected exception (driver, pool, process) is treated as transient: retried every cycle and
            // alerted after alertAfterAttempts; only failures known to repeat identically report BLOCKED
            boolean retryable = !(e instanceof PipelineException) || ((PipelineException) e).isRetryable();
            String error = (retryable ? "" : "[non-retryable] ") + describe(e);
            int attempts = p.getAttempts() + 1;
            try {
                checkpoints.recordFailure(name(), owner(), ttl(), p.getBatchId(), error);
            } catch (LeaseLostException lost) {
                throw lost;
            } catch (RuntimeException storeFailure) {
                LOG.warn("could not record the failure of batch {}: {}", p.getBatchId(), storeFailure.toString());
            }
            if (attempts >= config.getAlertAfterAttempts()) {
                LOG.error("ALERT pipeline {} batch {} failed {} times: {}", name(), p, attempts, error, e);
            } else {
                LOG.warn("batch {} failed (attempt {}): {}", p.getBatchId(), attempts, error, e);
            }
            if (!retryable) {
                metrics.recordFailure(true);
                return CycleReport.blocked(p, error + " - resolve with resolveBlockedStep / skipPendingBatch");
            }
            throw e instanceof PipelineException ? (PipelineException) e
                    : new PipelineException("batch " + p.getBatchId() + " failed: " + e, e);
        }
    }

    private CycleReport doExecute(CheckpointStore.PendingBatch p, boolean replay, Declared declared) {
        String id = p.getBatchId();
        BatchStep step = p.getStep();
        boolean analyse = !step.isAfter(BatchStep.USAGE);
        Accumulator acc = new Accumulator(p);
        if (replay) {
            LOG.info("resuming {}", p);
            acc.frozen = frozenDeadLetters(p);
        }
        int records;
        switch (p.getKind()) {
            case SLICE:
            case LATE: {
                List<AuditLogRecord> batchRecords;
                if (declared.records != null) {
                    batchRecords = declared.records;
                } else if (!step.isAfter(BatchStep.INTENT) && !(batchRecords = readMembers(p)).isEmpty()) {
                    // the membership load (atomic) committed before the step could be recorded: it is the batch
                    advance(p, BatchStep.PREPARE);
                    step = BatchStep.PREPARE;
                } else {
                    batchRecords = readBatchRecords(p);
                }
                records = batchRecords.size();
                if (!step.isAfter(BatchStep.INTENT)) {
                    checkpoints.verifyLease(name(), owner(), ttl());
                    sink.writeMembers(id, p.getSince(), batchRecords);
                    advance(p, BatchStep.PREPARE);
                }
                if (analyse) {
                    analyseAll(batchRecords, acc);
                }
                break;
            }
            case REPAIR: {
                List<DeadLetter> letters;
                if (!step.isAfter(BatchStep.INTENT)) {
                    letters = declared.letters != null ? declared.letters
                            : sink.deadLetters(EnumSet.of(DeadLetter.Status.OPEN), REPAIRABLE, p.getRangeStart(),
                            p.getRangeEnd(), null, config.getMaxRecordsPerBatch());
                    writeLetters(id, "reserve", letters.stream()
                            .map(d -> d.with(DeadLetter.Status.REPAIRING, id, null)).collect(Collectors.toList()));
                    advance(p, BatchStep.PREPARE);
                }
                // the reservation is chunked: an earlier attempt may have reserved letters this one did not select
                letters = reserved(p);
                records = letters.size();
                if (analyse) {
                    for (DeadLetter d : letters) {
                        acc.originals.put(d.getQueryId(), d);
                    }
                    analyseAll(letters.stream().map(DeadLetter::getRecord).collect(Collectors.toList()), acc);
                }
                break;
            }
            case REBUILD: {
                if (!step.isAfter(BatchStep.INTENT)) {
                    prepareRebuild(p);
                    advance(p, BatchStep.PREPARE);
                }
                records = analyse ? analyseProcessed(p, acc) : 0;
                break;
            }
            default:
                throw new IllegalStateException("unknown batch kind " + p.getKind());
        }

        List<HourlyUsageRow> usageRows = acc.usage.drain();
        List<LineageRow> lineageRows = acc.lineage.drain();
        long filtered = 0;
        boolean usageDone = !step.isBefore(BatchStep.USAGE);
        boolean lineageDone = !step.isBefore(BatchStep.LINEAGE_STAGE);
        if (analyse) {
            // a previous attempt may have committed a load without recording its step: ask its label now, before
            // deciding which failures still need a dead letter
            boolean queryFailures = acc.letters.stream().anyMatch(d -> d.getRecord().isQuery());
            boolean writeFailures = acc.letters.stream().anyMatch(d -> !d.getRecord().isQuery());
            if (!usageDone && (!usageRows.isEmpty() || queryFailures)) {
                usageDone = !mayWrite(p, replay, BatchStep.USAGE, "usage");
            }
            writeFailures(p, acc, usageDone, lineageDone);
        }
        if (step.ordinal() < BatchStep.DEAD_LETTERS.ordinal()) {
            advance(p, BatchStep.DEAD_LETTERS);
        }
        if (step.ordinal() < BatchStep.DICTIONARY.ordinal()) {
            if (!usageRows.isEmpty()) {
                checkpoints.verifyLease(name(), owner(), ttl());
                sink.writeUserDictionary(id, usageRows);
            }
            advance(p, BatchStep.DICTIONARY);
        }
        if (step.ordinal() < BatchStep.USAGE.ordinal()) {
            if (!usageRows.isEmpty() && !usageDone) {
                checkpoints.verifyLease(name(), owner(), ttl());
                filtered += filteredRows(sink.writeUsage(id, usageRows));
            }
            advance(p, BatchStep.USAGE);
        }
        if (step.ordinal() < BatchStep.LINEAGE_STAGE.ordinal()) {
            if (replay) {
                // an earlier attempt may have loaded some chunks: start from an empty stage
                checkpoints.verifyLease(name(), owner(), ttl());
                sink.clearBatchLineage(id);
            }
            if (!lineageRows.isEmpty()) {
                checkpoints.verifyLease(name(), owner(), ttl());
                filtered += filteredRows(sink.writeBatchLineage(id, p.getSince(), lineageRows));
            }
            advance(p, BatchStep.LINEAGE_STAGE);
        }
        boolean lineageKnownEmpty = analyse && lineageRows.isEmpty();
        if (step.ordinal() < BatchStep.LINEAGE_DAILY.ordinal()) {
            if (!lineageKnownEmpty && mayWrite(p, replay, BatchStep.LINEAGE_DAILY, "daily")) {
                checkpoints.verifyLease(name(), owner(), ttl());
                sink.insertDaily(id);
            }
            advance(p, BatchStep.LINEAGE_DAILY);
        }
        if (step.ordinal() < BatchStep.REFRESH.ordinal()) {
            if (!lineageKnownEmpty || (p.getKind() == BatchKind.REBUILD && isDayStart(p.getRangeStart()))) {
                checkpoints.verifyLease(name(), owner(), ttl());
                sink.refreshLineage(id);
            }
            advance(p, BatchStep.REFRESH);
        }
        if (step.ordinal() < BatchStep.RESOLVE.ordinal()) {
            resolve(p);
            advance(p, BatchStep.RESOLVE);
        }

        LocalDateTime now = now();
        sink.writeBatchLog(new BatchLogEntry(id, p.getKind(), p.getRangeStart(), p.getRangeEnd(), records,
                acc.succeeded, acc.skipped, acc.failed, acc.degraded, usageRows.size(), lineageRows.size(),
                acc.letters.size(), filtered, p.getAttempts() + 1, p.getSince(), now));
        if (p.getKind() == BatchKind.REBUILD && !isDayStart(p.getRangeEnd())) {
            // the rest of the day was purged: its next hour is declared atomically with this commit
            CheckpointStore.PendingBatch next = declareRebuildHour(p.getRangeEnd());
            checkpoints.commitAndDeclare(name(), owner(), ttl(), id, 0, next);
            LOG.info("rebuild hour [{}, {}) committed; next hour {} declared", p.getRangeStart(), p.getRangeEnd(),
                    next.getBatchId());
        } else {
            checkpoints.commit(name(), owner(), ttl(), id, p.getKind().advancesWatermark() ? p.getRangeEnd() : null,
                    p.getKind() == BatchKind.REBUILD || p.getKind() == BatchKind.REPAIR ? 0 : records);
        }
        metrics.recordCommit(p.getKind(), records, lineageRows.size(), usageRows.size(), acc.letters.size(),
                filtered, replay);
        return CycleReport.committed(p, records, acc.statuses, usageRows.size(), lineageRows.size(),
                acc.letters.size(), filtered, replay);
    }

    private void advance(CheckpointStore.PendingBatch p, BatchStep step) {
        checkpoints.advanceStep(name(), owner(), ttl(), p.getBatchId(), step);
    }

    private static long filteredRows(StreamLoadClient.Result result) {
        return result == null ? 0 : result.getFilteredRows();
    }

    /**
     * Label guard of an additive step of a resumed batch. Steps run strictly in order, so only the step right
     * after the persisted one can have been started by a previous attempt; every later step is certainly new.
     *
     * @return {@code false} when a previous attempt committed the step
     */
    private boolean mayWrite(CheckpointStore.PendingBatch p, boolean replay, BatchStep guarded, String step) {
        if (!replay || p.getStep().ordinal() + 1 != guarded.ordinal()) {
            return true;
        }
        String label = sink.label(p.getBatchId(), step);
        StreamLoadClient.LoadState state = sink.loadState(label);
        switch (state) {
            case COMMITTED:
                LOG.info("label {} already committed by a previous attempt: step not repeated", label);
                return false;
            case IN_PROGRESS:
                throw new PipelineException("load " + label + " of a previous attempt is still in flight");
            case ABORTED:
                return true;
            default:
                if (now().isBefore(p.getSince().plus(config.getLabelRetention()))) {
                    return true;
                }
                throw new BatchBlockedException("label " + label + " is unknown to the FE and batch "
                        + p.getBatchId() + " was declared at " + p.getSince() + ", longer ago than the label "
                        + "retention " + config.getLabelRetention() + ": whether the load committed cannot be "
                        + "decided automatically");
        }
    }

    private void requireAuditCompleteOrBlock(LocalDateTime from, LocalDateTime to) {
        try {
            requireAuditComplete(from, to);
        } catch (IllegalStateException e) {
            throw new BatchBlockedException(e.getMessage());
        }
    }

    /**
     * PREPARE of a REBUILD batch. A batch starting a day purges the whole day (usage and daily lineage) and records
     * its edges for the refresh; the following hours of the day only add. A REBUILD declared by an earlier version
     * spans whole days and is handled by the same rule.
     */
    private void prepareRebuild(CheckpointStore.PendingBatch p) {
        LocalDateTime start = p.getRangeStart();
        if (!isDayStart(start)) {
            requireAuditCompleteOrBlock(start, p.getRangeEnd());
            return;
        }
        LocalDateTime end = ceilDay(p.getRangeEnd());
        requireAuditCompleteOrBlock(start, end);
        checkpoints.verifyLease(name(), owner(), ttl());
        sink.recordEdgesForRefresh(p.getBatchId(), p.getSince(), start, end);
        sink.purgeLineageDaily(start, end);
        sink.purgeUsage(start, end);
    }

    // ------------------------------------------------------------------------------------------- records

    /** Records of a SLICE / LATE batch: the membership once written, else the declaration query again. */
    private List<AuditLogRecord> readBatchRecords(CheckpointStore.PendingBatch p) {
        if (p.getStep().isAfter(BatchStep.INTENT)) {
            return readMembers(p);
        }
        if (p.getKind() == BatchKind.LATE) {
            return source.fetchUnprocessed(name(), p.getRangeStart(), p.getRangeEnd(),
                    config.getMaxRecordsPerBatch());
        }
        return readRange(p.getRangeStart(), p.getRangeEnd());
    }

    private List<AuditLogRecord> readMembers(CheckpointStore.PendingBatch p) {
        List<AuditLogRecord> all = new ArrayList<>();
        Watermark cursor = Watermark.startingAt(p.getRangeStart());
        while (true) {
            List<AuditLogRecord> page = source.fetchMembers(name(), p.getBatchId(), p.getRangeStart(),
                    p.getRangeEnd(), cursor, config.getPageSize());
            all.addAll(page);
            if (page.size() < config.getPageSize()) {
                return all;
            }
            cursor = last(page);
        }
    }

    private List<AuditLogRecord> readRange(LocalDateTime from, LocalDateTime to) {
        List<AuditLogRecord> all = new ArrayList<>();
        Watermark cursor = Watermark.startingAt(from);
        while (true) {
            List<AuditLogRecord> page = source.fetchRange(from, to, cursor, config.getPageSize());
            all.addAll(page);
            if (page.size() < config.getPageSize()) {
                return all;
            }
            cursor = last(page);
        }
    }

    private static Watermark last(List<AuditLogRecord> page) {
        AuditLogRecord r = page.get(page.size() - 1);
        return new Watermark(r.getTimestamp(), r.getQueryId());
    }

    private List<DeadLetter> reserved(CheckpointStore.PendingBatch p) {
        return sink.deadLetters(EnumSet.of(DeadLetter.Status.REPAIRING), REPAIRABLE, p.getRangeStart(),
                p.getRangeEnd(), p.getBatchId(), Integer.MAX_VALUE);
    }

    /**
     * REBUILD: every member record of the range. The range is cut into {@code sliceSize} windows read by
     * {@code readThreads} parallel readers (each a keyset scan of its own window, so no read re-scans what was read
     * before); pages go through a bounded queue to this thread, which analyses them on the analysis pool. Only the
     * aggregates and at most {@code 2 * readThreads} pages stay in memory. Progress is reported every
     * {@code progressInterval}.
     */
    private int analyseProcessed(CheckpointStore.PendingBatch p, Accumulator acc) {
        List<LocalDateTime> starts = new ArrayList<>();
        for (LocalDateTime t = p.getRangeStart(); t.isBefore(p.getRangeEnd()); t = t.plus(config.getSliceSize())) {
            starts.add(t);
        }
        if (starts.isEmpty()) {
            return 0;
        }
        long total = source.countMembers(name(), p.getRangeStart(), p.getRangeEnd());
        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(2 * config.getReadThreads());
        AtomicBoolean cancelled = new AtomicBoolean();
        ExecutorService readers = Executors.newFixedThreadPool(Math.min(config.getReadThreads(), starts.size()),
                namedThreads("audit-reader"));
        Progress progress = new Progress(p, total);
        int analysed = 0;
        try {
            for (LocalDateTime start : starts) {
                LocalDateTime end = min(start.plus(config.getSliceSize()), p.getRangeEnd());
                readers.execute(() -> readWindow(start, end, queue, cancelled));
            }
            int finished = 0;
            long nextReport = System.nanoTime() + config.getProgressInterval().toNanos();
            while (finished < starts.size()) {
                Object item = queue.poll(200, TimeUnit.MILLISECONDS);
                if (item == WINDOW_DONE) {
                    finished++;
                } else if (item instanceof Throwable) {
                    Throwable cause = (Throwable) item;
                    throw cause instanceof PipelineException ? (PipelineException) cause
                            : new PipelineException("reading the audit records of " + p.getBatchId() + " failed: "
                            + cause, cause);
                } else if (item != null) {
                    @SuppressWarnings("unchecked")
                    List<AuditLogRecord> page = (List<AuditLogRecord>) item;
                    analyseAll(page, acc);
                    analysed += page.size();
                }
                if (System.nanoTime() >= nextReport) {
                    // a long analysis keeps its lease (a standby must not take over a live rebuild) and stops at
                    // once when it was lost
                    checkpoints.verifyLease(name(), owner(), ttl());
                    report(progress.at(analysed, false));
                    nextReport = System.nanoTime() + config.getProgressInterval().toNanos();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipelineException("rebuild interrupted", e);
        } finally {
            cancelled.set(true);
            readers.shutdownNow();
        }
        report(progress.at(analysed, true));
        return analysed;
    }

    private static final Object WINDOW_DONE = new Object();

    /** One reader task: the member records of {@code [start, end)}, page by page, then {@link #WINDOW_DONE}. */
    private void readWindow(LocalDateTime start, LocalDateTime end, BlockingQueue<Object> queue,
                            AtomicBoolean cancelled) {
        try {
            Watermark cursor = Watermark.startingAt(start);
            while (!cancelled.get()) {
                List<AuditLogRecord> page = source.fetchProcessed(name(), start, end, cursor, config.getPageSize());
                if (!page.isEmpty()) {
                    queue.put(page);
                }
                if (page.size() < config.getPageSize()) {
                    break;
                }
                cursor = last(page);
            }
            queue.put(WINDOW_DONE);
        } catch (InterruptedException e) {
            // cancelled by the consumer
            Thread.currentThread().interrupt();
        } catch (RuntimeException | Error e) {
            if (!cancelled.get()) {
                try {
                    queue.put(e);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void report(Progress progress) {
        LOG.info("{}", progress);
        try {
            progressListener.accept(progress);
        } catch (RuntimeException e) {
            LOG.warn("progress listener failed: {}", e.toString());
        }
    }

    /**
     * Progress of the analysis of a batch.
     */
    public static final class Progress {
        private final CheckpointStore.PendingBatch batch;
        private final long total;
        private final long startNanos;
        private final long analysed;
        private final long elapsedMillis;
        private final boolean finished;

        Progress(CheckpointStore.PendingBatch batch, long total) {
            this(batch, total, System.nanoTime(), 0, 0, false);
        }

        private Progress(CheckpointStore.PendingBatch batch, long total, long startNanos, long analysed,
                         long elapsedMillis, boolean finished) {
            this.batch = batch;
            this.total = total;
            this.startNanos = startNanos;
            this.analysed = analysed;
            this.elapsedMillis = elapsedMillis;
            this.finished = finished;
        }

        Progress at(long n, boolean done) {
            return new Progress(batch, total, startNanos, n, (System.nanoTime() - startNanos) / 1_000_000, done);
        }

        public CheckpointStore.PendingBatch getBatch() {
            return batch;
        }

        /** @return member records of the batch range (counted when the analysis started) */
        public long getTotal() {
            return total;
        }

        public long getAnalysed() {
            return analysed;
        }

        public long getElapsedMillis() {
            return elapsedMillis;
        }

        public boolean isFinished() {
            return finished;
        }

        /** @return records per second so far */
        public double getRate() {
            return elapsedMillis == 0 ? 0 : analysed * 1000.0 / elapsedMillis;
        }

        @Override
        public String toString() {
            double rate = getRate();
            long remaining = Math.max(0, total - analysed);
            String eta = finished ? "0s" : rate <= 0 ? "-" : Math.round(remaining / rate) + "s";
            return String.format("PROGRESS %s [%s, %s) analysed=%d/%d (%.1f%%) rate=%.0f/s elapsed=%ds eta=%s%s",
                    batch.getKind(), batch.getRangeStart(), batch.getRangeEnd(), analysed, total,
                    total == 0 ? 100.0 : Math.min(100.0, analysed * 100.0 / total), rate, elapsedMillis / 1000, eta,
                    finished ? " done" : "");
        }
    }

    /** Query ids that a previous attempt of this batch dead-lettered: they are never analysed again in it. */
    private Set<String> frozenDeadLetters(CheckpointStore.PendingBatch p) {
        if (p.getKind() == BatchKind.REPAIR) {
            // failures of a repair go back to OPEN and are no longer REPAIRING: excluded by the input query
            return Collections.emptySet();
        }
        Set<String> ids = new HashSet<>();
        for (DeadLetter d : sink.deadLettersOfBatch(p.getBatchId(), p.getRangeStart(), p.getRangeEnd())) {
            if (d.getKind().isRepairable()) {
                ids.add(d.getQueryId());
            }
        }
        return ids;
    }

    // ------------------------------------------------------------------------------------------- analysis

    private void analyseAll(List<AuditLogRecord> records, Accumulator acc) {
        List<AuditLogRecord> todo = new ArrayList<>(records.size());
        for (AuditLogRecord r : records) {
            if (acc.frozen.contains(r.getQueryId())) {
                acc.failed++;
            } else {
                todo.add(r);
            }
        }
        List<Callable<SqlParseResult>> tasks = new ArrayList<>(todo.size());
        for (AuditLogRecord r : todo) {
            tasks.add(() -> analyzer.analyze(r));
        }
        List<Future<SqlParseResult>> futures;
        try {
            futures = analysisPool.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipelineException("analysis interrupted", e);
        }
        for (int i = 0; i < todo.size(); i++) {
            AuditLogRecord record = todo.get(i);
            SqlParseResult result;
            try {
                result = futures.get(i).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PipelineException("analysis interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof MetadataAccessException) {
                    throw new PipelineException("metadata unavailable, batch will be retried: "
                            + cause.getMessage(), cause);
                }
                result = SqlParseResult.of(record.getQueryId(), ParseStatus.ANALYSIS_ERROR,
                        "internal analyzer error: " + cause);
            }
            acc.accept(record, result);
        }
    }

    /**
     * Writes the dead letters of this attempt (records that failed now and were not dead-lettered before). A record
     * whose contribution an earlier attempt already loaded (a query once usage is written, a write once the lineage
     * stage is written) gets no dead letter: repairing it would count it twice.
     */
    private void writeFailures(CheckpointStore.PendingBatch p, Accumulator acc, boolean usageDone,
                               boolean lineageDone) {
        List<DeadLetter> letters = acc.letters.stream()
                .filter(d -> d.getRecord().isQuery() ? !usageDone : !lineageDone)
                .collect(Collectors.toList());
        if (letters.size() < acc.letters.size()) {
            LOG.warn("batch {}: {} record(s) failed only on replay after their contribution was loaded by an "
                    + "earlier attempt; kept as loaded", p.getBatchId(), acc.letters.size() - letters.size());
        }
        writeLetters(p.getBatchId(), "failed", letters);
    }

    /** Dead letters in chunks; each chunk is labelled by its content, so a replay of the same chunk is dropped. */
    private void writeLetters(String batchId, String tag, List<DeadLetter> letters) {
        for (int i = 0; i < letters.size(); i += DEAD_LETTER_CHUNK) {
            List<DeadLetter> chunk = letters.subList(i, Math.min(letters.size(), i + DEAD_LETTER_CHUNK));
            String content = chunk.stream().map(d -> d.getQueryId() + '/' + d.getKind() + '/' + d.getStatus())
                    .collect(Collectors.joining(","));
            checkpoints.verifyLease(name(), owner(), ttl());
            sink.writeDeadLetters(batchId, tag + "_" + batchId(content).substring(0, 16), chunk);
        }
    }

    /** RESOLVE: REPAIR resolves what it reserved and did not fail; REBUILD what it re-analysed successfully. */
    private void resolve(CheckpointStore.PendingBatch p) {
        String id = p.getBatchId();
        if (p.getKind() == BatchKind.REPAIR) {
            writeLetters(id, "resolved", reserved(p).stream()
                    .map(d -> d.with(DeadLetter.Status.RESOLVED, id, "repaired by batch " + id))
                    .collect(Collectors.toList()));
        } else if (p.getKind() == BatchKind.REBUILD) {
            List<DeadLetter> open = sink.deadLetters(EnumSet.of(DeadLetter.Status.OPEN), REPAIRABLE,
                    p.getRangeStart(), p.getRangeEnd(), null, Integer.MAX_VALUE);
            writeLetters(id, "resolved", open.stream().filter(d -> !id.equals(d.getBatchId()))
                    .map(d -> d.with(DeadLetter.Status.RESOLVED, id, "re-analysed successfully by rebuild " + id))
                    .collect(Collectors.toList()));
        }
    }

    /**
     * What one attempt of a batch derived from its records.
     */
    private final class Accumulator {
        final CheckpointStore.PendingBatch batch;
        final HourlyUsageAggregator usage = new HourlyUsageAggregator();
        final LineageAggregator lineage = new LineageAggregator();
        final List<DeadLetter> letters = new ArrayList<>();
        final Map<ParseStatus, Integer> statuses = new EnumMap<>(ParseStatus.class);
        final Map<String, DeadLetter> originals = new HashMap<>();
        Set<String> frozen = Collections.emptySet();
        int succeeded;
        int skipped;
        int failed;
        int degraded;

        Accumulator(CheckpointStore.PendingBatch batch) {
            this.batch = batch;
        }

        void accept(AuditLogRecord record, SqlParseResult r) {
            statuses.merge(r.getStatus(), 1, Integer::sum);
            metrics.recordStatus(r.getStatus(), r.isDegraded());
            if (r.isDegraded()) {
                degraded++;
            }
            if (r.getStatus().isError()) {
                failed++;
                letters.add(deadLetter(record, r));
                return;
            }
            if (!r.isSuccess()) {
                skipped++;
                return;
            }
            succeeded++;
            StatementType type = r.getStatementType().orElse(StatementType.OTHER);
            if (type.isQuery()) {
                usage.add(record, r);
            } else if (type.isWrite()) {
                lineage.add(record, r);
                if (SCHEMA_CHANGING.contains(type)) {
                    r.getTargetTable().ifPresent(metadata::invalidate);
                }
            }
        }

        private DeadLetter deadLetter(AuditLogRecord record, SqlParseResult r) {
            String message = r.getMessage().orElse(r.getStatus().name());
            DeadLetter original = originals.get(record.getQueryId());
            if (original != null) {
                // a repair keeps the original kind and batch (the PRIMARY KEY includes the kind)
                return original.with(DeadLetter.Status.OPEN, null, message);
            }
            DeadLetter.Kind kind = r.getStatus() == ParseStatus.PARSE_ERROR ? DeadLetter.Kind.PARSE_ERROR
                    : DeadLetter.Kind.ANALYSIS_ERROR;
            return DeadLetter.ofRecord(record, kind, batch.getBatchId(), message);
        }
    }

    /**
     * What a declaring cycle already read, so the first attempt does not read it twice.
     */
    private static final class Declared {
        static final Declared NONE = new Declared(null, null, null);
        final CheckpointStore.PendingBatch batch;
        final List<AuditLogRecord> records;
        final List<DeadLetter> letters;

        Declared(CheckpointStore.PendingBatch batch, List<AuditLogRecord> records, List<DeadLetter> letters) {
            this.batch = batch;
            this.records = records;
            this.letters = letters;
        }
    }

    // ============================================================================================ helpers

    private boolean acquireLease() {
        return checkpoints.tryAcquireLease(name(), owner(), ttl(), config.getInitialStart());
    }

    private void requireLease() {
        if (!acquireLease()) {
            throw new IllegalStateException("another instance holds the lease of " + name());
        }
    }

    private void requireNoPending() {
        checkpoint().getPending().ifPresent(p -> {
            throw new IllegalStateException("batch " + p + " is pending: let it finish (or skip it) first");
        });
    }

    private CheckpointStore.Checkpoint checkpoint() {
        return checkpoints.load(name()).orElseThrow(() -> new PipelineException("checkpoint missing after lease"));
    }

    private String name() {
        return config.getPipelineName();
    }

    private String owner() {
        return config.getOwner();
    }

    private Duration ttl() {
        return config.getLeaseTtl();
    }

    private LocalDateTime now() {
        return LocalDateTime.now(clock).withNano(0);
    }

    private static LocalDateTime max(LocalDateTime a, LocalDateTime b) {
        return a.isAfter(b) ? a : b;
    }

    private static LocalDateTime min(LocalDateTime a, LocalDateTime b) {
        return a.isBefore(b) ? a : b;
    }

    private static String describe(Throwable e) {
        StringBuilder sb = new StringBuilder(e.toString());
        Throwable c = e.getCause();
        while (c != null && c != e) {
            sb.append(" <- ").append(c);
            e = c;
            c = c.getCause();
        }
        return sb.toString();
    }

    /**
     * A SLICE id is a function of the checkpoint epoch and its range (two instances racing on the same slice use the
     * same labels; a pipeline reset under the same name gets a new epoch, so it never collides with labels StarRocks
     * still remembers from before the reset). Every other
     * batch can recur with the same definition (the same range rebuilt twice, the same dead letters repaired again
     * after failing), and a reused id would reuse labels and silently skip loads: they get a random component.
     */
    private static String nonce() {
        return java.util.UUID.randomUUID().toString();
    }

    /** Deterministic batch id: 32 hex characters of SHA-256 over the parts. */
    static String batchId(String... parts) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (String part : parts) {
                md.update(part.getBytes(StandardCharsets.UTF_8));
                md.update((byte) 1);
            }
            byte[] d = md.digest();
            StringBuilder sb = new StringBuilder(32);
            for (int i = 0; i < 16; i++) {
                sb.append(Character.forDigit((d[i] >> 4) & 0xF, 16)).append(Character.forDigit(d[i] & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // ============================================================================================ scheduling

    /**
     * Runs a cycle every {@code interval} (fixed delay). After a committed batch the next cycle starts at once
     * (catch-up); failures are logged and retried at the next tick.
     */
    public synchronized void start(Duration interval) {
        if (scheduler != null) {
            throw new IllegalStateException("already started");
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(namedThreads("audit-pipeline"));
        scheduler.scheduleWithFixedDelay(this::runUntilCaughtUp, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void runUntilCaughtUp() {
        try {
            CycleReport report;
            do {
                report = runOnce();
                LOG.info("pipeline cycle: {}", report);
            } while (report.getOutcome() == CycleReport.Outcome.COMMITTED && !Thread.currentThread().isInterrupted());
        } catch (RuntimeException e) {
            LOG.warn("pipeline cycle failed, will retry: {}", e.getMessage(), e);
        }
    }

    /** Stops the scheduler and gives the lease up so a standby instance can take over at once. */
    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        analysisPool.shutdownNow();
        synchronized (cycleLock) {
            try {
                checkpoints.releaseLease(name(), owner());
            } catch (RuntimeException e) {
                LOG.warn("could not release the lease of {}: {}", name(), e.toString());
            }
        }
    }

    /**
     * Stack of the analysis threads. The generated parser and the visitor recurse with the nesting of a statement:
     * production ETL statements (CTE chains, CASE ladders, deeply nested sub-queries) need more than the JVM default
     * of 512 KB - 1 MB, and a too small stack turns valid statements into "nesting too deep" dead letters. Owning the
     * size here means no {@code -Xss} has to be remembered at deployment.
     */
    public static final long ANALYSIS_STACK_BYTES = 16L * 1024 * 1024;

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(null, r, prefix + "-" + n.incrementAndGet(), ANALYSIS_STACK_BYTES);
            t.setDaemon(true);
            return t;
        };
    }
}
