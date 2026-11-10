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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 *   <li>the additive steps (USAGE, LINEAGE_STAGE, LINEAGE_DAILY) ask the FE for their label state first: committed →
 *       skipped; in flight → retried later; aborted or never seen → executed; unknown after the label retention →
 *       {@link BatchBlockedException} (operator: {@link #resolveBlockedStep});</li>
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
    private static final Set<BatchStep> GUARDED_STEPS = EnumSet.of(BatchStep.USAGE, BatchStep.LINEAGE_STAGE,
            BatchStep.LINEAGE_DAILY);
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

    /**
     * Operator: recomputes whole days {@code [fromDay, toDay)} from the batch membership (after an analyzer change).
     * Usage and daily lineage of the days are purged and rebuilt; the lineage table is refreshed for every edge that
     * had or has facts in those days. Refused when audit records of members are gone (audit retention), since the
     * rebuild would then lose data.
     */
    public CycleReport rebuild(LocalDateTime fromDay, LocalDateTime toDay) {
        synchronized (cycleLock) {
            requireLease();
            CheckpointStore.Checkpoint cp = checkpoint();
            requireNoPending();
            if (!fromDay.equals(fromDay.truncatedTo(ChronoUnit.DAYS))
                    || !toDay.equals(toDay.truncatedTo(ChronoUnit.DAYS)) || !fromDay.isBefore(toDay)) {
                throw new IllegalArgumentException("rebuild needs whole days fromDay < toDay");
            }
            if (toDay.isAfter(cp.getCommittedUntil())) {
                throw new IllegalArgumentException("days up to " + toDay + " are not committed yet (watermark "
                        + cp.getCommittedUntil() + ")");
            }
            requireAuditComplete(fromDay, toDay);
            metadata.invalidateAll();
            CheckpointStore.PendingBatch batch = CheckpointStore.PendingBatch.declare(
                    batchId(name(), BatchKind.REBUILD.name(), nonce(), fromDay.toString(), toDay.toString()),
                    BatchKind.REBUILD, fromDay, toDay, now());
            checkpoints.savePending(name(), owner(), ttl(), batch);
            return execute(batch, false, Declared.NONE);
        }
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
                    requireAuditCompleteOrBlock(p);
                    checkpoints.verifyLease(name(), owner(), ttl());
                    sink.recordEdgesForRefresh(id, p.getSince(), p.getRangeStart(), p.getRangeEnd());
                    sink.purgeLineageDaily(p.getRangeStart(), p.getRangeEnd());
                    sink.purgeUsage(p.getRangeStart(), p.getRangeEnd());
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
            if (!lineageDone && (!lineageRows.isEmpty() || writeFailures)) {
                lineageDone = !mayWrite(p, replay, BatchStep.LINEAGE_STAGE, "lineage");
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
            if (!lineageRows.isEmpty() && !lineageDone) {
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
            if (!lineageKnownEmpty || p.getKind() == BatchKind.REBUILD) {
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
        checkpoints.commit(name(), owner(), ttl(), id, p.getKind().advancesWatermark() ? p.getRangeEnd() : null,
                p.getKind() == BatchKind.REBUILD || p.getKind() == BatchKind.REPAIR ? 0 : records);
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

    private void requireAuditCompleteOrBlock(CheckpointStore.PendingBatch p) {
        try {
            requireAuditComplete(p.getRangeStart(), p.getRangeEnd());
        } catch (IllegalStateException e) {
            throw new BatchBlockedException(e.getMessage());
        }
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

    /** REBUILD: every member record of the days, page by page (only the aggregates stay in memory). */
    private int analyseProcessed(CheckpointStore.PendingBatch p, Accumulator acc) {
        int n = 0;
        Watermark cursor = Watermark.startingAt(p.getRangeStart());
        while (true) {
            List<AuditLogRecord> page = source.fetchProcessed(name(), p.getRangeStart(), p.getRangeEnd(), cursor,
                    config.getPageSize());
            analyseAll(page, acc);
            n += page.size();
            if (page.size() < config.getPageSize()) {
                return n;
            }
            cursor = last(page);
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
