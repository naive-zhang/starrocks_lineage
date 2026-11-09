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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
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

/**
 * Incremental audit → lineage / usage pipeline.
 *
 * <p>One <b>cycle</b> ({@link #runOnce()}):</p>
 * <ol>
 *   <li>take / renew the single-writer lease;</li>
 *   <li>if the checkpoint holds a pending batch intent (previous cycle failed after declaring it), replay exactly
 *       its record range {@code (watermark, end]} under the same batch id; otherwise read up to
 *       {@code maxRecordsPerCycle} records strictly after the watermark and older than {@code now - lateArrival}
 *       (keyset pagination on {@code (timestamp, queryId)}), derive a deterministic batch id from the range and
 *       persist the intent;</li>
 *   <li>analyse the records concurrently on {@code analyzerThreads} threads with the thread-safe
 *       {@link StarRocksSqlAnalyzer}; a {@link MetadataAccessException} aborts the cycle (no commit, retried
 *       later); every other outcome is final for the record;</li>
 *   <li>pre-aggregate: queries into {@link HourlyUsageAggregator} (hour, column, user), writes into
 *       {@link LineageAggregator} (edge);</li>
 *   <li>write: user dictionary, usage, lineage (stage + guarded merge) - every write idempotent per batch id;</li>
 *   <li>commit the end watermark, clearing the intent.</li>
 * </ol>
 * <p>Whatever fails before step 6 leaves the watermark and the intent in place, so the next cycle repeats the same
 * batch and the idempotent sinks make the repetition invisible (exactly-once).</p>
 *
 * <p>Created DDL objects (CTAS / view / materialized view targets) are invalidated in the metadata cache so that
 * following batches see their schema.</p>
 */
public final class AuditLogSyncPipeline implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AuditLogSyncPipeline.class);
    private static final Set<StatementType> SCHEMA_CHANGING = EnumSet.of(StatementType.CREATE_TABLE_AS_SELECT,
            StatementType.CREATE_VIEW, StatementType.ALTER_VIEW, StatementType.CREATE_MATERIALIZED_VIEW);

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

    /**
     * Runs one cycle. Cycles never overlap (also when called concurrently with the scheduler).
     *
     * @return what happened
     * @throws PipelineException when the cycle failed; nothing was committed and the cycle will be repeated
     */
    public CycleReport runOnce() {
        synchronized (cycleLock) {
            try {
                return doRunOnce();
            } catch (RuntimeException e) {
                metrics.recordFailure();
                throw e instanceof PipelineException ? e : new PipelineException("cycle failed: " + e.getMessage(), e);
            }
        }
    }

    private CycleReport doRunOnce() {
        String name = config.getPipelineName();
        String owner = config.getOwner();
        if (!checkpoints.tryAcquireLease(name, owner, config.getLeaseTtl(),
                Watermark.startingAt(config.getInitialStart()))) {
            return CycleReport.leaseNotAcquired();
        }
        CheckpointStore.Checkpoint cp = checkpoints.load(name)
                .orElseThrow(() -> new PipelineException("checkpoint missing after lease"));
        Watermark start = cp.getWatermark();
        Optional<CheckpointStore.PendingBatch> pending = cp.getPending();

        List<AuditLogRecord> records;
        String batchId;
        Watermark end;
        if (pending.isPresent()) {
            batchId = pending.get().getBatchId();
            end = pending.get().getEnd();
            records = readRange(start, end);
            LOG.info("replaying batch {} ({} records, {} -> {})", batchId, records.size(), start, end);
        } else {
            LocalDateTime upper = LocalDateTime.now(clock).minus(config.getLateArrival());
            records = readNew(start, upper);
            if (records.isEmpty()) {
                return CycleReport.idle(start);
            }
            AuditLogRecord last = records.get(records.size() - 1);
            end = new Watermark(last.getTimestamp(), last.getQueryId());
            batchId = batchId(name, start, end);
            checkpoints.savePending(name, owner, config.getLeaseTtl(), new CheckpointStore.PendingBatch(batchId, end));
        }

        List<SqlParseResult> results = analyzeAll(records);

        HourlyUsageAggregator usage = new HourlyUsageAggregator();
        LineageAggregator lineage = new LineageAggregator();
        Map<ParseStatus, Integer> statuses = new EnumMap<>(ParseStatus.class);
        for (int i = 0; i < records.size(); i++) {
            AuditLogRecord record = records.get(i);
            SqlParseResult r = results.get(i);
            statuses.merge(r.getStatus(), 1, Integer::sum);
            metrics.recordStatus(r.getStatus(), r.isDegraded());
            if (!r.isSuccess()) {
                continue;
            }
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

        List<HourlyUsageRow> usageRows = usage.drain();
        List<LineageRow> lineageRows = lineage.drain();
        UserDictionary dictionary = new UserDictionary(sink.loadUserDictionary());
        for (HourlyUsageRow row : usageRows) {
            dictionary.idOf(row.getUserName());
        }
        List<UserDictionary.Entry> newUsers = dictionary.drainPending();
        if (!newUsers.isEmpty()) {
            sink.writeUserDictionary(batchId, newUsers, LocalDateTime.now(clock));
        }
        sink.writeUsage(batchId, usageRows, dictionary);
        sink.writeLineage(batchId, lineageRows);

        checkpoints.commit(name, owner, config.getLeaseTtl(), end, records.size());
        metrics.recordCommit(lineageRows.size(), usageRows.size(), pending.isPresent());
        return new CycleReport(CycleReport.Outcome.COMMITTED, batchId, records.size(), statuses, usageRows.size(),
                lineageRows.size(), newUsers.size(), end, pending.isPresent());
    }

    /** New records after {@code start}, page by page, up to the cycle limit. */
    private List<AuditLogRecord> readNew(Watermark start, LocalDateTime upper) {
        List<AuditLogRecord> all = new ArrayList<>();
        Watermark cursor = start;
        while (all.size() < config.getMaxRecordsPerCycle()) {
            int limit = Math.min(config.getPageSize(), config.getMaxRecordsPerCycle() - all.size());
            List<AuditLogRecord> page = source.fetch(cursor, upper, limit);
            all.addAll(page);
            if (page.size() < limit) {
                break;
            }
            AuditLogRecord last = page.get(page.size() - 1);
            cursor = new Watermark(last.getTimestamp(), last.getQueryId());
        }
        return all;
    }

    /** Exactly the records in {@code (start, end]}. */
    private List<AuditLogRecord> readRange(Watermark start, Watermark end) {
        List<AuditLogRecord> all = new ArrayList<>();
        Watermark cursor = start;
        // exclusive bound one whole second after the end: a sub-second bound is truncated by StarRocks and would
        // drop the end record itself (review R5-03); records after `end` in that second are filtered below
        LocalDateTime upper = end.getTimestamp().withNano(0).plusSeconds(1);
        while (true) {
            List<AuditLogRecord> page = source.fetch(cursor, upper, config.getPageSize());
            for (AuditLogRecord r : page) {
                if (new Watermark(r.getTimestamp(), r.getQueryId()).compareTo(end) <= 0) {
                    all.add(r);
                }
            }
            if (page.size() < config.getPageSize()) {
                return all;
            }
            AuditLogRecord last = page.get(page.size() - 1);
            cursor = new Watermark(last.getTimestamp(), last.getQueryId());
            if (cursor.compareTo(end) >= 0) {
                return all;
            }
        }
    }

    private List<SqlParseResult> analyzeAll(List<AuditLogRecord> records) {
        List<Callable<SqlParseResult>> tasks = new ArrayList<>(records.size());
        for (AuditLogRecord r : records) {
            tasks.add(() -> analyzer.analyze(r));
        }
        List<SqlParseResult> results = new ArrayList<>(records.size());
        try {
            for (Future<SqlParseResult> f : analysisPool.invokeAll(tasks)) {
                results.add(f.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipelineException("analysis interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof MetadataAccessException) {
                throw new PipelineException("metadata unavailable, batch will be retried: " + cause.getMessage(),
                        cause);
            }
            throw new PipelineException("analysis failed: " + cause, cause);
        }
        return results;
    }

    /** Deterministic id of the record range {@code (start, end]} of a pipeline: 32 hex chars. */
    static String batchId(String pipeline, Watermark start, Watermark end) {
        String raw = pipeline + '\u0001' + start + '\u0001' + end;
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(32);
            for (int i = 0; i < 16; i++) {
                sb.append(Character.forDigit((d[i] >> 4) & 0xF, 16)).append(Character.forDigit(d[i] & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Runs a cycle every {@code interval} (fixed delay). Failures are logged and retried at the next tick; when a
     * cycle is full (more records are waiting) the next one starts immediately.
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
            } while (report.getOutcome() == CycleReport.Outcome.COMMITTED
                    && report.getRecords() >= config.getMaxRecordsPerCycle());
        } catch (RuntimeException e) {
            LOG.warn("pipeline cycle failed, will retry: {}", e.getMessage(), e);
        }
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        analysisPool.shutdownNow();
    }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
