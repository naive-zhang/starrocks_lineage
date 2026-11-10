package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.ParseStatus;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Cumulative, thread-safe counters of a pipeline (exported by the embedding service). The two gauges an alert should
 * watch are {@link #getConsecutiveFailures()} and {@link #isBlocked()}.
 */
public final class PipelineMetrics {

    private final AtomicLongArray statuses = new AtomicLongArray(ParseStatus.values().length);
    private final AtomicLong degraded = new AtomicLong();
    private final AtomicLong lineageRows = new AtomicLong();
    private final AtomicLong usageRows = new AtomicLong();
    private final AtomicLong committedBatches = new AtomicLong();
    private final AtomicLong failedCycles = new AtomicLong();
    private final AtomicLong replayedBatches = new AtomicLong();
    private final AtomicLong consecutiveFailures = new AtomicLong();
    private final AtomicBoolean blocked = new AtomicBoolean();
    private final AtomicLong lateRecords = new AtomicLong();
    private final AtomicLong deadLetters = new AtomicLong();
    private final AtomicLong filteredRows = new AtomicLong();
    private final AtomicLong skippedBatches = new AtomicLong();

    void recordStatus(ParseStatus status, boolean isDegraded) {
        statuses.incrementAndGet(status.ordinal());
        if (isDegraded) {
            degraded.incrementAndGet();
        }
    }

    void recordCommit(BatchKind kind, int records, int lineage, int usage, int letters, long filtered,
                      boolean replay) {
        lineageRows.addAndGet(lineage);
        usageRows.addAndGet(usage);
        deadLetters.addAndGet(letters);
        filteredRows.addAndGet(filtered);
        committedBatches.incrementAndGet();
        if (kind == BatchKind.LATE) {
            lateRecords.addAndGet(records);
        }
        if (replay) {
            replayedBatches.incrementAndGet();
        }
        consecutiveFailures.set(0);
        blocked.set(false);
    }

    void recordIdle() {
        consecutiveFailures.set(0);
        blocked.set(false);
    }

    void recordFailure(boolean isBlocked) {
        failedCycles.incrementAndGet();
        consecutiveFailures.incrementAndGet();
        blocked.set(isBlocked);
    }

    void recordSkip() {
        skippedBatches.incrementAndGet();
        blocked.set(false);
        consecutiveFailures.set(0);
    }

    public long statusCount(ParseStatus status) {
        return statuses.get(status.ordinal());
    }

    public Map<ParseStatus, Long> statusCounts() {
        Map<ParseStatus, Long> m = new EnumMap<>(ParseStatus.class);
        for (ParseStatus s : ParseStatus.values()) {
            m.put(s, statuses.get(s.ordinal()));
        }
        return m;
    }

    public long getDegraded() {
        return degraded.get();
    }

    public long getLineageRows() {
        return lineageRows.get();
    }

    public long getUsageRows() {
        return usageRows.get();
    }

    public long getCommittedBatches() {
        return committedBatches.get();
    }

    public long getFailedCycles() {
        return failedCycles.get();
    }

    public long getReplayedBatches() {
        return replayedBatches.get();
    }

    /** @return failed cycles since the last commit (alert when it keeps growing) */
    public long getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    /** @return {@code true} while the pending batch fails with a non-retryable error (operator action needed) */
    public boolean isBlocked() {
        return blocked.get();
    }

    /** @return records processed by reconciliation (arrived after their slice was committed) */
    public long getLateRecords() {
        return lateRecords.get();
    }

    public long getDeadLetters() {
        return deadLetters.get();
    }

    public long getFilteredRows() {
        return filteredRows.get();
    }

    public long getSkippedBatches() {
        return skippedBatches.get();
    }
}
