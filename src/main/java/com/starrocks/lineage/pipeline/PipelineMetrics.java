package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.ParseStatus;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Cumulative, thread-safe counters of a pipeline (exported by the embedding service).
 */
public final class PipelineMetrics {

    private final AtomicLongArray statuses = new AtomicLongArray(ParseStatus.values().length);
    private final AtomicLong degraded = new AtomicLong();
    private final AtomicLong lineageRows = new AtomicLong();
    private final AtomicLong usageRows = new AtomicLong();
    private final AtomicLong committedCycles = new AtomicLong();
    private final AtomicLong failedCycles = new AtomicLong();
    private final AtomicLong replayedCycles = new AtomicLong();

    void recordStatus(ParseStatus status, boolean isDegraded) {
        statuses.incrementAndGet(status.ordinal());
        if (isDegraded) {
            degraded.incrementAndGet();
        }
    }

    void recordCommit(int lineage, int usage, boolean replay) {
        lineageRows.addAndGet(lineage);
        usageRows.addAndGet(usage);
        committedCycles.incrementAndGet();
        if (replay) {
            replayedCycles.incrementAndGet();
        }
    }

    void recordFailure() {
        failedCycles.incrementAndGet();
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

    public long getCommittedCycles() {
        return committedCycles.get();
    }

    public long getFailedCycles() {
        return failedCycles.get();
    }

    public long getReplayedCycles() {
        return replayedCycles.get();
    }
}
