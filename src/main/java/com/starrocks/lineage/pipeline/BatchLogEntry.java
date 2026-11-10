package com.starrocks.lineage.pipeline;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * One row of {@code audit_lineage_batch_log}: what a committed batch did (observability, audits, the "is this dead
 * letter's batch committed" check of a repair).
 */
public final class BatchLogEntry {

    private final String batchId;
    private final BatchKind kind;
    private final LocalDateTime rangeStart;
    private final LocalDateTime rangeEnd;
    private final int records;
    private final int succeeded;
    private final int skipped;
    private final int failed;
    private final int degraded;
    private final int usageRows;
    private final int lineageRows;
    private final int deadLetters;
    private final long filteredRows;
    private final int attempts;
    private final LocalDateTime startedTime;
    private final LocalDateTime committedTime;

    public BatchLogEntry(String batchId, BatchKind kind, LocalDateTime rangeStart, LocalDateTime rangeEnd, int records,
                         int succeeded, int skipped, int failed, int degraded, int usageRows, int lineageRows,
                         int deadLetters, long filteredRows, int attempts, LocalDateTime startedTime,
                         LocalDateTime committedTime) {
        this.batchId = Objects.requireNonNull(batchId, "batchId");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.records = records;
        this.succeeded = succeeded;
        this.skipped = skipped;
        this.failed = failed;
        this.degraded = degraded;
        this.usageRows = usageRows;
        this.lineageRows = lineageRows;
        this.deadLetters = deadLetters;
        this.filteredRows = filteredRows;
        this.attempts = attempts;
        this.startedTime = startedTime;
        this.committedTime = committedTime;
    }

    public String getBatchId() {
        return batchId;
    }

    public BatchKind getKind() {
        return kind;
    }

    public LocalDateTime getRangeStart() {
        return rangeStart;
    }

    public LocalDateTime getRangeEnd() {
        return rangeEnd;
    }

    public int getRecords() {
        return records;
    }

    public int getSucceeded() {
        return succeeded;
    }

    public int getSkipped() {
        return skipped;
    }

    public int getFailed() {
        return failed;
    }

    public int getDegraded() {
        return degraded;
    }

    public int getUsageRows() {
        return usageRows;
    }

    public int getLineageRows() {
        return lineageRows;
    }

    public int getDeadLetters() {
        return deadLetters;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public int getAttempts() {
        return attempts;
    }

    public LocalDateTime getStartedTime() {
        return startedTime;
    }

    public LocalDateTime getCommittedTime() {
        return committedTime;
    }
}
