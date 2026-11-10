package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.ParseStatus;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * Result of one pipeline cycle or operation.
 */
public final class CycleReport {

    /**
     * Cycle outcome.
     */
    public enum Outcome {
        /** A batch was written and committed. */
        COMMITTED,
        /** Nothing to process. */
        IDLE,
        /** Another instance holds the lease. */
        LEASE_NOT_ACQUIRED,
        /** The pending batch fails with a non-retryable error; an operator has to decide (see the message). */
        BLOCKED
    }

    private final Outcome outcome;
    private final BatchKind kind;
    private final String batchId;
    private final LocalDateTime rangeStart;
    private final LocalDateTime rangeEnd;
    private final int records;
    private final Map<ParseStatus, Integer> statuses;
    private final int usageRows;
    private final int lineageRows;
    private final int deadLetters;
    private final long filteredRows;
    private final boolean replay;
    private final String message;

    private CycleReport(Outcome outcome, BatchKind kind, String batchId, LocalDateTime rangeStart,
                        LocalDateTime rangeEnd, int records, Map<ParseStatus, Integer> statuses, int usageRows,
                        int lineageRows, int deadLetters, long filteredRows, boolean replay, String message) {
        this.outcome = outcome;
        this.kind = kind;
        this.batchId = batchId;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.records = records;
        Map<ParseStatus, Integer> s = new EnumMap<>(ParseStatus.class);
        s.putAll(statuses);
        this.statuses = Collections.unmodifiableMap(s);
        this.usageRows = usageRows;
        this.lineageRows = lineageRows;
        this.deadLetters = deadLetters;
        this.filteredRows = filteredRows;
        this.replay = replay;
        this.message = message;
    }

    static CycleReport committed(CheckpointStore.PendingBatch batch, int records, Map<ParseStatus, Integer> statuses,
                                 int usageRows, int lineageRows, int deadLetters, long filteredRows, boolean replay) {
        return new CycleReport(Outcome.COMMITTED, batch.getKind(), batch.getBatchId(), batch.getRangeStart(),
                batch.getRangeEnd(), records, statuses, usageRows, lineageRows, deadLetters, filteredRows, replay,
                null);
    }

    static CycleReport idle(String message) {
        return new CycleReport(Outcome.IDLE, null, null, null, null, 0, Collections.emptyMap(), 0, 0, 0, 0, false,
                message);
    }

    static CycleReport leaseNotAcquired() {
        return new CycleReport(Outcome.LEASE_NOT_ACQUIRED, null, null, null, null, 0, Collections.emptyMap(), 0, 0,
                0, 0, false, null);
    }

    static CycleReport blocked(CheckpointStore.PendingBatch batch, String message) {
        return new CycleReport(Outcome.BLOCKED, batch.getKind(), batch.getBatchId(), batch.getRangeStart(),
                batch.getRangeEnd(), 0, Collections.emptyMap(), 0, 0, 0, 0, true, message);
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public Optional<BatchKind> getKind() {
        return Optional.ofNullable(kind);
    }

    public Optional<String> getBatchId() {
        return Optional.ofNullable(batchId);
    }

    public Optional<LocalDateTime> getRangeStart() {
        return Optional.ofNullable(rangeStart);
    }

    public Optional<LocalDateTime> getRangeEnd() {
        return Optional.ofNullable(rangeEnd);
    }

    public int getRecords() {
        return records;
    }

    public int statusCount(ParseStatus status) {
        return statuses.getOrDefault(status, 0);
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

    /** @return {@code true} when the cycle resumed a batch declared by an earlier (failed) cycle */
    public boolean isReplay() {
        return replay;
    }

    public Optional<String> getMessage() {
        return Optional.ofNullable(message);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CycleReport{").append(outcome);
        if (batchId != null) {
            sb.append(", ").append(kind).append(' ').append(batchId).append(" [").append(rangeStart).append(", ")
                    .append(rangeEnd).append(')');
        }
        sb.append(", records=").append(records).append(", statuses=").append(statuses).append(", usageRows=")
                .append(usageRows).append(", lineageRows=").append(lineageRows).append(", deadLetters=")
                .append(deadLetters).append(", filteredRows=").append(filteredRows);
        if (replay) {
            sb.append(", replay");
        }
        if (message != null) {
            sb.append(", ").append(message);
        }
        return sb.append('}').toString();
    }
}
