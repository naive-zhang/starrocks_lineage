package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.ParseStatus;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * Result of one pipeline cycle.
 */
public final class CycleReport {

    /**
     * Cycle outcome.
     */
    public enum Outcome {
        /** A batch was written and the watermark advanced. */
        COMMITTED,
        /** Nothing new to process. */
        IDLE,
        /** Another instance holds the lease. */
        LEASE_NOT_ACQUIRED
    }

    private final Outcome outcome;
    private final String batchId;
    private final int records;
    private final Map<ParseStatus, Integer> statuses;
    private final int usageRows;
    private final int lineageRows;
    private final int newUsers;
    private final Watermark watermark;
    private final boolean replay;

    CycleReport(Outcome outcome, String batchId, int records, Map<ParseStatus, Integer> statuses, int usageRows,
                int lineageRows, int newUsers, Watermark watermark, boolean replay) {
        this.outcome = outcome;
        this.batchId = batchId;
        this.records = records;
        Map<ParseStatus, Integer> s = new EnumMap<>(ParseStatus.class);
        s.putAll(statuses);
        this.statuses = Collections.unmodifiableMap(s);
        this.usageRows = usageRows;
        this.lineageRows = lineageRows;
        this.newUsers = newUsers;
        this.watermark = watermark;
        this.replay = replay;
    }

    static CycleReport idle(Watermark watermark) {
        return new CycleReport(Outcome.IDLE, null, 0, Collections.emptyMap(), 0, 0, 0, watermark, false);
    }

    static CycleReport leaseNotAcquired() {
        return new CycleReport(Outcome.LEASE_NOT_ACQUIRED, null, 0, Collections.emptyMap(), 0, 0, 0, null, false);
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public Optional<String> getBatchId() {
        return Optional.ofNullable(batchId);
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

    public int getNewUsers() {
        return newUsers;
    }

    public Optional<Watermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    /** @return {@code true} when the cycle replayed a pending batch of a failed cycle */
    public boolean isReplay() {
        return replay;
    }

    @Override
    public String toString() {
        return "CycleReport{" + outcome + (batchId == null ? "" : ", batch=" + batchId) + ", records=" + records
                + ", statuses=" + statuses + ", usageRows=" + usageRows + ", lineageRows=" + lineageRows
                + ", newUsers=" + newUsers + ", watermark=" + watermark + (replay ? ", replay" : "") + '}';
    }
}
