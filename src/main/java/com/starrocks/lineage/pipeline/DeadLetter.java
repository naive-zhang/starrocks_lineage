package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * One row of {@code audit_lineage_dead_letter} (PRIMARY KEY {@code pipeline_name, query_id, kind}).
 *
 * <ul>
 *   <li>{@link Kind#PARSE_ERROR} / {@link Kind#ANALYSIS_ERROR}: a record that could not be analysed. The full audit
 *       record is kept, so it can be repaired later without the audit table.</li>
 *   <li>{@link Kind#LOAD_FILTERED}: rows rejected by a Stream Load (data quality); {@code query_id} is the load
 *       label and {@code detail} the StarRocks error-log URL. The good rows of the same load were kept.</li>
 *   <li>{@link Kind#BATCH_SKIPPED}: a pending batch skipped by an operator; {@code query_id} is the batch id.</li>
 * </ul>
 */
public final class DeadLetter {

    /**
     * Dead-letter kind.
     */
    public enum Kind {
        PARSE_ERROR(true),
        ANALYSIS_ERROR(true),
        LOAD_FILTERED(false),
        BATCH_SKIPPED(false);

        private final boolean repairable;

        Kind(boolean repairable) {
            this.repairable = repairable;
        }

        /** @return {@code true} when the dead letter holds an audit record that can be re-analysed */
        public boolean isRepairable() {
            return repairable;
        }
    }

    /**
     * Life cycle.
     */
    public enum Status {
        OPEN,
        REPAIRING,
        RESOLVED
    }

    private final String queryId;
    private final Kind kind;
    private final Status status;
    private final String batchId;
    private final String repairBatchId;
    private final LocalDateTime auditTime;
    private final String errorMessage;
    private final String detail;
    private final AuditLogRecord record;

    public DeadLetter(String queryId, Kind kind, Status status, String batchId, String repairBatchId,
                      LocalDateTime auditTime, String errorMessage, String detail, AuditLogRecord record) {
        this.queryId = Objects.requireNonNull(queryId, "queryId");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.status = Objects.requireNonNull(status, "status");
        this.batchId = Objects.requireNonNull(batchId, "batchId");
        this.repairBatchId = repairBatchId;
        this.auditTime = Objects.requireNonNull(auditTime, "auditTime");
        this.errorMessage = errorMessage == null ? "" : errorMessage;
        this.detail = detail;
        this.record = record;
        if (kind.isRepairable() && record == null) {
            throw new IllegalArgumentException("a " + kind + " dead letter needs its audit record");
        }
    }

    /** Dead letter of a record that failed to parse / analyse. */
    public static DeadLetter ofRecord(AuditLogRecord record, Kind kind, String batchId, String message) {
        return new DeadLetter(record.getQueryId(), kind, Status.OPEN, batchId, null, record.getTimestamp(), message,
                null, record);
    }

    /** @return a copy with another status / repair batch / message */
    public DeadLetter with(Status newStatus, String newRepairBatchId, String newMessage) {
        return new DeadLetter(queryId, kind, newStatus, batchId, newRepairBatchId, auditTime,
                newMessage == null ? errorMessage : newMessage, detail, record);
    }

    public String getQueryId() {
        return queryId;
    }

    public Kind getKind() {
        return kind;
    }

    public Status getStatus() {
        return status;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getRepairBatchId() {
        return repairBatchId;
    }

    public LocalDateTime getAuditTime() {
        return auditTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getDetail() {
        return detail;
    }

    /** @return the audit record (repairable kinds), else {@code null} */
    public AuditLogRecord getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return "DeadLetter{" + kind + " " + queryId + " " + status + " batch=" + batchId + " " + errorMessage + '}';
    }
}
