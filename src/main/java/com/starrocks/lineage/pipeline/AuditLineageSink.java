package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

/**
 * Storage of a pipeline. Every write method is one atomic operation identified by a label derived from the batch id
 * ({@link #label}); replaying it is either a no-op (label already used) or an idempotent upsert. The two additive
 * writes ({@link #writeUsage}, {@link #insertDaily}) are the ones whose replay after the StarRocks label retention
 * must be guarded by the caller with {@link #loadState}.
 */
public interface AuditLineageSink {

    /** @return the label of {@code step} of batch {@code batchId} (also used to query its state) */
    String label(String batchId, String step);

    /** Upserts the membership of the batch (PRIMARY KEY: idempotent). */
    StreamLoadClient.Result writeMembers(String batchId, LocalDateTime batchTime, List<AuditLogRecord> records);

    /** Upserts dead letters (PRIMARY KEY: idempotent). */
    StreamLoadClient.Result writeDeadLetters(String batchId, String tag, List<DeadLetter> letters);

    /** Adds the users of the batch to the dictionary (AGGREGATE, first_seen MIN: idempotent). */
    StreamLoadClient.Result writeUserDictionary(String batchId, List<HourlyUsageRow> rows);

    /**
     * Adds hourly usage (SUM: <b>additive</b>). Rows StarRocks rejects are isolated: a LOAD_FILTERED dead letter is
     * written and the remaining rows are loaded under the same label.
     */
    StreamLoadClient.Result writeUsage(String batchId, List<HourlyUsageRow> rows);

    /** Upserts the lineage contribution of the batch (PRIMARY KEY: idempotent). */
    StreamLoadClient.Result writeBatchLineage(String batchId, LocalDateTime batchTime, List<LineageRow> rows);

    /**
     * REBUILD: records every edge with daily rows in {@code [fromDay, toDay)} as refresh-only rows of the batch, so
     * edges whose contribution disappears are refreshed too (labelled INSERT).
     */
    void recordEdgesForRefresh(String batchId, LocalDateTime batchTime, LocalDateTime fromDay, LocalDateTime toDay);

    /** Adds the batch contribution to the daily lineage fact table (labelled INSERT, <b>additive</b>). */
    void insertDaily(String batchId);

    /** Recomputes the lineage table for every edge of the batch from the daily facts (idempotent). */
    void refreshLineage(String batchId);

    /** REBUILD: deletes hourly usage of {@code [from, to)} (idempotent). */
    void purgeUsage(LocalDateTime from, LocalDateTime to);

    /** REBUILD: deletes daily lineage of {@code [fromDay, toDay)} (idempotent). */
    void purgeLineageDaily(LocalDateTime fromDay, LocalDateTime toDay);

    /**
     * Dead letters of the pipeline.
     *
     * @param repairBatchId only those reserved by this repair batch, or {@code null}
     */
    List<DeadLetter> deadLetters(Set<DeadLetter.Status> statuses, Set<DeadLetter.Kind> kinds, LocalDateTime from,
                                 LocalDateTime to, String repairBatchId, int limit);

    /**
     * Dead letters (any kind, any status) currently attributed to batch {@code batchId} whose audit time is in
     * {@code [from, to)}: the records a resumed batch must not analyse again.
     */
    List<DeadLetter> deadLettersOfBatch(String batchId, LocalDateTime from, LocalDateTime to);

    /** State of a label in the FE. */
    StreamLoadClient.LoadState loadState(String label);

    /** Upserts the batch log. */
    void writeBatchLog(BatchLogEntry entry);

    /** @return user ids shared by several user names (should always be empty) */
    List<Long> userIdCollisions();
}
