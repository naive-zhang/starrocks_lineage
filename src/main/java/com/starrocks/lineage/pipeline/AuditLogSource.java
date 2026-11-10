package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Reader of the audit table, alone or joined with the batch membership of a pipeline. Every method returns records
 * ordered by {@code (timestamp, queryId)} strictly after {@code after} (keyset pagination).
 */
public interface AuditLogSource {

    /** Records with {@code from <= timestamp < to}. */
    List<AuditLogRecord> fetchRange(LocalDateTime from, LocalDateTime to, Watermark after, int limit);

    /** Records that are members of batch {@code batchId} (exact replay of a batch). */
    List<AuditLogRecord> fetchMembers(String pipeline, String batchId, LocalDateTime from, LocalDateTime to,
                                      Watermark after, int limit);

    /** Records that are members of any batch of the pipeline (rebuild of a range). */
    List<AuditLogRecord> fetchProcessed(String pipeline, LocalDateTime from, LocalDateTime to, Watermark after,
                                        int limit);

    /** Records in the range that are members of no batch of the pipeline (late arrivals). */
    List<AuditLogRecord> fetchUnprocessed(String pipeline, LocalDateTime from, LocalDateTime to, int limit);

    /** @return number of membership rows of the pipeline in the range */
    long countMembers(String pipeline, LocalDateTime from, LocalDateTime to);

    /** @return number of those membership rows whose audit record still exists (audit retention check) */
    long countProcessed(String pipeline, LocalDateTime from, LocalDateTime to);
}
