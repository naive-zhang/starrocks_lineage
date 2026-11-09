package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Incremental reader of the audit table.
 */
public interface AuditLogSource {

    /**
     * @param after      exclusive lower bound
     * @param upperBound exclusive upper bound on {@code timestamp} (now minus the late-arrival window)
     * @param limit      page size
     * @return records strictly after {@code after} and before {@code upperBound}, ordered by
     *         {@code (timestamp, queryId)}
     * @throws PipelineException on read failure
     */
    List<AuditLogRecord> fetch(Watermark after, LocalDateTime upperBound, int limit);
}
