package com.starrocks.lineage.pipeline;

/**
 * A pending batch cannot be resumed safely without an operator decision, e.g. a load step was in flight longer ago
 * than the StarRocks label retention and the FE no longer knows whether it committed.
 * Resolved with {@link AuditLogSyncPipeline#resolveBlockedStep(AuditLogSyncPipeline.StepResolution)} or
 * {@link AuditLogSyncPipeline#skipPendingBatch(String)}.
 */
public class BatchBlockedException extends PipelineException {

    private static final long serialVersionUID = 1L;

    public BatchBlockedException(String message) {
        super(message, null, false);
    }
}
