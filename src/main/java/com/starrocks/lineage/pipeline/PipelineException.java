package com.starrocks.lineage.pipeline;

/**
 * Failure of a pipeline step (read, load, merge, checkpoint). The current cycle is abandoned without advancing
 * the watermark; the next cycle re-reads the same records and the idempotent sinks make the retry exactly-once.
 */
public class PipelineException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PipelineException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineException(String message) {
        super(message);
    }
}
