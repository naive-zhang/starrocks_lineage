package com.starrocks.lineage.pipeline;

/**
 * Failure of a pipeline step. The current batch is not committed; its intent and step progress stay in the
 * checkpoint and the next cycle resumes it.
 *
 * <p>{@link #isRetryable()} separates transient failures (network, FE / BE unavailable, a load still running) from
 * failures that will repeat identically (bad data that cannot be isolated, a lost lease, a blocked step). Neither
 * kind ever skips data automatically: a non-retryable failure is surfaced (metrics, logs, {@link CycleReport}) for an
 * operator decision.</p>
 */
public class PipelineException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final boolean retryable;

    public PipelineException(String message, Throwable cause) {
        this(message, cause, true);
    }

    public PipelineException(String message) {
        this(message, null, true);
    }

    public PipelineException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public static PipelineException nonRetryable(String message, Throwable cause) {
        return new PipelineException(message, cause, false);
    }

    public boolean isRetryable() {
        return retryable;
    }
}
