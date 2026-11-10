package com.starrocks.lineage.pipeline;

/**
 * Another instance holds the pipeline lease: this instance must stop writing immediately (fencing).
 */
public class LeaseLostException extends PipelineException {

    private static final long serialVersionUID = 1L;

    public LeaseLostException(String message) {
        super(message, null, false);
    }
}
