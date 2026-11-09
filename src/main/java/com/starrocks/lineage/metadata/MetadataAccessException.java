package com.starrocks.lineage.metadata;

/**
 * Infrastructure failure while reading metadata (connection refused, timeout, permission denied ...).
 * A missing table is <b>not</b> an error: it is reported as {@code Optional.empty()}.
 */
public class MetadataAccessException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MetadataAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
