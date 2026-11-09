package com.starrocks.lineage.model;

/**
 * Outcome of analysing one audit record.
 */
public enum ParseStatus {
    /** Lineage and/or usage were extracted. */
    SUCCESS,
    /** {@code state = 'ERR'} or {@code errorCode} non-zero. */
    SKIPPED_FAILED_QUERY,
    /** SHOW / DESC / SET / USE / ADMIN / EXPLAIN / KILL ... */
    SKIPPED_MANAGEMENT,
    /** Empty after comment removal. */
    SKIPPED_EMPTY,
    /** Valid SQL whose statement type is out of scope (e.g. CREATE TABLE without AS SELECT). */
    SKIPPED_UNSUPPORTED,
    /** The official grammar rejected the statement. */
    PARSE_ERROR,
    /** The statement parsed but semantic resolution failed (unknown table without metadata, ...). */
    ANALYSIS_ERROR;

    public boolean isSkipped() {
        return name().startsWith("SKIPPED");
    }

    public boolean isError() {
        return this == PARSE_ERROR || this == ANALYSIS_ERROR;
    }
}
