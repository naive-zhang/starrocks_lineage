package com.starrocks.lineage.pipeline;

/**
 * Steps of a batch, in execution order. The checkpoint stores the last <b>completed</b> step; a resumed batch starts
 * after it. Every step is a single atomic write (one labelled Stream Load, one labelled {@code INSERT}, one
 * idempotent statement), so a step is either entirely applied or not at all.
 */
public enum BatchStep {
    /** Intent saved; nothing written yet. */
    INTENT,
    /** SLICE / LATE: membership loaded. REPAIR: dead letters marked REPAIRING. REBUILD: old contribution purged. */
    PREPARE,
    /** Failed records written to the dead-letter table. */
    DEAD_LETTERS,
    /** Users of the batch written to the user dictionary. */
    DICTIONARY,
    /** Hourly usage loaded. */
    USAGE,
    /** Lineage contribution of the batch loaded (the exact edge set of the batch). */
    LINEAGE_STAGE,
    /** Contribution added to the daily lineage fact table (labelled INSERT). */
    LINEAGE_DAILY,
    /** Lineage table recomputed for every edge of the batch (idempotent). */
    REFRESH,
    /** REPAIR / REBUILD: dead letters resolved. */
    RESOLVE;

    public boolean isAfter(BatchStep other) {
        return ordinal() > other.ordinal();
    }

    public boolean isBefore(BatchStep other) {
        return ordinal() < other.ordinal();
    }
}
