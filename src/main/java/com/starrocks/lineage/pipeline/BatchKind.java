package com.starrocks.lineage.pipeline;

/**
 * What a batch processes.
 */
public enum BatchKind {
    /** The audit records of one fixed time slice; committing it advances the watermark. */
    SLICE(true),
    /** Records of committed slices that were not members of any batch (late arrivals), found by reconciliation. */
    LATE(false),
    /** Dead-lettered records re-analysed after a fix. */
    REPAIR(false),
    /** Whole days recomputed from the batch membership (after an analyzer change). */
    REBUILD(false);

    private final boolean advancesWatermark;

    BatchKind(boolean advancesWatermark) {
        this.advancesWatermark = advancesWatermark;
    }

    public boolean advancesWatermark() {
        return advancesWatermark;
    }
}
