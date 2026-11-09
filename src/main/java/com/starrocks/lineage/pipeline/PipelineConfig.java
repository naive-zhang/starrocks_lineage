package com.starrocks.lineage.pipeline;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Immutable pipeline configuration; build with {@link #builder(String, String)}.
 */
public final class PipelineConfig {

    private final String pipelineName;
    private final String owner;
    private final int pageSize;
    private final int maxRecordsPerCycle;
    private final Duration lateArrival;
    private final Duration leaseTtl;
    private final int analyzerThreads;
    private final LocalDateTime initialStart;

    private PipelineConfig(Builder b) {
        this.pipelineName = requireName(b.pipelineName, "pipelineName");
        this.owner = requireName(b.owner, "owner");
        if (b.pageSize < 1 || b.maxRecordsPerCycle < b.pageSize) {
            throw new IllegalArgumentException("need 1 <= pageSize <= maxRecordsPerCycle");
        }
        if (b.analyzerThreads < 1) {
            throw new IllegalArgumentException("analyzerThreads must be >= 1");
        }
        if (b.lateArrival.isNegative() || b.leaseTtl.isNegative() || b.leaseTtl.isZero()) {
            throw new IllegalArgumentException("lateArrival must be >= 0 and leaseTtl > 0");
        }
        this.pageSize = b.pageSize;
        this.maxRecordsPerCycle = b.maxRecordsPerCycle;
        this.lateArrival = b.lateArrival;
        this.leaseTtl = b.leaseTtl;
        this.analyzerThreads = b.analyzerThreads;
        this.initialStart = Objects.requireNonNull(b.initialStart, "initialStart");
    }

    private static String requireName(String v, String what) {
        if (v == null || v.trim().isEmpty() || v.length() > 64) {
            throw new IllegalArgumentException(what + " must be 1..64 characters");
        }
        return v;
    }

    /**
     * @param pipelineName checkpoint key, e.g. {@code audit_lineage}
     * @param owner        identity of this instance (host:pid), for the lease
     */
    public static Builder builder(String pipelineName, String owner) {
        return new Builder(pipelineName, owner);
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public String getOwner() {
        return owner;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getMaxRecordsPerCycle() {
        return maxRecordsPerCycle;
    }

    public Duration getLateArrival() {
        return lateArrival;
    }

    public Duration getLeaseTtl() {
        return leaseTtl;
    }

    public int getAnalyzerThreads() {
        return analyzerThreads;
    }

    public LocalDateTime getInitialStart() {
        return initialStart;
    }

    /**
     * Builder with production defaults: 5 000 records per page, 200 000 per cycle, 5 minutes late arrival
     * window (the audit loader plugin flushes in batches), 10 minutes lease, one analysis thread per core.
     */
    public static final class Builder {
        private final String pipelineName;
        private final String owner;
        private int pageSize = 5_000;
        private int maxRecordsPerCycle = 200_000;
        private Duration lateArrival = Duration.ofMinutes(5);
        private Duration leaseTtl = Duration.ofMinutes(10);
        private int analyzerThreads = Runtime.getRuntime().availableProcessors();
        private LocalDateTime initialStart = LocalDateTime.now().minusDays(1);

        private Builder(String pipelineName, String owner) {
            this.pipelineName = pipelineName;
            this.owner = owner;
        }

        public Builder pageSize(int v) {
            this.pageSize = v;
            return this;
        }

        public Builder maxRecordsPerCycle(int v) {
            this.maxRecordsPerCycle = v;
            return this;
        }

        public Builder lateArrival(Duration v) {
            this.lateArrival = Objects.requireNonNull(v);
            return this;
        }

        public Builder leaseTtl(Duration v) {
            this.leaseTtl = Objects.requireNonNull(v);
            return this;
        }

        public Builder analyzerThreads(int v) {
            this.analyzerThreads = v;
            return this;
        }

        /** Where to start when no checkpoint exists yet. */
        public Builder initialStart(LocalDateTime v) {
            this.initialStart = Objects.requireNonNull(v);
            return this;
        }

        public PipelineConfig build() {
            return new PipelineConfig(this);
        }
    }
}
