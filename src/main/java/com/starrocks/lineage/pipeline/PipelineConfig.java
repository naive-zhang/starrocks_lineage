package com.starrocks.lineage.pipeline;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Immutable pipeline configuration; build with {@link #builder(String, String)}.
 */
public final class PipelineConfig {

    private final String pipelineName;
    private final String owner;
    private final int pageSize;
    private final int maxRecordsPerBatch;
    private final Duration sliceSize;
    private final int maxEmptySlicesPerBatch;
    private final Duration lateArrival;
    private final Duration reconcileInterval;
    private final Duration reconcileWindow;
    private final Duration labelRetention;
    private final int alertAfterAttempts;
    private final Duration leaseTtl;
    private final int analyzerThreads;
    private final LocalDateTime initialStart;

    private PipelineConfig(Builder b) {
        this.pipelineName = requireName(b.pipelineName, "pipelineName");
        this.owner = requireName(b.owner, "owner");
        if (b.pageSize < 1 || b.maxRecordsPerBatch < b.pageSize) {
            throw new IllegalArgumentException("need 1 <= pageSize <= maxRecordsPerBatch");
        }
        if (b.analyzerThreads < 1) {
            throw new IllegalArgumentException("analyzerThreads must be >= 1");
        }
        if (b.alertAfterAttempts < 1 || b.maxEmptySlicesPerBatch < 1) {
            throw new IllegalArgumentException("alertAfterAttempts and maxEmptySlicesPerBatch must be >= 1");
        }
        requirePositive(b.sliceSize, "sliceSize");
        requirePositive(b.leaseTtl, "leaseTtl");
        requirePositive(b.reconcileInterval, "reconcileInterval");
        requirePositive(b.reconcileWindow, "reconcileWindow");
        requirePositive(b.labelRetention, "labelRetention");
        if (b.lateArrival.isNegative()) {
            throw new IllegalArgumentException("lateArrival must be >= 0");
        }
        if (Duration.ofHours(1).toNanos() % b.sliceSize.toNanos() != 0 || b.sliceSize.getNano() != 0) {
            throw new IllegalArgumentException("sliceSize must be whole seconds and divide one hour, e.g. 1m, 5m, "
                    + "10m, 15m, 30m, 1h");
        }
        this.pageSize = b.pageSize;
        this.maxRecordsPerBatch = b.maxRecordsPerBatch;
        this.sliceSize = b.sliceSize;
        this.maxEmptySlicesPerBatch = b.maxEmptySlicesPerBatch;
        this.lateArrival = b.lateArrival;
        this.reconcileInterval = b.reconcileInterval;
        this.reconcileWindow = b.reconcileWindow;
        this.labelRetention = b.labelRetention;
        this.alertAfterAttempts = b.alertAfterAttempts;
        this.leaseTtl = b.leaseTtl;
        this.analyzerThreads = b.analyzerThreads;
        this.initialStart = alignToSlice(Objects.requireNonNull(b.initialStart, "initialStart"), b.sliceSize);
    }

    private static void requirePositive(Duration d, String what) {
        if (d == null || d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(what + " must be > 0");
        }
    }

    private static String requireName(String v, String what) {
        if (v == null || v.trim().isEmpty() || v.length() > 64) {
            throw new IllegalArgumentException(what + " must be 1..64 characters");
        }
        return v;
    }

    /** @return {@code t} rounded down to a slice boundary (slices are aligned on the hour) */
    static LocalDateTime alignToSlice(LocalDateTime t, Duration sliceSize) {
        LocalDateTime hour = t.truncatedTo(ChronoUnit.HOURS);
        long intoHour = Duration.between(hour, t).getSeconds();
        long slice = sliceSize.getSeconds();
        return hour.plusSeconds(intoHour - intoHour % slice);
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

    /** @return upper bound of records of one LATE / REPAIR batch (a SLICE is always processed whole) */
    public int getMaxRecordsPerBatch() {
        return maxRecordsPerBatch;
    }

    public Duration getSliceSize() {
        return sliceSize;
    }

    /** @return how many consecutive empty slices are committed together */
    public int getMaxEmptySlicesPerBatch() {
        return maxEmptySlicesPerBatch;
    }

    public Duration getLateArrival() {
        return lateArrival;
    }

    public Duration getReconcileInterval() {
        return reconcileInterval;
    }

    public Duration getReconcileWindow() {
        return reconcileWindow;
    }

    /**
     * @return how long StarRocks is assumed to remember a load label; must be below the FE
     *         {@code label_keep_max_second} (3 days by default)
     */
    public Duration getLabelRetention() {
        return labelRetention;
    }

    public int getAlertAfterAttempts() {
        return alertAfterAttempts;
    }

    public Duration getLeaseTtl() {
        return leaseTtl;
    }

    public int getAnalyzerThreads() {
        return analyzerThreads;
    }

    /** @return start of the coverage when no checkpoint exists yet, aligned to a slice boundary */
    public LocalDateTime getInitialStart() {
        return initialStart;
    }

    /**
     * Builder with production defaults: 5-minute slices, records older than 5 minutes (the audit loader plugin
     * flushes in batches), reconciliation of the last 24 hours every 15 minutes, labels trusted for 2 days (FE keeps
     * them 3), alert after 5 failed attempts, 10 minutes lease, one analysis thread per core.
     */
    public static final class Builder {
        private final String pipelineName;
        private final String owner;
        private int pageSize = 5_000;
        private int maxRecordsPerBatch = 200_000;
        private Duration sliceSize = Duration.ofMinutes(5);
        private int maxEmptySlicesPerBatch = 288;
        private Duration lateArrival = Duration.ofMinutes(5);
        private Duration reconcileInterval = Duration.ofMinutes(15);
        private Duration reconcileWindow = Duration.ofHours(24);
        private Duration labelRetention = Duration.ofDays(2);
        private int alertAfterAttempts = 5;
        private Duration leaseTtl = Duration.ofMinutes(10);
        private int analyzerThreads = Runtime.getRuntime().availableProcessors();
        private LocalDateTime initialStart = LocalDateTime.now().truncatedTo(ChronoUnit.DAYS).minusDays(1);

        private Builder(String pipelineName, String owner) {
            this.pipelineName = pipelineName;
            this.owner = owner;
        }

        public Builder pageSize(int v) {
            this.pageSize = v;
            return this;
        }

        public Builder maxRecordsPerBatch(int v) {
            this.maxRecordsPerBatch = v;
            return this;
        }

        public Builder sliceSize(Duration v) {
            this.sliceSize = Objects.requireNonNull(v);
            return this;
        }

        public Builder maxEmptySlicesPerBatch(int v) {
            this.maxEmptySlicesPerBatch = v;
            return this;
        }

        public Builder lateArrival(Duration v) {
            this.lateArrival = Objects.requireNonNull(v);
            return this;
        }

        public Builder reconcileInterval(Duration v) {
            this.reconcileInterval = Objects.requireNonNull(v);
            return this;
        }

        public Builder reconcileWindow(Duration v) {
            this.reconcileWindow = Objects.requireNonNull(v);
            return this;
        }

        public Builder labelRetention(Duration v) {
            this.labelRetention = Objects.requireNonNull(v);
            return this;
        }

        public Builder alertAfterAttempts(int v) {
            this.alertAfterAttempts = v;
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

        /** Where the coverage starts when no checkpoint exists yet (rounded down to a slice boundary). */
        public Builder initialStart(LocalDateTime v) {
            this.initialStart = Objects.requireNonNull(v);
            return this;
        }

        public PipelineConfig build() {
            return new PipelineConfig(this);
        }
    }
}
