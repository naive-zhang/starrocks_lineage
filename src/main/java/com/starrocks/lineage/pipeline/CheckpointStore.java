package com.starrocks.lineage.pipeline;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * Durable state of a pipeline: committed watermark, coverage start, the pending batch with its step progress, the
 * single-writer lease and the last reconciliation time.
 *
 * <p><b>Fencing.</b> Every mutating call verifies that the caller still owns the lease and fails with
 * {@link LeaseLostException} otherwise; {@link #verifyLease} is called before every write step of a batch, so an
 * instance that was paused while another one took over stops before its next write. Every write is read back.</p>
 */
public interface CheckpointStore {

    Optional<Checkpoint> load(String pipeline);

    /**
     * Takes or renews the lease; creates the checkpoint ({@code committedUntil = coverageStart = initialStart}) the
     * first time.
     *
     * @return {@code false} when another owner holds a lease that has not expired
     */
    boolean tryAcquireLease(String pipeline, String owner, Duration ttl, LocalDateTime initialStart);

    /** Renews the lease; {@link LeaseLostException} when another owner took it. */
    void verifyLease(String pipeline, String owner, Duration ttl);

    /** Gives the lease up (shutdown) so another instance can take over at once. */
    void releaseLease(String pipeline, String owner);

    /** Declares a new batch (step {@link BatchStep#INTENT}); fails when another batch is pending. */
    void savePending(String pipeline, String owner, Duration ttl, PendingBatch pending);

    /** Records that {@code step} of the pending batch {@code batchId} completed. */
    void advanceStep(String pipeline, String owner, Duration ttl, String batchId, BatchStep step);

    /** Resets the in-flight timer of the pending batch (operator decision after a blocked step). */
    void restartPendingClock(String pipeline, String owner, Duration ttl, String batchId);

    /** Counts a failed attempt of the pending batch and keeps the error for operators. */
    void recordFailure(String pipeline, String owner, Duration ttl, String batchId, String error);

    /**
     * Completes the pending batch {@code batchId}: clears it, adds {@code records} and, when
     * {@code newCommittedUntil} is not {@code null}, moves the watermark there. The watermark never moves
     * backwards.
     */
    void commit(String pipeline, String owner, Duration ttl, String batchId, LocalDateTime newCommittedUntil,
                long records);

    /**
     * Completes the pending batch {@code batchId} and declares {@code next} in the same atomic checkpoint write (a
     * chain of batches that must not stop half-way: the hours of a rebuilt day). The watermark does not move.
     */
    void commitAndDeclare(String pipeline, String owner, Duration ttl, String batchId, long records,
                          PendingBatch next);

    /** Stores the time of the last reconciliation that found no late record. */
    void markReconciled(String pipeline, String owner, Duration ttl, LocalDateTime at);

    /**
     * Stored checkpoint.
     */
    final class Checkpoint {
        private final String epoch;
        private final LocalDateTime committedUntil;
        private final LocalDateTime coverageStart;
        private final PendingBatch pending;
        private final long processedRows;
        private final LocalDateTime lastReconcile;
        private final String owner;
        private final LocalDateTime leaseExpire;

        public Checkpoint(String epoch, LocalDateTime committedUntil, LocalDateTime coverageStart,
                          PendingBatch pending, long processedRows, LocalDateTime lastReconcile, String owner,
                          LocalDateTime leaseExpire) {
            this.epoch = Objects.requireNonNull(epoch, "epoch");
            this.committedUntil = Objects.requireNonNull(committedUntil, "committedUntil");
            this.coverageStart = Objects.requireNonNull(coverageStart, "coverageStart");
            this.pending = pending;
            this.processedRows = processedRows;
            this.lastReconcile = lastReconcile;
            this.owner = owner;
            this.leaseExpire = leaseExpire;
        }

        /**
         * @return random identity of this checkpoint generation, created with the checkpoint. Slice batch ids include
         *         it: a pipeline that is reset and started again under the same name must not reproduce batch ids
         *         (and load labels) StarRocks still remembers, or every load would be dropped as a duplicate
         */
        public String getEpoch() {
            return epoch;
        }

        /** @return every record with {@code timestamp < committedUntil} belongs to a committed slice */
        public LocalDateTime getCommittedUntil() {
            return committedUntil;
        }

        /** @return start of the audit history this pipeline is responsible for */
        public LocalDateTime getCoverageStart() {
            return coverageStart;
        }

        public Optional<PendingBatch> getPending() {
            return Optional.ofNullable(pending);
        }

        public long getProcessedRows() {
            return processedRows;
        }

        public Optional<LocalDateTime> getLastReconcile() {
            return Optional.ofNullable(lastReconcile);
        }

        public String getOwner() {
            return owner;
        }

        public LocalDateTime getLeaseExpire() {
            return leaseExpire;
        }
    }

    /**
     * A declared batch and its progress.
     */
    final class PendingBatch {
        private final String batchId;
        private final BatchKind kind;
        private final LocalDateTime rangeStart;
        private final LocalDateTime rangeEnd;
        private final BatchStep step;
        private final LocalDateTime since;
        private final int attempts;
        private final String lastError;

        public PendingBatch(String batchId, BatchKind kind, LocalDateTime rangeStart, LocalDateTime rangeEnd,
                            BatchStep step, LocalDateTime since, int attempts, String lastError) {
            this.batchId = Objects.requireNonNull(batchId, "batchId");
            this.kind = Objects.requireNonNull(kind, "kind");
            this.rangeStart = Objects.requireNonNull(rangeStart, "rangeStart");
            this.rangeEnd = Objects.requireNonNull(rangeEnd, "rangeEnd");
            this.step = Objects.requireNonNull(step, "step");
            this.since = Objects.requireNonNull(since, "since");
            this.attempts = attempts;
            this.lastError = lastError;
        }

        /** A new intent. */
        public static PendingBatch declare(String batchId, BatchKind kind, LocalDateTime start, LocalDateTime end,
                                           LocalDateTime now) {
            return new PendingBatch(batchId, kind, start, end, BatchStep.INTENT, now, 0, null);
        }

        public PendingBatch withStep(BatchStep newStep) {
            return new PendingBatch(batchId, kind, rangeStart, rangeEnd, newStep, since, attempts, lastError);
        }

        public PendingBatch withFailure(String error) {
            return new PendingBatch(batchId, kind, rangeStart, rangeEnd, step, since, attempts + 1, error);
        }

        public PendingBatch withSince(LocalDateTime newSince) {
            return new PendingBatch(batchId, kind, rangeStart, rangeEnd, step, newSince, attempts, lastError);
        }

        public String getBatchId() {
            return batchId;
        }

        public BatchKind getKind() {
            return kind;
        }

        /** @return audit time range of the batch: slice, late window, repair / rebuild range */
        public LocalDateTime getRangeStart() {
            return rangeStart;
        }

        public LocalDateTime getRangeEnd() {
            return rangeEnd;
        }

        /** @return the last completed step */
        public BatchStep getStep() {
            return step;
        }

        /** @return when the batch was declared (or its clock restarted by an operator) */
        public LocalDateTime getSince() {
            return since;
        }

        public int getAttempts() {
            return attempts;
        }

        public Optional<String> getLastError() {
            return Optional.ofNullable(lastError);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PendingBatch)) {
                return false;
            }
            PendingBatch p = (PendingBatch) o;
            return batchId.equals(p.batchId) && kind == p.kind && rangeStart.equals(p.rangeStart)
                    && rangeEnd.equals(p.rangeEnd) && step == p.step && since.equals(p.since)
                    && attempts == p.attempts && Objects.equals(lastError, p.lastError);
        }

        @Override
        public int hashCode() {
            return Objects.hash(batchId, kind, rangeStart, rangeEnd, step, since, attempts);
        }

        @Override
        public String toString() {
            return kind + " " + batchId + " [" + rangeStart + ", " + rangeEnd + ") step=" + step + " since=" + since
                    + " attempts=" + attempts + (lastError == null ? "" : " lastError=" + lastError);
        }
    }
}
