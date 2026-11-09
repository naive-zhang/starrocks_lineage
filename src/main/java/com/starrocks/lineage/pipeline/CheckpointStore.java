package com.starrocks.lineage.pipeline;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Durable watermark, pending batch intent and single-writer lease of a pipeline.
 *
 * <p><b>Batch intent.</b> Before a batch is written to any sink, its id and end watermark are persisted
 * ({@link #savePending}). A crash or failure after that point leaves the intent in place, and the next cycle replays
 * <i>exactly</i> the same record range under the same batch id, which is what makes the label-based / batch-guarded
 * sinks exactly-once. {@link #commit} advances the watermark and clears the intent in one write.</p>
 */
public interface CheckpointStore {

    /** @return the checkpoint of the pipeline, empty before the first lease */
    Optional<Checkpoint> load(String pipeline);

    /**
     * Takes or renews the lease; creates the checkpoint at {@code initial} when none exists.
     *
     * @return {@code false} when another owner holds a lease that has not expired
     */
    boolean tryAcquireLease(String pipeline, String owner, Duration ttl, Watermark initial);

    /** Persists the intent of the batch about to be written (watermark unchanged). */
    void savePending(String pipeline, String owner, Duration ttl, PendingBatch pending);

    /**
     * Advances the watermark, clears the pending intent and renews the lease.
     *
     * @throws PipelineException when the lease was lost or the write failed
     */
    void commit(String pipeline, String owner, Duration ttl, Watermark watermark, long processedRows);

    /**
     * Stored checkpoint.
     */
    final class Checkpoint {
        private final Watermark watermark;
        private final PendingBatch pending;
        private final long processedRows;

        public Checkpoint(Watermark watermark, PendingBatch pending, long processedRows) {
            this.watermark = Objects.requireNonNull(watermark, "watermark");
            this.pending = pending;
            this.processedRows = processedRows;
        }

        public Watermark getWatermark() {
            return watermark;
        }

        public Optional<PendingBatch> getPending() {
            return Optional.ofNullable(pending);
        }

        public long getProcessedRows() {
            return processedRows;
        }
    }

    /**
     * Intent of a batch: records in {@code (watermark, end]} under {@code batchId}.
     */
    final class PendingBatch {
        private final String batchId;
        private final Watermark end;

        public PendingBatch(String batchId, Watermark end) {
            this.batchId = Objects.requireNonNull(batchId, "batchId");
            this.end = Objects.requireNonNull(end, "end");
        }

        public String getBatchId() {
            return batchId;
        }

        public Watermark getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PendingBatch)) {
                return false;
            }
            PendingBatch p = (PendingBatch) o;
            return batchId.equals(p.batchId) && end.equals(p.end);
        }

        @Override
        public int hashCode() {
            return Objects.hash(batchId, end);
        }

        @Override
        public String toString() {
            return batchId + "@" + end;
        }
    }
}
