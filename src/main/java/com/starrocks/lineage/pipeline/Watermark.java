package com.starrocks.lineage.pipeline;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Objects;

/**
 * Position in {@code starrocks_audit_tbl__}: the {@code (timestamp, queryId)} of the last fully processed record.
 * {@code queryId} breaks ties between records of the same timestamp, so the order is total and a page boundary
 * never skips or repeats a record.
 */
public final class Watermark implements Comparable<Watermark> {

    private static final Comparator<Watermark> ORDER =
            Comparator.comparing(Watermark::getTimestamp).thenComparing(Watermark::getQueryId);

    private final LocalDateTime timestamp;
    private final String queryId;

    public Watermark(LocalDateTime timestamp, String queryId) {
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
        this.queryId = Objects.requireNonNull(queryId, "queryId");
    }

    /** @return a watermark before every record of {@code start} and later */
    public static Watermark startingAt(LocalDateTime start) {
        return new Watermark(start.minusNanos(1000), "");
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getQueryId() {
        return queryId;
    }

    @Override
    public int compareTo(Watermark o) {
        return ORDER.compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Watermark)) {
            return false;
        }
        Watermark that = (Watermark) o;
        return timestamp.equals(that.timestamp) && queryId.equals(that.queryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, queryId);
    }

    @Override
    public String toString() {
        return timestamp + "/" + queryId;
    }
}
