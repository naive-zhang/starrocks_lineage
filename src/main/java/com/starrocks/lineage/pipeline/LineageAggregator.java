package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.SqlParseResult;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Batch pre-aggregation of lineage keyed by {@code (day of the statement, edge identity)} - full equality, never a
 * hash. Same concurrency model as {@link HourlyUsageAggregator}.
 */
public final class LineageAggregator {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile ConcurrentMap<Key, LineageRow> current = new ConcurrentHashMap<>();

    /**
     * @return number of edges added; {@code 0} when the result carries no lineage
     */
    public int add(AuditLogRecord record, SqlParseResult result) {
        Objects.requireNonNull(record, "record");
        if (!result.isSuccess() || result.getLineageEdges().isEmpty()
                || !result.getStatementType().isPresent()) {
            return 0;
        }
        LocalDateTime day = record.getTimestamp().truncatedTo(ChronoUnit.DAYS);
        lock.readLock().lock();
        try {
            ConcurrentMap<Key, LineageRow> map = current;
            for (ColumnLineageEdge edge : result.getLineageEdges()) {
                LineageRow row = new LineageRow(day, edge, result.getStatementType().get(), record.getDigest(),
                        record.getQueryId(), record.effectiveUser(), record.getTimestamp(), record.getTimestamp(), 1L);
                map.merge(new Key(day, edge), row, LineageRow::merge);
            }
        } finally {
            lock.readLock().unlock();
        }
        return result.getLineageEdges().size();
    }

    public int size() {
        return current.size();
    }

    /** @return the batch rows (sorted by day and edge id) and resets the aggregator atomically */
    public List<LineageRow> drain() {
        ConcurrentMap<Key, LineageRow> snapshot;
        lock.writeLock().lock();
        try {
            snapshot = current;
            current = new ConcurrentHashMap<>();
        } finally {
            lock.writeLock().unlock();
        }
        List<LineageRow> rows = new ArrayList<>(snapshot.values());
        rows.sort(Comparator.comparing(LineageRow::getStatDay).thenComparing(LineageRow::getEdgeId));
        return rows;
    }

    private static final class Key {
        private final LocalDateTime day;
        private final ColumnLineageEdge edge;

        Key(LocalDateTime day, ColumnLineageEdge edge) {
            this.day = day;
            this.edge = edge;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key)) {
                return false;
            }
            Key k = (Key) o;
            return day.equals(k.day) && edge.equals(k.edge);
        }

        @Override
        public int hashCode() {
            return Objects.hash(day, edge);
        }
    }
}
