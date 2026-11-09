package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.SqlParseResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Batch pre-aggregation of lineage edges keyed by the edge identity ({@link ColumnLineageEdge#equals}: target,
 * source, lineage type - full equality, not a hash). Same concurrency model as {@link HourlyUsageAggregator}.
 */
public final class LineageAggregator {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile ConcurrentMap<ColumnLineageEdge, LineageRow> current = new ConcurrentHashMap<>();

    /**
     * @return number of edges added; {@code 0} when the result carries no lineage
     */
    public int add(AuditLogRecord record, SqlParseResult result) {
        Objects.requireNonNull(record, "record");
        if (!result.isSuccess() || result.getLineageEdges().isEmpty()
                || !result.getStatementType().isPresent()) {
            return 0;
        }
        lock.readLock().lock();
        try {
            ConcurrentMap<ColumnLineageEdge, LineageRow> map = current;
            for (ColumnLineageEdge edge : result.getLineageEdges()) {
                LineageRow row = new LineageRow(edge, result.getStatementType().get(), record.getDigest(),
                        record.getQueryId(), record.effectiveUser(), record.getTimestamp(), record.getTimestamp(), 1L);
                map.merge(edge, row, LineageRow::merge);
            }
        } finally {
            lock.readLock().unlock();
        }
        return result.getLineageEdges().size();
    }

    public int size() {
        return current.size();
    }

    /** @return the batch rows (sorted by edge id) and resets the aggregator atomically */
    public List<LineageRow> drain() {
        ConcurrentMap<ColumnLineageEdge, LineageRow> snapshot;
        lock.writeLock().lock();
        try {
            snapshot = current;
            current = new ConcurrentHashMap<>();
        } finally {
            lock.writeLock().unlock();
        }
        List<LineageRow> rows = new ArrayList<>(snapshot.values());
        rows.sort(Comparator.comparing(LineageRow::getEdgeId));
        return rows;
    }
}
