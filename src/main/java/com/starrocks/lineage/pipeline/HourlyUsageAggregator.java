package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnUsageStat;
import com.starrocks.lineage.model.SqlParseResult;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory hourly pre-aggregation of column usage, so that millions of audited queries become a few thousand rows
 * per flush instead of one tiny load per query (which would create many small rowset versions on the tablets).
 *
 * <p><b>Key.</b> {@code (statHour, column, user)} compared with full {@code equals} on every component - no
 * composite numeric hash is ever used as identity, so two different keys whose hash codes collide (e.g. columns
 * {@code "Aa"} and {@code "BB"}) stay separate rows; {@link ConcurrentHashMap} only uses the hash for bucketing.</p>
 *
 * <p><b>Concurrency.</b> Analysis threads call {@link #add} concurrently: they share the read lock and update their
 * key with an atomic {@code compute}. {@link #drain()} takes the write lock, swaps the map and therefore returns a
 * consistent snapshot: every {@code add} is either completely in the drained rows or completely in the next
 * window, never split.</p>
 */
public final class HourlyUsageAggregator {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile ConcurrentMap<Key, Counters> current = new ConcurrentHashMap<>();

    /**
     * Adds the usage of one analysed query.
     *
     * @return number of (column) entries added; {@code 0} when the result carries no usage
     */
    public int add(AuditLogRecord record, SqlParseResult result) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(result, "result");
        if (!result.isSuccess() || result.getColumnUsages().isEmpty()) {
            return 0;
        }
        LocalDateTime hour = record.getTimestamp().truncatedTo(ChronoUnit.HOURS);
        String user = record.effectiveUser();
        long queryTime = Math.max(0L, record.getQueryTime());
        long scanBytes = Math.max(0L, record.getScanBytes());
        lock.readLock().lock();
        try {
            ConcurrentMap<Key, Counters> map = current;
            for (ColumnUsageStat stat : result.getColumnUsages().values()) {
                if (!stat.isAccessed()) {
                    continue;
                }
                map.compute(new Key(hour, stat.getColumn(), user), (k, c) -> {
                    Counters counters = c == null ? new Counters() : c;
                    counters.add(stat, scanBytes, queryTime, record.getTimestamp());
                    return counters;
                });
            }
        } finally {
            lock.readLock().unlock();
        }
        return result.getColumnUsages().size();
    }

    /** @return number of distinct {@code (hour, column, user)} keys currently held */
    public int size() {
        return current.size();
    }

    /**
     * Atomically takes every pre-aggregated row and resets the aggregator.
     *
     * @return rows sorted by hour, column and user (deterministic load files)
     */
    public List<HourlyUsageRow> drain() {
        ConcurrentMap<Key, Counters> snapshot;
        lock.writeLock().lock();
        try {
            snapshot = current;
            current = new ConcurrentHashMap<>();
        } finally {
            lock.writeLock().unlock();
        }
        List<HourlyUsageRow> rows = new ArrayList<>(snapshot.size());
        for (Map.Entry<Key, Counters> e : snapshot.entrySet()) {
            Key k = e.getKey();
            Counters c = e.getValue();
            rows.add(new HourlyUsageRow(k.hour, k.column, k.user, c.access, c.select, c.where, c.join, c.groupBy,
                    c.orderBy, c.scanBytes, c.queryTimeSum, c.maxQueryTime, c.lastAccess));
        }
        rows.sort(Comparator.comparing(HourlyUsageRow::getStatHour)
                .thenComparing(HourlyUsageRow::getColumn)
                .thenComparing(HourlyUsageRow::getUserName));
        return rows;
    }

    private static final class Key {
        private final LocalDateTime hour;
        private final ColumnIdentifier column;
        private final String user;
        private final int hash;

        Key(LocalDateTime hour, ColumnIdentifier column, String user) {
            this.hour = hour;
            this.column = column;
            this.user = user;
            this.hash = Objects.hash(hour, column, user);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key k = (Key) o;
            return hour.equals(k.hour) && column.equals(k.column) && user.equals(k.user);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /** Mutated only inside {@code ConcurrentHashMap.compute}, which serialises updates of one key. */
    private static final class Counters {
        private long access;
        private long select;
        private long where;
        private long join;
        private long groupBy;
        private long orderBy;
        private long scanBytes;
        private long queryTimeSum;
        private long maxQueryTime;
        private LocalDateTime lastAccess;

        void add(ColumnUsageStat stat, long queryScanBytes, long queryTime, LocalDateTime at) {
            access += stat.getAccessQueryCnt();
            select += stat.getSelectClauseCnt();
            where += stat.getWhereClauseCnt();
            join += stat.getJoinClauseCnt();
            groupBy += stat.getGroupByCnt();
            orderBy += stat.getOrderByCnt();
            scanBytes += queryScanBytes;
            queryTimeSum += queryTime;
            maxQueryTime = Math.max(maxQueryTime, queryTime);
            if (lastAccess == null || at.isAfter(lastAccess)) {
                lastAccess = at;
            }
        }
    }
}
