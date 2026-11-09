package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.ColumnIdentifier;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * One row of {@code audit_column_hourly_usage} as emitted by the pipeline: the subtotals of <b>one user</b> on
 * <b>one column</b> during <b>one hour</b> (load contract of 03_audit_column_hourly_usage.sql). The aggregate table
 * sums the counters and unions {@code to_bitmap(userId)} / {@code hll_hash(userName)} across users.
 */
public final class HourlyUsageRow {

    private final LocalDateTime statHour;
    private final ColumnIdentifier column;
    private final String userName;
    private final long accessQueryCnt;
    private final long selectClauseCnt;
    private final long whereClauseCnt;
    private final long joinClauseCnt;
    private final long groupByCnt;
    private final long orderByCnt;
    private final long totalScanBytes;
    private final long totalQueryTimeMs;
    private final long maxQueryTimeMs;
    private final LocalDateTime lastAccessTime;

    HourlyUsageRow(LocalDateTime statHour, ColumnIdentifier column, String userName, long accessQueryCnt,
                   long selectClauseCnt, long whereClauseCnt, long joinClauseCnt, long groupByCnt, long orderByCnt,
                   long totalScanBytes, long totalQueryTimeMs, long maxQueryTimeMs, LocalDateTime lastAccessTime) {
        this.statHour = Objects.requireNonNull(statHour, "statHour");
        this.column = Objects.requireNonNull(column, "column");
        this.userName = Objects.requireNonNull(userName, "userName");
        this.accessQueryCnt = accessQueryCnt;
        this.selectClauseCnt = selectClauseCnt;
        this.whereClauseCnt = whereClauseCnt;
        this.joinClauseCnt = joinClauseCnt;
        this.groupByCnt = groupByCnt;
        this.orderByCnt = orderByCnt;
        this.totalScanBytes = totalScanBytes;
        this.totalQueryTimeMs = totalQueryTimeMs;
        this.maxQueryTimeMs = maxQueryTimeMs;
        this.lastAccessTime = Objects.requireNonNull(lastAccessTime, "lastAccessTime");
    }

    public LocalDateTime getStatHour() {
        return statHour;
    }

    public ColumnIdentifier getColumn() {
        return column;
    }

    public String getUserName() {
        return userName;
    }

    public long getAccessQueryCnt() {
        return accessQueryCnt;
    }

    public long getSelectClauseCnt() {
        return selectClauseCnt;
    }

    public long getWhereClauseCnt() {
        return whereClauseCnt;
    }

    public long getJoinClauseCnt() {
        return joinClauseCnt;
    }

    public long getGroupByCnt() {
        return groupByCnt;
    }

    public long getOrderByCnt() {
        return orderByCnt;
    }

    public long getTotalScanBytes() {
        return totalScanBytes;
    }

    public long getTotalQueryTimeMs() {
        return totalQueryTimeMs;
    }

    public long getMaxQueryTimeMs() {
        return maxQueryTimeMs;
    }

    public LocalDateTime getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public String toString() {
        return "HourlyUsageRow{" + statHour + ", " + column + ", user=" + userName + ", access=" + accessQueryCnt
                + ", select=" + selectClauseCnt + ", where=" + whereClauseCnt + ", join=" + joinClauseCnt
                + ", groupBy=" + groupByCnt + ", orderBy=" + orderByCnt + ", scanBytes=" + totalScanBytes
                + ", maxQueryTimeMs=" + maxQueryTimeMs + '}';
    }
}
