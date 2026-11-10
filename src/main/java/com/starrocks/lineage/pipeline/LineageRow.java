package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.StatementType;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Contribution of one batch to one edge on one day ({@code audit_lineage_batch_lineage} row, then
 * {@code audit_column_lineage_daily} row). {@link #merge(LineageRow)} folds two observations of the same edge and day.
 */
public final class LineageRow {

    private final LocalDateTime statDay;
    private final ColumnLineageEdge edge;
    private final StatementType stmtType;
    private final String sqlDigest;
    private final String lastQueryId;
    private final String lastExecUser;
    private final LocalDateTime firstSeenTime;
    private final LocalDateTime lastSeenTime;
    private final long execCount;

    LineageRow(LocalDateTime statDay, ColumnLineageEdge edge, StatementType stmtType, String sqlDigest,
               String lastQueryId, String lastExecUser, LocalDateTime firstSeenTime, LocalDateTime lastSeenTime,
               long execCount) {
        this.statDay = Objects.requireNonNull(statDay, "statDay").truncatedTo(ChronoUnit.DAYS);
        this.edge = Objects.requireNonNull(edge, "edge");
        this.stmtType = Objects.requireNonNull(stmtType, "stmtType");
        this.sqlDigest = sqlDigest;
        this.lastQueryId = Objects.requireNonNull(lastQueryId, "lastQueryId");
        this.lastExecUser = Objects.requireNonNull(lastExecUser, "lastExecUser");
        this.firstSeenTime = Objects.requireNonNull(firstSeenTime, "firstSeenTime");
        this.lastSeenTime = Objects.requireNonNull(lastSeenTime, "lastSeenTime");
        this.execCount = execCount;
    }

    /**
     * Folds another observation of the same edge and day: counts add up, {@code first_seen} keeps the minimum and the
     * {@code last_*} attributes follow the most recent statement; on equal timestamps the larger query id wins so the
     * result does not depend on the order in which concurrent threads delivered the observations.
     */
    LineageRow merge(LineageRow other) {
        if (!edge.equals(other.edge) || !statDay.equals(other.statDay)) {
            throw new IllegalArgumentException("different edge / day: " + this + " / " + other);
        }
        boolean otherIsNewer = other.lastSeenTime.isAfter(lastSeenTime)
                || (other.lastSeenTime.equals(lastSeenTime) && other.lastQueryId.compareTo(lastQueryId) > 0);
        LineageRow newest = otherIsNewer ? other : this;
        LocalDateTime first = other.firstSeenTime.isBefore(firstSeenTime) ? other.firstSeenTime : firstSeenTime;
        return new LineageRow(statDay, newest.edge, newest.stmtType, newest.sqlDigest, newest.lastQueryId,
                newest.lastExecUser, first, newest.lastSeenTime, execCount + other.execCount);
    }

    public LocalDateTime getStatDay() {
        return statDay;
    }

    public ColumnLineageEdge getEdge() {
        return edge;
    }

    public String getEdgeId() {
        return edge.edgeId();
    }

    public StatementType getStmtType() {
        return stmtType;
    }

    public String getSqlDigest() {
        return sqlDigest;
    }

    public String getLastQueryId() {
        return lastQueryId;
    }

    public String getLastExecUser() {
        return lastExecUser;
    }

    public LocalDateTime getFirstSeenTime() {
        return firstSeenTime;
    }

    public LocalDateTime getLastSeenTime() {
        return lastSeenTime;
    }

    public long getExecCount() {
        return execCount;
    }

    @Override
    public String toString() {
        return "LineageRow{" + statDay.toLocalDate() + " " + edge + ", count=" + execCount + ", last=" + lastQueryId
                + '}';
    }
}
