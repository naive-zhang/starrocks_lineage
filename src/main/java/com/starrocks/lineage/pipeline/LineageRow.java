package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.StatementType;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * One pre-aggregated row of {@code audit_column_lineage_stage}: an edge plus the statistics of every statement of
 * the batch that produced it. Immutable; {@link #merge(LineageRow)} folds two observations of the same edge.
 */
public final class LineageRow {

    private final ColumnLineageEdge edge;
    private final StatementType stmtType;
    private final String sqlDigest;
    private final String lastQueryId;
    private final String lastExecUser;
    private final LocalDateTime firstSeenTime;
    private final LocalDateTime lastSeenTime;
    private final long execCount;

    LineageRow(ColumnLineageEdge edge, StatementType stmtType, String sqlDigest, String lastQueryId,
               String lastExecUser, LocalDateTime firstSeenTime, LocalDateTime lastSeenTime, long execCount) {
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
     * Folds another observation of the same edge: counts add up, {@code first_seen} keeps the minimum and every
     * {@code last_*} attribute (transform, expression, statement type, digest, query id, user) follows the most
     * recent statement; on equal timestamps the larger query id wins so the result does not depend on the order in
     * which concurrent threads delivered the observations.
     */
    LineageRow merge(LineageRow other) {
        if (!edge.equals(other.edge)) {
            throw new IllegalArgumentException("different edges: " + edge + " / " + other.edge);
        }
        boolean otherIsNewer = other.lastSeenTime.isAfter(lastSeenTime)
                || (other.lastSeenTime.equals(lastSeenTime) && other.lastQueryId.compareTo(lastQueryId) > 0);
        LineageRow newest = otherIsNewer ? other : this;
        LocalDateTime first = other.firstSeenTime.isBefore(firstSeenTime) ? other.firstSeenTime : firstSeenTime;
        return new LineageRow(newest.edge, newest.stmtType, newest.sqlDigest, newest.lastQueryId,
                newest.lastExecUser, first, newest.lastSeenTime, execCount + other.execCount);
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
        return "LineageRow{" + edge + ", count=" + execCount + ", last=" + lastQueryId + '}';
    }
}
