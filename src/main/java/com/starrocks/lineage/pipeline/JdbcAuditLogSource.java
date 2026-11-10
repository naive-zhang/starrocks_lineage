package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link AuditLogSource} on {@code starrocks_audit_db__.starrocks_audit_tbl__} (StarRocks 3.1 audit loader schema) and
 * {@code audit_lineage_batch_member}.
 *
 * <p>StarRocks 3.1 has no row-value comparison, so {@code (timestamp, queryId) > (t, q)} is written as
 * {@code timestamp > t OR (timestamp = t AND queryId > q)}; the range predicate on {@code timestamp} keeps partition
 * pruning on both day-partitioned tables.</p>
 */
public final class JdbcAuditLogSource implements AuditLogSource {

    /** Default audit table of the StarRocks audit loader plugin. */
    public static final String DEFAULT_TABLE = "starrocks_audit_db__.starrocks_audit_tbl__";
    /** Default membership table. */
    public static final String DEFAULT_MEMBER_TABLE = "audit_lineage_db.audit_lineage_batch_member";

    private static final String COLUMNS = "a.queryId, a.`timestamp`, a.queryType, a.clientIp, a.`user`, "
            + "a.authorizedUser, a.`catalog`, a.db, a.state, a.errorCode, a.queryTime, a.scanBytes, a.scanRows, "
            + "a.returnRows, a.cpuCostNs, a.memCostBytes, a.isQuery, a.stmt, a.digest";
    private static final String KEYSET = " AND (a.`timestamp` > ? OR (a.`timestamp` = ? AND a.queryId > ?))";
    private static final String ORDER = " ORDER BY a.`timestamp`, a.queryId LIMIT ?";

    private final DataSource dataSource;
    private final String table;
    private final String memberTable;
    private final int queryTimeoutSeconds;

    public JdbcAuditLogSource(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE, DEFAULT_MEMBER_TABLE, 120);
    }

    /**
     * @param table       qualified audit table name (trusted configuration)
     * @param memberTable qualified membership table name (trusted configuration)
     */
    public JdbcAuditLogSource(DataSource dataSource, String table, String memberTable, int queryTimeoutSeconds) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (!table.matches("[A-Za-z0-9_.`]+") || !memberTable.matches("[A-Za-z0-9_.`]+")) {
            throw new IllegalArgumentException("invalid table name: " + table + " / " + memberTable);
        }
        this.table = table;
        this.memberTable = memberTable;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    @Override
    public List<AuditLogRecord> fetchRange(LocalDateTime from, LocalDateTime to, Watermark after, int limit) {
        String sql = "SELECT " + COLUMNS + " FROM " + table + " a WHERE a.`timestamp` >= ? AND a.`timestamp` < ?"
                + KEYSET + ORDER;
        return query(sql, from, to, after, limit);
    }

    @Override
    public List<AuditLogRecord> fetchMembers(String pipeline, String batchId, LocalDateTime from, LocalDateTime to,
                                             Watermark after, int limit) {
        String sql = "SELECT " + COLUMNS + " FROM " + table + " a JOIN " + memberTable + " m "
                + "ON m.query_id = a.queryId AND m.audit_time = a.`timestamp` "
                + "WHERE m.pipeline_name = ? AND m.batch_id = ? AND m.audit_time >= ? AND m.audit_time < ? "
                + "AND a.`timestamp` >= ? AND a.`timestamp` < ?" + KEYSET + ORDER;
        return query(sql, from, to, after, limit, pipeline, batchId);
    }

    @Override
    public List<AuditLogRecord> fetchProcessed(String pipeline, LocalDateTime from, LocalDateTime to,
                                               Watermark after, int limit) {
        String sql = "SELECT " + COLUMNS + " FROM " + table + " a LEFT SEMI JOIN " + memberTable + " m "
                + "ON m.query_id = a.queryId AND m.audit_time = a.`timestamp` AND m.pipeline_name = ? "
                + "AND m.audit_time >= ? AND m.audit_time < ? "
                + "WHERE a.`timestamp` >= ? AND a.`timestamp` < ?" + KEYSET + ORDER;
        return query(sql, from, to, after, limit, pipeline, null);
    }

    @Override
    public List<AuditLogRecord> fetchUnprocessed(String pipeline, LocalDateTime from, LocalDateTime to, int limit) {
        String sql = "SELECT " + COLUMNS + " FROM " + table + " a LEFT ANTI JOIN " + memberTable + " m "
                + "ON m.query_id = a.queryId AND m.audit_time = a.`timestamp` AND m.pipeline_name = ? "
                + "AND m.audit_time >= ? AND m.audit_time < ? "
                + "WHERE a.`timestamp` >= ? AND a.`timestamp` < ?" + KEYSET + ORDER;
        return query(sql, from, to, Watermark.startingAt(from), limit, pipeline, null);
    }

    @Override
    public long countMembers(String pipeline, LocalDateTime from, LocalDateTime to) {
        return count("SELECT COUNT(*) FROM " + memberTable + " m WHERE m.pipeline_name = ? AND m.audit_time >= ? "
                + "AND m.audit_time < ?", pipeline, from, to);
    }

    @Override
    public long countProcessed(String pipeline, LocalDateTime from, LocalDateTime to) {
        return count("SELECT COUNT(*) FROM " + memberTable + " m JOIN " + table + " a ON a.queryId = m.query_id "
                + "AND a.`timestamp` = m.audit_time WHERE m.pipeline_name = ? AND m.audit_time >= ? "
                + "AND m.audit_time < ? AND a.`timestamp` >= ? AND a.`timestamp` < ?", pipeline, from, to, from, to);
    }

    private long count(String sql, Object... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            for (int i = 0; i < params.length; i++) {
                Object p = params[i];
                if (p instanceof LocalDateTime) {
                    ps.setTimestamp(i + 1, Timestamp.valueOf((LocalDateTime) p));
                } else {
                    ps.setString(i + 1, (String) p);
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new PipelineException("audit count failed: " + e.getMessage(), e);
        }
    }

    /**
     * Binds, in order: [pipeline, (batchId), from, to] for member queries, then from, to, keyset (3), limit.
     */
    private List<AuditLogRecord> query(String sql, LocalDateTime from, LocalDateTime to, Watermark after, int limit,
                                       String pipeline, String batchId) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            int i = 1;
            if (pipeline != null) {
                ps.setString(i++, pipeline);
                if (batchId != null) {
                    ps.setString(i++, batchId);
                }
                ps.setTimestamp(i++, Timestamp.valueOf(from));
                ps.setTimestamp(i++, Timestamp.valueOf(to));
            }
            ps.setTimestamp(i++, Timestamp.valueOf(from));
            ps.setTimestamp(i++, Timestamp.valueOf(to));
            Timestamp cursor = Timestamp.valueOf(after.getTimestamp());
            ps.setTimestamp(i++, cursor);
            ps.setTimestamp(i++, cursor);
            ps.setString(i++, after.getQueryId());
            ps.setInt(i, limit);
            List<AuditLogRecord> records = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    records.add(AuditLogRecord.builder()
                            .queryId(rs.getString(1))
                            .timestamp(rs.getTimestamp(2).toLocalDateTime())
                            .queryType(rs.getString(3))
                            .clientIp(rs.getString(4))
                            .user(rs.getString(5))
                            .authorizedUser(rs.getString(6))
                            .catalog(rs.getString(7))
                            .db(rs.getString(8))
                            .state(rs.getString(9))
                            .errorCode(rs.getString(10))
                            .queryTime(rs.getLong(11))
                            .scanBytes(rs.getLong(12))
                            .scanRows(rs.getLong(13))
                            .returnRows(rs.getLong(14))
                            .cpuCostNs(rs.getLong(15))
                            .memCostBytes(rs.getLong(16))
                            .isQuery(rs.getInt(17) != 0)
                            .stmt(rs.getString(18))
                            .digest(rs.getString(19))
                            .build());
                }
            }
            return records;
        } catch (SQLException e) {
            throw new PipelineException("audit read failed: " + e.getMessage(), e);
        }
    }

    private List<AuditLogRecord> query(String sql, LocalDateTime from, LocalDateTime to, Watermark after, int limit) {
        return query(sql, from, to, after, limit, null, null);
    }
}
