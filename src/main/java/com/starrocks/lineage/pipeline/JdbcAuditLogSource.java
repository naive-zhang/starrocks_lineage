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
 * Keyset-paginated reader of {@code starrocks_audit_db__.starrocks_audit_tbl__} (StarRocks 3.1 audit loader schema).
 *
 * <p>StarRocks 3.1 has no row-value comparison, so {@code (timestamp, queryId) > (t, q)} is written as
 * {@code timestamp > t OR (timestamp = t AND queryId > q)}; the leading {@code timestamp >= t} conjunct keeps
 * partition pruning on the (day partitioned) audit table.</p>
 */
public final class JdbcAuditLogSource implements AuditLogSource {

    /** Default audit table of the StarRocks audit loader plugin. */
    public static final String DEFAULT_TABLE = "starrocks_audit_db__.starrocks_audit_tbl__";

    private final DataSource dataSource;
    private final String sql;
    private final int queryTimeoutSeconds;

    public JdbcAuditLogSource(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE, 60);
    }

    /**
     * @param table qualified audit table name (trusted configuration, not user input)
     */
    public JdbcAuditLogSource(DataSource dataSource, String table, int queryTimeoutSeconds) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (!table.matches("[A-Za-z0-9_.`]+")) {
            throw new IllegalArgumentException("invalid audit table name: " + table);
        }
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.sql = "SELECT queryId, `timestamp`, queryType, clientIp, `user`, authorizedUser, `catalog`, db, state, "
                + "errorCode, queryTime, scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, isQuery, stmt, "
                + "digest FROM " + table
                + " WHERE `timestamp` >= ? AND (`timestamp` > ? OR (`timestamp` = ? AND queryId > ?))"
                + " AND `timestamp` < ? ORDER BY `timestamp`, queryId LIMIT ?";
    }

    @Override
    public List<AuditLogRecord> fetch(Watermark after, LocalDateTime upperBound, int limit) {
        Timestamp from = Timestamp.valueOf(after.getTimestamp());
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            ps.setTimestamp(1, from);
            ps.setTimestamp(2, from);
            ps.setTimestamp(3, from);
            ps.setString(4, after.getQueryId());
            ps.setTimestamp(5, Timestamp.valueOf(upperBound));
            ps.setInt(6, limit);
            List<AuditLogRecord> records = new ArrayList<>(limit);
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
            throw new PipelineException("audit read after " + after + " failed: " + e.getMessage(), e);
        }
    }
}
