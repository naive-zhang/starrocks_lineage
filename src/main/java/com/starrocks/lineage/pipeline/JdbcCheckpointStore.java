package com.starrocks.lineage.pipeline;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link CheckpointStore} on {@code audit_lineage_sync_checkpoint} (05_audit_lineage_sync_checkpoint.sql).
 *
 * <p>Writes are plain {@code INSERT}s: on the StarRocks PRIMARY KEY table an insert is an upsert of the single row of
 * the pipeline. A commit is read back and verified: if the store did not persist it (e.g. a table created without
 * the primary key, which would append rows instead), the cycle fails loudly instead of replaying the same batch
 * forever (review R4-05).</p>
 *
 * <p>StarRocks has no row lock / compare-and-set, so the lease is <i>best effort</i>: acquire = read, refuse when a
 * foreign unexpired lease exists, write, re-read and verify ownership. Two instances racing inside the same few
 * milliseconds could both pass; the batch intent plus the idempotent sinks keep the data exactly-once even then,
 * so the lease protects throughput, not correctness.</p>
 */
public final class JdbcCheckpointStore implements CheckpointStore {

    private static final String SELECT = "SELECT watermark_time, watermark_query_id, owner, lease_expire_time, "
            + "processed_rows, pending_batch_id, pending_end_time, pending_end_query_id FROM %s "
            + "WHERE pipeline_name = ? ORDER BY update_time DESC, lease_expire_time DESC LIMIT 1";
    private static final String INSERT = "%s INTO %s (pipeline_name, watermark_time, watermark_query_id, owner, "
            + "lease_expire_time, processed_rows, pending_batch_id, pending_end_time, pending_end_query_id, "
            + "update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final DataSource dataSource;
    private final String selectSql;
    private final String insertSql;
    private final Clock clock;

    public JdbcCheckpointStore(DataSource dataSource) {
        this(dataSource, "audit_lineage_db.audit_lineage_sync_checkpoint", Clock.systemDefaultZone(), "INSERT");
    }

    /**
     * @param upsertVerb {@code INSERT} on the StarRocks PRIMARY KEY table (insert = upsert); the upsert verb of
     *                   another engine (e.g. H2 {@code MERGE}) in tests
     */
    public JdbcCheckpointStore(DataSource dataSource, String table, Clock clock, String upsertVerb) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (!table.matches("[A-Za-z0-9_.`]+")) {
            throw new IllegalArgumentException("invalid checkpoint table name: " + table);
        }
        if (!upsertVerb.matches("[A-Z]+")) {
            throw new IllegalArgumentException("invalid upsert verb: " + upsertVerb);
        }
        this.selectSql = String.format(SELECT, table);
        this.insertSql = String.format(INSERT, upsertVerb, table);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Optional<Checkpoint> load(String pipeline) {
        return read(pipeline).map(r -> r.checkpoint);
    }

    @Override
    public boolean tryAcquireLease(String pipeline, String owner, Duration ttl, Watermark initial) {
        LocalDateTime now = LocalDateTime.now(clock);
        Optional<Row> existing = read(pipeline);
        if (existing.isPresent() && foreignLease(existing.get(), owner, now)) {
            return false;
        }
        Checkpoint cp = existing.map(r -> r.checkpoint).orElse(new Checkpoint(initial, null, 0L));
        write(pipeline, owner, now.plus(ttl), cp.getWatermark(), cp.getProcessedRows(),
                cp.getPending().orElse(null), now);
        return read(pipeline).map(r -> r.owner.equals(owner)).orElse(false);
    }

    @Override
    public void savePending(String pipeline, String owner, Duration ttl, PendingBatch pending) {
        LocalDateTime now = LocalDateTime.now(clock);
        Row row = requireOwnRow(pipeline, owner, now);
        write(pipeline, owner, now.plus(ttl), row.checkpoint.getWatermark(), row.checkpoint.getProcessedRows(),
                pending, now);
    }

    @Override
    public void commit(String pipeline, String owner, Duration ttl, Watermark watermark, long processedRows) {
        LocalDateTime now = LocalDateTime.now(clock);
        Row row = requireOwnRow(pipeline, owner, now);
        write(pipeline, owner, now.plus(ttl), watermark, row.checkpoint.getProcessedRows() + processedRows, null, now);
        Checkpoint stored = read(pipeline).map(r -> r.checkpoint).orElse(null);
        if (stored == null || !stored.getWatermark().equals(watermark) || stored.getPending().isPresent()) {
            throw new PipelineException("checkpoint commit of " + pipeline + " to " + watermark
                    + " was not persisted (is pipeline_name the PRIMARY KEY?)");
        }
    }

    private Row requireOwnRow(String pipeline, String owner, LocalDateTime now) {
        Row row = read(pipeline).orElseThrow(() -> new PipelineException("no checkpoint for pipeline " + pipeline));
        if (foreignLease(row, owner, now)) {
            throw new PipelineException("lease of pipeline " + pipeline + " is held by " + row.owner);
        }
        return row;
    }

    private static boolean foreignLease(Row row, String owner, LocalDateTime now) {
        return !row.owner.equals(owner) && row.leaseExpire.isAfter(now);
    }

    private Optional<Row> read(String pipeline) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(selectSql)) {
            ps.setString(1, pipeline);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                Watermark wm = new Watermark(rs.getTimestamp(1).toLocalDateTime(), rs.getString(2));
                PendingBatch pending = null;
                String batchId = rs.getString(6);
                if (batchId != null && !batchId.isEmpty()) {
                    pending = new PendingBatch(batchId, new Watermark(rs.getTimestamp(7).toLocalDateTime(),
                            rs.getString(8)));
                }
                return Optional.of(new Row(new Checkpoint(wm, pending, rs.getLong(5)), rs.getString(3),
                        rs.getTimestamp(4).toLocalDateTime()));
            }
        } catch (SQLException e) {
            throw new PipelineException("checkpoint read failed: " + e.getMessage(), e);
        }
    }

    private void write(String pipeline, String owner, LocalDateTime leaseExpire, Watermark wm, long processed,
                       PendingBatch pending, LocalDateTime now) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(insertSql)) {
            ps.setString(1, pipeline);
            ps.setTimestamp(2, Timestamp.valueOf(wm.getTimestamp()));
            ps.setString(3, wm.getQueryId());
            ps.setString(4, owner);
            ps.setTimestamp(5, Timestamp.valueOf(leaseExpire));
            ps.setLong(6, processed);
            if (pending == null) {
                ps.setNull(7, Types.VARCHAR);
                ps.setNull(8, Types.TIMESTAMP);
                ps.setNull(9, Types.VARCHAR);
            } else {
                ps.setString(7, pending.getBatchId());
                ps.setTimestamp(8, Timestamp.valueOf(pending.getEnd().getTimestamp()));
                ps.setString(9, pending.getEnd().getQueryId());
            }
            ps.setTimestamp(10, Timestamp.valueOf(now));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PipelineException("checkpoint write failed: " + e.getMessage(), e);
        }
    }

    private static final class Row {
        private final Checkpoint checkpoint;
        private final String owner;
        private final LocalDateTime leaseExpire;

        Row(Checkpoint checkpoint, String owner, LocalDateTime leaseExpire) {
            this.checkpoint = checkpoint;
            this.owner = owner;
            this.leaseExpire = leaseExpire;
        }
    }
}
