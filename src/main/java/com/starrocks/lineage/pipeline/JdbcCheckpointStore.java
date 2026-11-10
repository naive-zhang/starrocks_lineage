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
import java.util.function.UnaryOperator;

/**
 * {@link CheckpointStore} on {@code audit_lineage_sync_checkpoint} (one row per pipeline, PRIMARY KEY
 * {@code pipeline_name}: an {@code INSERT} is an upsert).
 *
 * <p>Every mutation is read-modify-write of the whole row, guarded by the lease owner, and read back: a write that
 * did not persist (wrong table model) or that lost a race fails loudly. StarRocks has no compare-and-set, so the
 * lease cannot exclude two writers racing inside the same milliseconds; the deterministic batch ids, the step
 * labels and the idempotent refresh keep the data exactly-once even then, and {@link #verifyLease} before each
 * write step stops an instance that lost its lease while paused.</p>
 */
public final class JdbcCheckpointStore implements CheckpointStore {

    private static final String COLUMNS = "pipeline_name, committed_until, coverage_start, owner, lease_expire_time, "
            + "processed_rows, pending_batch_id, pending_kind, pending_start_time, pending_end_time, pending_step, "
            + "pending_since, pending_attempts, last_error, last_reconcile_time, update_time, pipeline_epoch";
    private static final int MAX_ERROR = 1000;

    private final DataSource dataSource;
    private final String table;
    private final Clock clock;
    private final String upsertVerb;

    public JdbcCheckpointStore(DataSource dataSource) {
        this(dataSource, "audit_lineage_db.audit_lineage_sync_checkpoint", Clock.systemDefaultZone(), "INSERT");
    }

    /**
     * @param upsertVerb {@code INSERT} on the StarRocks PRIMARY KEY table; another engine's upsert verb in tests
     */
    public JdbcCheckpointStore(DataSource dataSource, String table, Clock clock, String upsertVerb) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (!table.matches("[A-Za-z0-9_.`]+")) {
            throw new IllegalArgumentException("invalid checkpoint table name: " + table);
        }
        if (!upsertVerb.matches("[A-Z]+")) {
            throw new IllegalArgumentException("invalid upsert verb: " + upsertVerb);
        }
        this.table = table;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.upsertVerb = upsertVerb;
    }

    @Override
    public Optional<Checkpoint> load(String pipeline) {
        return read(pipeline);
    }

    @Override
    public boolean tryAcquireLease(String pipeline, String owner, Duration ttl, LocalDateTime initialStart) {
        LocalDateTime now = now();
        Optional<Checkpoint> existing = read(pipeline);
        if (existing.isPresent() && foreign(existing.get(), owner, now)) {
            return false;
        }
        Checkpoint base = existing.orElse(new Checkpoint(java.util.UUID.randomUUID().toString(), initialStart,
                initialStart, null, 0L, null, owner, now));
        write(pipeline, new Checkpoint(base.getEpoch(), base.getCommittedUntil(), base.getCoverageStart(),
                base.getPending().orElse(null), base.getProcessedRows(), base.getLastReconcile().orElse(null), owner,
                now.plus(ttl)));
        return read(pipeline).map(c -> owner.equals(c.getOwner())).orElse(false);
    }

    @Override
    public void verifyLease(String pipeline, String owner, Duration ttl) {
        mutate(pipeline, owner, ttl, c -> c);
    }

    @Override
    public void releaseLease(String pipeline, String owner) {
        Optional<Checkpoint> c = read(pipeline);
        if (c.isPresent() && owner.equals(c.get().getOwner())) {
            write(pipeline, copy(c.get(), c.get().getPending().orElse(null), c.get().getCommittedUntil(),
                    c.get().getProcessedRows(), c.get().getLastReconcile().orElse(null), owner, now()));
        }
    }

    @Override
    public void savePending(String pipeline, String owner, Duration ttl, PendingBatch pending) {
        Checkpoint after = mutate(pipeline, owner, ttl, c -> {
            if (c.getPending().isPresent() && !c.getPending().get().getBatchId().equals(pending.getBatchId())) {
                throw PipelineException.nonRetryable("batch " + c.getPending().get().getBatchId()
                        + " is pending, cannot declare " + pending.getBatchId(), null);
            }
            return withPending(c, pending);
        });
        requirePending(after, pending.getBatchId());
    }

    @Override
    public void advanceStep(String pipeline, String owner, Duration ttl, String batchId, BatchStep step) {
        Checkpoint after = mutate(pipeline, owner, ttl, c -> {
            PendingBatch p = requirePending(c, batchId);
            return withPending(c, step.isAfter(p.getStep()) ? p.withStep(step) : p);
        });
        if (step.isAfter(requirePending(after, batchId).getStep())) {
            throw new PipelineException("step " + step + " of batch " + batchId + " was not persisted");
        }
    }

    @Override
    public void restartPendingClock(String pipeline, String owner, Duration ttl, String batchId) {
        mutate(pipeline, owner, ttl, c -> withPending(c, requirePending(c, batchId).withSince(now())));
    }

    @Override
    public void recordFailure(String pipeline, String owner, Duration ttl, String batchId, String error) {
        String e = error == null ? "" : (error.length() > MAX_ERROR ? error.substring(0, MAX_ERROR) : error);
        mutate(pipeline, owner, ttl, c -> withPending(c, requirePending(c, batchId).withFailure(e)));
    }

    @Override
    public void commit(String pipeline, String owner, Duration ttl, String batchId, LocalDateTime newCommittedUntil,
                       long records) {
        Checkpoint after = mutate(pipeline, owner, ttl, c -> {
            requirePending(c, batchId);
            LocalDateTime until = c.getCommittedUntil();
            if (newCommittedUntil != null) {
                if (newCommittedUntil.isBefore(until)) {
                    throw PipelineException.nonRetryable("watermark of " + pipeline + " would move backwards from "
                            + until + " to " + newCommittedUntil, null);
                }
                until = newCommittedUntil;
            }
            return copy(c, null, until, c.getProcessedRows() + records, c.getLastReconcile().orElse(null), owner,
                    c.getLeaseExpire());
        });
        if (after.getPending().isPresent()
                || (newCommittedUntil != null && !after.getCommittedUntil().equals(newCommittedUntil))) {
            throw PipelineException.nonRetryable("checkpoint commit of " + pipeline + " batch " + batchId
                    + " was not persisted (is pipeline_name the PRIMARY KEY?)", null);
        }
    }

    @Override
    public void markReconciled(String pipeline, String owner, Duration ttl, LocalDateTime at) {
        mutate(pipeline, owner, ttl, c -> copy(c, c.getPending().orElse(null), c.getCommittedUntil(),
                c.getProcessedRows(), at, owner, c.getLeaseExpire()));
    }

    // ------------------------------------------------------------------------------------------------ internals

    private Checkpoint mutate(String pipeline, String owner, Duration ttl, UnaryOperator<Checkpoint> change) {
        LocalDateTime now = now();
        Checkpoint current = read(pipeline).orElseThrow(() -> PipelineException.nonRetryable(
                "no checkpoint for pipeline " + pipeline, null));
        if (!owner.equals(current.getOwner())) {
            if (current.getLeaseExpire().isAfter(now)) {
                throw new LeaseLostException("lease of pipeline " + pipeline + " is held by " + current.getOwner());
            }
            throw new LeaseLostException("lease of pipeline " + pipeline + " expired and was taken by "
                    + current.getOwner());
        }
        Checkpoint changed = change.apply(current);
        Checkpoint renewed = copy(changed, changed.getPending().orElse(null), changed.getCommittedUntil(),
                changed.getProcessedRows(), changed.getLastReconcile().orElse(null), owner, now.plus(ttl));
        write(pipeline, renewed);
        Checkpoint stored = read(pipeline).orElseThrow(() -> PipelineException.nonRetryable(
                "checkpoint of " + pipeline + " vanished", null));
        if (!owner.equals(stored.getOwner())) {
            throw new LeaseLostException("lease of pipeline " + pipeline + " was taken by " + stored.getOwner()
                    + " concurrently");
        }
        return stored;
    }

    private static boolean foreign(Checkpoint c, String owner, LocalDateTime now) {
        return !owner.equals(c.getOwner()) && c.getLeaseExpire().isAfter(now);
    }

    private static PendingBatch requirePending(Checkpoint c, String batchId) {
        PendingBatch p = c.getPending().orElseThrow(() -> PipelineException.nonRetryable(
                "no pending batch (expected " + batchId + ")", null));
        if (!p.getBatchId().equals(batchId)) {
            throw PipelineException.nonRetryable("pending batch is " + p.getBatchId() + ", not " + batchId, null);
        }
        return p;
    }

    private static Checkpoint withPending(Checkpoint c, PendingBatch p) {
        return copy(c, p, c.getCommittedUntil(), c.getProcessedRows(), c.getLastReconcile().orElse(null),
                c.getOwner(), c.getLeaseExpire());
    }

    private static Checkpoint copy(Checkpoint c, PendingBatch p, LocalDateTime until, long processed,
                                   LocalDateTime reconcile, String owner, LocalDateTime leaseExpire) {
        return new Checkpoint(c.getEpoch(), until, c.getCoverageStart(), p, processed, reconcile, owner,
                leaseExpire);
    }

    private LocalDateTime now() {
        return LocalDateTime.now(clock).withNano(0);
    }

    private Optional<Checkpoint> read(String pipeline) {
        String sql = "SELECT " + COLUMNS + " FROM " + table + " WHERE pipeline_name = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pipeline);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                PendingBatch pending = null;
                String batchId = rs.getString(7);
                if (batchId != null && !batchId.isEmpty()) {
                    pending = new PendingBatch(batchId, BatchKind.valueOf(rs.getString(8)), ts(rs, 9), ts(rs, 10),
                            BatchStep.valueOf(rs.getString(11)), ts(rs, 12), rs.getInt(13), rs.getString(14));
                }
                return Optional.of(new Checkpoint(rs.getString(17), ts(rs, 2), ts(rs, 3), pending, rs.getLong(6),
                        ts(rs, 15),
                        rs.getString(4), ts(rs, 5)));
            }
        } catch (SQLException e) {
            throw new PipelineException("checkpoint read failed: " + e.getMessage(), e);
        }
    }

    private static LocalDateTime ts(ResultSet rs, int i) throws SQLException {
        Timestamp t = rs.getTimestamp(i);
        return t == null ? null : t.toLocalDateTime();
    }

    private void write(String pipeline, Checkpoint c) {
        String sql = upsertVerb + " INTO " + table + " (" + COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                + "?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, pipeline);
            ps.setTimestamp(2, Timestamp.valueOf(c.getCommittedUntil()));
            ps.setTimestamp(3, Timestamp.valueOf(c.getCoverageStart()));
            ps.setString(4, c.getOwner());
            ps.setTimestamp(5, Timestamp.valueOf(c.getLeaseExpire()));
            ps.setLong(6, c.getProcessedRows());
            PendingBatch p = c.getPending().orElse(null);
            if (p == null) {
                for (int i = 7; i <= 14; i++) {
                    ps.setNull(i, i == 13 ? Types.INTEGER : (i == 9 || i == 10 || i == 12 ? Types.TIMESTAMP
                            : Types.VARCHAR));
                }
            } else {
                ps.setString(7, p.getBatchId());
                ps.setString(8, p.getKind().name());
                ps.setTimestamp(9, Timestamp.valueOf(p.getRangeStart()));
                ps.setTimestamp(10, Timestamp.valueOf(p.getRangeEnd()));
                ps.setString(11, p.getStep().name());
                ps.setTimestamp(12, Timestamp.valueOf(p.getSince()));
                ps.setInt(13, p.getAttempts());
                ps.setString(14, p.getLastError().orElse(null));
            }
            if (c.getLastReconcile().isPresent()) {
                ps.setTimestamp(15, Timestamp.valueOf(c.getLastReconcile().get()));
            } else {
                ps.setNull(15, Types.TIMESTAMP);
            }
            ps.setTimestamp(16, Timestamp.valueOf(now()));
            ps.setString(17, c.getEpoch());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PipelineException("checkpoint write failed: " + e.getMessage(), e);
        }
    }
}
