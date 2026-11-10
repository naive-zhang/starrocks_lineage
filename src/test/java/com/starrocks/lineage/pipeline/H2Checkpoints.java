package com.starrocks.lineage.pipeline;

import org.h2.jdbcx.JdbcDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;

/**
 * {@code audit_lineage_sync_checkpoint} (sql/ddl/05) on H2 in MySQL mode; {@code MERGE} plays the StarRocks PRIMARY
 * KEY upsert.
 */
final class H2Checkpoints implements AutoCloseable {

    static final String TABLE = "audit_lineage_db.audit_lineage_sync_checkpoint";

    final JdbcDataSource dataSource = new JdbcDataSource();
    private final Connection keepAlive;

    H2Checkpoints() throws SQLException {
        dataSource.setURL("jdbc:h2:mem:cp_" + System.nanoTime() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;"
                + "DB_CLOSE_DELAY=-1");
        keepAlive = dataSource.getConnection();
        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE SCHEMA audit_lineage_db");
            st.execute("CREATE TABLE " + TABLE + " (pipeline_name VARCHAR(64) PRIMARY KEY, "
                    + "committed_until TIMESTAMP NOT NULL, coverage_start TIMESTAMP NOT NULL, "
                    + "owner VARCHAR(128) NOT NULL, lease_expire_time TIMESTAMP NOT NULL, processed_rows BIGINT, "
                    + "pending_batch_id VARCHAR(64), pending_kind VARCHAR(16), pending_start_time TIMESTAMP, "
                    + "pending_end_time TIMESTAMP, pending_step VARCHAR(32), pending_since TIMESTAMP, "
                    + "pending_attempts INT, last_error VARCHAR(1024), last_reconcile_time TIMESTAMP, "
                    + "update_time TIMESTAMP NOT NULL, pipeline_epoch VARCHAR(64) NOT NULL)");
        }
    }

    JdbcCheckpointStore store(Clock clock) {
        return new JdbcCheckpointStore(dataSource, TABLE, clock, "MERGE");
    }

    @Override
    public void close() throws SQLException {
        keepAlive.close();
    }
}
