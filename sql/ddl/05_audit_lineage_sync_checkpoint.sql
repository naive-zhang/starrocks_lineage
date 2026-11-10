-- =====================================================================================================================
-- Checkpoint of a pipeline (one row per pipeline, PRIMARY KEY: an INSERT is an upsert). JdbcCheckpointStore.
--
--   committed_until      : every audit record with `timestamp` < committed_until belongs to a committed SLICE.
--                          Slices are fixed, hour-aligned [start, start + slice) ranges (5 minutes by default).
--   coverage_start       : start of the audit history the pipeline is responsible for (reconciliation lower bound).
--   owner / lease_expire_time : single-writer lease; every mutation checks the owner and renews it, and the lease is
--                          verified before every write step (fencing of a paused instance).
--   pending_*            : the declared batch (id, kind SLICE | LATE | REPAIR | REBUILD, audit range) and its last
--                          COMPLETED step (INTENT, PREPARE, DEAD_LETTERS, DICTIONARY, USAGE, LINEAGE_STAGE,
--                          LINEAGE_DAILY, REFRESH, RESOLVE); a resumed batch continues after it.
--                          pending_since is the declaration time (label-retention guard and batch_time).
--   pending_attempts / last_error : failed attempts of the pending batch (alerting, operator decisions).
--   last_reconcile_time  : last reconciliation that found no late record.
--   pipeline_epoch       : random id of this checkpoint generation, part of every SLICE batch id. A pipeline that is
--                          reset (checkpoint deleted) and started again under the same name must not reproduce batch
--                          ids whose load labels StarRocks still remembers (label_keep_max_second, 3 days): every
--                          load would be dropped as "Label Already Exists". Found by the container tests.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_lineage_sync_checkpoint (
    pipeline_name       VARCHAR(64)    NOT NULL,
    committed_until     DATETIME       NOT NULL,
    coverage_start      DATETIME       NOT NULL,
    owner               VARCHAR(128)   NOT NULL,
    lease_expire_time   DATETIME       NOT NULL,
    processed_rows      BIGINT         NOT NULL DEFAULT "0",
    pending_batch_id    VARCHAR(64)    NULL,
    pending_kind        VARCHAR(16)    NULL,
    pending_start_time  DATETIME       NULL,
    pending_end_time    DATETIME       NULL,
    pending_step        VARCHAR(32)    NULL,
    pending_since       DATETIME       NULL,
    pending_attempts    INT            NULL,
    last_error          VARCHAR(1024)  NULL,
    last_reconcile_time DATETIME       NULL,
    update_time         DATETIME       NOT NULL,
    pipeline_epoch      VARCHAR(64)    NOT NULL
) ENGINE = OLAP
PRIMARY KEY (pipeline_name)
DISTRIBUTED BY HASH(pipeline_name) BUCKETS 1
PROPERTIES (
    "replication_num" = "3"
);
