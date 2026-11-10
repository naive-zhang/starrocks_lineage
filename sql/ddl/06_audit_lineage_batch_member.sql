-- =====================================================================================================================
-- Batch membership: which batch processed which audit record.
--   * Written as the first step (PREPARE) of every SLICE / LATE batch, one Stream Load (label <batch_id>_members).
--   * A resumed batch reads EXACTLY its members back (join with the audit table), never a new range read: records that
--     arrive while a batch is in flight cannot change it (Stage 5, gap 5).
--   * Reconciliation finds late records with an anti join: audit records of committed time that are members of no
--     batch (gap 6).
--   * Rebuild recomputes days from the members of the pipeline (gap 7); it refuses when member audit records are gone.
--   * Retention = usage retention (400 days), so every day that has usage can be rebuilt while the audit table still
--     holds it.
--   * PRIMARY KEY (pipeline_name, query_id, audit_time): rewriting the membership is idempotent. Keep pipeline names
--     short: the encoded primary key is limited to 128 bytes.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_lineage_batch_member (
    pipeline_name    VARCHAR(64)    NOT NULL,
    query_id         VARCHAR(64)    NOT NULL COMMENT "audit queryId",
    audit_time       DATETIME       NOT NULL COMMENT "audit `timestamp`",
    batch_id         VARCHAR(64)    NOT NULL
) ENGINE = OLAP
PRIMARY KEY (pipeline_name, query_id, audit_time)
COMMENT "Audit records processed by each batch of a pipeline"
PARTITION BY date_trunc('day', audit_time)
DISTRIBUTED BY HASH(query_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "partition_live_number" = "400",
    "enable_persistent_index" = "true"
);
