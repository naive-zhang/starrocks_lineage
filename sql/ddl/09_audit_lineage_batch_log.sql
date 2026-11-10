-- =====================================================================================================================
-- One row per committed batch (observability and audits).
--   SELECT kind, COUNT(*), SUM(records), SUM(dead_letters), SUM(filtered_rows), MAX(attempts)
--   FROM audit_lineage_db.audit_lineage_batch_log WHERE committed_time >= CURRENT_DATE() GROUP BY kind;
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_lineage_batch_log (
    batch_id         VARCHAR(64)    NOT NULL,
    pipeline_name    VARCHAR(64)    NOT NULL,
    kind             VARCHAR(16)    NOT NULL,
    range_start      DATETIME       NULL,
    range_end        DATETIME       NULL,
    records          INT            NOT NULL,
    succeeded        INT            NOT NULL,
    skipped          INT            NOT NULL,
    failed           INT            NOT NULL,
    degraded         INT            NOT NULL,
    usage_rows       INT            NOT NULL,
    lineage_rows     INT            NOT NULL,
    dead_letters     INT            NOT NULL,
    filtered_rows    BIGINT         NOT NULL,
    attempts         INT            NOT NULL,
    started_time     DATETIME       NULL,
    committed_time   DATETIME       NOT NULL
) ENGINE = OLAP
PRIMARY KEY (batch_id)
DISTRIBUTED BY HASH(batch_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "3"
);
