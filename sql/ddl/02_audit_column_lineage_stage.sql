-- =====================================================================================================================
-- Staging table for lineage edges of one pipeline batch (Stream Load target).
--   * DUPLICATE KEY, partitioned by batch day, 3-day TTL through dynamic partitioning.
--   * The pipeline Stream-Loads one batch (pre-aggregated per edge_id in memory), then runs the MERGE statement
--     below in one transaction-free INSERT ... SELECT. Re-running the merge for the same batch_id is prevented by the
--     checkpoint table (03_audit_lineage_sync_checkpoint.sql): the checkpoint only advances after a successful merge,
--     and the merge of a batch is skipped when audit_lineage_merge_log already contains its batch_id.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_column_lineage_stage (
    batch_day        DATE           NOT NULL,
    batch_id         VARCHAR(64)    NOT NULL,
    edge_id          VARCHAR(64)    NOT NULL,
    target_catalog   VARCHAR(128)   NOT NULL,
    target_db        VARCHAR(256)   NOT NULL,
    target_table     VARCHAR(256)   NOT NULL,
    target_column    VARCHAR(256)   NOT NULL,
    source_catalog   VARCHAR(128)   NOT NULL,
    source_db        VARCHAR(256)   NOT NULL,
    source_table     VARCHAR(256)   NOT NULL,
    source_column    VARCHAR(256)   NOT NULL,
    lineage_type     VARCHAR(16)    NOT NULL,
    transform_type   VARCHAR(16)    NOT NULL,
    transform_expr   VARCHAR(2048)  NULL,
    stmt_type        VARCHAR(32)    NOT NULL,
    sql_digest       VARCHAR(64)    NULL,
    last_query_id    VARCHAR(64)    NOT NULL,
    last_exec_user   VARCHAR(128)   NOT NULL,
    first_seen_time  DATETIME       NOT NULL,
    last_seen_time   DATETIME       NOT NULL,
    exec_count       BIGINT         NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (batch_day, batch_id, edge_id)
PARTITION BY RANGE (batch_day) ()
DISTRIBUTED BY HASH(edge_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- ---------------------------------------------------------------------------------------------------------------------
-- MERGE of one staged batch into audit_column_lineage (executed by the pipeline, ${batch_id} bound as a parameter).
-- exec_count is accumulated, first_seen_time keeps the minimum, last_* columns follow the newest statement.
-- ---------------------------------------------------------------------------------------------------------------------
-- INSERT INTO audit_lineage_db.audit_column_lineage
-- SELECT s.edge_id,
--        s.target_catalog, s.target_db, s.target_table, s.target_column,
--        s.source_catalog, s.source_db, s.source_table, s.source_column,
--        s.lineage_type,
--        IF(o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time, s.transform_type, o.transform_type),
--        IF(o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time, s.transform_expr, o.transform_expr),
--        IF(o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time, s.stmt_type,      o.stmt_type),
--        IF(o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time, s.sql_digest,     o.sql_digest),
--        IF(o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time, s.last_query_id,  o.last_query_id),
--        IF(o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time, s.last_exec_user, o.last_exec_user),
--        IF(o.edge_id IS NULL, s.first_seen_time, LEAST(o.first_seen_time, s.first_seen_time)),
--        IF(o.edge_id IS NULL, s.last_seen_time,  GREATEST(o.last_seen_time, s.last_seen_time)),
--        IFNULL(o.exec_count, 0) + s.exec_count,
--        NOW()
-- FROM audit_lineage_db.audit_column_lineage_stage s
-- LEFT JOIN audit_lineage_db.audit_column_lineage o ON o.edge_id = s.edge_id
-- WHERE s.batch_id = ?;
