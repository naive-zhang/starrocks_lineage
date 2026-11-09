-- =====================================================================================================================
-- Staging table for lineage edges of one pipeline batch (Stream Load target).
--   * DUPLICATE KEY, partitioned by batch day, 3-day TTL through dynamic partitioning.
--   * The pipeline Stream-Loads one batch (pre-aggregated per edge_id in memory) with the label
--     <batch_id>_stage_<n> (a retried load is rejected as "Label Already Exists"), then runs the MERGE below.
--   * The MERGE is a single atomic INSERT ... SELECT guarded by last_batch_id <> batch_id, so replaying a batch
--     after a crash writes nothing (exactly-once without multi-statement transactions).
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
-- MERGE of one staged batch into audit_column_lineage (executed by StarRocksSink#mergeLineage; ? = update_time,
-- batch_id, batch_id). exec_count accumulates, first_seen_time keeps the minimum, last_* follow the newest statement.
-- ---------------------------------------------------------------------------------------------------------------------
-- INSERT INTO audit_lineage_db.audit_column_lineage (edge_id, target_catalog, target_db, target_table, target_column,
--        source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr,
--        stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count,
--        last_batch_id, update_time)
-- SELECT s.edge_id, s.target_catalog, s.target_db, s.target_table, s.target_column,
--        s.source_catalog, s.source_db, s.source_table, s.source_column, s.lineage_type,
--        CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_type ELSE o.transform_type END,
--        CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_expr ELSE o.transform_expr END,
--        CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.stmt_type      ELSE o.stmt_type      END,
--        CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.sql_digest     ELSE o.sql_digest     END,
--        CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_query_id  ELSE o.last_query_id  END,
--        CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_exec_user ELSE o.last_exec_user END,
--        CASE WHEN o.edge_id IS NULL OR s.first_seen_time < o.first_seen_time THEN s.first_seen_time ELSE o.first_seen_time END,
--        CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_seen_time ELSE o.last_seen_time END,
--        COALESCE(o.exec_count, 0) + s.exec_count,
--        s.batch_id, ?
-- FROM audit_lineage_db.audit_column_lineage_stage s
-- LEFT JOIN audit_lineage_db.audit_column_lineage o ON o.edge_id = s.edge_id
-- WHERE s.batch_id = ? AND (o.edge_id IS NULL OR o.last_batch_id IS NULL OR o.last_batch_id <> ?);
