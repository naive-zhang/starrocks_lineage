-- =====================================================================================================================
-- Daily lineage facts: one row per (day of the audit timestamp, edge). Source of truth of audit_column_lineage.
--
-- Model     : AGGREGATE KEY (stat_day, edge_id). exec_count SUM, first_seen_time MIN, last_seen_time MAX; the
--             descriptive attributes are REPLACE (last loaded batch of the day wins - StarRocks 3.1 has no
--             "value of the max last_seen_time" aggregate; they are constant per edge except transform / statement
--             details).
-- Writes    : one labelled INSERT ... SELECT per batch from audit_lineage_batch_lineage (07)
--             (label <batch_id>_daily: a replay inside the FE label retention is rejected as "already used"; after
--             it the pipeline asks get_load_state and blocks instead of guessing).
-- Rebuild   : a day range is DELETEd (key range) and recomputed from the batch membership (06).
-- Partition : expression partitioning, partitions are created by the loads themselves for ANY day - a first run that
--             starts yesterday, a late record from last week and a rebuild of last month all have a partition;
--             partition_live_number keeps 400 days.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_column_lineage_daily (
    stat_day         DATETIME       NOT NULL COMMENT "day of the audit timestamp (00:00:00)",
    edge_id          VARCHAR(64)    NOT NULL COMMENT "sha256(target4, source4, lineage_type)",
    target_catalog   VARCHAR(128)   REPLACE  NOT NULL,
    target_db        VARCHAR(256)   REPLACE  NOT NULL,
    target_table     VARCHAR(256)   REPLACE  NOT NULL,
    target_column    VARCHAR(256)   REPLACE  NOT NULL,
    source_catalog   VARCHAR(128)   REPLACE  NOT NULL,
    source_db        VARCHAR(256)   REPLACE  NOT NULL,
    source_table     VARCHAR(256)   REPLACE  NOT NULL,
    source_column    VARCHAR(256)   REPLACE  NOT NULL,
    lineage_type     VARCHAR(16)    REPLACE  NOT NULL,
    transform_type   VARCHAR(16)    REPLACE  NOT NULL,
    transform_expr   VARCHAR(2048)  REPLACE  NULL,
    stmt_type        VARCHAR(32)    REPLACE  NOT NULL,
    sql_digest       VARCHAR(64)    REPLACE  NULL,
    last_query_id    VARCHAR(64)    REPLACE  NOT NULL,
    last_exec_user   VARCHAR(128)   REPLACE  NOT NULL,
    first_seen_time  DATETIME       MIN      NOT NULL,
    last_seen_time   DATETIME       MAX      NOT NULL,
    exec_count       BIGINT         SUM      NOT NULL DEFAULT "0"
) ENGINE = OLAP
AGGREGATE KEY (stat_day, edge_id)
COMMENT "Daily column lineage facts (exec_count per day) - audit_column_lineage is recomputed from it"
PARTITION BY date_trunc('day', stat_day)
DISTRIBUTED BY HASH(edge_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "partition_live_number" = "400",
    "compression" = "LZ4"
);
