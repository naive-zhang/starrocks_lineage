-- =====================================================================================================================
-- Lineage contribution of each batch (step LINEAGE_STAGE, one Stream Load, label <batch_id>_lineage).
--   * Frozen once loaded: the daily facts (02) are added from it with one labelled INSERT ... SELECT and the lineage
--     table (01) is refreshed for exactly its edges, so the steps after it never depend on a re-analysis.
--   * refresh_only = true rows (REBUILD) list the edges that HAD facts in the rebuilt days, so edges whose facts
--     disappear are refreshed (and deleted from 01) as well; they are never added to the daily facts.
--   * PRIMARY KEY (batch_id, edge_id, stat_day, refresh_only, batch_time): batch ids are SHA-256 of the pipeline name and
--     the batch definition, so the pipeline name is not part of the key (128-byte primary key limit).
--   * Partitioned by batch_time, 30 days: only needed until the batch is committed (plus audits).
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_lineage_batch_lineage (
    batch_id         VARCHAR(64)    NOT NULL,
    edge_id          VARCHAR(64)    NOT NULL,
    stat_day         DATETIME       NOT NULL,
    refresh_only     BOOLEAN        NOT NULL,
    batch_time       DATETIME       NOT NULL COMMENT "declaration time of the batch",
    pipeline_name    VARCHAR(64)    NOT NULL,
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
PRIMARY KEY (batch_id, edge_id, stat_day, refresh_only, batch_time)
COMMENT "Lineage contribution of each pipeline batch"
PARTITION BY date_trunc('day', batch_time)
DISTRIBUTED BY HASH(batch_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3",
    "partition_live_number" = "30"
);
