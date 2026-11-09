-- =====================================================================================================================
-- Target table 1: column level lineage of WRITE statements
--   INSERT INTO / INSERT OVERWRITE, CTAS, CREATE / ALTER VIEW, CREATE MATERIALIZED VIEW, UPDATE, DELETE, SUBMIT TASK
--
-- Model    : PRIMARY KEY (StarRocks >= 3.0 : primary key and sort key decoupled with ORDER BY).
-- Key      : edge_id = SHA-256 hex of (target 4-tuple, source 4-tuple, lineage_type), computed by
--            ColumnLineageEdge#edgeId(). The natural 9-column key (up to 9 x 256 bytes) violates the
--            128-byte encoded primary key limit of StarRocks 3.1, a fixed 64-byte hash does not.
-- Sort key : (target_db, target_table, target_column) so "where does this column come from" is a prefix-index scan.
-- Counters : exec_count / first_seen_time are merged by the pipeline through audit_column_lineage_stage
--            (see 02_audit_column_lineage_stage.sql) because a Primary Key table cannot SUM on upsert.
-- Replay   : last_batch_id makes the merge idempotent: a batch that was already merged is skipped by the merge
--            statement itself, so a crash between merge and merge log cannot double exec_count.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_column_lineage (
    edge_id          VARCHAR(64)    NOT NULL COMMENT "sha256(target4, source4, lineage_type) - physical primary key",
    target_catalog   VARCHAR(128)   NOT NULL COMMENT "target catalog, lower-case",
    target_db        VARCHAR(256)   NOT NULL COMMENT "target database (case preserved)",
    target_table     VARCHAR(256)   NOT NULL COMMENT "target table / view / materialized view (case preserved)",
    target_column    VARCHAR(256)   NOT NULL COMMENT "target column, lower-case",
    source_catalog   VARCHAR(128)   NOT NULL COMMENT "source catalog, '__constant__' for literal sources",
    source_db        VARCHAR(256)   NOT NULL COMMENT "source database, '__constant__' for literal sources",
    source_table     VARCHAR(256)   NOT NULL COMMENT "source table, '__constant__' for literal sources",
    source_column    VARCHAR(256)   NOT NULL COMMENT "source column, '__constant__' for literal sources",
    lineage_type     VARCHAR(16)    NOT NULL COMMENT "DIRECT | INDIRECT",
    transform_type   VARCHAR(16)    NOT NULL COMMENT "IDENTITY | EXPRESSION | AGGREGATION | WINDOW | CONSTANT",
    transform_expr   VARCHAR(2048)  NULL     COMMENT "expression text producing the target (truncated to 2048)",
    stmt_type        VARCHAR(32)    NOT NULL COMMENT "INSERT_INTO | INSERT_OVERWRITE | CREATE_TABLE_AS_SELECT | ...",
    sql_digest       VARCHAR(64)    NULL     COMMENT "audit digest of the last statement producing the edge",
    last_query_id    VARCHAR(64)    NOT NULL COMMENT "audit queryId of the last statement producing the edge",
    last_exec_user   VARCHAR(128)   NOT NULL COMMENT "user of the last statement producing the edge",
    first_seen_time  DATETIME       NOT NULL COMMENT "audit timestamp of the first statement producing the edge",
    last_seen_time   DATETIME       NOT NULL COMMENT "audit timestamp of the last statement producing the edge",
    exec_count       BIGINT         NOT NULL DEFAULT "0" COMMENT "number of successful statements producing the edge",
    last_batch_id    VARCHAR(64)    NULL     COMMENT "pipeline batch of the last merge - replay guard (Stage 4 R4-01)",
    update_time      DATETIME       NOT NULL COMMENT "wall clock time of the last merge"
) ENGINE = OLAP
PRIMARY KEY (edge_id)
COMMENT "Column level lineage extracted from starrocks_audit_db__.starrocks_audit_tbl__"
DISTRIBUTED BY HASH(edge_id) BUCKETS 8
ORDER BY (target_db, target_table, target_column)
PROPERTIES (
    "replication_num" = "3",
    "enable_persistent_index" = "true",
    "bloom_filter_columns" = "source_table, source_column, target_table",
    "compression" = "LZ4"
);
