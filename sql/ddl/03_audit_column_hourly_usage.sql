-- =====================================================================================================================
-- Target table 2: hourly column usage of pure QUERY statements (top level queryStatement)
--
-- Model     : AGGREGATE KEY (stat_hour, catalog_name, db_name, table_name, column_name)
-- Partition : expression partitioning date_trunc('day', stat_hour): a load creates the partition of ANY day it
--             touches (first run starting yesterday, late records, rebuilds), partition_live_number keeps 400 days.
--             Dynamic partitions only pre-create today..+N and rejected yesterday's rows (Stage 5, gap 1).
-- Bucketing : HASH(db_name, table_name) - every column of one table lands in the same tablet so the dominant query
--             "usage of table X during the last N days" touches one tablet per partition.
--
-- Counting semantics (identical to ColumnUsageStat):
--   access_query_cnt : number of queries that accessed the column (a column repeated inside one query counts once)
--   *_clause_cnt     : number of queries that used the column at least once in that clause
--                      group_by_cnt includes HAVING and window PARTITION BY,
--                      order_by_cnt includes window ORDER BY, where_clause_cnt includes QUALIFY
--   user_bitmap      : exact distinct users - to_bitmap(user_id), user_id = first 63 bits of SHA-256(user)
--                      (UserIds; stateless, identical on every instance; StarRocks 3.1 has no bitmap_hash64 and
--                      bitmap_hash is 32-bit and collides, see review R-04)
--   user_hll         : approximate distinct users - hll_hash(user)
--
-- Additivity        : the usage load of a batch is the only non-idempotent write of the usage path. It is ONE Stream
--                     Load per batch (label <batch_id>_usage); a replay after the FE label retention is decided with
--                     get_load_state, never guessed (Stage 5, gap 4).
-- Load contract     : the pipeline emits ONE ROW PER (stat_hour, column, user) with that user's subtotals. SUM metrics
--                     add up to the per-column totals and the bitmap/HLL of a single user merge into the column UV.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_column_hourly_usage (
    stat_hour          DATETIME       NOT NULL COMMENT "hour bucket of the audit timestamp, minute/second = 0",
    catalog_name       VARCHAR(128)   NOT NULL COMMENT "catalog, lower-case",
    db_name            VARCHAR(256)   NOT NULL COMMENT "database (case preserved)",
    table_name         VARCHAR(256)   NOT NULL COMMENT "table / view (case preserved)",
    column_name        VARCHAR(256)   NOT NULL COMMENT "column, lower-case",
    access_query_cnt   BIGINT         SUM     DEFAULT "0" COMMENT "queries accessing the column",
    select_clause_cnt  BIGINT         SUM     DEFAULT "0" COMMENT "queries using the column in SELECT",
    where_clause_cnt   BIGINT         SUM     DEFAULT "0" COMMENT "queries using the column in WHERE / QUALIFY",
    join_clause_cnt    BIGINT         SUM     DEFAULT "0" COMMENT "queries using the column in JOIN ON / USING",
    group_by_cnt       BIGINT         SUM     DEFAULT "0" COMMENT "queries using the column in GROUP BY / HAVING / PARTITION BY",
    order_by_cnt       BIGINT         SUM     DEFAULT "0" COMMENT "queries using the column in ORDER BY / window ORDER BY",
    user_bitmap        BITMAP         BITMAP_UNION        COMMENT "exact distinct users (UserIds.idOf(user))",
    user_hll           HLL            HLL_UNION           COMMENT "approximate distinct users (hll_hash(user))",
    total_scan_bytes   BIGINT         SUM     DEFAULT "0" COMMENT "sum of scanBytes of the accessing queries",
    total_query_time_ms BIGINT        SUM     DEFAULT "0" COMMENT "sum of queryTime of the accessing queries",
    max_query_time_ms  BIGINT         MAX     DEFAULT "0" COMMENT "max queryTime of the accessing queries",
    last_access_time   DATETIME       MAX                 COMMENT "latest audit timestamp in the hour"
) ENGINE = OLAP
AGGREGATE KEY (stat_hour, catalog_name, db_name, table_name, column_name)
COMMENT "Hourly column access statistics extracted from starrocks_audit_db__.starrocks_audit_tbl__"
PARTITION BY date_trunc('day', stat_hour)
DISTRIBUTED BY HASH(db_name, table_name) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "partition_live_number" = "400",
    "compression" = "LZ4"
);
