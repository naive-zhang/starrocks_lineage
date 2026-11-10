-- =====================================================================================================================
-- Smoke test step 3 (run as lineage_etl, BEFORE the first pipeline cycle; audit_lineage_db is still empty).
-- Each block states the expected result. Stop and investigate on the first deviation.
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.1 Audit source: the exact query of JdbcAuditLogSource#fetchRange (literal bounds). Expected: up to 5 rows, no
--     error.
--     An "Unknown column" error means the audit loader plugin schema differs (e.g. no `catalog` column).
-- ---------------------------------------------------------------------------------------------------------------------
SELECT a.queryId, a.`timestamp`, a.queryType, a.clientIp, a.`user`, a.authorizedUser, a.`catalog`, a.db, a.state,
       a.errorCode, a.queryTime, a.scanBytes, a.scanRows, a.returnRows, a.cpuCostNs, a.memCostBytes, a.isQuery,
       LEFT(a.stmt, 60) AS stmt, a.digest
FROM starrocks_audit_db__.starrocks_audit_tbl__ a
WHERE a.`timestamp` >= DATE_SUB(NOW(), INTERVAL 1 HOUR) AND a.`timestamp` < NOW()
  AND (a.`timestamp` > DATE_SUB(NOW(), INTERVAL 1 HOUR)
       OR (a.`timestamp` = DATE_SUB(NOW(), INTERVAL 1 HOUR) AND a.queryId > ''))
ORDER BY a.`timestamp`, a.queryId LIMIT 5;

-- Reconciliation: the anti join of JdbcAuditLogSource#fetchUnprocessed. Expected: no error (rows = audit records of
-- the last hour that no batch of the smoke pipeline processed - all of them before the first cycle).
SELECT COUNT(*) AS unprocessed
FROM starrocks_audit_db__.starrocks_audit_tbl__ a
LEFT ANTI JOIN audit_lineage_db.audit_lineage_batch_member m
  ON m.query_id = a.queryId AND m.audit_time = a.`timestamp` AND m.pipeline_name = 'audit_lineage_smoke'
 AND m.audit_time >= DATE_SUB(NOW(), INTERVAL 1 HOUR) AND m.audit_time < NOW()
WHERE a.`timestamp` >= DATE_SUB(NOW(), INTERVAL 1 HOUR) AND a.`timestamp` < NOW();

-- Time zone: the pipeline compares audit timestamps with the JVM clock. Expected: same zone as the JVM
-- (-Duser.timezone=...) that will run the pipeline.
SHOW VARIABLES LIKE '%time_zone%';
SELECT NOW();

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.2 Metadata: the exact query of JdbcStarRocksMetadataProvider. Expected: order_id, user_id, amount, dt in order.
-- ---------------------------------------------------------------------------------------------------------------------
SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE
FROM information_schema.columns
WHERE TABLE_SCHEMA = 'lineage_smoke_db' AND TABLE_NAME = 'orders'
ORDER BY ORDINAL_POSITION;
-- For every external catalog used by audited queries (replace the names). Expected: the column list.
-- DESC `hive_catalog`.`some_db`.`some_table`;

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.3 Checkpoint table: INSERT must upsert on the PRIMARY KEY. Expected: 1 row, committed_until 2024-01-01 00:05:00,
--     pending batch 'b' at step USAGE.
-- ---------------------------------------------------------------------------------------------------------------------
INSERT INTO audit_lineage_db.audit_lineage_sync_checkpoint (pipeline_name, committed_until, coverage_start, owner,
    lease_expire_time, processed_rows, pending_batch_id, pending_kind, pending_start_time, pending_end_time,
    pending_step, pending_since, pending_attempts, last_error, last_reconcile_time, update_time, pipeline_epoch)
VALUES ('smoke_cp', '2024-01-01 00:00:00', '2024-01-01 00:00:00', 'o', NOW(), 0, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NOW(), 'e1');
INSERT INTO audit_lineage_db.audit_lineage_sync_checkpoint (pipeline_name, committed_until, coverage_start, owner,
    lease_expire_time, processed_rows, pending_batch_id, pending_kind, pending_start_time, pending_end_time,
    pending_step, pending_since, pending_attempts, last_error, last_reconcile_time, update_time, pipeline_epoch)
VALUES ('smoke_cp', '2024-01-01 00:05:00', '2024-01-01 00:00:00', 'o', NOW(), 1, 'b', 'SLICE', '2024-01-01 00:05:00',
        '2024-01-01 00:10:00', 'USAGE', NOW(), 0, NULL, NULL, NOW(), 'e1');
SELECT COUNT(*) AS rows_expect_1, MAX(committed_until) AS expect_0005, MAX(pending_batch_id) AS expect_b,
       MAX(pending_step) AS expect_usage
FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'smoke_cp';
DELETE FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'smoke_cp';

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.4 Lineage path: batch contribution -> daily facts (labelled INSERT) -> refresh of the lineage table. The two
--     pipeline statements are copied verbatim from StarRocksSink#insertDaily / #refreshLineage (parameters inlined).
--     Re-running this file fails at the first labelled INSERT with "Label [smoke_b1_daily_1] has already been used":
--     that IS the replay protection; to re-run, change the _1 suffixes.
-- ---------------------------------------------------------------------------------------------------------------------
-- (a) batch smoke_b1 (2 executions on 2024-01-01) -> Expected: exec_count=2, IDENTITY, q-b1
INSERT INTO audit_lineage_db.audit_lineage_batch_lineage (batch_id, edge_id, stat_day, refresh_only, batch_time, pipeline_name, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count)
VALUES ('smoke_b1', 'smoke_edge_1', '2024-01-01 00:00:00', false, NOW(), 'smoke', 'default_catalog', 'smoke', 't2', 'x', 'default_catalog', 'smoke', 't1', 'a', 'DIRECT', 'IDENTITY', 'a', 'INSERT_INTO', NULL, 'q-b1', 'smoke', '2024-01-01 10:00:00', '2024-01-01 10:00:00', 2);
INSERT INTO audit_lineage_db.audit_column_lineage_daily WITH LABEL smoke_b1_daily_1 (stat_day, edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count)
SELECT stat_day, edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count
FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b1' AND refresh_only = false;
INSERT INTO audit_lineage_db.audit_column_lineage (edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count, update_time)
SELECT a.edge_id, l.target_catalog, l.target_db, l.target_table, l.target_column, l.source_catalog, l.source_db, l.source_table, l.source_column, l.lineage_type, l.transform_type, l.transform_expr, l.stmt_type, l.sql_digest, l.last_query_id, l.last_exec_user, a.first_seen, a.last_seen, a.cnt, NOW()
FROM (SELECT edge_id, MIN(first_seen_time) AS first_seen, MAX(last_seen_time) AS last_seen, SUM(exec_count) AS cnt, MAX(stat_day) AS last_day
      FROM audit_lineage_db.audit_column_lineage_daily
      WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b1')
      GROUP BY edge_id) a
JOIN audit_lineage_db.audit_column_lineage_daily l ON l.edge_id = a.edge_id AND l.stat_day = a.last_day;
DELETE FROM audit_lineage_db.audit_column_lineage
WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b1')
  AND edge_id NOT IN (SELECT edge_id FROM audit_lineage_db.audit_column_lineage_daily
                      WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b1'));
SELECT exec_count, transform_type, last_query_id, first_seen_time, last_seen_time
FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';

-- (b) REPLAY of the refresh (crash before the step was recorded) -> Expected: unchanged, exec_count still 2
INSERT INTO audit_lineage_db.audit_column_lineage (edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count, update_time)
SELECT a.edge_id, l.target_catalog, l.target_db, l.target_table, l.target_column, l.source_catalog, l.source_db, l.source_table, l.source_column, l.lineage_type, l.transform_type, l.transform_expr, l.stmt_type, l.sql_digest, l.last_query_id, l.last_exec_user, a.first_seen, a.last_seen, a.cnt, NOW()
FROM (SELECT edge_id, MIN(first_seen_time) AS first_seen, MAX(last_seen_time) AS last_seen, SUM(exec_count) AS cnt, MAX(stat_day) AS last_day
      FROM audit_lineage_db.audit_column_lineage_daily
      WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b1')
      GROUP BY edge_id) a
JOIN audit_lineage_db.audit_column_lineage_daily l ON l.edge_id = a.edge_id AND l.stat_day = a.last_day;
DELETE FROM audit_lineage_db.audit_column_lineage
WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b1')
  AND edge_id NOT IN (SELECT edge_id FROM audit_lineage_db.audit_column_lineage_daily
                      WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b1'));
SELECT exec_count, transform_type, last_query_id, first_seen_time, last_seen_time
FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';

-- (c) batch smoke_b2 (3 executions on 2024-01-02, newer statement) -> Expected: exec_count=5, EXPRESSION, q-b2,
--     first_seen 2024-01-01 10:00:00, last_seen 2024-01-02 10:00:00; still ONE row for the edge, two fact rows
INSERT INTO audit_lineage_db.audit_lineage_batch_lineage (batch_id, edge_id, stat_day, refresh_only, batch_time, pipeline_name, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count)
VALUES ('smoke_b2', 'smoke_edge_1', '2024-01-02 00:00:00', false, NOW(), 'smoke', 'default_catalog', 'smoke', 't2', 'x', 'default_catalog', 'smoke', 't1', 'a', 'DIRECT', 'EXPRESSION', 'a', 'INSERT_INTO', NULL, 'q-b2', 'smoke', '2024-01-02 09:00:00', '2024-01-02 10:00:00', 3);
INSERT INTO audit_lineage_db.audit_column_lineage_daily WITH LABEL smoke_b2_daily_1 (stat_day, edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count)
SELECT stat_day, edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count
FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b2' AND refresh_only = false;
INSERT INTO audit_lineage_db.audit_column_lineage (edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count, update_time)
SELECT a.edge_id, l.target_catalog, l.target_db, l.target_table, l.target_column, l.source_catalog, l.source_db, l.source_table, l.source_column, l.lineage_type, l.transform_type, l.transform_expr, l.stmt_type, l.sql_digest, l.last_query_id, l.last_exec_user, a.first_seen, a.last_seen, a.cnt, NOW()
FROM (SELECT edge_id, MIN(first_seen_time) AS first_seen, MAX(last_seen_time) AS last_seen, SUM(exec_count) AS cnt, MAX(stat_day) AS last_day
      FROM audit_lineage_db.audit_column_lineage_daily
      WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b2')
      GROUP BY edge_id) a
JOIN audit_lineage_db.audit_column_lineage_daily l ON l.edge_id = a.edge_id AND l.stat_day = a.last_day;
DELETE FROM audit_lineage_db.audit_column_lineage
WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b2')
  AND edge_id NOT IN (SELECT edge_id FROM audit_lineage_db.audit_column_lineage_daily
                      WHERE edge_id IN (SELECT edge_id FROM audit_lineage_db.audit_lineage_batch_lineage WHERE pipeline_name = 'smoke' AND batch_id = 'smoke_b2'));
SELECT exec_count, transform_type, last_query_id, first_seen_time, last_seen_time
FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';
SELECT COUNT(*) AS rows_expect_1 FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';
SELECT COUNT(*) AS fact_rows_expect_2 FROM audit_lineage_db.audit_column_lineage_daily WHERE edge_id = 'smoke_edge_1';

-- (d) clean up the synthetic edge
DELETE FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';
DELETE FROM audit_lineage_db.audit_column_lineage_daily WHERE stat_day >= '2024-01-01' AND stat_day < '2024-01-03';
DELETE FROM audit_lineage_db.audit_lineage_batch_lineage WHERE batch_id IN ('smoke_b1', 'smoke_b2');

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.5 After running stream_load_check.sh: bitmap / HLL mapping of the usage load.
--     Expected: access=2, uv_exact=2, uv_approx=2 (and unchanged after the duplicate-label reload).
-- ---------------------------------------------------------------------------------------------------------------------
SELECT SUM(access_query_cnt) AS access, BITMAP_UNION_COUNT(user_bitmap) AS uv_exact,
       HLL_UNION_AGG(user_hll) AS uv_approx, BITMAP_TO_STRING(BITMAP_UNION(user_bitmap)) AS ids_expect_1_2
FROM audit_lineage_db.audit_column_hourly_usage WHERE catalog_name = '__smoke__';
SELECT * FROM audit_lineage_db.audit_user_dict WHERE user_name LIKE '__smoke_%';

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.6 Reset the synthetic rows (tables only hold smoke data at this point - do NOT run once real data is loaded).
-- ---------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE audit_lineage_db.audit_column_hourly_usage;
TRUNCATE TABLE audit_lineage_db.audit_user_dict;
