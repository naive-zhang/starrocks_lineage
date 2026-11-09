-- =====================================================================================================================
-- Smoke test step 3 (run as lineage_etl, BEFORE the first pipeline cycle; audit_lineage_db is still empty).
-- Each block states the expected result. Stop and investigate on the first deviation.
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.1 Audit source: the exact query of JdbcAuditLogSource (literal bounds). Expected: up to 5 rows, no error.
--     An "Unknown column" error means the audit loader plugin schema differs (e.g. no `catalog` column).
-- ---------------------------------------------------------------------------------------------------------------------
SELECT queryId, `timestamp`, queryType, clientIp, `user`, authorizedUser, `catalog`, db, state, errorCode, queryTime,
       scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, isQuery, LEFT(stmt, 60) AS stmt, digest
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE `timestamp` >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
  AND (`timestamp` > DATE_SUB(NOW(), INTERVAL 1 HOUR)
       OR (`timestamp` = DATE_SUB(NOW(), INTERVAL 1 HOUR) AND queryId > ''))
  AND `timestamp` < NOW()
ORDER BY `timestamp`, queryId LIMIT 5;

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
-- 3.3 Checkpoint table: INSERT must upsert on the PRIMARY KEY. Expected: 1 row with watermark_query_id = 'q2'.
-- ---------------------------------------------------------------------------------------------------------------------
INSERT INTO audit_lineage_db.audit_lineage_sync_checkpoint (pipeline_name, watermark_time, watermark_query_id, owner,
    lease_expire_time, processed_rows, pending_batch_id, pending_end_time, pending_end_query_id, update_time)
VALUES ('smoke_cp', '2024-01-01 00:00:00', 'q1', 'o', NOW(), 0, NULL, NULL, NULL, NOW());
INSERT INTO audit_lineage_db.audit_lineage_sync_checkpoint (pipeline_name, watermark_time, watermark_query_id, owner,
    lease_expire_time, processed_rows, pending_batch_id, pending_end_time, pending_end_query_id, update_time)
VALUES ('smoke_cp', '2024-01-01 00:00:01', 'q2', 'o', NOW(), 1, 'b', '2024-01-01 00:00:02', 'q3', NOW());
SELECT COUNT(*) AS rows_expect_1, MAX(watermark_query_id) AS expect_q2, MAX(pending_batch_id) AS expect_b
FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'smoke_cp';
DELETE FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'smoke_cp';

-- ---------------------------------------------------------------------------------------------------------------------
-- 3.4 Lineage merge (statement copied verbatim from StarRocksSink#mergeLineage, parameters inlined).
-- ---------------------------------------------------------------------------------------------------------------------
-- (a) batch smoke_b1 -> Expected: "1 row affected"; then exec_count=2, IDENTITY, q-b1, last_batch_id=smoke_b1
INSERT INTO audit_lineage_db.audit_column_lineage_stage (batch_day, batch_id, edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count)
VALUES (CURDATE(), 'smoke_b1', 'smoke_edge_1', 'default_catalog', 'smoke', 't2', 'x', 'default_catalog', 'smoke', 't1', 'a', 'DIRECT', 'IDENTITY', 'a', 'INSERT_INTO', NULL, 'q-b1', 'smoke', '2024-01-01 10:00:00', '2024-01-01 10:00:00', 2);
INSERT INTO audit_lineage_db.audit_column_lineage (edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count, last_batch_id, update_time)
SELECT s.edge_id, s.target_catalog, s.target_db, s.target_table, s.target_column,
       s.source_catalog, s.source_db, s.source_table, s.source_column, s.lineage_type,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_type ELSE o.transform_type END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_expr ELSE o.transform_expr END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.stmt_type ELSE o.stmt_type END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.sql_digest ELSE o.sql_digest END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_query_id ELSE o.last_query_id END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_exec_user ELSE o.last_exec_user END,
       CASE WHEN o.edge_id IS NULL OR s.first_seen_time < o.first_seen_time THEN s.first_seen_time ELSE o.first_seen_time END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_seen_time ELSE o.last_seen_time END,
       COALESCE(o.exec_count, 0) + s.exec_count, s.batch_id, NOW()
FROM audit_lineage_db.audit_column_lineage_stage s
LEFT JOIN audit_lineage_db.audit_column_lineage o ON o.edge_id = s.edge_id
WHERE s.batch_id = 'smoke_b1' AND (o.edge_id IS NULL OR o.last_batch_id IS NULL OR o.last_batch_id <> 'smoke_b1');
SELECT exec_count, transform_type, last_query_id, first_seen_time, last_seen_time, last_batch_id
FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';

-- (b) REPLAY of smoke_b1 (crash before the checkpoint) -> Expected: "0 rows affected"; exec_count still 2
INSERT INTO audit_lineage_db.audit_column_lineage (edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count, last_batch_id, update_time)
SELECT s.edge_id, s.target_catalog, s.target_db, s.target_table, s.target_column,
       s.source_catalog, s.source_db, s.source_table, s.source_column, s.lineage_type,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_type ELSE o.transform_type END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_expr ELSE o.transform_expr END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.stmt_type ELSE o.stmt_type END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.sql_digest ELSE o.sql_digest END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_query_id ELSE o.last_query_id END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_exec_user ELSE o.last_exec_user END,
       CASE WHEN o.edge_id IS NULL OR s.first_seen_time < o.first_seen_time THEN s.first_seen_time ELSE o.first_seen_time END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_seen_time ELSE o.last_seen_time END,
       COALESCE(o.exec_count, 0) + s.exec_count, s.batch_id, NOW()
FROM audit_lineage_db.audit_column_lineage_stage s
LEFT JOIN audit_lineage_db.audit_column_lineage o ON o.edge_id = s.edge_id
WHERE s.batch_id = 'smoke_b1' AND (o.edge_id IS NULL OR o.last_batch_id IS NULL OR o.last_batch_id <> 'smoke_b1');
SELECT exec_count, transform_type, last_query_id, first_seen_time, last_seen_time, last_batch_id
FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';

-- (c) batch smoke_b2, newer statement -> Expected: exec_count=5, EXPRESSION, q-b2, first_seen 2023-12-31 10:00:00,
--     last_seen 2024-01-02 10:00:00, last_batch_id=smoke_b2; still ONE row for the edge (PK upsert)
INSERT INTO audit_lineage_db.audit_column_lineage_stage (batch_day, batch_id, edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count)
VALUES (CURDATE(), 'smoke_b2', 'smoke_edge_1', 'default_catalog', 'smoke', 't2', 'x', 'default_catalog', 'smoke', 't1', 'a', 'DIRECT', 'EXPRESSION', 'a', 'INSERT_INTO', NULL, 'q-b2', 'smoke', '2023-12-31 10:00:00', '2024-01-02 10:00:00', 3);
INSERT INTO audit_lineage_db.audit_column_lineage (edge_id, target_catalog, target_db, target_table, target_column, source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count, last_batch_id, update_time)
SELECT s.edge_id, s.target_catalog, s.target_db, s.target_table, s.target_column,
       s.source_catalog, s.source_db, s.source_table, s.source_column, s.lineage_type,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_type ELSE o.transform_type END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.transform_expr ELSE o.transform_expr END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.stmt_type ELSE o.stmt_type END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.sql_digest ELSE o.sql_digest END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_query_id ELSE o.last_query_id END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_exec_user ELSE o.last_exec_user END,
       CASE WHEN o.edge_id IS NULL OR s.first_seen_time < o.first_seen_time THEN s.first_seen_time ELSE o.first_seen_time END,
       CASE WHEN o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time THEN s.last_seen_time ELSE o.last_seen_time END,
       COALESCE(o.exec_count, 0) + s.exec_count, s.batch_id, NOW()
FROM audit_lineage_db.audit_column_lineage_stage s
LEFT JOIN audit_lineage_db.audit_column_lineage o ON o.edge_id = s.edge_id
WHERE s.batch_id = 'smoke_b2' AND (o.edge_id IS NULL OR o.last_batch_id IS NULL OR o.last_batch_id <> 'smoke_b2');
SELECT exec_count, transform_type, last_query_id, first_seen_time, last_seen_time, last_batch_id
FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';
SELECT COUNT(*) AS rows_expect_1 FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';

-- (d) clean up the synthetic edge (the stage table is truncated in step 3.6)
DELETE FROM audit_lineage_db.audit_column_lineage WHERE edge_id = 'smoke_edge_1';

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
TRUNCATE TABLE audit_lineage_db.audit_column_lineage_stage;
TRUNCATE TABLE audit_lineage_db.audit_user_dict;
