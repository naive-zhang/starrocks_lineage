-- =====================================================================================================================
-- Smoke test step 6 (after the cycle that processed 04_scenario.sql): compare with the expected tables of
-- docs/staging-smoke-test.md section 6.
-- =====================================================================================================================

-- V1. checkpoint: pending_batch_id must be NULL, watermark at or after the last scenario statement
SELECT pipeline_name, watermark_time, watermark_query_id, owner, processed_rows, pending_batch_id
FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'audit_lineage_smoke';

-- V2. column usage of the scenario (5 rows expected)
SELECT table_name, column_name,
       SUM(access_query_cnt) AS access, SUM(select_clause_cnt) AS sel, SUM(where_clause_cnt) AS wh,
       SUM(join_clause_cnt) AS jn, SUM(group_by_cnt) AS gb, SUM(order_by_cnt) AS ob,
       BITMAP_UNION_COUNT(user_bitmap) AS uv, HLL_UNION_AGG(user_hll) AS uv_approx
FROM audit_lineage_db.audit_column_hourly_usage
WHERE catalog_name = 'default_catalog' AND db_name = 'lineage_smoke_db'
GROUP BY table_name, column_name
ORDER BY table_name, column_name;

-- V3. who read orders.amount (business query Q5 shape): exactly the smoke user
SELECT d.user_name
FROM (SELECT BITMAP_UNION(user_bitmap) AS bm FROM audit_lineage_db.audit_column_hourly_usage
      WHERE db_name = 'lineage_smoke_db' AND table_name = 'orders' AND column_name = 'amount') b
CROSS JOIN LATERAL UNNEST(BITMAP_TO_ARRAY(b.bm)) AS t(user_id)
JOIN audit_lineage_db.audit_user_dict d ON d.user_id = t.user_id;

-- V4. lineage of the scenario writes (8 rows expected)
SELECT target_table, target_column, source_table, source_column, lineage_type, transform_type, stmt_type,
       exec_count, last_exec_user
FROM audit_lineage_db.audit_column_lineage
WHERE target_db = 'lineage_smoke_db' AND target_table IN ('user_gmv', 'v_orders')
ORDER BY target_table, target_column, lineage_type, source_table, source_column;
