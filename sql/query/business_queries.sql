-- =====================================================================================================================
-- Standard business queries (StarRocks 3.1.x). Parameters are written as ${name}.
-- =====================================================================================================================

-- Q1. Hourly access of one column: access count, per-clause frequency, distinct users (exact and approximate),
--     resource water levels.
SELECT stat_hour,
       SUM(access_query_cnt)                 AS access_query_cnt,
       SUM(select_clause_cnt)                AS select_clause_cnt,
       SUM(where_clause_cnt)                 AS where_clause_cnt,
       SUM(join_clause_cnt)                  AS join_clause_cnt,
       SUM(group_by_cnt)                     AS group_by_cnt,
       SUM(order_by_cnt)                     AS order_by_cnt,
       BITMAP_UNION_COUNT(user_bitmap)       AS distinct_users,
       HLL_UNION_AGG(user_hll)               AS distinct_users_approx,
       SUM(total_scan_bytes)                 AS total_scan_bytes,
       MAX(max_query_time_ms)                AS max_query_time_ms
FROM audit_lineage_db.audit_column_hourly_usage
WHERE stat_hour >= '${start_time}' AND stat_hour < '${end_time}'
  AND catalog_name = '${catalog}' AND db_name = '${db}' AND table_name = '${table}' AND column_name = '${column}'
GROUP BY stat_hour
ORDER BY stat_hour;

-- Q2. Daily roll-up for every column of one table with exact UV across hours (bitmap union is lossless).
SELECT DATE(stat_hour)                       AS stat_date,
       column_name,
       SUM(access_query_cnt)                 AS access_query_cnt,
       SUM(where_clause_cnt)                 AS where_clause_cnt,
       SUM(join_clause_cnt)                  AS join_clause_cnt,
       BITMAP_UNION_COUNT(user_bitmap)       AS distinct_users
FROM audit_lineage_db.audit_column_hourly_usage
WHERE stat_hour >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
  AND catalog_name = '${catalog}' AND db_name = '${db}' AND table_name = '${table}'
GROUP BY DATE(stat_hour), column_name
ORDER BY stat_date, access_query_cnt DESC;

-- Q3. Top 50 hottest columns of the last 30 days, with exact UV over the whole window.
SELECT catalog_name, db_name, table_name, column_name,
       SUM(access_query_cnt)                 AS access_query_cnt,
       BITMAP_UNION_COUNT(user_bitmap)       AS distinct_users,
       SUM(total_scan_bytes)                 AS total_scan_bytes
FROM audit_lineage_db.audit_column_hourly_usage
WHERE stat_hour >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY catalog_name, db_name, table_name, column_name
ORDER BY access_query_cnt DESC
LIMIT 50;

-- Q4. Cold columns: default_catalog columns never read during the last 90 days (candidates for clean-up).
SELECT c.TABLE_SCHEMA AS db_name, c.TABLE_NAME AS table_name, c.COLUMN_NAME AS column_name
FROM information_schema.columns c
LEFT JOIN (
    SELECT DISTINCT db_name, table_name, column_name
    FROM audit_lineage_db.audit_column_hourly_usage
    WHERE stat_hour >= DATE_SUB(NOW(), INTERVAL 90 DAY) AND catalog_name = 'default_catalog'
) u ON u.db_name = c.TABLE_SCHEMA AND u.table_name = c.TABLE_NAME AND u.column_name = LOWER(c.COLUMN_NAME)
WHERE c.TABLE_SCHEMA NOT IN ('information_schema', '_statistics_', 'starrocks_audit_db__', 'audit_lineage_db')
  AND u.column_name IS NULL
ORDER BY db_name, table_name, column_name;

-- Q5. Which users read a column during a day: decode the bitmap through the user dictionary.
--     The join chain is written left-deep (b CROSS JOIN LATERAL UNNEST ... JOIN d): in "b, UNNEST(...) JOIN d" the
--     JOIN binds to UNNEST first and b is not visible to it (verified on StarRocks 3.1.17).
SELECT d.user_name
FROM (
    SELECT BITMAP_UNION(user_bitmap) AS bm
    FROM audit_lineage_db.audit_column_hourly_usage
    WHERE stat_hour >= '${day}' AND stat_hour < DATE_ADD('${day}', INTERVAL 1 DAY)
      AND catalog_name = '${catalog}' AND db_name = '${db}' AND table_name = '${table}' AND column_name = '${column}'
) b
CROSS JOIN LATERAL UNNEST(BITMAP_TO_ARRAY(b.bm)) AS t(user_id)
JOIN audit_lineage_db.audit_user_dict d ON d.user_id = t.user_id
ORDER BY d.user_name;

-- Q6. Upstream lineage of one target column (one hop).
SELECT source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr,
       stmt_type, exec_count, last_seen_time, last_exec_user
FROM audit_lineage_db.audit_column_lineage
WHERE target_db = '${db}' AND target_table = '${table}' AND target_column = '${column}'
  AND target_catalog = '${catalog}'
ORDER BY lineage_type, source_db, source_table, source_column;

-- Q7. Downstream impact of one source column (one hop) - bloom filter on source_table / source_column.
SELECT target_catalog, target_db, target_table, target_column, lineage_type, transform_type, last_seen_time
FROM audit_lineage_db.audit_column_lineage
WHERE source_catalog = '${catalog}' AND source_db = '${db}' AND source_table = '${table}'
  AND source_column = '${column}'
ORDER BY target_db, target_table, target_column;

-- Q8. Multi-hop upstream (up to 5 hops) of a column, DIRECT edges only, via recursive expansion in the application:
--     StarRocks 3.1 has no recursive CTE, so the service repeats Q6 breadth-first with a visited set.
