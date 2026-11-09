-- =====================================================================================================================
-- Smoke test step 6: run as lineage_smoke_user, in this order, all within a few minutes.
-- Every statement is fully qualified, so the session database does not matter.
-- F1 is EXPECTED TO FAIL (it must be filtered as state = 'ERR'); M1 is a management statement (skipped).
-- =====================================================================================================================
-- S1
SELECT amount, amount + 1 FROM lineage_smoke_db.orders WHERE amount > 10;
-- S2 (same statement again: access_query_cnt must become 2, not 4)
SELECT amount, amount + 1 FROM lineage_smoke_db.orders WHERE amount > 10;
-- S3
SELECT o.order_id FROM lineage_smoke_db.orders o
WHERE EXISTS (SELECT 1 FROM lineage_smoke_db.users u WHERE u.user_id = o.user_id AND u.city = 'HZ');
-- S4
SELECT city, COUNT(*) FROM lineage_smoke_db.users GROUP BY city ORDER BY city;
-- W1
INSERT INTO lineage_smoke_db.user_gmv SELECT user_id, SUM(amount) FROM lineage_smoke_db.orders GROUP BY user_id;
-- W2
CREATE VIEW lineage_smoke_db.v_orders AS
SELECT order_id, amount * 2 AS amt2 FROM lineage_smoke_db.orders WHERE dt >= '2024-01-01';
-- F1 (expected error: Column 'no_such_col' cannot be resolved)
SELECT no_such_col FROM lineage_smoke_db.orders;
-- M1
SHOW TABLES FROM lineage_smoke_db;
