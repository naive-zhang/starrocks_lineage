-- =====================================================================================================================
-- Smoke test step 2 (run as an admin account): an isolated database and a dedicated user, so every usage / lineage
-- row of the scenario (04_scenario.sql) is attributable and has an exactly known expected value.
-- =====================================================================================================================
CREATE DATABASE IF NOT EXISTS lineage_smoke_db;

CREATE TABLE IF NOT EXISTS lineage_smoke_db.orders (
    order_id BIGINT, user_id BIGINT, amount DECIMAL(18, 2), dt DATE
) DUPLICATE KEY (order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS lineage_smoke_db.users (
    user_id BIGINT, name VARCHAR(64), city VARCHAR(64)
) DUPLICATE KEY (user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS lineage_smoke_db.user_gmv (
    uid BIGINT, gmv DECIMAL(38, 2)
) DUPLICATE KEY (uid) DISTRIBUTED BY HASH(uid) BUCKETS 1 PROPERTIES ("replication_num" = "1");

INSERT INTO lineage_smoke_db.orders VALUES (1, 10, 25.00, '2024-06-01'), (2, 11, 5.00, '2024-06-01');
INSERT INTO lineage_smoke_db.users VALUES (10, 'a', 'HZ'), (11, 'b', 'SH');

CREATE USER IF NOT EXISTS 'lineage_smoke_user'@'%' IDENTIFIED BY 'CHANGE_ME_SMOKE';
GRANT SELECT, INSERT ON ALL TABLES IN DATABASE lineage_smoke_db TO USER 'lineage_smoke_user'@'%';
GRANT CREATE VIEW ON DATABASE lineage_smoke_db TO USER 'lineage_smoke_user'@'%';
