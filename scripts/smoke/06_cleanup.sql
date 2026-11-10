-- =====================================================================================================================
-- Smoke test step 9 (run as an admin account): remove the smoke objects and everything the smoke pipeline wrote.
-- =====================================================================================================================
DROP DATABASE IF EXISTS lineage_smoke_db FORCE;
DROP USER IF EXISTS 'lineage_smoke_user'@'%';
DELETE FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'audit_lineage_smoke';
DELETE FROM audit_lineage_db.audit_lineage_batch_member WHERE pipeline_name = 'audit_lineage_smoke';
DELETE FROM audit_lineage_db.audit_lineage_dead_letter WHERE pipeline_name = 'audit_lineage_smoke';
DELETE FROM audit_lineage_db.audit_lineage_batch_log WHERE pipeline_name = 'audit_lineage_smoke';
DELETE FROM audit_lineage_db.audit_lineage_batch_lineage WHERE batch_time >= '2000-01-01'
    AND pipeline_name = 'audit_lineage_smoke';
DELETE FROM audit_lineage_db.audit_column_lineage WHERE target_db = 'lineage_smoke_db';
-- Usage and daily facts of lineage_smoke_db are harmless (the database no longer exists). To start production from a
-- clean slate instead, empty everything the smoke run wrote:
-- TRUNCATE TABLE audit_lineage_db.audit_column_hourly_usage;
-- TRUNCATE TABLE audit_lineage_db.audit_column_lineage_daily;
-- TRUNCATE TABLE audit_lineage_db.audit_column_lineage;
-- TRUNCATE TABLE audit_lineage_db.audit_user_dict;
-- TRUNCATE TABLE audit_lineage_db.audit_lineage_batch_member;
-- TRUNCATE TABLE audit_lineage_db.audit_lineage_batch_lineage;
-- TRUNCATE TABLE audit_lineage_db.audit_lineage_dead_letter;
-- TRUNCATE TABLE audit_lineage_db.audit_lineage_batch_log;
-- TRUNCATE TABLE audit_lineage_db.audit_lineage_sync_checkpoint;
