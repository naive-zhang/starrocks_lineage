-- =====================================================================================================================
-- Smoke test step 9 (run as an admin account): remove the smoke objects.
-- =====================================================================================================================
DROP DATABASE IF EXISTS lineage_smoke_db FORCE;
DROP USER IF EXISTS 'lineage_smoke_user'@'%';
DELETE FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'audit_lineage_smoke';
DELETE FROM audit_lineage_db.audit_column_lineage WHERE target_db = 'lineage_smoke_db';
-- The usage rows of lineage_smoke_db are harmless (the database no longer exists). To start production from a
-- clean slate instead, empty everything the smoke run wrote:
-- TRUNCATE TABLE audit_lineage_db.audit_column_hourly_usage;
-- TRUNCATE TABLE audit_lineage_db.audit_column_lineage;
-- TRUNCATE TABLE audit_lineage_db.audit_column_lineage_stage;
-- TRUNCATE TABLE audit_lineage_db.audit_user_dict;
-- TRUNCATE TABLE audit_lineage_db.audit_lineage_merge_log;
-- TRUNCATE TABLE audit_lineage_db.audit_lineage_sync_checkpoint;
