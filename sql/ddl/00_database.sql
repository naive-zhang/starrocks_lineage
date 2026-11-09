-- =====================================================================================================================
-- StarRocks 3.1.x : database hosting the audit lineage / usage results.
-- Kept separate from starrocks_audit_db__ so that the audit loader plugin's database is never written by this engine.
-- =====================================================================================================================
CREATE DATABASE IF NOT EXISTS audit_lineage_db;
