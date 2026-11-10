-- =====================================================================================================================
-- Smoke test step 2 (run as an admin account AFTER sql/ddl/00..09 have been executed)
-- Pipeline account: reads the audit table, sees metadata of every business table, writes audit_lineage_db.
-- Adjust to your security policy; the hard requirement is that information_schema.columns / DESC show every
-- table the audited users query (StarRocks only lists objects the account has a privilege on).
-- =====================================================================================================================
CREATE USER IF NOT EXISTS 'lineage_etl'@'%' IDENTIFIED BY 'CHANGE_ME_ETL';

GRANT SELECT ON TABLE starrocks_audit_db__.starrocks_audit_tbl__ TO USER 'lineage_etl'@'%';
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN DATABASE audit_lineage_db TO USER 'lineage_etl'@'%';

-- metadata visibility of default_catalog
GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'lineage_etl'@'%';
-- one line per external catalog that audited queries use, e.g.:
-- GRANT USAGE ON CATALOG hive_catalog TO USER 'lineage_etl'@'%';

SHOW GRANTS FOR 'lineage_etl'@'%';
