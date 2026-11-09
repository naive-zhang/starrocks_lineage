-- =====================================================================================================================
-- Smoke test step 7 (exactly-once drill): rewind the checkpoint of the smoke pipeline to the state "batch declared,
-- written, but not committed" - what a crash after the lineage merge leaves behind - then run one cycle again.
-- Replace the ${...} placeholders:
--   ${W0_TIME} / ${W0_QUERY_ID}   : V1 of 05_verify.sql captured BEFORE the scenario cycle (step 6.1)
--   ${BATCH_ID} / ${END_TIME} / ${END_QUERY_ID} : the SMOKE_REPORT line of the scenario cycle
-- =====================================================================================================================
INSERT INTO audit_lineage_db.audit_lineage_sync_checkpoint (pipeline_name, watermark_time, watermark_query_id, owner,
    lease_expire_time, processed_rows, pending_batch_id, pending_end_time, pending_end_query_id, update_time)
VALUES ('audit_lineage_smoke', '${W0_TIME}', '${W0_QUERY_ID}', 'replay-drill', NOW(), 0,
        '${BATCH_ID}', '${END_TIME}', '${END_QUERY_ID}', NOW());

SELECT watermark_time, watermark_query_id, pending_batch_id, pending_end_time, pending_end_query_id
FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'audit_lineage_smoke';
