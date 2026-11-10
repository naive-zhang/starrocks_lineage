-- =====================================================================================================================
-- Smoke test step 7 (exactly-once drill): put the scenario batch back into the checkpoint as "declared, not
-- committed" (step INTENT, watermark at its start) - what a crash anywhere between the first write and the commit
-- leaves behind, at its worst - then run the cycle script again. Every write of the batch is already in StarRocks, so
-- the replay must find its membership, be refused by StarRocks on every label and change nothing.
-- Replace the ${...} placeholders with the SMOKE_REPORT line of the scenario batch (records=8):
--   ${BATCH_ID} / ${RANGE_START} / ${RANGE_END}
-- The owner 'replay-drill' has an expired lease, so the cycle script takes the checkpoint over at once.
-- =====================================================================================================================
INSERT INTO audit_lineage_db.audit_lineage_sync_checkpoint (pipeline_name, committed_until, coverage_start, owner,
    lease_expire_time, processed_rows, pending_batch_id, pending_kind, pending_start_time, pending_end_time,
    pending_step, pending_since, pending_attempts, last_error, last_reconcile_time, update_time, pipeline_epoch)
SELECT pipeline_name, '${RANGE_START}', coverage_start, 'replay-drill', NOW(), processed_rows, '${BATCH_ID}', 'SLICE',
       '${RANGE_START}', '${RANGE_END}', 'INTENT', NOW(), 0, NULL, last_reconcile_time, NOW(), pipeline_epoch
FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'audit_lineage_smoke';

SELECT committed_until, pending_batch_id, pending_kind, pending_start_time, pending_end_time, pending_step
FROM audit_lineage_db.audit_lineage_sync_checkpoint WHERE pipeline_name = 'audit_lineage_smoke';
