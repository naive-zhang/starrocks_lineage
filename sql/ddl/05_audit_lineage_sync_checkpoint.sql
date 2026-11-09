-- =====================================================================================================================
-- Incremental pull checkpoint + single-writer lease + merge log.
--   watermark_time / watermark_query_id : (timestamp, queryId) of the last fully processed audit row.
--     Next pull:  WHERE (`timestamp`, queryId) > (watermark_time, watermark_query_id)
--                  AND `timestamp` <  NOW() - INTERVAL <late_arrival_seconds> SECOND
--     expressed as  `timestamp` > t OR (`timestamp` = t AND queryId > q)  because StarRocks 3.1 has no row compare.
--   owner / lease_expire_time : only the owner with a live lease may advance the checkpoint (single writer).
--   pending_* : intent of the batch being written (id + end watermark). Saved before any sink write and cleared by
--     the commit; a failed cycle leaves it, and the next cycle replays exactly (watermark, pending_end] under the
--     same batch id, which keeps Stream Load labels and the lineage merge guard exactly-once (Stage 4 R4-02).
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_lineage_sync_checkpoint (
    pipeline_name       VARCHAR(64)    NOT NULL,
    watermark_time      DATETIME       NOT NULL,
    watermark_query_id  VARCHAR(64)    NOT NULL,
    owner               VARCHAR(128)   NOT NULL,
    lease_expire_time   DATETIME       NOT NULL,
    processed_rows      BIGINT         NOT NULL DEFAULT "0",
    pending_batch_id    VARCHAR(64)    NULL,
    pending_end_time    DATETIME       NULL,
    pending_end_query_id VARCHAR(64)   NULL,
    update_time         DATETIME       NOT NULL
) ENGINE = OLAP
PRIMARY KEY (pipeline_name)
DISTRIBUTED BY HASH(pipeline_name) BUCKETS 1
PROPERTIES (
    "replication_num" = "3"
);

CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_lineage_merge_log (
    batch_id            VARCHAR(64)    NOT NULL,
    pipeline_name       VARCHAR(64)    NOT NULL,
    lineage_rows        BIGINT         NOT NULL,
    usage_rows          BIGINT         NOT NULL,
    merged_time         DATETIME       NOT NULL
) ENGINE = OLAP
PRIMARY KEY (batch_id)
DISTRIBUTED BY HASH(batch_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "3"
);
