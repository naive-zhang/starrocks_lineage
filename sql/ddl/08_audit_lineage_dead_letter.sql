-- =====================================================================================================================
-- Dead letters (Stage 5, gaps 2 and 7). Nothing a batch cannot process is dropped silently:
--   PARSE_ERROR / ANALYSIS_ERROR : the full audit record (stmt, user, namespace, metrics) of a statement that failed;
--                                  repairable after an analyzer fix without the audit table
--                                  (AuditLogSyncPipeline#repairDeadLetters).
--   LOAD_FILTERED                : rows StarRocks rejected in a load; query_id = load label, detail = error-log URL.
--                                  The good rows of the same load were kept.
--   BATCH_SKIPPED                : a pending batch skipped by an operator; query_id = batch id.
-- status: OPEN -> REPAIRING (reserved by repair batch repair_batch_id) -> RESOLVED, or back to OPEN on a new failure.
-- PRIMARY KEY (pipeline_name, query_id, kind): every write is an idempotent upsert.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_lineage_dead_letter (
    pipeline_name    VARCHAR(64)      NOT NULL,
    query_id         VARCHAR(64)      NOT NULL,
    kind             VARCHAR(16)      NOT NULL,
    status           VARCHAR(16)      NOT NULL,
    batch_id         VARCHAR(64)      NOT NULL COMMENT "batch that produced the dead letter",
    repair_batch_id  VARCHAR(64)      NULL     COMMENT "repair batch that reserved / resolved it",
    audit_time       DATETIME         NOT NULL,
    error_message    VARCHAR(2048)    NULL,
    detail           VARCHAR(2048)    NULL,
    stmt             VARCHAR(1048576) NULL,
    query_type       VARCHAR(32)      NULL,
    client_ip        VARCHAR(64)      NULL,
    user_name        VARCHAR(256)     NULL,
    authorized_user  VARCHAR(256)     NULL,
    catalog_name     VARCHAR(128)     NULL,
    db_name          VARCHAR(256)     NULL,
    state            VARCHAR(16)      NULL,
    error_code       VARCHAR(128)     NULL,
    query_time       BIGINT           NULL,
    scan_bytes       BIGINT           NULL,
    scan_rows        BIGINT           NULL,
    return_rows      BIGINT           NULL,
    digest           VARCHAR(64)      NULL,
    is_query         TINYINT          NULL,
    updated_time     DATETIME         NOT NULL
) ENGINE = OLAP
PRIMARY KEY (pipeline_name, query_id, kind)
COMMENT "Records and loads a pipeline could not process"
DISTRIBUTED BY HASH(query_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "3"
);
