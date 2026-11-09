-- =====================================================================================================================
-- Global user dictionary: user name -> dense BIGINT id used in audit_column_hourly_usage.user_bitmap.
--   * Dense, collision-free ids keep Roaring bitmaps small and make UV roll-ups exact across hours/days/months.
--   * Ids are assigned by the single-writer pipeline (max(user_id) + 1, loaded in memory at start-up, written BEFORE
--     the usage rows that reference them). PRIMARY KEY on user_name makes a replayed write idempotent.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_user_dict (
    user_name        VARCHAR(256)   NOT NULL COMMENT "audit `user` value",
    user_id          BIGINT         NOT NULL COMMENT "dense id, unique, assigned by the pipeline",
    first_seen_time  DATETIME       NOT NULL COMMENT "first audit timestamp of the user"
) ENGINE = OLAP
PRIMARY KEY (user_name)
DISTRIBUTED BY HASH(user_name) BUCKETS 1
PROPERTIES (
    "replication_num" = "3"
);
