-- =====================================================================================================================
-- User dictionary: user name -> user_id used in audit_column_hourly_usage.user_bitmap.
--   * user_id = first 63 bits of SHA-256(user_name) (UserIds). A content-derived id needs no coordination: two
--     instances racing, or a paused instance resuming after a take-over, compute the same id (a counter could hand one
--     id to two users and merge them in the exact UV - Stage 5, gap 3).
--   * The table is only a reverse lookup (bitmap_to_array -> user names) and the collision monitor:
--       SELECT user_id FROM audit_lineage_db.audit_user_dict GROUP BY user_id HAVING COUNT(DISTINCT user_name) > 1;
--     must stay empty (probability ~5e-8 for one million users).
--   * AGGREGATE (first_seen_time MIN): every write is idempotent and order-independent.
-- =====================================================================================================================
CREATE TABLE IF NOT EXISTS audit_lineage_db.audit_user_dict (
    user_name        VARCHAR(256)   NOT NULL COMMENT "audit `user` value",
    user_id          BIGINT         NOT NULL COMMENT "UserIds.idOf(user_name)",
    first_seen_time  DATETIME       MIN      NOT NULL COMMENT "first audit timestamp of the user"
) ENGINE = OLAP
AGGREGATE KEY (user_name, user_id)
DISTRIBUTED BY HASH(user_name) BUCKETS 1
PROPERTIES (
    "replication_num" = "3"
);
