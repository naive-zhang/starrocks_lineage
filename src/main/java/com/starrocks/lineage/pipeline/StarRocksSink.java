package com.starrocks.lineage.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link AuditLineageSink} for StarRocks 3.1 ({@code sql/ddl}).
 *
 * <ul>
 *   <li>Stream Load, one request per step (no chunking: a step must be atomic for exactly-once).</li>
 *   <li>Every string is truncated to its column width in UTF-8 bytes before loading, so the usual causes of rejected
 *       rows cannot happen; anything StarRocks still rejects is isolated as a LOAD_FILTERED dead letter.</li>
 *   <li>The lineage table is never incremented: {@link #refreshLineage} recomputes the touched edges from the daily
 *       facts, so it can run any number of times.</li>
 * </ul>
 */
public final class StarRocksSink implements AuditLineageSink {

    public static final String USAGE_COLUMNS = "stat_hour,catalog_name,db_name,table_name,column_name,access_query_cnt,"
            + "select_clause_cnt,where_clause_cnt,join_clause_cnt,group_by_cnt,order_by_cnt,total_scan_bytes,"
            + "total_query_time_ms,max_query_time_ms,last_access_time,user_id,user_name,"
            + "user_bitmap=to_bitmap(user_id),user_hll=hll_hash(user_name)";
    public static final String DICT_COLUMNS = "user_name,user_id,first_seen_time";
    public static final String MEMBER_COLUMNS = "pipeline_name,query_id,audit_time,batch_id";
    public static final String EDGE_COLUMNS = "target_catalog, target_db, target_table, target_column, source_catalog, "
            + "source_db, source_table, source_column, lineage_type, transform_type, transform_expr, stmt_type, "
            + "sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count";
    public static final String BATCH_LINEAGE_COLUMNS = "pipeline_name,batch_id,edge_id,stat_day,refresh_only,"
            + "batch_time,"
            + EDGE_COLUMNS.replace(" ", "");
    public static final String DEAD_LETTER_COLUMNS = "pipeline_name,query_id,kind,status,batch_id,repair_batch_id,"
            + "audit_time,"
            + "error_message,detail,stmt,query_type,client_ip,user_name,authorized_user,catalog_name,db_name,state,"
            + "error_code,query_time,scan_bytes,scan_rows,return_rows,digest,is_query,updated_time";

    private static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final DataSource dataSource;
    private final StreamLoadClient streamLoad;
    private final String database;
    private final String pipeline;
    private final Clock clock;
    private final ObjectMapper json = new ObjectMapper();

    public StarRocksSink(DataSource dataSource, StreamLoadClient streamLoad, String database, String pipeline) {
        this(dataSource, streamLoad, database, pipeline, Clock.systemDefaultZone());
    }

    public StarRocksSink(DataSource dataSource, StreamLoadClient streamLoad, String database, String pipeline,
                         Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.streamLoad = Objects.requireNonNull(streamLoad, "streamLoad");
        if (!database.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("invalid database: " + database);
        }
        if (pipeline == null || pipeline.isEmpty() || pipeline.length() > 64) {
            throw new IllegalArgumentException("pipeline name must be 1..64 characters");
        }
        this.database = database;
        this.pipeline = pipeline;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public String label(String batchId, String step) {
        return batchId + "_" + step;
    }

    // ------------------------------------------------------------------------------------------------ loads

    @Override
    public StreamLoadClient.Result writeMembers(String batchId, LocalDateTime batchTime, List<AuditLogRecord> records) {
        List<Map<String, Object>> rows = new ArrayList<>(records.size());
        for (AuditLogRecord r : records) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("pipeline_name", pipeline);
            row.put("query_id", Text.truncate(r.getQueryId(), 64));
            row.put("audit_time", DATETIME.format(r.getTimestamp()));
            row.put("batch_id", batchId);
            rows.add(row);
        }
        return load("audit_lineage_batch_member", batchId, label(batchId, "members"), MEMBER_COLUMNS, rows, false);
    }

    @Override
    public StreamLoadClient.Result writeDeadLetters(String batchId, String tag, List<DeadLetter> letters) {
        List<Map<String, Object>> rows = new ArrayList<>(letters.size());
        String now = DATETIME.format(LocalDateTime.now(clock));
        for (DeadLetter d : letters) {
            AuditLogRecord r = d.getRecord();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("pipeline_name", pipeline);
            row.put("query_id", Text.truncate(d.getQueryId(), 64));
            row.put("kind", d.getKind().name());
            row.put("status", d.getStatus().name());
            row.put("batch_id", d.getBatchId());
            row.put("repair_batch_id", d.getRepairBatchId());
            row.put("audit_time", DATETIME.format(d.getAuditTime()));
            row.put("error_message", Text.truncate(d.getErrorMessage(), 2048));
            row.put("detail", Text.truncate(d.getDetail(), 2048));
            row.put("stmt", r == null ? null : Text.truncate(r.getStmt(), 1_048_576));
            row.put("query_type", r == null ? null : Text.truncate(r.getQueryType(), 32));
            row.put("client_ip", r == null ? null : Text.truncate(r.getClientIp(), 64));
            row.put("user_name", r == null ? null : Text.truncate(r.getUser(), 256));
            row.put("authorized_user", r == null ? null : Text.truncate(r.getAuthorizedUser(), 256));
            row.put("catalog_name", r == null ? null : Text.truncate(r.getCatalog(), 128));
            row.put("db_name", r == null ? null : Text.truncate(r.getDb(), 256));
            row.put("state", r == null ? null : Text.truncate(r.getState(), 16));
            row.put("error_code", r == null ? null : Text.truncate(r.getErrorCode(), 128));
            row.put("query_time", r == null ? null : r.getQueryTime());
            row.put("scan_bytes", r == null ? null : r.getScanBytes());
            row.put("scan_rows", r == null ? null : r.getScanRows());
            row.put("return_rows", r == null ? null : r.getReturnRows());
            row.put("digest", r == null ? null : Text.truncate(r.getDigest(), 64));
            row.put("is_query", r == null ? null : (r.isQuery() ? 1 : 0));
            row.put("updated_time", now);
            rows.add(row);
        }
        return load("audit_lineage_dead_letter", batchId, label(batchId, "dlq_" + tag), DEAD_LETTER_COLUMNS, rows,
                false);
    }

    @Override
    public StreamLoadClient.Result writeUserDictionary(String batchId, List<HourlyUsageRow> usage) {
        Map<String, LocalDateTime> firstSeen = new LinkedHashMap<>();
        for (HourlyUsageRow u : usage) {
            firstSeen.merge(u.getUserName(), u.getLastAccessTime(), (a, b) -> a.isBefore(b) ? a : b);
        }
        List<Map<String, Object>> rows = new ArrayList<>(firstSeen.size());
        for (Map.Entry<String, LocalDateTime> e : firstSeen.entrySet()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("user_name", Text.truncate(e.getKey(), 256));
            row.put("user_id", UserIds.idOf(e.getKey()));
            row.put("first_seen_time", DATETIME.format(e.getValue()));
            rows.add(row);
        }
        return load("audit_user_dict", batchId, label(batchId, "dict"), DICT_COLUMNS, rows, false);
    }

    @Override
    public StreamLoadClient.Result writeUsage(String batchId, List<HourlyUsageRow> usage) {
        return load("audit_column_hourly_usage", batchId, label(batchId, "usage"), USAGE_COLUMNS, usageRows(usage),
                true);
    }

    static List<Map<String, Object>> usageRows(List<HourlyUsageRow> usage) {
        List<Map<String, Object>> rows = new ArrayList<>(usage.size());
        for (HourlyUsageRow u : usage) {
            ColumnIdentifier col = u.getColumn();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("stat_hour", DATETIME.format(u.getStatHour()));
            row.put("catalog_name", Text.truncate(col.getCatalog(), 128));
            row.put("db_name", Text.truncate(col.getDatabase(), 256));
            row.put("table_name", Text.truncate(col.getTableName(), 256));
            row.put("column_name", Text.truncate(col.getColumn(), 256));
            row.put("access_query_cnt", u.getAccessQueryCnt());
            row.put("select_clause_cnt", u.getSelectClauseCnt());
            row.put("where_clause_cnt", u.getWhereClauseCnt());
            row.put("join_clause_cnt", u.getJoinClauseCnt());
            row.put("group_by_cnt", u.getGroupByCnt());
            row.put("order_by_cnt", u.getOrderByCnt());
            row.put("total_scan_bytes", u.getTotalScanBytes());
            row.put("total_query_time_ms", u.getTotalQueryTimeMs());
            row.put("max_query_time_ms", u.getMaxQueryTimeMs());
            row.put("last_access_time", DATETIME.format(u.getLastAccessTime()));
            row.put("user_id", UserIds.idOf(u.getUserName()));
            row.put("user_name", Text.truncate(u.getUserName(), 256));
            rows.add(row);
        }
        return rows;
    }

    @Override
    public StreamLoadClient.Result writeBatchLineage(String batchId, LocalDateTime batchTime, List<LineageRow> rows) {
        List<Map<String, Object>> out = new ArrayList<>(rows.size());
        for (LineageRow r : rows) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("pipeline_name", pipeline);
            row.put("batch_id", batchId);
            row.put("edge_id", r.getEdgeId());
            row.put("stat_day", DATETIME.format(r.getStatDay()));
            row.put("refresh_only", false);
            row.put("batch_time", DATETIME.format(batchTime));
            putEdge(row, r);
            out.add(row);
        }
        return load("audit_lineage_batch_lineage", batchId, label(batchId, "lineage"), BATCH_LINEAGE_COLUMNS, out,
                true);
    }

    static void putEdge(Map<String, Object> row, LineageRow r) {
        ColumnLineageEdge e = r.getEdge();
        row.put("target_catalog", Text.truncate(e.getTarget().getCatalog(), 128));
        row.put("target_db", Text.truncate(e.getTarget().getDatabase(), 256));
        row.put("target_table", Text.truncate(e.getTarget().getTableName(), 256));
        row.put("target_column", Text.truncate(e.getTarget().getColumn(), 256));
        row.put("source_catalog", Text.truncate(e.getSource().getCatalog(), 128));
        row.put("source_db", Text.truncate(e.getSource().getDatabase(), 256));
        row.put("source_table", Text.truncate(e.getSource().getTableName(), 256));
        row.put("source_column", Text.truncate(e.getSource().getColumn(), 256));
        row.put("lineage_type", e.getLineageType().name());
        row.put("transform_type", e.getTransformType().name());
        row.put("transform_expr", Text.truncate(e.getTransformExpr(), 2048));
        row.put("stmt_type", r.getStmtType().name());
        row.put("sql_digest", Text.truncate(r.getSqlDigest(), 64));
        row.put("last_query_id", Text.truncate(r.getLastQueryId(), 64));
        row.put("last_exec_user", Text.truncate(r.getLastExecUser(), 128));
        row.put("first_seen_time", DATETIME.format(r.getFirstSeenTime()));
        row.put("last_seen_time", DATETIME.format(r.getLastSeenTime()));
        row.put("exec_count", r.getExecCount());
    }

    /**
     * Strict load first; on rejected rows a LOAD_FILTERED dead letter is written (before anything is loaded, so the
     * evidence survives a crash), then the same label is re-sent tolerating the rejected rows.
     */
    private StreamLoadClient.Result load(String table, String batchId, String label, String columns,
                                         List<Map<String, Object>> rows, boolean isolateRejectedRows) {
        if (rows.isEmpty()) {
            return null;
        }
        byte[] body;
        try {
            body = json.writeValueAsBytes(rows);
        } catch (JsonProcessingException e) {
            throw PipelineException.nonRetryable("serialising " + table + " rows failed", e);
        }
        try {
            return streamLoad.load(database, table, label, columns, body, false);
        } catch (StreamLoadClient.DataQualityException rejected) {
            if (!isolateRejectedRows) {
                throw rejected;
            }
            LocalDateTime now = LocalDateTime.now(clock);
            DeadLetter evidence = new DeadLetter(label, DeadLetter.Kind.LOAD_FILTERED, DeadLetter.Status.OPEN,
                    batchId, null, now, rejected.getMessage(), rejected.getErrorUrl(), null);
            writeDeadLetters(batchId, "filtered_" + table, Collections.singletonList(evidence));
            return streamLoad.load(database, table, label, columns, body, true);
        }
    }

    // ------------------------------------------------------------------------------------------------ SQL steps

    @Override
    public void recordEdgesForRefresh(String batchId, LocalDateTime batchTime, LocalDateTime fromDay,
                                      LocalDateTime toDay) {
        String sql = "INSERT INTO " + database + ".audit_lineage_batch_lineage WITH LABEL "
                + label(batchId, "refresh_edges") + " (" + BATCH_LINEAGE_COLUMNS + ") "
                + "SELECT ?, ?, edge_id, stat_day, true, ?, " + EDGE_COLUMNS.replace("exec_count", "0") + " "
                + "FROM " + database + ".audit_column_lineage_daily WHERE stat_day >= ? AND stat_day < ?";
        labelledInsert(sql, pipeline, batchId, batchTime, fromDay, toDay);
    }

    @Override
    public void insertDaily(String batchId) {
        String sql = "INSERT INTO " + database + ".audit_column_lineage_daily WITH LABEL " + label(batchId, "daily")
                + " (stat_day, edge_id, " + EDGE_COLUMNS + ") SELECT stat_day, edge_id, " + EDGE_COLUMNS
                + " FROM " + database + ".audit_lineage_batch_lineage "
                + "WHERE pipeline_name = ? AND batch_id = ? AND refresh_only = false";
        labelledInsert(sql, pipeline, batchId);
    }

    /** Runs a labelled INSERT; "label already used" means a previous attempt committed it. */
    private void labelledInsert(String sql, Object... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            ps.executeUpdate();
        } catch (SQLException e) {
            String m = e.getMessage() == null ? "" : e.getMessage();
            if (m.contains("has already been used")) {
                return;
            }
            throw new PipelineException("labelled insert failed: " + m, e);
        }
    }

    @Override
    public void refreshLineage(String batchId) {
        String edges = "SELECT edge_id FROM " + database + ".audit_lineage_batch_lineage "
                + "WHERE pipeline_name = ? AND batch_id = ?";
        String daily = database + ".audit_column_lineage_daily";
        String upsert = "INSERT INTO " + database + ".audit_column_lineage (edge_id, " + EDGE_COLUMNS
                + ", update_time) SELECT a.edge_id, "
                + EDGE_COLUMNS.replace("first_seen_time", "a.first_seen").replace("last_seen_time", "a.last_seen")
                .replace("exec_count", "a.cnt").replaceAll("(^|, )(?!a\\.)", "$1l.")
                + ", ? FROM (SELECT edge_id, MIN(first_seen_time) AS first_seen, MAX(last_seen_time) AS last_seen, "
                + "SUM(exec_count) AS cnt, MAX(stat_day) AS last_day FROM " + daily
                + " WHERE edge_id IN (" + edges + ") GROUP BY edge_id) a JOIN " + daily
                + " l ON l.edge_id = a.edge_id AND l.stat_day = a.last_day";
        String delete = "DELETE FROM " + database + ".audit_column_lineage WHERE edge_id IN (" + edges + ") "
                + "AND edge_id NOT IN (SELECT edge_id FROM " + daily + " WHERE edge_id IN (" + edges + "))";
        execute(upsert, Timestamp.valueOf(LocalDateTime.now(clock)), pipeline, batchId);
        execute(delete, pipeline, batchId, pipeline, batchId);
    }

    @Override
    public void purgeUsage(LocalDateTime from, LocalDateTime to) {
        execute("DELETE FROM " + database + ".audit_column_hourly_usage WHERE stat_hour >= ? AND stat_hour < ?",
                from, to);
    }

    @Override
    public void purgeLineageDaily(LocalDateTime fromDay, LocalDateTime toDay) {
        execute("DELETE FROM " + database + ".audit_column_lineage_daily WHERE stat_day >= ? AND stat_day < ?",
                fromDay, toDay);
    }

    private static final String DEAD_LETTER_SELECT = "SELECT query_id, kind, status, batch_id, repair_batch_id, "
            + "audit_time, error_message, detail, stmt, query_type, client_ip, user_name, authorized_user, "
            + "catalog_name, db_name, state, error_code, query_time, scan_bytes, scan_rows, return_rows, digest, "
            + "is_query FROM ";

    @Override
    public List<DeadLetter> deadLetters(Set<DeadLetter.Status> statuses, Set<DeadLetter.Kind> kinds,
                                        LocalDateTime from, LocalDateTime to, String repairBatchId, int limit) {
        if (statuses.isEmpty() || kinds.isEmpty()) {
            return Collections.emptyList();
        }
        String sql = DEAD_LETTER_SELECT + database + ".audit_lineage_dead_letter WHERE pipeline_name = ? "
                + "AND status IN (" + in(statuses.size()) + ") AND kind IN (" + in(kinds.size()) + ") "
                + "AND audit_time >= ? AND audit_time < ? "
                + (repairBatchId == null ? "" : "AND repair_batch_id = ? ") + "ORDER BY audit_time, query_id LIMIT ?";
        List<Object> params = new ArrayList<>();
        params.add(pipeline);
        statuses.forEach(s -> params.add(s.name()));
        kinds.forEach(k -> params.add(k.name()));
        params.add(from);
        params.add(to);
        if (repairBatchId != null) {
            params.add(repairBatchId);
        }
        params.add(limit);
        return readDeadLetters(sql, params.toArray());
    }

    @Override
    public List<DeadLetter> deadLettersOfBatch(String batchId, LocalDateTime from, LocalDateTime to) {
        String sql = DEAD_LETTER_SELECT + database + ".audit_lineage_dead_letter WHERE pipeline_name = ? "
                + "AND batch_id = ? AND audit_time >= ? AND audit_time < ? ORDER BY audit_time, query_id";
        return readDeadLetters(sql, pipeline, batchId, from, to);
    }

    private List<DeadLetter> readDeadLetters(String sql, Object... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            List<DeadLetter> result = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    DeadLetter.Kind kind = DeadLetter.Kind.valueOf(rs.getString(2));
                    LocalDateTime auditTime = rs.getTimestamp(6).toLocalDateTime();
                    AuditLogRecord record = null;
                    if (kind.isRepairable()) {
                        record = AuditLogRecord.builder().queryId(rs.getString(1)).timestamp(auditTime)
                                .stmt(rs.getString(9)).queryType(rs.getString(10)).clientIp(rs.getString(11))
                                .user(rs.getString(12)).authorizedUser(rs.getString(13)).catalog(rs.getString(14))
                                .db(rs.getString(15)).state(rs.getString(16)).errorCode(rs.getString(17))
                                .queryTime(rs.getLong(18)).scanBytes(rs.getLong(19)).scanRows(rs.getLong(20))
                                .returnRows(rs.getLong(21)).digest(rs.getString(22)).isQuery(rs.getInt(23) != 0)
                                .build();
                    }
                    result.add(new DeadLetter(rs.getString(1), kind, DeadLetter.Status.valueOf(rs.getString(3)),
                            rs.getString(4), rs.getString(5), auditTime, rs.getString(7), rs.getString(8), record));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new PipelineException("dead letter read failed: " + e.getMessage(), e);
        }
    }

    @Override
    public StreamLoadClient.LoadState loadState(String label) {
        return streamLoad.loadState(database, label);
    }

    @Override
    public void writeBatchLog(BatchLogEntry e) {
        execute("INSERT INTO " + database + ".audit_lineage_batch_log (batch_id, pipeline_name, kind, range_start, "
                        + "range_end, records, succeeded, skipped, failed, degraded, usage_rows, lineage_rows, "
                        + "dead_letters, filtered_rows, attempts, started_time, committed_time) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                e.getBatchId(), pipeline, e.getKind().name(), e.getRangeStart(), e.getRangeEnd(), e.getRecords(),
                e.getSucceeded(), e.getSkipped(), e.getFailed(), e.getDegraded(), e.getUsageRows(),
                e.getLineageRows(), e.getDeadLetters(), e.getFilteredRows(), e.getAttempts(), e.getStartedTime(),
                e.getCommittedTime());
    }

    @Override
    public List<Long> userIdCollisions() {
        List<Long> ids = new ArrayList<>();
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT user_id FROM " + database + ".audit_user_dict GROUP BY user_id "
                     + "HAVING COUNT(DISTINCT user_name) > 1")) {
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
        } catch (SQLException e) {
            throw new PipelineException("user id collision check failed: " + e.getMessage(), e);
        }
        return ids;
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private void execute(String sql, Object... params) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PipelineException("statement failed: " + e.getMessage() + "\n" + sql, e);
        }
    }

    private static void bind(PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object p = params[i];
            if (p instanceof LocalDateTime) {
                ps.setTimestamp(i + 1, Timestamp.valueOf((LocalDateTime) p));
            } else {
                ps.setObject(i + 1, p);
            }
        }
    }

    private static String in(int n) {
        return Collections.nCopies(Math.max(1, n), "?").stream().collect(Collectors.joining(", "));
    }

    /**
     * UTF-8 aware truncation (StarRocks VARCHAR lengths are bytes).
     */
    static final class Text {
        private Text() {
        }

        static String truncate(String s, int maxBytes) {
            if (s == null) {
                return null;
            }
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            if (b.length <= maxBytes) {
                return s;
            }
            int end = maxBytes;
            while (end > 0 && (b[end] & 0xC0) == 0x80) {
                end--;
            }
            return new String(b, 0, end, StandardCharsets.UTF_8);
        }
    }
}
