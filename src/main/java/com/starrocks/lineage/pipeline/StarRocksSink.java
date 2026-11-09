package com.starrocks.lineage.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link AuditLineageSink} for StarRocks 3.1.
 *
 * <ul>
 *   <li>{@code audit_user_dict}, {@code audit_column_hourly_usage} and {@code audit_column_lineage_stage} are written
 *       with Stream Load; labels are {@code <batchId>_<kind>_<chunk>} so a retried batch is de-duplicated by
 *       StarRocks itself ("Label Already Exists").</li>
 *   <li>Usage rows map {@code user_bitmap = to_bitmap(user_id)} and {@code user_hll = hll_hash(user_name)} in the
 *       {@code columns} header: one row per (hour, column, user), the aggregate table unions them.</li>
 *   <li>The stage is merged into the PRIMARY KEY lineage table by a single atomic {@code INSERT ... SELECT}
 *       guarded by {@code last_batch_id <> batchId}: replaying a merged batch writes nothing, so a crash between the
 *       merge and the merge-log write cannot double {@code exec_count} (review R4-01).</li>
 * </ul>
 */
public final class StarRocksSink implements AuditLineageSink {

    static final String USAGE_COLUMNS = "stat_hour,catalog_name,db_name,table_name,column_name,access_query_cnt,"
            + "select_clause_cnt,where_clause_cnt,join_clause_cnt,group_by_cnt,order_by_cnt,total_scan_bytes,"
            + "total_query_time_ms,max_query_time_ms,last_access_time,user_id,user_name,"
            + "user_bitmap=to_bitmap(user_id),user_hll=hll_hash(user_name)";
    static final String STAGE_COLUMNS = "batch_day,batch_id,edge_id,target_catalog,target_db,target_table,"
            + "target_column,source_catalog,source_db,source_table,source_column,lineage_type,transform_type,"
            + "transform_expr,stmt_type,sql_digest,last_query_id,last_exec_user,first_seen_time,last_seen_time,"
            + "exec_count";
    static final String DICT_COLUMNS = "user_name,user_id,first_seen_time";

    private static final String LINEAGE_COLUMNS = "edge_id, target_catalog, target_db, target_table, target_column, "
            + "source_catalog, source_db, source_table, source_column, lineage_type, transform_type, transform_expr, "
            + "stmt_type, sql_digest, last_query_id, last_exec_user, first_seen_time, last_seen_time, exec_count, "
            + "last_batch_id, update_time";
    private static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final DataSource dataSource;
    private final StreamLoadClient streamLoad;
    private final String database;
    private final int chunkRows;
    private final String upsertVerb;
    private final Clock clock;
    private final ObjectMapper json = new ObjectMapper();

    public StarRocksSink(DataSource dataSource, StreamLoadClient streamLoad, String database) {
        this(dataSource, streamLoad, database, 100_000, "INSERT", Clock.systemDefaultZone());
    }

    /**
     * @param chunkRows  maximum rows per stream load request
     * @param upsertVerb {@code INSERT} for StarRocks PRIMARY KEY tables (insert = upsert); another engine's upsert
     *                   verb (e.g. H2 {@code MERGE}) for tests
     */
    public StarRocksSink(DataSource dataSource, StreamLoadClient streamLoad, String database, int chunkRows,
                         String upsertVerb, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.streamLoad = streamLoad;
        if (!database.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("invalid database: " + database);
        }
        if (chunkRows < 1) {
            throw new IllegalArgumentException("chunkRows must be >= 1");
        }
        if (!upsertVerb.matches("[A-Z]+")) {
            throw new IllegalArgumentException("invalid upsert verb: " + upsertVerb);
        }
        this.database = database;
        this.chunkRows = chunkRows;
        this.upsertVerb = upsertVerb;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Map<String, Long> loadUserDictionary() {
        Map<String, Long> dict = new HashMap<>();
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT user_name, user_id FROM " + database + ".audit_user_dict")) {
            while (rs.next()) {
                dict.put(rs.getString(1), rs.getLong(2));
            }
            return dict;
        } catch (SQLException e) {
            throw new PipelineException("loading the user dictionary failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void writeUserDictionary(String batchId, List<UserDictionary.Entry> entries, LocalDateTime seenAt) {
        List<Map<String, Object>> rows = new ArrayList<>(entries.size());
        for (UserDictionary.Entry e : entries) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("user_name", e.getUserName());
            row.put("user_id", e.getUserId());
            row.put("first_seen_time", DATETIME.format(seenAt));
            rows.add(row);
        }
        loadChunks("audit_user_dict", batchId + "_dict", DICT_COLUMNS, rows);
    }

    @Override
    public void writeUsage(String batchId, List<HourlyUsageRow> usage, UserDictionary dictionary) {
        loadChunks("audit_column_hourly_usage", batchId + "_usage", USAGE_COLUMNS, usageRows(usage, dictionary));
    }

    static List<Map<String, Object>> usageRows(List<HourlyUsageRow> usage, UserDictionary dictionary) {
        List<Map<String, Object>> rows = new ArrayList<>(usage.size());
        for (HourlyUsageRow u : usage) {
            ColumnIdentifier col = u.getColumn();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("stat_hour", DATETIME.format(u.getStatHour()));
            row.put("catalog_name", col.getCatalog());
            row.put("db_name", col.getDatabase());
            row.put("table_name", col.getTableName());
            row.put("column_name", col.getColumn());
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
            row.put("user_id", dictionary.idOf(u.getUserName()));
            row.put("user_name", u.getUserName());
            rows.add(row);
        }
        return rows;
    }

    @Override
    public void writeLineage(String batchId, List<LineageRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        loadChunks("audit_column_lineage_stage", batchId + "_stage", STAGE_COLUMNS,
                stageRows(batchId, rows, LocalDate.now(clock)));
        int merged = mergeLineage(batchId);
        logMerge(batchId, merged);
    }

    static List<Map<String, Object>> stageRows(String batchId, List<LineageRow> rows, LocalDate batchDay) {
        List<Map<String, Object>> out = new ArrayList<>(rows.size());
        for (LineageRow r : rows) {
            ColumnLineageEdge e = r.getEdge();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("batch_day", batchDay.toString());
            row.put("batch_id", batchId);
            row.put("edge_id", e.edgeId());
            row.put("target_catalog", e.getTarget().getCatalog());
            row.put("target_db", e.getTarget().getDatabase());
            row.put("target_table", e.getTarget().getTableName());
            row.put("target_column", e.getTarget().getColumn());
            row.put("source_catalog", e.getSource().getCatalog());
            row.put("source_db", e.getSource().getDatabase());
            row.put("source_table", e.getSource().getTableName());
            row.put("source_column", e.getSource().getColumn());
            row.put("lineage_type", e.getLineageType().name());
            row.put("transform_type", e.getTransformType().name());
            row.put("transform_expr", e.getTransformExpr());
            row.put("stmt_type", r.getStmtType().name());
            row.put("sql_digest", r.getSqlDigest());
            row.put("last_query_id", r.getLastQueryId());
            row.put("last_exec_user", r.getLastExecUser());
            row.put("first_seen_time", DATETIME.format(r.getFirstSeenTime()));
            row.put("last_seen_time", DATETIME.format(r.getLastSeenTime()));
            row.put("exec_count", r.getExecCount());
            out.add(row);
        }
        return out;
    }

    /**
     * Merges one staged batch. Returns the number of rows written (0 when the batch was already merged).
     */
    int mergeLineage(String batchId) {
        String newest = "o.edge_id IS NULL OR s.last_seen_time >= o.last_seen_time";
        String sql = upsertVerb + " INTO " + database + ".audit_column_lineage (" + LINEAGE_COLUMNS + ") "
                + "SELECT s.edge_id, s.target_catalog, s.target_db, s.target_table, s.target_column, "
                + "s.source_catalog, s.source_db, s.source_table, s.source_column, s.lineage_type, "
                + "CASE WHEN " + newest + " THEN s.transform_type ELSE o.transform_type END, "
                + "CASE WHEN " + newest + " THEN s.transform_expr ELSE o.transform_expr END, "
                + "CASE WHEN " + newest + " THEN s.stmt_type ELSE o.stmt_type END, "
                + "CASE WHEN " + newest + " THEN s.sql_digest ELSE o.sql_digest END, "
                + "CASE WHEN " + newest + " THEN s.last_query_id ELSE o.last_query_id END, "
                + "CASE WHEN " + newest + " THEN s.last_exec_user ELSE o.last_exec_user END, "
                + "CASE WHEN o.edge_id IS NULL OR s.first_seen_time < o.first_seen_time THEN s.first_seen_time "
                + "ELSE o.first_seen_time END, "
                + "CASE WHEN " + newest + " THEN s.last_seen_time ELSE o.last_seen_time END, "
                + "COALESCE(o.exec_count, 0) + s.exec_count, s.batch_id, ? "
                + "FROM " + database + ".audit_column_lineage_stage s "
                + "LEFT JOIN " + database + ".audit_column_lineage o ON o.edge_id = s.edge_id "
                + "WHERE s.batch_id = ? AND (o.edge_id IS NULL OR o.last_batch_id IS NULL OR o.last_batch_id <> ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now(clock)));
            ps.setString(2, batchId);
            ps.setString(3, batchId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new PipelineException("lineage merge of batch " + batchId + " failed: " + e.getMessage(), e);
        }
    }

    private void logMerge(String batchId, int lineageRows) {
        String sql = upsertVerb + " INTO " + database + ".audit_lineage_merge_log (batch_id, pipeline_name, "
                + "lineage_rows, usage_rows, merged_time) VALUES (?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, batchId);
            ps.setString(2, "lineage");
            ps.setLong(3, lineageRows);
            ps.setLong(4, 0L);
            ps.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now(clock)));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PipelineException("merge log of batch " + batchId + " failed: " + e.getMessage(), e);
        }
    }

    private void loadChunks(String table, String labelPrefix, String columns, List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return;
        }
        if (streamLoad == null) {
            throw new PipelineException("no stream load client configured for " + table);
        }
        for (int from = 0, chunk = 0; from < rows.size(); from += chunkRows, chunk++) {
            List<Map<String, Object>> part = rows.subList(from, Math.min(rows.size(), from + chunkRows));
            byte[] body;
            try {
                body = json.writeValueAsBytes(part);
            } catch (JsonProcessingException e) {
                throw new PipelineException("serialising " + table + " rows failed", e);
            }
            streamLoad.load(database, table, labelPrefix + "_" + chunk, columns, body);
        }
    }
}
