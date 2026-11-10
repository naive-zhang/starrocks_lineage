package com.starrocks.lineage.cli;

import com.starrocks.lineage.pipeline.CheckpointStore;
import com.starrocks.lineage.pipeline.JdbcCheckpointStore;
import com.starrocks.lineage.pipeline.StarRocksSink;
import com.starrocks.lineage.pipeline.StreamLoadClient;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Pre-production checks ({@code check} command): everything the pipeline needs from the cluster, each verified by
 * doing it - the real audit query shape, a real PRIMARY KEY upsert and delete, a real Stream Load through FE and BE
 * and its label state. The only rows written are marked {@code __preflight__} and deleted again.
 */
final class Preflight {

    /** Result of one check. */
    enum Level { OK, WARN, FAIL }

    /** One line of the report. */
    static final class Item {
        final String name;
        final Level level;
        final String detail;

        Item(String name, Level level, String detail) {
            this.name = name;
            this.level = level;
            this.detail = detail;
        }

        @Override
        public String toString() {
            return "CHECK " + name + " " + level + " " + detail;
        }
    }

    static final List<String> AUDIT_COLUMNS = Arrays.asList("queryId", "timestamp", "queryType", "clientIp", "user",
            "authorizedUser", "catalog", "db", "state", "errorCode", "queryTime", "scanBytes", "scanRows",
            "returnRows", "cpuCostNs", "memCostBytes", "isQuery", "stmt", "digest");

    /** Columns the pipeline writes or reads, per table of sql/ddl (a table built from older DDL fails here). */
    static final Map<String, List<String>> TARGET_COLUMNS = new LinkedHashMap<>();

    static {
        List<String> edge = split(StarRocksSink.EDGE_COLUMNS);
        List<String> lineage = new ArrayList<>(Arrays.asList("edge_id"));
        lineage.addAll(edge);
        lineage.add("update_time");
        List<String> daily = new ArrayList<>(Arrays.asList("stat_day", "edge_id"));
        daily.addAll(edge);
        List<String> usage = new ArrayList<>();
        for (String c : split(StarRocksSink.USAGE_COLUMNS)) {
            String name = c.contains("=") ? c.substring(0, c.indexOf('=')) : c;
            if (!name.equals("user_id") && !name.equals("user_name")) {
                usage.add(name);
            }
        }
        TARGET_COLUMNS.put("audit_column_lineage", lineage);
        TARGET_COLUMNS.put("audit_column_lineage_daily", daily);
        TARGET_COLUMNS.put("audit_column_hourly_usage", usage);
        TARGET_COLUMNS.put("audit_user_dict", split(StarRocksSink.DICT_COLUMNS));
        TARGET_COLUMNS.put("audit_lineage_sync_checkpoint", Arrays.asList("pipeline_name", "committed_until",
                "coverage_start", "owner", "lease_expire_time", "processed_rows", "pending_batch_id", "pending_kind",
                "pending_start_time", "pending_end_time", "pending_step", "pending_since", "pending_attempts",
                "last_error", "last_reconcile_time", "update_time", "pipeline_epoch"));
        TARGET_COLUMNS.put("audit_lineage_batch_member", split(StarRocksSink.MEMBER_COLUMNS));
        TARGET_COLUMNS.put("audit_lineage_batch_lineage", split(StarRocksSink.BATCH_LINEAGE_COLUMNS));
        TARGET_COLUMNS.put("audit_lineage_dead_letter", split(StarRocksSink.DEAD_LETTER_COLUMNS));
        TARGET_COLUMNS.put("audit_lineage_batch_log", Arrays.asList("batch_id", "pipeline_name", "kind",
                "range_start", "range_end", "records", "succeeded", "skipped", "failed", "degraded", "usage_rows",
                "lineage_rows", "dead_letters", "filtered_rows", "attempts", "started_time", "committed_time"));
    }

    private static final Set<String> SYSTEM_DATABASES = new LinkedHashSet<>(Arrays.asList("information_schema",
            "_statistics_", "sys", "starrocks_audit_db__"));

    private final CliConfig config;
    private final DataSource ds;
    private final StreamLoadClient streamLoad;
    private final Clock clock;
    private final List<Item> items = new ArrayList<>();

    Preflight(CliConfig config, DataSource ds, StreamLoadClient streamLoad, Clock clock) {
        this.config = config;
        this.ds = ds;
        this.streamLoad = streamLoad;
        this.clock = clock;
    }

    private static List<String> split(String columns) {
        List<String> out = new ArrayList<>();
        for (String c : columns.split(",")) {
            out.add(c.trim());
        }
        return out;
    }

    /** Runs every check; a check that cannot run is a FAIL with its error, the others still run. */
    List<Item> run() {
        if (!check("connection", this::connection)) {
            return items;
        }
        check("clock", this::clock);
        check("audit_table", this::auditTable);
        check("target_tables", this::targetTables);
        check("metadata_visibility", this::metadata);
        check("sql_write", this::sqlWrite);
        check("stream_load", this::streamLoad);
        check("pipeline_state", this::pipelineState);
        return items;
    }

    boolean passed() {
        return items.stream().noneMatch(i -> i.level == Level.FAIL);
    }

    private interface Check {
        Item run() throws Exception;
    }

    private boolean check(String name, Check c) {
        Item item;
        try {
            item = c.run();
        } catch (Exception e) {
            item = new Item(name, Level.FAIL, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        items.add(item);
        return item.level != Level.FAIL;
    }

    // ------------------------------------------------------------------------------------------------ checks

    private Item connection() throws SQLException {
        String version = scalar("SELECT current_version()");
        String user = scalar("SELECT current_user()");
        Level level = version.startsWith("3.1.") ? Level.OK : Level.WARN;
        return new Item("connection", level, "StarRocks " + version + " as " + user
                + (level == Level.WARN ? " (built and verified for 3.1.x)" : ""));
    }

    private Item clock() throws SQLException {
        LocalDateTime db;
        try (Connection c = ds.getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT NOW()")) {
            rs.next();
            db = rs.getTimestamp(1).toLocalDateTime();
        }
        LocalDateTime jvm = LocalDateTime.now(clock);
        long skew = Math.abs(Duration.between(db, jvm).getSeconds());
        String zone = scalar("SELECT @@time_zone");
        String detail = "starrocks=" + db.withNano(0) + " (" + zone + ") jvm=" + jvm.withNano(0) + " ("
                + clock.getZone() + ") skew=" + skew + "s";
        if (skew > 120) {
            return new Item("clock", Level.FAIL, detail + " - set TIME_ZONE=" + zone);
        }
        return new Item("clock", Level.OK, detail);
    }

    private Item auditTable() throws SQLException {
        String[] dbTable = config.auditTable().split("\\.");
        Set<String> columns = columns(dbTable[0], dbTable[1]);
        if (columns.isEmpty()) {
            return new Item("audit_table", Level.FAIL, config.auditTable() + " not found or not readable");
        }
        List<String> missing = new ArrayList<>();
        for (String c : AUDIT_COLUMNS) {
            if (!columns.contains(c.toLowerCase(Locale.ROOT))) {
                missing.add(c);
            }
        }
        if (!missing.isEmpty()) {
            return new Item("audit_table", Level.FAIL, "missing columns " + missing
                    + " (upgrade the audit loader plugin)");
        }
        long lastHour = Long.parseLong(scalar("SELECT COUNT(*) FROM " + config.auditTable()
                + " WHERE `timestamp` >= DATE_SUB(NOW(), INTERVAL 1 HOUR)"));
        return new Item("audit_table", lastHour > 0 ? Level.OK : Level.WARN, "19 columns present, " + lastHour
                + " records in the last hour" + (lastHour == 0 ? " (is the audit loader plugin writing?)" : ""));
    }

    private Item targetTables() throws SQLException {
        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, List<String>> t : TARGET_COLUMNS.entrySet()) {
            Set<String> present = columns(config.lineageDb(), t.getKey());
            if (present.isEmpty()) {
                problems.add(t.getKey() + " missing or not visible to " + config.get("SR_USER"));
                continue;
            }
            List<String> missing = new ArrayList<>();
            for (String c : t.getValue()) {
                if (!present.contains(c)) {
                    missing.add(c);
                }
            }
            if (!missing.isEmpty()) {
                problems.add(t.getKey() + " lacks " + missing);
            }
        }
        for (String old : Arrays.asList("audit_column_lineage_stage", "audit_lineage_merge_log")) {
            if (!columns(config.lineageDb(), old).isEmpty()) {
                problems.add(old + " is a table of the old (Stage 4) schema");
            }
        }
        if (!problems.isEmpty()) {
            return new Item("target_tables", Level.FAIL, problems + " - recreate " + config.lineageDb()
                    + " with sql/ddl/00..09");
        }
        return new Item("target_tables", Level.OK, TARGET_COLUMNS.size() + " tables of " + config.lineageDb()
                + " match sql/ddl");
    }

    private Item metadata() throws SQLException {
        Set<String> excluded = new LinkedHashSet<>(SYSTEM_DATABASES);
        excluded.add(config.lineageDb());
        StringBuilder in = new StringBuilder();
        for (String db : excluded) {
            in.append(in.length() == 0 ? "" : ", ").append('\'').append(db).append('\'');
        }
        long tables;
        long databases;
        try (Connection c = ds.getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*), COUNT(DISTINCT TABLE_SCHEMA) FROM "
                     + "information_schema.tables WHERE TABLE_SCHEMA NOT IN (" + in + ")")) {
            rs.next();
            tables = rs.getLong(1);
            databases = rs.getLong(2);
        }
        if (tables == 0) {
            return new Item("metadata_visibility", Level.FAIL, "no business table is visible to "
                    + config.get("SR_USER") + ": grant SELECT (see scripts/smoke/01_grants.sql), otherwise every "
                    + "statement is analysed without metadata");
        }
        return new Item("metadata_visibility", Level.OK, tables + " business tables in " + databases
                + " databases visible (compare with the cluster; invisible tables degrade the lineage - run "
                + "`analyze`)");
    }

    private Item sqlWrite() throws SQLException {
        String table = config.lineageDb() + ".audit_lineage_sync_checkpoint";
        String name = "__preflight__";
        String insert = "INSERT INTO " + table + " (pipeline_name, committed_until, coverage_start, owner, "
                + "lease_expire_time, processed_rows, update_time, pipeline_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Timestamp now = Timestamp.valueOf(LocalDateTime.now(clock).withNano(0));
        try (Connection c = ds.getConnection()) {
            for (long v = 1; v <= 2; v++) {
                try (PreparedStatement ps = c.prepareStatement(insert)) {
                    ps.setString(1, name);
                    ps.setTimestamp(2, now);
                    ps.setTimestamp(3, now);
                    ps.setString(4, config.owner());
                    ps.setTimestamp(5, now);
                    ps.setLong(6, v);
                    ps.setTimestamp(7, now);
                    ps.setString(8, "preflight");
                    ps.executeUpdate();
                }
            }
            String rows = scalar("SELECT CONCAT(COUNT(*), '/', MAX(processed_rows)) FROM " + table
                    + " WHERE pipeline_name = '" + name + "'");
            try (Statement st = c.createStatement()) {
                st.executeUpdate("DELETE FROM " + table + " WHERE pipeline_name = '" + name + "'");
            }
            if (!"1/2".equals(rows)) {
                return new Item("sql_write", Level.FAIL, "two INSERTs left " + rows + " (rows/value): "
                        + "audit_lineage_sync_checkpoint is not a PRIMARY KEY table");
            }
        }
        return new Item("sql_write", Level.OK, "INSERT upserts on the PRIMARY KEY, DELETE allowed");
    }

    private Item streamLoad() throws SQLException {
        String label = ("preflight_" + config.owner().replaceAll("[^A-Za-z0-9_]", "_") + "_"
                + System.currentTimeMillis());
        if (label.length() > 100) {
            label = label.substring(label.length() - 100);
        }
        String now = LocalDateTime.now(clock).withNano(0).toString().replace('T', ' ');
        String body = "[{\"pipeline_name\":\"__preflight__\",\"query_id\":\"" + label + "\",\"kind\":\"BATCH_SKIPPED\","
                + "\"status\":\"RESOLVED\",\"batch_id\":\"preflight\",\"audit_time\":\"" + now + "\","
                + "\"updated_time\":\"" + now + "\"}]";
        long t0 = System.currentTimeMillis();
        StreamLoadClient.Result r = streamLoad.load(config.lineageDb(), "audit_lineage_dead_letter", label,
                "pipeline_name,query_id,kind,status,batch_id,audit_time,updated_time",
                body.getBytes(StandardCharsets.UTF_8));
        long ms = System.currentTimeMillis() - t0;
        StreamLoadClient.LoadState state = streamLoad.loadState(config.lineageDb(), label);
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM " + config.lineageDb() + ".audit_lineage_dead_letter "
                    + "WHERE pipeline_name = '__preflight__'");
        }
        if (r.getLoadedRows() != 1 || state != StreamLoadClient.LoadState.COMMITTED) {
            return new Item("stream_load", Level.FAIL, "loaded " + r.getLoadedRows() + " row(s), label state "
                    + state);
        }
        return new Item("stream_load", Level.OK, "FE " + config.feHttpUrl() + " -> BE load in " + ms
                + " ms, get_load_state=" + state);
    }

    private Item pipelineState() {
        JdbcCheckpointStore store = new JdbcCheckpointStore(ds, config.lineageDb() + ".audit_lineage_sync_checkpoint",
                clock, "INSERT");
        Optional<CheckpointStore.Checkpoint> cp = store.load(config.pipeline());
        if (!cp.isPresent()) {
            return new Item("pipeline_state", Level.OK, "pipeline " + config.pipeline() + " not started yet: the "
                    + "first run starts " + config.get("INITIAL_LOOKBACK_HOURS") + " h back");
        }
        CheckpointStore.Checkpoint c = cp.get();
        String detail = "pipeline " + config.pipeline() + " committed until " + c.getCommittedUntil() + ", owner "
                + c.getOwner() + " (lease until " + c.getLeaseExpire() + ")"
                + c.getPending().map(p -> ", pending " + p).orElse("");
        boolean failing = c.getPending().map(p -> p.getAttempts() > 0).orElse(false);
        return new Item("pipeline_state", failing ? Level.WARN : Level.OK, detail);
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private Set<String> columns(String db, String table) throws SQLException {
        Set<String> out = new LinkedHashSet<>();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("SELECT COLUMN_NAME FROM "
                + "information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
            ps.setString(1, db);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1).toLowerCase(Locale.ROOT));
                }
            }
        }
        return out;
    }

    private String scalar(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return String.valueOf(rs.getString(1));
        }
    }
}
