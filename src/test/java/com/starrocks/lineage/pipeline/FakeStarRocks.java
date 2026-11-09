package com.starrocks.lineage.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double of a StarRocks cluster: an FE that answers stream loads with {@code 307} to a BE endpoint on the same
 * server, a BE that enforces Basic auth, {@code Expect: 100-continue}, label uniqueness, and an H2 database (MySQL
 * mode) holding the {@code audit_lineage_db} tables. Stream-loaded rows of the lineage stage and the user dictionary
 * are inserted into H2 so the JDBC merge can run on them; every load is also recorded.
 */
final class FakeStarRocks implements AutoCloseable {

    static final String USER = "loader";
    static final String PASSWORD = "secret";

    final HttpServer server;
    final DataSource dataSource;
    private final Connection keepAlive;
    private final ObjectMapper json = new ObjectMapper();
    private final Set<String> labels = Collections.synchronizedSet(new HashSet<>());
    final List<Load> loads = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger failNextLoads = new AtomicInteger();

    FakeStarRocks() throws Exception {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:sr_" + System.nanoTime() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        dataSource = h2;
        keepAlive = h2.getConnection();
        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE SCHEMA audit_lineage_db");
            st.execute("CREATE TABLE audit_lineage_db.audit_user_dict (user_name VARCHAR(256) PRIMARY KEY, "
                    + "user_id BIGINT NOT NULL, first_seen_time TIMESTAMP NOT NULL)");
            String lineageCols = "target_catalog VARCHAR(128), target_db VARCHAR(256), target_table VARCHAR(256), "
                    + "target_column VARCHAR(256), source_catalog VARCHAR(128), source_db VARCHAR(256), "
                    + "source_table VARCHAR(256), source_column VARCHAR(256), lineage_type VARCHAR(16), "
                    + "transform_type VARCHAR(16), transform_expr VARCHAR(2048), stmt_type VARCHAR(32), "
                    + "sql_digest VARCHAR(64), last_query_id VARCHAR(64), last_exec_user VARCHAR(128), "
                    + "first_seen_time TIMESTAMP, last_seen_time TIMESTAMP, exec_count BIGINT";
            st.execute("CREATE TABLE audit_lineage_db.audit_column_lineage (edge_id VARCHAR(64) PRIMARY KEY, "
                    + lineageCols + ", last_batch_id VARCHAR(64), update_time TIMESTAMP)");
            st.execute("CREATE TABLE audit_lineage_db.audit_column_lineage_stage (batch_day DATE, "
                    + "batch_id VARCHAR(64), edge_id VARCHAR(64), " + lineageCols + ")");
            st.execute("CREATE TABLE audit_lineage_db.audit_lineage_merge_log (batch_id VARCHAR(64) PRIMARY KEY, "
                    + "pipeline_name VARCHAR(64), lineage_rows BIGINT, usage_rows BIGINT, merged_time TIMESTAMP)");
            st.execute("CREATE TABLE audit_lineage_db.audit_lineage_sync_checkpoint (pipeline_name VARCHAR(64) "
                    + "PRIMARY KEY, "
                    + "watermark_time TIMESTAMP, watermark_query_id VARCHAR(64), owner VARCHAR(128), "
                    + "lease_expire_time TIMESTAMP, processed_rows BIGINT, pending_batch_id VARCHAR(64), "
                    + "pending_end_time TIMESTAMP, pending_end_query_id VARCHAR(64), update_time TIMESTAMP(6))");
        }
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/", this::fe);
        server.createContext("/be/", this::be);
        server.start();
    }

    URI feUrl() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    StreamLoadClient client() {
        return new StreamLoadClient(feUrl(), USER, PASSWORD, java.time.Duration.ofSeconds(30));
    }

    List<Load> loadsInto(String table) {
        List<Load> result = new ArrayList<>();
        synchronized (loads) {
            for (Load l : loads) {
                if (l.table.equals(table)) {
                    result.add(l);
                }
            }
        }
        return result;
    }

    private void fe(HttpExchange ex) throws IOException {
        drain(ex.getRequestBody());
        ex.getResponseHeaders().add("Location", "/be" + ex.getRequestURI().getPath());
        ex.sendResponseHeaders(307, -1);
        ex.close();
    }

    private void be(HttpExchange ex) throws IOException {
        byte[] body = drain(ex.getRequestBody());
        String auth = ex.getRequestHeaders().getFirst("Authorization");
        String expected = "Basic " + java.util.Base64.getEncoder()
                .encodeToString((USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
        if (!expected.equals(auth)) {
            reply(ex, 401, "{\"Status\":\"Fail\",\"Message\":\"no auth\"}");
            return;
        }
        String[] path = ex.getRequestURI().getPath().split("/");
        String db = path[3];
        String table = path[4];
        String label = ex.getRequestHeaders().getFirst("label");
        if (failNextLoads.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            reply(ex, 200, "{\"Status\":\"Fail\",\"Message\":\"too many versions\",\"ErrorURL\":\"http://be/err\"}");
            return;
        }
        if (!labels.add(label)) {
            reply(ex, 200, "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"FINISHED\"}");
            return;
        }
        JsonNode rows = json.readTree(body);
        Load load = new Load(db, table, label, ex.getRequestHeaders().getFirst("columns"),
                ex.getRequestHeaders().getFirst("Expect"), rows);
        loads.add(load);
        try {
            if ("audit_column_lineage_stage".equals(table) || "audit_user_dict".equals(table)) {
                insert(db + "." + table, rows, "audit_user_dict".equals(table));
            }
        } catch (SQLException e) {
            reply(ex, 200, "{\"Status\":\"Fail\",\"Message\":\"" + e.getMessage().replace('"', '\'') + "\"}");
            return;
        }
        reply(ex, 200, "{\"Status\":\"Success\",\"NumberLoadedRows\":" + rows.size() + "}");
    }

    private void insert(String table, JsonNode rows, boolean upsert) throws SQLException {
        for (JsonNode row : rows) {
            Map<String, String> values = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> it = row.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                values.put(e.getKey(), e.getValue().isNull() ? null : e.getValue().asText());
            }
            String sql = (upsert ? "MERGE" : "INSERT") + " INTO " + table + " (" + String.join(",", values.keySet())
                    + ") VALUES (" + String.join(",", Collections.nCopies(values.size(), "?")) + ")";
            try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                int i = 1;
                for (String v : values.values()) {
                    ps.setString(i++, v);
                }
                ps.executeUpdate();
            }
        }
    }

    private static byte[] drain(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static void reply(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    Map<String, Object> lineageRow(String edgeId) throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SELECT * FROM audit_lineage_db.audit_column_lineage "
                     + "WHERE edge_id = '" + edgeId + "'")) {
            if (!rs.next()) {
                return null;
            }
            Map<String, Object> m = new HashMap<>();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                m.put(rs.getMetaData().getColumnLabel(i).toLowerCase(), rs.getObject(i));
            }
            return m;
        }
    }

    long count(String table) throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM audit_lineage_db." + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Override
    public void close() throws Exception {
        server.stop(0);
        keepAlive.close();
    }

    /** One accepted stream load. */
    static final class Load {
        final String db;
        final String table;
        final String label;
        final String columns;
        final String expect;
        final JsonNode rows;

        Load(String db, String table, String label, String columns, String expect, JsonNode rows) {
            this.db = db;
            this.table = table;
            this.label = label;
            this.columns = columns;
            this.expect = expect;
            this.rows = rows;
        }
    }
}
