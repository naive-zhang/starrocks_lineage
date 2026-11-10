package com.starrocks.lineage.it;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.JdbcStarRocksMetadataProvider;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.pipeline.AuditLogSyncPipeline;
import com.starrocks.lineage.pipeline.CycleReport;
import com.starrocks.lineage.pipeline.JdbcAuditLogSource;
import com.starrocks.lineage.pipeline.JdbcCheckpointStore;
import com.starrocks.lineage.pipeline.PipelineConfig;
import com.starrocks.lineage.pipeline.StarRocksSink;
import com.starrocks.lineage.pipeline.StreamLoadClient;
import com.starrocks.lineage.production.ProductionFixture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The 30 production INSERT statements on a real StarRocks 3.1.17 (the production version), with the tables they use
 * created from the production schema dump:
 * <ol>
 *   <li>every statement is <b>executed</b> by the engine - the SQL is what production ran;</li>
 *   <li>analysed with the engine's own {@code information_schema} (the production metadata path), the lineage is
 *       identical to the reviewed snapshot computed from the dump;</li>
 *   <li>through the whole pipeline with the real sink, the lineage table holds exactly that lineage.</li>
 * </ol>
 */
@Testcontainers(disabledWithoutDocker = true)
class ProductionInsertsIT {

    private static StarRocksCluster sr;
    private static final List<TableIdentifier> TABLES = new ArrayList<>();

    @BeforeAll
    static void createProductionTables() throws Exception {
        sr = StarRocksCluster.get();
        sr.resetPipelineTables();
        ProductionFixture.Schema schema = ProductionFixture.schema();
        TreeSet<String> names = new TreeSet<>();
        for (List<String> lines : ProductionFixture.golden(s -> {
            throw new IllegalStateException("snapshot is read, not written, here");
        }).values()) {
            for (String line : lines) {
                if (line.substring(3).startsWith("T ")) {
                    names.add(line.substring(5));
                }
            }
        }
        sr.execute("DROP DATABASE IF EXISTS lbdb FORCE", "CREATE DATABASE lbdb");
        try (Connection c = sr.dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("USE lbdb");
            for (String name : names) {
                TableIdentifier id = TableIdentifier.of("default_catalog", "lbdb", name.substring(5));
                String ddl = schema.ddl(id).replace("\"replication_num\" = \"3\"", "\"replication_num\" = \"1\"");
                st.execute(ddl.trim().replaceAll(";$", ""));
                TABLES.add(id);
            }
        }
    }

    private static long num(Object o) {
        return o instanceof BigDecimal ? ((BigDecimal) o).longValue() : ((Number) o).longValue();
    }

    @Test
    void theEngineExecutesEveryStatementAndRealMetadataGivesTheSnapshot() throws Exception {
        assertEquals(67, TABLES.size(), "tables referenced by the 30 statements");
        try (Connection c = sr.dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("USE lbdb");
            for (ProductionFixture.Statement s : ProductionFixture.statements()) {
                try {
                    st.execute(s.getText());
                } catch (SQLException e) {
                    throw new AssertionError("StarRocks 3.1 rejected " + s + ": " + e.getMessage(), e);
                }
            }
        }

        JdbcStarRocksMetadataProvider metadata = new JdbcStarRocksMetadataProvider(sr.dataSource());
        StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(metadata);
        Map<Integer, List<String>> golden = ProductionFixture.golden(s -> {
            throw new IllegalStateException("snapshot is read, not written, here");
        });
        for (ProductionFixture.Statement s : ProductionFixture.statements()) {
            SqlParseResult r = analyzer.analyze("it", s.getText(), ProductionFixture.NAMESPACE);
            assertTrue(r.isSuccess() && !r.isDegraded(), () -> s + " " + r.getMessage() + " " + r.getWarnings());
            assertEquals(golden.get(s.getIndex()), ProductionFixture.snapshot(s, r),
                    "real information_schema gives the same lineage as the dump for " + s);
        }
    }

    @Test
    void thePipelineWritesTheProductionLineage() throws Exception {
        sr.resetPipelineTables();
        LocalDateTime hour = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).minusDays(1).plusHours(8);
        List<ProductionFixture.Statement> statements = ProductionFixture.statements();
        try (Connection c = sr.dataSource().getConnection(); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO starrocks_audit_db__.starrocks_audit_tbl__ (queryId, `timestamp`, queryType, clientIp, "
                        + "`user`, authorizedUser, resourceGroup, `catalog`, db, state, errorCode, queryTime, "
                        + "scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, stmtId, isQuery, feIp, stmt, "
                        + "digest, planCpuCosts, planMemCosts) VALUES (?, ?, 'Query', '10.0.0.1', 'ds_etl', "
                        + "'ds_etl', 'default_wg', 'default_catalog', 'lbdb', 'EOF', '', 1000, 0, 0, 0, 0, 0, 1, 0, "
                        + "'fe', ?, ?, 0, 0)")) {
            for (int i = 0; i < statements.size(); i++) {
                ps.setString(1, "prod-" + statements.get(i).getTaskInstanceId());
                ps.setObject(2, hour.plusMinutes(i));
                ps.setString(3, statements.get(i).getText());
                ps.setString(4, "ds-" + statements.get(i).getIndex());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        JdbcStarRocksMetadataProvider metadata = new JdbcStarRocksMetadataProvider(sr.dataSource());
        try (StreamLoadClient client = sr.streamLoadClient();
             AuditLogSyncPipeline p = new AuditLogSyncPipeline(PipelineConfig.builder("prod_it", "it-node")
                     .initialStart(hour.minusHours(1)).sliceSize(Duration.ofHours(1)).analyzerThreads(4).build(),
                     new JdbcAuditLogSource(sr.dataSource()), new JdbcCheckpointStore(sr.dataSource()),
                     new StarRocksSink(sr.dataSource(), client, "audit_lineage_db", "prod_it"),
                     new StarRocksSqlAnalyzer(metadata), metadata, Clock.systemUTC())) {
            long records = 0;
            for (int i = 0; i < 100; i++) {
                CycleReport r = p.runOnce();
                if (r.getOutcome() != CycleReport.Outcome.COMMITTED) {
                    break;
                }
                records += r.getRecords();
            }
            assertEquals(30, records);
            assertEquals(0, p.getMetrics().getDegraded());
        }
        long expectedEdges = 0;
        for (List<String> lines : ProductionFixture.golden(s -> {
            throw new IllegalStateException("snapshot is read, not written, here");
        }).values()) {
            String totals = lines.get(lines.size() - 1);
            expectedEdges += Long.parseLong(totals.replaceAll(".* edges=(\\d+) .*", "$1"));
        }
        assertEquals(expectedEdges, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_column_lineage")
                .get(0).get("c")), "one lineage row per edge of the 30 statements");
        assertEquals(30L, num(sr.query("SELECT COUNT(DISTINCT target_table) c FROM audit_lineage_db"
                + ".audit_column_lineage").get(0).get("c")));
        assertEquals(1L, num(sr.query("SELECT MAX(exec_count) c FROM audit_lineage_db.audit_column_lineage")
                .get(0).get("c")));
        assertEquals(0L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_dead_letter")
                .get(0).get("c")), "no production statement is a dead letter");
        assertEquals(30L, num(sr.query("SELECT COUNT(*) c FROM audit_lineage_db.audit_lineage_batch_member")
                .get(0).get("c")));
    }
}
