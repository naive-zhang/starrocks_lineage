package com.starrocks.lineage.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shipped DDL ({@code sql/ddl}) and business queries ({@code sql/query}) on a real StarRocks 3.1.
 */
@Testcontainers(disabledWithoutDocker = true)
class StarRocksDdlIT {

    private static StarRocksCluster sr;

    @BeforeAll
    static void start() {
        sr = StarRocksCluster.get();
        sr.applyProjectDdl();
    }

    @Test
    void versionIsBranch31() {
        String version = String.valueOf(sr.query("SELECT current_version() AS v").get(0).get("v"));
        assertTrue(version.startsWith("3.1."), version);
    }

    @Test
    void everyTableIsCreatedWithTheIntendedModel() {
        Set<String> tables = sr.query("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = "
                + "'audit_lineage_db'").stream().map(r -> String.valueOf(r.get("TABLE_NAME")))
                .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(new TreeSet<>(java.util.Arrays.asList("audit_column_hourly_usage", "audit_column_lineage",
                "audit_column_lineage_stage", "audit_lineage_merge_log", "audit_lineage_sync_checkpoint",
                "audit_user_dict")), tables);

        String lineage = createTable("audit_column_lineage");
        assertTrue(lineage.contains("PRIMARY KEY(`edge_id`)"), lineage);
        assertTrue(lineage.contains("ORDER BY(`target_db`, `target_table`, `target_column`)"), lineage);
        assertTrue(lineage.contains("`last_batch_id`"), lineage);

        String usage = createTable("audit_column_hourly_usage");
        assertTrue(usage.contains("AGGREGATE KEY(`stat_hour`, `catalog_name`, `db_name`, `table_name`, "
                + "`column_name`)"), usage);
        assertTrue(usage.contains("`user_bitmap` bitmap BITMAP_UNION"), usage);
        assertTrue(usage.contains("`user_hll` hll HLL_UNION"), usage);
        assertTrue(usage.contains("`max_query_time_ms` bigint(20) MAX"), usage);
        assertTrue(usage.contains("\"dynamic_partition.time_unit\" = \"DAY\""), usage);

        String checkpoint = createTable("audit_lineage_sync_checkpoint");
        assertTrue(checkpoint.contains("PRIMARY KEY(`pipeline_name`)"), checkpoint);
        assertTrue(checkpoint.contains("`pending_batch_id`"), checkpoint);
    }

    @Test
    void dynamicPartitionsExistForTodayAndTheNextDays() {
        List<Map<String, Object>> usage = sr.query("SHOW PARTITIONS FROM audit_lineage_db.audit_column_hourly_usage");
        assertTrue(usage.size() >= 4, "today + 3 days pre-created: " + usage.size());
        List<Map<String, Object>> stage = sr.query(
                "SHOW PARTITIONS FROM audit_lineage_db.audit_column_lineage_stage");
        assertTrue(stage.size() >= 3, "today + 2 days pre-created: " + stage.size());
    }

    @Test
    void everyBusinessQueryRunsOnTheRealSchema() {
        String script = StarRocksCluster.read(StarRocksCluster.projectDir().resolve("sql/query/business_queries.sql"))
                .replace("${start_time}", "2024-01-01 00:00:00").replace("${end_time}", "2030-01-01 00:00:00")
                .replace("${day}", "2024-06-18").replace("${catalog}", "default_catalog")
                .replace("${db}", "sales").replace("${table}", "orders").replace("${column}", "amount");
        List<String> statements = StarRocksCluster.splitScript(script);
        assertEquals(7, statements.size(), "Q1..Q7 (Q8 is a comment)");
        List<List<Map<String, Object>>> results = sr.executeScript(script);
        assertEquals(7, results.size());
    }

    private static String createTable(String table) {
        return String.valueOf(sr.query("SHOW CREATE TABLE audit_lineage_db." + table).get(0).get("Create Table"));
    }
}
