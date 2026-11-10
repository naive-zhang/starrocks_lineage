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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        assertEquals(new TreeSet<>(StarRocksCluster.PIPELINE_TABLES), tables);

        String lineage = createTable("audit_column_lineage");
        assertTrue(lineage.contains("PRIMARY KEY(`edge_id`)"), lineage);
        assertTrue(lineage.contains("ORDER BY(`target_db`, `target_table`, `target_column`)"), lineage);
        assertFalse(lineage.contains("last_batch_id"), "a projection of the daily facts, never incremented");

        String daily = createTable("audit_column_lineage_daily");
        assertTrue(daily.contains("AGGREGATE KEY(`stat_day`, `edge_id`)"), daily);
        assertTrue(daily.contains("`exec_count` bigint(20) SUM"), daily);
        assertTrue(daily.contains("`first_seen_time` datetime MIN"), daily);
        assertTrue(daily.contains("`last_seen_time` datetime MAX"), daily);
        assertTrue(daily.contains("date_trunc('day', stat_day)"), daily);

        String usage = createTable("audit_column_hourly_usage");
        assertTrue(usage.contains("AGGREGATE KEY(`stat_hour`, `catalog_name`, `db_name`, `table_name`, "
                + "`column_name`)"), usage);
        assertTrue(usage.contains("`user_bitmap` bitmap BITMAP_UNION"), usage);
        assertTrue(usage.contains("`user_hll` hll HLL_UNION"), usage);
        assertTrue(usage.contains("`max_query_time_ms` bigint(20) MAX"), usage);
        assertTrue(usage.contains("date_trunc('day', stat_hour)"), usage);
        assertTrue(usage.contains("\"partition_live_number\" = \"400\""), usage);
        assertFalse(usage.contains("dynamic_partition"), "gap 1: dynamic partitions rejected yesterday's rows");

        assertTrue(createTable("audit_user_dict").contains("`first_seen_time` datetime MIN"));
        String checkpoint = createTable("audit_lineage_sync_checkpoint");
        assertTrue(checkpoint.contains("PRIMARY KEY(`pipeline_name`)"), checkpoint);
        assertTrue(checkpoint.contains("`pending_step`"), checkpoint);
        assertTrue(createTable("audit_lineage_batch_member")
                .contains("PRIMARY KEY(`pipeline_name`, `query_id`, `audit_time`)"));
        assertTrue(createTable("audit_lineage_batch_lineage")
                .contains("PRIMARY KEY(`batch_id`, `edge_id`, `stat_day`, `refresh_only`, `batch_time`)"));
        assertTrue(createTable("audit_lineage_dead_letter")
                .contains("PRIMARY KEY(`pipeline_name`, `query_id`, `kind`)"));
    }

    /** Gap 1: a load creates the partition of any day it touches, yesterday and last year included. */
    @Test
    void expressionPartitionsAreCreatedByTheLoadsForPastDays() {
        sr.execute("INSERT INTO audit_lineage_db.audit_column_hourly_usage (stat_hour, catalog_name, db_name, "
                        + "table_name, column_name, access_query_cnt, user_bitmap, user_hll, last_access_time) "
                        + "VALUES (DATE_SUB(DATE_TRUNC('hour', NOW()), INTERVAL 1 DAY), 'c', 'd', 'ddl_it', 'x', 1, "
                        + "to_bitmap(1), hll_hash('u'), NOW()), "
                        + "(DATE_SUB(DATE_TRUNC('hour', NOW()), INTERVAL 200 DAY), 'c', 'd', 'ddl_it', 'x', 1, "
                        + "to_bitmap(1), hll_hash('u'), NOW())");
        List<Map<String, Object>> rows = sr.query("SELECT COUNT(*) AS n FROM audit_lineage_db"
                + ".audit_column_hourly_usage WHERE table_name = 'ddl_it'");
        assertEquals(2L, ((Number) rows.get(0).get("n")).longValue());
        assertTrue(sr.query("SHOW PARTITIONS FROM audit_lineage_db.audit_column_hourly_usage").size() >= 2);
    }

    @Test
    void everyBusinessQueryRunsOnTheRealSchema() {
        String script = StarRocksCluster.read(StarRocksCluster.projectDir().resolve("sql/query/business_queries.sql"))
                .replace("${start_time}", "2024-01-01 00:00:00").replace("${end_time}", "2030-01-01 00:00:00")
                .replace("${day}", "2024-06-18").replace("${catalog}", "default_catalog")
                .replace("${db}", "sales").replace("${table}", "orders").replace("${column}", "amount");
        List<String> statements = StarRocksCluster.splitScript(script);
        assertEquals(9, statements.size(), "Q1..Q7, Q9, Q10 (Q8 is a comment)");
        List<List<Map<String, Object>>> results = sr.executeScript(script);
        assertEquals(9, results.size());
    }

    private static String createTable(String table) {
        return String.valueOf(sr.query("SHOW CREATE TABLE audit_lineage_db." + table).get(0).get("Create Table"));
    }
}
