package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnUsageStat;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.production.ProductionFixture;
import com.starrocks.sql.parser.SqlModeHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One alias for a table and a sub-query of the same FROM: StarRocks 3.1 checks alias uniqueness separately for
 * tables and for sub-queries, and a qualified column belongs to the relation that has it. Found by the pipeline on
 * production (288 dead letters "Not unique table/alias: 't1'"); every case was run on StarRocks 3.1.17 first.
 */
class DuplicateAliasTest {

    private static final NamespaceContext NS = NamespaceContext.of("default_catalog", "d");
    private static final InMemoryMetadataProvider METADATA = new InMemoryMetadataProvider()
            .register("default_catalog.d.f", "code", "area", "w")
            .register("default_catalog.d.st", "site_code", "site_path", "area");
    private static final StarRocksSqlAnalyzer STRICT = new StarRocksSqlAnalyzer(METADATA,
            SqlModeHelper.MODE_DEFAULT, true);
    private static final String SUB = "(SELECT area, SUM(w) wsum FROM f GROUP BY area)";

    private static ColumnUsageStat usage(SqlParseResult r, String column) {
        return r.usageOf(ColumnIdentifier.parse("default_catalog.d." + column))
                .orElseThrow(() -> new AssertionError("no usage of " + column + ": " + r.getColumnUsages()));
    }

    /** Accepted by 3.1.17: the column decides which t1 is meant. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT t1.wsum, t1.site_path FROM f t LEFT JOIN " + SUB + " t1 ON t.area = t1.area "
                + "INNER JOIN st t1 ON t.code = t1.site_code",
        "SELECT wsum, site_path FROM f t LEFT JOIN " + SUB + " t1 ON t.area = t1.area "
                + "INNER JOIN st t1 ON t.code = t1.site_code",
        "SELECT t1.wsum, t1.site_path FROM st t1 JOIN " + SUB + " t1 ON 1 = 1",
        "SELECT t1.site_path FROM f t LEFT JOIN " + SUB + " t1 ON t.area = t1.area "
                + "INNER JOIN st t1 ON t.code = t1.site_code AND t1.wsum > 0",
        "SELECT t1.wsum, t1.site_path FROM st t1 JOIN (WITH c AS " + SUB + " SELECT * FROM c) t1 ON 1 = 1"
    })
    void aTableAndASubqueryMayShareAnAlias(String sql) {
        SqlParseResult r = STRICT.analyze("q", sql, NS);
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> r.getMessage().orElse("") + " | " + sql);
        assertFalse(r.isDegraded(), () -> r.getWarnings() + " | " + sql);
        assertEquals(1, usage(r, "st.site_path").getAccessQueryCnt(), "t1.site_path is st.site_path");
        assertTrue(r.getColumnUsages().containsKey(ColumnIdentifier.parse("default_catalog.d.f.w")),
                "t1.wsum is SUM(f.w)");
    }

    /** {@code t1.*} expands both relations in FROM order (3.1.17: area, wsum, site_code, site_path, area). */
    @Test
    void qualifiedStarExpandsBothRelations() {
        StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(new InMemoryMetadataProvider()
                .register("default_catalog.d.f", "code", "area", "w")
                .register("default_catalog.d.st", "site_code", "site_path", "area")
                .register("default_catalog.d.tgt", "c1", "c2", "c3", "c4", "c5"));
        SqlParseResult r = analyzer.analyze("q", "INSERT INTO tgt SELECT t1.* FROM f t LEFT JOIN " + SUB
                + " t1 ON t.area = t1.area INNER JOIN st t1 ON t.code = t1.site_code", NS);
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> r.getMessage().orElse(""));
        assertFalse(r.isDegraded(), r.getWarnings()::toString);
        assertTrue(r.getLineageEdges().stream().anyMatch(e -> e.getTarget().getColumn().equals("c4")
                && e.getSource().getColumn().equals("site_path")), "4th column of t1.* is st.site_path");
        assertTrue(r.getLineageEdges().stream().anyMatch(e -> e.getTarget().getColumn().equals("c2")
                && e.getSource().getColumn().equals("w")), "2nd column of t1.* is wsum = SUM(f.w)");
    }

    /** Rejected by 3.1.17, rejected here with the same class of error. */
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "SELECT t1.area FROM f t LEFT JOIN (SELECT area, SUM(w) wsum FROM f GROUP BY area) t1 ON t.area = t1.area "
                + "INNER JOIN st t1 ON t.code = t1.site_code | is ambiguous",
        "SELECT x.w FROM f x JOIN st x ON x.code = x.site_code | Not unique",
        "SELECT t1.w FROM f t1, st t1 | Not unique",
        "SELECT t1.wsum FROM (SELECT area, SUM(w) wsum FROM f GROUP BY area) t1 "
                + "JOIN (SELECT site_path sp FROM st) t1 ON 1 = 1 | Not unique"
    })
    void sameKindDuplicatesAndSharedColumnsAreErrors(String sql, String expected) {
        SqlParseResult r = STRICT.analyze("q", sql, NS);
        assertEquals(ParseStatus.ANALYSIS_ERROR, r.getStatus(), sql);
        assertTrue(r.getMessage().get().contains(expected), r.getMessage().get());
    }

    /** The production query (BI report, 288 occurrences). */
    @Test
    void theProductionQueryIsAnalysedWithTheProductionSchema() throws IOException {
        String sql;
        try (InputStream in = getClass().getResourceAsStream("/production/duplicate_alias_table_and_subquery.sql")) {
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(ProductionFixture.schema().metadata());
        SqlParseResult r = analyzer.analyze("0287a8e7-db1f-11f1-b32f-525400872ca6", sql, ProductionFixture.NAMESPACE);
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> r.getMessage().orElse(""));
        assertFalse(r.isDegraded(), r.getWarnings()::toString);
        ColumnUsageStat sitePath = r.usageOf(ColumnIdentifier.parse(
                "default_catalog.lbdb.bd_customer_area_station.site_path")).get();
        assertEquals(1, sitePath.getAccessQueryCnt());
        assertEquals(1, sitePath.getWhereClauseCnt(), "the site_path filter of the outer blocks");
        ColumnUsageStat siteCode = r.usageOf(ColumnIdentifier.parse(
                "default_catalog.lbdb.bd_customer_area_station.site_code")).get();
        assertEquals(1, siteCode.getJoinClauseCnt(), "inner join ... on t.plan_handover_center_code = t1.site_code");
        ColumnUsageStat weight = r.usageOf(ColumnIdentifier.parse(
                "default_catalog.lbdb.ads_trans_center_plan_transfer_cnt.two_frequency_send_weight")).get();
        assertEquals(1, weight.getSelectClauseCnt());
    }
}
