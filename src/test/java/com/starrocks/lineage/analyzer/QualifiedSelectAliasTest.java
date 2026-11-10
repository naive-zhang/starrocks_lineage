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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code ORDER BY t.alias}: StarRocks 3.1 qualifies the output field of a bare column item with its relation, so a
 * select alias can be referenced as {@code relation.alias} in ORDER BY / GROUP BY / HAVING. Found by {@code analyze}
 * on the production cluster (13 of 20,000 queries); every case below was run on StarRocks 3.1.17 first.
 */
class QualifiedSelectAliasTest {

    private static final NamespaceContext NS = NamespaceContext.of("default_catalog", "q");
    private static final InMemoryMetadataProvider METADATA = new InMemoryMetadataProvider()
            .register("default_catalog.q.w", "waybill_no", "send_site_code", "site_code", "x");
    private static final StarRocksSqlAnalyzer LENIENT = new StarRocksSqlAnalyzer(METADATA);
    private static final StarRocksSqlAnalyzer STRICT = new StarRocksSqlAnalyzer(METADATA,
            SqlModeHelper.MODE_DEFAULT, true);

    private static ColumnUsageStat usage(SqlParseResult r, String column) {
        return r.usageOf(ColumnIdentifier.parse("default_catalog.q.w." + column))
                .orElseThrow(() -> new AssertionError("no usage of " + column + ": " + r.getColumnUsages()));
    }

    /** Accepted by StarRocks 3.1.17. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT t.send_site_code sendSiteCode FROM w t ORDER BY t.sendSiteCode",
        "SELECT send_site_code sendSiteCode FROM w t ORDER BY t.sendSiteCode",
        "SELECT t.send_site_code sendSiteCode, count(*) FROM w t GROUP BY t.sendSiteCode",
        "SELECT t.send_site_code sendSiteCode, count(*) FROM w t GROUP BY t.send_site_code "
                + "HAVING t.sendSiteCode = 'a'",
        "SELECT t.send_site_code sendSiteCode FROM (SELECT send_site_code FROM w) t ORDER BY t.sendSiteCode",
        "SELECT w.send_site_code sendSiteCode FROM w ORDER BY w.sendSiteCode",
        "SELECT t.send_site_code sendSiteCode FROM w t ORDER BY t.SENDSITECODE",
        "SELECT t.* FROM w t ORDER BY t.send_site_code"
    })
    void acceptedByTheEngineResolvesToTheSourceColumn(String sql) {
        for (StarRocksSqlAnalyzer analyzer : new StarRocksSqlAnalyzer[]{LENIENT, STRICT}) {
            SqlParseResult r = analyzer.analyze("q", sql, NS);
            assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> r.getMessage().orElse("") + " | " + sql);
            assertFalse(r.isDegraded(), () -> r.getWarnings() + " | " + sql);
            ColumnUsageStat send = usage(r, "send_site_code");
            assertEquals(1, send.getAccessQueryCnt(), sql);
            assertTrue(send.getOrderByCnt() + send.getGroupByCnt() >= 1, () -> send + " | " + sql);
        }
    }

    /** Rejected by StarRocks 3.1.17 ("Column 't.sendSiteCode' cannot be resolved"): rejected in strict mode too. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT upper(t.send_site_code) sendSiteCode FROM w t ORDER BY t.sendSiteCode",
        "SELECT t.send_site_code sendSiteCode FROM w t ORDER BY z.sendSiteCode",
        "SELECT t.send_site_code sendSiteCode FROM w t WHERE t.sendSiteCode = 'a'",
        "SELECT t.site_code siteCode FROM (SELECT site_code FROM w) t ORDER BY t.sendSiteCode"
    })
    void rejectedByTheEngineIsNotResolvedThroughAnAlias(String sql) {
        SqlParseResult r = STRICT.analyze("q", sql, NS);
        assertEquals(ParseStatus.ANALYSIS_ERROR, r.getStatus(), sql);
        assertTrue(r.getMessage().get().contains("cannot be resolved"), r.getMessage().get());
    }

    /** A real column of the relation wins over an alias of the same name (FROM scope first, as in StarRocks). */
    @Test
    void aColumnOfTheRelationWinsOverAQualifiedAlias() {
        SqlParseResult r = STRICT.analyze("q", "SELECT t.site_code x FROM w t ORDER BY t.x", NS);
        assertEquals(ParseStatus.SUCCESS, r.getStatus());
        assertEquals(1, usage(r, "x").getOrderByCnt(), "t.x is the column x");
        assertEquals(0, usage(r, "site_code").getOrderByCnt());
    }

    /** The production query that failed: {@code ... ) t ... order by t.sendSiteCode, t.waybillNo LIMIT 200}. */
    @Test
    void theProductionQueryIsAnalysedWithTheProductionSchema() throws IOException {
        String sql;
        try (InputStream in = getClass().getResourceAsStream("/production/order_by_qualified_alias.sql")) {
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(ProductionFixture.schema().metadata());
        SqlParseResult r = analyzer.analyze("c9535b9d-dce9-11f1-b32f-525400872ca6", sql, ProductionFixture.NAMESPACE);
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> r.getMessage().orElse(""));
        assertFalse(r.isDegraded(), r.getWarnings()::toString);
        ColumnUsageStat send = r.usageOf(ColumnIdentifier.parse(
                "default_catalog.lbdb.ods_tx_tx_site_should_sign_detail.send_site_code")).get();
        assertEquals(1, send.getSelectClauseCnt());
        assertEquals(1, send.getOrderByCnt(), "ORDER BY t.sendSiteCode is send_site_code");
        ColumnUsageStat waybill = r.usageOf(ColumnIdentifier.parse(
                "default_catalog.lbdb.ods_tx_tx_site_should_sign_detail.waybill_no")).get();
        assertEquals(1, waybill.getOrderByCnt(), "ORDER BY t.waybillNo is waybill_no (also the window ORDER BY "
                + "partition column, counted once per clause)");
        assertEquals(1, waybill.getAccessQueryCnt());
    }
}
