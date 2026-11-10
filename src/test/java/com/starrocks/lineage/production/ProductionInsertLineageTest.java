package com.starrocks.lineage.production;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.model.TransformType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Column lineage of the 30 production INSERT statements ({@link ProductionFixture}) analysed with the complete
 * production schema, exactly as the pipeline would: one audit statement each, comments included, session database
 * {@code lbdb}.
 *
 * <p>Every statement is checked three ways:</p>
 * <ol>
 *   <li><b>Structural oracles</b>, independent of the analyzer's output: the statement analyses without any
 *       degradation; the target is the declared one; every target column is a column of the target table; the
 *       columns that receive a DIRECT edge are exactly the INSERT column list (or every column of the table when there
 *       is no list); every source is a column of a table of the dump.</li>
 *   <li><b>Golden snapshot</b> ({@code production/inserts_30.lineage}): every DIRECT edge with its transform, the
 *       control columns of the INDIRECT edges and the referenced tables. Any later change of production lineage is
 *       visible. Regenerate with {@code -Dproduction.golden.write=<file>} and review the diff.</li>
 *   <li><b>Hand-verified edges</b> of selected statements, derived by reading the SQL (the {@code handVerified*} tests).</li>
 * </ol>
 */
class ProductionInsertLineageTest {

    private static final Map<Integer, SqlParseResult> RESULTS = new ConcurrentHashMap<>();
    private static InMemoryMetadataProvider metadata;
    private static StarRocksSqlAnalyzer analyzer;

    @BeforeAll
    static void load() {
        metadata = ProductionFixture.schema().metadata();
        analyzer = new StarRocksSqlAnalyzer(metadata);
    }

    static Stream<ProductionFixture.Statement> statements() {
        return ProductionFixture.statements().stream();
    }

    static SqlParseResult analyse(ProductionFixture.Statement s) {
        return RESULTS.computeIfAbsent(s.getIndex(), i -> analyzer.analyze("prod-" + s.getTaskInstanceId(),
                s.getText(), ProductionFixture.NAMESPACE));
    }

    // ================================================================================================ the data

    @Test
    void theProvidedDataIsCompletelyLoaded() {
        ProductionFixture.Schema schema = ProductionFixture.schema();
        assertEquals(997, schema.tables().size(), "997 base tables in the dump");
        Map<String, Integer> expected = new TreeMap<>();
        expected.put("dperm", 20);
        expected.put("eagle", 3);
        expected.put("lbdb", 955);
        expected.put("lbdb_uat", 17);
        expected.put("test", 2);
        assertEquals(expected, new TreeMap<>(schema.tablesPerDatabase()), "counts of the dump header");
        assertTrue(schema.table(TableIdentifier.of("default_catalog", "lbdb", "dws_busi_site_tags_info_f_d_1\r\n"))
                .isPresent(), "the table whose name ends with CRLF is kept as exported");
        assertTrue(schema.tables().values().stream().allMatch(t -> !t.getColumns().isEmpty()));

        List<ProductionFixture.Statement> statements = ProductionFixture.statements();
        assertEquals(30, statements.size());
        for (int i = 0; i < 30; i++) {
            assertEquals(i + 1, statements.get(i).getIndex(), "file order");
            assertFalse(statements.get(i).getTaskInstanceId().isEmpty());
        }
        assertEquals(30, statements.stream().map(ProductionFixture.Statement::getTarget).distinct().count(),
                "30 different targets");
        assertTrue(statements.stream().allMatch(s -> schema.table(s.getTarget()).isPresent()),
                "every target is a table of the dump");
    }

    // ================================================================================================ oracles

    @ParameterizedTest(name = "{0}")
    @MethodSource("statements")
    void analysesWithoutDegradation(ProductionFixture.Statement s) {
        SqlParseResult r = analyse(s);
        assertEquals(ParseStatus.SUCCESS, r.getStatus(), () -> r.getMessage().orElse(""));
        assertEquals(StatementType.INSERT_INTO, r.getStatementType().get());
        assertEquals(s.getTarget(), r.getTargetTable().get());
        assertFalse(r.isDegraded(), () -> "warnings: " + r.getWarnings());
        assertEquals("SLL", r.getPredictionMode().orElse(""), "the fast parser path suffices");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("statements")
    void everyTargetColumnIsWrittenFromItsSources(ProductionFixture.Statement s) {
        SqlParseResult r = analyse(s);
        TableSchema target = ProductionFixture.schema().table(s.getTarget()).get();
        Set<String> tableColumns = target.getColumnNames().stream().map(c -> c.toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> written = new TreeSet<>(s.columnList().orElse(new ArrayList<>(tableColumns)));
        assertTrue(tableColumns.containsAll(written), "the column list names columns of the table");

        Set<String> direct = new TreeSet<>();
        for (ColumnLineageEdge e : r.getLineageEdges()) {
            assertEquals(s.getTarget(), e.getTarget().getTable());
            assertTrue(tableColumns.contains(e.getTarget().getColumn()), () -> "not a target column: " + e);
            if (e.getLineageType() == LineageType.DIRECT) {
                direct.add(e.getTarget().getColumn());
            }
        }
        assertEquals(written, direct, "each written column has a DIRECT edge, no other column has one");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("statements")
    void everySourceIsAColumnOfAProductionTable(ProductionFixture.Statement s) {
        SqlParseResult r = analyse(s);
        for (ColumnLineageEdge e : r.getLineageEdges()) {
            ColumnIdentifier src = e.getSource();
            if (src.isConstant()) {
                assertEquals(TransformType.CONSTANT, e.getTransformType(), e::toString);
                assertEquals(LineageType.DIRECT, e.getLineageType(), e::toString);
                continue;
            }
            TableSchema table = ProductionFixture.schema().table(src.getTable())
                    .orElseThrow(() -> new AssertionError("source is not a table of the dump (CTE / alias leak?): "
                            + e));
            assertTrue(table.hasColumn(src.getColumn()), () -> "no such column: " + e);
        }
        for (TableIdentifier t : r.getSourceTables()) {
            assertTrue(ProductionFixture.schema().table(t).isPresent(), () -> "not a table: " + t);
        }
    }

    // ================================================================================================ golden

    @ParameterizedTest(name = "{0}")
    @MethodSource("statements")
    void lineageMatchesTheReviewedSnapshot(ProductionFixture.Statement s) throws IOException {
        Map<Integer, List<String>> golden = ProductionFixture.golden(ProductionInsertLineageTest::analyse);
        assertEquals(golden.get(s.getIndex()), ProductionFixture.snapshot(s, analyse(s)),
                "lineage of " + s + " changed; regenerate with -Dproduction.golden.write=<file> and review");
    }

    // ================================================================================================ by hand

    private static Set<String> directSources(SqlParseResult r, String targetColumn) {
        return r.getLineageEdges().stream().filter(e -> e.getLineageType() == LineageType.DIRECT)
                .filter(e -> e.getTarget().getColumn().equals(targetColumn))
                .map(e -> (e.getSource().isConstant() ? "<constant>" : e.getSource().getTable().getTable() + "."
                        + e.getSource().getColumn()) + " " + e.getTransformType())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static Set<String> indirectSources(SqlParseResult r) {
        return r.getLineageEdges().stream().filter(e -> e.getLineageType() == LineageType.INDIRECT)
                .map(e -> e.getSource().getTable().getTable() + "." + e.getSource().getColumn())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static ProductionFixture.Statement statement(int index) {
        return ProductionFixture.statements().get(index - 1);
    }

    private static Set<String> set(String... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    /**
     * [01] {@code insert into lbdb.dws_busi_inbill_site_wgt_rt(17 columns) select ... from
     * lbdb.tmp_busi_inbill_site_wgt_rt_1}: 16 columns copied by name, {@code stats_date_end_tm} is the literal
     * {@code '2026-11-10 09:05:00'}, no WHERE / JOIN, so no INDIRECT edge; {@code etl_time} is not written.
     */
    @Test
    void handVerifiedStatement01() {
        SqlParseResult r = analyse(statement(1));
        assertEquals(17, r.getLineageEdges().size());
        assertEquals(set("<constant> CONSTANT"), directSources(r, "stats_date_end_tm"));
        for (String c : Arrays.asList("stats_date_bgn_tm", "stats_site_lvl", "stats_site_code", "weight_type",
                "weight_group", "class_area_code", "stats_date", "stats_site_name", "stats_hour", "stats_min",
                "calc_weight_ccd", "target_val", "workday_cnt_ccm", "total_workday_cnt_cur_mon", "total_ticket_ccd",
                "parent_site_code")) {
            assertEquals(set("tmp_busi_inbill_site_wgt_rt_1." + c + " IDENTITY"), directSources(r, c), c);
        }
        assertTrue(directSources(r, "etl_time").isEmpty(), "filled by DEFAULT CURRENT_TIMESTAMP");
        assertTrue(indirectSources(r).isEmpty());
    }

    /**
     * [23] {@code INSERT INTO dim_gis_tb_base_site_info_d (14 columns) SELECT ..., NOW() AS etl_time FROM
     * ods_gis_tb_base_site WHERE stats_date = '2026-11-09' AND electronic_fence IS NOT NULL}: 13 identity copies,
     * one constant, and the two filter columns control all 14 target columns (14 + 2 x 14 = 42 edges).
     */
    @Test
    void handVerifiedStatement23() {
        SqlParseResult r = analyse(statement(23));
        assertEquals(42, r.getLineageEdges().size());
        assertEquals(set("<constant> CONSTANT"), directSources(r, "etl_time"));
        for (String c : Arrays.asList("stats_date", "id", "site_code", "site_name", "parent_id", "site_type",
                "site_nature", "service_type", "coordinate", "electronic_fence", "approve_status", "bis_status",
                "modified_time")) {
            assertEquals(set("ods_gis_tb_base_site." + c + " IDENTITY"), directSources(r, c), c);
        }
        assertEquals(set("ods_gis_tb_base_site.electronic_fence", "ods_gis_tb_base_site.stats_date"),
                indirectSources(r));
        assertEquals(set("default_catalog.lbdb.dim_gis_tb_base_site_info_d", "default_catalog.lbdb"
                + ".ods_gis_tb_base_site"), r.getSourceTables().stream().map(Object::toString)
                .collect(Collectors.toCollection(TreeSet::new)), "referenced tables include the target");
    }

    /**
     * [08] reads its own target ({@code select * from dwd_oper_sign_timeout_detail_rt t1}) and joins the latest row
     * per waybill of {@code ods_tx_tx_site_should_sign_detail} ({@code row_number() ... rn = 1}). The join
     * {@code left join tx_basic_dict_entry t3 ON type_code = 'pick_up'} uses an <b>unqualified</b> {@code type_code}:
     * of the three relations in scope only {@code tx_basic_dict_entry} has that column (which is why StarRocks ran
     * the statement), so it must resolve there - with incomplete metadata it is ambiguous.
     */
    @Test
    void handVerifiedStatement08() {
        SqlParseResult r = analyse(statement(8));
        assertEquals(set("dwd_oper_sign_timeout_detail_rt.waybill_no IDENTITY"), directSources(r, "waybill_no"));
        assertEquals(set("dwd_oper_sign_timeout_detail_rt.stats_date IDENTITY"), directSources(r, "stats_date"));
        assertEquals(set("ods_tx_tx_site_should_sign_detail.sign_status IDENTITY"), directSources(r, "sign_status"));
        assertEquals(set("ods_tx_tx_site_should_sign_detail.reach_scan_time IDENTITY"),
                directSources(r, "arrival_scan_time"), "renamed in the subquery");
        assertEquals(set("tx_basic_dict_entry.dict_name IDENTITY"), directSources(r, "delivery_type_name"));
        assertEquals(set("<constant> CONSTANT"), directSources(r, "sign_timeout_time"));
        assertEquals(set("<constant> CONSTANT"), directSources(r, "etl_time"));
        Set<String> control = indirectSources(r);
        assertTrue(control.contains("tx_basic_dict_entry.type_code"), "unqualified join column resolved: " + control);
        assertTrue(control.containsAll(set("dwd_oper_sign_timeout_detail_rt.stats_date",
                "ods_tx_tx_site_should_sign_detail.update_time", "ods_tx_tx_site_should_sign_detail.etl_time",
                "ods_tx_tx_site_should_sign_detail.validity_lasted_sign_time", "tx_basic_dict_entry.dict_code")),
                "filters, window ORDER BY and join keys: " + control);
    }

    /**
     * [15] full outer join of two CTEs merged with {@code COALESCE}, department levels derived by
     * {@code CASE WHEN dept5_name LIKE '%部' THEN dept5_hid ...}, then four UNION ALL roll-ups summed again:
     * <ul>
     *   <li>{@code center_name = COALESCE(a.center_name, b.center_name)}: {@code a} from
     *       {@code dwd_oper_region_position_status_i_d.center_name}, {@code b} from {@code dept4_name};</li>
     *   <li>{@code employed_avg = sum((COALESCE(employed_early_count, 0) + COALESCE(employed_count, 0)) / 2.0)} with
     *       {@code employed_early_count = sum(if(labor_date <= ...data_date..., 1, 0))}.</li>
     * </ul>
     */
    @Test
    void handVerifiedStatement15() {
        SqlParseResult r = analyse(statement(15));
        assertEquals(set("dwd_oper_region_position_status_i_d.center_name EXPRESSION",
                "ods_ehr_tb_emp_adc_f_d.dept4_name EXPRESSION"), directSources(r, "center_name"));
        assertEquals(set("dwd_oper_region_position_status_i_d.employed_count AGGREGATION",
                "ods_ehr_tb_emp_adc_f_d.data_date AGGREGATION", "ods_ehr_tb_emp_adc_f_d.labor_date AGGREGATION"),
                directSources(r, "employed_avg"));
        Set<String> department = directSources(r, "department_hid");
        assertTrue(department.containsAll(set("dwd_oper_region_position_status_i_d.department_hid EXPRESSION",
                "ods_ehr_tb_emp_adc_f_d.dept5_hid EXPRESSION", "ods_ehr_tb_emp_adc_f_d.dept5_name EXPRESSION")),
                department::toString);
    }
}
