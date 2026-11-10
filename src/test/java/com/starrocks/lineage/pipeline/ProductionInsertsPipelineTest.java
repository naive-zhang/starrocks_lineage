package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.production.ProductionFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The 30 production INSERT statements ({@link ProductionFixture}) as audit records of one hour, through the whole
 * pipeline with the production schema: every statement is a lineage SUCCESS, nothing is dead-lettered, and the
 * lineage table holds exactly the edges the analyzer derives, each executed once.
 */
class ProductionInsertsPipelineTest {

    private static final LocalDateTime HOUR = LocalDateTime.of(2026, 11, 10, 8, 0);

    private FakeWarehouse wh;
    private H2Checkpoints h2;
    private AuditLogSyncPipeline pipeline;
    private InMemoryMetadataProvider metadata;

    @BeforeEach
    void setUp() throws Exception {
        wh = new FakeWarehouse("audit_lineage");
        h2 = new H2Checkpoints();
        metadata = ProductionFixture.schema().metadata();
        MutableClock clock = new MutableClock(HOUR.plusHours(2));
        pipeline = new AuditLogSyncPipeline(PipelineConfig.builder("audit_lineage", "node").initialStart(HOUR)
                .analyzerThreads(4).sliceSize(java.time.Duration.ofHours(1)).build(), wh, h2.store(clock), wh,
                new StarRocksSqlAnalyzer(metadata), metadata, clock);
        List<ProductionFixture.Statement> statements = ProductionFixture.statements();
        for (int i = 0; i < statements.size(); i++) {
            wh.addAudit(statements.get(i).auditRecord(HOUR.plusMinutes(i)));
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        pipeline.close();
        h2.close();
    }

    @Test
    void theThirtyStatementsProduceTheirLineageThroughThePipeline() {
        CycleReport report = pipeline.runOnce();
        assertEquals(CycleReport.Outcome.COMMITTED, report.getOutcome(), report.toString());
        assertEquals(30, report.getRecords());
        assertEquals(30, report.statusCount(ParseStatus.SUCCESS), report.toString());
        assertEquals(0, report.getDeadLetters());
        assertEquals(0, pipeline.getMetrics().getDegraded(), "production schema: nothing degraded");
        assertTrue(wh.deadLetters.isEmpty(), wh.deadLetters::toString);
        assertEquals(0, wh.totalAccess(), "INSERTs produce lineage, not query usage");

        StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(metadata);
        Set<String> expected = new HashSet<>();
        for (ProductionFixture.Statement s : ProductionFixture.statements()) {
            SqlParseResult r = analyzer.analyze("x", s.getText(), ProductionFixture.NAMESPACE);
            for (ColumnLineageEdge e : r.getLineageEdges()) {
                expected.add(e.edgeId());
            }
        }
        assertEquals(expected, wh.lineage.keySet(), "the lineage table holds exactly the analysed edges");
        assertTrue(wh.lineage.values().stream().allMatch(l -> l.execCount == 1),
                "every edge was produced by exactly one of the 30 statements");
        assertTrue(wh.lineage.values().stream().allMatch(l -> l.lastQueryId.startsWith("prod-")));
        assertEquals(30, wh.members.size());

        BatchLogEntry log = wh.batchLog.get(report.getBatchId().get());
        assertEquals(30, log.getSucceeded());
        assertEquals(0, log.getFailed());
        assertEquals(expected.size(), log.getLineageRows(), "one day, one row per edge");
    }
}
