package com.starrocks.lineage.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageAndUsageModelTest {

    private static final ColumnIdentifier SRC_A = ColumnIdentifier.parse("default_catalog.ods.t1.a");
    private static final ColumnIdentifier TGT_X = ColumnIdentifier.parse("default_catalog.dw.t2.x");

    @Test
    void transformTypeDominance() {
        assertEquals(TransformType.EXPRESSION, TransformType.IDENTITY.combine(TransformType.EXPRESSION));
        assertEquals(TransformType.AGGREGATION, TransformType.AGGREGATION.combine(TransformType.EXPRESSION));
        assertEquals(TransformType.WINDOW, TransformType.AGGREGATION.combine(TransformType.WINDOW));
        assertEquals(TransformType.IDENTITY, TransformType.CONSTANT.combine(TransformType.IDENTITY));
        assertEquals(TransformType.IDENTITY, TransformType.IDENTITY.combine(null));
    }

    @Test
    void edgeIdentityIsTargetSourceLineageType() {
        ColumnLineageEdge direct = ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.DIRECT, TransformType.IDENTITY, "a");
        ColumnLineageEdge directAgg =
                ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.DIRECT, TransformType.AGGREGATION, "sum(a)");
        ColumnLineageEdge indirect =
                ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.INDIRECT, TransformType.IDENTITY, "a > 0");
        assertEquals(direct, directAgg);
        assertEquals(direct.edgeId(), directAgg.edgeId());
        assertNotEquals(direct, indirect);
        assertNotEquals(direct.edgeId(), indirect.edgeId());
        assertEquals(64, direct.edgeId().length());
        assertTrue(direct.edgeId().matches("[0-9a-f]{64}"));

        ColumnLineageEdge merged = direct.merge(directAgg);
        assertEquals(TransformType.AGGREGATION, merged.getTransformType());
        assertEquals("sum(a)", merged.getTransformExpr());
        assertThrows(IllegalArgumentException.class, () -> direct.merge(indirect));
    }

    @Test
    void edgeIdIsStableAcrossNameSpellings() {
        ColumnLineageEdge e1 = ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.DIRECT, TransformType.IDENTITY, null);
        ColumnLineageEdge e2 = ColumnLineageEdge.of(ColumnIdentifier.parse("DEFAULT_CATALOG.dw.t2.`X`"),
                ColumnIdentifier.of("Default_Catalog", "ods", "t1", "A"), LineageType.DIRECT,
                TransformType.EXPRESSION, null);
        assertEquals(e1.edgeId(), e2.edgeId());
    }

    @Test
    void constantEdgeInvariants() {
        ColumnLineageEdge c = ColumnLineageEdge.constant(TGT_X, "'N/A'");
        assertTrue(c.getSource().isConstant());
        assertEquals(TransformType.CONSTANT, c.getTransformType());
        assertThrows(IllegalArgumentException.class,
                () -> ColumnLineageEdge.of(TGT_X, ColumnIdentifier.CONSTANT, LineageType.DIRECT,
                        TransformType.IDENTITY, null));
        assertThrows(IllegalArgumentException.class,
                () -> ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.DIRECT, TransformType.CONSTANT, null));
        assertThrows(IllegalArgumentException.class,
                () -> ColumnLineageEdge.of(TGT_X, ColumnIdentifier.CONSTANT, LineageType.INDIRECT,
                        TransformType.CONSTANT, null));
        assertThrows(IllegalArgumentException.class,
                () -> ColumnLineageEdge.of(ColumnIdentifier.CONSTANT, SRC_A, LineageType.DIRECT,
                        TransformType.IDENTITY, null));
    }

    @Test
    void longExpressionsAreTruncated() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 3000; i++) {
            sb.append('x');
        }
        ColumnLineageEdge e = ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.DIRECT, TransformType.EXPRESSION,
                sb.toString());
        assertEquals(ColumnLineageEdge.MAX_EXPR_LENGTH, e.getTransformExpr().length());
        assertTrue(e.getTransformExpr().endsWith("..."));
    }

    /** Architect rule 7: SELECT a, a + 1 FROM t WHERE a > 10 counts a once per query and once per clause. */
    @Test
    void usageIsDeduplicatedPerQueryAndPerClause() {
        ColumnUsageStat a = new ColumnUsageStat(SRC_A)
                .record(ClauseType.SELECT)
                .record(ClauseType.SELECT)
                .record(ClauseType.WHERE);
        assertEquals(1, a.getAccessQueryCnt());
        assertEquals(1, a.getSelectClauseCnt());
        assertEquals(1, a.getWhereClauseCnt());
        assertEquals(0, a.getJoinClauseCnt());
        assertEquals(0, a.getGroupByCnt());
        assertEquals(0, a.getOrderByCnt());
        assertEquals(2, a.occurrences(ClauseType.SELECT));
    }

    @Test
    void clauseToMetricMapping() {
        ColumnUsageStat s = new ColumnUsageStat(SRC_A)
                .record(ClauseType.HAVING)
                .record(ClauseType.WINDOW_ORDER_BY)
                .record(ClauseType.JOIN_USING)
                .record(ClauseType.QUALIFY);
        assertEquals(1, s.getGroupByCnt(), "HAVING counts as group_by");
        assertEquals(1, s.getOrderByCnt(), "window ORDER BY counts as order_by");
        assertEquals(1, s.getJoinClauseCnt());
        assertEquals(1, s.getWhereClauseCnt(), "QUALIFY counts as where");
        assertEquals(0, s.getSelectClauseCnt());

        ColumnUsageStat other = new ColumnUsageStat(SRC_A).record(ClauseType.TABLE_FUNCTION);
        assertEquals(1, other.getAccessQueryCnt(), "an access outside counted clauses is still an access");
        assertEquals(0, other.getSelectClauseCnt() + other.getWhereClauseCnt() + other.getJoinClauseCnt()
                + other.getGroupByCnt() + other.getOrderByCnt());
        assertEquals(0, new ColumnUsageStat(SRC_A).getAccessQueryCnt());
        assertThrows(IllegalArgumentException.class, () -> new ColumnUsageStat(ColumnIdentifier.CONSTANT));
        assertThrows(IllegalArgumentException.class, () -> s.mergeFrom(new ColumnUsageStat(TGT_X)));
    }

    @Test
    void parseResultFoldsDuplicates() {
        SqlParseResult r = SqlParseResult.builder("q1")
                .statementType(StatementType.INSERT_INTO)
                .targetTable(TGT_X.getTable())
                .addEdge(ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.DIRECT, TransformType.IDENTITY, "a"))
                .addEdge(ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.DIRECT, TransformType.WINDOW, "w"))
                .addEdge(ColumnLineageEdge.of(TGT_X, SRC_A, LineageType.INDIRECT, TransformType.IDENTITY, "a>0"))
                .recordUsage(SRC_A, ClauseType.SELECT)
                .recordUsage(SRC_A, ClauseType.SELECT)
                .addSourceTable(SRC_A.getTable())
                .addSourceTable(SRC_A.getTable())
                .build();
        assertEquals(2, r.getLineageEdges().size());
        assertEquals(TransformType.WINDOW, r.getLineageEdges().get(0).getTransformType());
        assertEquals(1, r.getSourceTables().size());
        assertEquals(2, r.usageOf(SRC_A).orElseThrow().occurrences(ClauseType.SELECT));
        assertTrue(r.isSuccess());
        assertThrows(UnsupportedOperationException.class, () -> r.getLineageEdges().clear());
    }

    @Test
    void parseResultForSkippedRecord() {
        SqlParseResult r = SqlParseResult.of("q2", ParseStatus.SKIPPED_MANAGEMENT, "SHOW");
        assertFalse(r.isSuccess());
        assertTrue(r.getStatus().isSkipped());
        assertFalse(r.getStatementType().isPresent());
        assertThrows(IllegalArgumentException.class, () -> SqlParseResult.of("q", ParseStatus.SUCCESS, null));
        assertThrows(IllegalStateException.class, () -> SqlParseResult.builder("q").build());
    }

    @Test
    void auditRecordFailureFilter() {
        AuditLogRecord.Builder base = AuditLogRecord.builder().queryId("q").timestamp(LocalDateTime.now());
        assertFalse(base.state("EOF").errorCode("").build().isFailed());
        assertFalse(base.state("OK").errorCode("0").build().isFailed());
        assertFalse(base.state(null).errorCode(null).build().isFailed());
        assertTrue(base.state("err").errorCode("").build().isFailed());
        assertTrue(base.state("OK").errorCode("1064").build().isFailed());
        assertTrue(base.state("EOF").errorCode("ANALYSIS_ERR").build().isFailed());
        assertEquals("alice", base.user(" alice ").build().effectiveUser());
        assertEquals("'bob'@'%'", base.user("").authorizedUser("'bob'@'%'").build().effectiveUser());
        assertEquals(NamespaceContext.of("hive", "ods"), base.catalog("hive").db("ods").build().namespace());
    }
}
