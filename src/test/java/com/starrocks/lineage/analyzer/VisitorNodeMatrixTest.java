package com.starrocks.lineage.analyzer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Iron rule #1: every node of the Stage 3 matrix is overridden with the exact name generated from the official
 * branch-3.1 grammar.
 */
class VisitorNodeMatrixTest {

    private static final Set<String> OVERRIDDEN = Arrays.stream(StarRocksAuditAstVisitor.class.getDeclaredMethods())
            .map(Method::getName).filter(n -> n.startsWith("visit")).collect(Collectors.toSet());

    @ParameterizedTest
    @ValueSource(strings = {
        "visitQueryStatement", "visitInsertStatement", "visitCreateTableAsSelectStatement",
        "visitCreateTemporaryTableStatement", "visitCreateViewStatement", "visitAlterViewStatement",
        "visitCreateMaterializedViewStatement", "visitSubmitTaskStatement", "visitUpdateStatement",
        "visitDeleteStatement", "visitQueryRelation", "visitWithClause", "visitCommonTableExpression",
        "visitQueryNoWith", "visitQueryPrimaryDefault", "visitQueryWithParentheses", "visitSetOperation",
        "visitQuerySpecification", "visitSelectSingle", "visitSelectAll", "visitFrom", "visitDual",
        "visitRelations", "visitRelation", "visitJoinRelation", "visitTableAtom", "visitSubqueryWithAlias",
        "visitInlineTable", "visitTableFunction", "visitNormalizedTableFunction", "visitFileTableFunction",
        "visitParenthesizedRelation", "visitSingleGroupingSet", "visitRollup", "visitCube",
        "visitMultipleGroupingSets", "visitColumnRef", "visitDereference", "visitLambdaFunctionExpr",
        "visitAggregationFunctionCall", "visitWindowFunctionCall", "visitSimpleFunctionCall", "visitOver",
        "visitExists", "visitSubqueryExpression", "visitScalarSubquery", "visitInSubquery", "visitTupleInSubquery",
        "visitSearchedCase", "visitSimpleCase", "visitCast", "visitPredicate", "visitLiteral"
    })
    void nodeIsOverridden(String method) {
        assertTrue(OVERRIDDEN.contains(method), method + " must be overridden by StarRocksAuditAstVisitor");
    }
}
