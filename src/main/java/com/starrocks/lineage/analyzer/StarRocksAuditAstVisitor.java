package com.starrocks.lineage.analyzer;

import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.ClauseType;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.Identifiers;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.StatementType;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.scope.AliasPolicy;
import com.starrocks.lineage.scope.ExpressionSourceTracker;
import com.starrocks.lineage.scope.FieldLineage;
import com.starrocks.lineage.scope.RelationDescriptor;
import com.starrocks.lineage.scope.RelationField;
import com.starrocks.lineage.scope.RelationKind;
import com.starrocks.lineage.scope.ResolvedColumn;
import com.starrocks.lineage.scope.Scope;
import com.starrocks.lineage.scope.ScopeStack;
import com.starrocks.lineage.scope.SemanticAnalysisException;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;
import com.starrocks.lineage.scope.StarExpansion;
import com.starrocks.sql.parser.StarRocksBaseVisitor;
import com.starrocks.sql.parser.StarRocksParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Column lineage / column usage extractor over the parse tree of the official StarRocks branch-3.1 grammar.
 *
 * <p>One instance analyses <b>one</b> statement ({@link #analyze(StarRocksParser.StatementContext)}) on one thread.
 * The visitor drives the Stage 2 symbol table ({@link ScopeStack}) and keeps three explicit state stacks:</p>
 * <ul>
 *   <li><b>clause stack</b> ({@link ClauseType}): the clause the visitor is in; attributed to every physical
 *       column resolved while it is on top. Sub-queries push their own clauses, so a column of
 *       {@code WHERE EXISTS (SELECT c1 FROM t2 WHERE t2.id = t1.id)} is counted as SELECT ({@code c1}) and WHERE
 *       ({@code t2.id}, {@code t1.id}).</li>
 *   <li><b>tracker stack</b> ({@link ExpressionSourceTracker}): the expression whose lineage is being built;
 *       transform contexts ({@code EXPRESSION / AGGREGATION / WINDOW}) and indirect contexts (window
 *       {@code PARTITION BY / ORDER BY}, sub-query predicates) are opened by the visit methods.</li>
 *   <li><b>alias policy</b>: select-alias visibility of the clause being analysed.</li>
 * </ul>
 * <p>Relation producing nodes return their {@link RelationDescriptor} through a single-slot register
 * ({@link #relationOf(ParseTree)}), because generated visitors return {@code Void}.</p>
 *
 * <p>Lineage rules for write statements: for every aligned target column, each direct source gives a
 * {@code DIRECT} edge with its own {@link TransformType}; a target fed only by literals gives a {@code CONSTANT}
 * edge; value-specific indirect sources (window partition / order) and every row-control source of the query
 * (WHERE, JOIN ON / USING, GROUP BY, HAVING, QUALIFY, ORDER BY + LIMIT, semi-join predicates, sub-query predicates)
 * give {@code INDIRECT} edges.</p>
 */
public final class StarRocksAuditAstVisitor extends StarRocksBaseVisitor<Void> {

    /**
     * Function names parsed as {@code simpleFunctionCall} that are aggregate functions in StarRocks 3.1.
     * (AVG / COUNT / MAX / MIN / SUM / ARRAY_AGG / GROUP_CONCAT have dedicated grammar rules.)
     */
    static final Set<String> AGGREGATE_FUNCTIONS = new HashSet<>(Arrays.asList(
            "avg", "count", "sum", "min", "max", "ndv", "approx_count_distinct", "multi_distinct_count",
            "multi_distinct_sum", "hll_union_agg", "hll_union", "hll_raw_agg", "bitmap_union", "bitmap_union_count",
            "bitmap_union_int", "bitmap_intersect", "bitmap_agg", "intersect_count", "percentile_approx",
            "percentile_union", "percentile_cont", "percentile_disc", "stddev", "stddev_pop", "stddev_samp", "std",
            "variance", "var_pop", "var_samp", "variance_pop", "variance_samp", "covar_pop", "covar_samp", "corr",
            "group_concat", "array_agg", "array_agg_distinct", "array_unique_agg", "any_value", "max_by", "min_by",
            "retention", "window_funnel", "count_if", "bool_or", "bool_and", "boolor_agg", "booland_agg", "every",
            "histogram", "dict_merge", "approx_top_k", "map_agg", "sum_map", "exchange_bytes", "exchange_speed"));

    private static final String ALL_COLUMNS = "*";

    private final ScopeStack stack;
    private final SqlParseResult.Builder result;
    private final boolean strictColumnResolution;

    private final Deque<ClauseType> clauses = new ArrayDeque<>();
    private final Deque<ExpressionSourceTracker> trackers = new ArrayDeque<>();
    private AliasPolicy aliasPolicy = AliasPolicy.NONE;
    private boolean collectUsage;
    private RelationDescriptor relationRegister;
    private int joinChainStart;

    /**
     * @param stack                  symbol table of this statement (namespace + metadata)
     * @param result                 builder receiving edges, usages, warnings
     * @param strictColumnResolution {@code false}: a column missing from known metadata (stale metadata, the
     *                               statement did succeed in StarRocks) is attributed to the single plausible
     *                               physical table with a warning instead of failing the statement
     */
    public StarRocksAuditAstVisitor(ScopeStack stack, SqlParseResult.Builder result, boolean strictColumnResolution) {
        this.stack = Objects.requireNonNull(stack, "stack");
        this.result = Objects.requireNonNull(result, "result");
        this.strictColumnResolution = strictColumnResolution;
    }

    // =================================================================================================================
    // Entry point
    // =================================================================================================================

    /**
     * Analyses one statement.
     *
     * @return the statement type, or empty when the statement is out of scope (the builder is left untouched)
     * @throws SemanticAnalysisException                       on semantic failure
     * @throws com.starrocks.lineage.model.UnresolvedNameException when a table name cannot be completed
     */
    public Optional<StatementType> analyze(StarRocksParser.StatementContext statement) {
        ParseTree child = statement.getChildCount() == 0 ? null : statement.getChild(0);
        StatementType type;
        if (child instanceof StarRocksParser.QueryStatementContext) {
            StarRocksParser.QueryStatementContext q = (StarRocksParser.QueryStatementContext) child;
            if (q.explainDesc() != null || q.optimizerTrace() != null) {
                return Optional.empty();
            }
            type = StatementType.QUERY;
        } else if (child instanceof StarRocksParser.InsertStatementContext) {
            StarRocksParser.InsertStatementContext i = (StarRocksParser.InsertStatementContext) child;
            if (i.explainDesc() != null) {
                return Optional.empty();
            }
            type = i.OVERWRITE() != null ? StatementType.INSERT_OVERWRITE : StatementType.INSERT_INTO;
        } else if (child instanceof StarRocksParser.CreateTableAsSelectStatementContext
                || child instanceof StarRocksParser.CreateTemporaryTableStatementContext) {
            type = StatementType.CREATE_TABLE_AS_SELECT;
        } else if (child instanceof StarRocksParser.CreateViewStatementContext) {
            type = StatementType.CREATE_VIEW;
        } else if (child instanceof StarRocksParser.AlterViewStatementContext) {
            if (((StarRocksParser.AlterViewStatementContext) child).queryStatement() == null) {
                return Optional.empty();
            }
            type = StatementType.ALTER_VIEW;
        } else if (child instanceof StarRocksParser.CreateMaterializedViewStatementContext) {
            type = StatementType.CREATE_MATERIALIZED_VIEW;
        } else if (child instanceof StarRocksParser.UpdateStatementContext) {
            if (((StarRocksParser.UpdateStatementContext) child).explainDesc() != null) {
                return Optional.empty();
            }
            type = StatementType.UPDATE;
        } else if (child instanceof StarRocksParser.DeleteStatementContext) {
            if (((StarRocksParser.DeleteStatementContext) child).explainDesc() != null) {
                return Optional.empty();
            }
            type = StatementType.DELETE;
        } else if (child instanceof StarRocksParser.SubmitTaskStatementContext) {
            type = StatementType.SUBMIT_TASK;
        } else {
            return Optional.empty();
        }
        collectUsage = type == StatementType.QUERY;
        result.statementType(type);
        visit(child);
        stack.assertEmpty();
        for (TableIdentifier t : stack.getReferencedTables()) {
            result.addSourceTable(t);
        }
        return Optional.of(type);
    }

    // =================================================================================================================
    // Statements
    // =================================================================================================================

    @Override
    public Void visitQueryStatement(StarRocksParser.QueryStatementContext ctx) {
        analyzeQueryRelation(ctx.queryRelation());
        return null;
    }

    @Override
    public Void visitInsertStatement(StarRocksParser.InsertStatementContext ctx) {
        RelationDescriptor target = stack.resolvePhysicalRelation(qualifiedNameParts(ctx.qualifiedName()), null);
        TableIdentifier targetTable = target.getTable().orElseThrow(IllegalStateException::new);
        result.targetTable(targetTable);
        List<String> targetColumns;
        if (ctx.columnAliases() != null) {
            targetColumns = identifiers(ctx.columnAliases().identifier());
        } else if (target.isSchemaKnown()) {
            targetColumns = names(target.getFields());
        } else {
            targetColumns = null;
            result.addWarning("target table " + targetTable + " has no metadata: INSERT without column list is "
                    + "aligned by the query output names");
        }
        RelationDescriptor source;
        if (ctx.queryStatement() != null) {
            source = analyzeQueryRelation(ctx.queryStatement().queryRelation());
        } else {
            source = analyzeValuesRows(ctx.expressionsWithDefault());
        }
        emitLineage(targetTable, targetColumns == null ? names(source.getFields()) : targetColumns, source);
        return null;
    }

    @Override
    public Void visitCreateTableAsSelectStatement(StarRocksParser.CreateTableAsSelectStatementContext ctx) {
        TableIdentifier targetTable = targetTable(ctx.qualifiedName());
        RelationDescriptor source = analyzeQueryRelation(ctx.queryStatement().queryRelation());
        List<String> columns = ctx.identifier().isEmpty() ? createdColumnNames(targetTable, source)
                : identifiers(ctx.identifier());
        emitLineage(targetTable, columns, source);
        return null;
    }

    @Override
    public Void visitCreateTemporaryTableStatement(StarRocksParser.CreateTemporaryTableStatementContext ctx) {
        TableIdentifier targetTable = targetTable(ctx.qualifiedName());
        RelationDescriptor source = analyzeQueryRelation(ctx.queryStatement().queryRelation());
        emitLineage(targetTable, createdColumnNames(targetTable, source), source);
        return null;
    }

    /**
     * Column names of an object created from a query without a column list (CTAS, view, materialized view).
     *
     * <p>StarRocks names an unaliased expression column after its <i>normalised</i> SQL ({@code row_number() over
     * (order by amount asc )}, {@code case when (amount > 1) then 1 end}), not after the source text (verified on
     * 3.1.17 by {@code AnalyzerDifferentialIT}). The audited statement succeeded, so the object exists: its real
     * column names are taken positionally from the metadata (the cached entry is refreshed first, the object is
     * new). Only when the metadata is unavailable or does not match the query arity are the derived names used,
     * with a warning.</p>
     */
    private List<String> createdColumnNames(TableIdentifier target, RelationDescriptor source) {
        stack.getMetadata().invalidate(target);
        Optional<TableSchema> schema = stack.getMetadata().getTableSchema(target);
        if (schema.isPresent() && schema.get().size() == source.size() && source.isPositionallyComplete()) {
            return schema.get().getColumnNames();
        }
        List<String> derived = names(source.getFields());
        boolean needsEngineName = false;
        for (RelationField f : source.getFields()) {
            String expr = f.getExpression();
            // an unaliased complex expression is named after its text, which StarRocks normalises differently
            if (expr != null && f.getName().equals(Identifiers.normalizeColumn(expr))
                    && !expr.matches("[A-Za-z0-9_`.]+")) {
                needsEngineName = true;
                break;
            }
        }
        if (needsEngineName || schema.isPresent()) {
            result.addWarning("column names of " + target + " derived from the query text"
                    + (schema.isPresent() ? " (metadata has " + schema.get().size() + " columns, query "
                    + source.size() + ")" : " (no metadata yet)") + ": unaliased expressions may be named "
                    + "differently by StarRocks");
        }
        return derived;
    }

    @Override
    public Void visitCreateViewStatement(StarRocksParser.CreateViewStatementContext ctx) {
        analyzeViewLike(ctx.qualifiedName(), ctx.columnNameWithComment(), ctx.queryStatement());
        return null;
    }

    @Override
    public Void visitAlterViewStatement(StarRocksParser.AlterViewStatementContext ctx) {
        analyzeViewLike(ctx.qualifiedName(), ctx.columnNameWithComment(), ctx.queryStatement());
        return null;
    }

    @Override
    public Void visitCreateMaterializedViewStatement(StarRocksParser.CreateMaterializedViewStatementContext ctx) {
        analyzeViewLike(ctx.mvName, ctx.columnNameWithComment(), ctx.queryStatement());
        return null;
    }

    private void analyzeViewLike(StarRocksParser.QualifiedNameContext name,
                                 List<StarRocksParser.ColumnNameWithCommentContext> columnList,
                                 StarRocksParser.QueryStatementContext query) {
        TableIdentifier targetTable = targetTable(name);
        RelationDescriptor source = analyzeQueryRelation(query.queryRelation());
        List<String> columns = new ArrayList<>();
        for (StarRocksParser.ColumnNameWithCommentContext c : columnList) {
            columns.add(identifierText(c.columnName));
        }
        emitLineage(targetTable, columns.isEmpty() ? createdColumnNames(targetTable, source) : columns, source);
    }

    @Override
    public Void visitSubmitTaskStatement(StarRocksParser.SubmitTaskStatementContext ctx) {
        if (ctx.createTableAsSelectStatement() != null) {
            visit(ctx.createTableAsSelectStatement());
        } else {
            visit(ctx.insertStatement());
        }
        return null;
    }

    /**
     * {@code [WITH ...] UPDATE t SET c = expr [, ...] [FROM relations] [WHERE cond]}: every assigned column receives
     * the DIRECT lineage of its expression and INDIRECT edges from the row-control of the block (WHERE, joins of the
     * FROM clause).
     */
    @Override
    public Void visitUpdateStatement(StarRocksParser.UpdateStatementContext ctx) {
        try (ScopeStack.Frame with = ctx.withClause() == null ? null : stack.pushCte()) {
            if (ctx.withClause() != null) {
                analyzeWithClause(ctx.withClause());
            }
            try (ScopeStack.Frame block = stack.pushQueryBlock()) {
                RelationDescriptor target = stack.resolvePhysicalRelation(qualifiedNameParts(ctx.qualifiedName()),
                        null);
                TableIdentifier targetTable = target.getTable().orElseThrow(IllegalStateException::new);
                result.targetTable(targetTable);
                block.scope().addRelation(target);
                visit(ctx.fromClause());
                if (ctx.where != null) {
                    addControl(analyzeExpression(ctx.where, ClauseType.WHERE, AliasPolicy.NONE));
                }
                List<String> columns = new ArrayList<>();
                List<RelationField> values = new ArrayList<>();
                for (StarRocksParser.AssignmentContext a : ctx.assignmentList().assignment()) {
                    String column = identifierText(a.identifier());
                    FieldLineage lineage = a.expressionOrDefault().expression() == null ? FieldLineage.constant()
                            : analyzeExpression(a.expressionOrDefault().expression(), ClauseType.UPDATE_SET,
                            AliasPolicy.NONE).build();
                    columns.add(column);
                    values.add(new RelationField(column, values.size(), lineage, text(a.expressionOrDefault())));
                }
                RelationDescriptor assigned = RelationDescriptor.derived(RelationKind.QUERY_OUTPUT, null, values,
                        block.scope().getControlSources());
                emitLineage(targetTable, columns, assigned);
            }
        }
        return null;
    }

    /**
     * {@code [WITH ...] DELETE FROM t [USING relations] [WHERE cond]}: no value flows, but every column of {@code t}
     * is affected by the predicate; the control sources give INDIRECT edges to every target column ({@code *} when
     * the target has no metadata).
     */
    @Override
    public Void visitDeleteStatement(StarRocksParser.DeleteStatementContext ctx) {
        try (ScopeStack.Frame with = ctx.withClause() == null ? null : stack.pushCte()) {
            if (ctx.withClause() != null) {
                analyzeWithClause(ctx.withClause());
            }
            try (ScopeStack.Frame block = stack.pushQueryBlock()) {
                RelationDescriptor target = stack.resolvePhysicalRelation(qualifiedNameParts(ctx.qualifiedName()),
                        null);
                TableIdentifier targetTable = target.getTable().orElseThrow(IllegalStateException::new);
                result.targetTable(targetTable);
                block.scope().addRelation(target);
                if (ctx.using != null) {
                    visit(ctx.using);
                }
                if (ctx.where != null) {
                    addControl(analyzeExpression(ctx.where, ClauseType.WHERE, AliasPolicy.NONE));
                }
                List<String> columns = target.isSchemaKnown() ? names(target.getFields())
                        : Collections.singletonList(ALL_COLUMNS);
                if (!target.isSchemaKnown()) {
                    result.addWarning("target table " + targetTable + " has no metadata: DELETE lineage targets '*'");
                }
                Set<ColumnIdentifier> control = block.scope().getControlSources();
                for (String c : columns) {
                    ColumnIdentifier tc = ColumnIdentifier.of(targetTable, c);
                    for (ColumnIdentifier src : control) {
                        result.addEdge(ColumnLineageEdge.of(tc, src, LineageType.INDIRECT, TransformType.IDENTITY,
                                ctx.where == null ? null : text(ctx.where)));
                    }
                }
            }
        }
        return null;
    }

    // =================================================================================================================
    // Query expressions
    // =================================================================================================================

    /**
     * {@code queryRelation : withClause? queryNoWith}. The WITH scope stays open while the body is analysed; the
     * returned relation is self-contained (physical lineage), so it outlives the scope.
     */
    RelationDescriptor analyzeQueryRelation(StarRocksParser.QueryRelationContext ctx) {
        if (ctx.withClause() == null) {
            return analyzeQueryNoWith(ctx.queryNoWith());
        }
        try (ScopeStack.Frame with = stack.pushCte()) {
            analyzeWithClause(ctx.withClause());
            return analyzeQueryNoWith(ctx.queryNoWith());
        }
    }

    @Override
    public Void visitQueryRelation(StarRocksParser.QueryRelationContext ctx) {
        relationRegister = analyzeQueryRelation(ctx);
        return null;
    }

    private void analyzeWithClause(StarRocksParser.WithClauseContext ctx) {
        for (StarRocksParser.CommonTableExpressionContext cte : ctx.commonTableExpression()) {
            visit(cte);
        }
    }

    /**
     * {@code name [(c1, ...)] AS (query)}: the body is analysed first, then registered, so the CTE is visible to
     * the following CTEs and the main query but not to itself.
     */
    @Override
    public Void visitCommonTableExpression(StarRocksParser.CommonTableExpressionContext ctx) {
        RelationDescriptor body = analyzeQueryRelation(ctx.queryRelation());
        List<String> aliases = ctx.columnAliases() == null ? Collections.emptyList()
                : identifiers(ctx.columnAliases().identifier());
        stack.registerCte(identifierText(ctx.name), aliases, body);
        return null;
    }

    @Override
    public Void visitWithClause(StarRocksParser.WithClauseContext ctx) {
        analyzeWithClause(ctx);
        return null;
    }

    /**
     * {@code queryPrimary (ORDER BY sortItem, ...)? limitElement?}. A trailing ORDER BY of a plain query
     * specification belongs to that block (select aliases and input columns are visible); after a set operation or
     * a parenthesised query it is resolved against the output. ORDER BY columns control which rows survive only
     * when a LIMIT is present; in that case they become row-control sources.
     */
    private RelationDescriptor analyzeQueryNoWith(StarRocksParser.QueryNoWithContext ctx) {
        List<StarRocksParser.SortItemContext> sort = ctx.sortItem();
        boolean limited = ctx.limitElement() != null;
        StarRocksParser.QueryPrimaryContext primary = ctx.queryPrimary();
        if (primary instanceof StarRocksParser.QueryPrimaryDefaultContext) {
            return analyzeQuerySpecification(((StarRocksParser.QueryPrimaryDefaultContext) primary)
                    .querySpecification(), sort, limited);
        }
        RelationDescriptor rel = relationOf(primary);
        if (sort.isEmpty()) {
            return rel;
        }
        Set<ColumnIdentifier> sortSources = new LinkedHashSet<>();
        try (ScopeStack.Frame out = stack.pushOutput(rel)) {
            for (StarRocksParser.SortItemContext item : sort) {
                Optional<Integer> ordinal = ordinal(item.expression());
                FieldLineage l;
                if (ordinal.isPresent()) {
                    if (ordinal.get() < 1 || ordinal.get() > rel.getReliablePrefix()) {
                        throw new SemanticAnalysisException(ErrorCode.ORDINAL_OUT_OF_RANGE,
                                "ORDER BY " + ordinal.get() + " outside the select list");
                    }
                    l = rel.field(ordinal.get() - 1).getLineage();
                    recordUsage(l, ClauseType.ORDER_BY);
                } else {
                    l = analyzeExpression(item.expression(), ClauseType.ORDER_BY, AliasPolicy.NONE).build();
                }
                sortSources.addAll(l.allSources());
            }
        }
        return limited ? rel.withControlSources(sortSources) : rel;
    }

    @Override
    public Void visitQueryNoWith(StarRocksParser.QueryNoWithContext ctx) {
        relationRegister = analyzeQueryNoWith(ctx);
        return null;
    }

    @Override
    public Void visitQueryPrimaryDefault(StarRocksParser.QueryPrimaryDefaultContext ctx) {
        relationRegister = analyzeQuerySpecification(ctx.querySpecification(), Collections.emptyList(), false);
        return null;
    }

    @Override
    public Void visitQueryWithParentheses(StarRocksParser.QueryWithParenthesesContext ctx) {
        relationRegister = analyzeQueryRelation(ctx.subquery().queryRelation());
        return null;
    }

    /**
     * {@code left (UNION | INTERSECT | EXCEPT | MINUS) [ALL | DISTINCT] right}: positional merge (iron rule #4).
     */
    @Override
    public Void visitSetOperation(StarRocksParser.SetOperationContext ctx) {
        RelationDescriptor left = relationOf(ctx.left);
        RelationDescriptor right = relationOf(ctx.right);
        RelationDescriptor merged = RelationDescriptor.setOperation(Arrays.asList(left, right));
        if (!left.isPositionallyComplete() || !right.isPositionallyComplete()) {
            result.addWarning("set operation over a branch whose column count is unknown (unexpanded *)");
        }
        relationRegister = merged;
        return null;
    }

    @Override
    public Void visitQuerySpecification(StarRocksParser.QuerySpecificationContext ctx) {
        relationRegister = analyzeQuerySpecification(ctx, Collections.emptyList(), false);
        return null;
    }

    /**
     * One query block. Visiting order: FROM, WHERE, SELECT (publishes aliases), GROUP BY, HAVING, QUALIFY,
     * ORDER BY; this is the order required by Stage 2 (aliases must exist before GROUP BY / HAVING / ORDER BY).
     */
    private RelationDescriptor analyzeQuerySpecification(StarRocksParser.QuerySpecificationContext ctx,
                                                         List<StarRocksParser.SortItemContext> orderBy,
                                                         boolean limited) {
        try (ScopeStack.Frame block = stack.pushQueryBlock()) {
            Scope scope = block.scope();
            visit(ctx.fromClause());
            if (ctx.where != null) {
                addControl(analyzeExpression(ctx.where, ClauseType.WHERE, AliasPolicy.NONE));
            }
            List<StarRocksParser.SelectItemContext> selectItems = new ArrayList<>(ctx.selectItem());
            // QUALIFY's operand is itself a selectItem child of querySpecification: it is not a projection
            selectItems.remove(ctx.qualifyFunction);
            analyzeSelectList(selectItems, scope);
            if (ctx.groupingElement() != null) {
                visit(ctx.groupingElement());
            }
            if (ctx.having != null) {
                addControl(analyzeExpression(ctx.having, ClauseType.HAVING, AliasPolicy.PREFER_INPUT));
            }
            if (ctx.qualifyFunction != null) {
                StarRocksParser.SelectItemContext q = ctx.qualifyFunction;
                if (q instanceof StarRocksParser.SelectSingleContext) {
                    addControl(analyzeExpression(((StarRocksParser.SelectSingleContext) q).expression(),
                            ClauseType.QUALIFY, AliasPolicy.PREFER_INPUT));
                }
            }
            Set<ColumnIdentifier> sortSources = new LinkedHashSet<>();
            for (StarRocksParser.SortItemContext item : orderBy) {
                Optional<Integer> ordinal = ordinal(item.expression());
                FieldLineage l;
                if (ordinal.isPresent()) {
                    l = scope.selectOutputByOrdinal(ordinal.get()).getLineage();
                    recordUsage(l, ClauseType.ORDER_BY);
                } else {
                    l = analyzeExpression(item.expression(), ClauseType.ORDER_BY, AliasPolicy.PREFER_ALIAS).build();
                }
                sortSources.addAll(l.allSources());
            }
            if (limited) {
                scope.addControlSources(sortSources);
            }
            return scope.toDerivedRelation(RelationKind.SUBQUERY, null);
        }
    }

    private void analyzeSelectList(List<StarRocksParser.SelectItemContext> items, Scope scope) {
        List<RelationField> outputs = new ArrayList<>();
        int reliablePrefix = -1;
        Set<String> unexpanded = new LinkedHashSet<>();
        for (StarRocksParser.SelectItemContext item : items) {
            if (item instanceof StarRocksParser.SelectSingleContext) {
                outputs.add(analyzeSelectSingle((StarRocksParser.SelectSingleContext) item, outputs.size()));
            } else {
                StarRocksParser.SelectAllContext all = (StarRocksParser.SelectAllContext) item;
                List<String> qualifier = all.qualifiedName() == null ? Collections.emptyList()
                        : qualifiedNameParts(all.qualifiedName());
                StarExpansion expansion = stack.expandStar(qualifier);
                int before = outputs.size();
                for (ResolvedColumn rc : expansion.getColumns()) {
                    recordUsage(rc, ClauseType.SELECT);
                    outputs.add(new RelationField(rc.getField().getName(), outputs.size(), rc.lineage(),
                            rc.getField().getName()));
                }
                if (!expansion.isComplete()) {
                    if (reliablePrefix < 0) {
                        reliablePrefix = before + expansion.getReliablePrefix();
                    }
                    unexpanded.addAll(expansion.getUnexpandedTables());
                    result.addWarning("SELECT " + (qualifier.isEmpty() ? "" : String.join(".", qualifier) + ".")
                            + "* over relation(s) without metadata: " + expansion.getUnexpandedTables());
                }
            }
        }
        scope.setSelectOutputs(outputs, reliablePrefix < 0 ? outputs.size() : reliablePrefix, unexpanded);
    }

    private RelationField analyzeSelectSingle(StarRocksParser.SelectSingleContext ctx, int index) {
        ExpressionSourceTracker t = analyzeExpression(ctx.expression(), ClauseType.SELECT, AliasPolicy.NONE);
        String name;
        if (ctx.identifier() != null) {
            name = identifierText(ctx.identifier());
        } else if (ctx.string() != null) {
            name = stringText(ctx.string());
        } else {
            name = implicitName(ctx.expression());
        }
        return new RelationField(name, index, t.build(), text(ctx.expression()));
    }

    @Override
    public Void visitSelectSingle(StarRocksParser.SelectSingleContext ctx) {
        throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE, "select items are analysed by the query block");
    }

    @Override
    public Void visitSelectAll(StarRocksParser.SelectAllContext ctx) {
        throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE, "select items are analysed by the query block");
    }

    // ------------------------------------------------------------------------------------------------ GROUP BY

    @Override
    public Void visitSingleGroupingSet(StarRocksParser.SingleGroupingSetContext ctx) {
        analyzeGroupingExpressions(ctx.expressionList().expression());
        return null;
    }

    @Override
    public Void visitRollup(StarRocksParser.RollupContext ctx) {
        if (ctx.expressionList() != null) {
            analyzeGroupingExpressions(ctx.expressionList().expression());
        }
        return null;
    }

    @Override
    public Void visitCube(StarRocksParser.CubeContext ctx) {
        if (ctx.expressionList() != null) {
            analyzeGroupingExpressions(ctx.expressionList().expression());
        }
        return null;
    }

    @Override
    public Void visitMultipleGroupingSets(StarRocksParser.MultipleGroupingSetsContext ctx) {
        for (StarRocksParser.GroupingSetContext set : ctx.groupingSet()) {
            analyzeGroupingExpressions(set.expression());
        }
        return null;
    }

    private void analyzeGroupingExpressions(List<StarRocksParser.ExpressionContext> expressions) {
        Scope scope = stack.currentQueryBlock();
        for (StarRocksParser.ExpressionContext e : expressions) {
            Optional<Integer> ordinal = ordinal(e);
            if (ordinal.isPresent()) {
                FieldLineage l = scope.selectOutputByOrdinal(ordinal.get()).getLineage();
                recordUsage(l, ClauseType.GROUP_BY);
                scope.addControlLineage(l);
            } else {
                addControl(analyzeExpression(e, ClauseType.GROUP_BY, AliasPolicy.PREFER_INPUT));
            }
        }
    }

    // =================================================================================================================
    // FROM clause
    // =================================================================================================================

    @Override
    public Void visitFrom(StarRocksParser.FromContext ctx) {
        if (ctx.relations() != null) {
            visit(ctx.relations());
        }
        return null;
    }

    @Override
    public Void visitDual(StarRocksParser.DualContext ctx) {
        return null;
    }

    /** {@code relation (',' LATERAL? relation)*}: comma joins; LATERAL needs nothing special (FROM order). */
    @Override
    public Void visitRelations(StarRocksParser.RelationsContext ctx) {
        for (StarRocksParser.RelationContext r : ctx.relation()) {
            visit(r);
        }
        return null;
    }

    /** {@code relationPrimary joinRelation*}: one join chain; its start index identifies the left side. */
    @Override
    public Void visitRelation(StarRocksParser.RelationContext ctx) {
        int savedChainStart = joinChainStart;
        joinChainStart = stack.currentQueryBlock().getAllRelations().size();
        try {
            visit(ctx.relationPrimary());
            for (StarRocksParser.JoinRelationContext join : ctx.joinRelation()) {
                visit(join);
            }
        } finally {
            joinChainStart = savedChainStart;
        }
        return null;
    }

    /**
     * {@code [INNER|CROSS|LEFT|RIGHT|FULL [OUTER]|LEFT/RIGHT SEMI|ANTI] JOIN [LATERAL] right [ON cond | USING (c)]}.
     * The criteria are analysed while both sides are visible; afterwards a SEMI / ANTI join hides the side that is
     * not part of its output (StarRocks {@code QueryAnalyzer#visitJoin}).
     */
    @Override
    public Void visitJoinRelation(StarRocksParser.JoinRelationContext ctx) {
        Scope scope = stack.currentQueryBlock();
        List<RelationDescriptor> left = new ArrayList<>(scope.getAllRelations().subList(joinChainStart,
                scope.getAllRelations().size()));
        int before = scope.getAllRelations().size();
        visit(ctx.rightRelation);
        List<RelationDescriptor> right = new ArrayList<>(scope.getAllRelations().subList(before,
                scope.getAllRelations().size()));
        if (ctx.joinCriteria() != null) {
            StarRocksParser.JoinCriteriaContext criteria = ctx.joinCriteria();
            if (criteria.ON() != null) {
                addControl(analyzeExpression(criteria.expression(), ClauseType.JOIN_ON, AliasPolicy.NONE));
            } else {
                List<String> usingColumns = identifiers(criteria.identifier());
                for (String column : usingColumns) {
                    usingSide(left, column);
                    usingSide(right, column);
                }
                scope.addUsingColumns(usingColumns);
            }
        }
        String joinType = ctx.outerAndSemiJoinType() == null ? ""
                : ctx.outerAndSemiJoinType().getText().toUpperCase(Locale.ROOT);
        if (joinType.contains("SEMI") || joinType.contains("ANTI")) {
            scope.hideRelations(joinType.startsWith("LEFT") ? right : left);
        }
        return null;
    }

    private void usingSide(List<RelationDescriptor> side, String column) {
        for (RelationDescriptor r : side) {
            if (stack.currentQueryBlock().getVisibleRelations().contains(r)) {
                List<RelationField> fields = r.lookup(column);
                Optional<RelationField> f = fields.isEmpty() ? r.onDemandField(column) : Optional.of(fields.get(0));
                if (f.isPresent()) {
                    FieldLineage l = f.get().getLineage();
                    recordUsage(l, ClauseType.JOIN_USING);
                    stack.currentQueryBlock().addControlLineage(l);
                    return;
                }
            }
        }
        throw new SemanticAnalysisException(ErrorCode.COLUMN_NOT_FOUND, "USING column '" + column
                + "' not found on one side of the join");
    }

    @Override
    public Void visitTableAtom(StarRocksParser.TableAtomContext ctx) {
        RelationDescriptor r = stack.resolveRelation(qualifiedNameParts(ctx.qualifiedName()), aliasText(ctx.alias));
        if (!r.isSchemaKnown()) {
            result.addWarning("no metadata for table "
                    + r.getTable().map(TableIdentifier::toQualifiedString).orElse(r.displayName()));
        }
        stack.currentQueryBlock().addRelation(r);
        return null;
    }

    @Override
    public Void visitSubqueryWithAlias(StarRocksParser.SubqueryWithAliasContext ctx) {
        RelationDescriptor sub = analyzeQueryRelation(ctx.subquery().queryRelation()).withAlias(aliasText(ctx.alias));
        if (ctx.columnAliases() != null) {
            sub = sub.withColumnAliases(identifiers(ctx.columnAliases().identifier()));
        }
        stack.currentQueryBlock().addRelation(sub);
        return null;
    }

    /** {@code (VALUES (..), (..)) [AS] t (c1, ..)}: column i lineage = union of row expressions at position i. */
    @Override
    public Void visitInlineTable(StarRocksParser.InlineTableContext ctx) {
        List<FieldLineage> columns = new ArrayList<>();
        for (StarRocksParser.RowConstructorContext row : ctx.rowConstructor()) {
            List<StarRocksParser.ExpressionContext> values = row.expressionList().expression();
            for (int i = 0; i < values.size(); i++) {
                FieldLineage l = analyzeExpression(values.get(i), ClauseType.SELECT, AliasPolicy.NONE).build();
                if (i < columns.size()) {
                    columns.set(i, columns.get(i).union(l));
                } else {
                    columns.add(l);
                }
            }
        }
        List<String> aliases = ctx.columnAliases() == null ? null : identifiers(ctx.columnAliases().identifier());
        stack.currentQueryBlock().addRelation(RelationDescriptor.values(aliasText(ctx.alias), aliases, columns));
        return null;
    }

    /** {@code fn(args) [AS] t (c1, ..)} - includes {@code UNNEST(arr1, arr2) AS t(c1, c2)} (iron rule #5). */
    @Override
    public Void visitTableFunction(StarRocksParser.TableFunctionContext ctx) {
        addTableFunction(ctx.qualifiedName(), ctx.expressionList(), ctx.alias, ctx.columnAliases());
        return null;
    }

    /** {@code TABLE(fn(args)) [AS] t (c1, ..)}. */
    @Override
    public Void visitNormalizedTableFunction(StarRocksParser.NormalizedTableFunctionContext ctx) {
        addTableFunction(ctx.qualifiedName(), ctx.expressionList(), ctx.alias, ctx.columnAliases());
        return null;
    }

    private void addTableFunction(StarRocksParser.QualifiedNameContext name, StarRocksParser.ExpressionListContext args,
                                  StarRocksParser.IdentifierContext alias,
                                  StarRocksParser.ColumnAliasesContext columnAliases) {
        List<String> nameParts = qualifiedNameParts(name);
        List<FieldLineage> argLineages = new ArrayList<>();
        for (StarRocksParser.ExpressionContext e : args.expression()) {
            argLineages.add(analyzeExpression(e, ClauseType.TABLE_FUNCTION, AliasPolicy.NONE).build());
        }
        RelationDescriptor r = RelationDescriptor.tableFunction(nameParts.get(nameParts.size() - 1),
                aliasText(alias), columnAliases == null ? null : identifiers(columnAliases.identifier()), argLineages);
        if (!r.isSchemaKnown()) {
            result.addWarning("table function " + r.displayName() + " without column aliases: output columns unknown");
        }
        stack.currentQueryBlock().addRelation(r);
    }

    /** {@code FILES(...)}: external files, no catalog lineage; output columns are unknown unless aliased. */
    @Override
    public Void visitFileTableFunction(StarRocksParser.FileTableFunctionContext ctx) {
        RelationDescriptor r = RelationDescriptor.tableFunction("files", aliasText(ctx.alias),
                ctx.columnAliases() == null ? null : identifiers(ctx.columnAliases().identifier()),
                Collections.emptyList());
        stack.currentQueryBlock().addRelation(r);
        return null;
    }

    @Override
    public Void visitParenthesizedRelation(StarRocksParser.ParenthesizedRelationContext ctx) {
        int saved = joinChainStart;
        try {
            visit(ctx.relations());
        } finally {
            joinChainStart = saved;
        }
        return null;
    }

    // =================================================================================================================
    // Expressions: column references
    // =================================================================================================================

    @Override
    public Void visitColumnRef(StarRocksParser.ColumnRefContext ctx) {
        reference(Collections.singletonList(identifierText(ctx.columnReference().identifier())), ctx);
        return null;
    }

    /**
     * {@code base.field}: a chain of dereferences ending in a column reference is a (qualified / struct) name and is
     * resolved as one; any other base ({@code f(x).a}, {@code arr[1].a}) is an expression.
     */
    @Override
    public Void visitDereference(StarRocksParser.DereferenceContext ctx) {
        List<String> parts = dereferenceParts(ctx);
        if (parts != null) {
            reference(parts, ctx);
            return null;
        }
        try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.EXPRESSION)) {
            visit(ctx.base);
        }
        return null;
    }

    private static List<String> dereferenceParts(StarRocksParser.DereferenceContext ctx) {
        Deque<String> parts = new ArrayDeque<>();
        ParserRuleContext node = ctx;
        while (node instanceof StarRocksParser.DereferenceContext) {
            StarRocksParser.DereferenceContext d = (StarRocksParser.DereferenceContext) node;
            if (d.fieldName != null) {
                parts.addFirst(identifierText(d.fieldName));
            } else {
                parts.addFirst(d.DOT_IDENTIFIER().getText().substring(1));
            }
            node = d.base;
        }
        if (node instanceof StarRocksParser.ColumnRefContext) {
            parts.addFirst(identifierText(((StarRocksParser.ColumnRefContext) node).columnReference().identifier()));
            return new ArrayList<>(parts);
        }
        return null;
    }

    private void reference(List<String> parts, ParserRuleContext at) {
        ResolvedColumn rc = resolve(parts, at);
        tracker().addReference(rc);
        if (rc.getKind() != ResolvedColumn.Kind.LAMBDA_PARAMETER) {
            recordUsage(rc, currentClause());
        }
        if (rc.isOnDemand()) {
            result.addWarning("column '" + String.join(".", parts) + "' resolved on a relation without metadata");
        }
    }

    private ResolvedColumn resolve(List<String> parts, ParserRuleContext at) {
        try {
            return stack.resolveColumn(parts, aliasPolicy);
        } catch (SemanticAnalysisException e) {
            if (strictColumnResolution || e.getErrorCode() != ErrorCode.COLUMN_NOT_FOUND) {
                throw e;
            }
            return lenientResolve(parts, e);
        }
    }

    /**
     * Stale metadata fallback: the statement succeeded in StarRocks, so a column missing from metadata existed at
     * execution time. It is attributed to the single physical table it can belong to (by qualifier, or the only
     * physical table of the innermost block), with a warning.
     */
    private ResolvedColumn lenientResolve(List<String> parts, SemanticAnalysisException original) {
        Scope scope = stack.currentQueryBlock();
        List<RelationDescriptor> candidates = new ArrayList<>();
        String column;
        if (parts.size() >= 2) {
            List<String> qualifier = parts.subList(0, parts.size() - 1);
            for (RelationDescriptor r : scope.getVisibleRelations()) {
                if (r.getKind() == RelationKind.TABLE && r.matchesQualifier(qualifier)) {
                    candidates.add(r);
                }
            }
            column = parts.get(parts.size() - 1);
        } else {
            for (RelationDescriptor r : scope.getVisibleRelations()) {
                if (r.getKind() == RelationKind.TABLE) {
                    candidates.add(r);
                }
            }
            column = parts.get(0);
        }
        if (candidates.size() != 1) {
            throw original;
        }
        TableIdentifier table = candidates.get(0).getTable().orElseThrow(IllegalStateException::new);
        ColumnIdentifier physical = ColumnIdentifier.of(table, column);
        result.addWarning("column " + physical + " is missing from metadata (stale?), attributed by lenient mode");
        RelationDescriptor synthetic = RelationDescriptor.derived(RelationKind.QUERY_OUTPUT, null,
                Collections.singletonList(new RelationField(column, 0, FieldLineage.physical(physical), column)),
                Collections.emptySet());
        try (ScopeStack.Frame f = stack.pushOutput(synthetic)) {
            return stack.resolveColumn(Collections.singletonList(column), AliasPolicy.NONE);
        }
    }

    // =================================================================================================================
    // Expressions: functions, windows, lambdas
    // =================================================================================================================

    @Override
    public Void visitAggregationFunctionCall(StarRocksParser.AggregationFunctionCallContext ctx) {
        if (ctx.over() != null) {
            try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.WINDOW)) {
                visit(ctx.aggregationFunction());
                visit(ctx.over());
            }
        } else {
            try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.AGGREGATION)) {
                tracker().markNonColumnNode();
                visit(ctx.aggregationFunction());
            }
        }
        return null;
    }

    @Override
    public Void visitWindowFunctionCall(StarRocksParser.WindowFunctionCallContext ctx) {
        try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.WINDOW)) {
            visit(ctx.windowFunction());
            visit(ctx.over());
        }
        return null;
    }

    /**
     * {@code OVER (PARTITION BY ... ORDER BY ... frame)}: partition / order columns are indirect for the value and
     * are counted as {@code WINDOW_PARTITION_BY} / {@code WINDOW_ORDER_BY} usage.
     */
    @Override
    public Void visitOver(StarRocksParser.OverContext ctx) {
        try (ExpressionSourceTracker.Context indirect = tracker().enterIndirect()) {
            pushClause(ClauseType.WINDOW_PARTITION_BY);
            try {
                for (StarRocksParser.ExpressionContext e : ctx.partition) {
                    visit(e);
                }
            } finally {
                popClause();
            }
            pushClause(ClauseType.WINDOW_ORDER_BY);
            try {
                for (StarRocksParser.SortItemContext s : ctx.sortItem()) {
                    visit(s.expression());
                }
                if (ctx.windowFrame() != null) {
                    visit(ctx.windowFrame());
                }
            } finally {
                popClause();
            }
        }
        return null;
    }

    /**
     * {@code name(args) [OVER (...)]}: aggregate when {@code name} is a StarRocks aggregate, window when followed by
     * OVER, scalar otherwise. Lambda arguments ({@code array_map((x, y) -> x + y, a1, a2)}) have their formal
     * parameters bound positionally to the element lineage of the non-lambda arguments (iron rule #5).
     */
    @Override
    public Void visitSimpleFunctionCall(StarRocksParser.SimpleFunctionCallContext ctx) {
        List<String> nameParts = qualifiedNameParts(ctx.qualifiedName());
        String fn = nameParts.get(nameParts.size() - 1).toLowerCase(Locale.ROOT);
        TransformType kind = ctx.over() != null ? TransformType.WINDOW
                : AGGREGATE_FUNCTIONS.contains(fn) ? TransformType.AGGREGATION : TransformType.EXPRESSION;
        try (ExpressionSourceTracker.Context c = tracker().enter(kind)) {
            tracker().markNonColumnNode();
            List<FieldLineage> arrayArgs = new ArrayList<>();
            List<StarRocksParser.LambdaFunctionExprContext> lambdas = new ArrayList<>();
            for (StarRocksParser.ExpressionContext arg : ctx.expression()) {
                StarRocksParser.LambdaFunctionExprContext lambda = unwrapLambda(arg);
                if (lambda != null) {
                    lambdas.add(lambda);
                } else {
                    arrayArgs.add(captureAndAdd(arg));
                }
            }
            for (StarRocksParser.LambdaFunctionExprContext lambda : lambdas) {
                analyzeLambda(lambda, arrayArgs);
            }
            if (ctx.over() != null) {
                visit(ctx.over());
            }
        }
        return null;
    }

    /**
     * Visits {@code arg} into a private tracker to learn its lineage, then adds it to the current expression under
     * the current contexts.
     */
    private FieldLineage captureAndAdd(StarRocksParser.ExpressionContext arg) {
        ExpressionSourceTracker own = new ExpressionSourceTracker();
        trackers.push(own);
        try {
            visit(arg);
        } finally {
            trackers.pop();
        }
        FieldLineage l = own.build();
        tracker().addLineage(l);
        return l;
    }

    private void analyzeLambda(StarRocksParser.LambdaFunctionExprContext lambda, List<FieldLineage> arrayArgs) {
        List<String> params = lambdaParameters(lambda);
        List<FieldLineage> bindings = new ArrayList<>();
        FieldLineage all = FieldLineage.constant();
        for (FieldLineage l : arrayArgs) {
            all = all.union(l);
        }
        for (int i = 0; i < params.size(); i++) {
            FieldLineage element = params.size() == arrayArgs.size() ? arrayArgs.get(i) : all;
            bindings.add(element.elevate(TransformType.EXPRESSION));
        }
        try (ScopeStack.Frame f = stack.pushLambda(params, bindings)) {
            if (lambda.expression() != null) {
                visit(lambda.expression());
            } else if (lambda.expressionList() != null) {
                visit(lambda.expressionList());
            }
        }
    }

    /** A lambda outside a function argument list (not produced by valid StarRocks SQL): parameters bound to nothing. */
    @Override
    public Void visitLambdaFunctionExpr(StarRocksParser.LambdaFunctionExprContext ctx) {
        analyzeLambda(ctx, Collections.emptyList());
        return null;
    }

    private static List<String> lambdaParameters(StarRocksParser.LambdaFunctionExprContext lambda) {
        if (lambda.identifier() != null) {
            return Collections.singletonList(identifierText(lambda.identifier()));
        }
        return identifiers(lambda.identifierList().identifier());
    }

    private static StarRocksParser.LambdaFunctionExprContext unwrapLambda(StarRocksParser.ExpressionContext e) {
        ParseTree node = e;
        while (true) {
            if (node instanceof StarRocksParser.LambdaFunctionExprContext) {
                return (StarRocksParser.LambdaFunctionExprContext) node;
            }
            if (node instanceof StarRocksParser.ExpressionDefaultContext
                    && ((StarRocksParser.ExpressionDefaultContext) node).BINARY() == null) {
                node = ((StarRocksParser.ExpressionDefaultContext) node).booleanExpression();
            } else if (node instanceof StarRocksParser.BooleanExpressionDefaultContext) {
                node = ((StarRocksParser.BooleanExpressionDefaultContext) node).predicate();
            } else if (node instanceof StarRocksParser.PredicateContext
                    && ((StarRocksParser.PredicateContext) node).predicateOperations() == null
                    && ((StarRocksParser.PredicateContext) node).valueExpression() != null) {
                node = ((StarRocksParser.PredicateContext) node).valueExpression();
            } else if (node instanceof StarRocksParser.ValueExpressionDefaultContext) {
                node = ((StarRocksParser.ValueExpressionDefaultContext) node).primaryExpression();
            } else {
                return null;
            }
        }
    }

    @Override
    public Void visitInformationFunction(StarRocksParser.InformationFunctionContext ctx) {
        tracker().markNonColumnNode();
        return null;
    }

    @Override
    public Void visitSpecialDateTime(StarRocksParser.SpecialDateTimeContext ctx) {
        tracker().markNonColumnNode();
        return null;
    }

    @Override
    public Void visitSpecialFunction(StarRocksParser.SpecialFunctionContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitExtract(StarRocksParser.ExtractContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitGroupingOperation(StarRocksParser.GroupingOperationContext ctx) {
        return expressionNode(ctx);
    }

    // =================================================================================================================
    // Expressions: sub-queries
    // =================================================================================================================

    /** {@code EXISTS (query)}: every source of the sub-query (output and control) only controls the value. */
    @Override
    public Void visitExists(StarRocksParser.ExistsContext ctx) {
        tracker().markNonColumnNode();
        RelationDescriptor sub = analyzeQueryRelation(ctx.queryRelation());
        addSubqueryAsIndirect(sub);
        return null;
    }

    /** Scalar sub-query {@code (SELECT max(x) ...)}: its single output flows into the value. */
    @Override
    public Void visitSubqueryExpression(StarRocksParser.SubqueryExpressionContext ctx) {
        RelationDescriptor sub = analyzeQueryRelation(ctx.subquery().queryRelation());
        addScalarSubquery(sub);
        return null;
    }

    /** {@code left op (query)}. */
    @Override
    public Void visitScalarSubquery(StarRocksParser.ScalarSubqueryContext ctx) {
        try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.EXPRESSION)) {
            visit(ctx.booleanExpression());
            addScalarSubquery(analyzeQueryRelation(ctx.queryRelation()));
        }
        return null;
    }

    /** {@code value [NOT] IN (query)}: the value is direct, the sub-query only controls the boolean. */
    @Override
    public Void visitInSubquery(StarRocksParser.InSubqueryContext ctx) {
        tracker().markNonColumnNode();
        addSubqueryAsIndirect(analyzeQueryRelation(ctx.queryRelation()));
        return null;
    }

    @Override
    public Void visitTupleInSubquery(StarRocksParser.TupleInSubqueryContext ctx) {
        try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.EXPRESSION)) {
            for (StarRocksParser.ExpressionContext e : ctx.expression()) {
                visit(e);
            }
            addSubqueryAsIndirect(analyzeQueryRelation(ctx.queryRelation()));
        }
        return null;
    }

    private void addScalarSubquery(RelationDescriptor sub) {
        tracker().markNonColumnNode();
        if (sub.size() > 0) {
            tracker().addLineage(sub.field(0).getLineage());
        }
        tracker().addIndirect(sub.getControlSources());
    }

    private void addSubqueryAsIndirect(RelationDescriptor sub) {
        Set<ColumnIdentifier> all = new LinkedHashSet<>(sub.getControlSources());
        for (RelationField f : sub.getFields()) {
            all.addAll(f.getLineage().allSources());
        }
        tracker().addIndirect(all);
    }

    // =================================================================================================================
    // Expressions: operators (every non-wrapper node is an EXPRESSION transform)
    // =================================================================================================================

    @Override
    public Void visitPredicate(StarRocksParser.PredicateContext ctx) {
        if (ctx.predicateOperations() == null && ctx.tupleInSubquery() == null) {
            return visitChildren(ctx);
        }
        return expressionNode(ctx);
    }

    @Override
    public Void visitLogicalNot(StarRocksParser.LogicalNotContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitLogicalBinary(StarRocksParser.LogicalBinaryContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitIsNull(StarRocksParser.IsNullContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitComparison(StarRocksParser.ComparisonContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitInList(StarRocksParser.InListContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitBetween(StarRocksParser.BetweenContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitLike(StarRocksParser.LikeContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitArithmeticBinary(StarRocksParser.ArithmeticBinaryContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitArithmeticUnary(StarRocksParser.ArithmeticUnaryContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitConcat(StarRocksParser.ConcatContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitCollate(StarRocksParser.CollateContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitCast(StarRocksParser.CastContext ctx) {
        try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.EXPRESSION)) {
            tracker().markNonColumnNode();
            visit(ctx.expression());
        }
        return null;
    }

    @Override
    public Void visitConvert(StarRocksParser.ConvertContext ctx) {
        try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.EXPRESSION)) {
            tracker().markNonColumnNode();
            visit(ctx.expression());
        }
        return null;
    }

    /** {@code CASE x WHEN ...}: operand, conditions and results are all part of the value (DIRECT). */
    @Override
    public Void visitSimpleCase(StarRocksParser.SimpleCaseContext ctx) {
        return expressionNode(ctx);
    }

    /** {@code CASE WHEN cond THEN v ... ELSE e END}: conditions and results are all part of the value (DIRECT). */
    @Override
    public Void visitSearchedCase(StarRocksParser.SearchedCaseContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitArrayConstructor(StarRocksParser.ArrayConstructorContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitMapConstructor(StarRocksParser.MapConstructorContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitCollectionSubscript(StarRocksParser.CollectionSubscriptContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitArraySlice(StarRocksParser.ArraySliceContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitArrowExpression(StarRocksParser.ArrowExpressionContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitOdbcFunctionCallExpression(StarRocksParser.OdbcFunctionCallExpressionContext ctx) {
        return expressionNode(ctx);
    }

    @Override
    public Void visitLiteral(StarRocksParser.LiteralContext ctx) {
        tracker().markNonColumnNode();
        return null;
    }

    @Override
    public Void visitUserVariableExpression(StarRocksParser.UserVariableExpressionContext ctx) {
        tracker().markNonColumnNode();
        return null;
    }

    @Override
    public Void visitSystemVariableExpression(StarRocksParser.SystemVariableExpressionContext ctx) {
        tracker().markNonColumnNode();
        return null;
    }

    /** {@code (expr)} is transparent. */
    @Override
    public Void visitParenthesizedExpression(StarRocksParser.ParenthesizedExpressionContext ctx) {
        visit(ctx.expression());
        return null;
    }

    private Void expressionNode(ParserRuleContext ctx) {
        try (ExpressionSourceTracker.Context c = tracker().enter(TransformType.EXPRESSION)) {
            tracker().markNonColumnNode();
            visitChildren(ctx);
        }
        return null;
    }

    // =================================================================================================================
    // Lineage emission
    // =================================================================================================================

    /**
     * Aligns target columns with the source relation <b>by position</b> (iron rule #3) and emits the edges.
     * Positions beyond the reliable prefix of the source are skipped with a warning.
     */
    private void emitLineage(TableIdentifier targetTable, List<String> targetColumns, RelationDescriptor source) {
        result.targetTable(targetTable);
        int aligned = Math.min(targetColumns.size(), source.getReliablePrefix());
        if (source.isPositionallyComplete() && targetColumns.size() != source.size()) {
            result.addWarning("target " + targetTable + " has " + targetColumns.size() + " columns but the query "
                    + "produces " + source.size() + ": aligned on the first " + aligned);
        } else if (aligned < targetColumns.size()) {
            result.addWarning("only the first " + aligned + " of " + targetColumns.size() + " target columns of "
                    + targetTable + " could be aligned (unexpanded * in the query)");
        }
        if (!source.getUnexpandedStarSources().isEmpty()) {
            result.addWarning("lineage incomplete: * over " + source.getUnexpandedStarSources());
        }
        for (int i = 0; i < aligned; i++) {
            ColumnIdentifier target = ColumnIdentifier.of(targetTable, targetColumns.get(i));
            RelationField field = source.field(i);
            FieldLineage lineage = field.getLineage();
            String expr = field.getExpression();
            if (lineage.isConstant()) {
                result.addEdge(ColumnLineageEdge.constant(target, expr));
            }
            for (Map.Entry<ColumnIdentifier, TransformType> e : lineage.getDirect().entrySet()) {
                result.addEdge(ColumnLineageEdge.of(target, e.getKey(), LineageType.DIRECT, e.getValue(), expr));
            }
            for (ColumnIdentifier src : lineage.getIndirect()) {
                result.addEdge(ColumnLineageEdge.of(target, src, LineageType.INDIRECT, TransformType.IDENTITY, expr));
            }
            for (ColumnIdentifier src : source.getControlSources()) {
                result.addEdge(ColumnLineageEdge.of(target, src, LineageType.INDIRECT, TransformType.IDENTITY, null));
            }
        }
    }

    /**
     * {@code INSERT ... VALUES (..), (..)}: column i lineage = union of the i-th expression of every row
     * ({@code DEFAULT} is a constant); the expression text is the first row expression that has a physical source.
     */
    private RelationDescriptor analyzeValuesRows(List<StarRocksParser.ExpressionsWithDefaultContext> rows) {
        List<FieldLineage> columns = new ArrayList<>();
        List<String> texts = new ArrayList<>();
        for (StarRocksParser.ExpressionsWithDefaultContext row : rows) {
            List<StarRocksParser.ExpressionOrDefaultContext> values = row.expressionOrDefault();
            for (int i = 0; i < values.size(); i++) {
                StarRocksParser.ExpressionOrDefaultContext v = values.get(i);
                FieldLineage l = v.expression() == null ? FieldLineage.constant()
                        : analyzeExpression(v.expression(), ClauseType.SELECT, AliasPolicy.NONE).build();
                if (i < columns.size()) {
                    if (columns.get(i).isConstant() && !l.isConstant()) {
                        texts.set(i, text(v));
                    }
                    columns.set(i, columns.get(i).union(l));
                } else {
                    columns.add(l);
                    texts.add(text(v));
                }
            }
        }
        List<RelationField> fields = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            fields.add(new RelationField("column_" + i, i, columns.get(i), texts.get(i)));
        }
        return RelationDescriptor.derived(RelationKind.VALUES, null, fields, Collections.emptySet());
    }

    // =================================================================================================================
    // State helpers
    // =================================================================================================================

    /**
     * Analyses one expression in a clause with its own tracker.
     *
     * @return the tracker, closed (its {@link ExpressionSourceTracker#build()} gives the lineage)
     */
    private ExpressionSourceTracker analyzeExpression(ParserRuleContext expression, ClauseType clause,
                                                      AliasPolicy policy) {
        ExpressionSourceTracker t = new ExpressionSourceTracker();
        AliasPolicy savedPolicy = aliasPolicy;
        trackers.push(t);
        pushClause(clause);
        aliasPolicy = policy;
        try {
            visit(expression);
        } finally {
            aliasPolicy = savedPolicy;
            popClause();
            trackers.pop();
        }
        return t;
    }

    private ExpressionSourceTracker tracker() {
        ExpressionSourceTracker t = trackers.peek();
        if (t == null) {
            throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE, "expression visited outside of a clause");
        }
        return t;
    }

    private void addControl(ExpressionSourceTracker t) {
        stack.currentQueryBlock().addControlLineage(t.build());
    }

    private void pushClause(ClauseType clause) {
        clauses.push(clause);
    }

    private void popClause() {
        clauses.pop();
    }

    private ClauseType currentClause() {
        ClauseType c = clauses.peek();
        return c == null ? ClauseType.SELECT : c;
    }

    private void recordUsage(ResolvedColumn rc, ClauseType clause) {
        recordUsage(rc.lineage(), clause);
    }

    /** Records one occurrence of every physical source of {@code lineage} (query statements only). */
    private void recordUsage(FieldLineage lineage, ClauseType clause) {
        if (!collectUsage) {
            return;
        }
        for (ColumnIdentifier c : lineage.allSources()) {
            result.recordUsage(c, clause);
        }
    }

    private RelationDescriptor relationOf(ParseTree tree) {
        relationRegister = null;
        visit(tree);
        RelationDescriptor r = relationRegister;
        relationRegister = null;
        if (r == null) {
            throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE,
                    "node did not produce a relation: " + tree.getClass().getSimpleName());
        }
        return r;
    }

    private TableIdentifier targetTable(StarRocksParser.QualifiedNameContext name) {
        TableIdentifier t = TableIdentifier.resolve(qualifiedNameParts(name), stack.getNamespace());
        result.targetTable(t);
        return t;
    }

    // =================================================================================================================
    // Text helpers
    // =================================================================================================================

    /** Unquoted parts of a {@code qualifiedName}, handling {@code DOT_IDENTIFIER} tokens ({@code t.1col}). */
    static List<String> qualifiedNameParts(StarRocksParser.QualifiedNameContext ctx) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (child instanceof StarRocksParser.IdentifierContext) {
                parts.add(identifierText((StarRocksParser.IdentifierContext) child));
            } else if (child instanceof TerminalNode
                    && ((TerminalNode) child).getSymbol().getType() == StarRocksParser.DOT_IDENTIFIER) {
                parts.add(child.getText().substring(1));
            }
        }
        return parts;
    }

    static String identifierText(StarRocksParser.IdentifierContext ctx) {
        return Identifiers.unquote(ctx.getText());
    }

    private static List<String> identifiers(List<StarRocksParser.IdentifierContext> ids) {
        List<String> result = new ArrayList<>(ids.size());
        for (StarRocksParser.IdentifierContext id : ids) {
            result.add(identifierText(id));
        }
        return result;
    }

    private static String aliasText(StarRocksParser.IdentifierContext alias) {
        return alias == null ? null : identifierText(alias);
    }

    private static String stringText(StarRocksParser.StringContext ctx) {
        String s = ctx.getText();
        if (s.length() >= 2) {
            char q = s.charAt(0);
            return s.substring(1, s.length() - 1).replace(String.valueOf(q) + q, String.valueOf(q));
        }
        return s;
    }

    private static List<String> names(List<RelationField> fields) {
        List<String> result = new ArrayList<>(fields.size());
        for (RelationField f : fields) {
            result.add(f.getName());
        }
        return result;
    }

    /** Original source text of a node (exact characters, whitespace included). */
    static String text(ParserRuleContext ctx) {
        Token start = ctx.getStart();
        Token stop = ctx.getStop();
        if (start == null || stop == null || stop.getStopIndex() < start.getStartIndex()) {
            return ctx.getText();
        }
        return start.getInputStream().getText(Interval.of(start.getStartIndex(), stop.getStopIndex()));
    }

    /**
     * Output name of an unaliased select item: the column name for a (qualified / struct) column reference,
     * otherwise the expression text.
     */
    private static String implicitName(StarRocksParser.ExpressionContext e) {
        ParseTree node = e;
        while (true) {
            if (node instanceof StarRocksParser.ColumnRefContext) {
                return identifierText(((StarRocksParser.ColumnRefContext) node).columnReference().identifier());
            }
            if (node instanceof StarRocksParser.DereferenceContext) {
                List<String> parts = dereferenceParts((StarRocksParser.DereferenceContext) node);
                if (parts != null) {
                    return parts.get(parts.size() - 1);
                }
                return text(e);
            }
            if (node instanceof StarRocksParser.ExpressionDefaultContext
                    && ((StarRocksParser.ExpressionDefaultContext) node).BINARY() == null) {
                node = ((StarRocksParser.ExpressionDefaultContext) node).booleanExpression();
            } else if (node instanceof StarRocksParser.BooleanExpressionDefaultContext) {
                node = ((StarRocksParser.BooleanExpressionDefaultContext) node).predicate();
            } else if (node instanceof StarRocksParser.PredicateContext
                    && ((StarRocksParser.PredicateContext) node).predicateOperations() == null
                    && ((StarRocksParser.PredicateContext) node).valueExpression() != null) {
                node = ((StarRocksParser.PredicateContext) node).valueExpression();
            } else if (node instanceof StarRocksParser.ValueExpressionDefaultContext) {
                node = ((StarRocksParser.ValueExpressionDefaultContext) node).primaryExpression();
            } else {
                return text(e);
            }
        }
    }

    /** {@code GROUP BY 2} / {@code ORDER BY 2}: a bare integer literal is a select-list ordinal. */
    private static Optional<Integer> ordinal(StarRocksParser.ExpressionContext e) {
        String t = e.getText();
        if (!t.isEmpty() && t.length() <= 9 && t.chars().allMatch(Character::isDigit)) {
            return Optional.of(Integer.parseInt(t));
        }
        return Optional.empty();
    }
}
