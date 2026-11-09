package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.TransformType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Accumulates the lineage of one expression while the AST visitor walks it.
 *
 * <p>The visitor brackets AST nodes with contexts:</p>
 * <pre>
 * try (ExpressionSourceTracker.Context c = tracker.enter(TransformType.AGGREGATION)) { visit(args); }
 * try (ExpressionSourceTracker.Context c = tracker.enterIndirect()) { visit(partitionBy); visit(orderBy); }
 * tracker.addReference(scopeStack.resolveColumn(parts, policy));
 * </pre>
 * <ul>
 *   <li>The effective transform is the dominant one of all open contexts (base {@code IDENTITY}): a column read
 *       inside {@code a + 1} is {@code EXPRESSION}, inside {@code SUM(a + 1)} {@code AGGREGATION}, inside
 *       {@code SUM(a) OVER (...)} {@code WINDOW}. Each source keeps its own transform, so {@code SUM(a) + b} gives
 *       {@code a -> AGGREGATION, b -> EXPRESSION}.</li>
 *   <li>Inside an indirect context (window PARTITION BY / ORDER BY, the predicate of a scalar sub-query ...) every
 *       source is recorded as indirect for this value.</li>
 *   <li>A resolved column contributes its whole {@link FieldLineage}: derived columns are composed, never
 *       re-analysed. Lambda parameters contribute the lineage bound to them.</li>
 *   <li>{@link #getReferences()} keeps every resolved reference for column-usage accounting.</li>
 * </ul>
 * Contexts must be closed in LIFO order; a mismatch raises {@link SemanticAnalysisException}. Not thread-safe.
 */
public final class ExpressionSourceTracker {

    private final Deque<TransformType> transforms = new ArrayDeque<>();
    private int indirectDepth;
    private TransformType strongestSeen = TransformType.IDENTITY;
    private boolean nonColumnNodeSeen;

    private final Map<ColumnIdentifier, TransformType> direct = new LinkedHashMap<>();
    private final Set<ColumnIdentifier> indirect = new LinkedHashSet<>();
    private final List<ResolvedColumn> references = new ArrayList<>();
    private final Deque<Context> openContexts = new ArrayDeque<>();

    public ExpressionSourceTracker() {
        transforms.push(TransformType.IDENTITY);
    }

    /** @return dominant transform of the open contexts */
    public TransformType currentTransform() {
        return transforms.peek();
    }

    /** @return {@code true} inside {@link #enterIndirect()} */
    public boolean isIndirect() {
        return indirectDepth > 0;
    }

    /**
     * Opens a transform context. {@link TransformType#CONSTANT} is not a valid context.
     */
    public Context enter(TransformType transform) {
        if (transform == null || transform == TransformType.CONSTANT) {
            throw new IllegalArgumentException("invalid transform context: " + transform);
        }
        TransformType effective = transforms.peek().combine(transform);
        transforms.push(effective);
        strongestSeen = strongestSeen.combine(effective);
        if (transform != TransformType.IDENTITY) {
            nonColumnNodeSeen = true;
        }
        Context c = new Context(false);
        openContexts.push(c);
        return c;
    }

    /** Opens an indirect context: sources seen inside only control the value. */
    public Context enterIndirect() {
        indirectDepth++;
        Context c = new Context(true);
        openContexts.push(c);
        return c;
    }

    /**
     * Notes that the expression is more than a bare column (literal operand, operator, function); used together
     * with {@link #isBareColumn()}.
     */
    public void markNonColumnNode() {
        nonColumnNodeSeen = true;
    }

    /** Adds a resolved column reference. */
    public void addReference(ResolvedColumn column) {
        references.add(column);
        addLineage(column.lineage());
    }

    /**
     * Adds a lineage (e.g. the output of a scalar sub-query) under the current contexts.
     */
    public void addLineage(FieldLineage lineage) {
        if (indirectDepth > 0) {
            indirect.addAll(lineage.allSources());
            return;
        }
        FieldLineage elevated = lineage.elevate(transforms.peek());
        for (Map.Entry<ColumnIdentifier, TransformType> e : elevated.getDirect().entrySet()) {
            direct.merge(e.getKey(), e.getValue(), TransformType::combine);
        }
        indirect.addAll(elevated.getIndirect());
    }

    /** Adds sources that only control the value (e.g. the WHERE columns of a scalar sub-query). */
    public void addIndirect(Collection<ColumnIdentifier> columns) {
        for (ColumnIdentifier c : columns) {
            if (!c.isConstant()) {
                indirect.add(c);
            }
        }
    }

    /** @return every column reference resolved for this expression, in visiting order */
    public List<ResolvedColumn> getReferences() {
        return Collections.unmodifiableList(references);
    }

    /**
     * @return strongest transform context opened at any time, e.g. {@code AGGREGATION} for {@code COUNT(*)} although
     *         it has no source column
     */
    public TransformType getStrongestTransformSeen() {
        return strongestSeen;
    }

    /** @return {@code true} when the expression is exactly one column reference (an {@code IDENTITY} candidate) */
    public boolean isBareColumn() {
        return !nonColumnNodeSeen && references.size() == 1;
    }

    /**
     * @return lineage of the expression
     * @throws SemanticAnalysisException when contexts are still open
     */
    public FieldLineage build() {
        if (!openContexts.isEmpty()) {
            throw new SemanticAnalysisException(SemanticAnalysisException.ErrorCode.SCOPE_STATE,
                    openContexts.size() + " expression context(s) still open");
        }
        return FieldLineage.of(direct, indirect);
    }

    /**
     * Handle of an open context; must be closed in LIFO order.
     */
    public final class Context implements AutoCloseable {
        private final boolean indirectContext;
        private boolean closed;

        private Context(boolean indirectContext) {
            this.indirectContext = indirectContext;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            if (openContexts.peek() != this) {
                throw new SemanticAnalysisException(SemanticAnalysisException.ErrorCode.SCOPE_STATE,
                        "expression contexts closed out of order");
            }
            openContexts.pop();
            if (indirectContext) {
                indirectDepth--;
            } else {
                transforms.pop();
            }
            closed = true;
        }
    }
}
