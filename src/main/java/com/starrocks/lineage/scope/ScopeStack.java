package com.starrocks.lineage.scope;

import com.starrocks.lineage.metadata.MetadataProvider;
import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.Identifiers;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The symbol table of one statement: a stack of {@link Scope}s plus the services that need the whole stack
 * (CTE lookup, correlated column resolution, physical table resolution).
 *
 * <p><b>Discipline.</b> Every push returns a {@link Frame} that must be closed in LIFO order, normally with
 * try-with-resources. Closing a frame that is not on top is reported as {@link ErrorCode#SCOPE_STATE} instead of
 * silently popping someone else's scope, so a visitor bug cannot corrupt name resolution for the rest of the
 * statement ("stack pollution"). {@link #assertEmpty()} verifies at the end of a statement that nothing leaked.</p>
 *
 * <p><b>Column resolution</b> ({@link #resolveColumn}) walks from the top of the stack outwards:</p>
 * <ol>
 *   <li>LAMBDA scopes: a formal parameter shadows any column of the same name;</li>
 *   <li>QUERY_BLOCK / OUTPUT scopes: {@link Scope#resolveLocal}; select aliases only in the innermost block;</li>
 *   <li>CTE scopes are transparent for columns;</li>
 *   <li>a hit in the k-th enclosing block is a correlated reference with {@code outerDepth = k}.</li>
 * </ol>
 *
 * <p><b>Relation resolution</b> ({@link #resolveRelation}): a one-part name is first looked up as a CTE from the
 * innermost WITH clause outwards (inner CTEs shadow outer ones and physical tables); otherwise the name is completed
 * with the session namespace and resolved through the {@link MetadataProvider}. A CTE is registered only after its
 * body has been analysed, so {@code WITH t AS (SELECT * FROM t)} reads the physical {@code t}, and a later CTE can
 * read an earlier one of the same WITH clause.</p>
 */
public final class ScopeStack {

    /** Default protection against pathological nesting. */
    public static final int DEFAULT_MAX_DEPTH = 256;

    private final Deque<Scope> stack = new ArrayDeque<>();
    private final NamespaceContext namespace;
    private final MetadataProvider metadata;
    private final int maxDepth;
    private final Set<TableIdentifier> referencedTables = new LinkedHashSet<>();

    public ScopeStack(NamespaceContext namespace, MetadataProvider metadata) {
        this(namespace, metadata, DEFAULT_MAX_DEPTH);
    }

    public ScopeStack(NamespaceContext namespace, MetadataProvider metadata, int maxDepth) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        if (maxDepth < 1) {
            throw new IllegalArgumentException("maxDepth must be >= 1");
        }
        this.maxDepth = maxDepth;
    }

    public NamespaceContext getNamespace() {
        return namespace;
    }

    public MetadataProvider getMetadata() {
        return metadata;
    }

    // ------------------------------------------------------------------------------------------------ push / pop

    /** Opens a query block ({@code querySpecification}). */
    public Frame pushQueryBlock() {
        return push(new Scope(Scope.Kind.QUERY_BLOCK, stack.peek()));
    }

    /** Opens a WITH clause; register its CTEs with {@link #registerCte} after analysing each body. */
    public Frame pushCte() {
        return push(new Scope(Scope.Kind.CTE, stack.peek()));
    }

    /**
     * Opens a lambda: {@code (x, y) -> ...}.
     *
     * @param parameters formal parameter names
     * @param bindings   lineage bound to each parameter (e.g. the element lineage of the array argument), or an
     *                   empty list when the parameters carry no lineage of their own
     */
    public Frame pushLambda(List<String> parameters, List<FieldLineage> bindings) {
        Objects.requireNonNull(parameters, "parameters");
        if (!bindings.isEmpty() && bindings.size() != parameters.size()) {
            throw new IllegalArgumentException("bindings must be empty or match the parameter count");
        }
        Scope scope = new Scope(Scope.Kind.LAMBDA, stack.peek());
        for (int i = 0; i < parameters.size(); i++) {
            scope.declareLambdaParameter(parameters.get(i),
                    bindings.isEmpty() ? FieldLineage.constant() : bindings.get(i));
        }
        return push(scope);
    }

    /** Opens a scope exposing a query output to its trailing ORDER BY. */
    public Frame pushOutput(RelationDescriptor output) {
        Scope scope = new Scope(Scope.Kind.OUTPUT, stack.peek());
        scope.addRelation(output);
        return push(scope);
    }

    private Frame push(Scope scope) {
        if (stack.size() >= maxDepth) {
            throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE,
                    "scope nesting deeper than " + maxDepth);
        }
        stack.push(scope);
        return new Frame(scope);
    }

    /** @return the top scope */
    public Scope current() {
        Scope top = stack.peek();
        if (top == null) {
            throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE, "scope stack is empty");
        }
        return top;
    }

    /** @return the innermost QUERY_BLOCK scope */
    public Scope currentQueryBlock() {
        for (Scope s : stack) {
            if (s.getKind() == Scope.Kind.QUERY_BLOCK) {
                return s;
            }
        }
        throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE, "no enclosing query block");
    }

    public int depth() {
        return stack.size();
    }

    public boolean isEmpty() {
        return stack.isEmpty();
    }

    /** @throws SemanticAnalysisException when scopes leaked (a frame was never closed) */
    public void assertEmpty() {
        if (!stack.isEmpty()) {
            throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE,
                    stack.size() + " scope(s) leaked, top = " + stack.peek());
        }
    }

    // ------------------------------------------------------------------------------------------------ CTE

    /**
     * Registers a CTE in the innermost WITH scope, which must be the top scope.
     *
     * @param columnAliases {@code WITH c(a, b) AS ...}; empty when absent
     * @param body          relation produced by the CTE body
     * @return the registered relation (renamed, kind CTE, aliased with the CTE name)
     */
    public RelationDescriptor registerCte(String name, List<String> columnAliases, RelationDescriptor body) {
        Scope top = current();
        if (top.getKind() != Scope.Kind.CTE) {
            throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE, "registerCte outside of a WITH scope");
        }
        RelationDescriptor cte = (body.getKind() == RelationKind.TABLE ? body : body.withKind(RelationKind.CTE))
                .withColumnAliases(columnAliases).withAlias(name);
        top.registerCte(name, cte);
        return cte;
    }

    /** @return the innermost visible CTE with that name */
    public Optional<RelationDescriptor> findCte(String name) {
        for (Scope s : stack) {
            if (s.getKind() == Scope.Kind.CTE) {
                Optional<RelationDescriptor> cte = s.findCte(name);
                if (cte.isPresent()) {
                    return cte;
                }
            }
        }
        return Optional.empty();
    }

    // ------------------------------------------------------------------------------------------------ relations

    /**
     * Resolves a relation name of a FROM clause.
     *
     * @param nameParts qualified name parts ({@code [t]}, {@code [db, t]}, {@code [catalog, db, t]})
     * @param alias     alias from the query, {@code null} when absent
     * <p>Table names keep their spelling: {@code default_catalog} OLAP table and database names are case-sensitive.
     * In an external catalog (Hive, Iceberg, Hudi, Paimon ...), names are case-insensitive and stored in lower case,
     * so when no table exists under the spelling used in the query the lower-case spelling is tried and, when it
     * exists, becomes the identifier - one physical table must map to one storage key.</p>
     *
     * @return a CTE reference (aliased with {@code alias} or the CTE name) or a physical table relation
     * @throws com.starrocks.lineage.model.UnresolvedNameException when the name cannot be completed
     * @throws com.starrocks.lineage.metadata.MetadataAccessException on metadata infrastructure failure
     */
    public RelationDescriptor resolveRelation(List<String> nameParts, String alias) {
        Objects.requireNonNull(nameParts, "nameParts");
        if (nameParts.size() == 1) {
            Optional<RelationDescriptor> cte = findCte(nameParts.get(0));
            if (cte.isPresent()) {
                return alias == null ? cte.get() : cte.get().withAlias(alias);
            }
        }
        TableIdentifier table = TableIdentifier.resolve(nameParts, namespace);
        Optional<TableSchema> schema = metadata.getTableSchema(table);
        if (!schema.isPresent() && !Identifiers.DEFAULT_CATALOG.equals(table.getCatalog())) {
            TableIdentifier lower = TableIdentifier.of(table.getCatalog(),
                    table.getDatabase().toLowerCase(Locale.ROOT), table.getTable().toLowerCase(Locale.ROOT));
            if (!lower.equals(table)) {
                Optional<TableSchema> lowerSchema = metadata.getTableSchema(lower);
                if (lowerSchema.isPresent()) {
                    table = lower;
                    schema = lowerSchema;
                }
            }
        }
        referencedTables.add(table);
        return RelationDescriptor.table(table, alias, schema);
    }

    /** @return every physical table resolved through {@link #resolveRelation}, in first-reference order */
    public Set<TableIdentifier> getReferencedTables() {
        return Collections.unmodifiableSet(referencedTables);
    }

    // ------------------------------------------------------------------------------------------------ columns

    /**
     * Resolves a column reference, see the class comment for the algorithm.
     *
     * @param parts  name parts as written ({@code [c]}, {@code [t, c]}, {@code [db, t, c]}, {@code [t, c, f]} ...)
     * @param policy select-alias visibility in the innermost block
     * @throws SemanticAnalysisException {@link ErrorCode#COLUMN_NOT_FOUND} or an ambiguity code
     */
    public ResolvedColumn resolveColumn(List<String> parts, AliasPolicy policy) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("column reference without name");
        }
        Objects.requireNonNull(policy, "policy");
        int outerDepth = 0;
        boolean innermostBlock = true;
        Iterator<Scope> it = stack.iterator();
        while (it.hasNext()) {
            Scope s = it.next();
            if (s.getKind() == Scope.Kind.LAMBDA) {
                Optional<RelationField> param = s.findLambdaParameter(parts.get(0));
                if (param.isPresent()) {
                    return new ResolvedColumn(ResolvedColumn.Kind.LAMBDA_PARAMETER, null, param.get(),
                            new java.util.ArrayList<>(parts.subList(1, parts.size())), outerDepth, false);
                }
            } else if (s.isBlock()) {
                Optional<ResolvedColumn> hit = s.resolveLocal(parts, innermostBlock ? policy : AliasPolicy.NONE);
                if (hit.isPresent()) {
                    return hit.get().atDepth(outerDepth);
                }
                innermostBlock = false;
                outerDepth++;
            }
        }
        throw new SemanticAnalysisException(ErrorCode.COLUMN_NOT_FOUND,
                "Column '" + String.join(".", parts) + "' cannot be resolved");
    }

    /** Shortcut for {@link #resolveColumn(List, AliasPolicy)} with {@link AliasPolicy#NONE}. */
    public ResolvedColumn resolveColumn(List<String> parts) {
        return resolveColumn(parts, AliasPolicy.NONE);
    }

    /**
     * Expands {@code *} / {@code q.*} in the innermost query block.
     *
     * @param qualifier empty list for {@code *}
     */
    public StarExpansion expandStar(List<String> qualifier) {
        return currentQueryBlock().expandStar(qualifier);
    }

    /**
     * Handle of one pushed scope. Closing is idempotent; closing out of LIFO order fails.
     */
    public final class Frame implements AutoCloseable {
        private final Scope scope;
        private boolean closed;

        private Frame(Scope scope) {
            this.scope = scope;
        }

        public Scope scope() {
            return scope;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            if (stack.peek() != scope) {
                throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE,
                        "closing " + scope + " but the top scope is " + stack.peek());
            }
            stack.pop();
            closed = true;
        }
    }
}
