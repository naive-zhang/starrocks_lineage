package com.starrocks.lineage.scope;

import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.Identifiers;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One level of the symbol table. Scopes are created and destroyed exclusively through {@link ScopeStack}.
 *
 * <ul>
 *   <li>{@link Kind#QUERY_BLOCK}: one {@code querySpecification}; owns the FROM namespace (relations in order),
 *       the SELECT outputs (for alias / ordinal references) and the block-level control sources.</li>
 *   <li>{@link Kind#OUTPUT}: exposes the output of a query expression to its trailing {@code ORDER BY} (e.g. after
 *       a set operation).</li>
 *   <li>{@link Kind#CTE}: owns the CTEs of one {@code WITH} clause, in definition order.</li>
 *   <li>{@link Kind#LAMBDA}: the formal parameters of one lambda; they shadow every column of the same name.</li>
 * </ul>
 * Not thread-safe; one statement is analysed by one thread.
 */
public final class Scope {

    /**
     * Scope kind.
     */
    public enum Kind {
        QUERY_BLOCK,
        OUTPUT,
        CTE,
        LAMBDA
    }

    private final Kind kind;
    private final Scope parent;
    private final int level;

    private final List<RelationDescriptor> relations = new ArrayList<>();
    private final Set<RelationDescriptor> hidden = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<String> usingColumns = new LinkedHashSet<>();
    private final Set<ColumnIdentifier> controlSources = new LinkedHashSet<>();
    private List<RelationField> selectOutputs = Collections.emptyList();
    private int selectReliablePrefix;
    private final Set<String> selectUnexpandedSources = new LinkedHashSet<>();

    private final Map<String, RelationDescriptor> ctes = new LinkedHashMap<>();
    private final Map<String, RelationField> lambdaParameters = new LinkedHashMap<>();

    Scope(Kind kind, Scope parent) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.parent = parent;
        this.level = parent == null ? 0 : parent.level + 1;
    }

    public Kind getKind() {
        return kind;
    }

    /** @return enclosing scope, {@code null} for the outermost one */
    public Scope getParent() {
        return parent;
    }

    /** @return 0 for the outermost scope */
    public int getLevel() {
        return level;
    }

    boolean isBlock() {
        return kind == Kind.QUERY_BLOCK || kind == Kind.OUTPUT;
    }

    private void requireKind(Kind... allowed) {
        for (Kind k : allowed) {
            if (k == kind) {
                return;
            }
        }
        throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE, "operation not allowed in a " + kind + " scope");
    }

    // ------------------------------------------------------------------------------------------------ FROM namespace

    /**
     * Adds a relation to the FROM namespace (in FROM / JOIN order).
     *
     * @throws SemanticAnalysisException {@link ErrorCode#DUPLICATE_RELATION} when another relation of this block is
     *                                   referenced through the same alias / table name ("Not unique table/alias")
     */
    public void addRelation(RelationDescriptor relation) {
        requireKind(Kind.QUERY_BLOCK, Kind.OUTPUT);
        Objects.requireNonNull(relation, "relation");
        Optional<String> name = relation.referenceName();
        if (name.isPresent()) {
            for (RelationDescriptor r : relations) {
                if (r.referenceName().equals(name)) {
                    throw new SemanticAnalysisException(ErrorCode.DUPLICATE_RELATION,
                            "Not unique table/alias: '" + relation.displayName() + "'");
                }
            }
        }
        relations.add(relation);
    }

    /**
     * Hides relations from name resolution and star expansion; used after a SEMI / ANTI join whose output only
     * contains one side (StarRocks {@code QueryAnalyzer#visitJoin}). The hidden relations keep contributing their
     * control sources.
     */
    public void hideRelations(Collection<RelationDescriptor> toHide) {
        requireKind(Kind.QUERY_BLOCK);
        for (RelationDescriptor r : toHide) {
            if (!containsIdentity(r)) {
                throw new SemanticAnalysisException(ErrorCode.SCOPE_STATE,
                        "relation " + r.displayName() + " is not part of this scope");
            }
            hidden.add(r);
        }
    }

    private boolean containsIdentity(RelationDescriptor r) {
        for (RelationDescriptor x : relations) {
            if (x == r) {
                return true;
            }
        }
        return false;
    }

    /**
     * Records the columns of a {@code JOIN ... USING (c1, c2)}: an unqualified reference to such a column is not
     * ambiguous and designates the left-most relation that has it.
     */
    public void addUsingColumns(Collection<String> columns) {
        requireKind(Kind.QUERY_BLOCK);
        for (String c : columns) {
            usingColumns.add(Identifiers.normalizeColumn(c));
        }
    }

    /** @return relations visible to name resolution, in FROM order */
    public List<RelationDescriptor> getVisibleRelations() {
        List<RelationDescriptor> result = new ArrayList<>(relations.size());
        for (RelationDescriptor r : relations) {
            if (!hidden.contains(r)) {
                result.add(r);
            }
        }
        return result;
    }

    /** @return every relation of the FROM clause, hidden ones included */
    public List<RelationDescriptor> getAllRelations() {
        return Collections.unmodifiableList(relations);
    }

    // ------------------------------------------------------------------------------------------------ control

    /** Adds row-control sources of this block (WHERE, JOIN ON / USING, GROUP BY, HAVING, QUALIFY ...). */
    public void addControlSources(Collection<ColumnIdentifier> columns) {
        requireKind(Kind.QUERY_BLOCK, Kind.OUTPUT);
        for (ColumnIdentifier c : columns) {
            if (!c.isConstant()) {
                controlSources.add(c);
            }
        }
    }

    /** Adds every physical source (direct and indirect) of a lineage as row-control source. */
    public void addControlLineage(FieldLineage lineage) {
        addControlSources(lineage.allSources());
    }

    /**
     * @return control sources of the block: its own plus those of every FROM relation (hidden ones included - the
     *         right side of a SEMI join still filters the rows)
     */
    public Set<ColumnIdentifier> getControlSources() {
        Set<ColumnIdentifier> all = new LinkedHashSet<>(controlSources);
        for (RelationDescriptor r : relations) {
            all.addAll(r.getControlSources());
        }
        return Collections.unmodifiableSet(all);
    }

    // ------------------------------------------------------------------------------------------------ SELECT output

    /**
     * Publishes the SELECT list of this block, for alias / ordinal references and {@link #toDerivedRelation}.
     *
     * @param outputs            fields in SELECT order (stars already expanded)
     * @param reliablePrefix     number of leading outputs with reliable positions
     * @param unexpandedSources  relations whose {@code *} could not be expanded
     */
    public void setSelectOutputs(List<RelationField> outputs, int reliablePrefix,
                                 Collection<String> unexpandedSources) {
        requireKind(Kind.QUERY_BLOCK);
        List<RelationField> indexed = new ArrayList<>(outputs.size());
        for (int i = 0; i < outputs.size(); i++) {
            indexed.add(outputs.get(i).withIndex(i));
        }
        if (reliablePrefix < 0 || reliablePrefix > indexed.size()) {
            throw new IllegalArgumentException("reliablePrefix outside 0.." + indexed.size());
        }
        this.selectOutputs = Collections.unmodifiableList(indexed);
        this.selectReliablePrefix = reliablePrefix;
        this.selectUnexpandedSources.clear();
        this.selectUnexpandedSources.addAll(unexpandedSources);
    }

    /** Publishes a fully known SELECT list. */
    public void setSelectOutputs(List<RelationField> outputs) {
        setSelectOutputs(outputs, outputs.size(), Collections.emptySet());
    }

    public List<RelationField> getSelectOutputs() {
        return selectOutputs;
    }

    /**
     * Resolves {@code GROUP BY n} / {@code ORDER BY n}.
     *
     * @param ordinal 1-based position
     * @throws SemanticAnalysisException {@link ErrorCode#ORDINAL_OUT_OF_RANGE} when outside the reliable prefix
     */
    public RelationField selectOutputByOrdinal(int ordinal) {
        requireKind(Kind.QUERY_BLOCK);
        if (ordinal < 1 || ordinal > selectReliablePrefix) {
            throw new SemanticAnalysisException(ErrorCode.ORDINAL_OUT_OF_RANGE,
                    "ordinal " + ordinal + " outside 1.." + selectReliablePrefix);
        }
        return selectOutputs.get(ordinal - 1);
    }

    /**
     * Builds the relation produced by this block (sub-query, CTE body, view / MV definition).
     */
    public RelationDescriptor toDerivedRelation(RelationKind relationKind, String alias) {
        requireKind(Kind.QUERY_BLOCK);
        return RelationDescriptor.derived(relationKind, alias, selectOutputs, getControlSources(),
                selectReliablePrefix, selectUnexpandedSources);
    }

    // ------------------------------------------------------------------------------------------------ CTE

    void registerCte(String name, RelationDescriptor body) {
        requireKind(Kind.CTE);
        String key = Identifiers.unquote(name).toLowerCase(Locale.ROOT);
        if (ctes.containsKey(key)) {
            throw new SemanticAnalysisException(ErrorCode.DUPLICATE_CTE, "duplicate CTE name '" + name + "'");
        }
        ctes.put(key, body);
    }

    Optional<RelationDescriptor> findCte(String name) {
        return Optional.ofNullable(ctes.get(Identifiers.unquote(name).toLowerCase(Locale.ROOT)));
    }

    /** @return the CTEs of this WITH clause in definition order (keys lower-case) */
    public Map<String, RelationDescriptor> getCtes() {
        return Collections.unmodifiableMap(ctes);
    }

    // ------------------------------------------------------------------------------------------------ lambda

    void declareLambdaParameter(String name, FieldLineage binding) {
        requireKind(Kind.LAMBDA);
        String n = Identifiers.normalizeColumn(name);
        lambdaParameters.put(n, new RelationField(n, lambdaParameters.size(), binding, n));
    }

    Optional<RelationField> findLambdaParameter(String name) {
        return Optional.ofNullable(lambdaParameters.get(Identifiers.normalizeColumn(name)));
    }

    // ------------------------------------------------------------------------------------------------ resolution

    /**
     * Resolves a (possibly qualified, possibly struct-dereferencing) name against this block only.
     *
     * <p>Algorithm, for {@code parts = [p0, ..., pn-1]}:</p>
     * <ol>
     *   <li>{@link AliasPolicy#PREFER_ALIAS} and one part: a select alias wins.</li>
     *   <li>Qualified interpretation, longest qualifier first ({@code k = min(3, n-1) .. 1}): qualifier
     *       {@code p0..pk-1} must match exactly one visible relation and {@code pk} must be one of its fields
     *       (or an on-demand field when its schema is unknown); {@code pk+1..} is a struct path.</li>
     *   <li>Unqualified interpretation: {@code p0} is a column of exactly one visible relation, the rest is a struct
     *       path. Several matches are ambiguous unless the column is a {@code USING} column or every match carries
     *       the same lineage. No match in known schemas: the single open relation, if any, owns the column.</li>
     *   <li>{@link AliasPolicy#PREFER_INPUT} and one part: a select alias.</li>
     * </ol>
     *
     * @return empty when the name is not visible in this block (the caller continues with the outer scope)
     */
    Optional<ResolvedColumn> resolveLocal(List<String> parts, AliasPolicy policy) {
        List<RelationDescriptor> visible = getVisibleRelations();
        if (policy == AliasPolicy.PREFER_ALIAS && parts.size() == 1) {
            Optional<ResolvedColumn> alias = resolveSelectAlias(parts.get(0));
            if (alias.isPresent()) {
                return alias;
            }
        }
        for (int k = Math.min(3, parts.size() - 1); k >= 1; k--) {
            List<String> qualifier = parts.subList(0, k);
            List<RelationDescriptor> matching = new ArrayList<>(1);
            for (RelationDescriptor r : visible) {
                if (r.matchesQualifier(qualifier)) {
                    matching.add(r);
                }
            }
            if (matching.isEmpty()) {
                continue;
            }
            if (matching.size() > 1) {
                throw new SemanticAnalysisException(ErrorCode.AMBIGUOUS_COLUMN,
                        "qualifier '" + String.join(".", qualifier) + "' matches several relations");
            }
            RelationDescriptor rel = matching.get(0);
            String column = parts.get(k);
            List<String> path = new ArrayList<>(parts.subList(k + 1, parts.size()));
            Optional<ResolvedColumn> hit = fieldOf(rel, column, path, true);
            if (hit.isPresent()) {
                return hit;
            }
        }
        Optional<ResolvedColumn> unqualified = resolveUnqualified(visible, parts);
        if (unqualified.isPresent()) {
            return unqualified;
        }
        if (policy == AliasPolicy.PREFER_INPUT && parts.size() == 1) {
            return resolveSelectAlias(parts.get(0));
        }
        return Optional.empty();
    }

    private Optional<ResolvedColumn> resolveUnqualified(List<RelationDescriptor> visible, List<String> parts) {
        String column = parts.get(0);
        List<String> path = new ArrayList<>(parts.subList(1, parts.size()));
        List<ResolvedColumn> hits = new ArrayList<>();
        for (RelationDescriptor r : visible) {
            fieldOf(r, column, path, false).ifPresent(hits::add);
        }
        if (hits.size() == 1) {
            return Optional.of(hits.get(0));
        }
        if (hits.size() > 1) {
            if (usingColumns.contains(Identifiers.normalizeColumn(column)) || sameLineage(hits)) {
                return Optional.of(hits.get(0));
            }
            throw new SemanticAnalysisException(ErrorCode.AMBIGUOUS_COLUMN,
                    "Column '" + column + "' is ambiguous");
        }
        List<RelationDescriptor> open = new ArrayList<>(1);
        for (RelationDescriptor r : visible) {
            if (!r.isSchemaKnown()) {
                open.add(r);
            }
        }
        if (open.size() == 1) {
            return fieldOf(open.get(0), column, path, true);
        }
        if (open.size() > 1) {
            throw new SemanticAnalysisException(ErrorCode.AMBIGUOUS_UNKNOWN_SCHEMA,
                    "Column '" + column + "' may belong to any of " + open.size() + " relations without metadata");
        }
        return Optional.empty();
    }

    private static boolean sameLineage(List<ResolvedColumn> hits) {
        FieldLineage first = hits.get(0).getField().getLineage();
        for (ResolvedColumn h : hits) {
            if (!h.getField().getLineage().equals(first)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Field {@code column} of {@code rel}; several fields with that name in one relation are ambiguous unless they
     * share the same lineage.
     */
    private static Optional<ResolvedColumn> fieldOf(RelationDescriptor rel, String column, List<String> path,
                                                   boolean allowOnDemand) {
        List<RelationField> fields = rel.lookup(column);
        if (fields.size() > 1) {
            FieldLineage first = fields.get(0).getLineage();
            for (RelationField f : fields) {
                if (!f.getLineage().equals(first)) {
                    throw new SemanticAnalysisException(ErrorCode.AMBIGUOUS_COLUMN,
                            "Column '" + column + "' is ambiguous in " + rel.displayName());
                }
            }
        }
        if (!fields.isEmpty()) {
            return Optional.of(new ResolvedColumn(ResolvedColumn.Kind.RELATION_FIELD, rel, fields.get(0), path, 0,
                    false));
        }
        if (allowOnDemand) {
            return rel.onDemandField(column).map(f -> new ResolvedColumn(ResolvedColumn.Kind.RELATION_FIELD, rel, f,
                    path, 0, true));
        }
        return Optional.empty();
    }

    private Optional<ResolvedColumn> resolveSelectAlias(String name) {
        if (kind != Kind.QUERY_BLOCK) {
            return Optional.empty();
        }
        List<RelationField> matches = new ArrayList<>(1);
        for (RelationField f : selectOutputs) {
            if (f.hasName(name)) {
                matches.add(f);
            }
        }
        if (matches.isEmpty()) {
            return Optional.empty();
        }
        FieldLineage first = matches.get(0).getLineage();
        for (RelationField f : matches) {
            if (!f.getLineage().equals(first)) {
                throw new SemanticAnalysisException(ErrorCode.AMBIGUOUS_COLUMN,
                        "Select alias '" + name + "' is ambiguous");
            }
        }
        return Optional.of(new ResolvedColumn(ResolvedColumn.Kind.SELECT_ALIAS, null, matches.get(0),
                Collections.emptyList(), 0, false));
    }

    /**
     * Expands {@code *} (empty qualifier) or {@code q.*} over the visible relations, see {@link StarExpansion}.
     */
    StarExpansion expandStar(List<String> qualifier) {
        List<RelationDescriptor> targets = new ArrayList<>();
        List<RelationDescriptor> visible = getVisibleRelations();
        if (qualifier == null || qualifier.isEmpty()) {
            targets.addAll(visible);
        } else {
            for (RelationDescriptor r : visible) {
                if (r.matchesQualifier(qualifier)) {
                    targets.add(r);
                }
            }
            if (targets.isEmpty()) {
                throw new SemanticAnalysisException(ErrorCode.RELATION_NOT_FOUND,
                        "Unknown table '" + String.join(".", qualifier) + "'");
            }
            if (targets.size() > 1) {
                throw new SemanticAnalysisException(ErrorCode.AMBIGUOUS_COLUMN,
                        "qualifier '" + String.join(".", qualifier) + "' matches several relations");
            }
        }
        List<ResolvedColumn> columns = new ArrayList<>();
        Set<String> unexpanded = new LinkedHashSet<>();
        int prefix = -1;
        for (RelationDescriptor r : targets) {
            if (prefix < 0 && !r.isPositionallyComplete()) {
                prefix = columns.size() + (r.isSchemaKnown() ? r.getReliablePrefix() : 0);
            }
            for (RelationField f : r.getFields()) {
                columns.add(new ResolvedColumn(ResolvedColumn.Kind.RELATION_FIELD, r, f, Collections.emptyList(),
                        0, false));
            }
            unexpanded.addAll(r.getUnexpandedStarSources());
        }
        return new StarExpansion(columns, prefix < 0 ? columns.size() : prefix, unexpanded);
    }

    @Override
    public String toString() {
        return "Scope{" + kind + "@" + level + ", relations=" + relations.size() + ", ctes=" + ctes.keySet()
                + ", lambda=" + lambdaParameters.keySet() + '}';
    }
}
