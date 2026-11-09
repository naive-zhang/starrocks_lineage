package com.starrocks.lineage.scope;

import com.starrocks.lineage.metadata.ColumnMetadata;
import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.ColumnIdentifier;
import com.starrocks.lineage.model.Identifiers;
import com.starrocks.lineage.model.TableIdentifier;
import com.starrocks.lineage.model.TransformType;
import com.starrocks.lineage.scope.SemanticAnalysisException.ErrorCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Namespace entry of a FROM clause: an ordered list of {@link RelationField}s plus how the relation can be
 * referenced (alias or table name) and the physical columns that control its rows.
 *
 * <p><b>Completeness.</b> A relation is <i>complete</i> when its full column list is known. It is incomplete when
 * the schema of a physical table could not be loaded ({@code schemaKnown == false}) or when a derived relation was
 * built from {@code SELECT *} over such a table. {@link #getReliablePrefix()} is the number of leading fields whose
 * positions are trustworthy; it equals {@link #size()} for complete relations. Positional consumers (set
 * operations, {@code INSERT} without column list, column alias lists) must only rely on that prefix.</p>
 *
 * <p><b>Open relations.</b> A relation with an unknown schema still accepts column references: a referenced
 * column is created on demand ({@link #onDemandField(String)}) with the lineage the relation would give it
 * (physical {@code table.column} for tables, the union of the arguments for table functions).</p>
 *
 * <p>Instances are immutable except for the private memo of on-demand fields, which only makes repeated lookups
 * return the same object. A descriptor belongs to the analysis of one statement and is not shared across
 * threads.</p>
 */
public final class RelationDescriptor {

    private final RelationKind kind;
    private final String alias;
    private final TableIdentifier table;
    private final String functionName;
    private final List<RelationField> fields;
    private final Set<ColumnIdentifier> controlSources;
    private final boolean schemaKnown;
    private final int reliablePrefix;
    private final boolean arityKnown;
    private final Set<String> unexpandedStarSources;
    private final FieldLineage openLineage;
    private final Map<String, RelationField> onDemand = new HashMap<>();

    private RelationDescriptor(RelationKind kind, String alias, TableIdentifier table, String functionName,
                               List<RelationField> fields, Collection<ColumnIdentifier> controlSources,
                               boolean schemaKnown, int reliablePrefix, boolean arityKnown,
                               Collection<String> unexpandedStarSources, FieldLineage openLineage) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.alias = alias == null || alias.trim().isEmpty() ? null : Identifiers.unquote(alias);
        this.table = table;
        this.functionName = functionName;
        List<RelationField> indexed = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            indexed.add(fields.get(i).withIndex(i));
        }
        this.fields = Collections.unmodifiableList(indexed);
        this.controlSources = Collections.unmodifiableSet(new LinkedHashSet<>(controlSources));
        this.schemaKnown = schemaKnown;
        if (reliablePrefix < 0 || reliablePrefix > indexed.size()) {
            throw new IllegalArgumentException("reliablePrefix " + reliablePrefix + " outside 0.." + indexed.size());
        }
        this.reliablePrefix = reliablePrefix;
        this.arityKnown = schemaKnown && arityKnown;
        this.unexpandedStarSources = Collections.unmodifiableSet(new LinkedHashSet<>(unexpandedStarSources));
        this.openLineage = openLineage;
        if (kind == RelationKind.TABLE && table == null) {
            throw new IllegalArgumentException("a TABLE relation needs a table identifier");
        }
    }

    // ------------------------------------------------------------------------------------------------ factories

    /**
     * Physical table / view. With a schema the relation is complete; without one it is open and every referenced
     * column becomes {@code table.column}.
     */
    public static RelationDescriptor table(TableIdentifier table, String alias, Optional<TableSchema> schema) {
        Objects.requireNonNull(table, "table");
        if (schema.isPresent()) {
            List<RelationField> fields = new ArrayList<>();
            for (ColumnMetadata c : schema.get().getColumns()) {
                fields.add(new RelationField(c.getName(), c.getOrdinal(),
                        FieldLineage.physical(ColumnIdentifier.of(table, c.getName())), c.getName()));
            }
            return new RelationDescriptor(RelationKind.TABLE, alias, table, null, fields, Collections.emptySet(),
                    true, fields.size(), true, Collections.emptySet(), null);
        }
        return new RelationDescriptor(RelationKind.TABLE, alias, table, null, Collections.emptyList(),
                Collections.emptySet(), false, 0, false, Collections.singleton(table.toQualifiedString()), null);
    }

    /**
     * Relation derived from a query (sub-query, CTE body, query output).
     *
     * @param reliablePrefix       number of leading fields with trustworthy positions; the arity is considered
     *                             known only when there is no unexpanded source and the prefix covers every field
     * @param unexpandedStarSources relations (qualified table name or table-function alias) whose {@code *} could
     *                              not be expanded inside the query
     */
    public static RelationDescriptor derived(RelationKind kind, String alias, List<RelationField> fields,
                                             Collection<ColumnIdentifier> controlSources, int reliablePrefix,
                                             Collection<String> unexpandedStarSources) {
        if (kind == RelationKind.TABLE) {
            throw new IllegalArgumentException("use table(...) for physical tables");
        }
        return new RelationDescriptor(kind, alias, null, null, fields, controlSources,
                true, reliablePrefix, unexpandedStarSources.isEmpty() && reliablePrefix == fields.size(),
                unexpandedStarSources, null);
    }

    /** Complete derived relation. */
    public static RelationDescriptor derived(RelationKind kind, String alias, List<RelationField> fields,
                                             Collection<ColumnIdentifier> controlSources) {
        return derived(kind, alias, fields, controlSources, fields.size(), Collections.emptySet());
    }

    /**
     * Inline {@code VALUES} table. Default column names are {@code column_0 .. column_n-1} like StarRocks.
     *
     * @param columnLineages lineage per column (usually constant; scalar sub-queries can carry sources)
     */
    public static RelationDescriptor values(String alias, List<String> columnAliases,
                                            List<FieldLineage> columnLineages) {
        int arity = columnLineages.size();
        if (columnAliases != null && !columnAliases.isEmpty() && columnAliases.size() != arity) {
            throw new SemanticAnalysisException(ErrorCode.COLUMN_ALIAS_MISMATCH,
                    "VALUES has " + arity + " columns but " + columnAliases.size() + " aliases");
        }
        List<RelationField> fields = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            String name = columnAliases == null || columnAliases.isEmpty() ? "column_" + i : columnAliases.get(i);
            fields.add(new RelationField(name, i, columnLineages.get(i), null));
        }
        return new RelationDescriptor(RelationKind.VALUES, alias, null, null, fields, Collections.emptySet(),
                true, arity, true, Collections.emptySet(), null);
    }

    /**
     * Table function in FROM.
     * <ul>
     *   <li>{@code unnest(a1, ..., an)} returns n columns, all named {@code unnest} by default (StarRocks 3.1
     *       {@code QueryAnalyzer#visitTableFunction}); column i is element extraction of argument i, so its lineage
     *       is the lineage of argument i elevated to {@link TransformType#EXPRESSION}.</li>
     *   <li>Other functions: with a column alias list the arity is known and every column derives from all
     *       arguments; without it the relation is open and on-demand columns derive from all arguments.</li>
     * </ul>
     * The default alias is {@code table_function_<name>} as in StarRocks.
     */
    public static RelationDescriptor tableFunction(String functionName, String alias, List<String> columnAliases,
                                                   List<FieldLineage> argumentLineages) {
        Objects.requireNonNull(functionName, "functionName");
        String fn = Identifiers.unquote(functionName).toLowerCase(Locale.ROOT);
        String effectiveAlias = alias == null || alias.trim().isEmpty() ? "table_function_" + fn : alias;
        boolean hasAliases = columnAliases != null && !columnAliases.isEmpty();
        FieldLineage all = FieldLineage.constant();
        for (FieldLineage l : argumentLineages) {
            all = all.union(l);
        }
        all = all.elevate(TransformType.EXPRESSION);

        List<RelationField> fields = new ArrayList<>();
        if ("unnest".equals(fn)) {
            int arity = argumentLineages.size();
            if (hasAliases && columnAliases.size() != arity) {
                throw new SemanticAnalysisException(ErrorCode.COLUMN_ALIAS_MISMATCH,
                        "unnest has " + arity + " columns available but " + columnAliases.size() + " specified");
            }
            for (int i = 0; i < arity; i++) {
                fields.add(new RelationField(hasAliases ? columnAliases.get(i) : "unnest", i,
                        argumentLineages.get(i).elevate(TransformType.EXPRESSION), null));
            }
        } else if (hasAliases) {
            for (int i = 0; i < columnAliases.size(); i++) {
                fields.add(new RelationField(columnAliases.get(i), i, all, null));
            }
        } else {
            return new RelationDescriptor(RelationKind.TABLE_FUNCTION, effectiveAlias, null, fn,
                    Collections.emptyList(), Collections.emptySet(), false, 0, false,
                    Collections.singleton(Identifiers.unquote(effectiveAlias)), all);
        }
        return new RelationDescriptor(RelationKind.TABLE_FUNCTION, effectiveAlias, null, fn, fields,
                Collections.emptySet(), true, fields.size(), true, Collections.emptySet(), null);
    }

    /**
     * Positional merge of set-operation branches ({@code UNION [ALL] / INTERSECT / EXCEPT / MINUS}).
     *
     * <ul>
     *   <li>Output names come from the first branch (SQL standard and StarRocks).</li>
     *   <li>Output field {@code i} has the union of the lineage of field {@code i} of every branch, regardless
     *       of the branch column names.</li>
     *   <li>Positionally complete branches with different arities: {@link ErrorCode#SET_OPERATION_ARITY_MISMATCH}
     *       - the statement succeeded in StarRocks, so the metadata used for {@code *} expansion is stale.</li>
     *   <li>Some branch positionally incomplete: the arity is taken from the positionally complete branches (or
     *       the largest reliable prefix when there is none), positions beyond a branch's reliable prefix are not
     *       merged for that branch, and lineage completeness is propagated through the unexpanded sources.</li>
     * </ul>
     * Control sources of all branches are united.
     */
    public static RelationDescriptor setOperation(List<RelationDescriptor> branches) {
        if (branches == null || branches.size() < 2) {
            throw new IllegalArgumentException("a set operation needs at least two branches");
        }
        Integer completeArity = null;
        int maxPrefix = 0;
        for (RelationDescriptor b : branches) {
            if (b.isPositionallyComplete()) {
                if (completeArity == null) {
                    completeArity = b.size();
                } else if (completeArity != b.size()) {
                    throw new SemanticAnalysisException(ErrorCode.SET_OPERATION_ARITY_MISMATCH,
                            "set operation branches have " + completeArity + " and " + b.size() + " columns");
                }
            }
            maxPrefix = Math.max(maxPrefix, b.getReliablePrefix());
        }
        int arity = completeArity != null ? completeArity : maxPrefix;
        if (completeArity != null && maxPrefix > completeArity) {
            throw new SemanticAnalysisException(ErrorCode.SET_OPERATION_ARITY_MISMATCH,
                    "set operation branch has at least " + maxPrefix + " columns but " + completeArity
                            + " expected");
        }
        RelationDescriptor nameSource = null;
        for (RelationDescriptor b : branches) {
            if (b.getReliablePrefix() == arity) {
                nameSource = b;
                break;
            }
        }
        RelationDescriptor first = branches.get(0);
        List<RelationField> out = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            FieldLineage merged = FieldLineage.constant();
            String expr = null;
            for (RelationDescriptor b : branches) {
                if (i < b.getReliablePrefix()) {
                    RelationField f = b.fields.get(i);
                    merged = merged.union(f.getLineage());
                    if (expr == null) {
                        expr = f.getExpression();
                    }
                }
            }
            String name = i < first.getReliablePrefix() ? first.fields.get(i).getName()
                    : nameSource != null ? nameSource.fields.get(i).getName() : "column_" + i;
            out.add(new RelationField(name, i, merged, expr));
        }
        Set<ColumnIdentifier> control = new LinkedHashSet<>();
        Set<String> stars = new LinkedHashSet<>();
        for (RelationDescriptor b : branches) {
            control.addAll(b.controlSources);
            stars.addAll(b.unexpandedStarSources);
        }
        return new RelationDescriptor(RelationKind.SET_OPERATION, null, null, null, out, control, true,
                completeArity != null ? arity : maxPrefix, completeArity != null, stars, null);
    }

    // ------------------------------------------------------------------------------------------------ derivations

    /** @return a copy referenced through a new alias ({@code FROM cte AS c}, {@code (...) t}) */
    public RelationDescriptor withAlias(String newAlias) {
        return new RelationDescriptor(kind, newAlias, table, functionName, fields, controlSources, schemaKnown,
                reliablePrefix, arityKnown, unexpandedStarSources, openLineage);
    }

    /** @return a copy whose kind is changed (e.g. a query output registered as a CTE) */
    public RelationDescriptor withKind(RelationKind newKind) {
        if (newKind == RelationKind.TABLE || kind == RelationKind.TABLE) {
            throw new IllegalArgumentException("cannot convert from/to TABLE");
        }
        return new RelationDescriptor(newKind, alias, table, functionName, fields, controlSources, schemaKnown,
                reliablePrefix, arityKnown, unexpandedStarSources, openLineage);
    }

    /**
     * Renames fields positionally ({@code t(a, b)}, {@code WITH c(a, b) AS}, {@code CREATE VIEW v(a, b)}).
     * For a complete relation the alias count must equal the arity. For an incomplete relation the alias list
     * defines the arity: fields beyond the reliable prefix get an empty lineage (their source column cannot be
     * identified) and the relation stays flagged through {@link #getUnexpandedStarSources()}.
     */
    public RelationDescriptor withColumnAliases(List<String> aliases) {
        if (aliases == null || aliases.isEmpty()) {
            return this;
        }
        if (!schemaKnown) {
            throw new SemanticAnalysisException(ErrorCode.COLUMN_ALIAS_MISMATCH,
                    "column aliases on a relation with unknown schema: " + displayName());
        }
        if (isPositionallyComplete() && aliases.size() != fields.size()) {
            throw new SemanticAnalysisException(ErrorCode.COLUMN_ALIAS_MISMATCH,
                    displayName() + " has " + fields.size() + " columns but " + aliases.size() + " aliases");
        }
        if (!isPositionallyComplete() && aliases.size() < reliablePrefix) {
            throw new SemanticAnalysisException(ErrorCode.COLUMN_ALIAS_MISMATCH,
                    displayName() + " has at least " + reliablePrefix + " columns but " + aliases.size() + " aliases");
        }
        List<RelationField> renamed = new ArrayList<>(aliases.size());
        for (int i = 0; i < aliases.size(); i++) {
            if (i < reliablePrefix) {
                renamed.add(fields.get(i).withName(aliases.get(i)));
            } else {
                renamed.add(new RelationField(aliases.get(i), i, FieldLineage.constant(), null));
            }
        }
        return new RelationDescriptor(kind, alias, table, functionName, renamed, controlSources, true,
                aliases.size(), true, unexpandedStarSources, null);
    }

    /** @return a copy with additional row-control sources */
    public RelationDescriptor withControlSources(Collection<ColumnIdentifier> extra) {
        if (extra.isEmpty()) {
            return this;
        }
        Set<ColumnIdentifier> c = new LinkedHashSet<>(controlSources);
        c.addAll(extra);
        return new RelationDescriptor(kind, alias, table, functionName, fields, c, schemaKnown, reliablePrefix,
                arityKnown, unexpandedStarSources, openLineage);
    }

    // ------------------------------------------------------------------------------------------------ lookup

    /** @return every field with that name (several for {@code SELECT a, a}) */
    public List<RelationField> lookup(String name) {
        List<RelationField> result = new ArrayList<>(1);
        for (RelationField f : fields) {
            if (f.hasName(name)) {
                result.add(f);
            }
        }
        return result;
    }

    /**
     * On-demand field of an open relation.
     *
     * @return empty when the relation is complete (an unknown column of a known schema does not exist)
     */
    public Optional<RelationField> onDemandField(String name) {
        if (schemaKnown) {
            return Optional.empty();
        }
        String n = Identifiers.normalizeColumn(name);
        RelationField f = onDemand.computeIfAbsent(n, key -> {
            FieldLineage lineage = table != null ? FieldLineage.physical(ColumnIdentifier.of(table, key))
                    : openLineage;
            return new RelationField(key, fields.size() + onDemand.size(), lineage, key);
        });
        return Optional.of(f);
    }

    /**
     * Qualifier matching for {@code q.col} / {@code q.*}, following StarRocks {@code Field#matchesPrefix}:
     * an aliased relation is only reachable through its alias (one part); an unaliased physical table through
     * {@code table}, {@code db.table} or {@code catalog.db.table}. Comparison is case-insensitive.
     */
    public boolean matchesQualifier(List<String> qualifier) {
        if (qualifier == null || qualifier.isEmpty()) {
            return false;
        }
        if (alias != null) {
            return qualifier.size() == 1 && alias.equalsIgnoreCase(Identifiers.unquote(qualifier.get(0)));
        }
        if (table == null) {
            return false;
        }
        String[] full = {table.getCatalog(), table.getDatabase(), table.getTable()};
        if (qualifier.size() > 3) {
            return false;
        }
        int offset = 3 - qualifier.size();
        for (int i = 0; i < qualifier.size(); i++) {
            if (!full[offset + i].equalsIgnoreCase(Identifiers.unquote(qualifier.get(i)))) {
                return false;
            }
        }
        return true;
    }

    /** @return the name under which the relation is referenced (alias, table name) for duplicate detection */
    public Optional<String> referenceName() {
        if (alias != null) {
            return Optional.of(alias.toLowerCase(Locale.ROOT));
        }
        if (table != null) {
            return Optional.of(table.toQualifiedString().toLowerCase(Locale.ROOT));
        }
        return Optional.empty();
    }

    // ------------------------------------------------------------------------------------------------ getters

    public RelationKind getKind() {
        return kind;
    }

    public Optional<String> getAlias() {
        return Optional.ofNullable(alias);
    }

    public Optional<TableIdentifier> getTable() {
        return Optional.ofNullable(table);
    }

    public Optional<String> getFunctionName() {
        return Optional.ofNullable(functionName);
    }

    public List<RelationField> getFields() {
        return fields;
    }

    public RelationField field(int index) {
        return fields.get(index);
    }

    public int size() {
        return fields.size();
    }

    public Set<ColumnIdentifier> getControlSources() {
        return controlSources;
    }

    /** @return {@code false} for an open relation (table without metadata, aliasless table function) */
    public boolean isSchemaKnown() {
        return schemaKnown;
    }

    /**
     * @return {@code true} when every position is known (the arity is reliable) even if the lineage of some
     *         position is missing; this is what positional consumers (set operations, star expansion, INSERT
     *         without column list) need
     */
    public boolean isPositionallyComplete() {
        return arityKnown && reliablePrefix == fields.size();
    }

    /**
     * @return {@code true} when the number of columns is known: physical table with schema, aliased table
     *         function, VALUES, any relation renamed by a column alias list, a derived relation without unexpanded
     *         {@code *}, or a set operation with at least one such branch
     */
    public boolean isArityKnown() {
        return arityKnown;
    }

    /** @return {@code true} when every field, its position and its lineage are known */
    public boolean isComplete() {
        return isPositionallyComplete() && unexpandedStarSources.isEmpty();
    }

    public int getReliablePrefix() {
        return reliablePrefix;
    }

    /**
     * @return relations with unknown schema whose {@code *} could not be expanded somewhere inside this relation
     *         (qualified table names, or table-function aliases); empty for a complete relation
     */
    public Set<String> getUnexpandedStarSources() {
        return unexpandedStarSources;
    }

    /** @return human readable name for messages */
    public String displayName() {
        if (alias != null) {
            return alias;
        }
        if (table != null) {
            return table.toQualifiedString();
        }
        return kind.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return "RelationDescriptor{" + kind + " " + displayName() + " " + fields
                + (isComplete() ? "" : " incomplete(prefix=" + reliablePrefix + ")") + '}';
    }
}
