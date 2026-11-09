package com.starrocks.lineage.scope;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Result of expanding {@code *} or {@code q.*} in the current query block.
 *
 * <p>{@link #getColumns()} lists the expanded columns in StarRocks order: visible relations in FROM order, each
 * relation's fields in declaration order; both sides of a {@code JOIN ... USING} are emitted (StarRocks 3.1 rewrites
 * USING into an ON equality and does not coalesce). When a relation with unknown schema is part of the expansion
 * the result is incomplete: {@link #getReliablePrefix()} columns precede the first unknown relation and are
 * positionally reliable.</p>
 */
public final class StarExpansion {

    private final List<ResolvedColumn> columns;
    private final int reliablePrefix;
    private final Set<String> unexpandedTables;

    StarExpansion(List<ResolvedColumn> columns, int reliablePrefix, Set<String> unexpandedTables) {
        this.columns = Collections.unmodifiableList(columns);
        this.reliablePrefix = reliablePrefix;
        this.unexpandedTables = Collections.unmodifiableSet(new LinkedHashSet<>(unexpandedTables));
    }

    public List<ResolvedColumn> getColumns() {
        return columns;
    }

    public int getReliablePrefix() {
        return reliablePrefix;
    }

    public Set<String> getUnexpandedTables() {
        return unexpandedTables;
    }

    public boolean isComplete() {
        return unexpandedTables.isEmpty();
    }
}
