package com.starrocks.lineage.scope;

/**
 * Whether and with which priority select-list aliases of the current query block are visible.
 *
 * <ul>
 *   <li>{@link #NONE}: SELECT list, WHERE, JOIN ON, window clauses - aliases are not visible.</li>
 *   <li>{@link #PREFER_INPUT}: GROUP BY / HAVING - an input column wins over an output alias of the same name
 *       (MySQL / StarRocks behaviour); the alias is used only when no input column matches.</li>
 *   <li>{@link #PREFER_ALIAS}: ORDER BY - an output alias wins over an input column (SQL standard).</li>
 * </ul>
 * Aliases are visible only for unqualified, single-part names and only in the innermost query block. The
 * precedence follows the MySQL rule StarRocks is compatible with: "an unqualified reference in ORDER BY is searched
 * in the select expressions first, then in the FROM columns; for GROUP BY and HAVING the FROM clause is searched
 * first".
 */
public enum AliasPolicy {
    NONE,
    PREFER_INPUT,
    PREFER_ALIAS
}
