package com.starrocks.qe;

/**
 * Binary-compatibility shim for the official StarRocks branch-3.1 {@code StarRocksLex.g4}.
 *
 * <p>The official lexer grammar embeds the following Java code verbatim:</p>
 * <pre>
 * private long sqlMode = com.starrocks.qe.SqlModeHelper.MODE_DEFAULT;
 * LOGICAL_OR: '||' {setType((sqlMode &amp; com.starrocks.qe.SqlModeHelper.MODE_PIPES_AS_CONCAT) == 0 ? ...);};
 * </pre>
 * <p>Because it uses fully qualified names, the generated lexer only compiles when this exact class exists.
 * All values are delegated to the canonical implementation {@link com.starrocks.sql.parser.SqlModeHelper}
 * so there is a single source of truth for the bit layout.</p>
 */
public final class SqlModeHelper {

    public static final long MODE_DEFAULT = com.starrocks.sql.parser.SqlModeHelper.MODE_DEFAULT;
    public static final long MODE_PIPES_AS_CONCAT = com.starrocks.sql.parser.SqlModeHelper.MODE_PIPES_AS_CONCAT;
    public static final long MODE_ANSI_QUOTES = com.starrocks.sql.parser.SqlModeHelper.MODE_ANSI_QUOTES;
    public static final long MODE_ONLY_FULL_GROUP_BY = com.starrocks.sql.parser.SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
    public static final long MODE_NO_BACKSLASH_ESCAPES =
            com.starrocks.sql.parser.SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES;
    public static final long MODE_SORT_NULLS_LAST = com.starrocks.sql.parser.SqlModeHelper.MODE_SORT_NULLS_LAST;

    private SqlModeHelper() {
    }

    /**
     * @see com.starrocks.sql.parser.SqlModeHelper#encode(String)
     */
    public static long encode(String sqlMode) {
        return com.starrocks.sql.parser.SqlModeHelper.encode(sqlMode);
    }

    /**
     * @see com.starrocks.sql.parser.SqlModeHelper#decode(long)
     */
    public static String decode(long sqlMode) {
        return com.starrocks.sql.parser.SqlModeHelper.decode(sqlMode);
    }
}
