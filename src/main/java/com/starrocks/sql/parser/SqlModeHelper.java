package com.starrocks.sql.parser;

import java.util.Locale;

/**
 * Lightweight re-implementation of the sql_mode bit flags that the official StarRocks lexer consults.
 *
 * <p>The bit layout follows the MySQL-compatible layout used by {@code com.starrocks.qe.SqlModeHelper} in
 * StarRocks branch-3.1. Only {@link #MODE_PIPES_AS_CONCAT} influences lexing of the official grammar
 * ({@code ||} becomes {@code CONCAT}); the other common flags are kept so that {@link #encode(String)} can
 * accept a typical {@code sql_mode} string taken from a session configuration.</p>
 */
public final class SqlModeHelper {

    public static final long MODE_REAL_AS_FLOAT = 1L;
    public static final long MODE_PIPES_AS_CONCAT = 1L << 1;
    public static final long MODE_ANSI_QUOTES = 1L << 2;
    public static final long MODE_IGNORE_SPACE = 1L << 3;
    public static final long MODE_NOT_USED = 1L << 4;
    public static final long MODE_ONLY_FULL_GROUP_BY = 1L << 5;
    public static final long MODE_NO_UNSIGNED_SUBTRACTION = 1L << 6;
    public static final long MODE_NO_DIR_IN_CREATE = 1L << 7;
    public static final long MODE_NO_AUTO_VALUE_ON_ZERO = 1L << 19;
    public static final long MODE_NO_BACKSLASH_ESCAPES = 1L << 20;
    public static final long MODE_STRICT_TRANS_TABLES = 1L << 21;
    public static final long MODE_STRICT_ALL_TABLES = 1L << 22;
    public static final long MODE_NO_ZERO_IN_DATE = 1L << 23;
    public static final long MODE_NO_ZERO_DATE = 1L << 24;
    public static final long MODE_INVALID_DATES = 1L << 25;
    public static final long MODE_ERROR_FOR_DIVISION_BY_ZERO = 1L << 26;
    public static final long MODE_HIGH_NOT_PRECEDENCE = 1L << 29;
    public static final long MODE_NO_ENGINE_SUBSTITUTION = 1L << 30;
    public static final long MODE_PAD_CHAR_TO_FULL_LENGTH = 1L << 31;
    public static final long MODE_SORT_NULLS_LAST = 1L << 32;
    public static final long MODE_ERROR_IF_OVERFLOW = 1L << 33;

    /** StarRocks default: {@code ONLY_FULL_GROUP_BY}. */
    public static final long MODE_DEFAULT = MODE_ONLY_FULL_GROUP_BY;

    private static final String[] NAMES = {
        "REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NOT_USED", "ONLY_FULL_GROUP_BY",
        "NO_UNSIGNED_SUBTRACTION", "NO_DIR_IN_CREATE", "NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES",
        "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE", "NO_ZERO_DATE", "INVALID_DATES",
        "ERROR_FOR_DIVISION_BY_ZERO", "HIGH_NOT_PRECEDENCE", "NO_ENGINE_SUBSTITUTION", "PAD_CHAR_TO_FULL_LENGTH",
        "SORT_NULLS_LAST", "ERROR_IF_OVERFLOW"
    };

    private static final long[] VALUES = {
        MODE_REAL_AS_FLOAT, MODE_PIPES_AS_CONCAT, MODE_ANSI_QUOTES, MODE_IGNORE_SPACE, MODE_NOT_USED,
        MODE_ONLY_FULL_GROUP_BY, MODE_NO_UNSIGNED_SUBTRACTION, MODE_NO_DIR_IN_CREATE, MODE_NO_AUTO_VALUE_ON_ZERO,
        MODE_NO_BACKSLASH_ESCAPES, MODE_STRICT_TRANS_TABLES, MODE_STRICT_ALL_TABLES, MODE_NO_ZERO_IN_DATE,
        MODE_NO_ZERO_DATE, MODE_INVALID_DATES, MODE_ERROR_FOR_DIVISION_BY_ZERO, MODE_HIGH_NOT_PRECEDENCE,
        MODE_NO_ENGINE_SUBSTITUTION, MODE_PAD_CHAR_TO_FULL_LENGTH, MODE_SORT_NULLS_LAST, MODE_ERROR_IF_OVERFLOW
    };

    private SqlModeHelper() {
    }

    /**
     * Converts a textual sql_mode (e.g. {@code "PIPES_AS_CONCAT,ONLY_FULL_GROUP_BY"}) into its bit mask.
     * Unknown names are rejected so that a typo in configuration is detected at start-up instead of
     * silently changing the meaning of {@code ||}.
     *
     * @param sqlMode comma separated mode list; {@code null} or blank yields {@code 0}
     * @return bit mask
     * @throws IllegalArgumentException when an unknown mode name is supplied
     */
    public static long encode(String sqlMode) {
        if (sqlMode == null || sqlMode.trim().isEmpty()) {
            return 0L;
        }
        long result = 0L;
        for (String raw : sqlMode.split(",")) {
            String name = raw.trim().toUpperCase(Locale.ROOT);
            if (name.isEmpty()) {
                continue;
            }
            int idx = indexOf(name);
            if (idx < 0) {
                throw new IllegalArgumentException("Unknown sql_mode: " + raw.trim());
            }
            result |= VALUES[idx];
        }
        return result;
    }

    /**
     * Converts a bit mask back into the canonical comma separated textual representation.
     *
     * @param sqlMode bit mask
     * @return names in declaration order, empty string for {@code 0}
     */
    public static String decode(long sqlMode) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < VALUES.length; i++) {
            if ((sqlMode & VALUES[i]) != 0) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(NAMES[i]);
            }
        }
        return sb.toString();
    }

    /**
     * @param sqlMode bit mask
     * @return whether {@code ||} must be lexed as string concatenation instead of logical OR
     */
    public static boolean isPipesAsConcat(long sqlMode) {
        return (sqlMode & MODE_PIPES_AS_CONCAT) != 0;
    }

    private static int indexOf(String name) {
        for (int i = 0; i < NAMES.length; i++) {
            if (NAMES[i].equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
