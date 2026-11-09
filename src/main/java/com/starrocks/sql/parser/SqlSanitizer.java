package com.starrocks.sql.parser;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Cleans the raw {@code stmt} column of {@code starrocks_audit_db__.starrocks_audit_tbl__} before parsing.
 *
 * <p>Responsibilities:</p>
 * <ol>
 *   <li>Remove driver / tool generated comments such as {@code /* ApplicationName=DBeaver 23.1 - Main *&#47;}
 *       and any other {@code /* ... *&#47;}, {@code -- ...} or MySQL style {@code # ...} comments. The official
 *       lexer has no rule for {@code #}, so leaving such a comment in place turns a valid statement into a
 *       syntax error. Optimizer hints ({@code /*+ ... *&#47;}) are <b>kept</b> because they are part of the
 *       statement and are routed by the official lexer to channel 2.</li>
 *   <li>Never touch the content of string literals ({@code '...'}, {@code "..."}) or backquoted identifiers,
 *       including escaped quotes ({@code \'}, {@code ''}) - a {@code '--'} inside a literal is not a comment.</li>
 *   <li>Replace each removed comment by one space so that {@code a/*x*&#47;b} cannot glue into {@code ab}.</li>
 *   <li>Trim surrounding whitespace and trailing semicolons.</li>
 *   <li>Classify the statement by its leading keyword so management statements
 *       ({@code SHOW / DESC / SET / USE / ADMIN / EXPLAIN / KILL ...}) are skipped without paying for a parse.</li>
 * </ol>
 * <p>The class is stateless and thread-safe.</p>
 */
public final class SqlSanitizer {

    /**
     * Leading keywords of statements that never produce lineage or column usage and must be skipped.
     */
    public static final Set<String> MANAGEMENT_KEYWORDS;

    static {
        Set<String> s = new HashSet<>();
        Collections.addAll(s,
                "SHOW", "DESC", "DESCRIBE", "SET", "USE", "ADMIN", "EXPLAIN", "KILL",
                "HELP", "GRANT", "REVOKE", "BEGIN", "START", "COMMIT", "ROLLBACK", "TRACE",
                "ANALYZE", "CANCEL", "RECOVER", "INSTALL", "UNINSTALL", "SYNC", "EXECUTE", "PREPARE",
                "DEALLOCATE", "LOCK", "UNLOCK", "BACKUP", "RESTORE", "REFRESH", "PAUSE", "RESUME", "STOP",
                "CLEAN", "SYSTEM");
        MANAGEMENT_KEYWORDS = Collections.unmodifiableSet(s);
    }

    private SqlSanitizer() {
    }

    /**
     * Sanitizes one audit statement.
     *
     * @param rawStmt the raw {@code stmt} value, may be {@code null}
     * @return the sanitized statement and its classification, never {@code null}
     */
    public static SanitizedSql sanitize(String rawStmt) {
        if (rawStmt == null) {
            return new SanitizedSql(null, "", "", 0);
        }
        String input = rawStmt;
        if (!input.isEmpty() && input.charAt(0) == '﻿') {
            input = input.substring(1);
        }
        StringBuilder out = new StringBuilder(input.length());
        int removed = 0;
        int i = 0;
        final int n = input.length();
        while (i < n) {
            char c = input.charAt(i);
            char next = i + 1 < n ? input.charAt(i + 1) : '\0';
            if (c == '\'' || c == '"' || c == '`') {
                i = copyQuoted(input, i, c, out);
            } else if (c == '/' && next == '*') {
                boolean hint = i + 2 < n && input.charAt(i + 2) == '+';
                int end = input.indexOf("*/", i + 2);
                int stop = end < 0 ? n : end + 2;
                if (hint) {
                    out.append(input, i, stop);
                } else {
                    appendSeparator(out);
                    removed++;
                }
                i = stop;
            } else if (c == '-' && next == '-') {
                i = skipToLineEnd(input, i);
                appendSeparator(out);
                removed++;
            } else if (c == '#') {
                i = skipToLineEnd(input, i);
                appendSeparator(out);
                removed++;
            } else {
                out.append(c);
                i++;
            }
        }
        String sql = trimTrailingSemicolons(out.toString().trim());
        return new SanitizedSql(rawStmt, sql, leadingKeyword(sql), removed);
    }

    /**
     * Extracts the first keyword of a sanitized statement, skipping leading parentheses (e.g.
     * {@code (SELECT ...) UNION (SELECT ...)}) and optimizer hints.
     *
     * @param sql sanitized SQL
     * @return upper-cased keyword or an empty string
     */
    static String leadingKeyword(String sql) {
        int i = 0;
        final int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c) || c == '(' || c == '　') {
                i++;
            } else if (c == '/' && i + 2 < n && sql.charAt(i + 1) == '*' && sql.charAt(i + 2) == '+') {
                int end = sql.indexOf("*/", i + 3);
                i = end < 0 ? n : end + 2;
            } else {
                break;
            }
        }
        int start = i;
        while (i < n && (Character.isLetter(sql.charAt(i)) || sql.charAt(i) == '_')) {
            i++;
        }
        return sql.substring(start, i).toUpperCase(Locale.ROOT);
    }

    private static int copyQuoted(String s, int start, char quote, StringBuilder out) {
        final int n = s.length();
        out.append(quote);
        int i = start + 1;
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\\' && quote != '`' && i + 1 < n) {
                out.append(c).append(s.charAt(i + 1));
                i += 2;
                continue;
            }
            out.append(c);
            i++;
            if (c == quote) {
                if (i < n && s.charAt(i) == quote) {
                    // doubled quote = escaped quote, stay inside the literal
                    out.append(quote);
                    i++;
                } else {
                    return i;
                }
            }
        }
        return i;
    }

    private static int skipToLineEnd(String s, int start) {
        int i = start;
        while (i < s.length() && s.charAt(i) != '\n' && s.charAt(i) != '\r') {
            i++;
        }
        return i;
    }

    private static void appendSeparator(StringBuilder out) {
        if (out.length() > 0 && !Character.isWhitespace(out.charAt(out.length() - 1))) {
            out.append(' ');
        }
    }

    private static String trimTrailingSemicolons(String sql) {
        int end = sql.length();
        while (end > 0 && (sql.charAt(end - 1) == ';' || Character.isWhitespace(sql.charAt(end - 1)))) {
            end--;
        }
        return sql.substring(0, end);
    }

    /**
     * Immutable result of {@link #sanitize(String)}.
     */
    public static final class SanitizedSql {
        private final String original;
        private final String sql;
        private final String leadingKeyword;
        private final int removedComments;

        SanitizedSql(String original, String sql, String leadingKeyword, int removedComments) {
            this.original = original;
            this.sql = Objects.requireNonNull(sql);
            this.leadingKeyword = Objects.requireNonNull(leadingKeyword);
            this.removedComments = removedComments;
        }

        /** @return raw statement exactly as stored in the audit table (may be {@code null}) */
        public String getOriginal() {
            return original;
        }

        /** @return statement with comments removed, trimmed, without trailing semicolons */
        public String getSql() {
            return sql;
        }

        /** @return upper-cased leading keyword, e.g. {@code SELECT}, {@code WITH}, {@code INSERT} */
        public String getLeadingKeyword() {
            return leadingKeyword;
        }

        /** @return number of comments removed */
        public int getRemovedComments() {
            return removedComments;
        }

        /** @return {@code true} when nothing but whitespace / comments remained */
        public boolean isEmpty() {
            return sql.isEmpty();
        }

        /** @return {@code true} when the statement is a management statement that must be skipped */
        public boolean isManagementStatement() {
            return MANAGEMENT_KEYWORDS.contains(leadingKeyword);
        }

        @Override
        public String toString() {
            return "SanitizedSql{keyword=" + leadingKeyword + ", sql=" + sql + '}';
        }
    }
}
