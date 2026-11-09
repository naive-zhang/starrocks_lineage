package com.starrocks.lineage.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Name normalisation rules shared by every identifier type. Centralised so that the parser, the metadata
 * provider and the storage layer can never disagree on how a name is spelled (which would split one physical
 * column into several aggregate rows).
 *
 * <ul>
 *   <li>Catalog names: case-insensitive in StarRocks - lower-cased.</li>
 *   <li>Database and table names: case-sensitive in StarRocks - kept verbatim (only unquoted).</li>
 *   <li>Column names: case-insensitive in StarRocks - lower-cased.</li>
 * </ul>
 */
public final class Identifiers {

    /** Catalog used when the audit record carries no catalog. */
    public static final String DEFAULT_CATALOG = "default_catalog";

    private Identifiers() {
    }

    /**
     * Removes surrounding backquotes and un-escapes doubled backquotes ({@code `a``b`} becomes {@code a`b}).
     *
     * @param name raw identifier text, may be {@code null}
     * @return unquoted name, {@code null} stays {@code null}
     */
    public static String unquote(String name) {
        if (name == null) {
            return null;
        }
        String trimmed = name.trim();
        if (trimmed.length() >= 2 && trimmed.charAt(0) == '`' && trimmed.charAt(trimmed.length() - 1) == '`') {
            return trimmed.substring(1, trimmed.length() - 1).replace("``", "`");
        }
        return trimmed;
    }

    public static String normalizeCatalog(String catalog) {
        String c = unquote(catalog);
        if (c == null || c.isEmpty()) {
            return DEFAULT_CATALOG;
        }
        return c.toLowerCase(Locale.ROOT);
    }

    public static String normalizeDatabase(String db) {
        return unquote(db);
    }

    public static String normalizeTable(String table) {
        return unquote(table);
    }

    public static String normalizeColumn(String column) {
        String c = unquote(column);
        return c == null ? null : c.toLowerCase(Locale.ROOT);
    }

    /**
     * Splits a dotted, possibly backquoted, qualified name: {@code hive.`my.db`.t} gives
     * {@code [hive, my.db, t]}.
     *
     * @param qualifiedName qualified name text
     * @return unquoted parts
     * @throws IllegalArgumentException for an empty part or an unterminated backquote
     */
    public static List<String> splitQualifiedName(String qualifiedName) {
        if (qualifiedName == null || qualifiedName.trim().isEmpty()) {
            throw new IllegalArgumentException("qualified name must not be empty");
        }
        String s = qualifiedName.trim();
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '`') {
                if (quoted && i + 1 < s.length() && s.charAt(i + 1) == '`') {
                    current.append('`');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (c == '.' && !quoted) {
                addPart(parts, current, qualifiedName);
            } else {
                current.append(c);
            }
        }
        if (quoted) {
            throw new IllegalArgumentException("unterminated backquote in: " + qualifiedName);
        }
        addPart(parts, current, qualifiedName);
        return Collections.unmodifiableList(parts);
    }

    private static void addPart(List<String> parts, StringBuilder current, String source) {
        String part = current.toString().trim();
        if (part.isEmpty()) {
            throw new IllegalArgumentException("empty name part in: " + source);
        }
        parts.add(part);
        current.setLength(0);
    }

    /**
     * Quotes a name with backquotes for safe embedding into generated SQL.
     *
     * @param name unquoted name
     * @return backquoted name with inner backquotes doubled
     */
    public static String quote(String name) {
        return '`' + name.replace("`", "``") + '`';
    }
}
