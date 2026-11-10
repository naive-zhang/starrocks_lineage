package com.starrocks.lineage.production;

import com.starrocks.lineage.metadata.ColumnMetadata;
import com.starrocks.lineage.metadata.InMemoryMetadataProvider;
import com.starrocks.lineage.metadata.TableSchema;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ColumnLineageEdge;
import com.starrocks.lineage.model.LineageType;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.model.NamespaceContext;
import com.starrocks.lineage.model.TableIdentifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Production data provided for the lineage acceptance tests (test resources {@code production/}):
 * <ul>
 *   <li>{@code schema_lbdb_20261110.sql.gz}: {@code SHOW CREATE TABLE} of the 997 base tables of the production
 *       StarRocks 3.1.17 cluster (dperm 20, eagle 3, lbdb 955, lbdb_uat 17, test 2), exported 2026-11-10;</li>
 *   <li>{@code inserts_30.sql}: 30 INSERT statements of DolphinScheduler project 8655547341888 taken from the task
 *       logs, each block {@code -- [NN] target} followed by its comments and the statement, verbatim.</li>
 * </ul>
 * Both files are kept byte-for-byte as delivered; this class only reads them.
 */
public final class ProductionFixture {

    public static final String SCHEMA_RESOURCE = "/production/schema_lbdb_20261110.sql.gz";
    public static final String STATEMENTS_RESOURCE = "/production/inserts_30.sql";
    public static final String GOLDEN = "/production/inserts_30.lineage";
    /** The JDBC data source of the DolphinScheduler tasks connects to {@code lbdb}. */
    public static final NamespaceContext NAMESPACE = NamespaceContext.of("default_catalog", "lbdb");

    private static final Pattern COLUMN = Pattern.compile("^  `([^`]+)` (\\S+)(.*)$");
    private static final Pattern MARKER = Pattern.compile("(?m)^-- \\[(\\d\\d)\\] (\\S+)[ \\t]*$");
    private static final Pattern TASK_INSTANCE = Pattern.compile("任务实例 ID (\\d+)");

    private static Schema schema;
    private static List<Statement> statements;

    private ProductionFixture() {
    }

    /** @return the parsed production schema (cached; parsing takes about a second) */
    public static synchronized Schema schema() {
        if (schema == null) {
            schema = parseSchema();
        }
        return schema;
    }

    /** @return the 30 statements in file order */
    public static synchronized List<Statement> statements() {
        if (statements == null) {
            statements = parseStatements(read(STATEMENTS_RESOURCE, false));
        }
        return statements;
    }

    // ------------------------------------------------------------------------------------------------ schema

    private static Schema parseSchema() {
        Map<TableIdentifier, TableSchema> tables = new LinkedHashMap<>();
        Map<TableIdentifier, String> ddl = new LinkedHashMap<>();
        Map<String, Integer> perDatabase = new LinkedHashMap<>();
        String database = null;
        String pendingName = null;
        String table = null;
        List<ColumnMetadata> columns = null;
        StringBuilder text = null;
        for (String line : lines(read(SCHEMA_RESOURCE, true))) {
            if (pendingName != null) {
                // a table name containing a line break (CRLF kept by the export): the name ends at the backtick
                int end = line.indexOf('`');
                pendingName = pendingName + "\n" + line.substring(0, end);
                if (end >= 0) {
                    table = pendingName;
                    pendingName = null;
                }
                text.append(line).append('\n');
                continue;
            }
            if (line.startsWith("USE `")) {
                database = line.substring(5, line.indexOf('`', 5));
                continue;
            }
            if (line.startsWith("CREATE TABLE `")) {
                int end = line.indexOf('`', 14);
                columns = new ArrayList<>();
                text = new StringBuilder(line).append('\n');
                if (end < 0) {
                    pendingName = line.substring(14);
                } else {
                    table = line.substring(14, end);
                }
                continue;
            }
            if (columns == null) {
                continue;
            }
            text.append(line).append('\n');
            Matcher m = COLUMN.matcher(line);
            if (m.find()) {
                String rest = m.group(3);
                columns.add(new ColumnMetadata(m.group(1), columns.size(), m.group(2),
                        !rest.contains("NOT NULL")));
            } else if (line.startsWith(") ENGINE")) {
                // properties follow until the terminating semicolon
                continue;
            } else if (line.endsWith(";") && line.startsWith(")")) {
                TableIdentifier id = TableIdentifier.of("default_catalog", database, table);
                tables.put(id, new TableSchema(id, columns));
                ddl.put(id, text.toString());
                perDatabase.merge(database, 1, Integer::sum);
                columns = null;
            }
        }
        return new Schema(tables, ddl, perDatabase);
    }

    /**
     * The parsed schema dump.
     */
    public static final class Schema {
        private final Map<TableIdentifier, TableSchema> tables;
        private final Map<TableIdentifier, String> ddl;
        private final Map<String, Integer> perDatabase;

        Schema(Map<TableIdentifier, TableSchema> tables, Map<TableIdentifier, String> ddl,
               Map<String, Integer> perDatabase) {
            this.tables = Collections.unmodifiableMap(tables);
            this.ddl = Collections.unmodifiableMap(ddl);
            this.perDatabase = Collections.unmodifiableMap(perDatabase);
        }

        public Map<TableIdentifier, TableSchema> tables() {
            return tables;
        }

        public Map<String, Integer> tablesPerDatabase() {
            return perDatabase;
        }

        public Optional<TableSchema> table(TableIdentifier id) {
            return Optional.ofNullable(tables.get(id));
        }

        /** @return the {@code CREATE TABLE} statement of {@code id} exactly as exported */
        public String ddl(TableIdentifier id) {
            String s = ddl.get(id);
            if (s == null) {
                throw new IllegalArgumentException("not in the dump: " + id);
            }
            return s;
        }

        /** @return a new metadata provider holding every table of the dump */
        public InMemoryMetadataProvider metadata() {
            InMemoryMetadataProvider p = new InMemoryMetadataProvider();
            tables.values().forEach(p::register);
            return p;
        }
    }

    // ------------------------------------------------------------------------------------------------ statements

    static List<Statement> parseStatements(String text) {
        Matcher m = MARKER.matcher(text);
        List<int[]> starts = new ArrayList<>();
        List<String[]> heads = new ArrayList<>();
        while (m.find()) {
            starts.add(new int[]{m.start()});
            heads.add(new String[]{m.group(1), m.group(2)});
        }
        List<Statement> result = new ArrayList<>();
        for (int i = 0; i < starts.size(); i++) {
            int from = starts.get(i)[0];
            int to = i + 1 < starts.size() ? starts.get(i + 1)[0] : text.length();
            String block = text.substring(from, to);
            Matcher task = TASK_INSTANCE.matcher(block);
            result.add(new Statement(Integer.parseInt(heads.get(i)[0]), heads.get(i)[1],
                    task.find() ? task.group(1) : "", block));
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * One production INSERT with the comments the task log carried.
     */
    public static final class Statement {
        private final int index;
        private final String declaredTarget;
        private final String taskInstanceId;
        private final String text;

        Statement(int index, String declaredTarget, String taskInstanceId, String text) {
            this.index = index;
            this.declaredTarget = declaredTarget;
            this.taskInstanceId = taskInstanceId;
            this.text = text;
        }

        public int getIndex() {
            return index;
        }

        /** @return the target as written in the index of the file (qualified or not) */
        public String getDeclaredTarget() {
            return declaredTarget;
        }

        public TableIdentifier getTarget() {
            return TableIdentifier.parse(declaredTarget, NAMESPACE);
        }

        public String getTaskInstanceId() {
            return taskInstanceId;
        }

        /** @return the block as it appears in the file: comment header, statement, terminating semicolon */
        public String getText() {
            return text;
        }

        /**
         * @return the explicit column list of the INSERT ({@code INSERT INTO t (c1, c2) ...}), lower case, or
         *         empty when the statement has none
         */
        public Optional<List<String>> columnList() {
            String code = text.replaceAll("--[^\\n]*", " ");
            Matcher m = Pattern.compile("(?is)\\binsert\\s+into\\s+[`\\w.]+\\s*\\(").matcher(code);
            if (!m.find()) {
                return Optional.empty();
            }
            int open = m.end();
            String after = code.substring(open).trim().toLowerCase(Locale.ROOT);
            if (after.startsWith("select") || after.startsWith("with")) {
                return Optional.empty();
            }
            int close = code.indexOf(')', open);
            List<String> cols = new ArrayList<>();
            for (String c : code.substring(open, close).split(",")) {
                cols.add(c.replace("`", "").trim().toLowerCase(Locale.ROOT));
            }
            return Optional.of(cols);
        }

        /** @return the statement as the audit loader would record it for the DolphinScheduler account */
        public AuditLogRecord auditRecord(LocalDateTime at) {
            return AuditLogRecord.builder().queryId("prod-" + taskInstanceId).timestamp(at).user("ds_etl")
                    .state("EOF").errorCode("").catalog("default_catalog").db("lbdb").queryType("query")
                    .queryTime(1000).scanBytes(0).isQuery(false).digest("ds-" + index).stmt(text).build();
        }

        @Override
        public String toString() {
            return String.format("[%02d] %s", index, declaredTarget);
        }
    }

    // ------------------------------------------------------------------------------------------------ snapshot

    /**
     * Sorted, line-oriented lineage of one statement: {@code T} referenced tables, {@code D} DIRECT edges
     * ({@code target <- source TRANSFORM}), {@code I} control columns of INDIRECT edges with the number of target
     * columns they reach, {@code N} edge totals.
     */
    public static List<String> snapshot(ProductionFixture.Statement s, SqlParseResult r) {
        String p = String.format("%02d ", s.getIndex());
        List<String> lines = new ArrayList<>();
        r.getSourceTables().stream().map(t -> p + "T " + t.getDatabase() + "." + t.getTable()).sorted()
                .forEach(lines::add);
        List<String> direct = new ArrayList<>();
        Map<String, Integer> indirect = new TreeMap<>();
        for (ColumnLineageEdge e : r.getLineageEdges()) {
            String src = e.getSource().isConstant() ? "<constant>" : e.getSource().getTable().getDatabase() + "."
                    + e.getSource().getTable().getTable() + "." + e.getSource().getColumn();
            if (e.getLineageType() == LineageType.DIRECT) {
                direct.add(p + "D " + e.getTarget().getColumn() + " <- " + src + " " + e.getTransformType());
            } else {
                indirect.merge(src, 1, Integer::sum);
            }
        }
        direct.stream().sorted().forEach(lines::add);
        indirect.forEach((src, n) -> lines.add(p + "I " + src + " x" + n));
        lines.add(p + "N edges=" + r.getLineageEdges().size() + " direct=" + direct.size());
        return lines;
    }

    /**
     * The reviewed snapshot {@code production/inserts_30.lineage}, by statement index. With
     * {@code -Dproduction.golden.write=<file>} the current lineage (computed with {@code analyser}) is written there
     * first, for review and replacement of the resource.
     */
    public static synchronized Map<Integer, List<String>> golden(
            java.util.function.Function<Statement, SqlParseResult> analyser) throws IOException {
        String write = System.getProperty("production.golden.write");
        if (write != null && !write.isEmpty()) {
            Path out = Paths.get(write);
            List<String> all = new ArrayList<>();
            for (ProductionFixture.Statement s : ProductionFixture.statements()) {
                all.addAll(snapshot(s, analyser.apply(s)));
            }
            Files.write(out, all, StandardCharsets.UTF_8);
        }
        Map<Integer, List<String>> byStatement = new HashMap<>();
        try (InputStream in = ProductionFixture.class.getResourceAsStream(GOLDEN)) {
            if (in == null) {
                throw new IllegalStateException("missing " + GOLDEN);
            }
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            for (String line : text.split("\n")) {
                if (!line.isEmpty()) {
                    byStatement.computeIfAbsent(Integer.parseInt(line.substring(0, 2)), k -> new ArrayList<>())
                            .add(line);
                }
            }
        }
        if (byStatement.size() != 30) {
            throw new IllegalStateException("the snapshot must cover the 30 statements: " + byStatement.keySet());
        }
        return byStatement;
    }

    // ------------------------------------------------------------------------------------------------ io

    private static String read(String resource, boolean gzip) {
        try (InputStream raw = ProductionFixture.class.getResourceAsStream(resource)) {
            if (raw == null) {
                throw new IllegalStateException("missing test resource " + resource);
            }
            InputStream in = gzip ? new GZIPInputStream(raw) : raw;
            StringBuilder sb = new StringBuilder();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                char[] buf = new char[1 << 16];
                int n;
                while ((n = r.read(buf)) > 0) {
                    sb.append(buf, 0, n);
                }
            }
            return sb.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String[] lines(String text) {
        return text.split("\n", -1);
    }
}
