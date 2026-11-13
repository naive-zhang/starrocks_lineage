package com.starrocks.lineage.cli;

import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.JdbcStarRocksMetadataProvider;
import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ParseStatus;
import com.starrocks.lineage.model.SqlParseResult;
import com.starrocks.lineage.pipeline.AuditLogSyncPipeline;
import com.starrocks.lineage.pipeline.CheckpointStore;
import com.starrocks.lineage.pipeline.CycleReport;
import com.starrocks.lineage.pipeline.JdbcAuditLogSource;
import com.starrocks.lineage.pipeline.JdbcCheckpointStore;
import com.starrocks.lineage.pipeline.PipelineMetrics;
import com.starrocks.lineage.pipeline.StarRocksSink;
import com.starrocks.lineage.pipeline.StreamLoadClient;
import com.starrocks.lineage.pipeline.Watermark;
import com.zaxxer.hikari.HikariDataSource;

import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standalone program: {@code java -jar starrocks-audit-lineage-<version>-all.jar <command> [--config file]}.
 * Only a Java 11+ runtime is needed. See {@code docs/production-deployment.md}.
 */
public final class AuditLineageCli {

    static final int OK = 0;
    static final int FAILED = 1;
    static final int USAGE = 2;
    static final int BLOCKED = 3;

    static final String HELP = String.join("\n",
            "Usage: java -jar starrocks-audit-lineage-all.jar <command> [--config <file>] [arguments]",
            "",
            "Commands:",
            "  check                      pre-production checks (connection, clock, audit table, target tables,",
            "                             metadata visibility, SQL write, Stream Load FE -> BE); exit 1 on a failure",
            "  analyze [hours] [sample]   dry run: analyse the latest audit records (default 1 hour, 2000 records),",
            "                             print the outcome distribution and failure samples; writes nothing",
            "  run                        process until caught up, then exit (for cron / a scheduler)",
            "  daemon                     run continuously every INTERVAL_SECONDS until stopped (SIGTERM)",
            "  status                     checkpoint, pending batch and dead letters of the pipeline",
            "  redo | mark-done           resolve a batch BLOCKED on an unknown load label",
            "  skip <reason>              give the pending batch up (recorded as a BATCH_SKIPPED dead letter)",
            "  repair <from> <to> [limit] re-analyse OPEN dead letters with audit time in [from, to)",
            "  rebuild <fromDay> <toDay>  recompute whole days [fromDay, toDay) from the batch membership, one",
            "                             batch per hour; prints every hour and PROGRESS lines; running it again",
            "                             resumes an interrupted rebuild",
            "",
            "Options: --config <file>, --threads <n> (analysis threads, overrides THREADS).",
            "",
            "Times are 'yyyy-MM-dd HH:mm:ss' (days: 'yyyy-MM-dd'). Configuration: environment variables or the",
            "--config properties file (environment wins). Keys and defaults:",
            "  " + String.join("\n  ", describeDefaults()),
            "",
            "Exit codes: 0 ok, 1 failure, 2 usage error, 3 pipeline BLOCKED (operator decision needed).");

    private AuditLineageCli() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.getenv(), System.out));
    }

    private static List<String> describeDefaults() {
        List<String> lines = new ArrayList<>();
        CliConfig.DEFAULTS.forEach((k, v) -> lines.add(k + (v == null ? " (required)" : "=" + v)));
        return lines;
    }

    /**
     * Testable entry point.
     *
     * @return the process exit code
     */
    static int run(String[] args, Map<String, String> env, PrintStream out) {
        String command = null;
        Path configFile = null;
        String threads = null;
        List<String> positional = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("--config") && i + 1 < args.length) {
                configFile = Paths.get(args[++i]);
            } else if (a.startsWith("--config=")) {
                configFile = Paths.get(a.substring("--config=".length()));
            } else if (a.equals("--threads") && i + 1 < args.length) {
                threads = args[++i];
            } else if (a.startsWith("--threads=")) {
                threads = a.substring("--threads=".length());
            } else if (a.equals("-h") || a.equals("--help") || a.equals("help")) {
                out.println(HELP);
                return OK;
            } else if (command == null) {
                command = a;
            } else {
                positional.add(a);
            }
        }
        if (command == null) {
            out.println(HELP);
            return USAGE;
        }
        CliConfig config;
        try {
            if (threads != null) {
                // the command line wins over the environment and the file
                Map<String, String> withThreads = new java.util.HashMap<>(env);
                withThreads.put("THREADS", threads);
                env = withThreads;
            }
            config = CliConfig.load(env, configFile);
        } catch (IllegalArgumentException e) {
            out.println("CONFIG ERROR " + e.getMessage());
            return USAGE;
        }
        if (!config.get("TIME_ZONE").isEmpty()) {
            // before any JDBC / clock use: audit timestamps are compared with this zone
            TimeZone.setDefault(TimeZone.getTimeZone(config.get("TIME_ZONE")));
        }
        try {
            return dispatch(command, positional, config, out);
        } catch (IllegalArgumentException e) {
            out.println("USAGE ERROR " + e.getMessage());
            return USAGE;
        } catch (RuntimeException e) {
            out.println("FAILED " + e);
            e.printStackTrace(out);
            return FAILED;
        }
    }

    private static int dispatch(String command, List<String> args, CliConfig config, PrintStream out) {
        switch (command) {
            case "check":
                return check(config, out);
            case "analyze":
                return analyze(config, args, out);
            case "run":
                return runUntilCaughtUp(config, out);
            case "daemon":
                return daemon(config, out);
            case "status":
                return status(config, out);
            case "redo":
            case "mark-done":
            case "skip":
            case "repair":
            case "rebuild":
                return operate(command, args, config, out);
            default:
                throw new IllegalArgumentException("unknown command '" + command + "' (see --help)");
        }
    }

    // ================================================================================================ commands

    private static int check(CliConfig config, PrintStream out) {
        out.println("CONFIG " + config.describe());
        try (HikariDataSource ds = config.dataSource(); StreamLoadClient sl = config.streamLoad()) {
            Preflight preflight = new Preflight(config, ds, sl, Clock.systemDefaultZone());
            for (Preflight.Item item : preflight.run()) {
                out.println(item);
            }
            boolean ok = preflight.passed();
            out.println(ok ? "CHECK RESULT PASSED" : "CHECK RESULT FAILED");
            return ok ? OK : FAILED;
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int analyze(CliConfig config, List<String> args, PrintStream out) {
        int hours = args.isEmpty() ? 1 : Integer.parseInt(args.get(0));
        int sample = args.size() < 2 ? 2000 : Integer.parseInt(args.get(1));
        try (HikariDataSource ds = config.dataSource()) {
            StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(new JdbcStarRocksMetadataProvider(ds));
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime from = now.minusHours(hours);
            List<AuditLogRecord> records = source(ds, config).fetchRange(from, now, Watermark.startingAt(from),
                    sample);
            Map<ParseStatus, Integer> counts = new EnumMap<>(ParseStatus.class);
            Map<String, Integer> warnings = new TreeMap<>();
            List<String> failures = new ArrayList<>();
            int[] degraded = {0};
            long t0 = System.nanoTime();
            onLargeStack(() -> {
                for (AuditLogRecord r : records) {
                    SqlParseResult res = analyzer.analyze(r);
                    counts.merge(res.getStatus(), 1, Integer::sum);
                    if (res.isDegraded()) {
                        degraded[0]++;
                        for (String w : res.getWarnings()) {
                            warnings.merge(w.replaceAll("[0-9]+", "#"), 1, Integer::sum);
                        }
                    }
                    if (res.getStatus().isError() && failures.size() < 30) {
                        String stmt = r.getStmt() == null ? "" : r.getStmt().replaceAll("\\s+", " ");
                        failures.add(res.getStatus() + " " + r.getQueryId() + " | " + res.getMessage().orElse("")
                                + " | " + (stmt.length() > 200 ? stmt.substring(0, 200) + "..." : stmt));
                    }
                }
            });
            long ms = Math.max(1, (System.nanoTime() - t0) / 1_000_000);
            int analysed = records.size() - counts.getOrDefault(ParseStatus.SKIPPED_FAILED_QUERY, 0)
                    - counts.getOrDefault(ParseStatus.SKIPPED_MANAGEMENT, 0)
                    - counts.getOrDefault(ParseStatus.SKIPPED_EMPTY, 0);
            int errors = counts.getOrDefault(ParseStatus.PARSE_ERROR, 0)
                    + counts.getOrDefault(ParseStatus.ANALYSIS_ERROR, 0);
            out.println("ANALYZE total=" + records.size() + " " + counts + " degraded=" + degraded[0]
                    + " error_rate_percent=" + (analysed == 0 ? 0 : Math.round(errors * 10000.0 / analysed) / 100.0)
                    + " single_thread_records_per_second=" + records.size() * 1000L / ms);
            failures.forEach(f -> out.println("  FAIL " + f));
            warnings.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(15)
                    .forEach(e -> out.println("  WARN x" + e.getValue() + " " + e.getKey()));
            return OK;
        }
    }

    private static int runUntilCaughtUp(CliConfig config, PrintStream out) {
        try (Resources r = new Resources(config)) {
            int committed = 0;
            long records = 0;
            CycleReport.Outcome last = null;
            for (int i = 0; i < 100_000; i++) {
                long t0 = System.currentTimeMillis();
                CycleReport report = r.pipeline.runOnce();
                last = report.getOutcome();
                out.println("REPORT " + describe(report) + " ms=" + (System.currentTimeMillis() - t0));
                if (last != CycleReport.Outcome.COMMITTED) {
                    break;
                }
                committed++;
                records += report.getRecords();
            }
            out.println("METRICS " + metrics(r.pipeline.getMetrics()));
            out.println("SUMMARY committed=" + committed + " records=" + records + " last_outcome=" + last);
            return last == CycleReport.Outcome.BLOCKED ? BLOCKED : OK;
        }
    }

    private static int daemon(CliConfig config, PrintStream out) {
        Resources r = new Resources(config);
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            out.println("DAEMON stopping: releasing the lease");
            r.close();
            stopped.countDown();
        }, "audit-lineage-shutdown"));
        out.println("DAEMON started pipeline=" + config.pipeline() + " owner=" + config.owner() + " interval="
                + config.interval() + " config=" + config.describe());
        r.pipeline.start(config.interval());
        try {
            while (!stopped.await(15, java.util.concurrent.TimeUnit.MINUTES)) {
                out.println("METRICS " + metrics(r.pipeline.getMetrics()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return OK;
    }

    private static int status(CliConfig config, PrintStream out) {
        try (HikariDataSource ds = config.dataSource()) {
            CheckpointStore.Checkpoint cp = checkpoints(ds, config).load(config.pipeline()).orElse(null);
            if (cp == null) {
                out.println("STATUS pipeline=" + config.pipeline() + " not started");
                return OK;
            }
            out.println("STATUS pipeline=" + config.pipeline() + " committed_until=" + cp.getCommittedUntil()
                    + " coverage_start=" + cp.getCoverageStart() + " owner=" + cp.getOwner() + " lease_expire="
                    + cp.getLeaseExpire() + " processed_rows=" + cp.getProcessedRows() + " last_reconcile="
                    + cp.getLastReconcile().map(Object::toString).orElse("-") + " pending="
                    + cp.getPending().map(Object::toString).orElse("-"));
            try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("SELECT kind, status, "
                    + "COUNT(*) FROM " + config.lineageDb() + ".audit_lineage_dead_letter WHERE pipeline_name = ? "
                    + "GROUP BY kind, status ORDER BY kind, status")) {
                ps.setString(1, config.pipeline());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.println("  dead_letters kind=" + rs.getString(1) + " status=" + rs.getString(2)
                                + " count=" + rs.getLong(3));
                    }
                }
            }
            return OK;
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int operate(String command, List<String> args, CliConfig config, PrintStream out) {
        try (Resources r = new Resources(config)) {
            CycleReport report = null;
            switch (command) {
                case "redo":
                    r.pipeline.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.REDO);
                    break;
                case "mark-done":
                    r.pipeline.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.MARK_DONE);
                    break;
                case "skip":
                    if (args.isEmpty() || String.join(" ", args).trim().isEmpty()) {
                        throw new IllegalArgumentException("skip needs a reason");
                    }
                    report = r.pipeline.skipPendingBatch(String.join(" ", args));
                    break;
                case "repair":
                    require(args, 2, "repair <from> <to> [limit]");
                    report = r.pipeline.repairDeadLetters(time(args.get(0)), time(args.get(1)),
                            args.size() > 2 ? Integer.parseInt(args.get(2)) : 1000);
                    break;
                default:
                    require(args, 2, "rebuild <fromDay> <toDay>");
                    long t0 = System.nanoTime();
                    out.println("REBUILD [" + args.get(0) + ", " + args.get(1) + ") threads=" + config.get("THREADS")
                            + " read_threads=" + config.get("REBUILD_READ_THREADS"));
                    report = r.pipeline.rebuild(time(args.get(0)), time(args.get(1)), (hour, done, total) -> {
                        long elapsed = (System.nanoTime() - t0) / 1_000_000_000L;
                        long eta = done == 0 ? 0 : elapsed * (total - done) / done;
                        out.println("REBUILD hour " + done + "/" + total + " elapsed=" + elapsed + "s eta=" + eta
                                + "s " + describe(hour));
                        out.flush();
                    });
                    break;
            }
            out.println("OPERATION " + command + " " + (report == null ? "outcome=DONE" : describe(report)));
            return report != null && report.getOutcome() == CycleReport.Outcome.BLOCKED ? BLOCKED : OK;
        } catch (IllegalStateException e) {
            out.println("OPERATION " + command + " refused: " + e.getMessage());
            return FAILED;
        }
    }

    // ================================================================================================ helpers

    /** Everything a pipeline needs, closed together (the pipeline first: it releases the lease). */
    private static final class Resources implements AutoCloseable {
        final HikariDataSource ds;
        final StreamLoadClient streamLoad;
        final AuditLogSyncPipeline pipeline;
        private boolean closed;

        Resources(CliConfig config) {
            this.ds = config.dataSource();
            this.streamLoad = config.streamLoad();
            JdbcStarRocksMetadataProvider metadata = new JdbcStarRocksMetadataProvider(ds);
            this.pipeline = new AuditLogSyncPipeline(config.pipelineConfig(LocalDateTime.now()), source(ds, config),
                    checkpoints(ds, config), new StarRocksSink(ds, streamLoad, config.lineageDb(), config.pipeline(),
                            Clock.systemDefaultZone(), config.loadChunkBytes(), config.loadMaxBytes()),
                    new StarRocksSqlAnalyzer(metadata), metadata, Clock.systemDefaultZone());
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            pipeline.close();
            try {
                streamLoad.close();
            } catch (java.io.IOException e) {
                System.err.println("closing the stream load client failed: " + e);
            }
            ds.close();
        }
    }

    private static JdbcAuditLogSource source(HikariDataSource ds, CliConfig config) {
        return new JdbcAuditLogSource(ds, config.auditTable(), config.lineageDb() + ".audit_lineage_batch_member",
                120);
    }

    private static JdbcCheckpointStore checkpoints(HikariDataSource ds, CliConfig config) {
        return new JdbcCheckpointStore(ds, config.lineageDb() + ".audit_lineage_sync_checkpoint",
                Clock.systemDefaultZone(), "INSERT");
    }

    static String describe(CycleReport r) {
        return "outcome=" + r.getOutcome() + " kind=" + r.getKind().map(Enum::name).orElse("-") + " batch="
                + r.getBatchId().orElse("-") + " range_start=\"" + r.getRangeStart().map(AuditLineageCli::sql)
                .orElse("-") + "\" range_end=\"" + r.getRangeEnd().map(AuditLineageCli::sql).orElse("-")
                + "\" records=" + r.getRecords() + " replay=" + r.isReplay() + " usage_rows=" + r.getUsageRows()
                + " lineage_rows=" + r.getLineageRows() + " dead_letters=" + r.getDeadLetters() + " filtered_rows="
                + r.getFilteredRows() + r.getMessage().map(m -> " message=\"" + m.replace('"', '\'') + "\"")
                .orElse("");
    }

    private static String metrics(PipelineMetrics m) {
        return m.statusCounts() + " degraded=" + m.getDegraded() + " committed_batches=" + m.getCommittedBatches()
                + " failed_cycles=" + m.getFailedCycles() + " consecutive_failures=" + m.getConsecutiveFailures()
                + " blocked=" + m.isBlocked() + " dead_letters=" + m.getDeadLetters() + " late_records="
                + m.getLateRecords() + " filtered_rows=" + m.getFilteredRows();
    }

    private static String sql(LocalDateTime t) {
        return t.toString().replace('T', ' ');
    }

    /** {@code yyyy-MM-dd} or {@code yyyy-MM-dd HH:mm[:ss]}. */
    static LocalDateTime time(String s) {
        String v = s.trim().replace(' ', 'T');
        try {
            return v.length() == 10 ? java.time.LocalDate.parse(v).atStartOfDay() : LocalDateTime.parse(v);
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("not a time: " + s + " (use 'yyyy-MM-dd HH:mm:ss')");
        }
    }

    private static void require(List<String> args, int n, String usage) {
        if (args.size() < n) {
            throw new IllegalArgumentException("usage: " + usage);
        }
    }

    /** Runs {@code body} on a thread with the analysis stack size (deeply nested production statements). */
    private static void onLargeStack(Runnable body) {
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        Thread t = new Thread(null, () -> {
            try {
                body.run();
            } catch (RuntimeException e) {
                failure.set(e);
            }
        }, "audit-analyze", AuditLogSyncPipeline.ANALYSIS_STACK_BYTES);
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }
}
