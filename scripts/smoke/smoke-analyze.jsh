// Smoke test, read-only: analyse the last SMOKE_SAMPLE (default 2000) audit records of the last
// SMOKE_LOOKBACK_HOURS (default 1) hours with the real metadata and print the outcome distribution plus samples of
// every failure and warning. Writes nothing. Usage: like smoke-run-cycle.jsh, with this file instead.
int analyzeSample() {
    try (HikariDataSource ds = dataSource()) {
        checkClock(ds);
        StarRocksSqlAnalyzer analyzer = new StarRocksSqlAnalyzer(new JdbcStarRocksMetadataProvider(ds));
        LocalDateTime now = LocalDateTime.now();
        List<AuditLogRecord> records = new JdbcAuditLogSource(ds).fetch(
                Watermark.startingAt(now.minusHours(Long.parseLong(env("SMOKE_LOOKBACK_HOURS", "1")))), now,
                Integer.parseInt(env("SMOKE_SAMPLE", "2000")));
        Map<ParseStatus, Integer> counts = new EnumMap<>(ParseStatus.class);
        Map<String, Integer> warnings = new TreeMap<>();
        List<String> failures = new ArrayList<>();
        int degraded = 0;
        long t0 = System.nanoTime();
        for (AuditLogRecord r : records) {
            SqlParseResult res = analyzer.analyze(r);
            counts.merge(res.getStatus(), 1, Integer::sum);
            if (res.isDegraded()) {
                degraded++;
                for (String w : res.getWarnings()) {
                    warnings.merge(w.replaceAll("[0-9]+", "#"), 1, Integer::sum);
                }
            }
            if (res.getStatus().isError() && failures.size() < 30) {
                String stmt = r.getStmt() == null ? "" : r.getStmt().replaceAll("\\s+", " ");
                failures.add(res.getStatus() + " " + r.getQueryId() + " | " + res.getMessage().orElse("") + " | "
                        + (stmt.length() > 200 ? stmt.substring(0, 200) + "..." : stmt));
            }
        }
        long ms = Math.max(1, (System.nanoTime() - t0) / 1_000_000);
        int analysed = records.size() - counts.getOrDefault(ParseStatus.SKIPPED_FAILED_QUERY, 0)
                - counts.getOrDefault(ParseStatus.SKIPPED_MANAGEMENT, 0) - counts.getOrDefault(ParseStatus.SKIPPED_EMPTY, 0);
        int errors = counts.getOrDefault(ParseStatus.PARSE_ERROR, 0) + counts.getOrDefault(ParseStatus.ANALYSIS_ERROR, 0);
        System.out.println("SMOKE_ANALYZE total=" + records.size() + " " + counts + " degraded=" + degraded
                + " error_rate_percent=" + (analysed == 0 ? 0 : errors * 100.0 / analysed)
                + " single_thread_records_per_second=" + records.size() * 1000L / ms);
        failures.forEach(f -> System.out.println("  FAIL " + f));
        warnings.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).limit(15)
                .forEach(e -> System.out.println("  WARN x" + e.getValue() + " " + e.getKey()));
        return 0;
    } catch (Throwable t) {
        t.printStackTrace(System.out);
        System.out.println("SMOKE_FAILED " + t);
        return 1;
    }
}
int smokeExitCode = analyzeSample();
/exit smokeExitCode
