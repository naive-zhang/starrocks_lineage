// Smoke test: run SMOKE_CYCLES (default 1) pipeline cycles against the cluster and print one SMOKE_REPORT line per
// cycle. Usage (see docs/staging-smoke-test.md):
//   jshell -J-Duser.timezone=Asia/Shanghai --class-path "target/classes:$(cat target/cp.txt)" \
//          scripts/smoke/smoke-common.jsh scripts/smoke/smoke-run-cycle.jsh
// Extra variables: SMOKE_PIPELINE=audit_lineage_smoke, SMOKE_LOOKBACK_HOURS=2 (first run only),
//   SMOKE_LATE_ARRIVAL_SECONDS=300, SMOKE_MAX_RECORDS=50000, SMOKE_THREADS=4, SMOKE_CYCLES=1 (0 = until idle),
//   SMOKE_OWNER=smoke-<hostname> (lease owner; keep it stable between runs)
int runCycles() {
    try (HikariDataSource ds = dataSource(); StreamLoadClient sl = streamLoad()) {
        checkClock(ds);
        JdbcStarRocksMetadataProvider metadata = new JdbcStarRocksMetadataProvider(ds);
        // a stable owner: re-running the script from the same machine renews the lease instead of waiting for the
        // previous run's lease (10 minutes) to expire
        String owner = env("SMOKE_OWNER", "smoke-" + java.net.InetAddress.getLocalHost().getHostName());
        PipelineConfig config = PipelineConfig.builder(env("SMOKE_PIPELINE", "audit_lineage_smoke"), owner)
                .initialStart(LocalDateTime.now().minusHours(Long.parseLong(env("SMOKE_LOOKBACK_HOURS", "2"))))
                .lateArrival(Duration.ofSeconds(Long.parseLong(env("SMOKE_LATE_ARRIVAL_SECONDS", "300"))))
                .maxRecordsPerCycle(Integer.parseInt(env("SMOKE_MAX_RECORDS", "50000")))
                .pageSize(Math.min(5000, Integer.parseInt(env("SMOKE_MAX_RECORDS", "50000"))))
                .analyzerThreads(Integer.parseInt(env("SMOKE_THREADS", "4")))
                .leaseTtl(Duration.ofMinutes(10)).build();
        try (AuditLogSyncPipeline pipeline = new AuditLogSyncPipeline(config, new JdbcAuditLogSource(ds),
                new JdbcCheckpointStore(ds), new StarRocksSink(ds, sl, "audit_lineage_db"),
                new StarRocksSqlAnalyzer(metadata), metadata, Clock.systemDefaultZone())) {
            int cycles = Integer.parseInt(env("SMOKE_CYCLES", "1"));
            for (int i = 0; cycles == 0 || i < cycles; i++) {
                long t0 = System.currentTimeMillis();
                CycleReport r = pipeline.runOnce();
                long ms = System.currentTimeMillis() - t0;
                System.out.println("SMOKE_REPORT outcome=" + r.getOutcome() + " batch=" + r.getBatchId().orElse("-")
                        + " records=" + r.getRecords() + " replay=" + r.isReplay()
                        + " end_time=\"" + r.getWatermark().map(w -> sqlTime(w.getTimestamp())).orElse("-") + "\""
                        + " end_query_id=" + r.getWatermark().map(Watermark::getQueryId).orElse("-")
                        + " usage_rows=" + r.getUsageRows() + " lineage_rows=" + r.getLineageRows()
                        + " new_users=" + r.getNewUsers() + " ms=" + ms
                        + " records_per_second=" + (ms == 0 ? 0 : r.getRecords() * 1000L / ms));
                System.out.println("  " + r);
                if (r.getOutcome() != CycleReport.Outcome.COMMITTED) {
                    break;
                }
            }
            System.out.println("SMOKE_METRICS " + pipeline.getMetrics().statusCounts() + " degraded="
                    + pipeline.getMetrics().getDegraded());
        }
        return 0;
    } catch (Throwable t) {
        t.printStackTrace(System.out);
        System.out.println("SMOKE_FAILED " + t);
        return 1;
    }
}
int smokeExitCode = runCycles();
/exit smokeExitCode
