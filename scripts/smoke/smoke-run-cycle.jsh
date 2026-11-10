// Smoke test: run pipeline cycles against the cluster until a cycle commits nothing (or SMOKE_CYCLES cycles) and
// print one SMOKE_REPORT line per cycle and a SMOKE_SUMMARY line. Usage (see docs/staging-smoke-test.md):
//   jshell -J-Duser.timezone=Asia/Shanghai --class-path "target/classes:$(cat target/cp.txt)" \
//          scripts/smoke/smoke-common.jsh scripts/smoke/smoke-run-cycle.jsh
// Extra variables: SMOKE_PIPELINE=audit_lineage_smoke, SMOKE_LOOKBACK_HOURS=2 (first run only),
//   SMOKE_SLICE_SECONDS=300, SMOKE_LATE_ARRIVAL_SECONDS=300, SMOKE_RECONCILE_SECONDS=900, SMOKE_THREADS=4,
//   SMOKE_CYCLES=0 (0 = until a cycle commits nothing), SMOKE_OWNER=smoke-<hostname> (lease owner; keep it stable)
int runCycles() {
    try (HikariDataSource ds = dataSource(); StreamLoadClient sl = streamLoad()) {
        checkClock(ds);
        int committed = 0;
        long records = 0;
        CycleReport.Outcome last = null;
        try (AuditLogSyncPipeline pipeline = smokePipeline(ds, sl)) {
            int cycles = Integer.parseInt(env("SMOKE_CYCLES", "0"));
            for (int i = 0; cycles == 0 || i < cycles; i++) {
                long t0 = System.currentTimeMillis();
                CycleReport r = pipeline.runOnce();
                long ms = System.currentTimeMillis() - t0;
                last = r.getOutcome();
                System.out.println("SMOKE_REPORT " + report(r) + " ms=" + ms + " records_per_second="
                        + (ms == 0 ? 0 : r.getRecords() * 1000L / ms));
                System.out.println("  " + r);
                if (r.getOutcome() != CycleReport.Outcome.COMMITTED) {
                    break;
                }
                committed++;
                records += r.getRecords();
            }
            PipelineMetrics m = pipeline.getMetrics();
            System.out.println("SMOKE_METRICS " + m.statusCounts() + " degraded=" + m.getDegraded()
                    + " dead_letters=" + m.getDeadLetters() + " late_records=" + m.getLateRecords()
                    + " blocked=" + m.isBlocked());
        }
        System.out.println("SMOKE_SUMMARY committed=" + committed + " records=" + records + " last_outcome=" + last);
        return 0;
    } catch (Throwable t) {
        t.printStackTrace(System.out);
        System.out.println("SMOKE_FAILED " + t);
        return 1;
    }
}
int smokeExitCode = runCycles();
/exit smokeExitCode
