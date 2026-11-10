// Operator actions on the smoke pipeline (the same calls a production service exposes). Usage like
// smoke-run-cycle.jsh, with this file instead and SMOKE_OP set:
//   SMOKE_OP=status     checkpoint, pending batch, failed attempts, open dead letters (default; writes nothing)
//   SMOKE_OP=redo       a batch BLOCKED on an unknown load label: the data of the step is absent -> execute it again
//   SMOKE_OP=mark-done  a batch BLOCKED on an unknown load label: the data of the step is present -> continue after it
//   SMOKE_OP=skip       give the pending batch up (SMOKE_REASON required; recorded as a BATCH_SKIPPED dead letter)
//   SMOKE_OP=repair     re-analyse OPEN parse / analysis dead letters with audit time in [SMOKE_FROM, SMOKE_TO)
//   SMOKE_OP=rebuild    recompute whole days [SMOKE_FROM, SMOKE_TO) ("yyyy-MM-dd 00:00:00") from the membership
// The decision rules are in docs/stage5-reliability.md, section "Operations".
int runOp() {
    String op = env("SMOKE_OP", "status");
    try (HikariDataSource ds = dataSource(); StreamLoadClient sl = streamLoad()) {
        checkClock(ds);
        if (op.equals("status")) {
            CheckpointStore.Checkpoint cp = new JdbcCheckpointStore(ds).load(smokeName())
                    .orElseThrow(() -> new IllegalStateException("no checkpoint for " + smokeName()));
            System.out.println("SMOKE_STATUS committed_until=" + q(cp.getCommittedUntil()) + " coverage_start="
                    + q(cp.getCoverageStart()) + " owner=" + cp.getOwner() + " lease_expire=" + q(cp.getLeaseExpire())
                    + " processed_rows=" + cp.getProcessedRows() + " last_reconcile="
                    + q(cp.getLastReconcile().orElse(null)) + " pending=" + cp.getPending().map(Object::toString)
                    .map(x -> "\"" + x + "\"").orElse("-"));
            try (var c = ds.getConnection(); var ps = c.prepareStatement("SELECT kind, status, COUNT(*) FROM "
                    + "audit_lineage_db.audit_lineage_dead_letter WHERE pipeline_name = ? GROUP BY kind, status "
                    + "ORDER BY kind, status")) {
                ps.setString(1, smokeName());
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        System.out.println("  dead_letters kind=" + rs.getString(1) + " status=" + rs.getString(2)
                                + " count=" + rs.getLong(3));
                    }
                }
            }
            return 0;
        }
        try (AuditLogSyncPipeline pipeline = smokePipeline(ds, sl)) {
            CycleReport r = null;
            switch (op) {
                case "redo":
                    pipeline.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.REDO);
                    break;
                case "mark-done":
                    pipeline.resolveBlockedStep(AuditLogSyncPipeline.StepResolution.MARK_DONE);
                    break;
                case "skip":
                    String reason = env("SMOKE_REASON", "");
                    if (reason.isEmpty()) {
                        throw new IllegalArgumentException("SMOKE_REASON is required to skip a batch");
                    }
                    r = pipeline.skipPendingBatch(reason);
                    break;
                case "repair":
                    r = pipeline.repairDeadLetters(parseTime(env("SMOKE_FROM", "")), parseTime(env("SMOKE_TO", "")),
                            Integer.parseInt(env("SMOKE_LIMIT", "1000")));
                    break;
                case "rebuild":
                    r = pipeline.rebuild(parseTime(env("SMOKE_FROM", "")), parseTime(env("SMOKE_TO", "")));
                    break;
                default:
                    throw new IllegalArgumentException("unknown SMOKE_OP " + op);
            }
            System.out.println("SMOKE_OP op=" + op + (r == null ? " outcome=DONE" : " " + report(r)));
        }
        return 0;
    } catch (Throwable t) {
        t.printStackTrace(System.out);
        System.out.println("SMOKE_FAILED " + t);
        return 1;
    }
}
int smokeExitCode = runOp();
/exit smokeExitCode
