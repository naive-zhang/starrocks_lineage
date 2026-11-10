package com.starrocks.lineage.pipeline;

import com.starrocks.lineage.model.AuditLogRecord;
import com.starrocks.lineage.model.ColumnIdentifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * In-memory StarRocks for the pipeline unit tests: the audit table, the membership and every target table with the
 * semantics of {@code sql/ddl} (AGGREGATE SUM / MIN / MAX / REPLACE, PRIMARY KEY upserts) and of the FE labels
 * (a used label makes the write a no-op; {@link #expireLabels()} forgets them like {@code label_keep_max_second}).
 *
 * <p>Fault injection: {@link #failBefore} throws before an operation is applied, {@link #crashAfter} after it was
 * applied (the classic "write committed, process died before recording it"), {@link #beforeOp} runs arbitrary code
 * (e.g. another instance taking over). Operation names: members, dlq, dict, usage, lineage, edges, daily, refresh,
 * purge_usage, purge_daily.</p>
 */
final class FakeWarehouse implements AuditLogSource, AuditLineageSink {

    private static final Comparator<AuditLogRecord> ORDER = Comparator.comparing(AuditLogRecord::getTimestamp)
            .thenComparing(AuditLogRecord::getQueryId);

    final String pipeline;
    final List<AuditLogRecord> audit = new ArrayList<>();
    final Map<String, String> members = new HashMap<>();
    final Set<String> labels = new HashSet<>();
    final Map<String, StreamLoadClient.LoadState> labelStates = new HashMap<>();
    final Map<String, UsageCell> usage = new TreeMap<>();
    final Map<String, DailyCell> daily = new TreeMap<>();
    final Map<String, LineageCell> lineage = new TreeMap<>();
    final Map<String, LineageRowCell> batchLineage = new LinkedHashMap<>();
    final Map<String, DeadLetter> deadLetters = new TreeMap<>();
    final Map<String, BatchLogEntry> batchLog = new LinkedHashMap<>();
    final Map<String, Long> dictionary = new TreeMap<>();
    final Map<String, Integer> opCounts = new HashMap<>();
    final Map<String, Integer> failBefore = new HashMap<>();
    final Map<String, Boolean> failNonRetryable = new HashMap<>();
    final Map<String, Integer> crashAfter = new HashMap<>();
    final Map<String, Runnable> beforeOp = new HashMap<>();
    volatile Predicate<HourlyUsageRow> rejectUsage = r -> false;

    FakeWarehouse(String pipeline) {
        this.pipeline = pipeline;
    }

    // ============================================================================================ test helpers

    synchronized void addAudit(AuditLogRecord r) {
        audit.add(r);
    }

    synchronized void deleteAudit(Predicate<AuditLogRecord> which) {
        audit.removeIf(which);
    }

    synchronized void expireLabels() {
        labels.clear();
    }

    synchronized int ops(String op) {
        return opCounts.getOrDefault(op, 0);
    }

    synchronized long accessCount(LocalDateTime hour, String column) {
        UsageCell c = usage.get(hour + "|" + column);
        return c == null ? 0 : c.access;
    }

    synchronized Set<Long> users(LocalDateTime hour, String column) {
        UsageCell c = usage.get(hour + "|" + column);
        return c == null ? new HashSet<>() : new HashSet<>(c.users);
    }

    synchronized long totalAccess() {
        return usage.values().stream().mapToLong(c -> c.access).sum();
    }

    synchronized Map<String, String> usageSnapshot() {
        Map<String, String> m = new TreeMap<>();
        usage.forEach((k, v) -> m.put(k, v.toString()));
        return m;
    }

    synchronized Map<String, String> lineageSnapshot() {
        Map<String, String> m = new TreeMap<>();
        lineage.forEach((k, v) -> m.put(k, v.toString()));
        return m;
    }

    synchronized List<DeadLetter> letters(DeadLetter.Kind kind) {
        return deadLetters.values().stream().filter(d -> d.getKind() == kind).collect(Collectors.toList());
    }

    synchronized DeadLetter letter(String queryId, DeadLetter.Kind kind) {
        return deadLetters.get(queryId + "/" + kind);
    }

    private synchronized void op(String name) {
        opCounts.merge(name, 1, Integer::sum);
        Runnable hook = beforeOp.remove(name);
        if (hook != null) {
            hook.run();
        }
        Integer n = failBefore.get(name);
        if (n != null && n > 0) {
            failBefore.put(name, n - 1);
            String msg = "injected failure before " + name;
            throw Boolean.TRUE.equals(failNonRetryable.get(name)) ? PipelineException.nonRetryable(msg, null)
                    : new PipelineException(msg);
        }
    }

    private synchronized void crash(String name) {
        Integer n = crashAfter.get(name);
        if (n != null && n > 0) {
            crashAfter.put(name, n - 1);
            throw new PipelineException("injected crash after " + name);
        }
    }

    /** @return {@code true} when the label was free (and is now used) */
    private boolean useLabel(String label) {
        return labels.add(label);
    }

    private static StreamLoadClient.Result result(String label, boolean already, long rows, long filtered) {
        return new StreamLoadClient.Result(label, already, rows, filtered, "");
    }

    // ============================================================================================ source

    private List<AuditLogRecord> select(Predicate<AuditLogRecord> p, LocalDateTime from, LocalDateTime to,
                                        Watermark after, int limit) {
        return audit.stream()
                .filter(r -> !r.getTimestamp().isBefore(from) && r.getTimestamp().isBefore(to))
                .filter(r -> after == null || new Watermark(r.getTimestamp(), r.getQueryId()).compareTo(after) > 0)
                .filter(p).sorted(ORDER).limit(limit).collect(Collectors.toList());
    }

    private static String key(AuditLogRecord r) {
        return r.getQueryId() + "@" + r.getTimestamp();
    }

    @Override
    public synchronized List<AuditLogRecord> fetchRange(LocalDateTime from, LocalDateTime to, Watermark after,
                                                        int limit) {
        return select(r -> true, from, to, after, limit);
    }

    @Override
    public synchronized List<AuditLogRecord> fetchMembers(String p, String batchId, LocalDateTime from,
                                                          LocalDateTime to, Watermark after, int limit) {
        return select(r -> batchId.equals(members.get(key(r))), from, to, after, limit);
    }

    @Override
    public synchronized List<AuditLogRecord> fetchProcessed(String p, LocalDateTime from, LocalDateTime to,
                                                            Watermark after, int limit) {
        return select(r -> members.containsKey(key(r)), from, to, after, limit);
    }

    @Override
    public synchronized List<AuditLogRecord> fetchUnprocessed(String p, LocalDateTime from, LocalDateTime to,
                                                              int limit) {
        return select(r -> !members.containsKey(key(r)), from, to, null, limit);
    }

    @Override
    public synchronized long countMembers(String p, LocalDateTime from, LocalDateTime to) {
        return members.keySet().stream().map(k -> LocalDateTime.parse(k.substring(k.indexOf('@') + 1)))
                .filter(t -> !t.isBefore(from) && t.isBefore(to)).count();
    }

    @Override
    public synchronized long countProcessed(String p, LocalDateTime from, LocalDateTime to) {
        return audit.stream().filter(r -> members.containsKey(key(r)))
                .filter(r -> !r.getTimestamp().isBefore(from) && r.getTimestamp().isBefore(to)).count();
    }

    // ============================================================================================ sink

    @Override
    public String label(String batchId, String step) {
        return batchId + "_" + step;
    }

    @Override
    public synchronized StreamLoadClient.Result writeMembers(String batchId, LocalDateTime batchTime,
                                                             List<AuditLogRecord> records) {
        op("members");
        String label = label(batchId, "members");
        if (useLabel(label)) {
            records.forEach(r -> members.put(key(r), batchId));
        }
        crash("members");
        return result(label, false, records.size(), 0);
    }

    @Override
    public synchronized StreamLoadClient.Result writeDeadLetters(String batchId, String tag,
                                                                 List<DeadLetter> letters) {
        op("dlq");
        String label = label(batchId, "dlq_" + tag);
        if (useLabel(label)) {
            letters.forEach(d -> deadLetters.put(d.getQueryId() + "/" + d.getKind(), d));
        }
        crash("dlq");
        return result(label, false, letters.size(), 0);
    }

    @Override
    public synchronized StreamLoadClient.Result writeUserDictionary(String batchId, List<HourlyUsageRow> rows) {
        op("dict");
        if (useLabel(label(batchId, "dict"))) {
            rows.forEach(r -> dictionary.put(r.getUserName(), UserIds.idOf(r.getUserName())));
        }
        crash("dict");
        return result(label(batchId, "dict"), false, rows.size(), 0);
    }

    @Override
    public synchronized StreamLoadClient.Result writeUsage(String batchId, List<HourlyUsageRow> rows) {
        op("usage");
        String label = label(batchId, "usage");
        if (!useLabel(label)) {
            return result(label, true, 0, 0);
        }
        long rejected = rows.stream().filter(rejectUsage).count();
        if (rejected > 0) {
            DeadLetter evidence = new DeadLetter(label, DeadLetter.Kind.LOAD_FILTERED, DeadLetter.Status.OPEN,
                    batchId, null, LocalDateTime.of(2000, 1, 1, 0, 0), rejected + " rows rejected", "http://err",
                    null);
            deadLetters.put(label + "/" + DeadLetter.Kind.LOAD_FILTERED, evidence);
        }
        for (HourlyUsageRow r : rows) {
            if (rejectUsage.test(r)) {
                continue;
            }
            UsageCell c = usage.computeIfAbsent(r.getStatHour() + "|" + r.getColumn().getColumn(),
                    k -> new UsageCell());
            c.access += r.getAccessQueryCnt();
            c.select += r.getSelectClauseCnt();
            c.where += r.getWhereClauseCnt();
            c.users.add(UserIds.idOf(r.getUserName()));
        }
        crash("usage");
        return result(label, false, rows.size() - rejected, rejected);
    }

    @Override
    public synchronized StreamLoadClient.Result writeBatchLineage(String batchId, LocalDateTime batchTime,
                                                                  List<LineageRow> rows) {
        op("lineage");
        String label = label(batchId, "lineage");
        if (useLabel(label)) {
            for (LineageRow r : rows) {
                batchLineage.put(batchId + "|" + r.getEdgeId() + "|" + r.getStatDay() + "|false|" + batchTime,
                        new LineageRowCell(batchId, r, false));
            }
        }
        crash("lineage");
        return result(label, false, rows.size(), 0);
    }

    @Override
    public synchronized void recordEdgesForRefresh(String batchId, LocalDateTime batchTime, LocalDateTime fromDay,
                                                   LocalDateTime toDay) {
        op("edges");
        if (useLabel(label(batchId, "refresh_edges"))) {
            daily.values().stream().filter(d -> !d.row.getStatDay().isBefore(fromDay)
                    && d.row.getStatDay().isBefore(toDay)).forEach(d -> batchLineage.put(batchId + "|"
                    + d.row.getEdgeId() + "|" + d.row.getStatDay() + "|true|" + batchTime,
                    new LineageRowCell(batchId, d.row, true)));
        }
        crash("edges");
    }

    @Override
    public synchronized void insertDaily(String batchId) {
        op("daily");
        if (useLabel(label(batchId, "daily"))) {
            for (LineageRowCell c : batchLineage.values()) {
                if (c.batchId.equals(batchId) && !c.refreshOnly) {
                    String k = c.row.getStatDay() + "|" + c.row.getEdgeId();
                    DailyCell d = daily.get(k);
                    daily.put(k, new DailyCell(d == null ? c.row : d.row.merge(c.row)));
                }
            }
        }
        crash("daily");
    }

    @Override
    public synchronized void refreshLineage(String batchId) {
        op("refresh");
        Set<String> edges = batchLineage.values().stream().filter(c -> c.batchId.equals(batchId))
                .map(c -> c.row.getEdgeId()).collect(Collectors.toSet());
        for (String edge : edges) {
            List<LineageRow> facts = daily.values().stream().map(d -> d.row).filter(r -> r.getEdgeId().equals(edge))
                    .sorted(Comparator.comparing(LineageRow::getStatDay)).collect(Collectors.toList());
            if (facts.isEmpty()) {
                lineage.remove(edge);
                continue;
            }
            LineageRow latest = facts.get(facts.size() - 1);
            LocalDateTime first = facts.stream().map(LineageRow::getFirstSeenTime).min(LocalDateTime::compareTo).get();
            LocalDateTime last = facts.stream().map(LineageRow::getLastSeenTime).max(LocalDateTime::compareTo).get();
            long count = facts.stream().mapToLong(LineageRow::getExecCount).sum();
            lineage.put(edge, new LineageCell(latest.getEdge().getTarget(), latest.getEdge().getSource(),
                    latest.getLastQueryId(), first, last, count));
        }
        crash("refresh");
    }

    @Override
    public synchronized void purgeUsage(LocalDateTime from, LocalDateTime to) {
        op("purge_usage");
        usage.keySet().removeIf(k -> {
            LocalDateTime hour = LocalDateTime.parse(k.substring(0, k.indexOf('|')));
            return !hour.isBefore(from) && hour.isBefore(to);
        });
        crash("purge_usage");
    }

    @Override
    public synchronized void purgeLineageDaily(LocalDateTime fromDay, LocalDateTime toDay) {
        op("purge_daily");
        daily.values().removeIf(d -> !d.row.getStatDay().isBefore(fromDay) && d.row.getStatDay().isBefore(toDay));
        crash("purge_daily");
    }

    @Override
    public synchronized List<DeadLetter> deadLetters(Set<DeadLetter.Status> statuses, Set<DeadLetter.Kind> kinds,
                                                     LocalDateTime from, LocalDateTime to, String repairBatchId,
                                                     int limit) {
        return deadLetters.values().stream()
                .filter(d -> statuses.contains(d.getStatus()) && kinds.contains(d.getKind()))
                .filter(d -> !d.getAuditTime().isBefore(from) && d.getAuditTime().isBefore(to))
                .filter(d -> repairBatchId == null || repairBatchId.equals(d.getRepairBatchId()))
                .sorted(Comparator.comparing(DeadLetter::getAuditTime).thenComparing(DeadLetter::getQueryId))
                .limit(limit).collect(Collectors.toList());
    }

    @Override
    public synchronized List<DeadLetter> deadLettersOfBatch(String batchId, LocalDateTime from, LocalDateTime to) {
        return deadLetters.values().stream().filter(d -> d.getBatchId().equals(batchId))
                .filter(d -> !d.getAuditTime().isBefore(from) && d.getAuditTime().isBefore(to))
                .collect(Collectors.toList());
    }

    @Override
    public synchronized StreamLoadClient.LoadState loadState(String label) {
        StreamLoadClient.LoadState s = labelStates.get(label);
        if (s != null) {
            return s;
        }
        return labels.contains(label) ? StreamLoadClient.LoadState.COMMITTED : StreamLoadClient.LoadState.UNKNOWN;
    }

    @Override
    public synchronized void writeBatchLog(BatchLogEntry entry) {
        batchLog.put(entry.getBatchId(), entry);
    }

    @Override
    public synchronized List<Long> userIdCollisions() {
        Map<Long, Set<String>> byId = new HashMap<>();
        dictionary.forEach((n, id) -> byId.computeIfAbsent(id, k -> new HashSet<>()).add(n));
        return byId.entrySet().stream().filter(e -> e.getValue().size() > 1).map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    // ============================================================================================ rows

    /** One (hour, column) row of audit_column_hourly_usage. */
    static final class UsageCell {
        long access;
        long select;
        long where;
        final Set<Long> users = new HashSet<>();

        @Override
        public String toString() {
            return "access=" + access + ",select=" + select + ",where=" + where + ",users=" + new TreeMap<>(
                    users.stream().collect(Collectors.toMap(u -> u, u -> 1)));
        }
    }

    /** One (day, edge) row of audit_column_lineage_daily. */
    static final class DailyCell {
        final LineageRow row;

        DailyCell(LineageRow row) {
            this.row = row;
        }
    }

    /** One row of audit_lineage_batch_lineage. */
    static final class LineageRowCell {
        final String batchId;
        final LineageRow row;
        final boolean refreshOnly;

        LineageRowCell(String batchId, LineageRow row, boolean refreshOnly) {
            this.batchId = batchId;
            this.row = row;
            this.refreshOnly = refreshOnly;
        }
    }

    /** One row of audit_column_lineage. */
    static final class LineageCell {
        final ColumnIdentifier target;
        final ColumnIdentifier source;
        final String lastQueryId;
        final LocalDateTime first;
        final LocalDateTime last;
        final long execCount;

        LineageCell(ColumnIdentifier target, ColumnIdentifier source, String lastQueryId, LocalDateTime first,
                    LocalDateTime last, long execCount) {
            this.target = target;
            this.source = source;
            this.lastQueryId = Objects.requireNonNull(lastQueryId);
            this.first = first;
            this.last = last;
            this.execCount = execCount;
        }

        @Override
        public String toString() {
            return source + "->" + target + " count=" + execCount + " first=" + first + " last=" + last + " q="
                    + lastQueryId;
        }
    }
}
