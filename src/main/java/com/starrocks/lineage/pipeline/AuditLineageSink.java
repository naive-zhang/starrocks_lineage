package com.starrocks.lineage.pipeline;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Destination of a pipeline batch. Every write is <b>idempotent per batch id</b>: re-writing a batch that was
 * (partially) written before must not duplicate counts. The pipeline writes in this order: user dictionary, usage,
 * lineage.
 */
public interface AuditLineageSink {

    /** @return persisted user dictionary (user name → id) */
    Map<String, Long> loadUserDictionary();

    /** Persists new dictionary entries (must complete before usage rows referencing them are written). */
    void writeUserDictionary(String batchId, List<UserDictionary.Entry> entries, LocalDateTime seenAt);

    /** Loads hourly usage rows; user ids come from {@code dictionary}. */
    void writeUsage(String batchId, List<HourlyUsageRow> rows, UserDictionary dictionary);

    /** Stages and merges lineage rows into the lineage table. */
    void writeLineage(String batchId, List<LineageRow> rows);
}
