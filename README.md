# StarRocks Audit Column Lineage & Hourly Usage Engine

Parses statements recorded in `starrocks_audit_db__.starrocks_audit_tbl__` with the **unmodified** StarRocks
branch-3.1 ANTLR4 grammar and produces:

- `audit_column_lineage`: column-level lineage of write statements (INSERT, CTAS, views, materialized views, UPDATE, DELETE, SUBMIT TASK);
- `audit_column_hourly_usage`: hourly column access statistics of queries (clause frequencies, exact and approximate distinct users, resource water levels).

Batches are exactly-once under crashes, restarts, take-overs and replays after the StarRocks label retention; late
audit records are reconciled, failed records are kept as dead letters and can be repaired, and whole days can be
rebuilt (Stage 5).

## Build

Requires JDK 11+ and Maven 3.6+.

```bash
mvn clean test
```

Integration tests against a real StarRocks 3.1.17 started with Testcontainers (requires Docker; skipped without it):

```bash
mvn verify -Pit
```

See [docs/container-verification.md](docs/container-verification.md) for what the container tests cover and
[docs/staging-smoke-test.md](docs/staging-smoke-test.md) for the staging smoke-test runbook (`scripts/smoke`).

## Layout

| Path | Content |
|---|---|
| `src/main/antlr4/com/starrocks/sql/parser/` | Official `StarRocks.g4` / `StarRocksLex.g4` (byte-identical to upstream branch-3.1) |
| `src/main/java/com/starrocks/qe/` | `SqlModeHelper` shim required by the code embedded in the official lexer |
| `src/main/java/com/starrocks/sql/parser/` | Case-insensitive stream, fail-fast error listener, SLL→LL parser, SQL sanitizer |
| `src/main/java/com/starrocks/lineage/model/` | Domain model (identifiers, lineage edges, usage stats, parse results, audit record) |
| `src/main/java/com/starrocks/lineage/metadata/` | `MetadataProvider` with JDBC (Caffeine-cached) and in-memory implementations |
| `src/main/java/com/starrocks/lineage/scope/` | Symbol table: `Scope` / `ScopeStack` / `RelationDescriptor` / `RelationField` / `FieldLineage` / `ExpressionSourceTracker` |
| `src/main/java/com/starrocks/lineage/analyzer/` | `StarRocksAuditAstVisitor` (79 official visit methods) and the `StarRocksSqlAnalyzer` facade |
| `src/main/java/com/starrocks/lineage/pipeline/` | `AuditLogSyncPipeline`: time-slice batches with persisted step progress, fenced lease, batch membership, label-state guard, reconciliation of late records, dead letters, repair / rebuild; hourly and daily pre-aggregation; Stream Load sink with rejected-row isolation |
| `sql/ddl/` | StarRocks 3.1 DDL of every target and bookkeeping table (expression partitioned) |
| `scripts/smoke/` | Staging smoke-test scripts and the operator tool `smoke-ops.jsh` (status, redo, mark-done, skip, repair, rebuild) |
| `sql/query/` | Standard business queries |
| `docs/` | Stage design documents and architect review logs |

## Stages

| Stage | Scope | Status |
|---|---|---|
| 1 | Architecture, DDL, Maven, `.g4` compatibility base, domain model, metadata | ✅ [docs/stage1-architecture.md](docs/stage1-architecture.md) |
| 2 | Symbol table and scope stack engine | ✅ [docs/stage2-scope-engine.md](docs/stage2-scope-engine.md) |
| 3 | `StarRocksAuditAstVisitor` and `StarRocksSqlAnalyzer` | ✅ [docs/stage3-ast-visitor.md](docs/stage3-ast-visitor.md) |
| 4 | Incremental pipeline, hourly pre-aggregation, acceptance tests | ✅ [docs/stage4-pipeline.md](docs/stage4-pipeline.md) |
| 5 | Reliability: first run across days, poison batches, fencing, label retention, deterministic replay, late records, dead letters / repair / rebuild | ✅ [docs/stage5-reliability.md](docs/stage5-reliability.md) |
| 6 | Acceptance on 30 production INSERT statements with the full production schema (997 tables), cross-checked on StarRocks 3.1.17; Stream Load routing fix for large loads | ✅ [docs/stage6-production-inserts.md](docs/stage6-production-inserts.md) |
| 7 | Standalone jar (`java -jar ... check / analyze / run / daemon / status / ...`) for production | ✅ [docs/stage7-standalone-jar.md](docs/stage7-standalone-jar.md), deployment: [docs/production-deployment.md](docs/production-deployment.md) |

## Running the pipeline

```java
HikariDataSource ds = ...; // jdbc:mysql://fe-host:9030
MetadataProvider metadata = new JdbcStarRocksMetadataProvider(ds);
StreamLoadClient streamLoad = new StreamLoadClient(URI.create("http://fe-host:8030"), "loader", "***",
        Duration.ofMinutes(5));
AuditLogSyncPipeline pipeline = new AuditLogSyncPipeline(
        PipelineConfig.builder("audit_lineage", hostname).build(),   // a short, stable name; one owner per instance
        new JdbcAuditLogSource(ds), new JdbcCheckpointStore(ds),
        new StarRocksSink(ds, streamLoad, "audit_lineage_db", "audit_lineage"),
        new StarRocksSqlAnalyzer(metadata), metadata, Clock.systemDefaultZone());
pipeline.start(Duration.ofMinutes(1));
// alert on pipeline.getMetrics().isBlocked() / getConsecutiveFailures(); operations:
// pipeline.resolveBlockedStep(...), skipPendingBatch(reason), repairDeadLetters(from, to, limit), rebuild(fromDay, toDay)
```

Create the tables first with `sql/ddl/00..09`.
