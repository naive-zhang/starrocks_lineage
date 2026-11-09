# StarRocks Audit Column Lineage & Hourly Usage Engine

Parses statements recorded in `starrocks_audit_db__.starrocks_audit_tbl__` with the **unmodified** StarRocks
branch-3.1 ANTLR4 grammar and produces:

- `audit_column_lineage`: column-level lineage of write statements (INSERT, CTAS, views, materialized views, UPDATE, DELETE, SUBMIT TASK);
- `audit_column_hourly_usage`: hourly column access statistics of queries (clause frequencies, exact and approximate distinct users, resource water levels).

## Build

Requires JDK 11+ and Maven 3.6+.

```bash
mvn clean test
```

## Layout

| Path | Content |
|---|---|
| `src/main/antlr4/com/starrocks/sql/parser/` | Official `StarRocks.g4` / `StarRocksLex.g4` (byte-identical to upstream branch-3.1) |
| `src/main/java/com/starrocks/qe/` | `SqlModeHelper` shim required by the code embedded in the official lexer |
| `src/main/java/com/starrocks/sql/parser/` | Case-insensitive stream, fail-fast error listener, SLL→LL parser, SQL sanitizer |
| `src/main/java/com/starrocks/lineage/model/` | Domain model (identifiers, lineage edges, usage stats, parse results, audit record) |
| `src/main/java/com/starrocks/lineage/metadata/` | `MetadataProvider` with JDBC (Caffeine-cached) and in-memory implementations |
| `sql/ddl/` | StarRocks 3.1 DDL of every target table |
| `sql/query/` | Standard business queries |
| `docs/` | Stage design documents and architect review logs |

## Stages

| Stage | Scope | Status |
|---|---|---|
| 1 | Architecture, DDL, Maven, `.g4` compatibility base, domain model, metadata | ✅ [docs/stage1-architecture.md](docs/stage1-architecture.md) |
| 2 | Symbol table and scope stack engine | pending |
| 3 | `StarRocksAuditAstVisitor` and `StarRocksSqlAnalyzer` | pending |
| 4 | Incremental pipeline, hourly pre-aggregation, acceptance tests | pending |
