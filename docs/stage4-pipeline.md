# Stage 4：审计日志增量 Pipeline、内存小时级预聚合器与高难度 JUnit 5 验收

> 状态：**终审签字（FINAL SIGN-OFF）**。`mvn test` 共 311 个用例全部通过，其中 Stage 4 新增 44 个。
> 代码位于 `com.starrocks.lineage.pipeline`。
> 验收套件为 `com.starrocks.lineage.analyzer.StarRocksSqlAnalyzerTest`（8 类高难度 SQL，共 20 个用例）。
>
> **⚠ 已被 Stage 5 取代的部分**：本文描述的 Pipeline 存储模型（stage 表 + `last_batch_id` 合并、计数器式用户字典、动态分区、按水位重放）已在 [Stage 5](stage5-reliability.md) 中重构。下文 R4-09 和 R4-10 两条结论是**错误的**，已在 Stage 5 第 3.4 节更正。本文保留作为历史记录。

---

## 📐 STEP 1：【首席架构师】架构设计与契约定义

### 1.1 端到端 Pipeline

```mermaid
flowchart LR
    subgraph SR[StarRocks 3.1]
        A[(starrocks_audit_tbl__)]
        CK[(audit_lineage_sync_checkpoint<br/>水位 + 租约 + 批次意图)]
        D[(audit_user_dict)]
        U[(audit_column_hourly_usage<br/>AGGREGATE)]
        ST[(audit_column_lineage_stage)]
        L[(audit_column_lineage<br/>PRIMARY KEY + last_batch_id)]
    end
    subgraph P[AuditLogSyncPipeline.runOnce]
        LS[① 抢占/续约租约]
        RD[② 有意图：精确重放 (wm, end]<br/>无意图：keyset 分页拉取新记录<br/>上界 = now - lateArrival<br/>持久化 PendingBatch]
        AN[③ N 线程并发分析<br/>StarRocksSqlAnalyzer]
        AG[④ HourlyUsageAggregator (小时, 列, 用户)<br/>LineageAggregator (edge)]
        W[⑤ 字典 → 用量 → 血缘 stage + 带防重条件的合并]
        CM[⑥ 提交水位、清除意图、回读校验]
    end
    CK --> LS --> RD
    A --> RD --> AN --> AG --> W --> CM --> CK
    W -->|Stream Load label=batch_dict_n| D
    W -->|Stream Load label=batch_usage_n| U
    W -->|Stream Load label=batch_stage_n| ST
    ST -->|INSERT…SELECT WHERE last_batch_id ≠ batch| L
```

### 1.2 Exactly-once 论证

StarRocks 3.1 没有跨语句事务，因此"每一步都幂等，再加上重放的批次必须完全相同"是唯一可行的路线。

| 步骤 | 失败后果 | 恢复机制 |
|---|---|---|
| ② 持久化意图之前 | 什么都没写 | 下一周期重新拉取 |
| ② 之后、⑤ 之中的任意位置 | 可能已经写入了部分数据 | 下一周期读到意图，按**同一区间、同一 batchId** 重放 |
| ⑤ 用户字典 | 已经写入 | PK upsert 本身幂等；重放时重新加载字典，同一用户得到相同的 id |
| ⑤ 用量和暂存表的 Stream Load | 已经写入 | 标签 `<batchId>_<kind>_<n>` 重复时，StarRocks 返回 `Label Already Exists`（FINISHED），客户端视为已加载 |
| ⑤ 血缘合并 | 已经合并 | 单条原子 `INSERT…SELECT … WHERE o.last_batch_id <> ?`，重放时写入 0 行 |
| ⑥ 提交 | 没有生效 | 回读校验，不一致就抛错，绝不会静默地无限重放 |

### 1.3 并发与内存模型

- **分析阶段**：使用固定大小的线程池执行 `invokeAll`，结果按记录顺序收集。`StarRocksSqlAnalyzer`、`StarRocksSqlParser` 以及 Caffeine 元数据缓存都是线程安全的。
- **预聚合阶段**：
  - 写入时：多个线程共享读锁，每个 key 用 `ConcurrentHashMap.compute` 原子更新；
  - 读出时：`drain` 获取写锁并整体替换 map，保证每一次 `add` 要么完整落在本批，要么完整落在下一批，不会被拆开。
- **key 的判等**：用全部字段的 `equals`（小时、列、用户），哈希只用于分桶，因此哈希冲突的两个 key 不会被合并成一行。
- **内存上限**：每个周期最多处理 `maxRecordsPerCycle` 条记录（默认 20 万），聚合器每个周期都重新创建，结束即释放。
- **调度**：使用 `scheduleWithFixedDelay`。如果一个周期正好处理满了上限，说明还有积压，立即继续下一周期，直到追平。`runOnce` 内部加锁，保证不会有两个周期同时执行。

### 1.4 八类验收用例规划

| # | 场景 | 验证要点 |
|---|---|---|
| 1 | 3 层 CTE + `RANK() OVER` + `UNION ALL`，写入不带列名的 INSERT | 精确的 DIRECT 边集合（7 条）；两个分支列名不同时仍按位置合并；6 个行控制列计为 INDIRECT；CTE 名不会被当作源表 |
| 2 | `INSERT INTO t SELECT t1.*`，目标表列名与源表完全不同 | 按 schema 顺序逐一对应（6 条），每列 3 条 INDIRECT，边总数精确为 24 |
| 3 | `LATERAL UNNEST(u.tags, u.scores) AS t(tag, score)` | 虚拟列还原为物理数组（EXPRESSION）；用量中只出现 `TABLE_FUNCTION` 和 `SELECT`，不产生虚构的列 |
| 4 | `array_map((x, y) -> x * y, scores, weights)`，同时存在物理列 `x` | 形参绑定到数组实参；`ods_users.x` 不会出现在血缘中；用量中 `x` 只计 1 次 |
| 5 | `WHERE EXISTS … AND NOT EXISTS …` 关联子查询 | 10 个列各子句的计数逐一精确断言 |
| 6 | `CREATE MATERIALIZED VIEW (cols) … REFRESH ASYNC EVERY … AS SELECT` 带 JOIN 和聚合 | 5 条 DIRECT 边（`SUM/COUNT(*)` 计为 AGGREGATION），每列 5 条 INDIRECT，`transform_expr` 保留原文 |
| 7 | 单条 SQL 中同一字段出现 9 次 | `access`、`select`、`where`、`join`、`order_by` 各为 1，`group_by` 为 0，原始出现次数 3/2/1/2；再交给小时聚合器，验证按用户、按小时切桶以及 SUM/MAX |
| 8 | 多段脏注释、hint、字符串中的伪注释、`#` 注释；12 种管理语句；失败语句和空语句 | 清洗后语义正确；分别归为 SKIPPED_MANAGEMENT、SKIPPED_FAILED_QUERY、SKIPPED_EMPTY |

---

## 💻 STEP 2：【核心开发工程师】生产级代码实现

| 文件 | 职责 |
|---|---|
| `Watermark` | `(timestamp, queryId)` 全序水位 |
| `AuditLogSource` / `JdbcAuditLogSource` | 基于 keyset 的分页拉取（3.1 不支持行值比较，所以展开成 OR 条件）；保留 `timestamp >=` 前导条件以利用分区裁剪 |
| `CheckpointStore` / `JdbcCheckpointStore` | 水位、单写者租约（尽力而为）、批次意图 `PendingBatch`；提交后回读校验 |
| `HourlyUsageAggregator` / `HourlyUsageRow` | 按 `(小时, 列, 用户)` 预聚合，读写锁加原子 compute |
| `LineageAggregator` / `LineageRow` | 按血缘边预聚合；count 累加，first_seen 取最小值，last_* 取最新一次（时间相同时按 queryId 决定，结果与线程执行顺序无关） |
| `UserDictionary` | 稠密且无碰撞的 user_id（替代 3.1 中不存在的 `bitmap_hash64`），线程安全 |
| `StreamLoadClient` | 手动跟随 307 重定向（保留 Authorization 和请求体）；`Expect: 100-continue`；标签幂等判定；`Publish Timeout` 视为成功；正在运行的同名任务可重试 |
| `AuditLineageSink` / `StarRocksSink` | 用量：每 (小时, 列, 用户) 一行，加上 `to_bitmap(user_id)` 和 `hll_hash(user_name)` 列映射；按块 Stream Load；血缘合并 SQL 带 `last_batch_id` 防重；记录合并日志 |
| `AuditLogSyncPipeline` / `PipelineConfig` / `PipelineMetrics` / `CycleReport` | 周期编排；建表、建视图、建物化视图后让对应的元数据缓存失效；指标统计；调度 |

DDL 同步更新：
- `01`：增加 `last_batch_id` 列；
- `02`：合并 SQL 改用可移植的 CASE 写法并加上防重条件；
- `05`：增加 `pending_batch_id`、`pending_end_time`、`pending_end_query_id` 三列。

测试（44 个）：
- `StarRocksSqlAnalyzerTest`（20 个）：8 类验收用例；
- `AggregatorsTest`（7 个）：哈希冲突、计数与小时切桶、16 线程并发写入同时反复 drain、血缘聚合、相同时间下的取值规则、用户字典并发、水位与 batchId；
- `StreamLoadAndSinkTest`（7 个）：重定向与认证、标签幂等、失败处理、运行中的任务、块切分与 user_id、字典往返、**合并幂等**；
- `AuditLogSyncPipelineTest`（9 个）：端到端、**合并后崩溃的精确重放**、元数据中断恢复、相同时间戳下的翻页、租约、缓存失效、后台调度、R4-05、配置校验；
- `JdbcCheckpointStoreTest`（1 个）：租约过期后接管并继承批次意图。

测试替身 `FakeStarRocks` 用 JDK HttpServer 模拟 FE 到 BE 的 307 重定向，并实现 Basic 认证、100-continue 和标签唯一性。数据库使用 H2（MySQL 模式）：暂存表和字典的 Stream Load 数据真实写入 H2，合并 SQL 也在 H2 上真实执行。

---

## 🔍 STEP 3：【首席架构师】Code Review 与【核心开发工程师】修正闭环（终审）

### 3.1 发现与修正

| # | 级别 | 维度 | 发现 | 修正 | 验证 |
|---|---|---|---|---|---|
| R4-01 | 阻断 | 幂等 | Stage 1 设计的"先 MERGE、再写 merge_log、按 log 判重"有崩溃窗口：两者之间崩溃会导致重放时再次合并，`exec_count` 被重复累加。 | 血缘表增加 `last_batch_id`；合并改为单条原子 `INSERT…SELECT … WHERE o.last_batch_id <> ?`。merge_log 只用于观测。 | `lineageMergeAccumulatesAndReplayIsANoOp`（重放后返回 0 行，计数仍为 7） |
| R4-02 | 阻断 | 幂等 | 重试时"现在"已经变了，同一批次可能多拉到新记录，于是 batchId 和标签都不同，用量被重复导入。 | 在任何写入之前持久化批次意图（id 和结束水位）；失败后按 `(wm, end]` **精确重放**同一 batchId；期间新到的记录进入下一批。 | `failedCycleIsReplayedExactlyOnce`（新记录 q08 不会混入重放批次） |
| R4-03 | 主要 | 正确性 | 规范要求的 `bitmap_hash64(user)` 在 3.1 中不存在（Stage 1 R-04 已发现），而 32 位 `bitmap_hash` 会碰撞，无法做到精确 UV。 | 使用 `UserDictionary` 分配稠密 id，再 `to_bitmap(user_id)`；每 (小时, 列, 用户) 一行，聚合表负责做位图并集。 | `usageRowsMapUserIdsAndChunkWithDeterministicLabels` |
| R4-04 | 主要 | 预聚合 Hash 冲突 | 如果把复合 key 压成一个 long 哈希来当身份标识，哈希冲突时两行会被静默合并。 | key 用全部字段的 `equals`，哈希只用于分桶。 | `hashCollidingKeysStaySeparate`（`"Aa"` 与 `"BB"` 的 hashCode 相同，仍是 4 行） |
| R4-05 | 主要 | 活性 | 联调中实测发现：检查点表如果缺少主键，`INSERT` 会变成追加写入，提交"静默失效"，导致同一批次被无限重放（测试曾因此卡死，用 jstack 定位）。 | 提交后回读校验水位和意图，不一致就抛 `PipelineException`；检查点存储也支持可配置的 upsert 语义。 | `unpersistedCommitFailsInsteadOfLoopingForever` |
| R4-06 | 主要 | 并发 | 如果 `drain()` 与 `add()` 并发执行时不做同步，一次查询的多个列可能被拆到两个批次，某一批次的计数会短暂偏低。 | 使用读写锁：`add` 持读锁，`drain` 持写锁并整体替换 map。 | `concurrentAddsAndDrainsLoseNothing`（16 线程 × 2000 次，SUM 精确） |
| R4-07 | 次要 | 确定性 | 同一条边在同一时刻被两条语句产生时，`last_query_id` 取决于线程调度顺序。 | 时间相同时按 queryId 的字典序决定，结果与执行顺序无关。 | `lineageMergeTieBreakIsOrderIndependent` |
| R4-08 | 次要 | 元数据新鲜度 | 同一批次之后的查询，可能还看不到刚建好的 CTAS 表、视图或物化视图的 schema。 | 遇到这些语句时主动让对应的元数据缓存失效。 | `createdObjectsAreInvalidatedInTheMetadataCache` |
| R4-09 | ~~接受风险~~ **已更正** | 迟到数据 | 重放区间内如果有迟到写入的审计记录（时间早于 end，但在首次拉取之后才写入），它们的用量会因为标签相同而被跳过。 | ~~`lateArrival` 窗口可以把这种风险降到可忽略的程度~~。**更正（Stage 5）**：这个判断是错的，插件积压时审计写入完全可能晚于窗口。现在用成员表反连接做对账，把这些记录作为 LATE 批次补算。 | Stage 5 `Gap6LateArrivals`、IT `auditRowsArrivingDuringAnInterruptedBatchGoToReconciliation` |
| R4-10 | ~~接受风险~~ **已更正** | 租约 | StarRocks 没有 CAS 操作，所以租约只是"尽力而为"。 | ~~正确性由批次意图和幂等写入保证，租约只用来避免重复工作~~。**更正（Stage 5）**：这个结论是错的。两个实例各自声明批次、暂停后被接管的僵尸实例继续写入、计数器式用户 id 的竞态，都会破坏正确性。现在每一步写入前都做隔离校验，SLICE 的 id 是确定性的（泄漏的写入会被标签去重），用户 id 改为无状态哈希。 | Stage 5 `Gap3Fencing`、IT `aPausedInstanceIsFencedOnARealCheckpoint` |

### 3.2 铁律终审核查（全项目）

1. **`.g4` 节点对齐**：官方 `.g4` 逐字节一致（sha256 校验）；79 个 `visitXxx` 重写；节点矩阵有 53 + 53 个反射测试。✅
2. **CTE 与子查询作用域**：用例 1 的三层 CTE 链组合正确；Stage 2 和 3 的屏蔽、关联、泄漏检测均有测试。✅
3. **`*` 与省略列名的 INSERT**：用例 2 中目标列名完全不同，6 列仍按位置对齐；未知表上的 `*` 不做任何猜测。✅
4. **集合操作按位置对齐**：用例 1 的两个 UNION ALL 分支列名不同，按下标正确合并。✅
5. **复杂语法穿透**：用例 3（UNNEST）、用例 4（Lambda，有同名物理列 `x` 干扰）、用例 1 和 6（窗口与 CASE）。✅
6. **子句上下文栈**：用例 5 中 10 个列各子句的计数逐一精确断言。✅
7. **单 Query 去重**：用例 7 中 9 处引用最终每个指标都为 1，并验证了小时上卷时 SUM 和 MAX 的正确性。✅
8. **零占位符**：全仓库 `grep` 检查，没有 TODO 或省略；唯一的 TODO 来自上游 `.g4` 原文。✅

**并发安全**：分析器、解析器、元数据缓存、聚合器、用户字典都是线程安全的；周期之间互斥；有 16 线程压力测试。✅

**Hash 冲突**：没有任何地方用哈希值当身份标识；`edge_id` 使用 SHA-256 的 64 位十六进制串，碰撞概率可以忽略。✅

**断言严密性**：8 类验收用例全部断言精确的边集合（`assertEquals(Set)`），而不是"至少包含"；计数逐字段断言；边总数和列总数也有精确断言，防止多出多余的边。✅

### 3.3 终审签字

【首席架构师】Stage 4 **APPROVED**，项目四个阶段**全部签字（FINAL SIGN-OFF）**。

尚未在真实的 StarRocks 3.1 集群上验证的事项：
- DDL 的实际建表；
- Stream Load 的实际行为，尤其是 `columns` 映射中的 `to_bitmap` 和 `hll_hash`；
- 合并 SQL 在 PK 表上的 upsert 语义。

以上已用 H2 与 HTTP 测试替身做了最大程度的模拟，上线前建议在预发集群执行一次 `sql/ddl/*` 并跑一个周期做冒烟验证。
