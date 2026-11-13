# Stage 9：分块写入、按小时重建、进度与多线程

> 状态：**终审签字（FINAL SIGN-OFF）**。
>
> 起因：生产上执行 `rebuild` 很长时间没有任何输出，最后失败，报错为：

```
stream load … to audit_lineage_db.audit_lineage_batch_lineage is 224 MB …
Broken pipe (Write failed)
```

## 📐 STEP 1：【首席架构师】设计

**问题分析**

1. **超过单次导入上限。** 一个 REBUILD 批次覆盖整个多天区间，批次血缘只用一次 Stream Load 写入，共 224 MB。BE 对单次 JSON 导入的上限 `streaming_load_max_batch_size_mb` 默认是 100 MB，超过后 BE 直接断开连接，客户端看到的就是 `Broken pipe`。
2. **耗时随记录数平方增长。** 重建按游标分页读取整个区间，但每一页都要重新执行一次覆盖整个区间的查询：审计表与成员表关联、排序、`LIMIT 5000`。总耗时大约随记录数的平方增长。
3. **没有进度输出，也无法调节线程数。** 读取只有一个线程。

**设计**

| 需求 | 设计 |
|---|---|
| 分块写入 | 批次血缘（主键表）按序列化后的字节数分块，块大小为 `LOAD_CHUNK_MB`，默认 32 MB。每块是一次独立导入，标签为 `<batch>_lineage_<本次尝试的随机串>_<序号>`。恢复的批次先执行 `clearBatchLineage`（删除本批次中非 refresh 的行），再整体重写，所以分块不影响"恰好一次"。原先对 LINEAGE_STAGE 的标签守卫随之取消；需要守卫的累加步骤只剩 USAGE 和 LINEAGE_DAILY |
| 不能分块的导入 | 用量是累加写入，不能分块。超过 `LOAD_MAX_MB`（默认 100，与 BE 默认值一致）时，在发送前就报一个**不可重试**的错误，错误信息中给出 BE 配置项和修改办法，状态为 BLOCKED，不会每分钟重试一次。按小时重建之后，一个小时的用量远小于这个上限 |
| 按小时拆批次 | `rebuild <fromDay> <toDay>` 仍按整天指定，但每个小时是一个 REBUILD 批次。**每天的 00 点批次**清除当天的用量和每日血缘事实，并把当天所有的边记录下来以便刷新；之后每个小时追加自己的用量和血缘，并刷新本小时涉及的边。当天全部小时完成后，结果与一次性重建完全相同。重建进行中，当天的数据是不完整的 |
| 一天内的批次链 | 第 H 小时提交时，在**同一次**检查点写入中声明第 H+1 小时（`CheckpointStore.commitAndDeclare`）。因此一天不会停在中途：无论是 `rebuild` 本身、`run` 还是 daemon，只要恢复待处理批次，就会把这一天做完 |
| 中断后续跑 | 再次执行同一条 `rebuild` 时，先把中断的那一天做完。如果中断的那一天在本次请求的范围内，就从它的下一天继续；否则从 `fromDay` 开始。每天完成时日志会打印"从这里续跑"的命令。上一版本留下的多天 REBUILD 批次也按同样的规则恢复，恢复时会用上分块写入 |
| 多线程 | 分析线程数由 `THREADS` 配置，命令行 `--threads N` 可以临时覆盖。读取线程数由 `REBUILD_READ_THREADS` 配置，默认 2。每个小时按 `SLICE_SECONDS`（5 分钟）切成若干窗口，每个窗口单独做游标扫描，由读取线程并行读取，再通过有界队列（容量为 2 × 读取线程数页）交给分析线程池，边读边分析。连接池大小为 `THREADS + REBUILD_READ_THREADS + 2` |
| 进度 | 每 `PROGRESS_SECONDS`（默认 30 秒）输出一行 `PROGRESS REBUILD [小时) analysed=已分析/总数 (百分比) rate=速率/s elapsed= eta=`，同时续租。CLI 每完成一个小时输出一行 `REBUILD hour i/N elapsed= eta= …` |

## 💻 STEP 2：【核心开发工程师】编码

- `StarRocksSink`
  - `writeBatchLineage` 分块写入；新增 `clearBatchLineage`。
  - `load` 超过上限时报不可重试的错误。
  - 新增构造参数 `chunkBytes` / `maxLoadBytes`。
  - 每次导入的 LOAD_FILTERED 证据使用各自的标签。
- `CheckpointStore.commitAndDeclare`：由 `JdbcCheckpointStore` 实现，提交和声明下一批次在同一次读改写中完成，写入后读回校验。
- `AuditLogSyncPipeline`
  - `rebuild(from, to, RebuildListener)`、`completeRebuildChain`、`prepareRebuild`（只有一天的第一个批次执行清除）。
  - 提交时串起批次链。
  - `analyseProcessed` 改为并行读取窗口，配合有界队列。
  - 新增 `Progress` / `setProgressListener`；分析过程中定期续租。
- `PipelineConfig.readThreads` / `progressInterval`。
- `CliConfig` 新增 `REBUILD_READ_THREADS`、`PROGRESS_SECONDS`、`LOAD_CHUNK_MB`、`LOAD_MAX_MB`；CLI 支持 `--threads`，并按小时输出结果。
- 测试
  - **单元测试**
    - 小时批次与每天只清除一次：`rebuildRunsOneBatchPerHourAndReportsEachHour`。
    - 由 daemon / `run` 补完中断的那一天：`rebuildRecomputesDaysExactlyAndSurvivesACrash`。
    - 从中断的小时续跑，并且用量标签不重复导入：`interruptedRebuildResumesFromTheInterruptedHour`。
    - 范围外的中断日先补完：`rebuildOutsideTheInterruptedDayCompletesThatDayFirst`。
    - 4 个读取线程的结果与单线程相同，并有进度输出：`rebuildReadsWithSeveralThreadsAndReportsProgress`。
    - 读取失败可以重试：`readerFailureFailsTheHourRetryably`。
    - 续租与被接管：`aLongRebuildHourRenewsItsLeaseAndStopsWhenItIsLost`。
    - 分块中途崩溃后清除并重写：`crashBetweenLineageChunksIsClearedAndRewrittenOnResume`。
    - 分块标签与行数：`batchLineageIsLoadedInChunksUnderTheirOwnLabels`。
    - 超过上限时的错误信息：`aLoadAboveTheBeLimitIsRefusedWithTheRemedy`。
    - 原子的批次链：`commitAndDeclareChainsBatchesAtomically`。
    - 配置项与 `--threads`。
  - **真实引擎（3.1.17）**
    - `StarRocksStorageIT.batchLineageIsChunkedAndClearedOnTheRealEngine`：带谓词的主键表 DELETE 保留 refresh 行，每日事实只包含重写后的内容。
    - `PipelineEndToEndIT` 全部改用 4 KB 分块；逐步崩溃回放测试覆盖分块写入后崩溃的情况。
    - `PipelineEndToEndIT.rebuildRestoresCorruptedDaysExactly` 改为 24 个小时批次，验证结果精确，并且可以重复执行。

## 🔍 STEP 3：【首席架构师】Code Review

| # | 级别 | 发现 | 处理 | 用例 |
|---|---|---|---|---|
| S9-01 | 阻断 | 按小时清除每日事实做不到：日事实按天聚合 | 只有一天的第一个批次清除整天，之后的小时只追加；批次链保证一天不会停在中途 | `rebuildRunsOneBatchPerHour…` |
| S9-02 | 阻断 | 小时批次提交之后、声明下一小时之前进程退出，会留下"已清除、未重算"的一天，而且没有任何待处理批次提醒 | 提交与声明合并为一次原子写入 `commitAndDeclare` | `commitAndDeclareChainsBatchesAtomically`、`…SurvivesACrash` |
| S9-03 | 阻断 | 分块之后，标签去重无法再保证恰好一次（每块的标签不同，恢复时分块结果也可能不同） | 批次血缘是主键表：恢复时先清除再重写；取消 LINEAGE_STAGE 的标签守卫 | `crashBetweenLineageChunks…`、IT 崩溃回放 |
| S9-04 | 主要 | 第一个小时的结束时间被截断成了开始时间（`ceilDay(00:00)` 等于它自身），批次链从未开始 | 结束时间取 `min(下一个整点, 下一天 00:00)` | `rebuildRunsOneBatchPerHour…` |
| S9-05 | 主要 | 一个小时的分析可能超过租约 TTL，而分析期间不写入，因此也不续租，备机会在重建进行中接管 | 每次输出进度时续租；租约丢失立即停止 | `aLongRebuildHourRenewsItsLease…` |
| S9-06 | 主要 | 对中途的小时执行 `skip` 会让当天的数据残缺 | `skip` 时输出 ALERT，给出需要重新重建的天 | — |
| S9-07 | 次要 | 同一批次多次导入出现被过滤的行时，证据的死信标签会冲突，后一次的证据被丢弃 | 证据标签带上导入步骤的名称 | — |
| S9-08 | 次要 | 大导入的警告中写着"调小 sliceSize"，这是错误的建议 | 改为说明上限，由 `LOAD_MAX_MB` 统一拦截 | — |

【首席架构师】**APPROVED**。

## 运维

```bash
# 重建 11-01 ~ 11-07 共 7 天：12 个分析线程，4 个读取线程
java -Xmx8g -jar audit-lineage.jar rebuild 2026-11-01 2026-11-08 --threads 12 --config lineage.properties
```

- 输出：每个小时一行 `REBUILD hour 37/168 elapsed=… eta=… outcome=COMMITTED …`；日志中每 30 秒一行 `PROGRESS`。
- 中断（Ctrl-C、进程被杀、失败）后，**执行同一条命令**即可续跑。daemon 或 `run` 也会把中断的那一天做完，但不会继续后面的天。
- 读取线程数通过 `REBUILD_READ_THREADS=4` 设置（配置文件或环境变量）。线程越多，对 FE 的并发查询越多。
- 如果 BE 的 `streaming_load_max_batch_size_mb` 已经调大，把 `LOAD_MAX_MB` 设成相同的值。
