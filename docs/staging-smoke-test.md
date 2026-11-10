# 预发集群冒烟验证 Runbook（StarRocks 3.1.x）

> **目标**：在接入生产之前，用真实的预发集群确认以下几点：
> - 建表语句能执行；
> - 权限配置正确；
> - Stream Load 中 `to_bitmap` / `hll_hash` 的映射有效；
> - 日事实表 + 血缘刷新的语义正确，标签状态可查询；
> - 解析成功率达标；
> - 端到端计数精确，失败语句进入死信表；
> - 崩溃重放满足 exactly-once；
> - 运维操作（状态、修复）可用。
>
> **耗时**：约 60–90 分钟。
>
> **已演练**：本 Runbook 的每一步、所用脚本和全部期望值，都已在 StarRocks **3.1.17** 容器中按顺序自动执行验证，见 `SmokeRunbookIT`：
> ```bash
> mvn verify -Pit -Dit.test=SmokeRunbookIT
> ```
>
> 与真实预发环境唯一的差别：容器镜像没有 audit loader 插件，所以测试代为写入审计行。在预发环境中，这些审计行由插件写入。

所有脚本都在 `scripts/smoke/` 下。

| 步骤 | 执行账号 | 脚本 |
|---|---|---|
| 2 建表与授权 | 管理员 | `sql/ddl/00..09`、`01_grants.sql`、`02_smoke_fixtures.sql` |
| 3 存储层手工核验 | `lineage_etl` | `03_manual_checks.sql`、`stream_load_check.sh` |
| 4 只读解析评估 | `lineage_etl` | `smoke-analyze.jsh` |
| 5 首次追平 | `lineage_etl` | `smoke-run-cycle.jsh` |
| 6 场景验证 | `lineage_smoke_user` / `lineage_etl` | `04_scenario.sql`、`smoke-run-cycle.jsh`、`05_verify.sql` |
| 7 Exactly-once 演练 | `lineage_etl` | `07_replay_drill.sql`、`smoke-run-cycle.jsh` |
| 8 运维操作 | `lineage_etl` | `smoke-ops.jsh` |
| 9 清理 | 管理员 | `06_cleanup.sql` |

---

## 0. 准备

**执行机要求**：
- JDK 11+（自带 `jshell`）、Maven 3.6+、`mysql` 客户端、`curl`；
- 能访问 FE 的 9030（查询端口）和 8030（HTTP 端口），以及 BE 的 8040（Stream Load 会被 FE 重定向到 BE）。

```bash
export SR_FE_HOST=fe-staging.example.com   # FE 地址
export SR_QUERY_PORT=9030
export SR_HTTP_PORT=8030
export SR_USER=lineage_etl                 # 第 2 步创建
export SR_PASSWORD='<lineage_etl 的密码>'
# 只有当 FE 重定向到的 BE 地址从执行机不可达时（NAT / K8s / 容器）才需要设置：
# export SR_BE_REWRITE="10.0.0.12:8040=be-staging.example.com:8040"
```

**0.1 时区**：Pipeline 用 JVM 时钟与审计表的 `timestamp` 比较，两者必须处于同一时区。

```sql
SHOW VARIABLES LIKE '%time_zone%';   -- 记下结果，例如 Asia/Shanghai
```

后续所有 `jshell` 命令都要加上 `-J-Duser.timezone=<该时区>`。jshell 脚本会自动输出 `SMOKE_CLOCK ... skew_seconds=N`，偏差超过 120 秒时直接报错退出。

**0.2 审计插件与审计表**：

```sql
SHOW PLUGINS;                                        -- 应包含 AuditLoader
DESC starrocks_audit_db__.starrocks_audit_tbl__;     -- 必须包含下列 19 列
```

必需的 19 列：`queryId, timestamp, queryType, clientIp, user, authorizedUser, catalog, db, state, errorCode, queryTime, scanBytes, scanRows, returnRows, cpuCostNs, memCostBytes, isQuery, stmt, digest`。

旧版插件可能缺少 `catalog` 列，此时第 3.1 步会报 `Unknown column`，需要先升级插件。

**0.3 插件刷写间隔**：查看插件配置 `max_batch_interval_sec`（默认 60 秒），第 6 步需要等待这段时间。

**0.4 冒烟参数**：Pipeline 按固定时间片（生产默认 5 分钟）处理"已经关闭"的时间片，即片尾早于"现在 − `lateArrival`"。为了缩短等待，冒烟时建议：

```bash
export SMOKE_SLICE_SECONDS=60            # 1 分钟一片（必须整除 1 小时）
export SMOKE_LATE_ARRIVAL_SECONDS=120    # ≥ 插件刷写间隔
```

迟于 `lateArrival` 才写入审计表的记录不会丢：对账（默认每 15 分钟，覆盖最近 24 小时）会用反连接找出不属于任何批次的记录，作为 LATE 批次补算。

## 1. 构建

```bash
mvn -B clean package
```

```bash
mvn -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt
```

**期望**：单元测试全部通过（`Failures: 0, Errors: 0`），并生成 `target/cp.txt`。

## 2. 建表与授权（管理员）

**2.1** 预发集群中如果 BE 少于 3 台，先把副本数改成实际的 BE 数量（这里以 1 为例）：

```bash
mkdir -p /tmp/ddl && for f in sql/ddl/0*.sql; do sed 's/"replication_num" = "3"/"replication_num" = "1"/' "$f" > /tmp/ddl/$(basename "$f"); done
```

```bash
cat /tmp/ddl/0*.sql | mysql -h "$SR_FE_HOST" -P "$SR_QUERY_PORT" -u root -p
```

**2.2 核对**：

```sql
SHOW TABLES FROM audit_lineage_db;                             -- 9 张表
SHOW CREATE TABLE audit_lineage_db.audit_column_lineage;       -- PRIMARY KEY(`edge_id`)、ORDER BY(target_db, ...)
SHOW CREATE TABLE audit_lineage_db.audit_column_lineage_daily; -- AGGREGATE KEY(stat_day, edge_id)、exec_count SUM
SHOW CREATE TABLE audit_lineage_db.audit_column_hourly_usage;  -- AGGREGATE KEY、BITMAP_UNION、HLL_UNION、
                                                               -- PARTITION BY date_trunc('day', stat_hour)
```

用量表、日事实表和成员表都使用**表达式分区**：写入任何一天的数据时由导入自动建分区，所以首次运行从昨天开始、迟到数据和重建历史日期都不会因为"分区不存在"被拒绝。

**2.3** 先把两个脚本里的 `CHANGE_ME_*` 换成真实密码，然后执行：

```bash
mysql -h "$SR_FE_HOST" -P "$SR_QUERY_PORT" -u root -p < scripts/smoke/01_grants.sql
```

```bash
mysql -h "$SR_FE_HOST" -P "$SR_QUERY_PORT" -u root -p < scripts/smoke/02_smoke_fixtures.sql
```

`01_grants.sql` 中给了全库 SELECT，是为了让 `information_schema` 能看到所有业务表。如有外部 catalog，按注释为每个 catalog 补上 `GRANT USAGE ON CATALOG`。

## 3. 存储层手工核验（`lineage_etl`）

**3.1** 执行 `03_manual_checks.sql` 中 **3.1–3.4 节**。可以逐段复制，也可以整体执行：

```bash
mysql -h "$SR_FE_HOST" -P "$SR_QUERY_PORT" -u lineage_etl -p -vvv < scripts/smoke/03_manual_checks.sql
```

整体执行时，3.5 节的结果在 Stream Load 之前为空，属于正常现象；执行完下面的 3.2 后再单独重跑 3.5 节即可。

期望结果：

| 小节 | 期望 |
|---|---|
| 3.1 审计查询 | 无报错，最多返回 5 行；对账反连接无报错；`time_zone` 与 0.1 一致 |
| 3.2 元数据 | `order_id, user_id, amount, dt` 按此顺序 |
| 3.3 检查点 upsert | `rows_expect_1 = 1`、`expect_0005 = 2024-01-01 00:05:00`、`expect_b = b`、`expect_usage = USAGE` |
| 3.4 (a) 日事实 + 刷新 | `exec_count = 2`、`IDENTITY`、`q-b1` |
| 3.4 (b) 重放刷新 | 结果不变，`exec_count` 仍为 2（刷新是幂等重算） |
| 3.4 (c) 新批次 | `exec_count = 5`、`EXPRESSION`、`q-b2`、`first_seen_time = 2024-01-01 10:00:00`；该边 1 行，日事实 2 行 |

3.4 中带标签的 `INSERT ... WITH LABEL smoke_b1_daily_1` 只能成功一次。重跑文件会报 `Label [...] has already been used`，这正是重放保护。确实需要重跑时，把 `_1` 后缀改掉。

**3.2** 执行 Stream Load 检查（在执行机上运行；需要 `SR_BE_REWRITE` 时，改为在能直连 BE 的机器上运行）：

```bash
bash scripts/smoke/stream_load_check.sh
```

期望：
- 第 1 次和第 3 次返回 `"Status": "Success"`；
- 第 2 次返回 `"Status": "Label Already Exists"` 且 `"ExistingJobStatus": "FINISHED"`；
- 第 4 次 `get_load_state` 返回 `"state":"VISIBLE"`，第 5 次（从未使用的标签）返回 `"state":"UNKNOWN"`。Pipeline 恢复中断的批次时，用这个接口判断一次累加型导入是否已经提交，所以 `lineage_etl` 必须能调用它。

**3.3** 执行 3.5–3.6 节：
- 3.5 的期望：`access = 2`、`uv_exact = 2`、`uv_approx = 2`、`ids_expect_1_2 = 1,2`；字典中 2 行；
- 3.6 会清空这些合成数据（此时表里只有冒烟数据）。

## 4. 只读解析评估（不写入任何数据）

```bash
SMOKE_LOOKBACK_HOURS=1 SMOKE_SAMPLE=2000 jshell -J-Duser.timezone=Asia/Shanghai \
  --class-path "target/classes:$(cat target/cp.txt)" \
  scripts/smoke/smoke-common.jsh scripts/smoke/smoke-analyze.jsh
```

输出示例：

```
SMOKE_ANALYZE total=2000 {SUCCESS=..., SKIPPED_MANAGEMENT=..., PARSE_ERROR=..., ANALYSIS_ERROR=...} degraded=... error_rate_percent=... single_thread_records_per_second=...
  FAIL ANALYSIS_ERROR <queryId> | <原因> | <语句前 200 字符>
  WARN x37 no metadata for table ...
```

**通过标准**：
- `error_rate_percent ≤ 1`（PARSE_ERROR + ANALYSIS_ERROR 占实际分析语句的比例）；
- 每一类 `FAIL` 都已经说明原因；
- `WARN` 中如果大量出现 `no metadata`，说明权限不足（见第 11 节），必须先解决。

请把输出完整保存，附到验收记录中。

## 5. 首次追平

```bash
SMOKE_LOOKBACK_HOURS=2 jshell -J-Duser.timezone=Asia/Shanghai \
  --class-path "target/classes:$(cat target/cp.txt)" \
  scripts/smoke/smoke-common.jsh scripts/smoke/smoke-run-cycle.jsh
```

- 默认 `SMOKE_CYCLES=0`：一直执行，直到某个周期没有提交任何批次（最后一行 `SMOKE_SUMMARY ... last_outcome=IDLE`）。
- 首次运行从"现在 − `SMOKE_LOOKBACK_HOURS`"（向下对齐到时间片边界）开始，跨天也可以。
- 每个 `SMOKE_REPORT` 行是一个批次：连续的空时间片合并成一个批次提交（`records=0`），遇到第一个有记录的时间片时结束该批次。

## 6. 场景验证

**6.1** 以 `lineage_smoke_user` 身份，按顺序执行 `04_scenario.sql`（数分钟内完成）。期望 **F1 报错**（`Column 'no_such_col' cannot be resolved`），其余语句都成功。

```bash
mysql -h "$SR_FE_HOST" -P "$SR_QUERY_PORT" -u lineage_smoke_user -p --force < scripts/smoke/04_scenario.sql
```

**6.2** 等待"插件刷写间隔 + 一个时间片 + `SMOKE_LATE_ARRIVAL_SECONDS`"（按 0.4 的参数约 4 分钟），让场景所在的时间片关闭。然后运行：

```bash
jshell -J-Duser.timezone=Asia/Shanghai --class-path "target/classes:$(cat target/cp.txt)" \
  scripts/smoke/smoke-common.jsh scripts/smoke/smoke-run-cycle.jsh
```

期望输出（场景所在的批次）：

```
SMOKE_REPORT outcome=COMMITTED kind=SLICE batch=<32 位十六进制> range_start="..." range_end="..." records=<≥ 8> replay=false ... dead_letters=0 ...
```

预发环境中如果有其他流量，`records` 会大于 8，场景也可能分到两个批次（跨越时间片边界）；只要下面核验的是冒烟库的数据，就不受影响。请记下场景批次的 `batch`、`range_start` 和 `range_end`，第 7 步要用。

**6.3** 执行 `05_verify.sql`，结果必须**逐行一致**：

- **V1**：`pending_batch_id = NULL`，`last_error = NULL`，`committed_until` 晚于场景语句。
- **V2**：列用量，共 5 行。`uv` 和 `uv_approx` 都为 1。

| table_name | column_name | access | sel | wh | jn | gb | ob |
|---|---|---|---|---|---|---|---|
| orders | amount | 2 | 2 | 2 | 0 | 0 | 0 |
| orders | order_id | 1 | 1 | 0 | 0 | 0 | 0 |
| orders | user_id | 1 | 0 | 1 | 0 | 0 | 0 |
| users | city | 2 | 1 | 1 | 0 | 1 | 1 |
| users | user_id | 1 | 0 | 1 | 0 | 0 | 0 |

  说明：
  - `amount` 在 S1 和 S2 中各出现两次，但每条查询只计 1 次，所以 `access` 是 2 而不是 4；
  - `city` 在 S4 的 GROUP BY 和 ORDER BY 中都有使用；
  - F1 失败、M1 是管理语句，都不参与计数。

- **V3**：只返回 `lineage_smoke_user` 一行。

- **V4**：血缘，共 8 行，`exec_count` 都为 1，`last_exec_user` 都为 `lineage_smoke_user`。

| target | source | lineage / transform | stmt_type |
|---|---|---|---|
| user_gmv.gmv | orders.amount | DIRECT / AGGREGATION | INSERT_INTO |
| user_gmv.gmv | orders.user_id | INDIRECT / IDENTITY | INSERT_INTO |
| user_gmv.uid | orders.user_id | DIRECT / IDENTITY | INSERT_INTO |
| user_gmv.uid | orders.user_id | INDIRECT / IDENTITY | INSERT_INTO |
| v_orders.amt2 | orders.amount | DIRECT / EXPRESSION | CREATE_VIEW |
| v_orders.amt2 | orders.dt | INDIRECT / IDENTITY | CREATE_VIEW |
| v_orders.order_id | orders.order_id | DIRECT / IDENTITY | CREATE_VIEW |
| v_orders.order_id | orders.dt | INDIRECT / IDENTITY | CREATE_VIEW |

- **V5**：死信，**0 行**。F1 在 StarRocks 中执行失败，作为失败语句跳过；M1 是管理语句。如果这里出现行，说明有语句引擎能执行而分析器处理不了，请附到验收记录中。
- **V6**：场景批次的批次日志，`records = 8`（有其他流量时更多）、`dead_letters = 0`、`attempts = 1`。

## 7. Exactly-once 演练

1. 把 6.2 记下的 `batch`、`range_start`、`range_end` 填入 `07_replay_drill.sql` 的占位符，然后以 `lineage_etl` 身份执行。
   - 期望：最后一条 SELECT 显示 `pending_batch_id` 等于该 batch，`pending_step = INTENT`。
   - 含义：检查点被回拨成"批次已声明、尚未提交"，并且步骤进度被抹掉。这是最坏的现场：该批次的所有写入其实都已经在 StarRocks 中。
2. 再按 6.2 的命令运行。期望第一行 `SMOKE_REPORT` 为 `replay=true`，`batch` 与 6.2 **相同**，`records` 与 6.2 相同。恢复时，批次从成员表读回自己的记录；每次导入都使用原来的标签，StarRocks 以 `Label Already Exists` 拒绝；刷新是幂等重算。
3. 重新执行 `05_verify.sql`，V2–V5 必须与 6.3 **完全相同**，不能有任何翻倍。

## 8. 运维操作与性能基线

**8.1 状态**（只读）：

```bash
jshell -J-Duser.timezone=Asia/Shanghai --class-path "target/classes:$(cat target/cp.txt)" \
  scripts/smoke/smoke-common.jsh scripts/smoke/smoke-ops.jsh
```

期望：`SMOKE_STATUS committed_until="..." ... pending=-`，以及各类死信的数量（冒烟场景下没有死信）。

**8.2 修复**：对场景时间范围执行一次修复。没有 OPEN 的死信时，期望 `SMOKE_OP op=repair outcome=IDLE`：

```bash
SMOKE_OP=repair SMOKE_FROM="<range_start>" SMOKE_TO="<range_end>" jshell -J-Duser.timezone=Asia/Shanghai \
  --class-path "target/classes:$(cat target/cp.txt)" scripts/smoke/smoke-common.jsh scripts/smoke/smoke-ops.jsh
```

其余操作（`redo` / `mark-done` / `skip` / `rebuild`）的使用条件见 `docs/stage5-reliability.md` 的"运维操作"一节。冒烟时不需要执行。

**8.3 性能基线**（记录即可）：从各次 `SMOKE_REPORT` 记录 `ms`、`records`、`records_per_second`，以及第 4 步的 `single_thread_records_per_second`，作为生产容量规划的输入。如有需要，可以调整 `SMOKE_THREADS` 对比效果。

## 9. 清理（管理员）

```bash
mysql -h "$SR_FE_HOST" -P "$SR_QUERY_PORT" -u root -p < scripts/smoke/06_cleanup.sql
```

如果希望生产从干净状态开始，执行文件末尾被注释掉的 TRUNCATE 语句。

## 10. 验收清单

- [ ] 1. 单元测试全部通过
- [ ] 2. DDL 执行成功（9 张表，表达式分区）
- [ ] 3. 3.1–3.5 的结果全部符合期望（重点：重放刷新结果不变、`uv_exact = 2`、重复标签返回 `Label Already Exists`、`get_load_state` 返回 `VISIBLE` / `UNKNOWN`）
- [ ] 4. `error_rate_percent ≤ 1`，并且失败样本已经分析
- [ ] 6. V2 和 V4 逐行一致，V5 为空
- [ ] 7. 重放演练 `replay=true`，结果没有任何变化
- [ ] 8. 状态与修复操作可用，性能基线已记录
- [ ] 9. 清理完成

## 11. 常见问题（均在 3.1.17 容器中实际遇到并确认）

| 现象 | 原因 | 处理 |
|---|---|---|
| BE 启动失败：`file descriptors limit is too small` | BE 要求 `nofile ≥ 60000` | 调高 `ulimit -n`；受限环境中可在 `be.conf` 中设置 `min_file_descriptor_number` |
| Stream Load 返回 `There is no 100-continue header` | 3.1 FE 强制要求 `Expect: 100-continue` | 本项目的客户端和 `stream_load_check.sh` 都已发送；自行编写客户端时必须带上 |
| Stream Load 连接被拒 / 超时，地址是 BE 内网 IP | FE 307 重定向到 BE 注册的地址 | 设置 `SR_BE_REWRITE`（客户端 API 对应 `StreamLoadClient(..., redirectRewriter)`） |
| 连续导入时第二次返回 `400 Bad Request` | 3.1 BE 不接受在 keep-alive 复用连接上发起的 Stream Load | 本项目客户端已强制每次新建连接（`Connection: close`） |
| `outcome=LEASE_NOT_ACQUIRED` | 另一个 owner 持有未过期的租约 | 等待租约 TTL（10 分钟）过期，或保持 `SMOKE_OWNER` 不变（默认 `smoke-<主机名>`）；正常退出的进程会立即释放租约 |
| `outcome=BLOCKED` | 待处理批次遇到不可重试的错误（消息中有原因） | 用 `smoke-ops.jsh` 查看 `pending`；按 `docs/stage5-reliability.md` 的"运维操作"选择 `redo` / `mark-done` / `skip`。不会自动跳过任何数据 |
| `LeaseLostException` | 进程暂停期间租约过期，被另一个实例接管 | 正常的隔离行为：该进程停止写入，由接管者完成批次 |
| 获取标签状态报 HTTP 401/403 | `lineage_etl` 无权调用 `get_load_state` | 检查 `01_grants.sql` 中 `audit_lineage_db` 的授权 |
| `SMOKE_CLOCK ... skew_seconds` 过大 | jshell 时区与 StarRocks 不一致 | 加上 `-J-Duser.timezone=<time_zone>` |
| 3.1 报 `Unknown column 'catalog'` | 审计插件版本过旧 | 升级 audit loader |
| 4 中大量出现 `no metadata for table` | `lineage_etl` 看不到这些表的元数据 | 按 `01_grants.sql` 补齐 SELECT 或 USAGE 授权 |
| 写入报 `The row is out of partition ranges` | 目标表仍是旧版动态分区 DDL | 按 `sql/ddl` 重建为表达式分区（Stage 5 修复了"首次运行跨天卡死"） |
| `checkpoint commit ... was not persisted` | 检查点表不是主键表 | 按 `sql/ddl/05` 重建 |
| 建表报 `replication_num` 相关错误 | BE 数量少于 3 | 按 2.1 修改副本数 |
