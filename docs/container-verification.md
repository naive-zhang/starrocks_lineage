# 真实 StarRocks 3.1 容器验证（Testcontainers）

```bash
mvn verify -Pit        # 单元测试 469 个 + 容器集成测试 48 个；需要 Docker，没有 Docker 时自动跳过 IT
mvn test               # 只跑单元测试（不依赖 Docker）
```

- **镜像**：`starrocks/allin1-ubuntu:3.1.17`（branch-3.1 的最新补丁版本），可通过 `-Dstarrocks.image=...` 替换。
- **运行方式**：一个 JVM 只启动一个容器，供所有 `*IT` 类共享，冷启动约 20 秒。
- **DDL 来源**：IT 执行的就是仓库中 `sql/ddl` 的建表脚本（把 `replication_num` 改为 1）。
- **审计表**：列与官方插件一致；插件 DDL 使用动态分区（只有今天及以后），测试需要写入昨天和更早的审计行（首次跨天运行、迟到记录、重建），所以测试用的审计表改用表达式分区，保留 30 天。

## 测试矩阵

| IT 类 | 用例数 | 用真实引擎验证的内容 |
|---|---|---|
| `StarRocksDdlIT` | 4 | 版本为 3.1.x；9 张表的模型（PK、AGGREGATE + SUM/MIN/MAX/REPLACE、BITMAP/HLL）；表达式分区能接受昨天和 200 天前的数据；`sql/query` 中 9 条业务查询全部能执行 |
| `StarRocksStorageIT` | 11 | 经 FE 重定向的 Stream Load、标签幂等与 `get_load_state`（VISIBLE / UNKNOWN）；真实的被拒行：严格导入失败后标签不被占用，同标签容错导入保留其余行，sink 写 `LOAD_FILTERED` 死信；63 位用户 id 的位图 UV（同小时 3、跨小时 4）与 Q5 解码；日事实 + 血缘刷新（生成的 SQL 原样执行），重放、重建、边删除；死信往返（含 1 MB 语句和中文）；批次日志；检查点步骤、隔离与提交；成员 JOIN / SEMI / ANTI JOIN 与审计保留期核对 |
| `AnalyzerDifferentialIT` | 21 | 8 个验收用例的 SQL 都能在 3.1 上**执行**，用真实元数据分析得到相同的血缘边；12 种查询形态下分析器推导的列名与引擎为视图生成的列名**完全一致**；`JOIN USING` 下的 `*` 与引擎结果集一致（13 列）；元数据提供者使用真实 `information_schema`，包括表名大小写、ALTER 之后的缓存失效 |
| `PipelineEndToEndIT` | 7 | 全真实组件：首次运行跨越昨天和今天；在每个写入步骤之后"杀进程"再恢复，快照逐字段不变；标签过保留期时 BLOCKED，`redo` 后精确一次；中断期间到达的审计行进入 LATE 批次；暂停的实例被隔离、接管者完成批次；死信在补齐表结构后修复；被篡改的历史日期经重建（中途崩溃后恢复）精确还原 |
| `ProductionInsertsIT` | 2 | 用生产 DDL 建出被引用的 67 张表，**执行 30 条生产 INSERT**；用真实 `information_schema` 解析出的血缘与评审后的快照逐行一致；30 条语句走完整 Pipeline，一次写入 17,756 条血缘（13.4 MB，经 FE），没有死信（见 `docs/stage6-production-inserts.md`） |
| `StandaloneJarIT` | 2 | 以独立进程 `java -jar` 运行打包后的 jar，使用低权限账号依次执行 check、analyze、run、status，再启动 daemon 并发送 SIGTERM，确认租约已释放；旧版 DDL 会被 `check` 拒绝 |
| `SmokeRunbookIT` | 1 | 按 `docs/staging-smoke-test.md` 逐步演练，使用低权限账号与原始脚本，并通过子进程调用 jshell（包括 `smoke-ops.jsh`） |

## 只有真实引擎才能发现的问题（均已修复）

| # | 级别 | 发现 | 修复 |
|---|---|---|---|
| R5-01 | 阻断 | 3.1 BE 对在 **keep-alive 复用连接**上发起的第二次 Stream Load 返回 `400 Bad Request`。HTTP 替身没有这个行为，所以之前一直没有暴露 | `StreamLoadClient` 禁用连接复用，并发送 `Connection: close` |
| R5-02 | 主要 | FE 307 重定向的目标是 BE **注册的地址**（容器内是 `127.0.0.1:8040`），在 NAT、K8s 或端口映射环境下不可达 | `StreamLoadClient` 增加 `redirectRewriter`；Runbook 增加 `SR_BE_REWRITE` |
| R5-03 | 阻断 | 重放区间上界写成 `end + 1µs`，但审计 `timestamp` 是秒级 DATETIME，比较时亚秒部分被截断，**最后一条记录被漏掉**（重放只读到 7/8 条）。初始水位 `start − 1µs` 有同样问题 | 所有水位边界改用整秒（`end + 1s` 再在内存中精确过滤；初始水位为 `(start − 1s, "￿")`） |
| R5-04 | 主要 | 没有列清单的 CTAS、视图、物化视图中，未加别名的复杂表达式会以**规范化后的 SQL** 命名（`row_number() over (order by amount asc )`、`case when (amount > 1) then 1 end`），与原文不同 | 语句执行成功意味着目标对象已存在，因此按位置采用目标对象**真实元数据**中的列名；元数据缺失时才回退为推导名并记录告警 |
| R5-05 | 主要 | 业务查询 Q5 中 `b, UNNEST(...) JOIN d` 的 JOIN 先与 UNNEST 结合，看不到 `b`，报 `b.ids cannot be resolved` | 改写成左深的 `b CROSS JOIN LATERAL UNNEST(BITMAP_TO_ARRAY(b.bm)) JOIN d` |
| R5-06 | 次要 | Runbook 脚本每次运行使用 `smoke-<pid>` 作为 owner，10 分钟内重跑会得到 `LEASE_NOT_ACQUIRED` | owner 改为稳定值 `smoke-<hostname>`，并可通过 `SMOKE_OWNER` 配置 |
| R5-07 | 次要 | `SMOKE_REPORT` 中的 `end_time` 含空格，复制或解析时会被截断成只有日期 | 输出时加引号 |
| S5-06 | 阻断 | 管道重置后用同名重启，SLICE 批次 id 与重置前完全相同，而 FE 仍记得这些标签（3 天），**所有导入被当作"标签已存在"静默跳过**。容器中表现为成员没有写入，对账又把刚处理过的记录处理了一遍 | 检查点增加随机 `pipeline_epoch`，参与 SLICE id 的计算（见 Stage 5） |
| S5-07 | 主要 | 意外异常被当作不可重试，"进程被杀"变成了 BLOCKED 而不是重试 | 只有已知会以相同方式重复的失败才不可重试 |
| S5-08 | 主要 | `LOAD_FILTERED` 死信的批次 id 从标签中截取，批次 id 含 `_` 时出错（由 StarRocks 真实拒绝的行暴露） | 显式传入批次 id |
| S5-09 | 主要 | 修复和重建使用的是缓存的元数据（10 分钟），刚补齐表结构就修复会再次失败 | 分析前调用 `invalidateAll()` |
| J-06 | 主要 | （在生产 `check` 中发现，多 FE 集群）Follower FE 对 `get_load_state` 返回 307 跳转到 Leader，正文为空；客户端不跟随跳转，因此无法判断导入是否已提交，中断恢复时会一直卡住 | 查询标签状态时跟随跳转，报错带上 HTTP 状态码和正文（见 Stage 7） |
| P-01 | 阻断 | 3.1 FE 对 Stream Load 直接返回 307 并关闭连接，不读取正文；httpclient5 不等待 `100 Continue` 就发送正文，正文超过 socket 缓冲区（生产 30 条语句的血缘为 13.4 MB）时出现 `Broken pipe`。小正文能写进缓冲区，因此此前的测试全部通过 | 先用不带正文、使用独立标签 `<label>_route` 的探测请求解析路由，正文只发给执行端（见 Stage 6） |

## 由引擎实证的设计假设（无需修改）

- 3.1 FE **强制要求** `Expect: 100-continue`（否则返回 `There is no 100-continue header`），正文为空的请求也一样；FE 返回 307 时不发送 `100 Continue`，也不会为该请求开启事务。
- 重复标签返回 `"Status": "Label Already Exists"` 和 `"ExistingJobStatus": "FINISHED"`，与客户端的判定逻辑一致。
- `JOIN USING` 不合并同名列；SEMI/ANTI JOIN 只输出一侧的列；UNNEST 默认列名为 `unnest`；VALUES 默认列名为 `column_i`；集合操作取第一个分支的列名。
- 在主键表上 `INSERT` 等同于 upsert；刷新 SQL 中 `INSERT … SELECT … JOIN 日事实表` 以及带子查询的 `DELETE` 可以正常执行。
- 表达式分区（AGGREGATE 含 BITMAP/HLL、PK）由导入自动创建任意一天的分区；`partition_live_number` 生效。
- 严格导入因数据质量失败后，同一标签可以再次用于 `max_filter_ratio = 1` 的导入。
- `get_load_state` 对 Stream Load 和带标签的 INSERT（包括 0 行的 INSERT）都返回 `VISIBLE`，对从未用过的标签返回 `UNKNOWN`；重复使用 INSERT 标签时报 `Label [x] has already been used.`；`label_keep_max_second = 259200`。

## 测试基础设施相关（不影响产品）

- 沙箱 Docker 主机的 `nofile` 硬上限只有 20000，而 BE 要求 ≥ 60000。测试在 `be.conf` 中设置 `min_file_descriptor_number = 10000`。
- 审计表的动态分区如果带 `history_partition_num`，历史分区会**异步分批**补建，建表后短时间内可能还没有当天分区。测试用的审计表因此改用表达式分区（见上文）。

## 已知差异（接受）

- 分析器对 `b, UNNEST(...) JOIN d` 比引擎更宽松，能够解析。由于审计中失败的语句已在前置过滤阶段剔除，这个差异不会产生错误的数据。
- 外部 catalog 的 `DESC` 路径没有在容器中验证，因为镜像不含 Hive/Iceberg。这部分仍由单元测试和替身覆盖。
