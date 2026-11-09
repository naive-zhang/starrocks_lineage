# 真实 StarRocks 3.1 容器验证（Testcontainers）

```bash
mvn verify -Pit        # 单元测试 311 个 + 容器集成测试 34 个；需要 Docker，没有 Docker 时自动跳过 IT
mvn test               # 只跑单元测试（不依赖 Docker）
```

- **镜像**：`starrocks/allin1-ubuntu:3.1.17`（branch-3.1 的最新补丁版本），可通过 `-Dstarrocks.image=...` 替换。
- **运行方式**：一个 JVM 只启动一个容器，供所有 `*IT` 类共享，冷启动约 20 秒。
- **DDL 来源**：IT 执行的就是仓库中 `sql/ddl` 的建表脚本（把 `replication_num` 改为 1）。

## 测试矩阵

| IT 类 | 用例数 | 用真实引擎验证的内容 |
|---|---|---|
| `StarRocksDdlIT` | 4 | 版本为 3.1.x；6 张表的模型（PK + ORDER BY、AGGREGATE + BITMAP/HLL、PK 检查点）；动态分区；`sql/query` 中 Q1–Q7 全部能执行 |
| `StarRocksStorageIT` | 6 | 经 FE 重定向的 Stream Load 与标签幂等；真实失败响应；`to_bitmap`/`hll_hash` 映射（同一小时 UV = 3，跨小时上卷 UV = 4）；Q5 位图解码；PK 合并累加与重放写入 0 行；检查点 upsert、租约与接管；审计源在相同时间戳下翻页 |
| `AnalyzerDifferentialIT` | 21 | 8 个验收用例的 SQL 都能在 3.1 上**执行**，用真实元数据分析得到相同的血缘边；12 种查询形态下分析器推导的列名与引擎为视图生成的列名**完全一致**；`JOIN USING` 下的 `*` 与引擎结果集一致（13 列）；元数据提供者使用真实 `information_schema`，包括表名大小写、ALTER 之后的缓存失效 |
| `PipelineEndToEndIT` | 2 | 全真实组件跑完整周期，用业务查询 Q1/Q5/Q6/Q7 精确核对；真实集群上"合并后崩溃 → 精确重放"，数据快照不变 |
| `SmokeRunbookIT` | 1 | 按 `docs/staging-smoke-test.md` 逐步演练，使用低权限账号与原始脚本，并通过子进程调用 jshell |

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

## 由引擎实证的设计假设（无需修改）

- 3.1 FE **强制要求** `Expect: 100-continue`（否则返回 `There is no 100-continue header`）。
- 重复标签返回 `"Status": "Label Already Exists"` 和 `"ExistingJobStatus": "FINISHED"`，与客户端的判定逻辑一致。
- `JOIN USING` 不合并同名列；SEMI/ANTI JOIN 只输出一侧的列；UNNEST 默认列名为 `unnest`；VALUES 默认列名为 `column_i`；集合操作取第一个分支的列名。
- 在主键表上 `INSERT` 等同于 upsert；合并 SQL 中 `INSERT … SELECT … LEFT JOIN 自身` 可以正常执行。

## 测试基础设施相关（不影响产品）

- 沙箱 Docker 主机的 `nofile` 硬上限只有 20000，而 BE 要求 ≥ 60000。测试在 `be.conf` 中设置 `min_file_descriptor_number = 10000`。
- 审计表的动态分区如果带 `history_partition_num`，历史分区会**异步分批**补建，建表后短时间内可能还没有当天分区。测试用的审计表与官方插件 DDL 保持一致，不加这个属性。

## 已知差异（接受）

- 分析器对 `b, UNNEST(...) JOIN d` 比引擎更宽松，能够解析。由于审计中失败的语句已在前置过滤阶段剔除，这个差异不会产生错误的数据。
- 外部 catalog 的 `DESC` 路径没有在容器中验证，因为镜像不含 Hive/Iceberg。这部分仍由单元测试和替身覆盖。
