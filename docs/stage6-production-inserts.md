# Stage 6：生产集群 30 条 INSERT 的血缘验收

> 状态：**终审签字（FINAL SIGN-OFF）**。
> - 30 条语句全部 `SUCCESS`，没有任何降级；每个写入列都有血缘，每个来源都是生产表的真实列。
> - 在 StarRocks 3.1.17（与生产版本相同）上执行了全部 30 条语句，用真实 `information_schema` 解析出的血缘与快照**逐行一致**。
> - 验收中发现 1 个**阻断级**生产缺陷（大批量 Stream Load 出现 `Broken pipe`），已按三步流程修复。

## 0. 提供的数据与测试覆盖

| 数据 | 内容 | 测试资源 |
|---|---|---|
| 表结构 | 生产集群全部 997 张基础表的 `SHOW CREATE TABLE`（dperm 20 / eagle 3 / lbdb 955 / lbdb_uat 17 / test 2），包括表名末尾带 CRLF 的那张表 | `src/test/resources/production/schema_lbdb_20261110.sql.gz`（原文件 gzip 压缩，内容逐字节不变） |
| 30 条写入 SQL | DolphinScheduler 项目 8655547341888 中 30 个任务实例的 INSERT，含日志头注释 | `src/test/resources/production/inserts_30.sql`（原样） |
| 评审后的血缘快照 | 每条语句的引用表、全部 DIRECT 边（含变换类型）、INDIRECT 控制列及其覆盖目标列数、边总数 | `src/test/resources/production/inserts_30.lineage`（1742 行） |

测试分为四层，全部基于上面的原始数据：

| 层 | 类 | 用例 | 内容 |
|---|---|---|---|
| 数据完整性 | `ProductionInsertLineageTest.theProvidedDataIsCompletelyLoaded` | 1 | 997 张表与每个库的数量、CRLF 表名、30 条语句的顺序、任务实例 ID、30 个互不相同且都存在于表结构中的目标表 |
| 结构判据（与分析器输出无关） | `analysesWithoutDegradation`、`everyTargetColumnIsWrittenFromItsSources`、`everySourceIsAColumnOfAProductionTable` | 30 × 3 | 状态为 SUCCESS、语句类型为 INSERT_INTO、目标表与索引一致、没有降级告警、SLL 一次解析通过；获得 DIRECT 边的列**恰好等于** INSERT 列清单（没有列清单时等于目标表的全部列）；所有来源都是生产表中真实存在的列；常量来源的边一律为 CONSTANT |
| 快照 | `lineageMatchesTheReviewedSnapshot` | 30 | 与评审后的快照逐行比较；用 `-Dproduction.golden.write=<file>` 重新生成，评审差异后再替换 |
| 人工核对 | `handVerifiedStatement01/08/15/23` | 4 | 阅读 SQL 后手工推导出的精确边（见 3.2） |
| Pipeline | `ProductionInsertsPipelineTest` | 1 | 30 条语句作为同一小时的审计记录走完整 Pipeline：30/30 成功，0 条死信，血缘表恰好包含分析出的边，每条边 `exec_count = 1` |
| 真实引擎 | `ProductionInsertsIT` | 2 | 在 3.1.17 中用生产 DDL 建出被引用的 67 张表，**执行全部 30 条语句**；用真实 `information_schema` 分析，结果与快照逐行一致；使用真实 sink 跑完整 Pipeline，血缘表的行数等于快照中的边数，没有死信 |

## 1. 逐条结果

列说明：引用表（含目标表）、写入列、DIRECT 边、INDIRECT 控制列、边总数；变换类型中 A = AGGREGATION、C = CONSTANT、E = EXPRESSION、Id = IDENTITY。

| # | 目标表 | 引用表 | 写入列 | DIRECT | 控制列 | 边总数 | DIRECT 变换分布 |
|---|---|---|---|---|---|---|---|
| 01 | `lbdb.dws_busi_inbill_site_wgt_rt` | 2 | 17 | 17 | 0 | 17 | C1 / Id16 |
| 02 | `lbdb.tmp_busi_inbill_site_wgt_rt_1` | 2 | 17 | 17 | 15 | 272 | A2 / Id15 |
| 03 | `lbdb.tmp_busi_inbill_site_wgt_rt_2` | 4 | 17 | 27 | 23 | 418 | A2 / E4 / Id21 |
| 04 | `dws_oper_org_violation_i_h` | 3 | 12 | 19 | 15 | 199 | A3 / C4 / E3 / Id9 |
| 05 | `dws_oper_operator_violation_i_h` | 4 | 11 | 18 | 23 | 271 | A5 / C4 / E3 / Id6 |
| 06 | `dws_oper_first_site_sign_distance_rt` | 8 | 18 | 34 | 46 | 862 | A16 / C2 / Id16 |
| 07 | `dws_oper_sign_first_site_distance_rt` | 7 | 15 | 30 | 44 | 690 | A14 / C2 / E1 / Id13 |
| 08 | `dwd_oper_sign_timeout_detail_rt` | 4 | 53 | 53 | 12 | 689 | C2 / Id51 |
| 09 | `dws_oper_center_retain_1d` | 2 | 21 | 21 | 19 | 420 | A1 / C2 / Id18 |
| 10 | `dws_busi_org_input_cost_i_d` | 5 | 9 | 22 | 23 | 229 | A6 / C4 / Id12 |
| 11 | `lbdb.ads_surch_policy_sign_site_review_1d` | 5 | 19 | 29 | 15 | 314 | A22 / C1 / E4 / Id2 |
| 12 | `dws_busi_org_shuttle_bus_input_i_d` | 2 | 12 | 23 | 8 | 119 | A12 / C3 / Id8 |
| 13 | `ads_compass_timepoint_progress_monitor` | 4 | 21 | 26 | 13 | 299 | A18 / C2 / E1 / Id5 |
| 14 | `dws_opt_site_send_transit_fee_i_d` | 8 | 34 | 34 | 35 | 1224 | A7 / C4 / E6 / Id17 |
| 15 | `dws_oper_center_resign_cnt_i_d` | 4 | 17 | 74 | 28 | 550 | A4 / C3 / E67 |
| 16 | `lbdb.dws_oper_waybill_retain_1d` | 10 | 60 | 82 | 45 | 2782 | A72 / C2 / E5 / Id3 |
| 17 | `lbdb.dwd_fin_wayb_sign_site_income_cost_i_m` | 3 | 29 | 29 | 6 | 203 | C2 / E1 / Id26 |
| 18 | `dws_oper_org_send_arrival_diff_bf9_1d` | 4 | 19 | 26 | 20 | 406 | A5 / C3 / Id18 |
| 19 | `dwd_oper_send_arrival_diff_af9_i_d` | 4 | 13 | 13 | 15 | 208 | C1 / Id12 |
| 20 | `dwd_oper_region_position_status_i_d` | 3 | 22 | 59 | 23 | 565 | A5 / E45 / Id9 |
| 21 | `dwd_busi_waybill_today_due_detail_i_d` | 2 | 18 | 20 | 6 | 128 | E3 / Id17 |
| 22 | `dim_gis_tb_base_time_efficient` | 6 | 26 | 26 | 18 | 494 | C1 / Id25 |
| 23 | `dim_gis_tb_base_site_info_d` | 2 | 14 | 14 | 2 | 42 | C1 / Id13 |
| 24 | `lbdb.dws_oper_eff_delivery_rate_v` | 3 | 31 | 35 | 21 | 686 | A14 / C2 / Id19 |
| 25 | `lbdb.dws_oper_eff_delivery_rate_delivery` | 2 | 23 | 25 | 20 | 485 | A5 / C2 / Id18 |
| 26 | `lbdb.dws_oper_eff_delivery_rate_dest_site` | 2 | 30 | 32 | 27 | 842 | A5 / C2 / Id25 |
| 27 | `lbdb.dws_oper_eff_delivery_rate` | 2 | 32 | 32 | 29 | 960 | A3 / C2 / Id27 |
| 28 | `lbdb.dwd_oper_eff_delivery_rate_should_sign` | 4 | 53 | 62 | 15 | 857 | C1 / E12 / Id49 |
| 29 | `lbdb.dwd_oper_eff_delivery_rate_waybill_change` | 3 | 10 | 10 | 11 | 120 | C2 / Id8 |
| 30 | `lbdb.dwd_oper_eff_delivery_rate_next_site` | 10 | 50 | 55 | 47 | 2405 | C3 / E30 / Id22 |

合计 17,756 条边（DIRECT 964 条）。01、06、07、18、19 的列清单中没有 `etl_time`（这一列由 `DEFAULT CURRENT_TIMESTAMP` 填充），所以 `etl_time` 没有血缘，这是正确的。

---

## 📐 STEP 1：【首席架构师】设计（仅针对需要改动生产代码的发现）

**分析器不需要改动。** 在完整表结构下，30 条语句在没有任何修改的情况下全部通过了 1 至 4 层的检验。之所以敢这样下结论，是因为判据不依赖分析器本身的输出：列清单是从 SQL 文本中独立解析的，来源是否存在是对照表结构检查的；同时引擎执行了全部语句，引擎元数据下的分析结果与快照一致。

**P-01（阻断）：大批量 Stream Load 出现 `Broken pipe`。** 真实引擎在把 30 条语句的血缘作为一个批次写入时暴露了这个问题：17,756 行，13.4 MB，经 FE 导入时 `Broken pipe`；直接写 BE 则成功，用 curl 写同样的数据也成功。

原因：3.1 的 FE 收到 Stream Load 后**直接返回 307 并关闭连接**，不发送 `100 Continue`；而 httpclient5 在这条路径上不等待 `100 Continue` 就开始发送正文。正文小于 socket 缓冲区时写入"成功"，客户端随后读到 307，所以此前所有测试（最多几十行）都正常通过；正文一旦更大，写入就撞上已关闭的连接。

设计：**正文永远不发给 FE，先用不带正文的探测请求解析路由。**

1. 探测请求不带正文，但必须带 `Expect: 100-continue`（实测：缺少这个头时，FE 即使正文为空也返回 `200 FAILED: There is no 100-continue header`）。探测请求使用**独立的标签** `<label>_route`。
2. 先向配置的 FE 发探测请求：返回重定向就得到候选执行端；返回非重定向，说明配置的地址本身就是执行端（直连 BE 或代理）。
3. 把正文发给候选执行端：
   - 返回 200：照常解析结果；
   - 返回重定向：候选其实是另一个 FE（例如 Follower 转发到 Leader），且正文很小，继续跟随；
   - 抛出 IOException：对候选再发一次探测。探测被重定向，说明它是一个在正文上挂断的 FE，继续跟随；探测没被重定向，说明它是执行端，这次 IOException 是真实故障，作为可重试异常抛出。真实标签保证重试仍是 exactly-once。
4. **安全性论证**：探测请求永远不携带真实标签。即使探测落到 BE 上被当作空导入执行，也只会消耗探测标签：默认 `empty_load_as_error=true` 时该导入失败、状态为 ABORTED（已实测）；`empty_load_as_error=false` 时以 0 行成功。两种情况都不可能让真实数据被当作"标签已存在"而丢弃。经 FE 正常导入时，探测不会产生任何事务（实测探测标签状态为 UNKNOWN）。

## 💻 STEP 2：【核心开发工程师】编码

- `StreamLoadClient.load`：先探测，再发正文，IOException 时重新探测（上面第 3 步）；`send(...)` 统一构造请求，探测与正文请求都带 `Expect: 100-continue`；`redirect(...)` 处理 Location 并应用 `redirectRewriter`。
- 正文超过 64 MB 时输出 WARN：3.1 中 JSON 导入受 BE 配置 `streaming_load_max_batch_size_mb` 限制（默认 100 MB）。实测每条边约 750 字节，即单次导入约 13 万条边。
- 测试替身 `FakeStarRocks` 改为与真实引擎一致：FE 和 BE 使用不同端口；FE 要求 `100-continue`；FE 收到正文时**直接挂断**；增加 Follower FE；空导入按 `empty_load_as_error` 处理。
- 新增测试 `StreamLoadClientTest`：
  - `theBodyNeverTravelsToTheFe`：在旧代码上会失败；
  - `aFollowerFeChainIsResolvedWithoutSendingTheBodyTwice`；
  - `aDirectBeUrlNeverConsumesTheRealLabelWithTheProbe`：覆盖两种 `empty_load_as_error`；
  - `anUnreachableFeIsARetryableFailure`。

## 🔍 STEP 3：【首席架构师】Code Review 与修正闭环

### 3.1 发现

| # | 级别 | 发现 | 处理 | 验证 |
|---|---|---|---|---|
| P-01 | 阻断 | 大批量（13.4 MB）Stream Load 经 FE 时 `Broken pipe`；生产中一个 5 分钟批次的血缘写入就可能达到这个量级 | 见 STEP 1、STEP 2 | `ProductionInsertsIT.thePipelineWritesTheProductionLineage`（修复前失败，修复后通过）、`StreamLoadClientTest` 中的 4 个新用例 |
| P-02 | 评审中修正 | 第一版探测请求没有带 `Expect` 头，FE 返回 `200 FAILED`，客户端把 FE 当作执行端又把正文发了过去，问题依旧 | 探测请求同样带 `Expect: 100-continue`；测试替身的 FE 也强制要求这个头 | 真实引擎复测：经 FE 和直连 BE 两种方式都写入 17,756 行 |
| P-03 | 接受（说明） | `getSourceTables()` 包含写入目标表本身（定义为"所有被引用的物理表"，由 `WriteLineageAnalysisTest` 固定） | 不修改；快照中的 `T` 行按这个约定记录，第 08 和 28 条确实也读取了自己的目标表 | 快照 |
| P-04 | 接受（监控） | 单次 JSON 导入上限为 100 MB（约 13 万条边） | 超过 64 MB 时输出 WARN，处理方式是减小 `sliceSize` | — |

### 3.2 人工核对（摘录）

- **[08]** 读取自身目标表（`select * from dwd_oper_sign_timeout_detail_rt t1`），再关联 `row_number()` 去重后的应签明细。JOIN 条件 `ON type_code = 'pick_up'` 中的 `type_code` **没有限定表名**；作用域内的三个关系中只有 `tx_basic_dict_entry` 有这一列，这也是引擎能执行这条语句的原因，所以它必须被解析到该表。元数据不完整时，这里会报二义性错误。
- **[15]** `center_name = COALESCE(a.center_name, b.center_name)` → 来源为 `dwd_oper_region_position_status_i_d.center_name` 和 `ods_ehr_tb_emp_adc_f_d.dept4_name`（EXPRESSION）。`employed_avg` 的来源为 `employed_count`、`labor_date`、`data_date`（AGGREGATION）。
- **[01] / [23]**：恒等复制、常量列（字面量和 `NOW()`），以及过滤列作为全部目标列的 INDIRECT 来源（23：14 + 2 × 14 = 42 条边）。

### 3.3 结论

【首席架构师】**APPROVED**。在生产表结构下，30 条生产写入语句的血缘全部正确、完整，并且经过真实引擎交叉验证；验收中暴露的 Stream Load 缺陷已经修复，并补充了回归测试。
