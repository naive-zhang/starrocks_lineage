# Stage 8：生产 `analyze` 暴露的两个问题

> 状态：**终审签字（FINAL SIGN-OFF）**。
> 生产集群 `analyze 1 20000` 的结果：成功 18,727 条，ANALYSIS_ERROR 13 条（0.07%），降级 2 条。

## 📐 STEP 1：【首席架构师】设计

**A. `ORDER BY t.别名`（13 条 ANALYSIS_ERROR）。** 例如生产语句 `SELECT t.send_site_code sendSiteCode … FROM (…) t … ORDER BY t.sendSiteCode, t.waybillNo`。子查询 `t` 里没有 `sendSiteCode` 这一列，它是外层 SELECT 的别名。下面的规则先在 3.1.17 上逐条实测：

| 写法 | 3.1.17 |
|---|---|
| `t.c AS a … ORDER BY t.a` | ✅ |
| `c AS a`（没写表前缀，但 `c` 属于 `t`）… `ORDER BY t.a` | ✅ |
| `GROUP BY t.a` / `HAVING t.a` | ✅ |
| 子查询 `t`；不起表别名时用表名 `w.a`；大小写不同 | ✅ |
| `upper(t.c) AS a … ORDER BY t.a`（表达式） | ❌ |
| `ORDER BY z.a`（限定名不对） | ❌ |
| `WHERE t.a` | ❌ |

结论：3.1 生成输出列时，如果 SELECT 项是一个单纯的列引用，就把它实际所属的关系保留为输出列的限定名。设计：
- 发布 SELECT 输出时，同时记录每一项的**来源关系**。只记录单纯的列引用（以及 `t.*` 展开出的列）在本层解析到的关系；表达式、外层相关引用、结构体子字段都不记录。
- 在 ORDER BY / GROUP BY / HAVING（会看到 SELECT 别名的子句）中，限定名 `q.n` 在 `q` 里找不到列 `n` 时，取来源关系为 `q`、别名为 `n` 的输出项。
- 优先级与 StarRocks 一致：FROM 中的真实列优先。对没有元数据的关系，先尝试别名再按需虚构列，因为别名是确凿证据，虚构列只是猜测。

**B. 视图没有元数据（2 条降级）。** 3.x 的权限体系中，视图和物化视图是独立的权限对象。只授权 `ON ALL TABLES` 时，`information_schema.columns` 看不到视图（已在 3.1.17 实测：补授权前不可见，补授权后可见）。设计：授权脚本补上 `ON ALL VIEWS` 和 `ON ALL MATERIALIZED VIEWS`；`check` 分开统计基础表和视图，看不到任何视图时给出 WARN 并提示授权语句。

## 💻 STEP 2：【核心开发工程师】编码

- `Scope.setSelectOutputs(..., origins)`、`Scope.resolveQualifiedSelectAlias`、`resolveLocal` 中的回退顺序。
- `StarRocksAuditAstVisitor.bareColumnOrigin` 记录来源关系。
- `scripts/smoke/01_grants.sql` 补两条授权。
- `Preflight.metadata` 分别统计表和视图。
- 测试：
  - `QualifiedSelectAliasTest` 14 个：
    - 引擎接受的 8 种写法，宽松和严格模式下都成功且不降级；
    - 引擎拒绝的 4 种写法，严格模式下报 `cannot be resolved`；
    - 真实列优先于别名；
    - 用户提供的生产语句配合生产表结构：`send_site_code` 和 `waybill_no` 的 ORDER BY 计数各为 1。
  - `StandaloneJarIT` 增加一个视图和一条 `SELECT * FROM 视图` 的审计记录：低权限账号的 `analyze` 结果中 `degraded=0`，并且没有出现 "without metadata"。

## 🔍 STEP 3：【首席架构师】Code Review

| # | 发现 | 处理 |
|---|---|---|
| S8-01 | 回退只能出现在会看到 SELECT 别名的子句中（策略不为 NONE），否则 `WHERE t.a` 也会被错误接受 | 回退以 `policy != NONE` 为前提；有用例覆盖 WHERE |
| S8-02 | 对没有元数据的关系，原先在别名回退之前就会按需虚构出列 `t.a`（错误的物理列） | 先查已知列，再查别名，最后才按需虚构 |
| S8-03 | 测试替身的表中有一列名叫 `x`，于是 `x.sendSiteCode` 被解析成结构体字段，不能代表"限定名不对" | 用例改用不存在的限定名 `z` |
| S8-04 | 元数据检查原先只统计数量，看不到"视图一个都不可见"的情况 | 分开统计，并给出 WARN |

【首席架构师】**APPROVED**。
