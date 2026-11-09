# Role & Objective
你是一位精通编译原理、ANTLR4 AST（抽象语法树）解析及 StarRocks 内核架构的资深大数据研发专家。
请帮我开发一个基于 **Maven (Java 11/17)** 构建的 **StarRocks (branch-3.1) 审计日志 SQL 解析与字段级血缘/热度分析引擎**。

**核心硬性约束**：
必须 strictly 使用与 **StarRocks 官方 GitHub 仓库 `branch-3.1` 完全一致的 ANTLR4 语法文件**（`StarRocksLex.g4` 和 `StarRocksParser.g4`），通过 `antlr4-maven-plugin` 自动生成 Lexer、Parser 和 Visitor，**严禁使用 JSqlParser、Druid、Calcite 或正则匹配替代 AST 解析**。

系统的核心输入是 StarRocks 3.1.x 标准审计表（`starrocks_audit_db__.starrocks_audit_tbl__`），核心输出是两张新建的 StarRocks 分析结果表：
1. **字段级血缘关系表**：针对写入类语句，解析源表/源字段到目标表/目标字段的完整映射。
2. **字段小时级访问热度汇总表**：针对纯查询类语句，统计各表各字段逐小时被访问的次数、访问去重用户数（UV）、各子句（SELECT/WHERE/JOIN/GROUP BY）出现频次等。

---

# Technical Requirements & Architecture

## 1. StarRocks 3.1.x 官方 ANTLR4 语法集成规范
- **语法文件对齐**：工程使用 StarRocks `branch-3.1` 源码下的 `StarRocksLex.g4` 与 `StarRocksParser.g4`。
- **关键编译问题处理**：
  - 官方 `StarRocksLex.g4` 的 `@members` 块中包含自定义变量与方法（如 `sqlMode` 判断、`comment` 嵌套状态处理等），请确保在 Maven 构建结构、包路径（如 `com.starrocks.sql.parser`）以及 Lexer/Parser 初始化配置中完全兼容官方 `.g4` 的内嵌代码。
  - **大小写不敏感流（Case-Insensitive CharStream）**：StarRocks 官方词法规则均为大写定义（如 `SELECT: 'SELECT';`），在喂给 `StarRocksLex` 之前，必须实现或封装 `CaseInsensitiveStream`（继承 `CodePointCharStream` 或包装 `CharStream`，使得 `LA(i)` 统一转大写匹配 Token，但 `getText()` 保留原始大小写文本）。
  - **错误恢复与快速模式**：配置 `PredictionMode.SLL` 失败后回退 `PredictionMode.LL` 的双阶段解析策略，并自定义 `BaseErrorListener` 捕获语法错误。
- **审计日志预处理（SqlSanitizer）**：
  - StarRocks 审计表中的 `stmt` 字段常包含 JDBC 驱动前缀注释（如 `/* ApplicationName=DBeaver ... */`）、转义换行符 `\n`、尾部多余分号或空白字符，需在送入 ANTLR4 前做规范化处理。
  - 过滤无效语句：跳过审计表中执行失败的语句（`state = 'ERR'` 或 `errorCode != 0`），以及无需分析血缘的管理类 AST 节点（如 `showStatement`, `describeStatement`, `setStatement`, `adminStatement`, `explainDesc`, `killStatement`, `useStatement`）。

## 2. 基于 `StarRocksParserBaseVisitor` 的 AST 解析核心要求
必须精确针对官方 `StarRocksParser.g4` 的规则节点编写 Visitor，攻克以下复杂 SQL 场景：
1. **核心入口规则覆盖**：
   - `sqlStatements` / `singleStatement`
   - 查询入口：`queryStatement` -> `queryRelation` -> `queryNoWith` -> `queryPrimary` (`querySpecification` / `subquery` / `tableValue`)
   - 写入入口：`insertStatement`、`createTableAsSelectStatement` (CTAS)、`createViewStatement` / `alterViewStatement`、`createMaterializedViewStatement`、`updateStatement`、`deleteStatement`、`submitTaskStatement` (若内嵌 CTAS/Insert)。
2. **多层 CTE (`withClause` / `commonTableExpression`) 与子查询作用域栈 (`ScopeStack`)**：
   - 支持 CTE 链式引用（CTE2 引用 CTE1）、CTE 指定列别名 `WITH cte(c1, c2) AS (...)`、同名覆盖（Shadowing）以及多级嵌套子查询（`subqueryWithAlias`）。
3. **集合操作 (`setOperation`)**：
   - 处理 `UNION [ALL | DISTINCT]`、`INTERSECT`、`EXCEPT` / `MINUS`，严格按照投影列位置（Positional Index）合并各分支的底层源字段集合。
4. **通配符展开 (`asterisk` / `qualifiedName.*`) 与未限定列名推导**：
   - 设计 `MetadataProvider` 接口（提供基于 StarRocks `information_schema.columns` + Caffeine 缓存的生产实现，以及基于内存 Map 的单元测试实现）。
   - 当遇到 `SELECT *`、`SELECT t.*`、`INSERT INTO tbl SELECT ...`（未显式指定目标列）或未带表别名前缀的列名 `SELECT col FROM t1 JOIN t2 ...` 时，通过当前 `Scope` 结合 `MetadataProvider` 精准定位归属表和字段顺序。
5. **横向展开 (`UNNEST` / `lateralView`) 与内联表 (`inlineTable` / `VALUES`)**：
   - 完整支持 StarRocks 3.1.x 的 `FROM tbl, UNNEST(arr_col1, arr_col2) AS unnest_alias(c1, c2)` 语法，将 `c1, c2` 溯源至原表的 `arr_col1, arr_col2`。
6. **复杂表达式穿透**：
   - 穿透 `caseExpress` (`CASE WHEN`)、`functionCall`、窗口函数 `windowSpec` (`OVER (PARTITION BY ... ORDER BY ...)`）、`castExpr`、Lambda 表达式 (`lambdaFunctionExpr`，如 `array_map(x -> x + 1, col)`，注意识别并排除 Lambda 形参 `x`，仅溯源物理列 `col`)、子查询表达式 (`inSubquery`, `exists` 等)。

## 3. 两大业务分支指标提取逻辑

### 分支 A：写入语句 -> 字段级血缘解析 (Lineage Extractor)
- **解析目标**：构建 `Target Table.Column` <-- `Set<Source Table.Column>` 的映射。
- **血缘分类**：
  - **Direct Lineage (`DIRECT`)**：来自 `SELECT` 投影项 (`selectSingle`) 直接映射或计算得出的字段，细分转换类型 `transform_type`：`IDENTITY`（直接透传）、`EXPRESSION`（普通函数/运算）、`AGGREGATION`（聚合函数）、`WINDOW`（窗口函数）、`CONSTANT`（纯常量/字面量）。
  - **Indirect Lineage (`INDIRECT`)**：来自 `FROM ... JOIN ON` (`joinCriteria`)、`WHERE`、`GROUP BY` (`groupByClause`)、`HAVING`、`QUALIFY` 中参与过滤或关联的物理表字段。

### 分支 B：纯查询语句 -> 字段访问热度统计 (Column Usage Analyzer)
- **解析目标**：当顶层语句为 `queryStatement` 时，穿透所有 CTE 和子查询别名，还原出**底层物理表（或视图/物化视图）的真实字段**，并识别其所在的子句上下文（Clause Context）：
  - `SELECT`（投影输出）
  - `WHERE`（过滤条件，包含关联子查询中的过滤）
  - `JOIN`（`JOIN ... ON` / `USING` 关联条件）
  - `GROUP_BY`（分组与 `HAVING` 聚合过滤）
  - `ORDER_BY`（排序与窗口 `ORDER BY`）
- **单 Query 计数规则**：在单条 SQL（同一个 `queryId`）内，若物理表 `db.tbl.col` 被多次引用，该字段的总访问次数 `total_access_cnt` **只记 +1**（单 Query 去重），各子句命中次数（如 `select_hit_cnt`, `where_hit_cnt`）也在单 Query 维度按布尔命中（0 或 1）或实际引用次数清晰界定并汇总到小时粒度。

---

# Storage Schema Specification (StarRocks 3.1.x DDL)

### 1. 输入源表：标准审计表 (`starrocks_audit_db__.starrocks_audit_tbl__`)
包含标准字段：`queryId`, `timestamp` (DATETIME), `queryType`, `clientIp`, `user`, `authorizedUser`, `resourceGroup`, `catalog`, `db`, `state`, `errorCode`, `queryTime`, `scanBytes`, `scanRows`, `returnRows`, `cpuCostNs`, `memCostBytes`, `stmtId`, `isQuery` (TINYINT), `feIp`, `stmt`, `digest`, `planCpuCosts`, `planMemCosts`。

### 2. 输出表 1：字段级血缘表 (`audit_column_lineage`)
- 采用 **PRIMARY KEY** 或 **UNIQUE KEY** 模型（按天/月分区）。
- 核心字段：
  - 目标端：`target_catalog`, `target_db`, `target_table`, `target_column`
  - 源端：`source_catalog`, `source_db`, `source_table`, `source_column`
  - 属性：`lineage_type` (`DIRECT` / `INDIRECT`), `transform_type`, `transform_expr` (截取表达式文本), `stmt_type` (`INSERT` / `CTAS` / `MV` / `VIEW` / `UPDATE`)
  - 追踪信息：`sql_digest`, `last_query_id`, `last_exec_user`, `first_seen_time`, `last_seen_time`, `exec_count`

### 3. 输出表 2：字段逐小时使用情况汇总表 (`audit_column_hourly_usage`)
- 采用 **AGGREGATE KEY** 模型（按天动态分区，以 `stat_hour`, `catalog_name`, `db_name`, `table_name`, `column_name` 为聚合 Key）。
- 核心统计指标（Value 列）：
  - `access_query_cnt` (`BIGINT SUM`)：访问该字段的 Query 总次数
  - `select_clause_cnt` (`BIGINT SUM`)：在 SELECT 中出现的 Query 次数
  - `where_clause_cnt` (`BIGINT SUM`)：在 WHERE 中出现的 Query 次数
  - `join_clause_cnt` (`BIGINT SUM`)：在 JOIN 中出现的 Query 次数
  - `group_by_cnt` (`BIGINT SUM`)：在 GROUP BY / HAVING 中出现的 Query 次数
  - `order_by_cnt` (`BIGINT SUM`)：在 ORDER BY 中出现的 Query 次数
  - `user_bitmap` (`BITMAP BITMAP_UNION`)：利用 `bitmap_hash64(user)` 统计访问该字段的精准去重用户数（UV）
  - `user_hll` (`HLL HLL_UNION`)：HLL 近似去重备份与高速上卷查询支持
  - `total_scan_bytes` (`BIGINT SUM`) / `max_query_time_ms` (`BIGINT MAX`)：关联查询开销指标
- *附加要求*：请同时给出一张明细/用户维度的辅助表或基于 `audit_column_hourly_usage` 查询“某表某字段每小时被访问多少次、被多少不同用户访问”的标准 SQL 示例。

---

# Implementation & Output Plan (关键约束：请严格遵守)

为了保证代码达到生产级可用标准，**严禁在核心类中出现 `// TODO: implement logic`、省略号 `...` 或伪代码**。由于完整工程代码量大，请严格按以下 **3 个阶段（Phases）** 分步输出。**现在请先输出【第一阶段】的全部完整代码，并在结尾停下，等待我指令后再输出【第二阶段】：**

### 【第一阶段】：Maven 工程配置（含官方 `.g4` 构建集成）、StarRocks DDL 与基础域模型
1. **完整的 `pom.xml`**：配置 `antlr4-maven-plugin`（精确到版本 4.9.3，与 StarRocks 3.1.x 官方一致），说明如何放置官方 `StarRocksLex.g4` 和 `StarRocksParser.g4` 目录结构。
2. **ANTLR4 底层支撑类**：
   - `CaseInsensitiveStream.java`（解决官方 `.g4` 大写 Token 匹配与原始大小写保留问题）。
   - 如果官方 `StarRocksLex.g4` / `StarRocksParser.g4` 依赖特定辅助方法或异常类（如 `ParsingException`、`SqlModeHelper`），请提供对应的兼容类以保证 `.g4` 零修改直接编译通过。
3. **完整的 StarRocks 3.1.x 建表 DDL 与查询 SQL**：包含 `audit_column_lineage` 表、`audit_column_hourly_usage` 表，以及查询字段热度与 UV 的示范 SQL。
4. **核心数据模型（POJO）与元数据接口**：
   - `TableIdentifier`, `ColumnIdentifier`, `ColumnLineageEdge`, `ColumnUsageStat`, `ClauseType` (Enum), `TransformType` (Enum), `ParseResult`。
   - `MetadataProvider` 接口及带 Caffeine 缓存的 `JdbcMetadataProvider`、`InMemoryMetadataProvider` 实现。

### 【第二阶段】：作用域栈引擎 (`ScopeStack`) 与官方 `StarRocksParserBaseVisitor` 完整实现（最核心）
1. **作用域与符号表管理 (`Scope`, `ScopeContext`, `RelationField`)**：完整实现 CTE 注册、子查询嵌套作用域入栈/出栈、`SELECT *` 展开、别名解析、Lambda 形参屏蔽、`UNNEST` 虚拟表列绑定。
2. **完整的 `StarRocksAuditAstVisitor extends StarRocksParserBaseVisitor<Void>`**：
   - 完整实现所有核心 Rule 的 `visitXxx` 方法（覆盖 `visitQueryStatement`, `visitInsertStatement`, `visitCreateTableAsSelectStatement`, `visitCreateMaterializedViewStatement`, `visitWithClause`, `visitQueryNoWith`, `visitSetOperation`, `visitQuerySpecification`, `visitFromClause`, `visitJoinRelation`, `visitSubqueryWithAlias`, `visitLateralView`, `visitSelectSingle`, `visitLambdaFunctionExpr`, `visitDereferenceExpression`, `visitColumnRef` 等）。
   - 实现表达式溯源提取器（区分 `DIRECT` / `INDIRECT` 血缘及当前所在的 `ClauseType` 上下文栈）。
3. **入口门面类 `StarRocksSqlAnalyzer`**：整合 `SqlSanitizer`、SLL/LL 双模式 Parser 调用与 Visitor 结果提取。

### 【第三阶段】：审计表增量同步 Pipeline、内存预聚合器与复杂 SQL JUnit 5 单元测试
1. **批处理/流处理 Pipeline (`AuditLogProcessor`)**：
   - 从 `starrocks_audit_db__.starrocks_audit_tbl__` 按时间水位线拉取审计记录。
   - 内存小时级预聚合器（将同一小时内的查询字段热度与用户 Hash 提前合并，减少落库压力）。
   - 通过 StarRocks Stream Load / JDBC Batch 写入两张目标表。
2. **全面的 JUnit 5 单元测试 (`StarRocksSqlAnalyzerTest`)**：
   - 编写至少 6 个高复杂度真实 StarRocks 3.1 SQL 测试用例（多级 CTE + `UNION ALL` + 窗口函数、`INSERT INTO` 省略列名 + `*` 展开、`LATERAL UNNEST` 数组展开、Lambda 高阶函数 `array_map`、`WHERE EXISTS` 关联子查询、纯查询多子句字段命中计数与单 Query 去重验证），并使用 `Assertions` 严格断言解析出的血缘边和字段各子句计数。
