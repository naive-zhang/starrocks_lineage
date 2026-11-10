# Stage 7：独立部署包（不依赖 jshell 与 Maven）

> 状态：**终审签字（FINAL SIGN-OFF）**。部署方式见 [production-deployment.md](production-deployment.md)。

## 📐 STEP 1：【首席架构师】设计

问题：Pipeline 只是一个 Java 库，生产服务器上没有可运行的程序。唯一的入口是冒烟用的 jshell 脚本，它依赖完整 JDK，每次运行都要重新编译脚本，还要靠人记住 `-Xss`、`-Duser.timezone` 等参数。

设计：用 maven-shade 打出一个可执行 jar，只依赖 Java 11+ 运行时。

| 命令 | 作用 | 是否写入 |
|---|---|---|
| `check` | 上线前检查：连接、时钟、审计表 19 列、9 张目标表与当前 DDL 一致（能识别旧版表）、元数据可见性、主键 upsert 与 DELETE、一次真实的 FE → BE Stream Load 及其标签状态 | 只写测试行，结束后删除 |
| `analyze` | 对最近的审计记录做只读试解析，输出成功率和失败样本 | 否 |
| `run` / `daemon` | 定时调度方式 / 常驻进程方式；SIGTERM 时释放租约 | 是 |
| `status`、`redo`、`mark-done`、`skip`、`repair`、`rebuild` | 运维操作 | 视操作而定 |

稳健性放进程序内部，不再依赖部署参数：
- 分析线程固定使用 16 MB 栈；
- 时区由 `TIME_ZONE` 在程序内设置；
- 运行时日志为 INFO，HTTP 客户端只输出 WARN（DEBUG 级别会把每次 Stream Load 的正文打到日志里）；
- 配置优先级为默认值 < 配置文件 < 环境变量；写错的键名直接报错；密码不会出现在日志中。

## 💻 STEP 2：【核心开发工程师】编码

- `com.starrocks.lineage.cli`：`CliConfig`（配置）、`Preflight`（检查）、`AuditLineageCli`（命令入口与退出码：0 正常 / 1 失败 / 2 用法错误 / 3 BLOCKED）。
- `AuditLogSyncPipeline.ANALYSIS_STACK_BYTES`：分析线程的栈大小。
- `src/main/resources/logback.xml`；`pom.xml` 中加入 maven-shade-plugin（合并 `META-INF/services` 以保留 JDBC 驱动注册，设置 `Multi-Release`）。
- 测试：
  - `CliConfigTest` 4 个、`AuditLineageCliTest` 3 个；
  - `StandaloneJarIT` 2 个：以独立进程 `java -jar` 运行**打包后的 jar**，使用 `01_grants.sql` 创建的低权限账号，依次执行 check → analyze → run → status → daemon，再发 SIGTERM，确认租约已释放；另一个用例验证旧版 DDL 会被 `check` 拒绝。

## 🔍 STEP 3：【首席架构师】Code Review 与修正闭环

| # | 发现 | 修正 |
|---|---|---|
| J-01 | 在真实集群上手工演练时，先执行了授权脚本、后建审计库，授权脚本在第 9 行中断，账号最终几乎没有权限。`check` 逐项给出了缺少的具体权限 | 部署手册中写明执行顺序；`target_tables` 的提示改为"missing or not visible to <user>"，不再只写"missing" |
| J-02 | 关闭资源时写了 `e.addSuppressed(e)`，这是自我抑制，本身会抛出 IllegalArgumentException | 改为把异常输出到 stderr |
| J-03 | 部署手册初稿建议运维操作与 daemon 共用同一个 OWNER，但同一个 owner 的两个进程不会互斥 | 改为先停掉 daemon 再执行运维操作 |
| J-04（测试） | `Process.destroy()` 发出 SIGTERM 后会立即关闭管道，daemon 的停止日志因此丢失 | 测试中把 daemon 的输出重定向到文件 |
| J-05（测试） | 两次 `run` 之间可能刚好有一个时间片关闭，第二次会提交一个空批次 | 断言改为 `records=0` 且 IDLE |
| J-06 | **生产 `check` 发现（多 FE 集群）**：`stream_load` 一项失败，报 `get_load_state ...: unexpected answer`，后面的内容为空。3.1 的 `GetStreamLoadState` 在非 Leader FE 上会先执行 `redirectToLeader`，返回 307 和空正文，而客户端查询标签状态时不跟随跳转。影响不止于 `check`：Pipeline 中断后恢复时也要查询标签状态，在 Follower 上会反复失败、一直卡住。单 FE 的测试容器无法暴露这个问题 | 查询标签状态时跟随 FE 的跳转（最多 3 跳，每一跳都重新带认证，地址同样应用 `SR_BE_REWRITE`）；查不到状态时报错带上 HTTP 状态码和正文片段；测试替身的 Follower FE 按 3.1 源码行为返回跳转；新增 `loadStateIsReadFromTheLeaderThroughAFollower`（在旧代码上失败）和 `anUnusableLoadStateAnswerNamesTheCodeAndBody` |

【首席架构师】**APPROVED**。
