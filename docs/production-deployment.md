# 生产部署手册

部署物是一个独立 jar：`starrocks-audit-lineage-<版本>-all.jar`，约 12 MB，包含全部依赖。服务器上**只需要 Java 11+**，不需要 Maven、jshell，也不需要源码。

以下每一步都由 `StandaloneJarIT` 在 StarRocks 3.1.17 上用同一个 jar、同样的低权限账号逐步验证过。

## 1. 构建（在任一台装有 JDK 11+ 和 Maven 的机器上）

```bash
mvn -B clean package
```

产物为 `target/starrocks-audit-lineage-1.0.0-SNAPSHOT-all.jar`，把它复制到生产服务器，例如 `/opt/audit-lineage/audit-lineage.jar`。

## 2. 建表与授权（管理员执行一次）

1. 执行 `sql/ddl/00..09`。BE 少于 3 台时，把其中的 `replication_num` 改成实际 BE 数。如果以前建过旧版的表（`audit_column_lineage_stage` / `audit_lineage_merge_log`），先删除 `audit_lineage_db` 再建。
2. 把 `scripts/smoke/01_grants.sql` 中的 `CHANGE_ME_ETL` 改成正式密码，然后执行。**审计库必须已经存在**，否则脚本会在中途报 `cannot find db` 并停止，账号最终没有任何权限（`check` 会检查出来）。

## 3. 配置 `/opt/audit-lineage/lineage.properties`

```properties
SR_FE_HOST=fe-host
SR_USER=lineage_etl
SR_PASSWORD=正式密码
TIME_ZONE=Asia/Shanghai
PIPELINE=audit_lineage
INITIAL_LOOKBACK_HOURS=24
```

- 文件中的值可以被同名环境变量覆盖；写错的键名会直接报错，不会被静默忽略。
- 全部键及默认值可用 `java -jar audit-lineage.jar --help` 查看。
- `TIME_ZONE` 填 FE 的 `time_zone`，`check` 命令会核对时钟偏差。
- `PIPELINE` 一旦确定就**不要再改**。
- 如果 BE 注册的地址从这台机器访问不到，配置 `SR_BE_REWRITE=BE内网IP:8040=可达地址:端口`，多条用逗号分隔。
- `OWNER` 默认取主机名，每台机器各不相同即可。
- 多 FE 集群中，`SR_FE_HOST` 可以填任意一个 FE（或 FE 前面的负载均衡地址）。导入和标签状态查询都会自动跟随跳转到 Leader，主备切换后无需改配置。

```bash
chmod 600 /opt/audit-lineage/lineage.properties
```

## 4. 上线前检查（代替冒烟，约 10 秒）

```bash
java -jar /opt/audit-lineage/audit-lineage.jar check --config /opt/audit-lineage/lineage.properties
```

必须出现 `CHECK RESULT PASSED`（退出码 0）。检查项如下：

| 检查项 | 含义 | 失败时的处理 |
|---|---|---|
| connection | 连接与版本 | 检查地址、端口、账号 |
| clock | JVM 与 StarRocks 的时钟偏差不超过 120 秒 | 按提示设置 `TIME_ZONE` |
| audit_table | 审计表 19 个必需列齐全，并显示最近 1 小时的记录数 | 升级审计插件 |
| target_tables | 9 张表与 `sql/ddl` 一致，不存在旧版表 | 按第 2 步重建 |
| metadata_visibility | 能看到的业务表数量 | 补 SELECT 授权 |
| sql_write | 主键表 upsert 与 DELETE 权限 | 检查授权 |
| stream_load | 真实执行一次 FE → BE 导入，并查询标签状态 | 检查网络和 `SR_BE_REWRITE` |
| pipeline_state | 当前水位与待处理批次 | — |

检查时写入的测试行会在结束后删除。

## 5. 试解析（只读，不写入任何数据）

```bash
java -jar /opt/audit-lineage/audit-lineage.jar analyze 1 5000 --config /opt/audit-lineage/lineage.properties
```

输出 `ANALYZE ... error_rate_percent=...`，以及失败语句样本和降级原因。通过标准建议为 `error_rate_percent ≤ 1`，并且每条失败样本的原因都已了解。

## 6. 启动

**方式 A：常驻进程（推荐）**，systemd 服务文件 `/etc/systemd/system/audit-lineage.service`：

```ini
[Unit]
Description=StarRocks audit lineage
After=network-online.target

[Service]
ExecStart=/usr/bin/java -Xmx4g -jar /opt/audit-lineage/audit-lineage.jar daemon --config /opt/audit-lineage/lineage.properties
Restart=always
RestartSec=30
TimeoutStopSec=120
User=audit-lineage

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload && systemctl enable --now audit-lineage
```

- 进程每分钟执行一轮；追平历史时会连续执行，直到追平为止。
- 停止时（SIGTERM）会立即释放租约，备机可以马上接管。
- 第二台机器部署同样的服务即可作为备机：租约保证同一时刻只有一台在写。

**方式 B：定时任务**（cron 或 DolphinScheduler 的 Shell 任务，每 5 分钟一次），`run` 命令执行到追平后退出：

```bash
java -Xmx4g -jar /opt/audit-lineage/audit-lineage.jar run --config /opt/audit-lineage/lineage.properties
```

退出码：0 正常，1 失败，3 批次阻塞（BLOCKED，需要人工决定）。可以把非 0 退出码直接作为调度告警。

## 7. 上线后检查与告警

- **告警**：日志中出现 `ALERT`、`outcome=BLOCKED`、`FAILED` 时告警；daemon 每 15 分钟输出一行 `METRICS ... blocked=... consecutive_failures=...`。
- **查看状态**：
  ```bash
  java -jar /opt/audit-lineage/audit-lineage.jar status --config /opt/audit-lineage/lineage.properties
  ```
- **核对数据**：用 `sql/query/business_queries.sql` 抽查：Q6 看熟悉表的上游，Q9 看每日写入，Q10 看死信分布。

## 8. 运维操作

使用条件见 `docs/stage5-reliability.md` 的"运维操作"一节。**先停掉 daemon**（`systemctl stop audit-lineage`；定时任务方式则在两次运行之间执行），操作完成后再启动。不要在 daemon 运行时，用同一个 `OWNER` 从另一个进程执行操作：两个进程会都认为自己持有租约，彼此不再互斥。

```bash
java -jar audit-lineage.jar redo --config ...                          # BLOCKED：确认数据不在 → 重做该步骤
java -jar audit-lineage.jar mark-done --config ...                     # BLOCKED：确认数据已在 → 跳过该步骤
java -jar audit-lineage.jar skip "原因" --config ...                    # 放弃待处理批次（记入死信）
java -jar audit-lineage.jar repair "2026-11-09" "2026-11-10" --config ...   # 修复区间内 OPEN 的死信
java -jar audit-lineage.jar rebuild 2026-11-01 2026-11-08 --config ...      # 按天重建
```

## 9. 回滚

停止服务（`systemctl stop audit-lineage`，或删除定时任务）即可。程序只写 `audit_lineage_db`，不写任何业务库。需要从头开始时，清空 `audit_lineage_db` 中的表再启动：新的检查点会生成新的批次编号，不会与 StarRocks 仍保留的旧标签冲突。
