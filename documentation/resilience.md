# v0.5 P1/P2：分层健康、共享预算与独立本地告警

**源码阶段完成 627 tests + 39 subtests、Ruff 和已安装 Hermes 的无网络契约探针。随后于 2026-10-19 经另行授权，完成单个测试 profile 的受控升级与独立本地 watcher 启动检查；没有做本版真实消息、故障注入或外部通知送达验收。** v0.4 历史受控验收不替代本版验证，后续操作仍需相应授权。

前置安全规则见[账号生命周期](account-lifecycle.md)、[QQ 运维指南](qq-account-operations.md)及[信息来源](qq-account-sources.md)。以下参数是本地工程策略，**不是腾讯认可的“免封阈值”**。

## 1. 健康不是一个布尔值

| 独立维度 | 记录的事实 | 不能推出什么 |
|---|---|---|
| transport | WebSocket 是否连接、最近观测时间 | 连接不代表身份、在线或可发送 |
| identity | `get_login_info` 身份是否匹配配置 | 匹配不是 P0 准入或无平台限制 |
| online | `get_status.online` 的布尔值或 unknown | 在线不证明后台或所有目标正常 |
| backend-good | `get_status.good` 的布尔值或 unknown | `good=true` 不证明群消息可送达 |
| admission | P0 状态与 revision | 恢复连接不等于人工 `allow` |
| target | 账号对某个群/私聊目标的预检及已发生的业务结果 | 单目标问题不证明全号冻结 |
| outcome | sent / rejected / unknown | unknown 不等于未发送；ACK 不等于已读 |

身份匹配且 `online=true` 才可能具备传输就绪条件；明确 `good=false` 也拒绝就绪，缺失/非布尔 `good` 保留 unknown，不虚构平台字段。身份不匹配即使后续状态查询失败也保留该证据；只有成功读取正确身份才更新观测，但不会解除已经锁存的 circuit。

网络断开、账号离线、后台降级分别保存为诊断，不自动贴“冻结”标签。只使用只读状态查询及实际业务结果，不发送健康探针，不增加 QQ 登录、验证码或自动解封逻辑。WebSocket 重连仍使用有上限的连接退避，**不是循环登录 QQ**。

## 2. 持久锁存 circuit，不冒充 P0 准入

- 身份不匹配或显式账号异常证据可形成账号 circuit；明确发送拒绝、结果不明、明确目标预检失败形成账号—目标 circuit。
- 普通发送错误只按相应操作/目标证据处理，不声称已经识别账号冻结。预检超时/断线仅表示该次只读判断不明，不据此制造目标限制证据。
- 目标级 circuit 只阻止该账号向该目标**发送**；不因该账号仍能接收被授权消息而丢弃入站。可由其他合格账号回复，但不能重发原先已派发失败/不明的同一意图。
- circuit 跨重启保持，无定时释放、半开探针或发送试探。新的正常观测、ACK、P0 `allow`、`promote` 都不清除 circuit。
- `health reset` 只清除**指定范围**：账号 reset 不清除其目标 circuit；目标 reset 不清除账号 circuit。它不 `allow`、不升主、不重置预算、不复活旧 revision、不重放未知结果。
- 最后派发前再次检查源回复 revision、所选账号准入、传输/目标 circuit 和原始年龄。之后已进入 RPC 的请求不能撤回，结果照常记账。

全池没有合格业务路由时写入本地 `pool_unavailable` 告警；恢复合格路由可解除该诊断，但不会清除账号/目标 circuit。多账号异常另作独立 fleet 事件；正常暂停也可能使业务池不可用，不据此诊断平台封禁。

## 3. 全部账号与发送入口共享一个预算

在 `gateway.platforms.onebot_fleet.extra` 内添加；完全省略 `budget` 时使用以下默认值：

```yaml
budget:
  global_per_hour: 60
  target_per_hour: 30
  min_interval_seconds: 5
  target_interval_seconds: 10
  max_pending_age_seconds: 900
```

| 配置 | 校验 | 作用 |
|---|---|---|
| global_per_hour | 整数 1..3600 | 同一 Fleet 全部账号、原生回复与 publisher 共用 |
| target_per_hour | 整数 1..3600 | 每个规范目标，跨账号/client/别名累计 |
| min_interval_seconds | 有限数值 1..3600 | 全局最小间隔 |
| target_interval_seconds | 有限数值 1..3600 | 目标最小间隔 |
| max_pending_age_seconds | 有限数值 1..86400 | 原始待发送年龄上限，默认 15 分钟 |

拒绝未知键、`enabled:false`、显式 null、布尔值、零、负数和非有限数，不能静默关闭约束。作用域是同一逻辑服务及其状态文件，不声称跨独立机器或独立部署协调。

预算无账号维度：换主、恢复连接、换 client、改目标别名或重启不清零。小时滚动额度与全局/目标间隔同时生效，不积攒可突发的 token。成功、明确失败和结果不明都消耗已占用次数，没有退款或清零 CLI。

发送前持久占用；实际发送由适配器单发送锁与实例锁串行化。RPC 结束后再次持久延长间隔，避免“占用后被 SQLite 阻塞很久，连续两次实际发送却挤在一起”。取得独占实例锁后，重启恢复也先延长已有目标的间隔，不清除旧次数和意图。完成时间落盘失败不篡改已知 ACK/unknown，但阻断后续业务，等待修复与重启，不能降为无预算发送。

### pending 与 unknown

- 原生回复遇到预算拒绝为终结失败，不交给宿主即时 retry/fallback，也不排成隐藏的新后台任务。
- 只有**确定没有进入任何发送 RPC**的预算拒绝，publisher 才能延后**原 ID**；NoRoute 同样只能用于未派发情形。原始创建时间不变，不退款已经占用的共享预算。
- 排队交接、初次派发与最后派发门禁均检查原始年龄，防止预检/本地写锁期间越过有效期。过期意图终结拒绝；已占用次数不退，结果/ID tombstone 不删除。
- 派发后超时、取消、崩溃或 ACK 不明仍终结为 unknown/uncertain，不跨号补发。重启、重复提交、人工 reset 都不能把它改成可自动重试。
- publisher 的 24 小时**正文保留期**、原有 client 限额与静默期仍保留，但不等于本版默认 900 秒**派发新鲜度**；旧 client 的 NoRoute 记账撤销不会退还共享预算。调用方不得换 ID 规避时效或幂等要求。

## 4. 本地命令与状态文件

下列命令使用已安装的本项目 CLI，或在独立开发环境通过 `python -m hermes_onebot_fleet.cli` 调用；不会自动选择 profile。`--state` 必须指向已有 Fleet **消息账本**，相应 sidecar 必须已存在，查询不初始化/修复它们。

```text
hermes-onebot-fleet health --state /path/to/onebot-fleet.sqlite3
hermes-onebot-fleet health --state /path/to/onebot-fleet.sqlite3 --action events
hermes-onebot-fleet health --state /path/to/onebot-fleet.sqlite3 --action alerts
hermes-onebot-fleet health --state /path/to/onebot-fleet.sqlite3 --action reset --account 10001 --reason reviewed --confirm
hermes-onebot-fleet health --state /path/to/onebot-fleet.sqlite3 --action reset --account 10001 --target obf:onebot-fleet:group:30001 --reason reviewed --confirm
hermes-onebot-fleet budget --state /path/to/onebot-fleet.sqlite3
hermes-onebot-fleet budget --state /path/to/onebot-fleet.sqlite3 --action audit
hermes-onebot-fleet alerts --state /path/to/onebot-fleet.sqlite3 --output /private/alerts.jsonl
hermes-onebot-fleet alerts --state /path/to/onebot-fleet.sqlite3 --output /private/alerts.jsonl --follow --interval 10 --stale-after 30
```

示例数字是占位符。`health reset` 需要已观测账号、非空原因和 `--confirm`；精确目标必须属于同一逻辑命名空间。health 原因仅记录哈希；P0 `accounts` 审计仍有人工原因文本，勿写入密钥或验证材料。`status/events/alerts` 不接受 mutation 参数。

| 文件 | 内容与权限边界 |
|---|---|
| `<ledger>.accounts.sqlite3` | P0 准入、revision、sticky primary；现有 CLI 不变 |
| `<ledger>.health.sqlite3` | 独立观测、锁存、事件与本地告警 |
| `<ledger>.budget.sqlite3` | 不含账号维度的预算、永久意图哈希及占用/间隔 |
| 输出 JSONL 与相邻 `.cursor.json` | 告警消费及 observer 游标，不是权限或放行配置 |

管理权限仍是本机文件访问权限，不是新增远程认证。**健康查询和告警文件保留必要的本地账号/目标标识，不是匿名化输出**；不输出 Token、验证材料、消息正文或原始远端异常。ID、状态、时间和审计同样是私密运维数据，分享前另行脱敏；预算标识只保留哈希。不要把这些文件上传公共仓库或直接转发至群。

## 5. 独立于 QQ 的告警消费

`alerts --follow` 是独立进程可运行的本地消费器，不需要 QQ Token、QQ 连接或调用发送 API，也不会安装/启动常驻服务。它读取已有健康 sidecar，把固定字段导出 JSONL，再持久保存相邻 cursor；活跃告警重复发生会计数，但不为每次重复刷一行。

- 检测 `observer_stale`、`observer_unavailable` 及恢复：健康记录默认超过 30 秒未更新、缺失或不可读会在消费器中发出独立状态。`observer_state=healthy` 只表示本地观测新鲜，不是 QQ 健康。
- cursor 跨重启保持，追加先 fsync 再原子更新 cursor；崩溃窗口可能重复导出，属于 **at-least-once**，不是 exactly-once。Windows 无通用目录 fsync 保证。
- 每次最多读取 1000 条 incident，达到窗口上限明确提示 `journal_window_full`，不宣称导出完整历史。默认状态/事件查询也有输出上限，不是数据库保留期限。
- 拒绝用 SQLite 主文件/sidecar、链接别名或不符合格式的既有文件当输出，拒绝损坏/异类 cursor，不自动覆盖修复。
- 告警有记录不等于已送达邮件/Webhook/短信。**没有配置或验收外部通知出口。** 受控测试部署已启动单独 watcher 进程，但没有安装操作系统服务、开机自启或 supervisor。如需值守，应另行授权 supervisor、离机监控及通知送达验证。
- 同机独立 watcher 可在 Gateway 停止后看到记录变旧；watcher 自己退出、整台主机关机或磁盘失效仍需要更外层、必要时离机的心跳监控，不能由进程内 SQLite 冒充主机 HA。

## 6. 验收与升级边界

离线验证覆盖分层观测、持久锁存/精确 reset、共享预算并发与恢复、原始年龄和派发时钟竞态、原生/publisher 同门禁、CLI 及无 QQ 告警导出。测试平台、已安装 Hermes 契约探针与 CI 记录见[测试地图](tests.md)。

源码阶段没有更新运行中的 profile。后续授权部署仅升级一个既有测试 profile：停收、冷备份原插件/config/全部 sidecar（含当时存在的 WAL/SHM）并校验独立 SQLite 快照；刷新原生插件缓存后恢复原有准入 revision/主角色。独立 watcher 已导出启动瞬态告警的恢复记录，另用隔离本地 fixture 检查观测不可用及重复合并；不修改实际健康记录作故障测试。详情见[部署验收范围](tests.md#guarded-v05-deployment-and-local-watcher--2026-10-19)。没有本版真实 QQ 消息、风控恢复、ACK 丢失、主机故障或外部通知送达验收。其他部署仍需授权和完整备份，不能删除状态解除锁存、预算或未知结果，也不能把旧版不认识 v0.5 sidecar 的降级当成安全回滚。
