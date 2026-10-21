# 分层健康、共享预算、节流与独立本地告警

**本次 2026-10-22 改进仅为源码/离线验证：只读节流、稳定重连、聚合止损与分钟上限尚未部署到既有 profile；不操作真实 QQ。** 当前验证见[测试地图](tests.md)。

**历史 v0.5 P1/P2：源码阶段完成 627 tests + 39 subtests、Ruff 和已安装 Hermes 的无网络契约探针。随后于 2026-10-19 经另行授权，完成单个测试 profile 的受控升级与独立本地 watcher 启动检查；没有做本版真实消息、故障注入或外部通知送达验收。** v0.4 历史受控验收不替代本版验证，后续操作仍需相应授权。

前置安全规则见[账号生命周期](account-lifecycle.md)、[QQ 运维指南](qq-account-operations.md)及[信息来源](qq-account-sources.md)。以下参数是本地工程策略，**不是腾讯认可的“免封阈值”**。

本次参考 [ai-companion-kit 教程第十三章](<https://github.com/pinganbbOvO/ai-companion-kit/blob/main/docs/完整教程-从零搭一只小鲸鱼.md>) 的减少重试、上报与请求频率经验。它是个人运维复盘，不是平台规则或因果实验；不能仅凭超时认定限速，也不承诺“停一天必恢复”。本项目保留更严格的持久人工解除与 unknown 不重放，不照搬三分钟后自动放行、拟人输入或随机停顿。

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
  global_per_minute: 6
  target_per_minute: 3
  global_per_hour: 60
  target_per_hour: 30
  min_interval_seconds: 5
  target_interval_seconds: 10
  max_pending_age_seconds: 900
```

| 配置 | 校验 | 作用 |
|---|---|---|
| global_per_minute | 整数 1..3600 | 同一 Fleet 滚动 60 秒硬上限，默认 6，全部账号/入口共用 |
| target_per_minute | 整数 1..3600 | 每个规范目标滚动 60 秒硬上限，默认 3，跨账号/client/别名 |
| global_per_hour | 整数 1..3600 | 同一 Fleet 全部账号、原生回复与 publisher 共用 |
| target_per_hour | 整数 1..3600 | 每个规范目标，跨账号/client/别名累计 |
| min_interval_seconds | 有限数值 1..3600 | 全局最小间隔 |
| target_interval_seconds | 有限数值 1..3600 | 目标最小间隔 |
| max_pending_age_seconds | 有限数值 1..86400 | 原始待发送年龄上限，默认 15 分钟 |

拒绝未知键、`enabled:false`、显式 null、布尔值、零、负数和非有限数，不能静默关闭约束。作用域是同一逻辑服务及其状态文件，不声称跨独立机器或独立部署协调。

预算无账号维度：换主、恢复连接、换 client、改目标别名或重启不清零。分钟、小时滚动额度与全局/目标间隔**同时**生效，不积攒可突发的 token。滚动窗口为 `(now-window, now]`，恰好到 60/3600 秒的旧占用不再计入次数，但永久意图记录仍在；完成与恢复间隔可能要求更久。收紧间隔立即持久延长已有截止时间，随后放宽也不缩短已保存的截止。成功、明确失败和结果不明都消耗已占用次数，没有退款或清零 CLI。

发送前持久占用；实际发送由适配器单发送锁与实例锁串行化。RPC 结束后再次持久延长间隔，避免“占用后被 SQLite 阻塞很久，连续两次实际发送却挤在一起”。取得独占实例锁后，重启恢复也先延长已有目标的间隔，不清除旧次数和意图。完成时间落盘失败不篡改已知 ACK/unknown，但阻断后续业务，等待修复与重启，不能降为无预算发送。

### 已有预算库升级

分钟级硬上限将预算库从 v2 升级为 v3；**只有适配器持有实例锁后的构造流程**执行事务迁移，`budget` 状态/审计 CLI 不迁移、不初始化，遇到 v2 明确失败。升级前停用 Gateway 并冷备份账本与全部 sidecar（含 WAL/SHM）；不要在线复制单个主文件，也不要删除旧库让它重新生成。迁移先验证完整旧 schema、策略和历史，再保留全部占用、时间、意图 tombstone 与间隔，只增加分钟规则/必要的更严格间隔；近期旧占用立即计入分钟上限。异常事务回滚，不猜测修复未知或损坏版本。v3 不支持直接降回不理解分钟保护的旧代码。

### pending 与 unknown

- 原生回复遇到预算拒绝为终结失败，不交给宿主即时 retry/fallback，也不排成隐藏的新后台任务。
- 只有**确定没有进入任何发送 RPC**的预算拒绝，publisher 才能延后**原 ID**；NoRoute 同样只能用于未派发情形。原始创建时间不变，不退款已经占用的共享预算。
- 排队交接、初次派发与最后派发门禁均检查原始年龄，防止预检/本地写锁期间越过有效期。过期意图终结拒绝；已占用次数不退，结果/ID tombstone 不删除。
- 派发后超时、取消、崩溃或 ACK 不明仍终结为 unknown/uncertain，不跨号补发。重启、重复提交、人工 reset 都不能把它改成可自动重试。
- publisher 的 24 小时**正文保留期**、原有 client 限额与静默期仍保留，但不等于本版默认 900 秒**派发新鲜度**；旧 client 的 NoRoute 记账撤销不会退还共享预算。调用方不得换 ID 规避时效或幂等要求。

## 只读请求节流（源码改进；未部署）

`extra.reads` 默认：`health_interval_seconds: 60`、`idle_health_interval_seconds: 180`、`health_backoff_max_seconds: 600`、`member_interval_seconds: 15`、`member_account_interval_seconds: 2`、`member_global_per_minute: 30`。数值必须有限且为 1..3600，分钟上限必须为整数；满足正常间隔 ≤ 闲置间隔 ≤ 失败退避上限，拒绝未知键、null、布尔值和关闭开关。

- 新 WebSocket 首次立即验证身份；同连接并发健康检查合并，正常巡检按 60 秒到期执行，暂停/隔离/账号锁存降为 180 秒。失败间隔指数递增至 600 秒；断开连接不查询。连接变化必须重新验证，旧结果不能认证新连接。
- 群成员预检在 RPC **之前**原子持久占用：全部账号/client/目标共用每滚动 60 秒 30 次上限，每账号至少 2 秒，同账号同目标至少 15 秒。拒绝、超时、结果不明与崩溃不退款；换意图、重启或放宽配置不能缩短已保存的间隔。
- 不缓存“可以发送”的正向权限。节流时跳过该候选路由，不复用旧成员身份/准入；每次真正派发仍需当次成功预检及最终 revision/锁存门禁。派发前先做一次**不占用**的发送预算否决，已知无额度时不先查询成员；最终持久预算占用仍在预检后执行。
- 只读配额库不可读、损坏或持久占用失败时阻断业务，不降级为无节流查询。状态只存哈希及元数据，不存查询结果、正文或 Token；保留历史，不提供退款/清空命令。
- 本地 monitor 每约 5 秒记录心跳；账号的 `updated_at` 只表示实际观测时间，不在空转时伪造远端新鲜度。独立 watcher 用本地心跳检查 observer，故 60/180 秒远端巡检不会触发默认 30 秒 observer stale；进程停止仍会变 stale。旧日志无心跳时回退到最后真实观测。

以上频率只约束本项目发往**本机 OneBot**的 RPC，不能据此换算腾讯侧请求或声称风控已解除。

## 重连稳定性（源码改进；未部署）

`extra.reconnect` 默认 `initial_seconds: 1`、`max_seconds: 60`、`stable_seconds: 120`、`jitter_ratio: 0.25`。连接失败和身份未确认均指数退避；短暂验证成功后立即掉线**不重置**退避，只有当前连接持续通过健康判断至少 120 秒后，下一轮退避才恢复初始值。中途健康检查失败会打断稳定期。

默认每次等待为当前指数上限的 75%..100%（可配置 jitter_ratio 0..0.5，一般为 `(1-jitter_ratio)..1` 倍），不会产生零等待或超过最大值；账号间独立随机化，减少同时重连。本地重连仍刷新 Token supplier 并重新验证身份，不循环登录 QQ，不自动放行 P0/目标 circuit，不重放已派发请求。停止会取消等待、读请求与未完成 socket 任务。退避属于进程内连接策略，人工反复重启会重置这段计时，不能拿重启绕过冷却；持久业务锁存和预算仍不重置。

参数拒绝未知键/null/布尔值/非有限数；初始和上限为 1..3600 秒且初始 ≤ 上限，稳定期为 1..86400 秒。心跳与退避调整不是平台认可的“安全阈值”。

## 跨目标失败聚合止损（源码改进；未部署）

`extra.stoploss` 默认滚动 `window_seconds: 300`；同一账号同时达到 `account_failures: 3` 次派发失败、`account_targets: 3` 个不同目标时锁存账号业务 hold；Fleet 同时达到 `fleet_failures: 4`、`fleet_targets: 3`、`fleet_accounts: 2` 时锁存 Fleet hold。失败包括明确发送拒绝和已进入发送 RPC 的结果不明；成员预检、只读超时、预算拒绝及确定派发前断线不计入。相同意图永久去重，不能重复累计阈值。

- 这只是**停止新增业务发送**的保守规则，不是封号判定、不改变 P0 准入，不自动 `quarantine`/`allow`/换主。任一账号聚合 hold 也阻断整个 Fleet 新发送，避免悄悄换号绕开止损；入站授权规则不因此改变。
- 原生回复与 publisher 共用门禁；在成员预检和最终派发前检查。达到阈值的那次已发生请求仍终结记账，后续不发探针、不自动补发。状态损坏/读写失败锁住业务，不绕过保护。
- hold 跨时间、成功 ACK、重启、恢复健康、P0 变更及普通 `health reset` 保持。只有显式 `stoploss reset --scope account|fleet --reason ... --confirm` 解除**精确范围**；账号解除不解除 Fleet，反之亦然。CLI 账号解除还须指定已记录的 `--account`。
- 解除记录该范围的证据序号水位：旧记录、旧意图去重和失败历史永久保留，不被旧窗口立即重新触发；新的失败可重新锁存。解除不清空目标 circuit、预算、revision 或 unknown 结果，也不是恢复发送的充分条件。
- 独立 sidecar 只存账号/目标/意图/人工原因 SHA256、固定错误类别、次数、时间与阈值快照；无消息正文、Token 或原始异常。`status/audit` 是本地诊断，不做网络探测。原 watcher 通过 monitor 的 `pool_unavailable` 看见业务不可用；具体聚合原因用 `stoploss` 查看，不伪装成远端封禁告警。
- 窗口为有限 1..86400 秒，次数为整数 1..3600，目标数和账号数不能大于相应失败数；拒绝未知键、null、布尔值和禁用开关。收紧配置可依据尚未被人工水位排除的窗口证据锁存；放宽不会自动解除已有 hold。

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
hermes-onebot-fleet reads --state /path/to/onebot-fleet.sqlite3
hermes-onebot-fleet reads --state /path/to/onebot-fleet.sqlite3 --action audit
hermes-onebot-fleet stoploss --state /path/to/onebot-fleet.sqlite3
hermes-onebot-fleet stoploss --state /path/to/onebot-fleet.sqlite3 --action audit
hermes-onebot-fleet stoploss --state /path/to/onebot-fleet.sqlite3 --action reset --scope account --account 10001 --reason reviewed --confirm
hermes-onebot-fleet stoploss --state /path/to/onebot-fleet.sqlite3 --action reset --scope fleet --reason reviewed --confirm
hermes-onebot-fleet alerts --state /path/to/onebot-fleet.sqlite3 --output /private/alerts.jsonl
hermes-onebot-fleet alerts --state /path/to/onebot-fleet.sqlite3 --output /private/alerts.jsonl --follow --interval 10 --stale-after 30
```

示例数字是占位符。`health reset` 需要已观测账号、非空原因和 `--confirm`；精确目标必须属于同一逻辑命名空间。health 原因仅记录哈希；P0 `accounts` 审计仍有人工原因文本，勿写入密钥或验证材料。`status/events/alerts` 不接受 mutation 参数。

| 文件 | 内容与权限边界 |
|---|---|
| `<ledger>.accounts.sqlite3` | P0 准入、revision、sticky primary；现有 CLI 不变 |
| `<ledger>.health.sqlite3` | 独立观测、锁存、事件与本地告警 |
| `<ledger>.budget.sqlite3` | 不含账号维度的预算、永久意图哈希及占用/间隔 |
| `<ledger>.reads.sqlite3` | 只读成员预检的持久配额、账号/目标哈希、间隔和审计；不存查询结果 |
| `<ledger>.stoploss.sqlite3` | 派发失败聚合、永久意图去重、账号/Fleet hold 与人工范围水位；全为哈希和元数据 |
| 输出 JSONL 与相邻 `.cursor.json` | 告警消费及 observer 游标，不是权限或放行配置 |

管理权限仍是本机文件访问权限，不是新增远程认证。**健康查询和告警文件保留必要的本地账号/目标标识，不是匿名化输出**；不输出 Token、验证材料、消息正文或原始远端异常。ID、状态、时间和审计同样是私密运维数据，分享前另行脱敏；预算标识只保留哈希。不要把这些文件上传公共仓库或直接转发至群。

## 5. 独立于 QQ 的告警消费

`alerts --follow` 是独立进程可运行的本地消费器，不需要 QQ Token、QQ 连接或调用发送 API，也不会安装/启动常驻服务。它读取已有健康 sidecar，把固定字段导出 JSONL，再持久保存相邻 cursor；活跃告警重复发生会计数，但不为每次重复刷一行。

- 检测 `observer_stale`、`observer_unavailable` 及恢复：本地 monitor 心跳默认超过 30 秒未更新、缺失或不可读会在消费器中发出独立状态（旧日志无心跳时用最后实际观测时间）。`observer_state=healthy` 只表示本地观测新鲜，不是 QQ 健康。
- cursor 跨重启保持，追加先 fsync 再原子更新 cursor；崩溃窗口可能重复导出，属于 **at-least-once**，不是 exactly-once。Windows 无通用目录 fsync 保证。
- 每次最多读取 1000 条 incident，达到窗口上限明确提示 `journal_window_full`，不宣称导出完整历史。默认状态/事件查询也有输出上限，不是数据库保留期限。
- 拒绝用 SQLite 主文件/sidecar、链接别名或不符合格式的既有文件当输出，拒绝损坏/异类 cursor，不自动覆盖修复。
- 告警有记录不等于已送达邮件/Webhook/短信。**没有配置或验收外部通知出口。** 受控测试部署已启动单独 watcher 进程，但没有安装操作系统服务、开机自启或 supervisor。如需值守，应另行授权 supervisor、离机监控及通知送达验证。
- 同机独立 watcher 可在 Gateway 停止后看到记录变旧；watcher 自己退出、整台主机关机或磁盘失效仍需要更外层、必要时离机的心跳监控，不能由进程内 SQLite 冒充主机 HA。

## 6. 验收与升级边界

离线验证覆盖分层观测、持久锁存/精确 reset、共享预算并发与恢复、原始年龄和派发时钟竞态、原生/publisher 同门禁、CLI 及无 QQ 告警导出。测试平台、已安装 Hermes 契约探针与 CI 记录见[测试地图](tests.md)。

源码阶段没有更新运行中的 profile。后续授权部署仅升级一个既有测试 profile：停收、冷备份原插件/config/全部 sidecar（含当时存在的 WAL/SHM）并校验独立 SQLite 快照；刷新原生插件缓存后恢复原有准入 revision/主角色。独立 watcher 已导出启动瞬态告警的恢复记录，另用隔离本地 fixture 检查观测不可用及重复合并；不修改实际健康记录作故障测试。详情见[部署验收范围](tests.md#guarded-v05-deployment-and-local-watcher--2026-10-19)。没有本版真实 QQ 消息、风控恢复、ACK 丢失、主机故障或外部通知送达验收。其他部署仍需授权和完整备份，不能删除状态解除锁存、预算或未知结果，也不能把旧版不认识 v0.5 sidecar 的降级当成安全回滚。
