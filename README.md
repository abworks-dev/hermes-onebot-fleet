# onebot-fleet

仓库：[abworks-dev/onebot-fleet](https://github.com/abworks-dev/onebot-fleet)（由原仓库更名，保留提交历史；旧 Python 包 / CLI 兼容入口不是漏改）。

**实验性 v0.6.0：宿主无关安全内核 + Hermes / AstrBot 两个宿主边界。仅源码重构与离线验证；没有升级运行中的 profile，没有启动 AstrBot / QQ，没有本版真实消息或 PC 插件联调结果。没有免封、生产就绪或 exactly-once 保证。**

```text
NapCat / OneBot v11 主号、备号
              │ 单一连接所有者
              ▼
      FleetCore（同进程）
 准入 / 去重 / 预算 / 止损 / 账本 / 保守回执
              │ 选择一个宿主，不能同号双开
       ┌──────┴──────┐
  Hermes 薄适配层   AstrBot 原生平台
                     │
               陪伴插件 / Inbox / 任务后端（另行集成）
```

## v0.6 新入口与不变项

- 发行包、CLI 主名为 `onebot-fleet`，Python 入口为 `onebot_fleet`。旧 `hermes_onebot_fleet` 导入与 `hermes-onebot-fleet` 命令保留兼容；安全收发算法只有一份。纯模块暂以旧包为单一源码、由新包按当前宿主命名空间导出，并非复制双份策略。
- `FleetCore` 接收显式 `Settings`、状态路径、窄凭据映射及入站 handoff；不导入 Hermes / AstrBot，也不从环境隐式搜集凭据。
- Hermes 平台 ID、插件 ID、`obf:` 逻辑目标、默认状态目录、账本和 sidecar 名称保持不变。**改发行名称不重置预算、准入、止损或 unknown。** 现有部署未被本次重构自动迁移。
- AstrBot 使用独立平台类型 `onebot_fleet`，固定平台实例 ID，保留真实用户 ID。只接 FleetCore 已准入的群 / 私聊文本；同群跨号不新建会话，私聊离线消息不承诺跨号代收。
- AstrBot 的普通回复、事件发送与当前事件内同会话的 `Context.send_message` 走相同 core 门禁；只接受整条纯 `Plain` 文本链。不提供 `aiocqhttp` / 原始 OneBot client，流式、主动 UMO 投递及媒体明确拒绝；可靠后台发送仍须走已授权的 durable publisher。
- `sent + message_id` 仅表示 OneBot ACK，不是人已读、任务成功或宿主插件全局成功；`uncertain/unknown` 不自动补发、不因换号重发。

### AstrBot 安装材料（不启用、不连接）

从本仓库暂存自包含插件，不依赖尚未发布的 PyPI 包。下面仅示例，目标必须不存在：

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -m onebot_fleet.cli stage-astrbot --destination '<全新目录>/astrbot_plugin_onebot_fleet'
```

生成目录含插件元数据、原生平台注册以及插件本地 `_vendor` 两个包；不修改 `sys.path`，不连接账号、不写令牌、不启用任何平台。现有目标拒绝覆盖。安装和配置见 [AstrBot 插件说明](astrbot_plugin_onebot_fleet/README.md) 与 [v0.6 接入与迁移](documentation/astrbot.md)。首版要求**安装后重启**，不承诺热装。

**AstrBot 首版必须关闭分段回复、自动引用 / @、转图、TTS 和流式。** 固定宿主版本存在“发送失败仍触发首个 after_sent hook”的行为；适配层会失败锁存并阻止后续发送，但无法让该宿主钩子自动变成可靠回执。未验收的陪伴 / 记忆插件不得据此判定送达。完整边界见插件说明。

### Hermes 兼容部署材料

仍支持 `python -m hermes_onebot_fleet.cli stage-plugin --hermes-home '<全新隔离 profile>'`，也可用 `python -m onebot_fleet.cli`。暂存现在包含新旧两个包，仍不启用、不覆盖、不改 profile 配置；不要对 Hermes 托管环境执行 pip 安装。本次未执行对运行中 profile 的暂存或升级。

## 历史版本记录与既有能力（不是 v0.6 实测）

**以下 v0.5 及更早的部署 / 真实 QQ 记录仅属于各自版本，不能作为 v0.6 或 AstrBot 的验收。** 历史 v0.5 P1/P2 曾完成离线验证及另行授权的受控测试 profile 部署和独立本地告警运行检查，未做该版真实消息 / 故障注入或外部通知送达验收。

此前多个 NapCat / OneBot v11 账号作为同一 Hermes profile 的 QQ 入口。无需 NoneBot、外部 HTTP API 代理或修改 Hermes 核心。目前只支持文本；当前协议实现与验证范围仅限 OneBot v11 / NapCatQQ，不代表已兼容其他 OneBot 实现。**以下为历史 v0.2 系列验收记录，不覆盖 v0.3 publisher：** 更名前的同一实现已于 2026-10-18 完成本地真实双账号群聊主号回复、断开主号后由备号处理 @主号、恢复后回到主号的三条消息验收，账本为 3 条 `handed_off` / 3 条 `sent`。更名后的 v0.2.0 也已于同日完成相同三步实测：主号、备号、主号依次各回复一条，新增账本为 3 条 `handed_off` / 3 条 `sent`，无 `uncertain`，结束后双账号服务恢复。这些窄范围本地结果均不是生产验收。

## v0.2.0 破坏性标识调整

项目名称为 `hermes-onebot-fleet`，Python 包为 `hermes_onebot_fleet`，适配器为 `OneBotFleetAdapter`；插件 ID 为 `onebot-fleet`，平台为 `onebot_fleet`，原生环境变量前缀为 `ONEBOT_FLEET_`，规范会话前缀为 `obf:`。这些标识构成破坏性调整，不提供旧标识兼容保证。请按下方示例创建全新隔离部署，重新配置凭据、准入和通知目标；不要直接复用已有会话或账本作为迁移，也不要修改正在使用的接入。

```text
NapCat 主号 ──认证 WebSocket──┐
                            ├─ onebot_fleet 原生适配器 ─ Hermes Gateway / Agent
NapCat 备号 ──认证 WebSocket──┘
```

## 已实现

- 多账号并连、身份/在线探测、断线重连；业务准入独立持久化，发送前保留合格的当前主角色，不按优先级自动回主；群消息检查账号是否在群。
- 群聊必须同时满足 **用户白名单 + 群白名单 + 结构化 @机器人**；机器人消息不触发。私聊也需要用户白名单。
- 跨账号观察去重，固定逻辑会话标识，保留真正的人类 user_id；不伪造“统一 QQ 用户”。
- SQLite 入站/发送账本、进程独占锁；取消/崩溃中的记录变为 uncertain，不由插件自动重放。
- 入站、排队交接、原始回复锚点、选号与实际发送前重新校验准入 revision；暂停后再放行不会复活旧任务。
- 接入 Hermes 原生会话、任务、命令和审批体系；不另写一套 Agent 执行器。
- 配置与令牌按 profile 隔离；仅允许 loopback WebSocket；模型无任意 OneBot 动作工具。
- v0.3 可显式启用适配器拥有的 loopback publisher：按 client/目标别名授权，独立 SQLite 持久去重，复用已有 OneBot 连接；不启用则不监听。
- v0.3 拒绝无稳定发布键、无原生回复上下文的主动发送（包括旧原生 cron 投递路径）；正常聊天回复锚点保留。后台 worker 必须只经 publisher 交付，不能回退原生 cron 或另开 OneBot sender。

## 必须知道的边界

1. **没有 exactly-once 保证。** OneBot 发送与本地 SQLite 提交不是原子事务；publisher 在重启时将 `sending` 置为 `unknown`，不自动重试，也不在已发出后的失败中换备号重发。保守 `unknown` 可能包括实际上未发送或已送达但未记录 ACK 的情况，需人工与 QQ 核对，不能自动补发。只有 `sent` 且带 `message_id` 才是已确认 OneBot ACK，不代表人已收到/读到。原生聊天仍依赖 Hermes 自己的执行/恢复语义；publisher 不提供宿主全局执行去重。
2. 账号故障切换 ≠ Hermes 主机高可用。Gateway 停机、NapCat 不重放的消息、鉴权前事件和队列溢出可能丢失；没有跨机器选主。
3. 私聊发送给离线主号的消息不会自动出现在备号。**跨账号入站冗余主要用于两个号都在的群。** 私聊要主动联系在线账号。
4. OneBot message_id 可能各账号不同。去重采用逻辑空间/聊天/用户/秒级时间/文本，因此同人同群同秒相同文本可能被合并。
5. 不支持图片、语音、文件、原生引用回复、流式编辑、按钮和独立进程 cron 发送。原生 clarify/审批的文本交互与通知尚未 QQ 实测；更名前后的双账号切换验收均不覆盖这些功能。
6. QQ 自动化的登录与账号风控风险由用户自行评估。不要使用重要主号直接进行故障实验。

详见[架构与可靠性边界](documentation/architecture.md)、[权限](documentation/permissions.md)和[测试地图](documentation/tests.md)。

## 本次源码改进：减少异常流量，而非承诺“防封”

参考外部实践，依次补充了：**只读请求节流 → 重连稳定性 → 跨目标失败聚合止损 → 分钟级突发限制**。正常健康巡检默认 60 秒、闲置 180 秒；成员预检独立持久配额；短暂断连不重置指数退避；多目标失败形成必须人工精确解除的持久业务 hold，不能靠换号、重启或 `health reset` 解除；全部入口共用滚动分钟 6/目标 3 次与原小时上限。

**本次仅修改仓库并做离线验证，未升级运行中的 profile，未重启 Gateway/NapCat，未操作真实 QQ。** 数值是可配置本地工程保护，不是腾讯的安全阈值；不新增 typing、拟人打字、自动试发或 unknown 补发。新增 `reads` / `stoploss` 本地诊断，预算库 v2→v3 仅在受控启动构造时事务迁移，CLI 不迁移，历史占用不清空。部署前须另行授权、停收并备份全部状态。详见[策略、参数、人工解除与迁移](documentation/resilience.md)和[本次离线验证](documentation/tests.md)。

## v0.5 P1/P2：分层健康、共享预算与本地告警

新增身份/传输/在线/后台/目标独立观测和持久锁存 circuit，全部账号的原生回复与 publisher 共用一个预算，按原始年龄拒绝旧任务并保留未知结果不重放。提供 `health`、`budget`、`alerts` 本地 CLI 与可独立运行的 JSONL 告警消费器，业务池不可用无需依赖 QQ 来记录告警。

**仅在另行授权的测试 profile 完成受控升级；不自动轮号、不强制登录、不自动解熔断。** 已启动独立本地 JSONL watcher，但未安装操作系统常驻服务/开机自启，日志记录不等于通知送达；尚未配置邮件/Webhook/离机监控。运维输出可能包含本地账号/目标标识，不含 Token、消息正文或验证材料。详见[完整策略、命令、状态文件及边界](documentation/resilience.md)；最终离线结果见[测试地图](documentation/tests.md)。

## v0.4 P0：人工准入，不自动轮号或强制登录

新增 `allowed / paused / quarantined / observing` 持久化业务状态、本地显式管理 CLI、原始回复/revision 门禁、跨重启的稳定主角色。只有人工 `allow` 可解除隔离；恢复连接或恢复准入不抢回仍合格的当前主角色，升主另行决定。它不是封号识别器，也不控制 NapCat 自己的登录/恢复行为。

先读[账号生命周期与管理命令](documentation/account-lifecycle.md)，再读[个人 QQ / NapCat 运维指南](documentation/qq-account-operations.md)和[日期、版本与信息来源](documentation/qq-account-sources.md)。升级前无 revision 的旧队列/回复拒绝；状态文件必须保留。降级旧插件会失去这些门禁，不能盲目回滚开流量。测试部署后，经账号本人确认可正常使用并另行授权，完成了三条真实群消息业务切换验收；准入恢复和验收后的升主均为明确人工管理动作，不增加自动轮号、强制保持登录或自动解封。验收范围见[测试地图](documentation/tests.md#guarded-p0-group-messages--2026-10-19)。

## v0.3 可选后台发布

在拥有 QQ 连接的 profile 的 `gateway.platforms.onebot_fleet.extra` 下合并（号码仅为示例）：

```yaml
publisher:
  enabled: true
  port: 18791
  clients:
    - id: ai-radar
      token_env: ONEBOT_FLEET_RADAR_PUBLISH_TOKEN
      targets:
        ai-test: obf:onebot-fleet:group:30001
      max_per_hour: 6
      quiet_hours:
        start: '22:00'
        end: '08:00'
        time_zone: Asia/Shanghai
```

`30001` 仍必须属于 Fleet 群白名单。为该 client 配置独立随机发布 token（至少 32 个非空白字符），不要把 OneBot/QQ token 给 worker。发布端仅监听 `127.0.0.1`，所有路径要求严格 `Authorization: Bearer <发布token>`；发送 `POST /v1/publications`，JSON 为 `{"publication_id":"radar-20261018-01","target":"ai-test","text":"示例摘要"}`，文本长度 1–1800 字符。通过 `GET /v1/publications/{id}` 与 `GET /v1/status` 查询自己的状态。

同 client 的 durable `publication_id` 必须稳定且不可改写：同 ID/相同正文和规范目标返回已有状态、不重发；改正文或目标返回 409。不要为网络重试或 `unknown` 随意换 ID。离线时允许 `pending` 等待，24 小时过期；有按 client 的每小时上限和有界队列。可选 `quiet_hours` 在服务端阻止静默期的新派发（含已暂存任务），期间仍可接收为 pending、查询及执行过期清理；省略则不启用。仅接受现代 Asia/Shanghai（固定 UTC+08）和不同的 HH:MM 起止点，区间含起点不含终点、可跨午夜。静默前已经发出的 RPC 无法撤回，不能保证远端绝不在边界后显示。worker 也必须执行一致的静默/新鲜度准入，不能把本地停止 POST 当成已暂存消息的撤回。该静默扩展仅完成离线测试，未启用或纳入上述真实验收。详见[后台任务与运行手册](documentation/cron.md)。

worker 使用独立项目、venv 和不含 QQ/OneBot 凭据的 profile，仅持有窄目标发布 capability；显式构造环境变量 allowlist，不能继承整个主 profile 环境。cron 必须设置 `deliver=local`、`failure-deliver=local`，publisher 是唯一对外交付路径。**同一 Windows 用户下的分目录/venv/profile 不是 OS 沙箱，不防恶意同用户进程读取文件、环境或凭据。** 官方来源及安装信任链不因 publisher 改变；不要改用非官方 Hermes 来源。

## 开发与本地测试

需要 Python 3.12–3.14，推荐 uv。**开发环境属于本仓库，不是 Hermes 管理的运行环境。**

```powershell
uv sync --extra dev --no-install-project
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check src tests tools plugin astrbot_plugin_onebot_fleet
.\.venv\Scripts\python.exe -m ruff format --check src tests tools plugin astrbot_plugin_onebot_fleet
```

离线测试使用本机临时 WebSocket 模拟端，不会登录 QQ、调用模型或修改现有 profile。
[真实宿主契约探测脚本](tools/probe_hermes.py)需通过 Hermes 选定的 Python/bootstrap 运行；它只在临时目录检查平台注册、适配器实例、原生 source/event 及即时重试契约，不连接 NapCat。

当前检查过的宿主：Hermes `0.22.1+78.gaff595f.dirty`、源码 `aff595ff116b2e5100b80579759978d911f0b116`，Windows Python 3.14；离线开发测试使用 Python 3.12。内部接口可能随 Hermes 更新变化，**不能据此承诺其他版本兼容**。

## 可选试装：先用全新隔离 profile

历史 v0.2 更名前后均完成上述本地真实双账号验收。v0.3 已通过窄范围本地跨 profile、原生定时任务与稳定发布 ID 重复提交/热重载验收，操作者确认两条发布各出现一次，普通群聊回复仍正常。双入口断网恢复及真实 ACK 丢失尚未通过；额外真实故障测试已暂缓。这不是通用生产兼容性保证，也不是迁移现有 profile 的操作指南。新部署仍应使用全新隔离 profile，只有在接受以上实验边界后再试装。

1. 使用原生 CLI 创建空 profile，不复制原有 QQ 适配器配置：

   ```powershell
   hermes profile create onebot-fleet-test --no-alias
   ```

2. 从源码仓库暂存插件，参数必须是**新 profile 的实际目录**：

   ```powershell
   $env:PYTHONPATH = 'src'
   .\.venv\Scripts\python.exe -m hermes_onebot_fleet.cli stage-plugin --hermes-home '<profile的绝对路径>'
   ```

   暂存仅创建该 profile 的插件目录；不覆盖已有插件、不写入凭据、不启用，也不修改模型设置。请勿把路径指向正在工作的旧 profile。

3. 参考[配置示例](examples/config.yaml)合并对应片段，不要整份覆盖原有配置。使用[环境变量示例](examples/.env.example)设置该 profile 的令牌和原生用户准入。示例号码都是假号码，两个 NapCat 账号分别使用独立随机 token。

4. 两个 NapCat 分别启用 OneBot 11 正向 WebSocket 服务，绑定 `127.0.0.1`、不同端口，消息格式 `array`，不报告自身消息。端口及 token 与配置一一对应；OneBot token 不是 WebUI token。不要让多个不同适配器同时处理同一组账号。

5. 通过 Hermes 原生插件/PM 管理启用和安装依赖：

   ```powershell
   hermes -p onebot-fleet-test plugins enable onebot-fleet
   hermes -p onebot-fleet-test plugins doctor onebot-fleet --ci
   ```

   同意依赖前检查来源。**不要对 Hermes 托管环境执行 pip/uv pip install**；暂存目录带本地依赖声明供 Hermes PM 使用。`plugins.isolation: in_process` 是原生平台适配器所需，不是 OS 沙箱。

6. 按安装版本的原生 gateway 管理方式加载新平台；如果使用共享 Gateway，先确认重载/重启影响。doctor 通过不代表真实 QQ 消息验收。

## 人工验收与回滚

- 主/备均在线：同一条群 @消息只进一次；不在白名单的人、群、无 @、机器人发送的消息均不触发。
- 发送新消息前断开主号：备号应接手，Hermes 会话不因物理账号变化而变化。
- 模拟“已发出但 ACK 丢失”：记录 uncertain，插件不得自行重发；另核对宿主恢复行为。
- 检查用户撤权、群撤权、重启、文本审批、原生通知；真实 QQ 上均需人工确认。
- 测试结束使用原生命令禁用新插件，恢复测试前备份；不要删除或重置原有 QQ profile。

查看本插件账本状态（不显示消息正文，不自动重试）：

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -m hermes_onebot_fleet.cli status --state '<profile>/plugin-data/onebot-fleet/state.sqlite3'
```

需要人工核对 uncertain 记录。账本保存消息与回复，按私密数据保护和备份；不要提交到 Git。参阅[配置/密钥与存储](documentation/variables.md)、[运行流程](documentation/flows.md)、[后台任务](documentation/cron.md)、[自动化边界](documentation/automation.md)。

MIT；参考与依赖说明见[第三方说明](THIRD_PARTY_NOTICES.md)。
