# Hermes OneBot Fleet

**实验性 v0.2.0 原生 Hermes 平台插件，不是已经验收的生产接入。**

多个 NapCat / OneBot v11 账号作为同一 Hermes profile 的 QQ 入口。无需 NoneBot、HTTP API 代理或修改 Hermes 核心。目前只支持文本；当前协议实现与验证范围仅限 OneBot v11 / NapCatQQ，不代表已兼容其他 OneBot 实现。更名前的同一实现已于 2026-10-18 完成本地真实双账号群聊主号回复、断开主号后由备号处理 @主号、恢复后回到主号的三条消息验收，账本为 3 条 `handed_off` / 3 条 `sent`。更名后的 v0.2.0 也已于同日完成相同三步实测：主号、备号、主号依次各回复一条，新增账本为 3 条 `handed_off` / 3 条 `sent`，无 `uncertain`，结束后双账号服务恢复。这些窄范围本地结果均不是生产验收。

## v0.2.0 破坏性标识调整

项目名称为 `hermes-onebot-fleet`，Python 包为 `hermes_onebot_fleet`，适配器为 `OneBotFleetAdapter`；插件 ID 为 `onebot-fleet`，平台为 `onebot_fleet`，原生环境变量前缀为 `ONEBOT_FLEET_`，规范会话前缀为 `obf:`。这些标识构成破坏性调整，不提供旧标识兼容保证。请按下方示例创建全新隔离部署，重新配置凭据、准入和通知目标；不要直接复用已有会话或账本作为迁移，也不要修改正在使用的接入。

```text
NapCat 主号 ──认证 WebSocket──┐
                            ├─ onebot_fleet 原生适配器 ─ Hermes Gateway / Agent
NapCat 备号 ──认证 WebSocket──┘
```

## 已实现

- 多账号并连、身份/在线探测、断线重连；发送前按优先级选健康账号，群消息检查账号是否在群。
- 群聊必须同时满足 **用户白名单 + 群白名单 + 结构化 @机器人**；机器人消息不触发。私聊也需要用户白名单。
- 跨账号观察去重，固定逻辑会话标识，保留真正的人类 user_id；不伪造“统一 QQ 用户”。
- SQLite 入站/发送账本、进程独占锁；取消/崩溃中的记录变为 uncertain，不由插件自动重放。
- 入站排队任务在交给 Hermes 前重新校验当前权限，撤销授权会拒绝旧任务。
- 接入 Hermes 原生会话、任务、命令和审批体系；不另写一套 Agent 执行器。
- 配置与令牌按 profile 隔离；仅允许 loopback WebSocket；模型无任意 OneBot 动作工具。

## 必须知道的边界

1. **不是完整的“绝不重复投递”实现。** 插件不会在发送结果不明后换号重发，并已阻止原生即时纯文本回退。但 Hermes 自己的 delivery ledger / 中断恢复可能独立重投递；当前缺少可供插件关联的稳定恢复标识。该边界未解决，不能宣称崩溃恢复 exactly-once，也不建议据此直接替换稳定接入。
2. 账号故障切换 ≠ Hermes 主机高可用。Gateway 停机、NapCat 不重放的消息、鉴权前事件和队列溢出可能丢失；没有跨机器选主。
3. 私聊发送给离线主号的消息不会自动出现在备号。**跨账号入站冗余主要用于两个号都在的群。** 私聊要主动联系在线账号。
4. OneBot message_id 可能各账号不同。去重采用逻辑空间/聊天/用户/秒级时间/文本，因此同人同群同秒相同文本可能被合并。
5. 不支持图片、语音、文件、原生引用回复、流式编辑、按钮和独立进程 cron 发送。原生 clarify/审批的文本交互与通知尚未 QQ 实测；更名前后的双账号切换验收均不覆盖这些功能。
6. QQ 自动化的登录与账号风控风险由用户自行评估。不要使用重要主号直接进行故障实验。

详见[架构与可靠性边界](documentation/architecture.md)、[权限](documentation/permissions.md)和[测试地图](documentation/tests.md)。

## 开发与本地测试

需要 Python 3.12–3.14，推荐 uv。**开发环境属于本仓库，不是 Hermes 管理的运行环境。**

```powershell
uv sync --extra dev --no-install-project
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check src tests tools plugin
.\.venv\Scripts\python.exe -m ruff format --check src tests tools plugin
```

离线测试使用本机临时 WebSocket 模拟端，不会登录 QQ、调用模型或修改现有 profile。
[真实宿主契约探测脚本](tools/probe_hermes.py)需通过 Hermes 选定的 Python/bootstrap 运行；它只在临时目录检查平台注册、适配器实例、原生 source/event 及即时重试契约，不连接 NapCat。

当前检查过的宿主：Hermes `0.22.1+78.gaff595f.dirty`、源码 `aff595ff116b2e5100b80579759978d911f0b116`，Windows Python 3.14；离线开发测试使用 Python 3.12。内部接口可能随 Hermes 更新变化，**不能据此承诺其他版本兼容**。

## 可选试装：先用全新隔离 profile

更名前后均已完成上述本地真实双账号验收；这不是通用生产兼容性保证，也不是迁移现有 profile 的操作指南。新部署仍应使用全新隔离 profile，只有在接受以上实验边界后再试装。

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
