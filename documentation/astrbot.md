# v0.6：onebot-fleet 宿主边界与 AstrBot 接入

## 范围与实测声明

本次交付是现有仓库内的宿主解耦，不新建另一个仓库 / HTTP 中转服务，不更改正在工作的账号或 Hermes profile。现有 GitHub 仓库已按用户要求更名为 [abworks-dev/onebot-fleet](https://github.com/abworks-dev/onebot-fleet)，保留原仓库历史。发行名为 `onebot-fleet`，版本 `0.6.0`。Hermes 兼容入口保留；AstrBot 为新原生平台，不是 `aiocqhttp` 模拟器。

首版验证范围为离线模拟 RPC、临时 SQLite、宿主 doubles 和源码契约检查。不因此声称真实 AstrBot 实例、真实 QQ、陪伴插件（PC）、记忆插件（MC）、图片 / 文件素材收集或主动陪伴已经联调通过。v0.5 及更早的本机真实消息记录不可继承为本版验收。

## 模块与所有权

| 边界 | 职责 | 不负责 |
| --- | --- | --- |
| `onebot_fleet.core.FleetCore` | 账号连接、规范入站、去重、授权 revision、粘性主角色、预算、健康 / 止损、可靠文本发送、publisher | 人格、模型、长期记忆、Inbox 接收、任务执行 |
| Hermes 兼容适配层 | 解析 profile 配置 / 凭据，转成原生 `MessageEvent` / `SendResult` | 第二套发送策略 |
| AstrBot 适配层 | 原生 platform / event / UMO、严格文本组件检查、ACK 结果映射、插件生命周期 | 任意 OneBot API、媒体假兼容、跨用户身份合并 |
| 陪伴 / Inbox / Hermes worker | 后续独立集成的业务逻辑 | 绕过 FleetCore 直接发送 |

核心接收显式凭据映射，只使用配置指定的 token 变量名；宿主负责在自己的环境范围内提供值。核心不导入任何宿主。纯策略模块暂以旧 `hermes_onebot_fleet` 包为单一来源，新 `onebot_fleet` 包通过当前 profile / vendor 命名空间引用，确保异常类型、ContextVar 和配置类型 identity 一致，而不把不同 profile 的模块串入全局。

同一组物理 QQ 账号只允许一个活跃连接所有者。不同 profile、不同 venv、不同状态路径都不构成跨进程账号互斥或 OS 沙箱。**不能同时打开旧 Hermes Fleet、新 AstrBot Fleet 和 AstrBot 内置 OneBot 来处理同一组账号。** 现有实例锁只保护指定账本路径，不证明其他路径没有占用同号。

## AstrBot 安装与配置

完整例子见 [插件说明](../astrbot_plugin_onebot_fleet/README.md)。暂存命令见 [README](../README.md)。

1. 只在全新目录生成插件材料；CLI 拒绝覆盖已有目标。
2. 生成物包括原生插件根目录和 `_vendor` 下的两个 Python 包；不从未知 PyPI 版本拉取本项目，不写入 `sys.path`。缺失 vendor 时，源码模板可使用已安装的 canonical 包；存在但损坏的 vendor 不允许静默降级。
3. 放入隔离 AstrBot 实例的插件目录。插件配置 schema 不存账号凭据；Fleet 配置属于平台实例配置。
4. 固定平台实例 `id`（只允许字母、数字、下划线、短横线，长度 1–128），固定 `logical_id`。账号 token 只由环境变量提供，不粘贴进平台 JSON。
5. 群入口仍要求用户白名单、群白名单及结构化 @；私聊仍要求真实用户白名单。配置保护与 FleetCore 不变。默认配置没有账号，平台不启用。
6. 关闭分段回复、自动引用、自动 @、转图、TTS 和流式。保留普通纯文本路径，重启加载首版适配器。
7. 先做离线和宿主无账号检查，再由操作者另行安排真实 QQ 小范围测试；不要把重要私人主号作为故障试验账号。

源码核对宿主固定为 AstrBot `1b10813c70949071fa11153cc4ec8c51421d13d0`，不是任意新旧版本兼容保证。

## 会话、权限和发送结果

- UMO 为 `platform_id:GroupMessage:chat_id` 或 `platform_id:FriendMessage:chat_id`。物理主备切换不改变它；修改平台 ID 会切开 AstrBot 会话。`logical_id` 不在 UMO 中，但影响 core 的目标命名空间与历史发送授权，不能随升级改名。
- 原始人类 QQ ID 保留。群内机器人消息不触发；不凭昵称、模型推测或共享账号替代真实用户。
- 普通回复锚定原始入站 dedup key，发送前仍检查它对应的目标、准入 revision、年龄和当前持久状态。UMO 字符串、metadata、公开 `create_event` 均不会被适配器当作回复授权。anchor 是相关性字符串，不是密码学 capability；本实现防止正常 API 误用，不抵抗可执行任意 Python 的同进程恶意插件。原始 event 对象可在 core 的有效年龄与准入窗口内继续锚定回复，只有 `Context.send_message` 路径额外要求当前活跃执行。
- 普通事件回复、`react`、`process_buffer`、当前活跃事件内同会话的 `Context.send_message` 最终走同一个 core。无活跃上下文 / 跨会话的 `Context.send_message` 抛出受控异常，而非返回一个会被宿主忽略的 `False`。
- 支持整条 `Plain` 组件链，不支持将图片、引用、文件等组件静默删除后“发送成功”。混合链在调用 core 前整体拒绝。原始 client 和流式明确拒绝。
- 确认 `success` 且非空 `message_id` 后才更新宿主基础发送指标 / `_has_send_oper`。并发相同文本复用稳定键；失败或 unknown 在事件内锁存，后续分段即便改文本也不再发送。
- core 的 `handed_off` 只表示入站已交给宿主队列，不是模型执行完成。队列满会明确失败，不能无限等待整个宿主任务并阻塞 Fleet 单入站 drain。
- publisher 是未锚定后台投递的独立能力入口，拥有独立 client / 目标授权、稳定 publication ID 与账本，但与普通回复共用核心预算 / 准入 / 止损。不要调用原生 UMO 主动投递来绕过它。
- `uncertain/unknown` 不补发、不换号重发。OneBot 与本地提交不是原子事务；ACK 不代表人已读，没有 exactly-once 保证。

## 已知宿主成功语义限制

固定版本 AstrBot 的 RespondStage 捕获发送异常后，仍可能调用**第一个** `OnAfterMessageSentEvent`，并继续分段流程。适配层会停止 / 清理结果、失败锁存并阻止后续物理发送，但不能阻止宿主第一次错误调用钩子，也不做全局 monkeypatch 或异常基类技巧。

因此：**不要把宿主 after_sent 当作送达凭证。** 首版通过 `event.fleet_receipts` 提供明确 ACK / 失败视图。依赖该钩子的 PC、MC、审计、关系或任务状态插件必须单独检查，未通过前不要宣称无损兼容。自动分段等高级能力暂不启用。

## 状态保持、切换与回滚

本次实现不自动切流，不修改现场状态。未来升级按以下顺序另行操作：

1. 停止新入站和所有 publisher / worker，等待已派发任务收束；对可能已发但无回执的任务保留 unknown。
2. 记录当前逻辑 ID、平台 ID、主角色、授权策略和实际状态路径；备份主 SQLite 及全部 sidecar 和 publisher 状态。不能只复制一份主账本。
3. 禁用并停止原连接所有者，确认实际没有第二实例使用这些账号；不能仅凭目录不同或 `enabled: false` 推断停止。
4. 在离线副本做状态 / schema 检查，保持原 logical_id / 路径，绝不以新名新库重建预算。跨宿主迁移仍需显式确认 AstrBot UMO 与 Hermes 会话不同；本版不迁移历史人格 / 会话。
5. 新实例先无业务启动，再按人工准入恢复小范围流量；检查 sidecar 的历史占用、hold 和 unknown 仍在。
6. 若回滚，先停止新入口。恢复时必须保留切换之后新增的终态和预算，不能简单还原旧快照后重新放流，否则可能补发已发送任务。无法对齐时维持停发并人工核对。

状态 / 预算 / 只读请求 / 重连 / 止损的既有约束详见 [可靠性策略](resilience.md)、[账号生命周期](account-lifecycle.md) 与 [配置及存储](variables.md)。

## 下一阶段验收（本次未执行）

- 固定 AstrBot 真实实例冷启动：禁用插件不构造 / 不连接；启用插件先初始化 capability，再启动平台；关闭只清理自己的实例。
- 双号同群同条 @ 去重、真实人类身份、主备切换 UMO 不变；暂停 / 放行不复活旧入站。
- 普通文本、事件主动响应、当前事件内同会话 `Context.send_message` 均具有同一 ACK / 失败语义；跨 UMO / 未锚定调用零发送。
- 混合媒体、自动引用、流式及直接 client 路径明确拒绝；不让 PC 误报成功，不形成第二 sender。
- ACK 丢失 / 取消 / 重启保持 unknown，不重发；分段失败终止后续片段。
- 群 / 私聊实际送达由操作者与 QQ 界面核对；不得据此推断离线私聊补收、生产稳定或免风控。
