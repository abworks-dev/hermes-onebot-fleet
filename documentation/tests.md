# Verification map

## v0.6 host-neutral core and AstrBot — existing offline coverage

Source-only refactor; no running profile upgrade, live AstrBot/QQ startup, credential change, ledger reset or remote release. All new dynamic tests use temporary state and host doubles; static host contract review is pinned to AstrBot `1b10813c70949071fa11153cc4ec8c51421d13d0` and is not a real-instance acceptance.

Final local Windows / Python 3.12 regression: **1254 tests + 39 subtests passed in 135.76s** (before refactor: 1201 + 39). The focused core/AstrBot/staged-artifact/CLI/identity set passed **58 tests**. `ruff check src tests tools plugin astrbot_plugin_onebot_fleet` and format-check passed (**70 files already formatted**); `git diff --check` passed. One initial full run failed only because the staged-artifact fixture omitted the newly mandatory platform `enable: true`; the fixture was corrected without relaxing the guard, then the whole suite was rerun successfully.

| Use case / documented rule | Expected result, including denial | Existing evidence |
|---|---|---|
| Host-independent safety orchestration | Core imports no host; explicit secrets; admission/revision, sticky routing, shared budgets and holds still gate sends; unknown is final | [Core tests](../tests/test_core.py), original safety suites below; [architecture](architecture.md), [contract](astrbot.md) |
| Hermes compatibility | Original platform/plugin identifiers, canonical targets and default state path stay unchanged; duplicate temporary stage refused | [Identity and native loader](../tests/test_identity.py), [CLI](../tests/test_cli.py), old adapter/integration suites |
| AstrBot activation and lifecycle | Disabled/inactive platform cannot construct/connect; initialized Star grants its own platform; revoke before terminate; no import-time activation | [AstrBot tests](../tests/test_astrbot.py); [integration contract](astrbot.md) |
| Anchored pure-text delivery | True human and stable UMO; entire non-Plain chain denied; same-event concurrent duplicate shares a key; ACK required before base success; failure/cancel latches later sends | [AstrBot tests](../tests/test_astrbot.py); [permissions](permissions.md) |
| Context and proactive boundaries | Only current active same immutable UMO is accepted; unanchored public event, raw client and stream denied; publisher remains separately authorized | [AstrBot tests](../tests/test_astrbot.py), existing publisher suites; [flows](flows.md) |
| Self-contained plugin staging | Uses only plugin-local vendored core/policy classes, no global host package alias; no state file or network on import/construction; inactive/revoked plugin denied | [Staged artifact test](../tests/test_packaging.py), [CLI tests](../tests/test_cli.py) |

### Proposed acceptance — not executed

| Type | Check / expected denial | Status |
|---|---|---|
| Guarded host integration | Real pinned AstrBot cold start/stop with no accounts; disabled plugin must start no platform; unload affects only owned instances | Proposed, no instance run |
| Guarded live | Separately authorized QQ text and same-group failover; UMO remains stable; ACK loss/cancel never causes replay | Proposed, no QQ action |
| Automated + guarded plugin integration | PC/MC must use explicit Fleet receipts, not host `after_sent`; media or unsupported client paths must fail visibly | Proposed, no compatibility verdict |
| Manual migration review | Preserve complete ledger/sidecar history, namespace and post-cutover terminal states; one connection owner | Proposed, no running profile touched |

### Gaps and non-guarantees

- **High: host/plugin end-to-end behavior.** Host doubles and source review cannot prove the real runtime or companion plugin integration. AstrBot may call the first `after_sent` hook after an exception; do not treat it as ACK. Same-process Python plugins are not sandboxed.
- **Medium: packaging installation.** Source staging is tested, but wheel build/install acceptance was not completed: the local build failed at the final file replacement with `PermissionError`. No alternate-shell/path retry or ACL change was used; the generated temporary artifact was removed after exact path/hash verification. No package was installed in a managed Hermes environment.
- **Out of scope:** media, active UMO sends, private offline-message recovery, OS/process high availability, universal host-version compatibility and immunity from account restrictions.
- The v0.6 checks are local only; no remote CI job or branch-protection requirement was verified in this run. Historical CI results below apply only to their recorded revisions.

## 风控经验落地 — source/offline hardening, 2026-10-22

本次按顺序完成只读节流、重连稳定性、跨目标失败聚合止损、分钟级突发上限。仅改仓库代码/测试/示例/文档；**没有升级既有 profile，没有重启 Gateway/NapCat，没有操作真实 QQ，也没有本次实测腾讯侧请求数或解限效果**。历史部署记录不构成本次部署。以下新测试使用临时 SQLite、伪时钟和 fake RPC；保留原有本机假服务测试，不访问 QQ。

| 新保护 | 离线覆盖 |
|---|---|
| 独立成员查询预算 | [ReadRequests](../tests/test_read_requests.py)：严格参数与 schema、滚动窗口、跨句柄/进程并发、重启/回拨、策略收紧与持久间隔、不退还占用、损坏/存储故障 fail-closed |
| 健康巡检节流与本地心跳分离 | [Request guards](../tests/test_request_guards.py)：正常/闲置/失败退避、断线零查询、single-flight/取消、旧 socket 不认证新连接、预算否决先于预检、不缓存正向权限、observer stale 不被巡检降频误触发 |
| 本地连接稳定性 | [Reconnect](../tests/test_reconnect.py)：指数上限/jitter 边界、短抖动不重置、持续验证稳定期、健康失败中断、取消/Token 刷新、账号独立及无隐式登录 |
| 持久聚合 hold | [StopLoss](../tests/test_stop_loss.py)：不同目标/账号阈值、原意图永久去重、手动精确水位、重启/回拨/并发/策略变更、损坏与事务失败 |
| 聚合业务与操作边界 | [Integration](../tests/test_stoploss_integration.py)、[CLI](../tests/test_stoploss_cli.py)：原生/publisher/换主同门禁、保留入站授权、health reset 不清聚合 hold、派发前故障不记聚合失败、派发中最终复核、unknown 三种情形永久不重放、无网络诊断与精确确认解除 |
| 滚动分钟硬上限与 v2→v3 | [Burst budget](../tests/test_burst_budget.py)：四种窗口及两类间隔同时生效、准确截止/空闲不攒额度、check 不构成 permit、跨线程/进程并发、完整旧库验证/原子迁移/失败回滚/诊断零写入、旧占用和永久 tombstone 保留 |
| 分钟预算提前否决 | [Burst integration](../tests/test_burst_integration.py)：原生/不同 client/换主/重启共用目标额度；全局耗尽后新群不查询成员；未派发原 ID 延期、到期仅发一次 |

最终完整回归：**Windows / Python 3.12.11，1201 tests + 39 subtests passed，100.65s**；`ruff check --no-cache src tests` 全部通过，`ruff format --check src tests` 为 **44 files already formatted**，`git diff --check` 通过。相同环境修改前基线为 627 tests + 39 subtests passed（57.57s）。本次未运行新版 Linux/Python 3.14 CI 或已安装 Hermes 的升级探针；以上不是生产可用或“免封”验收。

## v0.5 P1/P2 — source/offline verification, 2026-10-19

Complete local Windows/Python 3.12 regression: **627 tests + 39 subtests passed in 79.43s**.
Ruff check/format passed for 34 Python files, dependency lock updated to 0.5.0 without new packages.
The installed Hermes/Python 3.14 contract probe passed using the committed dependency environment
without launcher maintenance: `network_started=false`, `profile_modified=false`, one terminal
uncertain-send attempt, two unkeyed sends denied, one original anchored send and its old-revision
reply denied after pause/allow. It uses temporary stores/fake transport, not a live daemon exercise.

A first standard bootstrap invocation entered the host's pre-existing source-update dependency
completion and was stopped; it is **not** counted as a side-effect-free probe. The final probe reads
selected site-packages directly without bootstrap/launch preparation. During that source-only phase
no Fleet profile/plugin was upgraded or Gateway/NapCat restarted, and no QQ operation or external
notification test was performed. Source commit `f0165f9` passed all four Windows/Linux × Python
3.12/3.14 offline CI jobs ([run 39212419697](https://github.com/abworks-dev/onebot-fleet/actions/runs/39212419697)).
A later, separately approved scoped deployment/local-watcher check is recorded below.

| Rule | Offline coverage |
|---|---|
| Independent evidence and locked circuits | [Health state](../tests/test_health.py): identity/offline/backend/unknown kept distinct; exact-scope manual reset does not allow/promote or clear other scopes |
| Global budget across accounts/entries | [Budget state](../tests/test_delivery_budget.py): atomic limits, no refund/switch reset, delayed completion, restart fences, current policy, clock rollback and transaction failures |
| Real adapter orchestration | [Integration](../tests/test_resilience_integration.py): native+publisher sharing, target-only circuits retain inbound, deferred never-sent work vs terminal unknown, original TTL across slow preflight/claim, completion failure preserves ACK but holds further work |
| Read-only transport evidence | [Health transport](../tests/test_resilience_transport.py), [RPC tests](../tests/test_transport.py): offline/null/good=false/identity mismatch even if status fails, no mutating probes |
| Local operator boundary | [CLI](../tests/test_resilience_cli.py): no state initialization, explicit confirmed exact reset, no secret echo, no sidecar/output clobber |
| QQ-independent alert consumer | [Alerts](../tests/test_alerts.py): durable cursor/coalescing, stale/unavailable observer, bounded history, corrupt cursor/output refusal, crash window and path protection |
| Review race regressions | [Edge cases](../tests/test_resilience_edges.py): original age crosses expiry during preflight/claim, identity evidence survives later status failure |
| Existing safety contracts | Full earlier P0, publisher, ACL, native-loader and uncertainty regressions remain in the same suite |

[Resilience policy](resilience.md) defines in-flight limits, persistence/privacy, defaults and what
remains unverified. Local incident logs/JSONL are not proof of delivery to an external operator;
real restriction recovery, delayed/lost remote ACK, long soak and host failure are not accepted.

## Guarded v0.5 deployment and local watcher — 2026-10-19

A separate operator authorization permitted upgrading the existing test profile and starting
local alert consumption. This is deployment/observation evidence, not a v0.5 real-message or
failure-injection acceptance.

- Stopped only the owning profile using the native control surface; cold-copied its plugin,
  configuration and complete plugin state directory (including WAL/SHM present at the time).
  Verified canonical SQLite snapshots and **28 persistent backup-file hashes**. No core patch,
  shared Gateway restart, NapCat restart, QQ login/logout or send action was performed.
- Staged v0.5 source matches the repository; refreshed the host plugin manager while this
  transport was disabled, then reactivated it. Both account admission revisions, both allowed
  states and the manually selected current primary were preserved exactly.
- The actual staged package passed **299 P1/P2 tests** on Windows/Python 3.12, using temporary
  stores and mock/local-only transports. They did not mutate the live ledgers or diagnostics.
- Both existing Gateway-owned authenticated connections reported the expected identity,
  online/good status and no diagnostics/circuits. Original message/publication counts stayed
  unchanged. New shared-budget state uses the documented defaults with no reservations;
  this is configuration/startup verification, not proof of real-send throttling.
- Started a separate singleton watcher with an explicit OS-environment allowlist, no injected
  QQ/publisher credentials and no Hermes bootstrap. Its 10-second polls/30-second stale threshold,
  fresh cursor, running process and empty stderr were checked. It exported the startup transient
  network/pool incidents as **resolved**; they are not account-risk diagnoses.
- A separate filesystem-only fixture with a nonexistent health database exported one
  `observer_unavailable` transition and zero duplicate transitions on the next poll. It neither
  interrupted the Gateway nor changed its live health database.

The output and cursor are private local files, not email/Webhook/QQ notification delivery.
Same-user file access is not sandboxed. No OS service, autostart, supervisor or off-host monitor
was installed. The watcher must remain running; its own exit/host outage needs a separate outer
monitor. Account IDs, exact paths, credentials and operational receipts remain outside the repo.
No real v0.5 chat/failover, remote ACK loss, account restriction, host failure, automatic restart
or external notification delivery was tested.

## v0.4 P0 — historical offline result, 2026-10-19

Complete local Windows/Python 3.12 regression: **328 tests + 39 subtests passed**. Ruff lint/format
passed. The installed Hermes/Python 3.14 native contract probe passed with `network_started=false`,
`profile_modified=false`, one terminal uncertain-send attempt and two unkeyed proactive sends denied.
It also exercised an admitted original reply and rejection of that old reply after pause/allow,
using temporary plugin ledgers and a fake transport. The native retry body is inspected directly,
excluding only its metrics decorator to avoid emitting profile telemetry.
The local host probe itself is **not** a full host-integration suite or live QQ test. Separately,
source commit `289de62` passed all four Windows/Linux × Python 3.12/3.14 offline CI jobs
([run 39201736434](https://github.com/abworks-dev/onebot-fleet/actions/runs/39201736434)).
During that implementation phase no profile plugin was upgraded or Gateway reloaded. A subsequent,
separately authorized test-profile deployment is recorded below; it is not a QQ message acceptance.

| P0 rule | Positive / negative coverage | Evidence |
|---|---|---|
| Persistent admission and revision | Restart/config re-add preserve blocked state; remove invalidates old revision; missing/unknown account/schema/namespace fail closed | [Account state tests](../tests/test_account_state.py), [state](../src/hermes_onebot_fleet/account_state.py) |
| Explicit operator control | Missing confirmation/account/reason denied; status never initializes/reactivates state; allow and promote are separate; audit retained | [CLI tests](../tests/test_account_cli.py), [CLI](../src/hermes_onebot_fleet/cli.py) |
| Sticky primary | Health failover selects backup; recovered high-priority account does not preempt it across restart; target-only membership failure preserves global preference | [Gate tests](../tests/test_account_gates.py), [review regressions](../tests/test_p0_review.py) |
| All ingress/handoff gates | Blocked online account cannot enqueue; queued/legacy/re-admitted stale revision cannot reach host; ingress snapshot avoids split-read authorization | [Gate tests](../tests/test_account_gates.py), [review regressions](../tests/test_p0_review.py) |
| Source reply admission | Reply must match an original handoff, target and current source revision; arbitrary anchors, late revoked/legacy work denied | [Gate tests](../tests/test_account_gates.py), [adapter tests](../tests/test_adapter.py) |
| Pre-dispatch fencing | Source/chosen account revocation during awaited preflight or sending claim is fenced even after immediate allow; known no-send tombstone cannot replay | [Review regressions](../tests/test_p0_review.py), [adapter](../src/hermes_onebot_fleet/adapter.py) |
| In-flight and publisher identity | Already-started RPC cannot be recalled; duplicate sent/unknown returns original receipt even when all accounts later pause; only never-attempted NoRoute may wait | [Gate tests](../tests/test_account_gates.py), [review regressions](../tests/test_p0_review.py), existing [publisher tests](../tests/test_publisher.py) |

[Lifecycle and commands](account-lifecycle.md) define first-start allowed defaults, quarantine latch,
local filesystem authorization, in-flight cutoff and upgrade/rollback limits. These tests do not
detect actual account restrictions or validate NapCat recovery, manual verification, real risk
controls, production reliability or remote exactly-once delivery. All older live evidence below
is version-specific; historical priority-based automatic return-to-primary is not the v0.4 policy.

## Guarded local P0 deployment — 2026-10-19

This is a separately approved **business-held test-profile deployment**, not live QQ message or
production acceptance. Source behavior is unchanged from the offline-tested v0.4 implementation.

- Backed up the old plugin/configuration and checked SQLite snapshots; stopped only the owning
  test profile. Seeded both account business holds **before the first v0.4 adapter connect**.
- Refreshed the host plugin manager with the transport disabled before reactivating that profile,
  preventing a cached pre-P0 adapter from briefly handling traffic. Shared Gateway PID stayed the
  same; no core patch, other-profile restart, NapCat launch, QQ login or forced account outage.
- Verified the copied v0.4 source matches the repository. Running the account-state/CLI/gate/review
  regressions against the **actual deployed package path** passed **180 tests** on Windows/Python 3.12.
  Those tests use isolated temporary stores and fake transports; they do not mutate production ledgers.
- Applied a test quarantine and attempted `observe`; quarantine remained latched across a native
  unload/reload of only the test profile. Final administrative state: one account `quarantined`,
  one `paused`, no current primary and no account `allowed`. This is an operator test state, not a
  diagnosis of a QQ restriction. No automatic allow or login operation was used.
- Original message ledger stayed **3 handed_off / 6 sent**, publication ledger **2 sent**. No new
  publication was submitted and no QQ test message was sent. Private receipts/backups remain local;
  account IDs, tokens and configuration are not committed here.

**Not verified by this deployment:** full Gateway/OS restart, real QQ reply/standby takeover,
real-risk recovery, transport reconnect delivery, revoked real model work or actual ACK loss.
At the end of that deployment phase the original holds remained in place; release and real-message
testing required separate operator approval. The subsequent approved group-message phase is below.
Old-work fences and ambiguous sends still have offline evidence only. Old versions do not enforce
these holds: a downgrade must not reopen traffic.

## Guarded P0 group messages — 2026-10-19

A separate operator approval permitted starting the existing local runtimes, completing required
login/verification manually, then explicitly allowing the two confirmed usable accounts. Existing
owner/group/structured-mention allowlists and disabled tools were retained. No guild/channel,
friend/group-management action, deliberate QQ outage or account restriction was used.

1. The allowlisted owner mentioned the initial primary; that primary answered the first marker.
2. The operator-authorized local `pause` blocked that account's **business admission**, not its QQ
   login or OneBot listener. A second owner message still mentioning it was answered by the standby.
3. After explicit `allow` of the initial primary, a third owner message was answered by the same
   standby. Re-admission did not preempt the stored current primary.

The owner confirmed exactly one visible reply per step. Comparing private baseline/final snapshots
found **exactly 3 new handed_off / 3 new sent**, each with a valid original revision/OneBot message
ID and account sequence **initial primary → standby → standby**. No new failed/uncertain/pending
message rows appeared; publication receipts were unchanged. Both authenticated services reported
online with their expected identities at the final connectivity check.

After confirming all three steps, the operator explicitly chose to **promote the initial primary
back**. The local management action changed the stored primary only, without login or send calls;
no fourth test message was sent. Both accounts ended `allowed`. This explicit promotion is not
automatic failback. Private account IDs, receipts and runtime details are not committed here.

This is narrow real **ordinary-group business-admission/sticky-selection** evidence. It does not
validate network/QQ-process failure, freezes or their recovery, private-chat takeover, full Gateway/
OS restart, real in-flight revocation, crash/ACK loss, automatic risk recognition, long-run stability
or a global exactly-once guarantee. Optional additional fault tests remain outside this acceptance.

## Existing coverage — historical v0.2 baseline

Historical offline Windows run: **45 tests passed, 39 subtests passed** on Python 3.12; Ruff lint and format checks passed.
Historical real installed Hermes contract probe on Python 3.14 passed without network startup or profile modifications.
These counts/results are preserved as v0.2 evidence. The complete v0.3 safety suite, including optional server-side quiet hours, passed locally on Windows/Python 3.12 with **148 tests + 39 subtests**, plus Ruff lint/format. A real-Hermes Python 3.14 contract probe also passed without network/profile mutation. Narrow live publisher evidence is recorded below; it does not establish production readiness.

Historical local live acceptance on 2026-10-18 used the same implementation before identifier renaming:
three real user group messages exercised primary reply, standby handling @primary after disabling the
primary OneBot service, and return to primary after restoration. The ledger recorded exactly
**3 handed_off / 3 sent**. Account IDs and private deployment details are not published. This is narrow
local evidence, not a production verdict or proof of other features.

Historical post-rename local check on 2026-10-18: both authenticated OneBot connections reported the
expected account identity, online/good status and membership in the allowed test group.
The renamed plugin passed the real-host contract probe and profile doctor; current owner/group
allowlists and disabled tool access were rechecked.

Historical post-rename v0.2.0 live acceptance repeated all three message steps on the same date:
primary sent FLEET-1; after disabling only its OneBot listener, standby answered a structured
@primary with FLEET-2; after restoring the listener, primary sent FLEET-3. The owner confirmed
exactly one visible reply at each step. Starting from an empty renamed ledger, the final counts
were **3 handed_off / 3 sent** (primary 2, standby 1), with no uncertain/failed/pending rows.
Both authenticated services were online again at the end. This is narrow local text/group evidence,
not a QQ-process/host outage test, global exactly-once guarantee or general production verdict.

| Use case / rule | Expected deny or success | Evidence | Status |
|---|---|---|---|
| Local account configuration | Reject non-loopback, URL credentials, invalid IDs/weak token/type/unknown fields | [Policy tests](../tests/test_policy.py), [config](../src/hermes_onebot_fleet/config.py) | Existing automated unit |
| User AND group and @ | Unauthorized user/group, fake CQ/@all, bot/self sender, bad structured events denied | [Policy tests](../tests/test_policy.py), [protocol](../src/hermes_onebot_fleet/protocol.py) | Existing automated unit |
| Recovered queued authorization | Revoked owner/group/namespace/mention rejected; drain marks denied without host invocation | [Recovery tests](../tests/test_recovery_policy.py), [adapter tests](../tests/test_adapter.py) | Existing automated unit |
| Authenticated socket | Bearer auth, login identity, online status, reconnect, RPC echo, timeout/disconnect without replay | [Transport tests](../tests/test_transport.py), [transport](../src/hermes_onebot_fleet/transport.py) | Existing loopback integration, no QQ |
| Account selection | Offline primary skipped; group membership preflight may examine standby | [Adapter tests](../tests/test_adapter.py), [adapter](../src/hermes_onebot_fleet/adapter.py) | Existing host-double unit |
| Uncertain side effect | No second-account send after timeout/cancellation; native immediate fallback suppressed | [Adapter tests](../tests/test_adapter.py), [host probe](../tools/probe_hermes.py) | Existing automated unit + local real-host probe |
| Reply identity | Same answer to different canonical reply anchors is not suppressed by stale native task context | [Adapter tests](../tests/test_adapter.py) | Existing automated unit |
| Persistent ledger | One atomic claim, queued-only recovery, sending -> uncertain, handed_off not requeued | [Store tests](../tests/test_store.py), [store](../src/hermes_onebot_fleet/store.py) | Existing automated unit |
| Source staging | Existing profile config and credentials unchanged; invalid profile/missing status DB denied; existing plugin refuses overwrite | [CLI tests](../tests/test_cli.py), [CLI](../src/hermes_onebot_fleet/cli.py) | Existing temporary-profile integration |
| One process owner | Second OS file lock fails; release permits takeover | [Store tests](../tests/test_store.py), [lock](../src/hermes_onebot_fleet/lock.py) | Existing Windows unit |
| Native registration | Real adapter ABC, platform registration/config, canonical source and empty metadata | [Host probe](../tools/probe_hermes.py), [plugin](../src/hermes_onebot_fleet/plugin.py) | Existing local real-host probe, not full gateway E2E |
| Two real QQ accounts, pre/post rename | Primary reply; disable primary OneBot service and @primary reaches standby; restore primary and replies return; each run exactly 3 handed_off / 3 sent | Local acceptance repeated on v0.2.0, 2026-10-18; [architecture](architecture.md) | Narrow guarded live pass; three single replies confirmed, both services restored |

[CI](../.github/workflows/tests.yml) runs offline tests and lint on Windows/Linux.
The four Windows/Linux × Python 3.12/3.14 jobs passed for renamed commit `b487b32`.
Branch protection has not been verified; this is not yet an enforced merge gate.
Real-Hermes probe is local-only because CI does not vendor a host installation.

## v0.3 test map — offline coverage and separate narrow live evidence

These automated tests exercise real local HTTP/SQLite with fake delivery, or adapter host/connection doubles; none logs into QQ or proves production readiness. The complete offline run above passed, including the quiet-hours additions; no live QQ action is part of the automated suite. Rules derive from [background operations](cron.md), [permissions](permissions.md),
[storage](variables.md) and [architecture](architecture.md).

| Use case / rule | Expected success and deny case | Evidence | Status |
|---|---|---|---|
| Explicit opt-in and scoped secrets | Disabled starts no resources; enabled requires explicit port/clients; weak/duplicate tokens and invalid fields rejected; captured credentials do not follow environment mutation | [Publisher tests](../tests/test_publisher.py), [publisher](../src/hermes_onebot_fleet/publisher.py) | Existing new automated unit/loopback source; run result not recorded here |
| Authenticate every path | Missing/wrong/query token denied even on unknown routes and methods; other client's status/ID invisible | [Publisher tests](../tests/test_publisher.py), [permissions](permissions.md) | Existing new automated loopback source |
| Loopback/HTTP hardening | Non-loopback Host, any Origin, query strings, malformed/duplicate JSON fields, unknown fields and >16 KiB payload denied; text only 1–1800 characters | [Publisher tests](../tests/test_publisher.py), [publisher](../src/hermes_onebot_fleet/publisher.py) | Existing new automated loopback source |
| Immutable publication identity | Same client/ID/canonical target/text returns existing state across restart, no second callback; changed content/target is 409; clients have distinct ID spaces | [Publisher tests](../tests/test_publisher.py), [cron contract](cron.md) | Existing new automated SQLite/HTTP source |
| Conservative uncertainty | Lost ACK, exception/cancellation and restart sending become unknown; duplicate/restart never replay; missing/invalid message_id is not confirmed sent | [Publisher tests](../tests/test_publisher.py), [adapter tests](../tests/test_adapter.py) | Existing new automated source |
| Safe offline wait only | Proven NoRoute returns pending/refunds attempt; stable ID/payload survives safe reconnect; ordinary post-dispatch failure never tries standby | [Publisher tests](../tests/test_publisher.py), [adapter tests](../tests/test_adapter.py) | Existing new automated source |
| Rate/queue/TTL | Persisted rate cap includes unknown; queued work also gated; queue bounded per client; 24h payload expiry keeps ID/hash tombstones; changed target policy rejects queued work | [Publisher tests](../tests/test_publisher.py), [variables](variables.md) | Existing new automated source |
| Sole delivery path | Adapter reuses owned connections; unkeyed contextless cron fallback cannot send/mint an ID; native reply anchors preserved | [Adapter tests](../tests/test_adapter.py), [adapter](../src/hermes_onebot_fleet/adapter.py) | Existing new automated host-double source |
| Server quiet hours | Strict modern Asia/Shanghai policy; start/end and midnight boundaries; pending intake/query stay available; queued/restarted work cannot newly dispatch in quiet; other clients remain fair; rate/expiry preserved; cooperative callback boundary cancellation becomes unknown | [Publisher tests](../tests/test_publisher.py) | Included in 148-test offline run, not enabled/live-tested |

Run from this repository's independent development venv (not the Hermes managed environment):

```powershell
uv sync --extra dev --no-install-project
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -m pytest -q tests/test_publisher.py tests/test_adapter.py
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check src tests tools plugin
.\.venv\Scripts\python.exe -m ruff format --check src tests tools plugin
```

The existing [CI workflow](../.github/workflows/tests.yml) discovers offline tests, including new files,
on its normal matrix. Branch-protection enforcement is still not verified; do not call this a confirmed
merge gate. Guarded live checks remain manual and must not run from ordinary CI.

## Narrow local publisher acceptance — 2026-10-18/19

- Independent worker profile submitted one fixed cross-profile item and one native one-shot cron item. Both had `sent` plus distinct OneBot message IDs; the operator confirmed each appeared once.
- Cron completed with `deliver=local`, `failure-deliver=local`; its native delivery was suppressed. The sanitized worker held only a narrow publication capability, no QQ/OneBot secrets.
- Same-ID/same-payload POST after acceptance and after hot-reloading only the owning profile retained the original receipt. The outbound ledger gained only the two intended publications.
- Real HTTP checks rejected anonymous/wrong capability (401), browser Origin/unauthorized target (403), and modified immutable content (409).
- The operator also confirmed ordinary allowlisted owner-mention group chat still replied normally. No QQ guild/channel or group-management actions were used.
- Both-entry offline recovery was not completed: standby login had expired, quick login failed, and extra live fault tests were paused. Primary listener was restored. Actual ACK-loss/crash ambiguity is covered by offline tests only, not a live pass.
- Private deployment receipts remain outside this repository; no account IDs, tokens or message bodies are published here.
- Subsequent optional server-side quiet-hours additions are offline-tested only; they were not enabled or included in this live acceptance.

## Remaining guarded/manual tests

| Use case | Rule / expected negative | Evidence | Type / status |
|---|---|---|---|
| v0.3 live publication and offline recovery | One item gets one ACK-backed sent with message_id; same ID never resends; both accounts offline stays pending, reconnect before 24h may safely send; post-dispatch ACK loss becomes unknown without standby retry | [Background operations](cron.md), [architecture](architecture.md) | Partial narrow live pass for normal delivery/repeated ID; offline recovery paused, real ACK loss untested |
| Worker isolation and sole delivery | Worker has no QQ tokens; independent project/venv/profile and explicit environment allowlist; deliver=local and failure-deliver=local cause no native cron/failure fallback; publish aliases stay scoped | [Permissions](permissions.md), [variables](variables.md) | Configuration/child-env audit + one real-host cron passed; not an OS sandbox proof |
| Operator reconciliation | Distinguish accepted/pending from sent with message_id; unknown never auto-retried/new-ID resent; manually verify QQ before deliberate new work | [Background operations](cron.md) | Guarded live/manual / proposed |
| Native owner controls | Unauthorized approvals denied; owner @ text clarification, commands and interrupt reach native flow | [Permissions](permissions.md), [flows](flows.md) | Guarded live / proposed |
| Profile isolation | Distinct secret scope, ledger, namespace and user session prevent cross-profile access | [Variables](variables.md), [adapter](../src/hermes_onebot_fleet/adapter.py) | Real-host integration / proposed |
| Managed PM enablement | PM resolves staged dependency without core mutation | [CLI](../src/hermes_onebot_fleet/cli.py) | Real-host integration / proposed |
| Sensitive storage | Operator confirms filesystem ACLs, retention policy and backup permissions | [Variables](variables.md) | Manual review / proposed |

## Gaps / not a green shipping verdict

1. **High: OneBot side effects and SQLite commits are not atomic; v0.3 is not exactly-once.**
   Publisher unknown is conservative and never automatically retried; it may leave an unsent item
   unsent or lose confirmation of an accepted send. Only sent + message_id confirms OneBot ACK,
   never human receipt. Normal publisher/repeated-ID checks passed locally; crash/ACK-loss reconciliation remains untested live.
   Unkeyed contextless native sends now fail closed, but local tests do not establish host-global
   recovery fencing or at-most-once execution. Stable host obligation identity is still absent.
2. Native resume may re-run model/tools outside plugin control; plugin ledger and host task store
   are not one transaction. Host task execution/recovery is deliberately not reimplemented here.
3. Live account risk controls, mute permissions, actual NapCat versions, notify/cron and group
   approval UX remain unverified. A successful mock transport test is not QQ validation.
4. FIFO task-context inheritance is not a universal durable identity. Native final reply anchors
   remain; authenticated durable publication IDs are required for background sends. Contextless
   proactive/old cron paths are denied, not silently routed or declared sent. Host-wide execution
   recovery remains outside the publication identity domain.
5. No recovery for pre-auth/overflow events, same-second identical-text collisions, cross-machine
   leader election, automatic retention, OS sandbox setup, or private messages to an offline bot.
6. Local isolated wheel builds were attempted but failed in the build backend with
   `FileNotFoundError: [Errno 2] No usable temporary directory found`. Direct development-venv
   temporary-file creation and source tests work; isolated build-worker environment remains
   unresolved. No wheel/release is published; source staging is the tested local installation path.
