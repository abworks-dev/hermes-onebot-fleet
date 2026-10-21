# Architecture and reliability boundaries

```
NapCat A -- authenticated OneBot11 WS --\
                                       native adapter -> Hermes gateway -> Agent
NapCat B -- authenticated OneBot11 WS --/

Worker (no QQ tokens) -- Bearer + target alias --> adapter-owned loopback publisher
                                                  -> publications.sqlite3
                                                  -> owned adapter/OneBot sockets
```

No NoneBot, API proxy, synthetic QQ identity, or Hermes core patch is used.
One adapter instance owns all physical accounts for one profile/logical namespace.

## Inbound

1. Connect only to configured loopback WebSocket endpoints; each account has its own token variable.
2. Verify logged-in account and online status with read-only actions before trusting events.
3. Verify event self_id, sender, configured user AND group whitelist and structured @ segment.
   All bot senders are excluded. Private chat requires the user whitelist. Group @ may target
   either configured bot, allowing a standby observer to handle @primary after primary failure.
4. Normalize text-only input and generate a cross-account semantic fingerprint. Reject rich attachments.
5. Persist the fingerprint and payload in a plugin-owned SQLite WAL ledger before handing off.
6. Canonical source chat_id is `obf:<logical_id>:<group|private>:<id>`; source.user_id stays the human QQ.
   Physical account identity is NOT in the session key. scope_id alone is not relied on for isolation.
7. Native BasePlatformAdapter handles task lifecycle, conversation queueing, clarifications and commands.
   metadata stays empty to satisfy native ingress. No custom approval/execution state is implemented.

`queued -> handing_off -> handed_off` describes adapter-to-Hermes handoff only, NOT
model completion. Exception/cancellation/crash during handoff becomes `uncertain`.
Only queued items are dispatched after restart. No handed-off/uncertain item is automatically replayed.
There is no atomic transaction spanning this ledger and Hermes' own runtime store.
The host must retain its own durable execution state. The plugin does not promise exactly-once execution.

OneBot message IDs may be account-local. Fingerprints use logical namespace, chat type/ID,
sender, timestamp (seconds), normalized text. Two identical legitimate messages from the
same sender/chat/second collapse intentionally. Deployments requiring lossless message identity
need a verified cross-account QQ sequence mapping; it is not fabricated here.

## Outbound

Canonical namespace + destination whitelist checked independently of inbound admission.
v0.4 additionally requires persistent business admission and captured revision checks. Initial
selection uses numeric priority/account ID; thereafter the current eligible primary is sticky,
persisted across restart. Losing admission/health permits failover, but recovery does not preempt
a still-eligible primary; manual promotion is separate from admission. Group sends preflight
actual membership using get_group_member_info. Read-only target-only preflight failure
can try another account; a send itself is NEVER replayed automatically on a different account.
If all accounts are offline, native send returns failure, not a false delivery acknowledgment.
The local monitor ticks about every 5 seconds; actual read-only health RPCs are single-flight,
due every 60 seconds normally, 180 seconds while idle/held, with failure backoff capped at 600.
Initial socket identity verification remains fresh and old-socket results cannot verify a replacement.
Group preflight consumes independent durable shared permits, never cached positive membership;
a non-reserving send-budget veto runs before it. Reconnect uses per-account jittered exponential
backoff and only resets after 120 seconds of continuously verified health, not a short successful flap.
These source changes are offline-tested, not a live profile upgrade or QQ-side request measurement.
A connected account is not necessarily able to speak in every group (mute/risk controls may differ).

Before dispatch, `sending` record includes chosen account and complete text. A confirmed
message ID transitions to `sent`; explicit server rejection to `failed`; timeout, disconnect,
cancellation or crash to `uncertain`. Unknown outcomes require human verification against QQ.
The record preserves the reply so operators do not need to re-run the model to inspect it.
It is not an autonomous retry outbox. The adapter marks its failures final to native
_send_with_retry, preventing the immediate plain-text fallback from changing/re-sending text.
v0.3 denies unkeyed contextless proactive sends, including the old native cron path; normal
native chat reply anchors remain supported. This closes that unkeyed background route, not all
Hermes execution/recovery semantics. Hermes' separate delivery ledger/interrupted-turn recovery
is still host-owned; the plugin does not provide a stable global host obligation identity or
remote QQ idempotency/fencing key. Strict no-duplicate behavior across all host restart/redelivery
is NOT promised. Do not remove host recovery or fabricate acknowledgments to conceal uncertainty.

An OS-held exclusive lock protects each ledger from multiple plugin processes. There is no
cross-machine leader election. One profile process owns coordination, all transports stay connected;
only one logical ingress fingerprint reaches Hermes. Final native replies use reply_to (the
canonical event ID) for correlation; task-context inheritance is not a durable publication identity.
Background publishers require an authenticated durable publication identity; arbitrary unkeyed
contextless sends fail closed.
Recovered queued inputs recheck the current logical namespace, users, groups, accounts and
mention target and the captured account admission revision; revoked work becomes denied.
v0.4 native reply_to must also resolve to an admitted original inbound row and canonical target.
Re-admission does not revive old revision-bound work. Selection and final pre-dispatch gates
check both source reply admission and chosen account eligibility; no gate recalls an in-flight RPC.

## v0.5 resilience — offline and guarded local deployment

Read-only observations separate transport, identity, online, backend-good and target evidence.
Persisted account/target circuits are distinct vetoes from P0 admission; successful health checks,
re-admission or promotion never clear them. Target circuits restrict outbound for that target, not
new authorized inbound; no mutating send probes or automatic half-open/reset are added.

All accounts, native replies and publisher callbacks share one persisted budget. Pre-dispatch
reservation is never refunded; completion and explicit exclusive-owner restart recovery extend
cooldown fences, preventing stale reservation timestamps from collapsing actual spacing. Original
work age is checked before handoff and dispatch, including after blocking/awaited preflight. Only
proven never-dispatched publisher work may defer the same ID; ambiguous sends never replay.

Health incidents and a standalone local JSONL/cursor consumer do not depend on QQ delivery.
A separately authorized test-profile upgrade started an independent local watcher without QQ
credentials; it is not an installed OS service, external notification destination or host HA.
See [deployment evidence](tests.md#guarded-v05-deployment-and-local-watcher--2026-10-19) and
[resilience policy and commands](resilience.md) for privacy, persistence, manual reset and limits.
Real v0.5 message/fault tests and external notification delivery remain unverified.

## v0.4 P0 business admission — scoped acceptance

[Account lifecycle](account-lifecycle.md) documents persistent allowed/paused/quarantined/observing
state, manual CLI, audit, sticky-primary semantics, upgrade/rollback and concurrency boundaries.
The sidecar is `<ledger filename>.accounts.sqlite3`; no Hermes internal schema is altered.
Transport observations and account administration are separate: online/reconnect/timers never
clear quarantine; pause does not stop NapCat's own recovery or terminate already-started host work.
Initial configured accounts start allowed; unknown IDs fail closed. Old queued/reply work without
a captured revision is denied. Do not discard state to release quarantine or reissue publications.
Source tests, a separately authorized held-profile deployment/reload and three real group replies
passed; see [local deployment](tests.md#guarded-local-p0-deployment--2026-10-19) and
[business-switch acceptance](tests.md#guarded-p0-group-messages--2026-10-19). Replies followed
initial primary → standby → same standby after initial-primary re-admission. Switching used local
business pause only, not QQ/network outage or account restrictions. The operator subsequently
requested explicit promotion back to the initial primary; that was not automatic failback.
No rotation, forced login, automatic risk detection or unquarantine was added. Older live records
below remain version-specific and do not validate v0.4 behavior.

## v0.3 adapter-owned loopback publisher

Opt-in [publisher](<../src/hermes_onebot_fleet/publisher.py>) starts/stops with the adapter, binds 127.0.0.1 on an explicitly configured
port (example 18791), and injects a callback into the existing adapter transport owner. It never
opens independent OneBot connections and never falls back to native cron delivery. The adapter
keeps using verified healthy accounts and the current destination policy. Pre-send read-only
selection may choose standby; failure after mutating dispatch never reroutes to standby.

All HTTP paths require a strict Bearer publish token. Token identity selects a client whose configured
target aliases are a subset of Fleet's canonical destination allowlist. POST /v1/publications accepts
only publication_id, target alias and text (1–1800 characters); GET /v1/publications/{id} and
GET /v1/status expose only that client's state without text/secrets. Unknown routes are authenticated
too; Host is loopback-checked, Origin/query strings are rejected, and there is no CORS integration.

The independent publications.sqlite3 is a plugin outbox, not Hermes DB. Identity is (client, durable
immutable publication_id); duplicates with the same canonical target/text return state without
resending, while changed target/text returns 409. Pending work is bounded, rate-capped and expires
after 24h. State is pending/sending/sent/unknown/rejected/expired. Startup changes sending to unknown
without automatic replay. Only explicit NoRoute proving **no mutating RPC** permits sending -> pending.
Timeout/disconnect/cancellation/ambiguous callback results are unknown and require manual reconciliation.
Only sent **with message_id** means confirmed OneBot ACK, never human receipt/read confirmation.

OneBot sends cannot commit atomically with SQLite. Conservatively refusing unknown retries may leave
an unsent publication unsent, or lose local confirmation of a delivered one; this is not exactly-once.
24h payload removal preserves ID/hash tombstones and does not permit ID reuse or unknown replay.

Optional per-client `quiet_hours` gates server dispatch of both old pending and newly accepted work. Intake/query/expiry still run; absent policy keeps the old behavior. Only modern Asia/Shanghai UTC+08 is supported. A callback begun just before quiet is cooperatively bounded to the boundary; a mutating RPC already dispatched cannot be recalled. Local worker quiet must match server policy; no per-item TTL/cancel API exists. Quiet-hours tests are offline-only and not part of the live evidence below.

Workers run in independent projects/venvs and token-free-QQ profiles with only narrow publish
capabilities and explicit environment allowlists. Their cron MUST use deliver=local and
failure-deliver=local so publisher is the sole outward delivery. Same-Windows-user isolation is not
an OS sandbox and does not defend against a malicious same-user process reading files/credentials.
See [background operations](cron.md), [permissions](permissions.md) and [variables](variables.md).
**Narrow local v0.3 acceptance (2026-10-18/19):** an independent worker profile with no QQ/OneBot credentials submitted one cross-profile publication; its native one-shot cron (`deliver=local`, `failure-deliver=local`) submitted a second. Both returned distinct `sent` receipts with message IDs. Repeating the original POST and hot-reloading the owning profile preserved those receipts without another send. The operator confirmed each test message appeared once and ordinary owner-mention group chat still replied normally. Missing/wrong capabilities, forbidden targets, browser Origin and changed immutable payload were rejected. Both-entry offline recovery was **not completed**: the standby had logged out, cached login failed, and extra live fault tests were paused. Live ACK-loss/crash uncertainty remains untested. No QQ guild/channel or group-management features were exercised.

## Lifecycle and unavailable features

Hermes restarts also stop the plugin; messages that NapCat does not replay during that gap
cannot be recovered by this in-process design. Initial pre-authentication events and overflow
past the bounded 256-event transport queue can be dropped. Do not present this as a general
message broker or total machine-failure failover. Running the same profile on two machines is unsupported.

v0.4.0 remains text-only; it does not translate quoted message IDs across accounts, fetch image URLs,
download files, expose arbitrary OneBot actions, or register agent tools. reply_to is not used to
create native QQ quoted replies. Rich message/voice/file events are rejected, not summarized from metadata.
No media, button, edit/streaming, reaction, account login automation, or standalone cron sender.
Native anchored chat replies remain available while gateway is running. Background delivery requires
the opt-in live adapter publisher; old unkeyed native cron sending and standalone QQ delivery are denied.
Clarify and approval rely on Hermes text fallbacks; real QQ interaction must be tested before reliance.

No email or public/indexable HTTP routes: no emails.md or seo.md. Periodic transport tasks and
agent handoff are documented below. This package uses Python 3.12–3.14, aiohttp and SQLite;
Hermes provides native sessions, model execution, approvals and profile-bound credentials.

## Related documents

- [Permissions](permissions.md), [protected flows](flows.md), [variables and storage](variables.md).
- [Background work](cron.md), [automation surface](automation.md), [verification map](tests.md).
- [Installation and usage](../README.md), [configuration example](../examples/config.yaml).

## Known risks linked to implementation

- [Adapter send correlation](../src/hermes_onebot_fleet/adapter.py) blocks unkeyed contextless sends but does not promise host-global recovery dedup without stable host obligation IDs.
- [Publisher](../src/hermes_onebot_fleet/publisher.py) cannot atomically commit OneBot send with SQLite; unknown is conservative and requires reconciliation. Independent worker profiles/venvs under the same Windows user are not a hostile-code sandbox.
- [Transport queue](../src/hermes_onebot_fleet/transport.py) is memory-only before ledger enqueue; overflow/pre-auth events can be lost.
- [Normalizer](../src/hermes_onebot_fleet/protocol.py) intentionally collapses identical same-sender/same-chat/same-second text.
- [Ledger](../src/hermes_onebot_fleet/store.py) is not atomic with native Hermes execution, retains private content and needs an operator retention policy.
- Private messages addressed to an offline bot are not delivered to another QQ account: only group observations offer cross-account inbound redundancy.
- Historical v0.2-series evidence only (not v0.3 publisher acceptance): pre-rename local two-account NapCat testing on 2026-10-18 covered primary reply, standby handling @primary while the primary OneBot service was disabled, and return to primary after restoration: three real messages, exactly 3 handed_off / 3 sent. The renamed v0.2.0 repeated this three-message live test on the same date: primary/standby/primary each sent one reply, exactly 3 handed_off / 3 sent with no uncertain rows; both services were restored. The owner confirmed a single visible reply at each step. This tests a disabled primary OneBot listener, not a killed QQ process or host failure; approval, notification and Hermes crash-recovery behavior remain unverified live.
