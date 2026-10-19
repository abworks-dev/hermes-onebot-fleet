# Background work and v0.3 publication operations

v0.3 publisher is experimental. Narrow local acceptance verified one cross-profile and one native-cron publication, original-ID duplicate submission and owning-profile hot reload without another send; the operator confirmed both messages once and ordinary group replies still working. Both-entry offline recovery and real ACK loss remain untested, and extra live fault tests are paused. No QQ guild/channel actions were tested. No production-readiness claim is made.

| Work | Cadence/entry | Credentials | Limits/retry | Visibility |
|---|---|---|---|---|
| Account socket | One task per account from adapter.connect | Bound profile OneBot token | Heartbeat 20s; reconnect exponential delay capped at transport.BACKOFF_MAX; 256 KiB frame limit | Healthy state used for selection; no message payload logging |
| Identity/online probe | Every 5s, plus immediately after socket connect | Same socket/token | Two read-only actions with 5s timeouts | Unhealthy accounts excluded |
| Inbound drain | 100ms idle poll | None | SQLite unique key; never re-handoff handed_off/uncertain entries | Fleet ledger states and uncertain IDs via CLI |
| Event dispatch | Socket events | None | Bounded 256-entry memory queue; overload not durably recovered | See [reliability gaps](architecture.md) |
| Publication HTTP intake | Opt-in from adapter.connect; loopback port explicitly configured | Dedicated per-client Bearer publish token | 16 KiB JSON body, text 1–1800 characters; immutable client + publication ID | Authenticated client-scoped GET endpoints |
| Publication drain | Wake on enqueue; timed 1s poll | Adapter callback reuses its owned OneBot connections | Client max_per_hour (example 6; configurable 1–60); at most 1000 pending/sending per client; 24h payload TTL | Independent publications.sqlite3; pending/sending/sent/unknown/rejected/expired |
| Worker cron | Operator-defined schedule in independent worker profile | Only narrow publish capability, no QQ credentials | MUST deliver=local and failure-deliver=local; bridge is sole outward delivery | Worker-local run log plus authenticated publication status |

These are in-process asyncio tasks, not a standalone QQ sender. Adapter shutdown stops HTTP intake
and cancels/awaits the publication worker before closing its SQLite connection, transports and Fleet
ledger lock. The worker's schedule remains Hermes-owned; it must not use Hermes' old native cron QQ
send path. v0.3 rejects unkeyed contextless proactive Adapter.send calls; native chat reply anchors remain.
No fallback to native delivery, a separate OneBot connection, or a standby send after dispatch failure.

## Enable only in the owning Fleet profile

Under gateway.platforms.onebot_fleet.extra, merge:

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

The number is fictitious. The canonical target must also pass the Fleet namespace and destination
allowlist. A client cannot add arbitrary QQ IDs. Disabled is the default; enabled requires an explicit
port and clients. Use a distinct >=32-character non-whitespace random token per client, supplied through
the owning profile's scoped secrets. Do not put token values in config, prompts, source or logs.

The server binds only 127.0.0.1. **Every path**, including status, missing routes and wrong methods,
requires exactly one valid `Authorization: Bearer <publish-token>` header. Tokens in URLs are not
accepted. Query strings and Origin headers are rejected; there is no CORS/browser integration. Host
must match the configured loopback host/port. Client identity comes from the token, not JSON/query input.

| Endpoint | Request | Meaning |
|---|---|---|
| POST /v1/publications | JSON exactly publication_id, target, text | target is an authorized alias, not a canonical QQ destination; 202 means accepted/existing state, NOT delivery ACK |
| GET /v1/publications/{id} | Same client's Bearer token | Own publication state, message_id/account/error; another client's ID is not visible |
| GET /v1/status | Same client's Bearer token | Own counts by state; no message text or token |

Example POST body (no secret value):

```json
{"publication_id":"radar-20261018-01","target":"ai-test","text":"Example digest"}
```

Persist a deterministic publication_id for the logical item **before** submitting. Re-submit only the
same ID/payload after a lost HTTP response. Within a client, the ID is durable and immutable: duplicate
canonical target + text returns the existing state without resending; changed text or canonical target
returns 409. Aliases resolving to the same canonical target are equivalent. Different clients have separate
ID spaces. Do not mint a new ID merely because the prior outcome is unknown. Rate/queue admission can
return 429; it is not an instruction to bypass limits with another client or delivery path.

## Optional server-side quiet hours

Omit `quiet_hours` to disable it. When present, all three fields shown above are required; strict `HH:MM`, different start/end, and only modern `Asia/Shanghai` (fixed UTC+08; no historical DST/tzdata dependency) are supported. The daily interval includes start, excludes end, and may wrap midnight.

Valid intake during quiet remains accepted/pending under the same authorization, queue/rate and 24h TTL rules. Queries and expiry continue. The worker gates both previously pending and new work before claiming or invoking delivery, without starving other clients. Worker-local quiet settings must match this policy: merely stopping new POSTs does not stop already accepted server work. There is no per-request TTL, cancellation or not-before API.

A callback started just before quiet is cooperatively time-bounded to the boundary; cancellation becomes unknown and is never replayed. An RPC already dispatched cannot be recalled, and remote arrival/display timing is not guaranteed. This policy prevents new local dispatch during quiet, not all remote visible effects. This extension has offline boundary/restart/fairness tests only; it was not enabled in the narrow live acceptance above.

## State, restart and reconciliation

- publications.sqlite3 is plugin-owned, beside the Fleet ledger, **not Hermes' database**. It records
  client/ID, payload hash, target/text, state, attempt history and ACK metadata. Do not modify Hermes DB
  to inject jobs or delivery outcomes.
- New work is pending. The worker durably records sending before invoking the adapter callback. The
  callback selects a verified healthy account, checks target policy/membership, and uses the adapter's
  existing OneBot socket; there is no standalone sender or second login.
- Only proven NoRoute, meaning **no mutating RPC was attempted**, may return sending to pending with
  backoff (and refund that rate attempt). Read-only route selection may choose a standby before send.
  Pending offline work may resume within its TTL; ordinary failures after dispatch never trigger rerouting.
- Timeout, cancellation, lost ACK, callback uncertainty and startup recovery of sending become unknown.
  Unknown is terminal for automatic dispatch: neither restart nor duplicate POST retries it. Rejected is
  also not an automatic-retry instruction. Inspect error/status and reconcile with QQ manually.
- **Only sent with message_id confirms a OneBot ACK.** It does not prove human receipt/read status.
  OneBot send and SQLite are not atomic; a crash can leave unknown even before the RPC occurred, or after
  QQ accepted it. Conservative handling may leave an unsent item unsent or lose local confirmation of a
  delivered item. There is no exactly-once guarantee. Manual reconciliation is required before any
  deliberate new publication; do not silently replace the ID and resend.
- Pending older than 24h becomes expired. Payload text/canonical target is cleared after 24h; hash/ID
  tombstones remain to prevent old-ID reuse. Expiry does not make an unknown item safe to retry, erase
  all private metadata, or shrink the ledger indefinitely. Retention/backup remain operator duties.

## Worker credential boundary

Use an independent project, venv and worker profile. Give it only the client publish token for the narrow
alias plus explicitly chosen model/network credentials. Build a child-process environment from an explicit
allowlist; exclude OneBot tokens, the owning Fleet profile's secrets and unrelated credentials. Do not
inherit the full parent environment and then merely unset a few known keys. Keep token values out of
cron prompts and logs. Configure both `deliver=local` and `failure-deliver=local`; normal output and error
output stay local unless explicitly submitted through this publisher. Disabling the client/publisher and
restarting the owning adapter is the delivery kill switch; pending work is rechecked against current policy.

Separate directories, profiles and virtualenvs under the **same Windows user are not an OS sandbox**.
A malicious same-user worker can read accessible files, inspect processes or obtain credentials; capability
scoping limits cooperative application behavior, not that adversary. Use separate OS identities/ACLs or a
proper sandbox if hostile code isolation is required. Official repository/source trust is unchanged.
