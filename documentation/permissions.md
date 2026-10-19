# Permissions

| Actor/resource | Operation | Enforced rule | Deny outcome |
|---|---|---|---|
| NapCat account | Connect | Loopback URL, per-account Bearer token, get_login_info matches configured QQ, online=true | Unhealthy; no accepted inbound |
| Human private sender | Dispatch | Actual sender in allowed_users and not a configured bot | Dropped before ledger/model |
| Human group sender | Dispatch | Actual sender in allowed_users AND group in allowed_groups AND explicit structured @configured bot | Dropped, including @all/textual CQ/reply-only |
| Configured bot | Trigger Agent | Never, even if it is in a user whitelist | Dropped to prevent bot loops |
| Hermes Agent | Anchored native chat reply | Original handing_off/handed_off inbound, matching canonical target, current source admission revision; destination ACL and chosen account admission | SendResult failure, not retried |
| Unkeyed/contextless proactive caller | Native Adapter.send, including old cron path | Durable publisher identity required for background sends | Fail closed; no native cron/standalone fallback |
| Publisher client | Any HTTP path or method | Exactly one strict Bearer header; dedicated token selects configured client, including for unknown paths | 401 without valid credentials |
| Publisher client | Submit publication | Authorized target alias, canonical target within current Fleet destination policy; exactly publication_id/target/text, text 1–1800 characters | 400 invalid shape/field, 403 unauthorized alias, no dispatch |
| Publisher client | Reuse client-scoped durable ID | Immutable canonical target and text; duplicates return existing state without resend | 409 if content/target changes |
| Publisher client | Read publication/status | Only token-selected client's records/counts, no arbitrary client selector | Other client's record is 404; no text/token response |
| Publisher HTTP caller | Browser/non-loopback-oriented request | Loopback bind and expected Host; no Origin, query parameters or CORS | Rejected even with valid token |
| Publisher dispatch | Send queued item | Current client/target policy rechecked; per-client rate cap, bounded queue and 24h TTL | Rejected/expired or remains pending; no arbitrary OneBot API |
| Worker cron | Deliver output/error | deliver=local and failure-deliver=local; only narrow publish capability may deliver externally | No native delivery fallback; failed/unknown bridge outcomes require review |
| Local operator | Configure/read ledgers | OS filesystem permissions; HTTP publisher has no admin/configuration API | No application role system |
| Local operator | Change account admission/primary | Existing sidecar; explicit account/reason/confirmation; quarantine only cleared by allow; promote requires allowed | Reject missing/unknown state or implicit release; audit mutation |
| Configured physical account | Ingress/handoff/send | Persistent allowed state AND current revision at each relevant gate; health alone is not admission | No new handoff/send; old revoked queue becomes denied |

Hermes' normal native gateway admission remains active via ONEBOT_FLEET_ALLOWED_USERS.
Pairing, allow-all, global grants do not bypass the plugin's earlier local owner/group check.
No trusted_inbound, role_authorized, arbitrary tools, remote configuration, or MCP server.
Publisher authorization does not grant worker ingress rights, QQ login/control, unrestricted destinations
or arbitrary RPC access. Target aliases are scoped by token-derived client, not trusted caller input.
The adapter callback alone owns side effects through its existing OneBot sockets. No retry/reroute after
mutating dispatch; only proven NoRoute before any mutating RPC may safely return a publication to pending.

Account management is local filesystem-authorized, not an authenticated multi-user admin service.
Pause does not revoke an RPC already past final admission or stop an already-started Hermes task;
old replies are nevertheless gated. Preserve account state/revisions across restart and rollback.
See [P0 lifecycle and limitations](account-lifecycle.md); no live P0 deployment/test occurred.

No row-level SQL security: Fleet, account-management and publication SQLite files rely on code-enforced client checks
and local OS permissions. The publication database is separate from Hermes DB; never grant workers
Hermes database-write access as a substitute for the HTTP capability. IDs/hash tombstones are durable;
unknown is not an authorization to replay or change IDs. Only sent plus message_id confirms OneBot ACK,
not human receipt. Neither publication nor native paths promise exactly-once semantics.

Use a separate worker project/venv/profile with **no QQ/OneBot tokens** and an explicit environment
allowlist. Separate directories, virtualenvs and profiles under the same Windows user are **not an OS
sandbox** and do not protect against a malicious same-user worker reading files/environment/credentials.
Use a separate OS identity/ACLs or an actual sandbox for that threat model. The plugin also does not
sandbox the Hermes Agent's filesystem; select OS isolation and toolsets separately.

Disabling group_sessions_per_user can intentionally share a group transcript between admitted users;
use true for user separation. Group source.user_id always remains the authenticated human QQ number.
See [worker operations](cron.md) and [secret storage/rotation](variables.md). Narrow v0.3 local cross-profile/cron acceptance confirmed capability-only worker execution, original-ID reconciliation and rejection boundaries; this is not hostile-code OS isolation or a production verdict. Extra live fault tests are paused. The deployment scope is ordinary allowlisted group/private text only: no QQ guild/channel, adding friends, inviting accounts or group-management operations. The plugin exposes no arbitrary OneBot-action tool.
