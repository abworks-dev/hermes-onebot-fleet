# Variables and storage

| Name | Consumer/source | Scope | Rotation/risk |
|---|---|---|---|
| accounts[].token_env | Name of a per-profile secret read via Hermes get_scoped_secret | Owning profile, never a config token value | Change matching NapCat WS token and profile secret; restart adapter; leaked token permits local bot control |
| accounts[].id/ws_url/priority | Profile platform extra | Non-secret; QQ identifiers still personal data | Initial/failover order uses lower priority number; eligible current primary stays sticky; URL restricted to loopback |
| allowed_users/allowed_groups | Profile platform extra | Non-secret authorization policy | Empty denies all; never grant the group as a substitute for owner restriction |
| logical_id | Profile platform extra | Durable session namespace | Keep stable during account failover; changing starts a new conversation namespace |
| state_path | Profile platform extra | Relative to owning profile/plugin-data/onebot-fleet, or explicit absolute path | Fleet ledger contains text/QQ IDs; do not share directories across profiles; protect backups |
| require_mention | Profile platform extra | Must be true in v0.4.0 | False is rejected |
| publisher.enabled | Profile platform extra | Adapter-owned, default false | Opt-in only; false starts no publication listener/worker |
| publisher.port | Profile platform extra | Explicit loopback port when enabled; example 18791 | Bind only 127.0.0.1; choose a free port per owning profile; no public bind option |
| publisher.clients[].id | Profile platform extra | Durable capability/ID namespace | Do not rename or recreate casually: identity is client + publication_id |
| publisher.clients[].token_env | Name of profile-scoped dedicated publish secret | Adapter and authorized worker only; separate from OneBot token | Distinct random >=32-character non-whitespace token per client; update both sides and restart adapter; leak permits only configured publications but still requires revocation |
| ONEBOT_FLEET_RADAR_PUBLISH_TOKEN | Example secret name referenced by token_env | Owning profile and narrowly authorized worker; never a literal config token | Never include value in prompts/logs, source, URL, examples or full inherited environment |
| publisher.clients[].targets | Profile platform extra, alias -> canonical target | Non-secret authorization policy; IDs remain private data | Example ai-test -> obf:onebot-fleet:group:30001; must be subset of Fleet allowlist; queued work rechecked after restart/config change |
| publisher.clients[].max_per_hour | Profile platform extra | Per-client attempt cap; default/example 6, valid 1–60 | Dispatch rate persisted across restart; only proven NoRoute refunds attempt |
| ONEBOT_FLEET_ALLOWED_USERS | Owning profile .env | Second native gateway admission list, not a secret | Match allowed_users; mismatch may deny legitimate owner |
| ONEBOT_FLEET_HOME_CHANNEL | Owning profile .env | Optional native notification target | Not a publisher/cron bypass: destination ACL and v0.3 context/durable-key guard still apply |
| Worker environment allowlist | Explicit child-process launch configuration | Independent worker project/venv/profile | Allow only necessary model/runtime variables and narrow publish token; exclude all QQ tokens and unrelated parent secrets |

All publisher settings are nested under gateway.platforms.onebot_fleet.extra.publisher.
The [operations guide](cron.md) contains the opt-in example and HTTP contract. Source examples use
fictitious identifiers; replace them privately, never publish runtime paths, real QQ IDs or secret values.
Official repository/source selection is unchanged by this feature.

No browser/client bundle exists and no secret is shipped client-side. Token query parameters are not
accepted. The plugin captures credentials from its bound profile when constructed; do not rely on
process-global environment changes for profile selection or rotation. Publishing uses strict Bearer
headers on all paths; do not enable HTTP debug/access logs that capture tokens or message text.

## Durable state and retention

- The Fleet state.sqlite3 (or configured state_path) retains incoming messages/replies. Operator status
  excludes text; protect the file and backups as private data. There is no general automatic Fleet
  ledger pruning. Stop the adapter and use a reviewed archive/prune policy; deleting dedup records can
  re-enable old-event processing.
- v0.4 `<ledger filename>.accounts.sqlite3` stores account admission/revisions, sticky primary and
  operator audit. It is independent of health, never contains credentials or automatically releases
  quarantine. Reasons are manual labels: do not paste secrets/message content into them. Preserve
  this sidecar with ledger backups; deletion is not an administration or rollback procedure. The
  local accounts CLI cannot initialize or recreate missing state. See [lifecycle](account-lifecycle.md).
- The separate publications.sqlite3 is beside the configured Fleet ledger, owned by the plugin,
  **not part of Hermes DB**. It stores client/ID, payload hash, alias/target/text, state, timestamps,
  rate attempts and ACK metadata, not Bearer tokens. Do not share a writable state directory between
  profiles or expose it to workers as an alternate submission API.
- Publication payload TTL is 24h: old pending becomes expired and old text/canonical target is cleared.
  Durable ID/hash tombstones remain, so expiry does not allow changed-payload reuse or automatic
  unknown resend. Some metadata persists indefinitely; this is not a full privacy/backup-erasure policy.
- Publication sending interrupted by restart becomes unknown, never auto-retried. Only sent plus
  message_id confirms OneBot ACK, not a person's receipt. OneBot send and SQLite are non-atomic;
  manually reconcile conservative unknown before considering deliberate new work.

## Worker and pre-go-live checklist

Use an independent worker project, venv and profile with no QQ/OneBot secrets. Give it only its narrow
publish token and necessary explicitly selected credentials. Construct an environment from an explicit
allowlist instead of inheriting the owning profile's full environment. Set cron deliver=local and
failure-deliver=local; publisher is the only external delivery capability, with no native cron fallback.
Separate profiles/venvs/directories under the **same Windows user are not an OS sandbox** and do not
protect against malicious same-user code reading accessible files or credentials. Use OS isolation for
that threat model.

Before go-live: generate distinct random OneBot tokens (>=16 non-whitespace characters) and publish
tokens (>=32), loopback-bind NapCat and publisher, confirm both logged-in identities, replace ALL example
identifiers, review aliases and rate caps, protect profile .env and all message/admission/publication SQLite files and backups, explicitly
select toolsets, test local/failure-local cron settings, and keep a tested configuration backup.
**v0.3 narrow local cross-profile/cron and stable-ID/hot-reload checks passed; both-entry offline recovery and real ACK loss remain untested.** The worker credential audit found only its scoped publish capability, with no QQ/OneBot secret names in its child environment; this does not prove OS isolation from malicious same-user code.
