# Variables and storage

| Name | Consumer/source | Scope | Rotation/risk |
|---|---|---|---|
| accounts[].token_env | Name of a per-profile secret read via Hermes get_scoped_secret | Owning profile, never a config token value | Change matching NapCat WS token and profile secret; restart adapter; leaked token permits local bot control |
| accounts[].id/ws_url/priority | Profile platform extra | Non-secret; QQ identifiers still personal data | Lower priority number wins; URL restricted to loopback |
| allowed_users/allowed_groups | Profile platform extra | Non-secret authorization policy | Empty denies all; never grant the group as a substitute for owner restriction |
| logical_id | Profile platform extra | Durable session namespace | Keep stable during account failover; changing starts a new conversation namespace |
| state_path | Profile platform extra | Relative to owning profile/plugin-data/qq-failover, or explicit absolute path | Ledger contains text/QQ IDs; do not share directories across profiles; protect backups |
| require_mention | Profile platform extra | Must be true in v0.1 | False is rejected |
| QQ_FAILOVER_ALLOWED_USERS | Owning profile .env | Second native gateway admission list, not a secret | Match allowed_users; mismatch may deny legitimate owner |
| QQ_FAILOVER_HOME_CHANNEL | Owning profile .env | Optional native notification target | Destination ACL still applies; only live-gateway sending supported |

No browser/client bundle exists, no token query parameters are accepted, and no secrets are
published in source examples. The plugin captures credentials from the bound profile when
constructed; do not rely on process-global environment changes for profile selection or rotation.
Ledger records contain messages and replies but not bearer tokens. Operator status excludes text.
There is no automatic pruning; stop the adapter and archive/prune the ledger under a reviewed
retention policy. Removing dedup records re-enables old-event processing if those events recur.

Before go-live: generate distinct random tokens (>=16 non-whitespace characters), loopback-bind
NapCat, confirm both logged-in identities, replace ALL example identifiers, protect profile .env
and ledger permissions, explicitly select toolsets, and keep a tested configuration backup.
