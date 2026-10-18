# Background work

| Work | Cadence/entry | Credentials | Limits/retry | Visibility |
|---|---|---|---|---|
| Account socket | One task per account from adapter.connect | Bound profile token | Heartbeat 20s; reconnect exponential delay capped at transport.BACKOFF_MAX; 256 KiB frame limit | Healthy state used for selection; no message payload logging |
| Identity/online probe | Every 5s, plus immediately after socket connect | Same socket/token | Two read-only actions with 5s timeouts | Unhealthy accounts excluded |
| Inbound drain | 100ms idle poll | None | SQLite unique key; never re-handoff handed_off/uncertain entries | Ledger states and uncertain IDs via CLI |
| Event dispatch | Socket events | None | Bounded 256-entry memory queue; overload not durably recovered | See reliability gaps |

These are in-process asyncio tasks, not independently deployed scheduled services. Shutdown cancels
and awaits tasks before releasing the OS ledger lock. No model-side scheduling or login automation.
A reconnect does not resend a potentially delivered QQ message. Hermes cron remains host-owned:
only the already-running native adapter can send, with the same outbound ACL; no standalone cron sender.
