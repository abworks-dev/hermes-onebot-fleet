# Permissions

| Actor/resource | Operation | Enforced rule | Deny outcome |
|---|---|---|---|
| NapCat account | Connect | Loopback URL, per-account Bearer token, get_login_info matches configured QQ, online=true | Unhealthy; no accepted inbound |
| Human private sender | Dispatch | Actual sender in allowed_users and not a configured bot | Dropped before ledger/model |
| Human group sender | Dispatch | Actual sender in allowed_users AND group in allowed_groups AND explicit structured @configured bot | Dropped, including @all/textual CQ/reply-only |
| Configured bot | Trigger Agent | Never, even if it is in a user whitelist | Dropped to prevent bot loops |
| Hermes Agent | Send | Canonical logical namespace and private/group destination allowlist | SendResult failure, not retried |
| Local operator | Configure/read ledger | OS filesystem permissions; no remote admin endpoint | No application role system |

Hermes' normal native gateway admission remains active via ONEBOT_FLEET_ALLOWED_USERS.
Pairing, allow-all, global grants do not bypass the plugin's earlier local owner/group check.
No trusted_inbound, role_authorized, arbitrary tools, remote configuration, or MCP server.
No row-level SQL security: SQLite is a local per-profile plugin file protected by OS permissions.
The plugin does not sandbox the Hermes Agent's filesystem; choose OS isolation and toolsets separately.
Disabling group_sessions_per_user can intentionally share a group transcript between admitted users;
use true for user separation. Group source.user_id always remains the authenticated human QQ number.
