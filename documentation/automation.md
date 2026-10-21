# Agent and automation boundary

Trigger: an owner-whitelisted text message; in a permitted group, structured @ is mandatory.
Owner: the Hermes profile, not any one physical bot QQ account. The plugin forwards to native
Hermes automatically after deterministic admission; the native host owns tool approval and task state.

Plugin API allowlist in code: get_login_info, get_status, get_group_member_info,
send_group_msg, send_private_msg. These are fixed application-owned operations. It exposes
NO model-callable tools or arbitrary OneBot action API, performs no URL download, and does not
execute content as commands. Hermes' own commands and configured toolsets remain host functionality;
the example starts with an empty platform toolset. There is no extra custom approval/command engine.

Steering is limited to the registered platform_hint. Hard controls are the endpoint/identity checks,
owner+group policy, structured mention parsing, canonical target validation, single-process lock,
dedup ledger and no automatic retry after an uncertain send. Source hardening adds shared persistent
read-only member-query permits, due/single-flight health reads, stable reconnect backoff, aggregate
failure business holds and rolling minute/hour send budgets. These are application-owned controls,
not agent tools or platform-ban detection; manual scoped reset cannot resurrect an uncertain intent.
See [resilience and deployment boundaries](resilience.md). Prompts are NOT an authorization boundary.
The output contract is native SendResult, with retryable=false for failure/uncertainty; confirmed sends
require a remote message_id. Text is escaped by structured segments, not interpreted as CQ commands.

Kill switch: disable this plugin/platform in the owning profile using native Hermes administration.
The opt-in loopback publication endpoint is authenticated and cannot change the whitelist or select accounts.
It accepts only preconfigured target aliases and immutable publication IDs. Native contextless sends are denied.
No remote admin endpoint or model tool can change the whitelist or select accounts. Tokens and raw
message bodies are not logged by this code; the ledger intentionally retains body/reply content locally.
There is no new per-user rate limiter: owner-only first, then host runtime/conversation queue limits.
Do not open this configuration to a public group without a separate resource-abuse review.
