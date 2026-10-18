# Architecture and reliability boundaries

```
NapCat A -- authenticated OneBot11 WS --\
                                       native adapter -> Hermes gateway -> Agent
NapCat B -- authenticated OneBot11 WS --/
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
Among verified healthy accounts, lowest numeric priority wins; account ID breaks ties.
Group sends preflight actual membership using get_group_member_info. Read-only preflight failure
can try another account; a send itself is NEVER replayed automatically on a different account.
If all accounts are offline, native send returns failure, not a false delivery acknowledgment.
Health is probed every 5 seconds and connections reconnect with capped exponential backoff.
A connected account is not necessarily able to speak in every group (mute/risk controls may differ).

Before dispatch, `sending` record includes chosen account and complete text. A confirmed
message ID transitions to `sent`; explicit server rejection to `failed`; timeout, disconnect,
cancellation or crash to `uncertain`. Unknown outcomes require human verification against QQ.
The record preserves the reply so operators do not need to re-run the model to inspect it.
It is not an autonomous retry outbox. The adapter marks its failures final to native
_send_with_retry, preventing the immediate plain-text fallback from changing/re-sending text.
However Hermes' separate delivery ledger and interrupted-turn recovery CAN reissue sends,
possibly with changed content and without the original reply anchor/context. Those host-owned
attempts are not reliably correlated by this plugin. There is no remote QQ idempotency/fencing
key. Strict no-duplicate behavior across host restart/redelivery is NOT implemented: this is
an experimental transport failover prototype, not a production exactly-once system. Do not
remove host recovery or fabricate successful acknowledgments to conceal this gap.

An OS-held exclusive lock protects each ledger from multiple plugin processes. There is no
cross-machine leader election. One profile process owns coordination, all transports stay connected;
only one logical ingress fingerprint reaches Hermes. Final native replies preferentially use
reply_to (the canonical event ID) for correlation. Task-context correlation is only a fallback:
native FIFO drains may inherit an earlier task context. Proactive/recovery sends without anchors
can have unique IDs and are not globally deduped. Recovered queued inputs recheck the current
logical namespace, users, groups, accounts and mention target; revoked work becomes denied.

## Lifecycle and unavailable features

Hermes restarts also stop the plugin; messages that NapCat does not replay during that gap
cannot be recovered by this in-process design. Initial pre-authentication events and overflow
past the bounded 256-event transport queue can be dropped. Do not present this as a general
message broker or total machine-failure failover. Running the same profile on two machines is unsupported.

v0.2.0 is text-only; it does not translate quoted message IDs across accounts, fetch image URLs,
download files, expose arbitrary OneBot actions, or register agent tools. reply_to is not used to
create native QQ quoted replies. Rich message/voice/file events are rejected, not summarized from metadata.
No media, button, edit/streaming, reaction, account login automation, or standalone cron sender.
Live native send/cron routes are available while gateway is running; out-of-process delivery is not.
Clarify and approval rely on Hermes text fallbacks; real QQ interaction must be tested before reliance.

No email or public/indexable HTTP routes: no emails.md or seo.md. Periodic transport tasks and
agent handoff are documented below. This package uses Python 3.12–3.14, aiohttp and SQLite;
Hermes provides native sessions, model execution, approvals and profile-bound credentials.

## Related documents

- [Permissions](permissions.md), [protected flows](flows.md), [variables and storage](variables.md).
- [Background work](cron.md), [automation surface](automation.md), [verification map](tests.md).
- [Installation and usage](../README.md), [configuration example](../examples/config.yaml).

## Known risks linked to implementation

- [Adapter send correlation](../src/hermes_onebot_fleet/adapter.py) cannot identify host-ledger replays without stable host obligation IDs.
- [Transport queue](../src/hermes_onebot_fleet/transport.py) is memory-only before ledger enqueue; overflow/pre-auth events can be lost.
- [Normalizer](../src/hermes_onebot_fleet/protocol.py) intentionally collapses identical same-sender/same-chat/same-second text.
- [Ledger](../src/hermes_onebot_fleet/store.py) is not atomic with native Hermes execution, retains private content and needs an operator retention policy.
- Private messages addressed to an offline bot are not delivered to another QQ account: only group observations offer cross-account inbound redundancy.
- Pre-rename local two-account NapCat testing on 2026-10-18 covered primary reply, standby handling @primary while the primary OneBot service was disabled, and return to primary after restoration: three real messages, exactly 3 handed_off / 3 sent. The renamed version still needs connection/message-path retesting; approval, notification and Hermes crash-recovery behavior remain unverified live.
