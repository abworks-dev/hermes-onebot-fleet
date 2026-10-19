# Protected flows

## Owner text -> Agent

Actor: admitted human. Preconditions: a verified NapCat account and owning Hermes profile.
The transport authenticates locally, then matches event self_id; a single account-state snapshot
must be allowed and supplies a durable revision. Normalization checks sender,
group and structured mention. Denied events cause no model call or ledger write. Accepted text
crosses the OneBot -> plugin trust boundary and is saved to the per-profile ledger. A unique
fingerprint suppresses duplicate observations from other bot accounts. The dispatcher constructs
a stable canonical source after rechecking the captured revision and current message policy, then
hands it to native Hermes, where normal gateway admission applies. Old/revoked work becomes denied.
An acknowledgment marks host handoff, not model/task completion. Native approval controls still
require the admitted owner to @ a configured bot in the group.

## Agent reply -> QQ

Actor: native Hermes runtime replying to a correlated inbound message. Preconditions: allowed canonical target
and a reply_to matching an original handed-off inbound with an unchanged allowed account revision.
Contextless cron/notification sends are rejected; inherited INGRESS is not authorization.
The adapter rechecks namespace/target/source admission, preserves a qualified sticky primary, and
checks group membership if applicable. After awaited preflight and again after the sending ledger
claim, source and selected account permits are revalidated before the OneBot side-effect boundary.
A final pre-dispatch revocation preserves a no-send terminal tombstone, not an automatic reroute.
Already-dispatched RPCs cannot be recalled by a concurrent pause.
Text is serialized as a structured text segment; CQ strings are not interpreted. Confirmed remote
message ID marks sent. Unknown delivery is recorded uncertain and is not retried by this transport.
Background jobs must use the optional restricted publisher, not the native proactive-send fallback.
Native conversation recovery itself is not an exactly-once transaction; changing reply text or reply IDs is not a
business publication identity. The publisher freezes both content and target before dispatch and never permits
host-native fallback to mint a new publication.
Explicitly rejected sends mark failed. No arbitrary API calls are exposed to the model.

## Background publication -> QQ

Actor: explicitly allowlisted publisher client, with a separate bearer token and fixed target aliases. The
loopback publisher authenticates every request and stores (client, publication_id, alias, text) before admission
returns 202. Identical duplicates query the same row; differing content or targets conflict. A single worker
uses the running Fleet connection, never a second QQ client. Only a pre-dispatch NoRoute can return pending;
any ambiguous send/restart becomes unknown and requires manual reconciliation. Client scope, rate and expiry
are enforced independently of source content and agent prompts. Status sent requires a validated numeric
remote message ID, not merely a callback or HTTP success.

## Restart/fault

Actor: local Hermes process. Acquire OS-exclusive ledger lock; a second process fails closed.
Recovery keeps queued inbound items dispatchable, but abandoned handoff/send states become uncertain.
Already handed-off events are not reinjected into the host. Operator compares uncertain records
with actual Hermes sessions and QQ before any manual action; there is no automatic requeue button.
The old plugin and the new plugin must NOT be enabled for the same account concurrently.
Account pause/quarantine/observation, revision and sticky preference survive restart. Re-admission
does not revive old replies; downgrading to a pre-P0 plugin loses these gates and requires an
operator-held traffic stop/configuration review. See [P0 lifecycle](account-lifecycle.md).
