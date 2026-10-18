# Protected flows

## Owner text -> Agent

Actor: admitted human. Preconditions: a verified NapCat account and owning Hermes profile.
The transport authenticates locally, then matches event self_id; normalization checks sender,
group and structured mention. Denied events cause no model call or ledger write. Accepted text
crosses the OneBot -> plugin trust boundary and is saved to the per-profile ledger. A unique
fingerprint suppresses duplicate observations from other bot accounts. The dispatcher constructs
a stable canonical source and hands it to native Hermes, where normal gateway admission applies.
An acknowledgment marks host handoff, not model/task completion. Native approval controls still
require the admitted owner to @ a configured bot in the group.

## Agent reply -> QQ

Actor: native Hermes runtime, including live notifications. Preconditions: allowed canonical target.
The adapter rechecks namespace and target policy, chooses a verified account, and checks group
membership if applicable. It records sending BEFORE crossing plugin -> OneBot side-effect boundary.
Text is serialized as a structured text segment; CQ strings are not interpreted. Confirmed remote
message ID marks sent. Unknown delivery is recorded uncertain and is not retried by this transport.
The immediate native plain-text fallback is suppressed. Independent native delivery-ledger and
crash-recovery attempts may still resend; strict host-recovery no-duplicates is an open gap.
Explicitly rejected sends mark failed. No arbitrary API calls are exposed to the model.

## Restart/fault

Actor: local Hermes process. Acquire OS-exclusive ledger lock; a second process fails closed.
Recovery keeps queued inbound items dispatchable, but abandoned handoff/send states become uncertain.
Already handed-off events are not reinjected into the host. Operator compares uncertain records
with actual Hermes sessions and QQ before any manual action; there is no automatic requeue button.
The old plugin and the new plugin must NOT be enabled for the same account concurrently.
