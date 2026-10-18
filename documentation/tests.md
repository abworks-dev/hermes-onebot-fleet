# Verification map

## Existing coverage

Offline Windows run: **42 tests passed, 39 subtests passed** on Python 3.12. Real installed Hermes
contract probe on Python 3.14 passed without network startup or profile modifications.

| Use case / rule | Expected deny or success | Evidence | Status |
|---|---|---|---|
| Local account configuration | Reject non-loopback, URL credentials, invalid IDs/weak token/type/unknown fields | [Policy tests](../tests/test_policy.py), [config](../src/hermes_qq_failover/config.py) | Existing automated unit |
| User AND group and @ | Unauthorized user/group, fake CQ/@all, bot/self sender, bad structured events denied | [Policy tests](../tests/test_policy.py), [protocol](../src/hermes_qq_failover/protocol.py) | Existing automated unit |
| Recovered queued authorization | Revoked owner/group/namespace/mention rejected; drain marks denied without host invocation | [Recovery tests](../tests/test_recovery_policy.py), [adapter tests](../tests/test_adapter.py) | Existing automated unit |
| Authenticated socket | Bearer auth, login identity, online status, reconnect, RPC echo, timeout/disconnect without replay | [Transport tests](../tests/test_transport.py), [transport](../src/hermes_qq_failover/transport.py) | Existing loopback integration, no QQ |
| Account selection | Offline primary skipped; group membership preflight may examine standby | [Adapter tests](../tests/test_adapter.py), [adapter](../src/hermes_qq_failover/adapter.py) | Existing host-double unit |
| Uncertain side effect | No second-account send after timeout/cancellation; native immediate fallback suppressed | [Adapter tests](../tests/test_adapter.py), [host probe](../tools/probe_hermes.py) | Existing automated unit + local real-host probe |
| Reply identity | Same answer to different canonical reply anchors is not suppressed by stale native task context | [Adapter tests](../tests/test_adapter.py) | Existing automated unit |
| Persistent ledger | One atomic claim, queued-only recovery, sending -> uncertain, handed_off not requeued | [Store tests](../tests/test_store.py), [store](../src/hermes_qq_failover/store.py) | Existing automated unit |
| Source staging | Existing profile config and credentials unchanged; invalid profile/missing status DB denied; existing plugin refuses overwrite | [CLI tests](../tests/test_cli.py), [CLI](../src/hermes_qq_failover/cli.py) | Existing temporary-profile integration |
| One process owner | Second OS file lock fails; release permits takeover | [Store tests](../tests/test_store.py), [lock](../src/hermes_qq_failover/lock.py) | Existing Windows unit |
| Native registration | Real adapter ABC, platform registration/config, canonical source and empty metadata | [Host probe](../tools/probe_hermes.py), [plugin](../src/hermes_qq_failover/plugin.py) | Existing local real-host probe, not full gateway E2E |

[CI](../.github/workflows/tests.yml) runs offline tests and lint on Windows/Linux. This workflow is
included, but its remote result and branch protection have not been verified; it is not yet an
enforced merge gate. Real-Hermes probe is local-only because CI does not vendor a host installation.

## Proposed tests (not completed)

| Use case | Rule / expected negative | Evidence | Type / status |
|---|---|---|---|
| Two real QQ accounts | No double ingress; primary offline before send selects standby; both offline reports failure | [Architecture](architecture.md), [adapter](../src/hermes_qq_failover/adapter.py) | Guarded live / proposed |
| Native owner controls | Unauthorized approvals denied; owner @ text clarification, commands and interrupt reach native flow | [Permissions](permissions.md), [flows](flows.md) | Guarded live / proposed |
| Profile isolation | Distinct secret scope, ledger, namespace and user session prevent cross-profile access | [Variables](variables.md), [adapter](../src/hermes_qq_failover/adapter.py) | Real-host integration / proposed |
| Managed PM enablement | PM resolves staged dependency without core mutation | [CLI](../src/hermes_qq_failover/cli.py) | Real-host integration / proposed |
| Sensitive storage | Operator confirms filesystem ACLs, retention policy and backup permissions | [Variables](variables.md) | Manual review / proposed |

## Gaps / not a green shipping verdict

1. **High: native Hermes delivery-ledger / crash-left recovery is not fenced by this plugin.**
   It can call send again with changed content or no original event ID. Transport-local tests
   do NOT establish host-wide at-most-once delivery. Stable durable host obligation identity /
   terminal-uncertain semantics are needed before making that promise. No production assertion.
2. Native resume may re-run model/tools outside plugin control; plugin ledger and host task store
   are not one transaction. Host task execution/recovery is deliberately not reimplemented here.
3. Live account risk controls, mute permissions, actual NapCat versions, notify/cron and group
   approval UX remain unverified. A successful mock transport test is not QQ validation.
4. FIFO task-context inheritance is not a universal durable identity. Canonical final reply anchors
   are preferred, but contextless/proactive/recovery sends have no global dedup guarantee.
5. No recovery for pre-auth/overflow events, same-second identical-text collisions, cross-machine
   leader election, automatic retention, OS sandbox setup, or private messages to an offline bot.
6. Local isolated wheel builds were attempted but failed in the build backend with
   `FileNotFoundError: [Errno 2] No usable temporary directory found`. Direct development-venv
   temporary-file creation and source tests work; isolated build-worker environment remains
   unresolved. No wheel/release is published; source staging is the tested local installation path.
