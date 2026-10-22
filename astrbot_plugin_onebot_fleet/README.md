# OneBot Fleet for AstrBot (0.6.0, experimental)

Native platform type `onebot_fleet`; not an `aiocqhttp` client shim. Contract checked against AstrBot commit `1b10813c70949071fa11153cc4ec8c51421d13d0` using offline host doubles. No real QQ account, PC/live-profile integration, real AstrBot instance, or end-to-end success has been verified. **Do not claim full AstrBot/plugin compatibility.**

## Install and cold start

Use the repository CLI's staged AstrBot plugin artifact: it contains this plugin root and both Python packages beneath `_vendor/`. The source template can alternatively import an already installed `onebot_fleet` package. No unpublished `onebot-fleet==0.6.0` PyPI dependency is required. Vendoring never mutates `sys.path`; errors inside a present vendor package fail closed instead of falling back to an unrelated installed version.

Install the staged directory as `data/plugins/astrbot_plugin_onebot_fleet`, enable the plugin, configure the platform, and **restart AstrBot**. First installation/reload is restart-only; hot install is not promised. AstrBot imports disabled plugins too: importing registers the platform class but does not grant its runtime capability. Only the active plugin's `initialize()` grants that capability. On shutdown/unload it is revoked before the plugin asks the platform manager to terminate its own platform IDs. It does not terminate unrelated platforms or call manager-wide shutdown.

Add an AstrBot platform entry (not plugin configuration):

```json
{
  "id": "onebot-fleet",
  "type": "onebot_fleet",
  "enable": true,
  "fleet": {
    "logical_id": "onebot-fleet",
    "state_path": "data/plugin_data/astrbot_plugin_onebot_fleet/fleet.sqlite3",
    "accounts": [
      {"id": "10001", "ws_url": "ws://127.0.0.1:3001", "token_env": "FLEET_TOKEN_A", "priority": 0}
    ],
    "allowed_users": ["20001"],
    "allowed_groups": ["30001"],
    "require_mention": true
  }
}
```

The IDs above are placeholders. Set the named token environment variable in the AstrBot process environment; never paste tokens into platform/plugin configuration. FleetCore receives only those explicitly named secrets. Each fleet needs its own persistent state path, shared with its account gates/budgets/publisher as documented by the core. Platform `id` is stable across physical-account failover and must match `[A-Za-z0-9_-]{1,128}`. Keep platform ID and logical ID fixed across restarts. No network connection or database is opened just by importing this plugin.

## Supported boundary

- Incoming normalized private/group text is admitted by FleetCore before queue handoff. Real sender IDs remain real sender IDs; no shared fictitious human identity. Sessions are `platform_id:FriendMessage:chat_id` or `platform_id:GroupMessage:chat_id`, independent of physical account. Group messages still require core allowlists and mentions.
- **Turn off `segmented_reply`, automatic quote/reply components, mention insertion, text-to-image (t2i), TTS and streaming.** Every component in an outbound chain must have exact type `Plain`. Images, mentions, quotes, audio, forwards, rich content, Plain subclasses and mixed-media chains are rejected before any send; media is never silently dropped.
- `event.send`, inherited `react` and `process_buffer` use the same receipt-aware send path. Streaming and `get_client()` are explicitly unsupported. No raw OneBot/aiocqhttp mutation bypass is exposed.
- Native replies use the immutable inbound dedup key as `reply_to`. Metadata, UMO strings and public `create_event()` cannot grant authorization. Concurrent identical replies reuse the stable durable key. A failed/unknown reply terminal-latches the event, stops it and clears pending results; later segments, reactions and retries on the event cannot send even if their text changes. Unknown outcomes are not resent on another account.
- `_has_send_oper` and the base host send metrics are updated only after `success` **and** a nonempty `message_id`. `event.fleet_receipts` is the adapter's immutable receipt view. It contains content digests, acknowledgements and generic failure codes, not tokens/raw exceptions.
- `Context.send_message` is allowed only while this adapter's own event is active in the host context, for the exact same immutable session. Missing/cross-session contexts fail with `FleetDeliveryError`, never a swallowed `False`. Proactive sends are advertised **unsupported**. Unanchored publications must use the core's durable publisher, not fabricate event/metadata anchors.
- Queue admission completes the core handoff; `handed_off` does not mean host execution or reply delivery is complete. A bounded full queue raises for the core to record; it does not indefinitely block the single inbound drain waiting for the entire host event.

## Important host limitation

At the pinned AstrBot commit, RespondStage catches ordinary send exceptions and can still invoke the **first `OnAfterMessageSentEvent` hook after a failed send**, then continue segmented processing. `stop_event()` cannot suppress that first hook. This plugin records receipts, terminal-latches subsequent sends, and stops/clears the failed event, but cannot truthfully change the host hook's meaning without a host fix. **Host `after_sent` does not prove delivery.** Audit downstream plugins before enabling them; plugins that assume it does may misreport success or perform unsafe follow-up actions. No `BaseException` trick, fake success, or global monkeypatch is used to bypass the host.

Compatibility and delivery claims are limited to offline tests until pinned-host and PC integration are separately verified. Restart after installing or reloading this first version.
