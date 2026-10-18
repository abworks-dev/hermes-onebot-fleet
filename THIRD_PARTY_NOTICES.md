# References and dependencies

This repository implements a new adapter using Hermes' native plugin API and the
OneBot 11 structured protocol. It does not vendor Hermes, NapCat, NoneBot, or
`nonebot-plugin-hermes` implementations. It uses aiohttp as a separately installed
dependency; its license remains applicable.

Interface/compatibility references inspected during development:
- NousResearch/hermes-agent: native platform registration, adapter ABC, source/event/session contracts.
- ii999/hermes-napcat (MIT, Copyright (c) 2026 ii999): community adapter reference and previously observed compatibility pitfalls. No engagement, tool policy, media, or transport implementation is vendored here.
- OneBot 11 protocol and NapCatQQ: external QQ transport, not included with this plugin.

No QQ client, credentials, live account identifiers, Hermes installation, model
provider configuration, runtime databases, or reference repository copies are
included. GitHub repository ownership does not imply endorsement by these projects.
