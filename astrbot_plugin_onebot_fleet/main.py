"""Exactly one native Star entrypoint; import-time registration is not activation."""

from astrbot.api.star import Context, Star

from . import adapter


class OneBotFleetPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context)
        self.context = context

    async def initialize(self) -> None:
        # Cold-start order: plugin initialization precedes platform initialization.
        # Disabled plugins may still be imported by AstrBot, so no import-time gate.
        adapter.activate(self)

    async def terminate(self) -> None:
        adapter.revoke(self)  # Revoke BEFORE awaiting any platform teardown.
        manager = self.context.platform_manager
        owned = [
            platform
            for platform in tuple(manager.platform_insts)
            if isinstance(platform, adapter.OneBotFleetAdapter) and platform._fleet_owner is self
        ]
        for platform in owned:
            await manager.terminate_platform(platform.meta().id)
