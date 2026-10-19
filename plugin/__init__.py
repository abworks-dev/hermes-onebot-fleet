"""Hermes directory plugin with profile-scoped relative module imports.

Keep implementation modules under the host-owned directory-plugin namespace so
native reload evicts the entire package rather than reusing global sys.modules.
"""


def register(ctx):
    from .src.hermes_onebot_fleet.plugin import register as register_native

    register_native(ctx)
