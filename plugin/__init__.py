"""Hermes directory-plugin loader for the staged self-contained source package."""

import sys
from pathlib import Path


def register(ctx):
    source = str(Path(__file__).resolve().parent / "src")
    if source not in sys.path:
        sys.path.insert(0, source)
    from hermes_qq_failover.plugin import register as register_native

    register_native(ctx)
