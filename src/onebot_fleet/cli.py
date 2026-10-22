"""Canonical offline operator entry point, retaining the legacy CLI alias."""

from importlib import import_module

_prefix, _separator, _ = __package__.rpartition(".")
main = import_module(f"{_prefix}.hermes_onebot_fleet.cli" if _separator else "hermes_onebot_fleet.cli").main

if __name__ == "__main__":
    main()
