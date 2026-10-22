"""Resolve the single-source pure modules within the current profile namespace."""

from importlib import import_module


def legacy_module(module_name: str):
    """Never alias a profile's modules into another profile or the global package."""
    package, leaf = module_name.rsplit(".", 1)
    prefix, separator, name = package.rpartition(".")
    if name != "onebot_fleet":
        raise ValueError("compatibility module must belong to onebot_fleet")
    legacy = f"{prefix}.hermes_onebot_fleet" if separator else "hermes_onebot_fleet"
    return import_module(f"{legacy}.{leaf}")
