"""OneBot Fleet: host-independent connections and conservative delivery safety.

Importing this package never starts a connection or initializes runtime state.
"""

from .core import FleetCore, FleetSendResult, parse_target, target

__all__ = ["FleetCore", "FleetSendResult", "parse_target", "target"]
__version__ = "0.6.0"
