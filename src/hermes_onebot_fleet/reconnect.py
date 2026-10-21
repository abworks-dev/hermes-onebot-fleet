"""Bounded local OneBot reconnect policy; not QQ login or a send retry."""

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields


@dataclass(frozen=True)
class ReconnectPolicy:
    initial_seconds: float = 1
    max_seconds: float = 60
    stable_seconds: float = 120
    jitter_ratio: float = 0.25

    def __post_init__(self):
        for name in ("initial_seconds", "max_seconds", "stable_seconds", "jitter_ratio"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError("reconnect settings must be finite numbers, not booleans")
        if not 1 <= self.initial_seconds <= self.max_seconds <= 3600:
            raise ValueError("reconnect requires 1 <= initial_seconds <= max_seconds <= 3600")
        if not 1 <= self.stable_seconds <= 86400 or not 0 <= self.jitter_ratio <= 0.5:
            raise ValueError("invalid reconnect stable_seconds or jitter_ratio")

    @classmethod
    def from_mapping(cls, value):
        if not isinstance(value, Mapping) or set(value) - {f.name for f in fields(cls)}:
            raise ValueError("invalid reconnect settings")
        return cls(**value)

    def delay(self, base: float, sample: float) -> float:
        # A nonzero floor avoids full-jitter rapid retries; the cap stays absolute.
        if type(sample) not in (int, float) or not math.isfinite(sample) or not 0 <= sample <= 1:
            raise ValueError("invalid reconnect random sample")
        return min(base, self.max_seconds) * (1 - self.jitter_ratio + self.jitter_ratio * sample)

    def status(self) -> dict:
        return asdict(self)
