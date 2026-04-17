from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ProgressSnapshot:
    phase: str
    message: str
    counters: dict[str, int] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)
    level: str = "INFO"
    extra: dict[str, Any] = field(default_factory=dict)
