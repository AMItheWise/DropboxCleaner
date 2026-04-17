from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Event

from app.models.events import ProgressSnapshot


class CancellationRequested(Exception):
    """Raised when the user requests graceful cancellation."""


class CancellationToken:
    def __init__(self) -> None:
        self._event = Event()

    def cancel(self) -> None:
        self._event.set()

    def check(self) -> None:
        if self._event.is_set():
            raise CancellationRequested("Run cancelled by user.")

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()


ProgressEmitter = Callable[[ProgressSnapshot], None]


@dataclass(slots=True)
class RunResult:
    run_id: str
    run_dir: str
    summary_path: str
    verification_path: str | None
