from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

from app.models.config import RetrySettings

T = TypeVar("T")


class RetryableError(Exception):
    """Marker exception used by tests and service wrappers."""


def retry_call(
    *,
    operation_name: str,
    func: Callable[[], T],
    logger: logging.Logger,
    retry_settings: RetrySettings,
    is_retryable: Callable[[Exception], bool],
    on_retry: Callable[[int, float, Exception], None] | None = None,
) -> T:
    attempt = 0
    backoff = retry_settings.initial_backoff_seconds
    while True:
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt > retry_settings.max_retries or not is_retryable(exc):
                raise
            wait_seconds = min(backoff, retry_settings.max_backoff_seconds)
            logger.warning(
                "%s failed with a retryable error on attempt %s/%s. Retrying in %.1fs.",
                operation_name,
                attempt,
                retry_settings.max_retries,
                wait_seconds,
                extra={"phase": "retry"},
            )
            if on_retry is not None:
                on_retry(attempt, wait_seconds, exc)
            time.sleep(wait_seconds)
            backoff *= retry_settings.backoff_multiplier
