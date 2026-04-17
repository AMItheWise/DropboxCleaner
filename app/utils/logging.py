from __future__ import annotations

import json
import logging
from logging import LogRecord
from pathlib import Path
from queue import Queue


class JsonLineFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", None),
            "phase": getattr(record, "phase", None),
            "event_type": getattr(record, "event_type", None),
        }
        extra_context = getattr(record, "context", None)
        if extra_context is not None:
            payload["context"] = extra_context
        return json.dumps(payload, ensure_ascii=False)


class UiLogHandler(logging.Handler):
    def __init__(self, queue: Queue[str]) -> None:
        super().__init__()
        self._queue = queue

    def emit(self, record: LogRecord) -> None:
        self._queue.put(self.format(record))


def build_run_logger(run_id: str, log_path: Path, jsonl_path: Path, ui_queue: Queue[str] | None = None) -> logging.Logger:
    logger = logging.getLogger(f"dropbox_cleaner.{run_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    log_path.parent.mkdir(parents=True, exist_ok=True)

    text_handler = logging.FileHandler(log_path, encoding="utf-8")
    text_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s [%(phase)s] %(message)s",
            "%Y-%m-%dT%H:%M:%SZ",
            defaults={"phase": "-"},
        )
    )
    logger.addHandler(text_handler)

    json_handler = logging.FileHandler(jsonl_path, encoding="utf-8")
    json_handler.setFormatter(JsonLineFormatter(datefmt="%Y-%m-%dT%H:%M:%SZ"))
    logger.addHandler(json_handler)

    if ui_queue is not None:
        ui_handler = UiLogHandler(ui_queue)
        ui_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
        logger.addHandler(ui_handler)

    return logger
