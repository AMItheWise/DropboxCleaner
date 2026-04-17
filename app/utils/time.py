from __future__ import annotations

from datetime import UTC, date, datetime, time


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat().replace("+00:00", "Z")


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_cutoff_date(value: str) -> datetime:
    parsed_date = date.fromisoformat(value)
    return datetime.combine(parsed_date, time.min, tzinfo=UTC)


def timestamp_slug(value: datetime) -> str:
    return value.astimezone(UTC).strftime("dropbox_archive_%Y-%m-%dT%H-%M-%SZ")
