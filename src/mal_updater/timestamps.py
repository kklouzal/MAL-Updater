"""Timestamp parsing shared by the CLI, health, scheduler and planner code."""

from __future__ import annotations

from datetime import datetime, timezone


def parse_utc_timestamp(value: object) -> datetime | None:
    """Parse an ISO-8601 or SQLite ``YYYY-MM-DD HH:MM:SS`` timestamp as an aware UTC datetime.

    A trailing ``Z`` is accepted, naive values are taken as UTC (SQLite
    CURRENT_TIMESTAMP and older telemetry), and anything unparseable gives None.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        # Converting an extreme offset can leave datetime's range.
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None
