from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import load_config


@dataclass(slots=True)
class ApiUsageSummary:
    provider: str
    window_seconds: int
    request_count: int
    success_count: int
    error_count: int
    by_operation: dict[str, int]
    last_event_at: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "window_seconds": self.window_seconds,
            "request_count": self.request_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "by_operation": self.by_operation,
            "last_event_at": self.last_event_at,
        }


def _events_path() -> Path:
    config = load_config()
    config.api_request_events_path.parent.mkdir(parents=True, exist_ok=True)
    return config.api_request_events_path


def record_api_request_event(
    provider: str,
    operation: str,
    *,
    url: str,
    method: str,
    outcome: str,
    status_code: int | None = None,
    error: str | None = None,
) -> None:
    event = {
        "at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provider": provider,
        "operation": operation,
        "url": url,
        "method": method,
        "outcome": outcome,
        "status_code": status_code,
        "error": error,
    }
    path = _events_path()
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, sort_keys=True) + "\n")


def summarize_recent_api_usage(*, provider: str, window_seconds: int = 3600) -> ApiUsageSummary:
    path = _events_path()
    if not path.exists():
        return ApiUsageSummary(provider, window_seconds, 0, 0, 0, {}, None)
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    request_count = 0
    success_count = 0
    error_count = 0
    by_operation: Counter[str] = Counter()
    last_event_at: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("provider") != provider:
            continue
        at_raw = event.get("at")
        if not isinstance(at_raw, str):
            continue
        try:
            at = datetime.fromisoformat(at_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if at < cutoff:
            continue
        request_count += 1
        operation = str(event.get("operation") or "unknown")
        by_operation[operation] += 1
        if event.get("outcome") == "ok":
            success_count += 1
        else:
            error_count += 1
        last_event_at = at_raw
    return ApiUsageSummary(provider, window_seconds, request_count, success_count, error_count, dict(by_operation), last_event_at)


def prune_api_request_events(*, retention_days: int = 14) -> int:
    path = _events_path()
    if not path.exists():
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    kept: list[str] = []
    removed = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
            at = datetime.fromisoformat(str(event.get("at", "")).replace("Z", "+00:00"))
        except Exception:
            removed += 1
            continue
        if at >= cutoff:
            kept.append(json.dumps(event, sort_keys=True))
        else:
            removed += 1
    path.write_text(("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
    return removed
