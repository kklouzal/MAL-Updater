from __future__ import annotations

import json
import math
import os
import threading
import uuid
import fcntl
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import AppConfig, load_config
from .persistence import DEFAULT_JSONL_MAX_LINE_BYTES, atomic_writer, iter_json_lines
from .redaction import sanitize_text, sanitize_url

REQUEST_TASK_ENV = "MAL_UPDATER_REQUEST_TASK"
REQUEST_RUN_ENV = "MAL_UPDATER_REQUEST_RUN_ID"
_MAX_API_EVENT_LINE_BYTES = DEFAULT_JSONL_MAX_LINE_BYTES


@dataclass(slots=True)
class ApiRequestContext:
    task: str | None
    run_id: str | None
    sequence: int = 0


@dataclass(frozen=True, slots=True)
class ApiEventBoundary:
    event_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class ApiEventPruneReport:
    status: str
    blocked: bool
    actual_removed: int
    expired_removed: int
    expired_candidates: int
    corrupt_records: int
    kept_records: int
    scanned_records: int

    @property
    def removed(self) -> int:
        """Compatibility alias for records actually removed from disk."""
        return self.actual_removed

    def as_dict(self) -> dict[str, int | str | bool]:
        return {
            "status": self.status,
            "blocked": self.blocked,
            "actual_removed": self.actual_removed,
            "expired_removed": self.expired_removed,
            "expired_candidates": self.expired_candidates,
            "corrupt_records": self.corrupt_records,
            "kept_records": self.kept_records,
            "scanned_records": self.scanned_records,
        }


@dataclass(slots=True)
class ApiUsageSummary:
    provider: str
    window_seconds: int
    request_count: int
    success_count: int
    error_count: int
    by_operation: dict[str, int]
    last_event_at: str | None
    task: str | None = None
    run_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "provider": self.provider,
            "window_seconds": self.window_seconds,
            "request_count": self.request_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "by_operation": self.by_operation,
            "last_event_at": self.last_event_at,
        }
        if self.task is not None:
            payload["task"] = self.task
        if self.run_id is not None:
            payload["run_id"] = self.run_id
        return payload


_request_context: ContextVar[ApiRequestContext | None] = ContextVar("mal_updater_api_request_context", default=None)
_append_lock = threading.Lock()


def begin_api_request_context(*, task: str | None, run_id: str | None = None) -> Token[ApiRequestContext | None]:
    normalized_task = str(task).strip() if task else None
    normalized_run = str(run_id).strip() if run_id else str(uuid.uuid4())
    return _request_context.set(ApiRequestContext(task=normalized_task, run_id=normalized_run))


def end_api_request_context(token: Token[ApiRequestContext | None]) -> None:
    _request_context.reset(token)


def current_api_request_context() -> ApiRequestContext:
    context = _request_context.get()
    if context is None:
        task = os.getenv(REQUEST_TASK_ENV) or None
        run_id = os.getenv(REQUEST_RUN_ENV) or None
        context = ApiRequestContext(task=task, run_id=run_id)
        _request_context.set(context)
    return context


def request_context_environment(*, task: str, run_id: str) -> dict[str, str]:
    return {REQUEST_TASK_ENV: task, REQUEST_RUN_ENV: run_id}


def sanitize_telemetry_url(url: str) -> str:
    """Compatibility wrapper around the shared central URL sanitizer."""
    return sanitize_url(url, max_length=1_000)


def _safe_error(error: object) -> str | None:
    if error is None:
        return None
    return sanitize_text(error, max_length=300)


def _events_path(config: AppConfig | None = None) -> Path:
    resolved_config = config if config is not None and hasattr(config, "api_request_events_path") else load_config()
    resolved_config.api_request_events_path.parent.mkdir(parents=True, exist_ok=True)
    return resolved_config.api_request_events_path


@contextmanager
def _event_file_lock(path: Path, *, exclusive: bool):
    lock_path = path.with_name(f"{path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _parse_event_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        # Historical telemetry may be naive. Treat it as UTC so one legacy
        # record cannot break pruning, budgets, or scheduler comparisons.
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _iter_events(config: AppConfig | None = None) -> Iterator[dict[str, Any]]:
    path = _events_path(config)
    if not path.exists():
        return
    with _event_file_lock(path, exclusive=False):
        if not path.exists():
            return
        for record in iter_json_lines(path, max_line_bytes=_MAX_API_EVENT_LINE_BYTES):
            if record.error is not None:
                continue
            if isinstance(record.value, dict):
                yield record.value


def capture_api_event_boundary(*, config: AppConfig | None = None) -> ApiEventBoundary:
    return ApiEventBoundary(frozenset(str(event["event_id"]) for event in _iter_events(config) if event.get("event_id")))


def count_api_events_since(
    boundary: ApiEventBoundary,
    *,
    provider: str,
    task: str | None = None,
    run_id: str | None = None,
    config: AppConfig | None = None,
) -> int:
    count = 0
    for event in _iter_events(config):
        event_id = event.get("event_id")
        if event.get("provider") != provider or not event_id or str(event_id) in boundary.event_ids:
            continue
        if task is not None and event.get("task") != task:
            continue
        if run_id is not None and event.get("run_id") != run_id:
            continue
        count += 1
    return count


def _recent_provider_event_times(
    *, provider: str, window_seconds: int, task: str | None = None,
    include_legacy_in_task: bool = False, config: AppConfig | None = None,
) -> list[datetime]:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    event_times: list[datetime] = []
    for event in _iter_events(config):
        if event.get("provider") != provider:
            continue
        if task is not None and event.get("task") != task:
            if not (include_legacy_in_task and event.get("task") is None and event.get("run_id") is None):
                continue
        at = _parse_event_timestamp(event.get("at"))
        if at is not None and at >= cutoff:
            event_times.append(at)
    event_times.sort()
    return event_times


def record_api_request_event(
    provider: str,
    operation: str,
    *,
    url: str,
    method: str,
    outcome: str,
    status_code: int | None = None,
    error: str | None = None,
    config: AppConfig | None = None,
) -> str:
    context = current_api_request_context()
    context.sequence += 1
    event_id = str(uuid.uuid4())
    event = {
        "schema_version": 2,
        "event_id": event_id,
        "at": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "provider": sanitize_text(provider, max_length=100),
        "operation": sanitize_text(operation, max_length=200),
        "url": sanitize_telemetry_url(url),
        "method": sanitize_text(method.upper(), max_length=20),
        "outcome": sanitize_text(outcome, max_length=100),
        "status_code": status_code,
        "error": _safe_error(error),
        "task": sanitize_text(context.task, max_length=200) if context.task is not None else None,
        "run_id": sanitize_text(context.run_id, max_length=200) if context.run_id is not None else None,
        "attempt_sequence": context.sequence,
    }
    path = _events_path(config)
    with _append_lock, _event_file_lock(path, exclusive=True):
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True) + "\n")
    return event_id


def summarize_recent_api_usage(
    *, provider: str, window_seconds: int = 3600, task: str | None = None, run_id: str | None = None,
    include_legacy_in_task: bool = False, config: AppConfig | None = None
) -> ApiUsageSummary:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    request_count = success_count = error_count = 0
    by_operation: Counter[str] = Counter()
    last_event_at: str | None = None
    for event in _iter_events(config):
        if event.get("provider") != provider:
            continue
        # Legacy events intentionally remain part of provider-global accounting, but cannot
        # be guessed into a task/run scope.
        if task is not None and event.get("task") != task:
            if not (include_legacy_in_task and event.get("task") is None and event.get("run_id") is None):
                continue
        if run_id is not None and event.get("run_id") != run_id:
            continue
        at_raw = event.get("at")
        at = _parse_event_timestamp(at_raw)
        if at is None or at < cutoff:
            continue
        request_count += 1
        by_operation[str(event.get("operation") or "unknown")] += 1
        if event.get("outcome") == "ok":
            success_count += 1
        else:
            error_count += 1
        if last_event_at is None or str(at_raw) > last_event_at:
            last_event_at = str(at_raw)
    return ApiUsageSummary(provider, window_seconds, request_count, success_count, error_count, dict(by_operation), last_event_at, task, run_id)


def estimate_budget_recovery_seconds_for_ratio(
    *, provider: str, limit: int, target_ratio: float, projected_requests: int = 0,
    window_seconds: int = 3600, task: str | None = None, include_legacy_in_task: bool = False,
    config: AppConfig | None = None,
) -> int:
    if limit <= 0:
        return 0
    event_times = _recent_provider_event_times(
        provider=provider, window_seconds=window_seconds, task=task,
        include_legacy_in_task=include_legacy_in_task, config=config,
    )
    if not event_times:
        return 0
    normalized_ratio = max(0.0, min(1.0, float(target_ratio)))
    allowed_requests = max(0, math.floor(limit * normalized_ratio) - max(0, int(projected_requests)) - 1)
    if len(event_times) <= allowed_requests:
        return 0
    now = datetime.now(timezone.utc)
    drop_count = len(event_times) - allowed_requests
    return max(0, math.ceil(((event_times[drop_count - 1] + timedelta(seconds=window_seconds)) - now).total_seconds()))


def estimate_budget_recovery_seconds(
    *, provider: str, limit: int, critical_ratio: float, projected_requests: int = 0,
    window_seconds: int = 3600, task: str | None = None, include_legacy_in_task: bool = False,
    config: AppConfig | None = None,
) -> int:
    return estimate_budget_recovery_seconds_for_ratio(
        provider=provider, limit=limit, target_ratio=critical_ratio, projected_requests=projected_requests,
        window_seconds=window_seconds, task=task, include_legacy_in_task=include_legacy_in_task, config=config,
    )


def prune_api_request_events_with_diagnostics(*, retention_days: int = 14, config: AppConfig | None = None) -> ApiEventPruneReport:
    path = _events_path(config)
    if not path.exists():
        return ApiEventPruneReport(
            status="ok",
            blocked=False,
            actual_removed=0,
            expired_removed=0,
            expired_candidates=0,
            corrupt_records=0,
            kept_records=0,
            scanned_records=0,
        )
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    kept_records = expired_candidates = corrupt_records = scanned_records = 0
    with _event_file_lock(path, exclusive=True):
        if not path.exists():
            return ApiEventPruneReport(
                status="ok",
                blocked=False,
                actual_removed=0,
                expired_removed=0,
                expired_candidates=0,
                corrupt_records=0,
                kept_records=0,
                scanned_records=0,
            )

        for record in iter_json_lines(path, max_line_bytes=_MAX_API_EVENT_LINE_BYTES):
            scanned_records += 1
            if record.error is not None:
                corrupt_records += 1
                continue
            event = record.value
            if not isinstance(event, dict):
                corrupt_records += 1
                continue
            at = _parse_event_timestamp(event.get("at"))
            if at is None:
                corrupt_records += 1
                continue
            if at >= cutoff:
                kept_records += 1
            else:
                expired_candidates += 1

        if corrupt_records:
            return ApiEventPruneReport(
                status="blocked_corrupt",
                blocked=True,
                actual_removed=0,
                expired_removed=0,
                expired_candidates=expired_candidates,
                corrupt_records=corrupt_records,
                kept_records=kept_records,
                scanned_records=scanned_records,
            )

        if expired_candidates == 0:
            return ApiEventPruneReport(
                status="ok",
                blocked=False,
                actual_removed=0,
                expired_removed=0,
                expired_candidates=0,
                corrupt_records=0,
                kept_records=kept_records,
                scanned_records=scanned_records,
            )

        expired_removed = 0
        with atomic_writer(path) as out:
            for record in iter_json_lines(path, max_line_bytes=_MAX_API_EVENT_LINE_BYTES):
                event = record.value
                if record.error is not None or not isinstance(event, dict):
                    raise RuntimeError("api_request_events_changed_during_locked_prune")
                at = _parse_event_timestamp(event.get("at"))
                if at is None:
                    raise RuntimeError("api_request_events_changed_during_locked_prune")
                if at >= cutoff:
                    out.write(json.dumps(event, sort_keys=True).encode("utf-8") + b"\n")
                else:
                    expired_removed += 1
    return ApiEventPruneReport(
        status="ok",
        blocked=False,
        actual_removed=expired_removed,
        expired_removed=expired_removed,
        expired_candidates=expired_candidates,
        corrupt_records=0,
        kept_records=kept_records,
        scanned_records=scanned_records,
    )


def prune_api_request_events(*, retention_days: int = 14, config: AppConfig | None = None) -> int:
    return prune_api_request_events_with_diagnostics(retention_days=retention_days, config=config).removed
