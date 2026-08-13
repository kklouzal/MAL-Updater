from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

DEFAULT_PERIODIC_EVIDENCE_REFRESH_TARGET_DAYS = 120
DEFAULT_PERIODIC_EVIDENCE_REFRESH_JITTER_DAYS = 15
PERIODIC_EVIDENCE_SCHEDULE_VERSION = "periodic-evidence-120d-v1"


def utc_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_periodic_evidence_key(*, surface: str, identity: Any) -> str:
    payload = {"surface": str(surface).strip().lower(), "identity": identity}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def periodic_evidence_schedule_key(
    *, surface: str, identity: Any, schedule_version: str = PERIODIC_EVIDENCE_SCHEDULE_VERSION
) -> str:
    canonical = canonical_periodic_evidence_key(surface=surface, identity=identity)
    return "sha256:" + hashlib.sha256(f"{schedule_version}\n{canonical}".encode("utf-8")).hexdigest()


def stable_periodic_evidence_jitter_days(
    *,
    surface: str,
    identity: Any,
    jitter_days: int = DEFAULT_PERIODIC_EVIDENCE_REFRESH_JITTER_DAYS,
    schedule_version: str = PERIODIC_EVIDENCE_SCHEDULE_VERSION,
) -> int:
    jitter = max(0, int(jitter_days))
    if jitter == 0:
        return 0
    key = periodic_evidence_schedule_key(surface=surface, identity=identity, schedule_version=schedule_version)
    bucket_count = 2 * jitter + 1
    return int(key.removeprefix("sha256:")[:16], 16) % bucket_count - jitter


def periodic_evidence_refresh_due_at(
    *,
    successful_at: datetime | str,
    surface: str,
    identity: Any,
    target_days: int = DEFAULT_PERIODIC_EVIDENCE_REFRESH_TARGET_DAYS,
    jitter_days: int = DEFAULT_PERIODIC_EVIDENCE_REFRESH_JITTER_DAYS,
    schedule_version: str = PERIODIC_EVIDENCE_SCHEDULE_VERSION,
) -> str | None:
    target = max(0, int(target_days))
    if target == 0:
        return None
    jitter = max(0, int(jitter_days))
    if jitter > target:
        raise ValueError("jitter_days must not exceed target_days")
    offset = stable_periodic_evidence_jitter_days(
        surface=surface,
        identity=identity,
        jitter_days=jitter,
        schedule_version=schedule_version,
    )
    return iso_utc(utc_datetime(successful_at) + timedelta(days=target + offset))


def periodic_evidence_is_due(
    *,
    successful_at: datetime | str | None,
    surface: str,
    identity: Any,
    now: datetime | str | None = None,
    target_days: int = DEFAULT_PERIODIC_EVIDENCE_REFRESH_TARGET_DAYS,
    jitter_days: int = DEFAULT_PERIODIC_EVIDENCE_REFRESH_JITTER_DAYS,
) -> bool:
    if successful_at is None:
        return True
    target = max(0, int(target_days))
    if target == 0:
        return True
    try:
        due = periodic_evidence_refresh_due_at(
            successful_at=successful_at,
            surface=surface,
            identity=identity,
            target_days=target,
            jitter_days=min(max(0, int(jitter_days)), target),
        )
        reference = utc_datetime(now or datetime.now(timezone.utc))
        return due is None or utc_datetime(due) <= reference
    except (TypeError, ValueError):
        return True
