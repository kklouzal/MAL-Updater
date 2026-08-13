from __future__ import annotations

import fcntl
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .periodic_evidence_lifecycle import (
    DEFAULT_PERIODIC_EVIDENCE_REFRESH_JITTER_DAYS,
    DEFAULT_PERIODIC_EVIDENCE_REFRESH_TARGET_DAYS,
    PERIODIC_EVIDENCE_SCHEDULE_VERSION,
    iso_utc,
    periodic_evidence_refresh_due_at,
    periodic_evidence_schedule_key,
    stable_periodic_evidence_jitter_days,
)

DEFAULT_PROVIDER_ELIGIBILITY_REFRESH_TARGET_DAYS = DEFAULT_PERIODIC_EVIDENCE_REFRESH_TARGET_DAYS
DEFAULT_PROVIDER_ELIGIBILITY_REFRESH_JITTER_DAYS = DEFAULT_PERIODIC_EVIDENCE_REFRESH_JITTER_DAYS
PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION = "provider-eligibility-120d-v1"
_PROVIDER_ELIGIBILITY_SURFACE = "provider_eligibility"


class ProviderEligibilityProcessLease:
    """Non-blocking provider-specific lease shared by daemon and manual runs."""

    def __init__(self, lease_dir: Path, provider: str) -> None:
        self.provider = str(provider).strip().lower()
        self.lock_path = lease_dir / f"provider-eligibility-{self.provider}.lock"
        self.status_path = lease_dir / f"provider-eligibility-{self.provider}.json"
        self.run_id = uuid.uuid4().hex
        self._handle: Any = None
        self.status: dict[str, Any] = {}

    def try_acquire(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            self.status = {"status": "busy", "provider": self.provider, "reason": "lease_busy"}
            return False
        self._handle = handle
        self.status = {
            "status": "running",
            "provider": self.provider,
            "pid": os.getpid(),
            "run_id": self.run_id,
            "started_at": _iso(datetime.now(timezone.utc)),
            "started_epoch": time.time(),
        }
        self.status_path.write_text(json.dumps(self.status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return True

    def release(self) -> None:
        if self._handle is None:
            return
        self.status = {**self.status, "status": "released", "released_at": _iso(datetime.now(timezone.utc))}
        self.status_path.write_text(json.dumps(self.status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


def _iso(value: datetime) -> str:
    return iso_utc(value)


def canonical_provider_eligibility_semantic_key(
    *,
    mal_anime_id: int,
    provider: str,
    provider_series_id: str,
) -> str:
    """Return the canonical, host- and runtime-independent semantic item key."""
    payload: dict[str, Any] = {
        "mal_anime_id": int(mal_anime_id),
        "provider": str(provider).strip().lower(),
        "provider_series_id": str(provider_series_id).strip(),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def provider_eligibility_refresh_schedule_key(
    *,
    mal_anime_id: int,
    provider: str,
    provider_series_id: str,
    schedule_version: str = PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
) -> str:
    identity = {
        "mal_anime_id": int(mal_anime_id),
        "provider": str(provider).strip().lower(),
        "provider_series_id": str(provider_series_id).strip(),
    }
    return periodic_evidence_schedule_key(
        surface=_PROVIDER_ELIGIBILITY_SURFACE,
        identity=identity,
        schedule_version=schedule_version,
    )


def stable_provider_eligibility_refresh_jitter_days(
    *,
    mal_anime_id: int,
    provider: str,
    provider_series_id: str,
    jitter_days: int = DEFAULT_PROVIDER_ELIGIBILITY_REFRESH_JITTER_DAYS,
    schedule_version: str = PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
) -> int:
    identity = {
        "mal_anime_id": int(mal_anime_id),
        "provider": str(provider).strip().lower(),
        "provider_series_id": str(provider_series_id).strip(),
    }
    return stable_periodic_evidence_jitter_days(
        surface=_PROVIDER_ELIGIBILITY_SURFACE,
        identity=identity,
        jitter_days=jitter_days,
        schedule_version=schedule_version,
    )


def provider_eligibility_refresh_due_at(
    *,
    successful_verified_at: datetime | str,
    mal_anime_id: int,
    provider: str,
    provider_series_id: str,
    target_days: int = DEFAULT_PROVIDER_ELIGIBILITY_REFRESH_TARGET_DAYS,
    jitter_days: int = DEFAULT_PROVIDER_ELIGIBILITY_REFRESH_JITTER_DAYS,
    schedule_version: str = PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
) -> str | None:
    """Schedule a successful outcome's refresh; target_days=0 explicitly disables it."""
    identity = {
        "mal_anime_id": int(mal_anime_id),
        "provider": str(provider).strip().lower(),
        "provider_series_id": str(provider_series_id).strip(),
    }
    return periodic_evidence_refresh_due_at(
        successful_at=successful_verified_at,
        surface=_PROVIDER_ELIGIBILITY_SURFACE,
        identity=identity,
        target_days=target_days,
        jitter_days=jitter_days,
        schedule_version=schedule_version,
    )
