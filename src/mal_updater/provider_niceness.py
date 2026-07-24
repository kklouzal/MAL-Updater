from __future__ import annotations

import email.utils
import fcntl
import json
import os
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path
from typing import Callable


_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def _thread_lock(key: str) -> threading.Lock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(key, threading.Lock())


@dataclass(slots=True)
class ProviderRequestGate:
    """A host/process-wide request-start gate backed by flock + a timestamp file.

    The spacing and jitter values are local niceness controls, not claims about a
    provider's published rate limits.  Injectable clock/sleep/random functions
    keep unit tests deterministic.
    """

    provider: str
    state_dir: Path
    spacing_seconds: float
    jitter_seconds: float = 0.0
    clock: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep
    uniform: Callable[[float, float], float] = random.uniform
    _thread_key: str = field(init=False)

    def __post_init__(self) -> None:
        gate_dir = Path(self.state_dir) / "provider-request-gates"
        self._thread_key = str((gate_dir / f"{self.provider}.lock").resolve())

    @property
    def lock_path(self) -> Path:
        return Path(self._thread_key)

    @property
    def state_path(self) -> Path:
        return self.lock_path.with_suffix(".json")

    def _target_gap(self) -> float:
        spacing = max(0.0, float(self.spacing_seconds))
        jitter = max(0.0, float(self.jitter_seconds))
        if jitter <= 0:
            return spacing
        return max(0.0, self.uniform(max(0.0, spacing - jitter), spacing + jitter))

    def wait_turn(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with _thread_lock(self._thread_key):
            with self.lock_path.open("a+", encoding="utf-8") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    last_started_at: float | None = None
                    try:
                        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
                        value = payload.get("last_request_started_at")
                        if isinstance(value, (int, float)):
                            last_started_at = float(value)
                    except (FileNotFoundError, OSError, ValueError, TypeError):
                        pass
                    now = self.clock()
                    if last_started_at is not None:
                        remaining = self._target_gap() - max(0.0, now - last_started_at)
                        if remaining > 0:
                            self.sleep(remaining)
                            now = self.clock()
                    tmp = self.state_path.with_name(f".{self.state_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
                    tmp.write_text(json.dumps({"provider": self.provider, "last_request_started_at": now}) + "\n", encoding="utf-8")
                    os.replace(tmp, self.state_path)
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def retry_after_seconds(value: str | None, *, now: Callable[[], float] = time.time) -> float | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, parsed.timestamp() - now())


def retry_delay_seconds(
    attempt: int,
    *,
    retry_after: str | None = None,
    base_seconds: float = 1.0,
    jitter_seconds: float = 0.25,
    cap_seconds: float = 60.0,
    uniform: Callable[[float, float], float] = random.uniform,
    now: Callable[[], float] = time.time,
) -> float:
    cap = max(0.0, float(cap_seconds))
    header_delay = retry_after_seconds(retry_after, now=now)
    if header_delay is not None:
        return min(cap, header_delay)
    base = max(0.0, float(base_seconds)) * (2 ** max(0, int(attempt) - 1))
    jitter = max(0.0, float(jitter_seconds))
    return min(cap, max(0.0, base + (uniform(0.0, jitter) if jitter else 0.0)))


def response_retry_after(response: object) -> str | None:
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    return getter("Retry-After") if callable(getter) else None
