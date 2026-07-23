from __future__ import annotations

import base64
import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any


def current_utc_timestamp_z(*, now: datetime | None = None) -> str:
    current = now if now is not None else datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    return current.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def login_session_timestamps(
    success: bool,
    *,
    now_string: Callable[[], str] = current_utc_timestamp_z,
) -> dict[str, str | None]:
    return {
        "last_login_attempt_at": now_string(),
        "last_login_success_at": now_string() if success else None,
    }


def decode_jwt_payload(token: str) -> dict[str, Any] | None:
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload_part = parts[1] + ("=" * ((4 - len(parts[1]) % 4) % 4))
    try:
        decoded = base64.urlsafe_b64decode(payload_part.encode()).decode()
        payload = json.loads(decoded)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def jwt_expiry_epoch(token: str) -> int | None:
    payload = decode_jwt_payload(token)
    exp = payload.get("exp") if isinstance(payload, dict) else None
    return int(exp) if isinstance(exp, (int, float)) else None


def seconds_until_jwt_expiry(token: str, *, now_epoch: int | None = None) -> int | None:
    exp = jwt_expiry_epoch(token)
    if exp is None:
        return None
    current = now_epoch if now_epoch is not None else int(datetime.now(timezone.utc).timestamp())
    return exp - current
