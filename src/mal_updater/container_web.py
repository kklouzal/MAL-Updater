"""Trusted-LAN container control-plane state."""
from __future__ import annotations

import json, os, secrets, threading, time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

from .auth import write_secret_file
from .config import AppConfig, load_mal_secrets
from .persistence import atomic_write_json, atomic_write_text

MAX_BODY = 64 * 1024


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text("utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


class RateLimiter:
    def __init__(self, limit: int = 6, window: float = 300):
        self.limit, self.window, self._hits, self._lock = limit, window, {}, threading.Lock()
    def allow(self, key: str, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        with self._lock:
            hits = [x for x in self._hits.get(key, []) if x > now - self.window]
            if len(hits) >= self.limit:
                self._hits[key] = hits
                return False
            hits.append(now); self._hits[key] = hits
            return True
    def clear(self, key: str) -> None:
        with self._lock:
            self._hits.pop(key, None)

class ControlStore:
    SECRET_NAMES = {
        "mal_client_id": "mal_client_id.txt", "mal_client_secret": "mal_client_secret.txt",
        "crunchyroll_username": "crunchyroll_username.txt", "crunchyroll_password": "crunchyroll_password.txt",
        "hidive_username": "hidive_username.txt", "hidive_password": "hidive_password.txt",
    }
    def __init__(self, config: AppConfig):
        self.config = config
        self.state_path = config.state_dir / "container-control.json"
        self.audit_path = config.state_dir / "container-audit.jsonl"
        # A process-local synchronizer token protects credential-free mutations.
        # It is readable only through same-origin fetch under the browser SOP and
        # is intentionally neither a credential nor persisted installation state.
        self.csrf_token = secrets.token_urlsafe(32)
        self.oauth: dict[str, dict[str, Any]] = {}
        self.rate = RateLimiter()
        self.lock = threading.RLock()
    def status(self) -> dict[str, Any]:
        with self.lock:
            state = _read_json(self.state_path)
        present = {k: (self.config.secrets_dir / v).is_file() for k, v in self.SECRET_NAMES.items()}
        mal = load_mal_secrets(self.config)
        complete = bool(mal.client_id) and bool(mal.access_token and mal.refresh_token)
        blockers = []
        if not mal.client_id:
            blockers.append("mal_client_id")
        if not (mal.access_token and mal.refresh_token):
            blockers.append("mal_oauth_tokens")
        return {
            "setup_complete": complete,
            "automation_desired": True,
            "automation_prerequisites_satisfied": complete,
            "automation_state": "ready" if complete else "blocked",
            "automation_blockers": blockers,
            "mal_oauth_complete": bool(mal.access_token and mal.refresh_token),
            "secrets_present": present,
            "providers": {
                "crunchyroll_enabled": bool(state.get("crunchyroll_enabled")),
                "hidive_enabled": bool(state.get("hidive_enabled")),
            },
            "write_posture": "conservative; onboarding does not approve MAL writes",
        }
    def save_settings(self, data: dict[str, Any]) -> None:
        allowed = {"crunchyroll_enabled", "hidive_enabled", "sync_every_seconds", "health_every_seconds"}
        if set(data) - allowed: raise ValueError("unknown setting")
        with self.lock:
            state = _read_json(self.state_path)
            for key in ("crunchyroll_enabled", "hidive_enabled"):
                if key in data and not isinstance(data[key], bool): raise ValueError("invalid setting")
            for key in ("sync_every_seconds", "health_every_seconds"):
                if key in data and (isinstance(data[key], bool) or not isinstance(data[key], int) or not 60 <= data[key] <= 2592000): raise ValueError("invalid setting")
            state.update(data)
            atomic_write_json(self.state_path, state, mode=0o600)
            lines = ["[service]"]
            for key in ("sync_every_seconds", "health_every_seconds"):
                if key in state: lines.append(f"{key} = {state[key]}")
            atomic_write_text(self.config.settings_path, "\n".join(lines) + "\n", mode=0o600)
        self.audit("settings_updated", fields=sorted(data))
    def save_secrets(self, data: dict[str, Any], remove: list[str] | None = None) -> None:
        if not isinstance(data, dict) or not isinstance(remove or [], list) or set(data) - set(self.SECRET_NAMES) or set(remove or []) - set(self.SECRET_NAMES): raise ValueError("unknown secret")
        if set(data) & set(remove or []): raise ValueError("secret cannot be replaced and removed together")
        with self.lock:
            for name, value in data.items():
                if not isinstance(value, str) or not value.strip() or len(value.encode()) > 4096: raise ValueError("invalid secret")
                write_secret_file(self.config.secrets_dir / self.SECRET_NAMES[name], value)
            for name in remove or []:
                (self.config.secrets_dir / self.SECRET_NAMES[name]).unlink(missing_ok=True)
        self.audit("secrets_changed", replaced=sorted(data), removed=sorted(remove or []))
    def begin_oauth(self, redirect_uri: str) -> dict[str, str]:
        parsed = urlparse(redirect_uri)
        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password or parsed.path != "/oauth/mal/callback" or parsed.query or parsed.fragment:
            raise ValueError("invalid OAuth redirect")
        mal = load_mal_secrets(self.config)
        if not mal.client_id: raise ValueError("MAL client ID required")
        state, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(64)[:96]
        with self.lock:
            now = time.monotonic()
            self.oauth = {k: v for k, v in self.oauth.items() if v["expires"] >= now}
            self.oauth[state] = {"verifier": verifier, "expires": now + 600, "redirect_uri": redirect_uri}
        query = {"response_type": "code", "client_id": mal.client_id, "redirect_uri": redirect_uri, "code_challenge": verifier, "code_challenge_method": "plain", "state": state}
        return {"authorization_url": f"{self.config.mal.auth_url}?{urlencode(query)}"}
    def consume_oauth(self, state: str) -> dict[str, Any]:
        with self.lock: item = self.oauth.pop(state, None)
        if not item or item["expires"] < time.monotonic(): raise ValueError("invalid OAuth state")
        return item
    def audit(self, event: str, **fields: Any) -> None:
        safe = {"time": int(time.time()), "event": event, **fields}
        line = json.dumps(safe, separators=(",", ":")) + "\n"
        with self.lock:
            self.audit_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(self.audit_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "a", encoding="utf-8") as f: f.write(line)
            os.chmod(self.audit_path, 0o600)
