"""Authenticated, deny-by-default container control plane state."""
from __future__ import annotations

import hashlib, hmac, ipaddress, json, os, secrets, threading, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

from .auth import write_secret_file
from .config import AppConfig, load_mal_secrets
from .persistence import atomic_write_json, atomic_write_text

MAX_BODY = 64 * 1024
PASSWORD_MIN = 12
PASSWORD_MAX_BYTES = 1024
SESSION_TTL = 12 * 3600
SCRYPT_N, SCRYPT_R, SCRYPT_P = 2**14, 8, 1


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text("utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _validate_password(password: str) -> bytes:
    raw = password.encode("utf-8")
    if len(raw) > PASSWORD_MAX_BYTES:
        raise ValueError("password too long")
    if len(password) < PASSWORD_MIN or password.lower() == password or password.upper() == password or not any(c.isdigit() for c in password):
        raise ValueError("password must be at least 12 characters with upper/lower case and a digit")
    return raw


def hash_password(password: str, *, salt: bytes | None = None) -> str:
    raw = _validate_password(password)
    salt = salt or os.urandom(16)
    digest = hashlib.scrypt(raw, salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P, dklen=32)
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        raw = password.encode("utf-8")
        if len(raw) > PASSWORD_MAX_BYTES:
            return False
        kind, n, r, p, salt_hex, expected_hex = encoded.split("$")
        n_i, r_i, p_i = int(n), int(r), int(p)
        expected = bytes.fromhex(expected_hex)
        salt = bytes.fromhex(salt_hex)
        if kind != "scrypt" or (n_i, r_i, p_i) != (SCRYPT_N, SCRYPT_R, SCRYPT_P) or len(salt) != 16 or len(expected) != 32:
            return False
        actual = hashlib.scrypt(raw, salt=salt, n=n_i, r=r_i, p=p_i, dklen=32)
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError, UnicodeError):
        return False


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


@dataclass
class Session:
    csrf: str
    expires: float


class ControlStore:
    SECRET_NAMES = {
        "mal_client_id": "mal_client_id.txt", "mal_client_secret": "mal_client_secret.txt",
        "crunchyroll_username": "crunchyroll_username.txt", "crunchyroll_password": "crunchyroll_password.txt",
        "hidive_username": "hidive_username.txt", "hidive_password": "hidive_password.txt",
    }
    def __init__(self, config: AppConfig, *, setup_token: str | None = None):
        self.config = config
        self.auth_path = config.secrets_dir / "container_auth.json"
        self.state_path = config.state_dir / "container-control.json"
        self.audit_path = config.state_dir / "container-audit.jsonl"
        self.setup_token = setup_token or secrets.token_urlsafe(32)
        self.sessions: dict[str, Session] = {}
        self.oauth: dict[str, dict[str, Any]] = {}
        self.rate = RateLimiter()
        self.lock = threading.RLock()
    @property
    def claimed(self) -> bool:
        return bool(_read_json(self.auth_path).get("password_hash"))
    def claim(self, token: str, password: str) -> None:
        with self.lock:
            if self.claimed:
                raise ValueError("setup already claimed")
            if not self.setup_token or not hmac.compare_digest(token, self.setup_token):
                raise ValueError("invalid setup token")
            password_hash = hash_password(password)
            atomic_write_json(self.auth_path, {"version": 1, "password_hash": password_hash}, mode=0o600)
            self.setup_token = ""
            self.audit("setup_claimed")
    def login(self, password: str, key: str) -> tuple[str, str]:
        if not self.rate.allow(key):
            raise PermissionError("rate_limited")
        if not verify_password(password, str(_read_json(self.auth_path).get("password_hash", ""))):
            raise ValueError("invalid credentials")
        self.rate.clear(key)
        sid, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        with self.lock:
            self.sessions[sid] = Session(csrf, time.monotonic() + SESSION_TTL)
        self.audit("login")
        return sid, csrf
    def session(self, sid: str | None) -> Session | None:
        with self.lock:
            item = self.sessions.get(sid or "")
            if not item or item.expires < time.monotonic():
                self.sessions.pop(sid or "", None)
                return None
            return item
    def logout(self, sid: str) -> None:
        with self.lock: self.sessions.pop(sid, None)
        self.audit("logout")
    def change_password(self, old: str, new: str) -> None:
        with self.lock:
            data = _read_json(self.auth_path)
            if not verify_password(old, str(data.get("password_hash", ""))):
                raise ValueError("invalid credentials")
            atomic_write_json(self.auth_path, {"version": 1, "password_hash": hash_password(new)}, mode=0o600)
            self.sessions.clear()
        self.audit("password_changed")
    def status(self) -> dict[str, Any]:
        with self.lock:
            state = _read_json(self.state_path)
        present = {k: (self.config.secrets_dir / v).is_file() for k, v in self.SECRET_NAMES.items()}
        mal = load_mal_secrets(self.config)
        complete = self.claimed and bool(mal.client_id) and bool(mal.access_token and mal.refresh_token)
        return {"claimed": self.claimed, "setup_complete": complete, "daemon_enabled": bool(state.get("daemon_enabled")), "mal_oauth_complete": bool(mal.access_token and mal.refresh_token), "secrets_present": present, "providers": {"crunchyroll_enabled": bool(state.get("crunchyroll_enabled")), "hidive_enabled": bool(state.get("hidive_enabled"))}, "write_posture": "conservative; onboarding does not approve MAL writes"}
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
            if any(name.startswith("mal_") for name in remove or []):
                state = _read_json(self.state_path); state["daemon_enabled"] = False
                atomic_write_json(self.state_path, state, mode=0o600)
        self.audit("secrets_changed", replaced=sorted(data), removed=sorted(remove or []))
    def set_daemon(self, enabled: bool) -> None:
        if enabled and not self.status()["setup_complete"]: raise ValueError("setup incomplete")
        with self.lock:
            state = _read_json(self.state_path); state["daemon_enabled"] = enabled
            atomic_write_json(self.state_path, state, mode=0o600)
        self.audit("daemon_state_changed", enabled=enabled)
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
