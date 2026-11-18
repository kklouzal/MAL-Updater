from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import AppConfig, MalSecrets, ensure_secret_directory, mal_callback_bind_warning
from .mal_client import (
    MalClient,
    TokenResponse,
    clear_mal_access_token_state,
    mal_token_expires_at,
    read_mal_access_token_expiry,
    write_mal_access_token_state,
)


class OAuthCallbackError(RuntimeError):
    pass


@dataclass(slots=True)
class OAuthCallbackResult:
    code: str
    state: str | None
    query: dict[str, list[str]]


@dataclass(slots=True)
class PersistedTokenPaths:
    access_token_path: Path
    refresh_token_path: Path


class _SingleCallbackServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], expected_state: str | None):
        super().__init__(server_address, _CallbackHandler)
        self.expected_state = expected_state
        self.result: OAuthCallbackResult | None = None
        self.error: str | None = None
        self.done = threading.Event()


class _CallbackHandler(BaseHTTPRequestHandler):
    server: _SingleCallbackServer

    def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover - noisy stdlib hook
        return

    def do_GET(self) -> None:  # noqa: N802 - stdlib interface
        parsed = urlparse(self.path)
        if parsed.path != "/callback":
            self._send_text(404, "Not found")
            return

        query = parse_qs(parsed.query, keep_blank_values=True)
        state = query.get("state", [None])[0]
        error = query.get("error", [None])[0]
        code = query.get("code", [None])[0]

        if self.server.expected_state and state != self.server.expected_state:
            self.server.error = "OAuth callback state mismatch"
            self._send_html(400, "<h1>MAL auth failed</h1><p>State mismatch. Return to the terminal.</p>")
            self.server.done.set()
            return

        if error:
            description = query.get("error_description", [""])[0]
            self.server.error = f"OAuth callback returned error={error}: {description}".strip()
            self._send_html(400, "<h1>MAL auth failed</h1><p>The authorization page returned an error. Return to the terminal.</p>")
            self.server.done.set()
            return

        if not code:
            self.server.error = "OAuth callback did not include a code"
            self._send_html(400, "<h1>MAL auth failed</h1><p>No authorization code was returned. Return to the terminal.</p>")
            self.server.done.set()
            return

        self.server.result = OAuthCallbackResult(code=code, state=state, query=query)
        self._send_html(200, "<h1>MAL auth complete</h1><p>You can close this tab and return to the terminal.</p>")
        self.server.done.set()

    def _send_text(self, status: int, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_html(self, status: int, body: str) -> None:
        encoded = f"<!doctype html><html><body>{body}</body></html>".encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def write_secret_file(path: Path, value: str) -> None:
    """Durably replace one private secret file (fsynced temp file + atomic rename)."""
    ensure_secret_directory(path.parent)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        temp_path = Path(tmp.name)
        try:
            tmp.write(value.strip() + "\n")
            tmp.flush()
            os.fsync(tmp.fileno())
        except BaseException:
            tmp.close()
            temp_path.unlink(missing_ok=True)
            raise
    try:
        os.chmod(temp_path, 0o600)
        temp_path.replace(path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    os.chmod(path, 0o600)
    dir_fd = os.open(str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def persist_token_response(
    token: TokenResponse, secrets: MalSecrets, *, obtained_at: datetime | None = None,
) -> PersistedTokenPaths:
    """Persist a MAL token grant so that no crash point strands a spent refresh token.

    MAL rotates the refresh token on every refresh grant, so the new refresh
    token is made durable first. The expiry record is dropped before and
    rewritten after both tokens, and it is bound to the access token by digest,
    so any interrupted sequence reads as "expiry unknown" and the next
    mal-refresh run renews instead of skipping.
    """
    obtained = obtained_at or datetime.now(timezone.utc)
    clear_mal_access_token_state(secrets)
    if token.refresh_token:
        write_secret_file(secrets.refresh_token_path, token.refresh_token)
    write_secret_file(secrets.access_token_path, token.access_token)
    write_mal_access_token_state(secrets, token, obtained_at=obtained)
    return PersistedTokenPaths(
        access_token_path=secrets.access_token_path,
        refresh_token_path=secrets.refresh_token_path,
    )


# MAL's documented grant returns ~31-day access tokens. Renewing a week before
# expiry keeps a wide margin for downtime and for refresh-token lifetimes that
# MAL does not document, while avoiding a refresh-token rotation every hour.
DEFAULT_MAL_TOKEN_REFRESH_MARGIN_SECONDS = 7 * 24 * 60 * 60


@dataclass(slots=True)
class MalTokenRefreshOutcome:
    refreshed: bool
    reason: str
    access_token_expires_at: datetime | None
    token: TokenResponse | None = None
    persisted: PersistedTokenPaths | None = None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": "ok",
            "refreshed": self.refreshed,
            "reason": self.reason,
            "access_token_expires_at": None
            if self.access_token_expires_at is None
            else self.access_token_expires_at.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }
        if self.persisted is not None:
            payload["access_token_path"] = str(self.persisted.access_token_path)
            payload["refresh_token_path"] = str(self.persisted.refresh_token_path)
        return payload


def refresh_mal_tokens_if_due(
    client: MalClient,
    secrets: MalSecrets,
    *,
    force: bool = False,
    min_remaining_seconds: int = DEFAULT_MAL_TOKEN_REFRESH_MARGIN_SECONDS,
    now: datetime | None = None,
) -> MalTokenRefreshOutcome:
    """Refresh only when forced, when expiry is unknown, or when it is within the margin.

    Expiry is unknown for tokens persisted before expiry recording existed, for
    grants without ``expires_in``, after an interrupted persist, and after any
    MAL API 401 for the current bearer token (MalClient clears the record).
    Raises MalApiError when a due refresh fails.
    """
    current = now or datetime.now(timezone.utc)
    expires_at = read_mal_access_token_expiry(secrets)
    if not force and expires_at is not None:
        remaining = (expires_at - current).total_seconds()
        if remaining > max(0, int(min_remaining_seconds)):
            return MalTokenRefreshOutcome(False, "access_token_not_near_expiry", expires_at)
    reason = "forced" if force else ("expiry_unknown" if expires_at is None else "access_token_near_expiry")
    token = client.refresh_access_token()
    persisted = persist_token_response(token, secrets, obtained_at=current)
    new_expiry = mal_token_expires_at(token, obtained_at=current)
    return MalTokenRefreshOutcome(True, reason, new_expiry, token=token, persisted=persisted)


def wait_for_oauth_callback(host: str, port: int, expected_state: str | None, timeout_seconds: float = 300.0) -> OAuthCallbackResult:
    server = _SingleCallbackServer((host, port), expected_state=expected_state)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
    thread.start()
    try:
        if not server.done.wait(timeout_seconds):
            raise TimeoutError(f"Timed out waiting for MAL OAuth callback after {timeout_seconds:.0f}s")
        if server.error:
            raise OAuthCallbackError(server.error)
        if server.result is None:
            raise OAuthCallbackError("OAuth callback finished without a result")
        return server.result
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1.0)


def format_auth_flow_prompt(config: AppConfig, authorization_url: str, timeout_seconds: float) -> str:
    lines = [
        "Starting local MAL OAuth callback listener.",
        f"bind_host={config.mal.bind_host}",
        f"redirect_uri={config.mal.redirect_uri}",
        f"timeout_seconds={int(timeout_seconds)}",
    ]
    warning = mal_callback_bind_warning(config.mal)
    if warning:
        lines.extend(["", f"WARNING: {warning}"])
    lines.extend(
        [
            "",
            "Open this URL in a browser that can reach the redirect_uri host:",
            authorization_url,
            "",
            "Waiting for a single /callback hit...",
        ]
    )
    return "\n".join(lines)
