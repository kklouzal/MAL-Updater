from __future__ import annotations
import base64, json, os, signal, subprocess, sys, threading, time
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any, Sequence
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

from .auth import persist_token_response
from .config import AppConfig, ensure_directories, load_config, load_mal_secrets
from .container_web import ControlStore, MAX_BODY, SESSION_TTL
from .container_ui import page as product_page
from .db import bootstrap_database
from .mal_client import TokenResponse
from .recommendation_dashboard import make_dashboard_handler

_TRUE_VALUES = {"1", "true", "yes", "on"}
def env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name); return default if value is None else value.strip().lower() in _TRUE_VALUES
def setup_mode(store: ControlStore | None = None) -> bool:
    return not (store.status()["daemon_enabled"] if store else env_flag("MAL_UPDATER_CONTAINER_ENABLE_DAEMON"))
def initialize_runtime(project_root: Path | None = None) -> AppConfig:
    config = load_config(project_root); ensure_directories(config); bootstrap_database(config.db_path); return config
def daemon_command(project_root: Path) -> list[str]:
    return [sys.executable, "-m", "mal_updater.cli", "--project-root", str(project_root), "service-run"]
def _status_payload(*, config: AppConfig, daemon: subprocess.Popen[bytes] | None, store: ControlStore) -> dict[str, Any]:
    state = store.status(); running = daemon is not None and daemon.poll() is None; enabled = state["daemon_enabled"]
    return {"status": "ok" if not enabled or running else "degraded", "ready": bool(state["setup_complete"] and (not enabled or running)), "mode": "operational" if enabled else "setup", "daemon_enabled": enabled, "daemon_running": running, "database_initialized": config.db_path.is_file(), "setup_complete": state["setup_complete"]}
def _page(_title: str, _body: str) -> str:
    return product_page("login")
def hmac_compare(a: str, b: str) -> bool:
    import hmac
    return hmac.compare_digest(a, b)

def _split_host(host: str) -> tuple[str, int | None]:
    if not host or any(x in host for x in "\r\n/@") or len(host) > 253: raise ValueError("invalid host")
    parsed = urlparse("//" + host)
    if parsed.username or parsed.password or not parsed.hostname: raise ValueError("invalid host")
    try: port = parsed.port
    except ValueError as exc: raise ValueError("invalid host") from exc
    return parsed.hostname.lower(), port
def valid_host(host: str) -> bool:
    import ipaddress
    try: name, _ = _split_host(host)
    except ValueError: return False
    if name == "localhost": return True
    try: address = ipaddress.ip_address(name)
    except ValueError:
        allowed = {x.strip().lower() for x in os.getenv("MAL_UPDATER_TRUSTED_HOSTS", "").split(",") if x.strip()}
        return name in allowed
    return address.is_loopback or address.is_private

def _trusted_proxy(address: str) -> bool:
    import ipaddress
    try: client = ipaddress.ip_address(address)
    except ValueError: return False
    for raw in os.getenv("MAL_UPDATER_TRUSTED_PROXY_CIDRS", "").split(","):
        try:
            if raw.strip() and client in ipaddress.ip_network(raw.strip(), strict=False): return True
        except ValueError: continue
    return False

def _oauth_exchange(config: AppConfig, code: str, verifier: str, redirect_uri: str) -> TokenResponse:
    secrets = load_mal_secrets(config)
    credentials = f"{secrets.client_id or ''}:{secrets.client_secret or ''}"
    body = urlencode({"grant_type": "authorization_code", "client_id": secrets.client_id or "", "code": code, "code_verifier": verifier, "redirect_uri": redirect_uri}).encode()
    req = Request(config.mal.token_url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json", "Authorization": "Basic " + base64.b64encode(credentials.encode()).decode()}, method="POST")
    try:
        with urlopen(req, timeout=min(config.request_timeout_seconds, 15)) as response: raw = json.loads(response.read(1024 * 1024))
    except (HTTPError, OSError, ValueError) as exc: raise ValueError("OAuth token exchange failed") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("access_token"), str): raise ValueError("OAuth token response invalid")
    return TokenResponse(raw["access_token"], str(raw.get("token_type", "Bearer")), raw.get("expires_in"), raw.get("refresh_token"), raw.get("scope"), raw)

def make_container_handler(config: AppConfig, daemon_ref: list[subprocess.Popen[bytes] | None], store: ControlStore, *, oauth_exchange: Any = None, connection_tester: Any = None) -> type:
    dashboard = make_dashboard_handler(config.db_path); secure_default = env_flag("MAL_UPDATER_COOKIE_SECURE")
    class Handler(dashboard):
        server_version = "MAL-Updater"
        sys_version = ""
        def log_message(self, format: str, *args: Any) -> None:  # avoid query-string/token logging
            print(json.dumps({"event": "http_request", "client": self.client_address[0], "method": self.command, "path": urlparse(self.path).path, "status": args[1] if len(args) > 1 else None}), flush=True)
        def _headers(self) -> None:
            self.send_header("X-Content-Type-Options", "nosniff"); self.send_header("X-Frame-Options", "DENY"); self.send_header("Referrer-Policy", "no-referrer"); self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, sort_keys=True).encode(); self.send_response(status); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Cache-Control", "no-store"); self._headers(); self.send_header("Content-Length", str(len(body))); self.end_headers()
            if self.command != "HEAD": self.wfile.write(body)
        def _send_html(self, text: str, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = text.encode(); self.send_response(status); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Cache-Control", "no-store"); self._headers(); self.send_header("Content-Length", str(len(body))); self.end_headers()
            if self.command != "HEAD": self.wfile.write(body)
        def _sid(self) -> str | None:
            try: cookie = SimpleCookie(self.headers.get("Cookie", ""))
            except Exception: return None
            morsel = cookie.get("mal_session"); return morsel.value if morsel else None
        def _session(self): return store.session(self._sid())
        def _json(self) -> dict[str, Any]:
            if self.headers.get("Transfer-Encoding"): raise ValueError("transfer encoding unsupported")
            if self.headers.get("Content-Type", "").split(";", 1)[0].lower() != "application/json": raise TypeError("content type")
            raw_length = self.headers.get("Content-Length")
            if raw_length is None: raise ValueError("content length required")
            try: length = int(raw_length)
            except ValueError as exc: raise ValueError("invalid body") from exc
            if length < 0 or length > MAX_BODY: raise OverflowError("body too large")
            value = json.loads(self.rfile.read(length));
            if not isinstance(value, dict): raise ValueError("object required")
            return value
        def _require(self, write: bool = False):
            session = self._session()
            if not session: self._send_json({"error": "authentication_required"}, HTTPStatus.UNAUTHORIZED); return None
            if write and not hmac_compare(self.headers.get("X-CSRF-Token", ""), session.csrf): self._send_json({"error": "csrf_failed"}, HTTPStatus.FORBIDDEN); return None
            return session
        def _origin(self) -> tuple[str, str]:
            host, scheme = self.headers.get("Host", ""), ("https" if secure_default else "http")
            if _trusted_proxy(self.client_address[0]):
                forwarded_host = self.headers.get("X-Forwarded-Host", "").split(",", 1)[0].strip()
                forwarded_proto = self.headers.get("X-Forwarded-Proto", "").split(",", 1)[0].strip().lower()
                if forwarded_host: host = forwarded_host
                if forwarded_proto in {"http", "https"}: scheme = forwarded_proto
            if not valid_host(host): raise ValueError("invalid host")
            return scheme, host
        def do_GET(self) -> None:
            parsed = urlparse(self.path); path = parsed.path
            if path in {"/healthz", "/readyz"}:
                payload = _status_payload(config=config, daemon=daemon_ref[0], store=store); self._send_json(payload, HTTPStatus.OK if path == "/healthz" or payload["ready"] else HTTPStatus.SERVICE_UNAVAILABLE); return
            if path == "/setup/status" and not store.claimed: self._send_json({"claimed": False, "setup_required": True}); return
            if not store.claimed:
                if path == "/": self._send_html(product_page("claim")); return
                self._send_json({"error": "setup_required"}, HTTPStatus.UNAUTHORIZED); return
            if path == "/login": self._send_html(product_page("login")); return
            if path == "/oauth/mal/callback":
                # State is the callback bearer credential; SameSite=Strict omits the admin cookie.
                q = parse_qs(parsed.query); state, code = q.get("state", [""])[0], q.get("code", [""])[0]
                try:
                    flow = store.consume_oauth(state)
                    if not code: raise ValueError("authorization code missing")
                    token = (oauth_exchange or _oauth_exchange)(config, code, flow["verifier"], flow["redirect_uri"])
                    persist_token_response(token, load_mal_secrets(config)); store.audit("mal_oauth_completed"); self._send_html(_page("MAL OAuth complete", "done")); return
                except Exception: self._send_json({"error": "oauth_failed"}, HTTPStatus.BAD_REQUEST); return
            if not self._require(): return
            if path == "/api/status": self._send_json(_status_payload(config=config, daemon=daemon_ref[0], store=store)); return
            if path == "/api/settings": self._send_json(store.status()); return
            if path in {"/", "/settings"}: self._send_html(product_page("app")); return
            self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
        def do_HEAD(self) -> None: self.do_GET()
        def do_POST(self) -> None:
            try: data = self._json()
            except TypeError: self._send_json({"error": "application_json_required"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE); return
            except OverflowError: self._send_json({"error": "body_too_large"}, HTTPStatus.REQUEST_ENTITY_TOO_LARGE); return
            except Exception: self._send_json({"error": "invalid_request"}, HTTPStatus.BAD_REQUEST); return
            path, key = urlparse(self.path).path, self.client_address[0]
            try:
                if path == "/api/setup/claim" and not store.claimed:
                    if not store.rate.allow("claim:" + key): raise PermissionError("rate_limited")
                    store.claim(str(data.get("setup_token", "")), str(data.get("password", ""))); self._send_json({"ok": True}, HTTPStatus.CREATED); return
                if path == "/api/login":
                    sid, csrf = store.login(str(data.get("password", "")), "login:" + key); scheme, _ = self._origin(); secure = secure_default or scheme == "https"
                    body = json.dumps({"ok": True, "csrf_token": csrf}).encode(); self.send_response(HTTPStatus.OK); self.send_header("Content-Type", "application/json"); self.send_header("Cache-Control", "no-store"); self.send_header("Set-Cookie", f"mal_session={sid}; Path=/; Max-Age={SESSION_TTL}; HttpOnly; SameSite=Strict" + ("; Secure" if secure else "")); self._headers(); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body); return
                if not self._require(write=True): return
                if path == "/api/logout": store.logout(self._sid() or ""); self._send_json({"ok": True}); return
                if path == "/api/password": store.change_password(str(data.get("current_password", "")), str(data.get("new_password", ""))); self._send_json({"ok": True}); return
                if path == "/api/settings": store.save_settings(data); self._send_json(store.status()); return
                if path == "/api/secrets": store.save_secrets(data.get("replace", {}), data.get("remove", [])); self._send_json(store.status()); return
                if path == "/api/daemon":
                    if set(data) != {"enabled"} or not isinstance(data["enabled"], bool): raise ValueError("invalid daemon request")
                    store.set_daemon(data["enabled"]); self._send_json(store.status()); return
                if path == "/api/connections/test":
                    kind = str(data.get("kind", ""))
                    if kind not in {"mal", "crunchyroll", "hidive"}: raise ValueError("unknown connection")
                    if not store.rate.allow("connection-test:" + key): raise PermissionError("rate_limited")
                    if connection_tester is None: raise ValueError("connection testing unavailable")
                    connection_tester(kind, timeout=10); self._send_json({"ok": True, "kind": kind, "message": "Connection succeeded"}); return
                if path == "/api/oauth/mal/start":
                    scheme, host = self._origin(); self._send_json(store.begin_oauth(f"{scheme}://{host}/oauth/mal/callback")); return
                self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
            except PermissionError: self._send_json({"error": "rate_limited"}, HTTPStatus.TOO_MANY_REQUESTS)
            except ValueError as exc: self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        def _method_not_allowed(self): self._send_json({"error": "method_not_allowed"}, HTTPStatus.METHOD_NOT_ALLOWED)
        do_PUT = _method_not_allowed; do_DELETE = _method_not_allowed; do_PATCH = _method_not_allowed; do_OPTIONS = _method_not_allowed
    return Handler

def _terminate_process(p: subprocess.Popen[bytes], timeout: float = 20) -> None:
    if p.poll() is None:
        p.terminate()
        try: p.wait(timeout=timeout)
        except subprocess.TimeoutExpired: p.kill(); p.wait(timeout=5)
def run_container(*, project_root: Path | None = None, host: str = "0.0.0.0", port: int = 8080, popen: Any = subprocess.Popen) -> int:
    config = initialize_runtime(project_root); store = ControlStore(config); daemon_ref = [None]
    print(json.dumps({"event": "first_run_setup_token", "setup_token": store.setup_token}) if not store.claimed else json.dumps({"event": "container_starting"}), flush=True)
    server = ThreadingHTTPServer((host, port), make_container_handler(config, daemon_ref, store)); stop = threading.Event()
    def supervise():
        failures, next_start = 0, 0.0
        while not stop.wait(1):
            enabled = store.status()["daemon_enabled"]
            child = daemon_ref[0]
            if child is not None and child.poll() is not None:
                failures += 1; next_start = time.monotonic() + min(60, 2 ** min(failures, 6)); daemon_ref[0] = None
            if enabled and daemon_ref[0] is None and time.monotonic() >= next_start:
                daemon_ref[0] = popen(daemon_command(config.project_root), start_new_session=False)
            if not enabled and daemon_ref[0] is not None:
                _terminate_process(daemon_ref[0]); daemon_ref[0] = None; failures = 0
    supervisor = threading.Thread(target=supervise, daemon=True); supervisor.start()
    def request_stop(*_): stop.set(); threading.Thread(target=server.shutdown, daemon=True).start()
    previous = {sig: signal.signal(sig, request_stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    try: server.serve_forever(.5)
    finally:
        stop.set(); server.server_close(); supervisor.join(timeout=2)
        if daemon_ref[0] is not None: _terminate_process(daemon_ref[0])
        for sig, handler in previous.items(): signal.signal(sig, handler)
    return 0
def main(argv: Sequence[str] | None = None) -> int:
    del argv
    try: port = int(os.getenv("MAL_UPDATER_CONTAINER_PORT", "8080"))
    except ValueError: raise SystemExit("invalid MAL_UPDATER_CONTAINER_PORT")
    return run_container(project_root=Path(os.getenv("MAL_UPDATER_CONTAINER_PROJECT_ROOT", "/app")), host=os.getenv("MAL_UPDATER_CONTAINER_HOST", "0.0.0.0"), port=port)
if __name__ == "__main__": raise SystemExit(main())
