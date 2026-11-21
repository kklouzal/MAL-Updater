from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from curl_cffi import requests as curl_requests
except ModuleNotFoundError:  # pragma: no cover - dependency/install health check covers broken environments
    curl_requests = None

from .auth import write_secret_file
from .persistence import atomic_write_json
from .auth_utils import current_utc_timestamp_z, login_session_timestamps
from .config import AppConfig, _read_secret_file, _resolve_secret_path
from .request_tracking import record_api_request_event
from .provider_niceness import ProviderRequestGate, response_retry_after, retry_delay_seconds

CRUNCHYROLL_TOKEN_URL = "https://www.crunchyroll.com/auth/v1/token"
CRUNCHYROLL_ME_URL = "https://www.crunchyroll.com/accounts/v1/me"
CRUNCHYROLL_DEVICE_CODE_URL = "https://www.crunchyroll.com/auth/v1/device/code"
CRUNCHYROLL_DEVICE_TOKEN_URL = "https://www.crunchyroll.com/auth/v1/device/token"
CRUNCHYROLL_ACTIVATE_URL = "https://www.crunchyroll.com/activate"
CRUNCHYROLL_DEVICE_NAME = "MAL-Updater"
# Android TV OAuth client (client_id er9ngg2ko5ohirla51i2). Crunchyroll retires
# app clients periodically; a retired client fails every grant with
# invalid_client / auth.obtain_access_token.client_inactive, and refresh tokens
# issued to the old client cannot be reused, so rotation needs a credential login.
CRUNCHYROLL_BASIC_AUTH_TOKEN = "ZXI5bmdnMmtvNW9oaXJsYTUxaTI6VmwxVDBQbjhRWko2d3o0NnhYRGxIekRGUDV0Wlgxb3E="
# Token requests must identify as the Android TV app: with the browser User-Agent
# that curl_cffi's Chrome impersonation sends, Crunchyroll answers the password
# grant with unsupported_grant_type (observed 2026-11-18).
CRUNCHYROLL_ANDROIDTV_USER_AGENT = "Crunchyroll/ANDROIDTV/3.76.0_22366 (Android 12; en-US; SHIELD Android TV Build/SR1A.220624.014)"
DEFAULT_CRUNCHYROLL_USERNAME_FILE = "crunchyroll_username.txt"
DEFAULT_CRUNCHYROLL_PASSWORD_FILE = "crunchyroll_password.txt"
DEFAULT_CRUNCHYROLL_DEVICE_TYPE = "ANDROIDTV"


class CrunchyrollAuthError(RuntimeError):
    pass


def crunchyroll_token_error_message(prefix: str, response: Any) -> str:
    """Describe a failed token-endpoint response, keeping Crunchyroll's specific error code."""
    message = f"{prefix}: HTTP {response.status_code}"
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        error = payload.get("error")
        error_description = payload.get("error_description")
        code = payload.get("code")
        if error or error_description:
            message = f"{prefix}: {error or 'unknown_error'} - {error_description or 'no description'}"
        if code:
            message += f" ({code})"
    return message


def _require_curl_requests():
    if curl_requests is None:
        raise CrunchyrollAuthError(
            "Crunchyroll requires curl_cffi browser-TLS transport; install project dependencies with `python3 -m pip install -e .`."
        )
    return curl_requests


@dataclass(slots=True)
class CrunchyrollCredentials:
    username: str | None
    password: str | None
    username_path: Path
    password_path: Path


@dataclass(slots=True)
class CrunchyrollStatePaths:
    root: Path
    refresh_token_path: Path
    device_id_path: Path
    session_state_path: Path
    sync_boundary_path: Path


@dataclass(slots=True)
class CrunchyrollBootstrapResult:
    profile: str
    locale: str
    username_path: Path
    password_path: Path
    refresh_token_path: Path
    device_id_path: Path
    session_state_path: Path
    account_id: str | None
    account_email: str | None
    access_token: str
    refresh_token: str
    device_id: str
    device_type: str


def _now_string() -> str:
    return current_utc_timestamp_z()


def resolve_crunchyroll_state_paths(config: AppConfig, profile: str = "default") -> CrunchyrollStatePaths:
    root = config.state_dir / "crunchyroll" / profile
    return CrunchyrollStatePaths(
        root=root,
        refresh_token_path=root / "refresh_token.txt",
        device_id_path=root / "device_id.txt",
        session_state_path=root / "session.json",
        sync_boundary_path=root / "sync_boundary.json",
    )


def load_crunchyroll_credentials(config: AppConfig) -> CrunchyrollCredentials:
    secret_files = config.secret_files
    username_path = _resolve_secret_path(
        "MAL_UPDATER_CRUNCHYROLL_USERNAME_FILE",
        secret_files,
        "crunchyroll_username",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_CRUNCHYROLL_USERNAME_FILE,
    )
    password_path = _resolve_secret_path(
        "MAL_UPDATER_CRUNCHYROLL_PASSWORD_FILE",
        secret_files,
        "crunchyroll_password",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_CRUNCHYROLL_PASSWORD_FILE,
    )
    return CrunchyrollCredentials(
        username=os.getenv("MAL_UPDATER_CRUNCHYROLL_USERNAME") or _read_secret_file(username_path),
        password=os.getenv("MAL_UPDATER_CRUNCHYROLL_PASSWORD") or _read_secret_file(password_path),
        username_path=username_path,
        password_path=password_path,
    )


def _write_session_state(
    *,
    state_paths: CrunchyrollStatePaths,
    profile: str,
    locale: str,
    device_type: str,
    account_id: str | None,
    last_error: str | None,
    success: bool,
    phase: str | None = None,
) -> None:
    state_paths.root.mkdir(parents=True, exist_ok=True)
    payload = {
        "profile": profile,
        "locale": locale,
        "refresh_token_present": state_paths.refresh_token_path.exists(),
        "device_id_present": state_paths.device_id_path.exists(),
        "device_type_hint": device_type,
        **login_session_timestamps(success, now_string=_now_string),
        "last_account_id_hint": account_id,
        "last_error": last_error,
        "crunchyroll_phase": phase or ("ready" if success else "auth_failed"),
    }
    atomic_write_json(state_paths.session_state_path, payload, indent=2)


def _request_pacer(config: AppConfig | None, pacer: Any | None) -> Any | None:
    if pacer is not None or config is None:
        return pacer
    return ProviderRequestGate(
        provider="crunchyroll",
        state_dir=config.state_dir,
        spacing_seconds=config.crunchyroll.request_spacing_seconds,
        jitter_seconds=config.crunchyroll.request_spacing_jitter_seconds,
    )


class _UnpacedRequests:
    """Pacer for interactive account linking: a person is waiting on each request,
    the volume is bounded by the device-code lifetime, and each request is still
    recorded in the API request events that feed the hourly budgets."""

    def wait_turn(self) -> None:
        return None


def _http_post(
    url: str,
    *,
    data: dict[str, str] | None = None,
    json_body: dict[str, str] | None = None,
    headers: dict[str, str],
    timeout_seconds: float,
    config: AppConfig | None = None,
    pacer: Any | None = None,
):
    transport = _require_curl_requests()
    active_pacer = _request_pacer(config, pacer)
    # Credential and refresh-token POSTs are single-attempt: the server may
    # consume or rotate auth material even when the response is ambiguous.
    attempts = 1
    for attempt in range(1, attempts + 1):
        if active_pacer is not None:
            active_pacer.wait_turn()
        try:
            if json_body is not None:
                response = transport.post(url, json=json_body, headers=headers, timeout=timeout_seconds, impersonate="chrome124")
            else:
                response = transport.post(url, data=data, headers=headers, timeout=timeout_seconds, impersonate="chrome124")
        except Exception as exc:
            record_api_request_event("crunchyroll", "auth_post", url=url, method="POST", outcome="request_error", error=type(exc).__name__, config=config)
            raise
        record_api_request_event("crunchyroll", "auth_post", url=url, method="POST", outcome="ok" if response.status_code < 400 else "http_error", status_code=response.status_code, config=config)
        return response
    raise CrunchyrollAuthError("Crunchyroll auth POST exhausted retries")


def _http_get(url: str, *, headers: dict[str, str], timeout_seconds: float, config: AppConfig | None = None, pacer: Any | None = None):
    transport = _require_curl_requests()
    active_pacer = _request_pacer(config, pacer)
    attempts = max(1, int(config.crunchyroll.retry_max_attempts)) if config is not None else 1
    for attempt in range(1, attempts + 1):
        if active_pacer is not None:
            active_pacer.wait_turn()
        try:
            response = transport.get(url, headers=headers, timeout=timeout_seconds, impersonate="chrome124")
        except Exception as exc:
            record_api_request_event("crunchyroll", "auth_get", url=url, method="GET", outcome="request_error", error=type(exc).__name__, config=config)
            if config is not None and attempt < attempts and ("timeout" in type(exc).__name__.lower() or "connection" in type(exc).__name__.lower()):
                time.sleep(retry_delay_seconds(attempt, base_seconds=config.crunchyroll.retry_backoff_base_seconds, jitter_seconds=config.crunchyroll.retry_backoff_jitter_seconds, cap_seconds=config.crunchyroll.retry_after_cap_seconds))
                continue
            raise
        record_api_request_event("crunchyroll", "auth_get", url=url, method="GET", outcome="ok" if response.status_code < 400 else "http_error", status_code=response.status_code, config=config)
        if config is not None and response.status_code in {429, 500, 502, 503, 504} and attempt < attempts:
            time.sleep(retry_delay_seconds(attempt, retry_after=response_retry_after(response), base_seconds=config.crunchyroll.retry_backoff_base_seconds, jitter_seconds=config.crunchyroll.retry_backoff_jitter_seconds, cap_seconds=config.crunchyroll.retry_after_cap_seconds))
            continue
        return response
    raise CrunchyrollAuthError("Crunchyroll auth GET exhausted retries")


def crunchyroll_login_with_credentials(
    config: AppConfig,
    *,
    profile: str = "default",
    timeout_seconds: float = 30.0,
    verify_account: bool = True,
    pacer: Any | None = None,
) -> CrunchyrollBootstrapResult:
    credentials = load_crunchyroll_credentials(config)
    if not credentials.username:
        raise CrunchyrollAuthError(f"Missing Crunchyroll username/email at {credentials.username_path}")
    if not credentials.password:
        raise CrunchyrollAuthError(f"Missing Crunchyroll password at {credentials.password_path}")

    state_paths = resolve_crunchyroll_state_paths(config, profile=profile)
    existing_device_id = _read_secret_file(state_paths.device_id_path)
    device_id = existing_device_id or str(uuid.uuid4())
    device_type = DEFAULT_CRUNCHYROLL_DEVICE_TYPE

    body = {
        "username": credentials.username,
        "password": credentials.password,
        "grant_type": "password",
        "scope": "offline_access",
        "device_id": device_id,
        "device_type": device_type,
    }
    headers = crunchyroll_token_headers(device_id)

    response = _http_post(CRUNCHYROLL_TOKEN_URL, data=body, headers=headers, timeout_seconds=timeout_seconds, config=config, pacer=pacer)
    if response.status_code >= 400:
        message = crunchyroll_token_error_message("Crunchyroll credential login failed", response)
        _write_session_state(
            state_paths=state_paths,
            profile=profile,
            locale=config.crunchyroll.locale,
            device_type=device_type,
            account_id=None,
            last_error=message,
            success=False,
        )
        raise CrunchyrollAuthError(message)

    return _complete_token_login(
        config,
        response.json(),
        state_paths=state_paths,
        profile=profile,
        device_id=device_id,
        device_type=device_type,
        credentials=credentials,
        login_label="credential login",
        verify_account=verify_account,
        timeout_seconds=timeout_seconds,
        pacer=pacer,
    )


def _complete_token_login(
    config: AppConfig,
    token_payload: Any,
    *,
    state_paths: CrunchyrollStatePaths,
    profile: str,
    device_id: str,
    device_type: str,
    credentials: CrunchyrollCredentials,
    login_label: str,
    verify_account: bool,
    timeout_seconds: float,
    pacer: Any | None,
) -> CrunchyrollBootstrapResult:
    """Validate a successful token response, optionally verify the account, then persist the session."""
    if not isinstance(token_payload, dict):
        token_payload = {}
    access_token = token_payload.get("access_token")
    refresh_token = token_payload.get("refresh_token")
    account_id = token_payload.get("account_id")
    if not access_token or not refresh_token:
        message = f"Crunchyroll {login_label} succeeded but did not return both access_token and refresh_token"
        _write_session_state(
            state_paths=state_paths,
            profile=profile,
            locale=config.crunchyroll.locale,
            device_type=device_type,
            account_id=account_id,
            last_error=message,
            success=False,
        )
        raise CrunchyrollAuthError(message)

    account_email = None
    if verify_account:
        me_response = _http_get(
            CRUNCHYROLL_ME_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout_seconds=timeout_seconds,
            config=config,
            pacer=pacer,
        )
        if me_response.status_code >= 400:
            message = f"Crunchyroll /accounts/v1/me verification failed: HTTP {me_response.status_code}"
            _write_session_state(
                state_paths=state_paths,
                profile=profile,
                locale=config.crunchyroll.locale,
                device_type=device_type,
                account_id=account_id,
                last_error=message,
                success=False,
            )
            raise CrunchyrollAuthError(message)
        me_payload = me_response.json()
        if isinstance(me_payload, dict):
            account_id = str(me_payload.get("account_id") or account_id or "") or None
            account_email = me_payload.get("email")

    write_secret_file(state_paths.refresh_token_path, refresh_token)
    write_secret_file(state_paths.device_id_path, device_id)
    _write_session_state(
        state_paths=state_paths,
        profile=profile,
        locale=config.crunchyroll.locale,
        device_type=device_type,
        account_id=account_id,
        last_error=None,
        success=True,
    )

    return CrunchyrollBootstrapResult(
        profile=profile,
        locale=config.crunchyroll.locale,
        username_path=credentials.username_path,
        password_path=credentials.password_path,
        refresh_token_path=state_paths.refresh_token_path,
        device_id_path=state_paths.device_id_path,
        session_state_path=state_paths.session_state_path,
        account_id=account_id,
        account_email=account_email,
        access_token=str(access_token),
        refresh_token=str(refresh_token),
        device_id=device_id,
        device_type=device_type,
    )


@dataclass(frozen=True, slots=True)
class CrunchyrollDeviceCode:
    """A pending TV-style account link: the user approves ``user_code`` at ``verification_url``."""

    user_code: str
    device_code: str
    device_id: str
    expires_in_seconds: int
    verification_url: str


def crunchyroll_token_headers(device_id: str, content_type: str = "application/x-www-form-urlencoded") -> dict[str, str]:
    """Headers for Crunchyroll's Android TV client token and device-code endpoints."""
    return {
        "Authorization": f"Basic {CRUNCHYROLL_BASIC_AUTH_TOKEN}",
        "Content-Type": content_type,
        "ETP-Anonymous-ID": device_id,
        "User-Agent": CRUNCHYROLL_ANDROIDTV_USER_AGENT,
    }


def crunchyroll_request_device_code(
    config: AppConfig,
    *,
    profile: str = "default",
    timeout_seconds: float = 15.0,
) -> CrunchyrollDeviceCode:
    """Start a password-free account link, like the Crunchyroll TV apps' activation codes.

    Nothing is persisted until the link is approved and polled successfully, so an
    abandoned or expired code leaves the current session untouched.
    """
    state_paths = resolve_crunchyroll_state_paths(config, profile=profile)
    device_id = _read_secret_file(state_paths.device_id_path) or str(uuid.uuid4())
    response = _http_post(
        CRUNCHYROLL_DEVICE_CODE_URL,
        data={"device_id": device_id, "device_type": DEFAULT_CRUNCHYROLL_DEVICE_TYPE, "device_name": CRUNCHYROLL_DEVICE_NAME},
        headers=crunchyroll_token_headers(device_id, "application/x-www-form-urlencoded"),
        timeout_seconds=timeout_seconds,
        config=config,
        pacer=_UnpacedRequests(),
    )
    if response.status_code >= 400:
        raise CrunchyrollAuthError(crunchyroll_token_error_message("Crunchyroll account link could not start", response))
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if not isinstance(payload, dict) or not payload.get("user_code") or not payload.get("device_code"):
        raise CrunchyrollAuthError("Crunchyroll account link could not start: response did not include a code")
    try:
        expires_in = int(payload.get("expires_in") or 300)
    except (TypeError, ValueError):
        expires_in = 300
    return CrunchyrollDeviceCode(
        user_code=str(payload["user_code"]),
        device_code=str(payload["device_code"]),
        device_id=device_id,
        expires_in_seconds=max(1, expires_in),
        verification_url=CRUNCHYROLL_ACTIVATE_URL,
    )


def crunchyroll_poll_device_code(
    config: AppConfig,
    code: CrunchyrollDeviceCode,
    *,
    profile: str = "default",
    timeout_seconds: float = 15.0,
) -> CrunchyrollBootstrapResult | None:
    """Poll once for approval of ``code``. Returns None while approval is still pending.

    Crunchyroll answers 204 (or 200 with an empty body) until the code is approved,
    then 200 with a normal token set bound to the Android TV client, which the
    regular refresh-token renewal keeps alive afterwards.
    """
    response = _http_post(
        CRUNCHYROLL_DEVICE_TOKEN_URL,
        json_body={"device_id": code.device_id, "device_code": code.device_code},
        headers=crunchyroll_token_headers(code.device_id, "application/json"),
        timeout_seconds=timeout_seconds,
        config=config,
        pacer=_UnpacedRequests(),
    )
    if response.status_code in {204, 428}:
        return None
    if response.status_code >= 400:
        raise CrunchyrollAuthError(crunchyroll_token_error_message("Crunchyroll account link failed", response))
    try:
        payload = response.json() if (response.content or b"").strip() else None
    except ValueError:
        payload = None
    if not isinstance(payload, dict) or not payload.get("access_token"):
        return None
    return _complete_token_login(
        config,
        payload,
        state_paths=resolve_crunchyroll_state_paths(config, profile=profile),
        profile=profile,
        device_id=code.device_id,
        device_type=DEFAULT_CRUNCHYROLL_DEVICE_TYPE,
        credentials=load_crunchyroll_credentials(config),
        login_label="account link",
        verify_account=True,
        timeout_seconds=timeout_seconds,
        pacer=_UnpacedRequests(),
    )


def crunchyroll_auth_material_present(config: AppConfig, *, profile: str = "default") -> bool:
    """True when unattended Crunchyroll auth is possible: a linked session or saved credentials."""
    credentials = load_crunchyroll_credentials(config)
    if credentials.username and credentials.password:
        return True
    return bool(_read_secret_file(resolve_crunchyroll_state_paths(config, profile=profile).refresh_token_path))
