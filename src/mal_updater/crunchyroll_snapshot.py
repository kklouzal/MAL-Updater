from __future__ import annotations

import json
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from curl_cffi import requests as curl_requests
except ModuleNotFoundError:  # pragma: no cover - dependency/install health check covers broken environments
    curl_requests = None

from .auth import write_secret_file
from .persistence import atomic_write_json
from .config import AppConfig, _read_secret_file
from .request_tracking import record_api_request_event
from .provider_niceness import ProviderRequestGate, response_retry_after, retry_delay_seconds
from .contracts import CrunchyrollSnapshot, EpisodeProgress, SeriesRef, WatchlistEntry
from .provider_snapshot import snapshot_to_dict as _snapshot_to_dict
from .provider_snapshot import write_snapshot_file as _write_snapshot_file
from .crunchyroll_auth import (
    CRUNCHYROLL_BASIC_AUTH_TOKEN,
    CRUNCHYROLL_ME_URL,
    CRUNCHYROLL_TOKEN_URL,
    CrunchyrollAuthError,
    CrunchyrollBootstrapResult,
    CrunchyrollStatePaths,
    crunchyroll_login_with_credentials,
    resolve_crunchyroll_state_paths,
)


class CrunchyrollSnapshotError(RuntimeError):
    pass


def _require_curl_requests():
    if curl_requests is None:
        raise CrunchyrollSnapshotError(
            "Crunchyroll requires curl_cffi browser-TLS transport; install project dependencies with `python3 -m pip install -e .`."
        )
    return curl_requests


class CrunchyrollUnauthorizedError(CrunchyrollSnapshotError):
    def __init__(self, url: str, status_code: int):
        super().__init__(f"Crunchyroll GET failed for {url}: HTTP {status_code}")
        self.url = url
        self.status_code = status_code


DEFAULT_CRUNCHYROLL_DEVICE_TYPE = "ANDROIDTV"
SYNC_BOUNDARY_SCHEMA_VERSION = 1
HISTORY_BOUNDARY_MARKER_LIMIT = 200
WATCHLIST_BOUNDARY_MARKER_LIMIT = 200
INCREMENTAL_BACKFILL_PAGE_LIMIT = 1
HOT_HISTORY_CATCHUP_PAGE_LIMIT = 100
BOOTSTRAP_RESUME_MAX_AGE_DAYS = 14
BOOTSTRAP_DRIFT_QUARANTINE_LIMIT = 3
CRUNCHYROLL_HISTORY_PAGE_LIMIT = 1000
CRUNCHYROLL_WATCHLIST_PAGE_LIMIT = 1000


@dataclass(slots=True)
class CrunchyrollAccessToken:
    access_token: str
    refresh_token: str
    account_id: str | None
    device_id: str
    device_type: str


@dataclass(slots=True)
class CrunchyrollFetchResult:
    snapshot: CrunchyrollSnapshot
    state_paths: CrunchyrollStatePaths
    account_email: str | None


@dataclass(slots=True)
class _SyncBoundary:
    generated_at: str | None
    account_id_hint: str | None
    history_markers: list[str]
    watchlist_markers: list[str]
    history_backfill_markers: list[str]
    watchlist_backfill_markers: list[str]


@dataclass(slots=True)
class _CrunchyrollRequestPacer:
    spacing_seconds: float
    jitter_seconds: float = 0.0
    last_request_started_at: float | None = None
    gate: ProviderRequestGate | None = None
    retry_max_attempts: int = 1
    retry_backoff_base_seconds: float = 1.0
    retry_backoff_jitter_seconds: float = 0.25
    retry_after_cap_seconds: float = 60.0

    def _target_spacing_seconds(self) -> float:
        if self.spacing_seconds <= 0:
            return 0.0
        if self.jitter_seconds <= 0:
            return self.spacing_seconds
        lower = max(0.0, self.spacing_seconds - self.jitter_seconds)
        upper = self.spacing_seconds + self.jitter_seconds
        return random.uniform(lower, upper)

    def wait_turn(self) -> None:
        if self.gate is not None:
            self.gate.wait_turn()
            return
        target_spacing_seconds = self._target_spacing_seconds()
        if target_spacing_seconds <= 0:
            self.last_request_started_at = time.monotonic()
            return
        now = time.monotonic()
        if self.last_request_started_at is not None:
            remaining = target_spacing_seconds - (now - self.last_request_started_at)
            if remaining > 0:
                time.sleep(remaining)
                now = time.monotonic()
        self.last_request_started_at = now


def _build_request_pacer(config: AppConfig) -> _CrunchyrollRequestPacer:
    spacing = max(0.0, float(config.crunchyroll.request_spacing_seconds))
    jitter = max(0.0, float(config.crunchyroll.request_spacing_jitter_seconds))
    state_dir = getattr(config, "state_dir", None)
    return _CrunchyrollRequestPacer(
        spacing_seconds=spacing,
        jitter_seconds=jitter,
        gate=(ProviderRequestGate(
            provider="crunchyroll", state_dir=state_dir,
            spacing_seconds=spacing, jitter_seconds=jitter,
        ) if state_dir is not None else None),
        retry_max_attempts=max(1, int(getattr(config.crunchyroll, "retry_max_attempts", 1))),
        retry_backoff_base_seconds=float(getattr(config.crunchyroll, "retry_backoff_base_seconds", 1.0)),
        retry_backoff_jitter_seconds=float(getattr(config.crunchyroll, "retry_backoff_jitter_seconds", 0.25)),
        retry_after_cap_seconds=float(getattr(config.crunchyroll, "retry_after_cap_seconds", 60.0)),
    )


@dataclass(slots=True)
class _CrunchyrollAuthSession:
    config: AppConfig
    profile: str
    timeout_seconds: float
    pacer: _CrunchyrollRequestPacer
    state_paths: CrunchyrollStatePaths
    token: CrunchyrollAccessToken
    auth_source: str
    account_email: str | None = None
    credential_rebootstrap_attempted: bool = False

    def authorized_json_get(self, url: str, *, params: dict[str, Any] | None = None, phase: str | None = None) -> Any:
        last_unauthorized: CrunchyrollUnauthorizedError | None = None
        refresh_error: CrunchyrollAuthError | None = None
        for attempt in range(3):
            try:
                return _authorized_json_get(
                    url,
                    access_token=self.token.access_token,
                    timeout_seconds=self.timeout_seconds,
                    params=params,
                    pacer=self.pacer,
                    phase=phase,
                    config=self.config,
                )
            except CrunchyrollUnauthorizedError as exc:
                last_unauthorized = exc
                if attempt == 0:
                    try:
                        self._refresh_with_refresh_token(exc)
                        continue
                    except CrunchyrollAuthError as refresh_exc:
                        refresh_error = refresh_exc
                        if not _auth_failure_allows_credential_rebootstrap(refresh_exc):
                            raise
                if not self.credential_rebootstrap_attempted:
                    self._rebootstrap_with_credentials(exc, refresh_error=refresh_error)
                    continue
                detail = f"{exc}; refresh-token recovery failed"
                if refresh_error is not None:
                    detail += f" ({refresh_error})"
                detail += "; credential rebootstrap already used for this run"
                _write_session_state(
                    state_paths=self.state_paths,
                    profile=self.profile,
                    locale=self.config.crunchyroll.locale,
                    device_type=self.token.device_type,
                    account_id=self.token.account_id,
                    last_error=detail,
                    success=False,
                    phase="auth_failed",
                )
                self.token.account_id = None
                self.account_email = None
                raise
        raise last_unauthorized or CrunchyrollSnapshotError(f"Crunchyroll authorization failed for {url}")

    def _refresh_with_refresh_token(self, exc: CrunchyrollUnauthorizedError) -> None:
        prior_account_id = self.token.account_id
        prior_account_email = self.account_email
        _write_session_state(
            state_paths=self.state_paths,
            profile=self.profile,
            locale=self.config.crunchyroll.locale,
            device_type=self.token.device_type,
            account_id=self.token.account_id,
            last_error=f"{exc}; retrying with refresh-token renewal",
            success=False,
            phase="auth_retrying_with_refresh_token",
        )
        token, state_paths = refresh_access_token(
            self.config,
            profile=self.profile,
            timeout_seconds=self.timeout_seconds,
            pacer=self.pacer,
        )
        self.token = token
        if prior_account_id and token.account_id == prior_account_id:
            self.account_email = prior_account_email
        elif prior_account_id != token.account_id:
            self.account_email = None
        self.state_paths = state_paths
        self.auth_source = "refresh_token_recovery"

    def _rebootstrap_with_credentials(
        self,
        exc: CrunchyrollUnauthorizedError,
        *,
        refresh_error: CrunchyrollAuthError | None = None,
    ) -> None:
        message = f"{exc}; retrying once with credential rebootstrap"
        if refresh_error is not None:
            message += f" after refresh-token recovery failed ({refresh_error})"
        _write_session_state(
            state_paths=self.state_paths,
            profile=self.profile,
            locale=self.config.crunchyroll.locale,
            device_type=self.token.device_type,
            account_id=self.token.account_id,
            last_error=message,
            success=False,
            phase="auth_retrying_with_credentials",
        )
        bootstrap = crunchyroll_login_with_credentials(
            self.config,
            profile=self.profile,
            timeout_seconds=self.timeout_seconds,
            verify_account=True,
            pacer=self.pacer,
        )
        self.token = _token_from_bootstrap(bootstrap)
        self.state_paths = resolve_crunchyroll_state_paths(self.config, profile=self.profile)
        self.auth_source = "credential_rebootstrap"
        self.account_email = bootstrap.account_email
        self.credential_rebootstrap_attempted = True



def _now_string() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _request_timeout(timeout_seconds: float) -> tuple[float, float]:
    return (timeout_seconds, timeout_seconds)


def _log_fetch_progress(message: str) -> None:
    print(f"[crunchyroll-fetch] {message}", file=sys.stderr, flush=True)


def _http_post(url: str, *, data: dict[str, str], headers: dict[str, str], timeout_seconds: float, config: AppConfig | None = None):
    transport = _require_curl_requests()
    request_exception = getattr(getattr(transport, "exceptions", None), "RequestException", Exception)
    try:
        response = transport.post(url, data=data, headers=headers, timeout=_request_timeout(timeout_seconds), impersonate="chrome124")
        record_api_request_event("crunchyroll", "http_post", url=url, method="POST", outcome="ok" if response.status_code < 400 else "http_error", status_code=response.status_code, config=config)
        return response
    except request_exception as exc:
        record_api_request_event("crunchyroll", "http_post", url=url, method="POST", outcome="request_error", error=type(exc).__name__, config=config)
        raise


def _http_get(url: str, *, headers: dict[str, str], timeout_seconds: float, config: AppConfig, params: dict[str, Any] | None = None):
    transport = _require_curl_requests()
    request_exception = getattr(getattr(transport, "exceptions", None), "RequestException", Exception)
    try:
        response = transport.get(url, headers=headers, timeout=_request_timeout(timeout_seconds), params=params, impersonate="chrome124")
        record_api_request_event("crunchyroll", "http_get", url=url, method="GET", outcome="ok" if response.status_code < 400 else "http_error", status_code=response.status_code, config=config)
        return response
    except request_exception as exc:
        record_api_request_event("crunchyroll", "http_get", url=url, method="GET", outcome="request_error", error=type(exc).__name__, config=config)
        raise


def _write_session_state(
    *,
    state_paths: CrunchyrollStatePaths,
    profile: str,
    locale: str,
    device_type: str,
    account_id: str | None,
    last_error: str | None,
    success: bool,
    phase: str,
) -> None:
    state_paths.root.mkdir(parents=True, exist_ok=True)
    payload = {
        "profile": profile,
        "locale": locale,
        "refresh_token_present": state_paths.refresh_token_path.exists(),
        "device_id_present": state_paths.device_id_path.exists(),
        "device_type_hint": device_type,
        "last_login_attempt_at": _now_string(),
        "last_login_success_at": _now_string() if success else None,
        "last_account_id_hint": account_id,
        "last_error": last_error,
        "crunchyroll_phase": phase,
    }
    atomic_write_json(state_paths.session_state_path, payload, indent=2)


def _read_device_id(state_paths: CrunchyrollStatePaths) -> str:
    return _read_secret_file(state_paths.device_id_path) or str(uuid.uuid4())


def refresh_access_token(
    config: AppConfig,
    *,
    profile: str = "default",
    timeout_seconds: float = 30.0,
    pacer: _CrunchyrollRequestPacer | None = None,
) -> tuple[CrunchyrollAccessToken, CrunchyrollStatePaths]:
    pacer = pacer or _build_request_pacer(config)
    state_paths = resolve_crunchyroll_state_paths(config, profile=profile)
    refresh_token = _read_secret_file(state_paths.refresh_token_path)
    if not refresh_token:
        raise CrunchyrollAuthError(f"Missing Crunchyroll refresh token at {state_paths.refresh_token_path}")

    device_id = _read_device_id(state_paths)
    device_type = DEFAULT_CRUNCHYROLL_DEVICE_TYPE
    body = {
        "grant_type": "refresh_token",
        "scope": "offline_access",
        "refresh_token": refresh_token,
        "device_id": device_id,
        "device_type": device_type,
    }
    headers = {
        "Authorization": f"Basic {CRUNCHYROLL_BASIC_AUTH_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded",
        "ETP-Anonymous-ID": device_id,
    }
    # Refresh tokens may be rotated even when the response is lost or transient;
    # never replay this credential POST automatically.
    attempts = 1
    response = None
    for attempt in range(1, attempts + 1):
        if pacer is not None:
            pacer.wait_turn()
        try:
            response = _http_post(CRUNCHYROLL_TOKEN_URL, data=body, headers=headers, timeout_seconds=timeout_seconds, config=config)
        except Exception as exc:
            if attempt < attempts and ("timeout" in type(exc).__name__.lower() or "connection" in type(exc).__name__.lower()):
                delay = retry_delay_seconds(
                    attempt,
                    base_seconds=getattr(pacer, "retry_backoff_base_seconds", 1.0),
                    jitter_seconds=getattr(pacer, "retry_backoff_jitter_seconds", 0.25),
                    cap_seconds=getattr(pacer, "retry_after_cap_seconds", 60.0),
                )
                if delay > 0:
                    time.sleep(delay)
                continue
            raise
        if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts:
            delay = retry_delay_seconds(
                attempt,
                retry_after=response_retry_after(response),
                base_seconds=getattr(pacer, "retry_backoff_base_seconds", 1.0),
                jitter_seconds=getattr(pacer, "retry_backoff_jitter_seconds", 0.25),
                cap_seconds=getattr(pacer, "retry_after_cap_seconds", 60.0),
            )
            if delay > 0:
                time.sleep(delay)
            continue
        break
    assert response is not None
    if response.status_code >= 400:
        message = f"Crunchyroll refresh-token login failed: HTTP {response.status_code}"
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            error = payload.get("error")
            error_description = payload.get("error_description")
            if error or error_description:
                message = f"Crunchyroll refresh-token login failed: {error or 'unknown_error'} - {error_description or 'no description'}"
        _write_session_state(
            state_paths=state_paths,
            profile=profile,
            locale=config.crunchyroll.locale,
            device_type=device_type,
            account_id=None,
            last_error=message,
            success=False,
            phase="auth_failed",
        )
        raise CrunchyrollAuthError(message)

    token_payload = response.json()
    access_token = token_payload.get("access_token")
    new_refresh_token = token_payload.get("refresh_token")
    account_id = token_payload.get("account_id")
    if not access_token or not new_refresh_token:
        message = "Crunchyroll refresh-token login succeeded but did not return both access_token and refresh_token"
        _write_session_state(
            state_paths=state_paths,
            profile=profile,
            locale=config.crunchyroll.locale,
            device_type=device_type,
            account_id=account_id,
            last_error=message,
            success=False,
            phase="auth_failed",
        )
        raise CrunchyrollAuthError(message)

    write_secret_file(state_paths.refresh_token_path, new_refresh_token)
    write_secret_file(state_paths.device_id_path, device_id)
    return (
        CrunchyrollAccessToken(
            access_token=access_token,
            refresh_token=new_refresh_token,
            account_id=str(account_id) if account_id else None,
            device_id=device_id,
            device_type=device_type,
        ),
        state_paths,
    )


def _authorized_json_get(
    url: str,
    *,
    access_token: str,
    timeout_seconds: float,
    config: AppConfig,
    params: dict[str, Any] | None = None,
    pacer: _CrunchyrollRequestPacer | None = None,
    phase: str | None = None,
) -> Any:
    phase_label = phase or "GET"
    attempts = max(1, int(getattr(pacer, "retry_max_attempts", 1)))
    response = None
    for attempt in range(1, attempts + 1):
        if pacer is not None:
            pacer.wait_turn()
        try:
            response = _http_get(
                url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout_seconds=timeout_seconds,
                params=params,
                config=config,
            )
        except Exception as exc:
            if attempt < attempts and ("timeout" in type(exc).__name__.lower() or "connection" in type(exc).__name__.lower()):
                delay = retry_delay_seconds(
                    attempt,
                    base_seconds=getattr(pacer, "retry_backoff_base_seconds", 1.0),
                    jitter_seconds=getattr(pacer, "retry_backoff_jitter_seconds", 0.25),
                    cap_seconds=getattr(pacer, "retry_after_cap_seconds", 60.0),
                )
                if delay > 0:
                    time.sleep(delay)
                continue
            raise CrunchyrollSnapshotError(f"Crunchyroll {phase_label} request failed for {url}: {exc}") from exc
        if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts:
            delay = retry_delay_seconds(
                attempt,
                retry_after=response_retry_after(response),
                base_seconds=getattr(pacer, "retry_backoff_base_seconds", 1.0),
                jitter_seconds=getattr(pacer, "retry_backoff_jitter_seconds", 0.25),
                cap_seconds=getattr(pacer, "retry_after_cap_seconds", 60.0),
            )
            if delay > 0:
                time.sleep(delay)
            continue
        break
    assert response is not None
    if response.status_code == 401:
        raise CrunchyrollUnauthorizedError(url, response.status_code)
    if response.status_code >= 400:
        raise CrunchyrollSnapshotError(f"Crunchyroll GET failed for {url}: HTTP {response.status_code}")
    return response.json()


def _panel_metadata(panel: dict[str, Any]) -> dict[str, Any]:
    if isinstance(panel.get("episode_metadata"), dict):
        return panel["episode_metadata"]
    if isinstance(panel.get("movie_metadata"), dict):
        return panel["movie_metadata"]
    return panel


def _pick_subtitle_locale(panel: dict[str, Any]) -> str | None:
    metadata = _panel_metadata(panel)
    locales = metadata.get("subtitle_locales")
    if isinstance(locales, list) and locales:
        first = locales[0]
        return str(first) if first else None
    return None


def _series_from_panel(panel: dict[str, Any]) -> SeriesRef | None:
    panel_type = panel.get("type")
    metadata = _panel_metadata(panel)
    if panel_type == "episode":
        provider_series_id = metadata.get("series_id")
        title = metadata.get("series_title") or provider_series_id
        season_title = metadata.get("season_title")
        season_number = metadata.get("season_number")
    elif panel_type == "movie":
        provider_series_id = metadata.get("movie_listing_id")
        title = metadata.get("movie_listing_title") or panel.get("title") or provider_series_id
        season_title = None
        season_number = None
    elif panel_type == "series":
        provider_series_id = panel.get("id")
        title = panel.get("title") or provider_series_id
        season_title = None
        season_number = None
    elif panel_type == "movie_listing":
        provider_series_id = panel.get("id")
        title = panel.get("title") or provider_series_id
        season_title = None
        season_number = None
    else:
        return None

    if not provider_series_id:
        return None
    return SeriesRef(
        provider_series_id=str(provider_series_id),
        title=str(title or provider_series_id),
        season_title=str(season_title) if season_title not in (None, "") else None,
        season_number=int(season_number) if isinstance(season_number, int) else None,
    )


def _progress_from_history_entry(entry: dict[str, Any]) -> EpisodeProgress | None:
    panel = entry.get("panel")
    if not isinstance(panel, dict):
        return None

    panel_type = panel.get("type")
    metadata = _panel_metadata(panel)
    playhead = entry.get("playhead")
    fully_watched = bool(entry.get("fully_watched"))
    last_watched_at = entry.get("date_played")

    if panel_type == "episode":
        duration_ms = int(metadata.get("duration_ms")) if isinstance(metadata.get("duration_ms"), int) else None
        playback_position_ms = int(playhead) if isinstance(playhead, int) else None
        if playback_position_ms is not None and duration_ms and playback_position_ms <= max(60000, duration_ms // 100):
            playback_position_ms *= 1000

        provider_series_id = metadata.get("series_id")
        provider_episode_id = panel.get("id")
        if not provider_series_id or not provider_episode_id:
            return None
        completion_ratio = None
        if duration_ms and playback_position_ms is not None and duration_ms > 0:
            completion_ratio = max(0.0, min(1.0, playback_position_ms / duration_ms))
        elif fully_watched:
            completion_ratio = 1.0
        episode_number = metadata.get("episode_number")
        return EpisodeProgress(
            provider_episode_id=str(provider_episode_id),
            provider_series_id=str(provider_series_id),
            episode_number=int(episode_number) if isinstance(episode_number, int) else None,
            episode_title=str(panel.get("title")) if panel.get("title") else None,
            playback_position_ms=playback_position_ms,
            duration_ms=duration_ms,
            completion_ratio=completion_ratio,
            last_watched_at=str(last_watched_at) if last_watched_at else None,
            audio_locale=str(metadata.get("audio_locale")) if metadata.get("audio_locale") else None,
            subtitle_locale=_pick_subtitle_locale(panel),
            rating=None,
        )

    if panel_type == "movie":
        duration_ms = int(metadata.get("duration_ms")) if isinstance(metadata.get("duration_ms"), int) else None
        playback_position_ms = int(playhead) if isinstance(playhead, int) else None
        if playback_position_ms is not None and duration_ms and playback_position_ms <= max(60000, duration_ms // 100):
            playback_position_ms *= 1000
        provider_series_id = metadata.get("movie_listing_id")
        provider_episode_id = panel.get("id")
        if not provider_series_id or not provider_episode_id:
            return None
        completion_ratio = None
        if duration_ms and playback_position_ms is not None and duration_ms > 0:
            completion_ratio = max(0.0, min(1.0, playback_position_ms / duration_ms))
        elif fully_watched:
            completion_ratio = 1.0
        return EpisodeProgress(
            provider_episode_id=str(provider_episode_id),
            provider_series_id=str(provider_series_id),
            episode_number=None,
            episode_title=str(panel.get("title")) if panel.get("title") else None,
            playback_position_ms=playback_position_ms,
            duration_ms=duration_ms,
            completion_ratio=completion_ratio,
            last_watched_at=str(last_watched_at) if last_watched_at else None,
            audio_locale=None,
            subtitle_locale=None,
            rating="movie",
        )

    return None


def _watchlist_from_entry(entry: dict[str, Any]) -> tuple[SeriesRef | None, WatchlistEntry | None]:
    panel = entry.get("panel")
    if not isinstance(panel, dict):
        return None, None
    series_ref = _series_from_panel(panel)
    if series_ref is None:
        return None, None
    if entry.get("fully_watched") is True:
        status = "fully_watched"
    elif entry.get("never_watched") is True:
        status = "never_watched"
    else:
        status = "in_progress"
    added_at = entry.get("date_added")
    return (
        series_ref,
        WatchlistEntry(
            provider_series_id=series_ref.provider_series_id,
            added_at=str(added_at) if added_at else None,
            status=status,
        ),
    )


def _dedupe_series(series_items: list[SeriesRef]) -> list[SeriesRef]:
    by_id: dict[str, SeriesRef] = {}
    for item in series_items:
        by_id.setdefault(item.provider_series_id, item)
    return list(by_id.values())


def _dedupe_progress(progress_items: list[EpisodeProgress]) -> list[EpisodeProgress]:
    """Choose the newest trustworthy observation for each episode."""
    by_id: dict[str, EpisodeProgress] = {}
    for item in progress_items:
        previous = by_id.get(item.provider_episode_id)
        if previous is None:
            by_id[item.provider_episode_id] = item
            continue
        item_key = (item.last_watched_at or "", item.playback_position_ms or -1)
        previous_key = (previous.last_watched_at or "", previous.playback_position_ms or -1)
        if item_key >= previous_key:
            by_id[item.provider_episode_id] = item
    return list(by_id.values())


def _history_entry_fingerprint(entry: dict[str, Any]) -> str | None:
    panel = entry.get("panel")
    if not isinstance(panel, dict):
        return None
    metadata = _panel_metadata(panel)
    provider_episode_id = panel.get("id")
    provider_series_id = metadata.get("series_id") or metadata.get("movie_listing_id")
    panel_type = panel.get("type")
    last_watched_at = entry.get("date_played")
    playhead = entry.get("playhead")
    fully_watched = entry.get("fully_watched")
    if not provider_episode_id or not provider_series_id or not panel_type:
        return None
    return "|".join(
        [
            str(panel_type),
            str(provider_series_id),
            str(provider_episode_id),
            str(last_watched_at or ""),
            str(playhead if playhead is not None else ""),
            str(bool(fully_watched)),
        ]
    )


def _watchlist_entry_fingerprint(entry: dict[str, Any]) -> str | None:
    series_ref, watchlist_entry = _watchlist_from_entry(entry)
    if series_ref is None or watchlist_entry is None:
        return None
    return "|".join(
        [
            str(series_ref.provider_series_id),
            str(watchlist_entry.status),
            str(watchlist_entry.added_at or ""),
        ]
    )


def _load_sync_boundary(state_paths: CrunchyrollStatePaths) -> _SyncBoundary | None:
    path = state_paths.sync_boundary_path
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        schema_version = int(payload.get("schema_version") or 0)
    except (TypeError, ValueError):
        return None
    if schema_version != SYNC_BOUNDARY_SCHEMA_VERSION:
        return None
    history = payload.get("history") if isinstance(payload.get("history"), dict) else {}
    watchlist = payload.get("watchlist") if isinstance(payload.get("watchlist"), dict) else {}
    return _SyncBoundary(
        generated_at=str(payload.get("generated_at")) if payload.get("generated_at") else None,
        account_id_hint=str(payload.get("account_id_hint")) if payload.get("account_id_hint") else None,
        history_markers=[str(item) for item in history.get("first_seen", []) if item],
        watchlist_markers=[str(item) for item in watchlist.get("first_seen", []) if item],
        history_backfill_markers=[str(item) for item in history.get("backfill_seen", []) if item],
        watchlist_backfill_markers=[str(item) for item in watchlist.get("backfill_seen", []) if item],
    )


def _load_requested_sync_boundary(state_paths: CrunchyrollStatePaths) -> tuple[_SyncBoundary | None, str]:
    if not state_paths.sync_boundary_path.exists():
        return None, "missing"
    boundary = _load_sync_boundary(state_paths)
    if boundary is None:
        return None, "invalid_or_corrupt"
    return boundary, "loaded"


def _unique_fingerprints(entries: list[dict[str, Any]], fingerprint_func, limit: int) -> list[str]:
    markers: list[str] = []
    for entry in entries:
        fingerprint = fingerprint_func(entry)
        if fingerprint and fingerprint not in markers:
            markers.append(fingerprint)
        if len(markers) >= limit:
            break
    return markers


def _write_sync_boundary(
    *,
    state_paths: CrunchyrollStatePaths,
    generated_at: str,
    account_id_hint: str | None,
    history_entries: list[dict[str, Any]],
    watchlist_entries: list[dict[str, Any]],
    history_backfill_entries: list[dict[str, Any]] | None = None,
    watchlist_backfill_entries: list[dict[str, Any]] | None = None,
    history_markers_override: list[str] | None = None,
    watchlist_markers_override: list[str] | None = None,
) -> None:
    state_paths.root.mkdir(parents=True, exist_ok=True)
    history_markers = list(history_markers_override or _unique_fingerprints(history_entries, _history_entry_fingerprint, HISTORY_BOUNDARY_MARKER_LIMIT))[:HISTORY_BOUNDARY_MARKER_LIMIT]
    watchlist_markers = list(watchlist_markers_override or _unique_fingerprints(watchlist_entries, _watchlist_entry_fingerprint, WATCHLIST_BOUNDARY_MARKER_LIMIT))[:WATCHLIST_BOUNDARY_MARKER_LIMIT]
    history_backfill_markers = _unique_fingerprints(history_backfill_entries or [], _history_entry_fingerprint, HISTORY_BOUNDARY_MARKER_LIMIT)
    watchlist_backfill_markers = _unique_fingerprints(watchlist_backfill_entries or [], _watchlist_entry_fingerprint, WATCHLIST_BOUNDARY_MARKER_LIMIT)
    payload = {
        "schema_version": SYNC_BOUNDARY_SCHEMA_VERSION,
        "generated_at": generated_at,
        "account_id_hint": account_id_hint,
        "history": {
            "marker_limit": HISTORY_BOUNDARY_MARKER_LIMIT,
            "retained_count": len(history_markers),
            "first_seen": history_markers,
            "backfill_seen": history_backfill_markers,
            "backfill_retained_count": len(history_backfill_markers),
        },
        "watchlist": {
            "marker_limit": WATCHLIST_BOUNDARY_MARKER_LIMIT,
            "retained_count": len(watchlist_markers),
            "first_seen": watchlist_markers,
            "backfill_seen": watchlist_backfill_markers,
            "backfill_retained_count": len(watchlist_backfill_markers),
        },
    }
    atomic_write_json(state_paths.sync_boundary_path, payload, indent=2)


def _bootstrap_resume_path(state_paths: CrunchyrollStatePaths) -> Path:
    return state_paths.root / "snapshot_bootstrap_resume.json"


def _load_bootstrap_resume(
    state_paths: CrunchyrollStatePaths, *, account_id: str, locale: str
) -> dict[str, Any] | None:
    path = _bootstrap_resume_path(state_paths)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        return None
    if payload.get("account_id_hint") != account_id or payload.get("locale") != locale:
        return None
    try:
        updated_at = datetime.fromisoformat(str(payload.get("updated_at") or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if (datetime.now(timezone.utc) - updated_at).total_seconds() > BOOTSTRAP_RESUME_MAX_AGE_DAYS * 86400:
        return None
    return payload


def _write_bootstrap_resume(state_paths: CrunchyrollStatePaths, payload: dict[str, Any]) -> None:
    atomic_write_json(_bootstrap_resume_path(state_paths), payload, indent=2)


def _page_fingerprint(entries: list[dict[str, Any]], fingerprint_func) -> str:
    return json.dumps([fingerprint_func(item) or repr(item) for item in entries], sort_keys=True)


def _token_from_bootstrap(result: CrunchyrollBootstrapResult) -> CrunchyrollAccessToken:
    return CrunchyrollAccessToken(
        access_token=result.access_token,
        refresh_token=result.refresh_token,
        account_id=result.account_id,
        device_id=result.device_id,
        device_type=result.device_type,
    )


def _start_auth_session(
    config: AppConfig,
    *,
    profile: str,
    timeout_seconds: float,
    pacer: _CrunchyrollRequestPacer,
) -> _CrunchyrollAuthSession:
    state_paths = resolve_crunchyroll_state_paths(config, profile=profile)
    try:
        token, state_paths = refresh_access_token(config, profile=profile, timeout_seconds=timeout_seconds, pacer=pacer)
        return _CrunchyrollAuthSession(
            config=config,
            profile=profile,
            timeout_seconds=timeout_seconds,
            pacer=pacer,
            state_paths=state_paths,
            token=token,
            auth_source="refresh_token",
        )
    except CrunchyrollAuthError as exc:
        if not _auth_failure_allows_credential_rebootstrap(exc):
            raise
        bootstrap = crunchyroll_login_with_credentials(
            config,
            profile=profile,
            timeout_seconds=timeout_seconds,
            verify_account=True,
            pacer=pacer,
        )
        return _CrunchyrollAuthSession(
            config=config,
            profile=profile,
            timeout_seconds=timeout_seconds,
            pacer=pacer,
            state_paths=resolve_crunchyroll_state_paths(config, profile=profile),
            token=_token_from_bootstrap(bootstrap),
            auth_source="credential_rebootstrap",
            account_email=bootstrap.account_email,
            credential_rebootstrap_attempted=True,
        )


def _auth_failure_allows_credential_rebootstrap(exc: CrunchyrollAuthError) -> bool:
    """Only definitive credential rejection permits a second credential POST."""
    message = str(exc).casefold()
    return any(marker in message for marker in ("http 400", "http 401", "http 403", "invalid_grant", "invalid refresh", "missing crunchyroll refresh token"))


def _fetch_snapshot_once(
    session: _CrunchyrollAuthSession,
    *,
    use_incremental_boundary: bool = True,
    max_history_pages: int | None = None,
    max_watchlist_pages: int | None = None,
    history_start_page: int = 1,
    watchlist_start: int = 0,
) -> CrunchyrollFetchResult:
    config = session.config
    _log_fetch_progress(f"account endpoint={CRUNCHYROLL_ME_URL}")
    account_payload = session.authorized_json_get(CRUNCHYROLL_ME_URL, phase="account")
    if not isinstance(account_payload, dict):
        raise CrunchyrollSnapshotError("Crunchyroll account response was not a JSON object")
    account_id = str(account_payload.get("account_id") or session.token.account_id or "") or None
    if not account_id:
        raise CrunchyrollSnapshotError("Crunchyroll account response did not include account_id")
    session.token.account_id = account_id
    if account_payload.get("email"):
        session.account_email = str(account_payload.get("email"))

    requested_incremental_boundary = use_incremental_boundary
    boundary_file_present = session.state_paths.sync_boundary_path.exists()
    loaded_boundary, sync_boundary_load_status = (
        _load_requested_sync_boundary(session.state_paths)
        if requested_incremental_boundary
        else (None, "not_requested")
    )
    boundary: _SyncBoundary | None = None
    sync_boundary_account_match: bool | None = None
    if requested_incremental_boundary:
        if loaded_boundary is not None:
            sync_boundary_account_match = loaded_boundary.account_id_hint == account_id
            if sync_boundary_account_match:
                sync_boundary_load_status = "valid_account_match"
                boundary = loaded_boundary
            elif loaded_boundary.account_id_hint:
                sync_boundary_load_status = "account_mismatch"
            else:
                sync_boundary_load_status = "missing_account_hint"

    hot_mode = requested_incremental_boundary and boundary is not None
    sync_boundary_bootstrap = requested_incremental_boundary and not hot_mode
    automatic_bootstrap_chunk = sync_boundary_bootstrap and (
        max_history_pages is not None or max_watchlist_pages is not None
    ) and history_start_page == 1 and watchlist_start == 0
    bootstrap_resume = (
        _load_bootstrap_resume(
            session.state_paths, account_id=account_id, locale=config.crunchyroll.locale
        )
        if automatic_bootstrap_chunk
        else None
    )
    resume_history_markers: list[str] = []
    resume_watchlist_markers: list[str] = []
    staged_history_entries: list[dict[str, Any]] = []
    staged_watchlist_entries: list[dict[str, Any]] = []
    bootstrap_history_page1_fingerprint: str | None = None
    bootstrap_watchlist_page1_fingerprint: str | None = None
    bootstrap_history_total: int | None = None
    bootstrap_watchlist_total: int | None = None
    bootstrap_drift_count = 0
    if bootstrap_resume is not None:
        if bootstrap_resume.get("quarantined") is True:
            raise CrunchyrollSnapshotError(
                "Crunchyroll bootstrap traversal is quarantined after repeated page-1/count drift; "
                "run an explicit full refresh or remove the non-secret bootstrap resume state after review"
            )
        history_start_page = max(1, int(bootstrap_resume.get("history_next_page") or 1))
        watchlist_start = max(0, int(bootstrap_resume.get("watchlist_next_start") or 0))
        resume_history_markers = [str(item) for item in bootstrap_resume.get("history_first_markers", []) if item]
        resume_watchlist_markers = [str(item) for item in bootstrap_resume.get("watchlist_first_markers", []) if item]
        staged_history_entries = [item for item in bootstrap_resume.get("history_entries", []) if isinstance(item, dict)]
        staged_watchlist_entries = [item for item in bootstrap_resume.get("watchlist_entries", []) if isinstance(item, dict)]
        bootstrap_history_page1_fingerprint = bootstrap_resume.get("history_page1_fingerprint")
        bootstrap_watchlist_page1_fingerprint = bootstrap_resume.get("watchlist_page1_fingerprint")
        bootstrap_history_total = bootstrap_resume.get("history_total") if isinstance(bootstrap_resume.get("history_total"), int) else None
        bootstrap_watchlist_total = bootstrap_resume.get("watchlist_total") if isinstance(bootstrap_resume.get("watchlist_total"), int) else None
        bootstrap_drift_count = max(0, int(bootstrap_resume.get("drift_count") or 0))
    sync_boundary_refresh_kind = (
        "hot"
        if hot_mode
        else "bootstrap_full_refresh" if sync_boundary_bootstrap else "explicit_full_refresh"
    )
    history_markers = set(boundary.history_markers) if boundary else set()
    watchlist_markers = set(boundary.watchlist_markers) if boundary else set()
    history_backfill_markers = set(boundary.history_backfill_markers) if boundary else set()
    watchlist_backfill_markers = set(boundary.watchlist_backfill_markers) if boundary else set()

    history_entries: list[dict[str, Any]] = []
    history_backfill_entries: list[dict[str, Any]] = []
    history_pages_fetched = 0
    history_backfill_pages_fetched = 0
    history_stopped_early = False
    history_backfill_exhausted = False
    history_front_boundary_seen = not history_markers
    history_backfill_cursor_seen = not history_backfill_markers
    history_partial = False
    history_boundary_complete = not hot_mode
    history_guard_or_duplicate = False
    seen_history_pages: set[str] = set()
    next_history_page: int | None = None
    page = max(1, history_start_page)
    while True:
        if page > CRUNCHYROLL_HISTORY_PAGE_LIMIT:
            raise CrunchyrollSnapshotError(f"Crunchyroll watch-history exceeded page guard ({CRUNCHYROLL_HISTORY_PAGE_LIMIT})")
        history_url = f"https://www.crunchyroll.com/content/v2/{account_id}/watch-history"
        _log_fetch_progress(f"watch-history page={page} endpoint={history_url}")
        history_payload = session.authorized_json_get(
            history_url,
            params={"page": page, "page_size": 100, "locale": config.crunchyroll.locale},
            phase=f"watch-history page {page}",
        )
        history_pages_fetched += 1
        if not isinstance(history_payload, dict):
            raise CrunchyrollSnapshotError("Crunchyroll watch-history response was not a JSON object")
        data = history_payload.get("data")
        if not isinstance(data, list):
            raise CrunchyrollSnapshotError("Crunchyroll watch-history response did not include a data list")
        batch = [item for item in data if isinstance(item, dict)]
        batch_fingerprint = json.dumps(
            [_history_entry_fingerprint(item) or repr(item) for item in batch],
            sort_keys=True,
        )
        if hot_mode and batch and batch_fingerprint in seen_history_pages:
            history_partial = True
            history_stopped_early = True
            history_guard_or_duplicate = True
            break
        if batch:
            seen_history_pages.add(batch_fingerprint)
        history_entries.extend(batch)
        batch_markers = {_history_entry_fingerprint(item) for item in batch}
        if not history_front_boundary_seen and history_markers.intersection(batch_markers):
            history_front_boundary_seen = True
            history_boundary_complete = True
        elif history_front_boundary_seen and not history_backfill_cursor_seen and history_backfill_markers.intersection(batch_markers):
            history_backfill_cursor_seen = True
        elif history_front_boundary_seen and history_backfill_cursor_seen and use_incremental_boundary and boundary:
            history_backfill_entries.extend(batch)
            history_backfill_pages_fetched += 1
            if history_backfill_pages_fetched >= INCREMENTAL_BACKFILL_PAGE_LIMIT:
                history_stopped_early = True
                break
        total = history_payload.get("total")
        if automatic_bootstrap_chunk and page == 1:
            bootstrap_history_page1_fingerprint = batch_fingerprint
            bootstrap_history_total = total if isinstance(total, int) else None
        if len(batch) < 100:
            if history_front_boundary_seen:
                history_backfill_exhausted = True
            break
        if isinstance(total, int) and (page * 100) >= total:
            if history_front_boundary_seen:
                history_backfill_exhausted = True
            break
        if hot_mode and history_front_boundary_seen:
            break
        if hot_mode and not history_front_boundary_seen and history_pages_fetched >= HOT_HISTORY_CATCHUP_PAGE_LIMIT:
            history_partial = True
            history_stopped_early = True
            history_guard_or_duplicate = True
            break
        if max_history_pages is not None and history_pages_fetched >= max_history_pages:
            history_partial = True
            history_stopped_early = True
            next_history_page = page + 1
            break
        page += 1

    watchlist_data: list[dict[str, Any]] = []
    watchlist_backfill_entries: list[dict[str, Any]] = []
    watchlist_total: int | None = None
    watchlist_pages_fetched = 0
    watchlist_backfill_pages_fetched = 0
    watchlist_stopped_early = False
    watchlist_backfill_exhausted = False
    watchlist_front_boundary_seen = not watchlist_markers
    watchlist_backfill_cursor_seen = not watchlist_backfill_markers
    watchlist_partial = False
    next_watchlist_start: int | None = None
    initial_watchlist_start = max(0, watchlist_start)
    watchlist_start = initial_watchlist_start
    while not hot_mode:
        if watchlist_pages_fetched >= CRUNCHYROLL_WATCHLIST_PAGE_LIMIT:
            raise CrunchyrollSnapshotError(f"Crunchyroll watchlist exceeded page guard ({CRUNCHYROLL_WATCHLIST_PAGE_LIMIT})")
        watchlist_url = f"https://www.crunchyroll.com/content/v2/discover/{account_id}/watchlist"
        _log_fetch_progress(f"watchlist start={watchlist_start} endpoint={watchlist_url}")
        watchlist_payload = session.authorized_json_get(
            watchlist_url,
            params={"locale": config.crunchyroll.locale, "n": 100, "start": watchlist_start},
            phase=f"watchlist start {watchlist_start}",
        )
        watchlist_pages_fetched += 1
        if not isinstance(watchlist_payload, dict):
            raise CrunchyrollSnapshotError("Crunchyroll watchlist response was not a JSON object")
        data = watchlist_payload.get("data")
        if not isinstance(data, list):
            raise CrunchyrollSnapshotError("Crunchyroll watchlist response did not include a data list")
        batch = [item for item in data if isinstance(item, dict)]
        watchlist_data.extend(batch)
        batch_markers = {_watchlist_entry_fingerprint(item) for item in batch}
        if not watchlist_front_boundary_seen and watchlist_markers.intersection(batch_markers):
            watchlist_front_boundary_seen = True
        elif watchlist_front_boundary_seen and not watchlist_backfill_cursor_seen and watchlist_backfill_markers.intersection(batch_markers):
            watchlist_backfill_cursor_seen = True
        elif watchlist_front_boundary_seen and watchlist_backfill_cursor_seen and use_incremental_boundary and boundary:
            watchlist_backfill_entries.extend(batch)
            watchlist_backfill_pages_fetched += 1
            if watchlist_backfill_pages_fetched >= INCREMENTAL_BACKFILL_PAGE_LIMIT:
                watchlist_stopped_early = True
                break
        total = watchlist_payload.get("total")
        if isinstance(total, int):
            watchlist_total = total
        if automatic_bootstrap_chunk and watchlist_start == 0:
            bootstrap_watchlist_page1_fingerprint = _page_fingerprint(batch, _watchlist_entry_fingerprint)
            bootstrap_watchlist_total = total if isinstance(total, int) else None
        if not batch:
            if watchlist_front_boundary_seen:
                watchlist_backfill_exhausted = True
            break
        watchlist_start += len(batch)
        if len(batch) < 100:
            if watchlist_front_boundary_seen:
                watchlist_backfill_exhausted = True
            break
        if watchlist_total is not None and watchlist_start >= watchlist_total:
            if watchlist_front_boundary_seen:
                watchlist_backfill_exhausted = True
            break
        if max_watchlist_pages is not None and watchlist_pages_fetched >= max_watchlist_pages:
            watchlist_partial = True
            watchlist_stopped_early = True
            next_watchlist_start = watchlist_start
            break

    bootstrap_generation_validated = not automatic_bootstrap_chunk
    bootstrap_drift_detected = False
    if automatic_bootstrap_chunk and not (history_partial or watchlist_partial):
        history_url = f"https://www.crunchyroll.com/content/v2/{account_id}/watch-history"
        history_recheck = session.authorized_json_get(
            history_url, params={"page": 1, "page_size": 100, "locale": config.crunchyroll.locale},
            phase="watch-history bootstrap page-1 revalidation",
        )
        watchlist_url = f"https://www.crunchyroll.com/content/v2/discover/{account_id}/watchlist"
        watchlist_recheck = session.authorized_json_get(
            watchlist_url, params={"locale": config.crunchyroll.locale, "n": 100, "start": 0},
            phase="watchlist bootstrap page-1 revalidation",
        )
        history_recheck_data = history_recheck.get("data") if isinstance(history_recheck, dict) else None
        watchlist_recheck_data = watchlist_recheck.get("data") if isinstance(watchlist_recheck, dict) else None
        history_recheck_batch = [item for item in history_recheck_data if isinstance(item, dict)] if isinstance(history_recheck_data, list) else []
        watchlist_recheck_batch = [item for item in watchlist_recheck_data if isinstance(item, dict)] if isinstance(watchlist_recheck_data, list) else []
        bootstrap_generation_validated = bool(
            isinstance(history_recheck_data, list)
            and isinstance(watchlist_recheck_data, list)
            and _page_fingerprint(history_recheck_batch, _history_entry_fingerprint) == bootstrap_history_page1_fingerprint
            and _page_fingerprint(watchlist_recheck_batch, _watchlist_entry_fingerprint) == bootstrap_watchlist_page1_fingerprint
            and (bootstrap_history_total is None or history_recheck.get("total") == bootstrap_history_total)
            and (bootstrap_watchlist_total is None or watchlist_recheck.get("total") == bootstrap_watchlist_total)
        )
        bootstrap_drift_detected = not bootstrap_generation_validated
        if bootstrap_drift_detected:
            history_partial = True
            watchlist_partial = True

    all_history_entries = staged_history_entries + history_entries
    all_watchlist_data = staged_watchlist_entries + watchlist_data
    progress = _dedupe_progress([item for item in (_progress_from_history_entry(entry) for entry in all_history_entries) if item is not None])
    history_series = [item for item in (_series_from_panel(entry.get("panel")) for entry in all_history_entries if isinstance(entry.get("panel"), dict)) if item is not None]

    watchlist_series: list[SeriesRef] = []
    watchlist_entries: list[WatchlistEntry] = []
    for entry in all_watchlist_data:
        if not isinstance(entry, dict):
            continue
        series_ref, watchlist_entry = _watchlist_from_entry(entry)
        if series_ref is not None:
            watchlist_series.append(series_ref)
        if watchlist_entry is not None:
            watchlist_entries.append(watchlist_entry)

    generated_at = _now_string()
    snapshot = CrunchyrollSnapshot(
        contract_version=config.contract_version,
        generated_at=generated_at,
        provider="crunchyroll",
        account_id_hint=account_id,
        series=_dedupe_series(history_series + watchlist_series),
        progress=progress,
        watchlist=watchlist_entries,
        raw={
            "status": "ok",
            "profile": session.profile,
            "state_root": str(session.state_paths.root),
            "session_state_path": str(session.state_paths.session_state_path),
            "sync_boundary_path": str(session.state_paths.sync_boundary_path),
            "sync_boundary_present": boundary is not None,
            "sync_boundary_file_present": boundary_file_present,
            "sync_boundary_mode": "hot" if hot_mode else "full_refresh",
            "sync_boundary_requested_mode": "incremental" if requested_incremental_boundary else "full_refresh",
            "sync_boundary_requested_incremental": requested_incremental_boundary,
            "requested_incremental_boundary": requested_incremental_boundary,
            "sync_boundary_effective_hot": hot_mode,
            "effective_hot": hot_mode,
            "explicit_full_refresh": not requested_incremental_boundary,
            "sync_boundary_refresh_kind": sync_boundary_refresh_kind,
            "sync_boundary_bootstrap": sync_boundary_bootstrap,
            "bootstrap_full_refresh": sync_boundary_bootstrap,
            "sync_boundary_bootstrap_complete": sync_boundary_bootstrap and not (history_partial or watchlist_partial),
            "bootstrap_full_refresh_complete": sync_boundary_bootstrap and not (history_partial or watchlist_partial),
            "bootstrap_generation_validated": bootstrap_generation_validated,
            "bootstrap_drift_detected": bootstrap_drift_detected,
            "bootstrap_drift_count": bootstrap_drift_count + (1 if bootstrap_drift_detected else 0),
            "bootstrap_staged_history_count": len(all_history_entries),
            "bootstrap_staged_watchlist_count": len(all_watchlist_data),
            "sync_boundary_usable": boundary is not None,
            "sync_boundary_load_status": sync_boundary_load_status,
            "sync_boundary_loaded_account_id_hint": loaded_boundary.account_id_hint if loaded_boundary is not None else None,
            "hot_surface_only": hot_mode,
            "sync_boundary_schema_version": SYNC_BOUNDARY_SCHEMA_VERSION,
            "sync_boundary_account_match": sync_boundary_account_match,
            "refresh_token_present": session.state_paths.refresh_token_path.exists(),
            "device_id_present": session.state_paths.device_id_path.exists(),
            "device_type_hint": session.token.device_type,
            "partial": history_partial or watchlist_partial,
            "history_count": len(history_entries),
            "history_start_page": max(1, history_start_page),
            "history_pages_fetched": history_pages_fetched,
            "history_page_limit_applied": max_history_pages,
            "history_partial": history_partial,
            "history_next_page": next_history_page,
            "history_stopped_early": history_stopped_early,
            "history_boundary_complete": history_boundary_complete,
            "history_guard_or_duplicate": history_guard_or_duplicate,
            "history_backfill_pages_fetched": history_backfill_pages_fetched,
            "history_backfill_exhausted": history_backfill_exhausted,
            "history_boundary_marker_count": len(history_markers),
            "watchlist_count": len(watchlist_entries),
            "watchlist_start": initial_watchlist_start,
            "watchlist_pages_fetched": watchlist_pages_fetched,
            "watchlist_page_limit_applied": max_watchlist_pages,
            "watchlist_partial": watchlist_partial,
            "watchlist_next_start": next_watchlist_start,
            "watchlist_stopped_early": watchlist_stopped_early,
            "watchlist_backfill_pages_fetched": watchlist_backfill_pages_fetched,
            "watchlist_backfill_exhausted": watchlist_backfill_exhausted,
            "watchlist_boundary_marker_count": len(watchlist_markers),
            "transport": "curl_cffi:chrome124" if curl_requests is not None else "requests",
            "request_spacing_seconds": config.crunchyroll.request_spacing_seconds,
            "request_spacing_jitter_seconds": config.crunchyroll.request_spacing_jitter_seconds,
            "retry_max_attempts": config.crunchyroll.retry_max_attempts,
            "retry_after_cap_seconds": config.crunchyroll.retry_after_cap_seconds,
            "niceness_policy": "local_host_process_gate",
            "auth_source": session.auth_source,
        },
    )
    is_partial = history_partial or watchlist_partial or (hot_mode and not history_boundary_complete)
    if automatic_bootstrap_chunk:
        if not resume_history_markers:
            resume_history_markers = _unique_fingerprints(history_entries, _history_entry_fingerprint, HISTORY_BOUNDARY_MARKER_LIMIT)
        if not resume_watchlist_markers:
            resume_watchlist_markers = _unique_fingerprints(watchlist_data, _watchlist_entry_fingerprint, WATCHLIST_BOUNDARY_MARKER_LIMIT)
        if is_partial:
            next_drift_count = bootstrap_drift_count + (1 if bootstrap_drift_detected else 0)
            reset_for_drift = bootstrap_drift_detected
            _write_bootstrap_resume(session.state_paths, {
                "schema_version": 1,
                "account_id_hint": account_id,
                "locale": config.crunchyroll.locale,
                "history_next_page": 1 if reset_for_drift else (next_history_page or history_start_page),
                "watchlist_next_start": 0 if reset_for_drift else (next_watchlist_start if next_watchlist_start is not None else watchlist_start),
                "history_first_markers": [] if reset_for_drift else resume_history_markers,
                "watchlist_first_markers": [] if reset_for_drift else resume_watchlist_markers,
                "history_page1_fingerprint": None if reset_for_drift else bootstrap_history_page1_fingerprint,
                "watchlist_page1_fingerprint": None if reset_for_drift else bootstrap_watchlist_page1_fingerprint,
                "history_total": None if reset_for_drift else bootstrap_history_total,
                "watchlist_total": None if reset_for_drift else bootstrap_watchlist_total,
                "history_entries": [] if reset_for_drift else all_history_entries,
                "watchlist_entries": [] if reset_for_drift else all_watchlist_data,
                "drift_count": next_drift_count,
                "quarantined": next_drift_count >= BOOTSTRAP_DRIFT_QUARANTINE_LIMIT,
                "last_diagnostic": "page-1-or-total-drift" if reset_for_drift else "chunk-incomplete",
                "updated_at": generated_at,
            })
    if not is_partial:
        _write_sync_boundary(
            state_paths=session.state_paths,
            generated_at=generated_at,
            account_id_hint=account_id,
            history_entries=all_history_entries,
            watchlist_entries=all_watchlist_data,
            history_backfill_entries=[] if history_backfill_exhausted else history_backfill_entries,
            watchlist_backfill_entries=[] if watchlist_backfill_exhausted else watchlist_backfill_entries,
            history_markers_override=resume_history_markers or None,
            watchlist_markers_override=resume_watchlist_markers or None,
        )
        if automatic_bootstrap_chunk:
            _bootstrap_resume_path(session.state_paths).unlink(missing_ok=True)
    _write_session_state(
        state_paths=session.state_paths,
        profile=session.profile,
        locale=config.crunchyroll.locale,
        device_type=session.token.device_type,
        account_id=account_id,
        last_error="partial snapshot; sync boundary not advanced" if is_partial else None,
        success=not is_partial,
        phase="python_live_snapshot_partial" if is_partial else "python_live_snapshot",
    )
    return CrunchyrollFetchResult(
        snapshot=snapshot,
        state_paths=session.state_paths,
        account_email=session.account_email,
    )


def fetch_snapshot(
    config: AppConfig,
    *,
    profile: str = "default",
    timeout_seconds: float = 30.0,
    use_incremental_boundary: bool = True,
    max_history_pages: int | None = None,
    max_watchlist_pages: int | None = None,
    history_start_page: int = 1,
    watchlist_start: int = 0,
) -> CrunchyrollFetchResult:
    pacer = _build_request_pacer(config)
    session = _start_auth_session(
        config,
        profile=profile,
        timeout_seconds=timeout_seconds,
        pacer=pacer,
    )
    return _fetch_snapshot_once(
        session,
        use_incremental_boundary=use_incremental_boundary,
        max_history_pages=max_history_pages,
        max_watchlist_pages=max_watchlist_pages,
        history_start_page=history_start_page,
        watchlist_start=watchlist_start,
    )


def snapshot_to_dict(snapshot: CrunchyrollSnapshot) -> dict[str, Any]:
    return _snapshot_to_dict(snapshot)


def write_snapshot_file(path: Path, snapshot: CrunchyrollSnapshot) -> Path:
    return _write_snapshot_file(path, snapshot)
