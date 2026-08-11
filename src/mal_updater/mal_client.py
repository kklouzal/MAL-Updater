from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from socket import timeout as SocketTimeout
from typing import Any, Callable, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from .config import AppConfig, MalSecrets
from .request_tracking import record_api_request_event
from .provider_niceness import ProviderRequestGate, retry_delay_seconds
from .db import bootstrap_database, find_covering_mal_anime_detail_cache, get_mal_anime_detail_cache, get_mal_anime_search_cache, upsert_mal_anime_detail_cache, upsert_mal_anime_search_cache


@dataclass(slots=True)
class OAuthPkcePair:
    code_verifier: str
    code_challenge: str


@dataclass(slots=True)
class TokenResponse:
    access_token: str
    token_type: str
    expires_in: int | None
    refresh_token: str | None
    scope: str | None
    raw: dict[str, Any]


class MalApiError(RuntimeError):
    pass


_T = TypeVar("_T")
_TIMEOUT_RETRY_ATTEMPTS = 2
_RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
MAL_SEARCH_CACHE_LOGIC_VERSION = "mal-search-v2"
MAL_DETAIL_CACHE_LOGIC_VERSION = "mal-detail-v1"
MAL_ANIME_SEARCH_QUERY_MAX_CHARS = 64

_MAL_LIST_STATUSES = frozenset({"completed", "watching", "on_hold", "dropped", "plan_to_watch"})


def _same_mal_api_origin(url: str, base_url: str) -> bool:
    parsed = urlparse(url)
    base = urlparse(base_url)
    if parsed.scheme != "https" or not parsed.netloc:
        return False
    if parsed.username or parsed.password:
        return False
    return parsed.scheme == base.scheme and parsed.netloc == base.netloc


def _path_query_from_mal_api_url(url: str, base_url: str) -> str:
    if not _same_mal_api_origin(url, base_url):
        raise MalApiError("MAL API pagination next URL points outside configured HTTPS MAL API origin")
    parsed = urlparse(url)
    base = urlparse(base_url)
    base_path = base.path.rstrip("/")
    expected_path = f"{base_path}/users/@me/animelist" if base_path else "/users/@me/animelist"
    if parsed.path != expected_path:
        raise MalApiError("MAL API pagination next URL path is outside /users/@me/animelist")
    relative_path = parsed.path[len(base_path):] if base_path else parsed.path
    return f"{relative_path}?{parsed.query}" if parsed.query else relative_path

_ANIME_SEARCH_NOISE_PATTERNS = (
    # Provider catalogs commonly append language/audio availability markers that
    # MAL rejects as noisy/invalid search text. Keep this suffix-oriented and
    # bracket-scoped so title words are not stripped in the middle of a query.
    re.compile(r"[\[(]\s*[^\])()]{0,80}\b(?:dub|sub)\s*[\])]", re.IGNORECASE),
    re.compile(r"\b(?:english|french|spanish|german|portuguese|castilian|latin\s+american\s+spanish)\s+(?:dub|sub)\b", re.IGNORECASE),
)


def _sanitize_anime_search_query(query: str) -> str:
    """Strip provider catalog noise that MAL rejects as invalid search text."""
    cleaned = " ".join(str(query).split()).strip()
    for pattern in _ANIME_SEARCH_NOISE_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    cleaned = re.sub(r"\(\s*\)", " ", cleaned)
    cleaned = re.sub(r"\s*[-:|–—]\s*(?=$|\))", " ", cleaned)
    return " ".join(cleaned.split()).strip()


def _read_http_error_detail(exc: HTTPError) -> str:
    """Read and close urllib HTTPError bodies so failing calls do not leak files."""
    try:
        if exc.fp is None:
            return str(exc)
        return exc.read().decode("utf-8", errors="replace")
    finally:
        exc.close()


def _decode_json_object(body: bytes, *, error_context: str) -> dict[str, Any]:
    """Decode a JSON object without reflecting an upstream body into errors."""
    if not body.strip():
        raise MalApiError(f"{error_context}: empty response body")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalApiError(f"{error_context}: malformed JSON response") from exc
    if not isinstance(payload, dict):
        raise MalApiError(f"{error_context}: JSON response must be an object")
    return payload


class MalClient:
    def __init__(self, config: AppConfig, secrets: MalSecrets):
        self.config = config
        self.secrets = secrets
        self._request_gate = ProviderRequestGate(
            provider="mal",
            state_dir=config.state_dir,
            spacing_seconds=config.mal.request_spacing_seconds,
            jitter_seconds=config.mal.request_spacing_jitter_seconds,
            clock=lambda: time.monotonic(),
            sleep=lambda seconds: time.sleep(seconds),
        )

    def _build_auth_headers(self, require_user: bool = False) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        access_token = self.secrets.access_token.strip() if self.secrets.access_token else ""
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        elif require_user:
            raise MalApiError("MAL access_token is not configured")
        elif self.secrets.client_id:
            headers["X-MAL-CLIENT-ID"] = self.secrets.client_id
        else:
            raise MalApiError("MAL client_id is not configured")
        return headers

    def _pace_request(self) -> None:
        self._request_gate.wait_turn()

    def _is_timeout_error(self, exc: BaseException) -> bool:
        if isinstance(exc, (TimeoutError, SocketTimeout)):
            return True
        if isinstance(exc, URLError):
            reason = exc.reason
            if isinstance(reason, (TimeoutError, SocketTimeout)):
                return True
            reason_text = str(reason).lower()
            return "timed out" in reason_text or "timeout" in reason_text
        return False

    def _format_timeout_message(self, error_context: str, exc: BaseException, *, attempts: int = _TIMEOUT_RETRY_ATTEMPTS) -> str:
        return f"{error_context}: timeout after {attempts} attempts"

    def _request_with_timeout_retry(
        self,
        error_context: str,
        func: Callable[[], _T],
        *,
        operation: str,
        url: str,
        method: str,
    ) -> _T:
        # Only read-only requests are retryable. OAuth authorization codes and
        # rotating refresh tokens may have been consumed even when a POST times
        # out or returns a transient upstream error.
        safe_to_retry = method.upper() == "GET"
        attempts = max(1, int(self.config.mal.retry_max_attempts)) if safe_to_retry else 1
        last_exc: BaseException | None = None
        for attempt in range(1, attempts + 1):
            self._pace_request()
            try:
                return func()
            except HTTPError as exc:
                record_api_request_event("mal", operation, url=url, method=method, outcome="http_error", status_code=exc.code, error=f"HTTP {exc.code}", config=self.config)
                if safe_to_retry and exc.code in _RETRYABLE_HTTP_STATUSES and attempt < attempts:
                    retry_after = exc.headers.get("Retry-After") if exc.headers is not None else None
                    delay = retry_delay_seconds(
                        attempt,
                        retry_after=retry_after,
                        base_seconds=self.config.mal.retry_backoff_base_seconds,
                        jitter_seconds=self.config.mal.retry_backoff_jitter_seconds,
                        cap_seconds=self.config.mal.retry_after_cap_seconds,
                    )
                    exc.close()
                    if delay > 0:
                        time.sleep(delay)
                    continue
                raise
            except (URLError, TimeoutError, SocketTimeout) as exc:
                outcome = "timeout" if self._is_timeout_error(exc) else "url_error"
                record_api_request_event("mal", operation, url=url, method=method, outcome=outcome, error=type(exc).__name__, config=self.config)
                if self._is_timeout_error(exc):
                    last_exc = exc
                    if attempt < attempts:
                        delay = retry_delay_seconds(
                            attempt,
                            base_seconds=self.config.mal.retry_backoff_base_seconds,
                            jitter_seconds=self.config.mal.retry_backoff_jitter_seconds,
                            cap_seconds=self.config.mal.retry_after_cap_seconds,
                        )
                        if delay > 0:
                            time.sleep(delay)
                        continue
                    raise MalApiError(self._format_timeout_message(error_context, exc, attempts=attempts)) from exc
                raise
        if last_exc is not None:
            raise MalApiError(self._format_timeout_message(error_context, last_exc, attempts=attempts)) from last_exc
        raise MalApiError(f"{error_context}: request failed without a captured exception")

    def generate_state(self) -> str:
        return secrets.token_urlsafe(32)

    def generate_pkce_pair(self) -> OAuthPkcePair:
        verifier = secrets.token_urlsafe(64)[:96]
        return OAuthPkcePair(code_verifier=verifier, code_challenge=verifier)

    def build_authorization_url(self, code_challenge: str, state: str | None = None) -> str:
        if not self.secrets.client_id:
            raise MalApiError("MAL client_id is not configured")
        query = {
            "response_type": "code",
            "client_id": self.secrets.client_id,
            "redirect_uri": self.config.mal.redirect_uri,
            "code_challenge": code_challenge,
            "code_challenge_method": "plain",
        }
        if state:
            query["state"] = state
        return f"{self.config.mal.auth_url}?{urlencode(query)}"

    def exchange_code(self, code: str, code_verifier: str) -> TokenResponse:
        if not self.secrets.client_id:
            raise MalApiError("MAL client_id is not configured")
        form = {
            "grant_type": "authorization_code",
            "client_id": self.secrets.client_id,
            "code": code,
            "code_verifier": code_verifier,
            "redirect_uri": self.config.mal.redirect_uri,
        }
        payload = urlencode(form).encode("utf-8")
        return self._post_form(self.config.mal.token_url, payload)

    def refresh_access_token(self, refresh_token: str | None = None) -> TokenResponse:
        token = refresh_token or self.secrets.refresh_token
        if not token:
            raise MalApiError("MAL refresh_token is not configured")
        if not self.secrets.client_id:
            raise MalApiError("MAL client_id is not configured")
        form = {
            "grant_type": "refresh_token",
            "refresh_token": token,
            "client_id": self.secrets.client_id,
        }
        payload = urlencode(form).encode("utf-8")
        return self._post_form(self.config.mal.token_url, payload)

    def get_my_user(self, access_token: str | None = None) -> dict[str, Any]:
        token = access_token or self.secrets.access_token
        if not token:
            raise MalApiError("MAL access_token is not configured")
        return self._get_json(
            f"/users/@me",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            error_context="MAL API GET /users/@me failed",
        )

    def search_anime(self, query: str, *, limit: int = 5, fields: str = "id,title,alternative_titles,media_type,status,num_episodes", force_refresh: bool = False) -> dict[str, Any]:
        sanitized_query = _sanitize_anime_search_query(query) or " ".join(str(query).split()).strip()
        # MAL rejects q values longer than 64 characters. Mapping already tries
        # bounded title fallbacks, so skip an invalid network request and let
        # those variants (or verified identity evidence) supply candidates.
        if len(sanitized_query) > MAL_ANIME_SEARCH_QUERY_MAX_CHARS:
            return {"data": []}
        normalized_query = " ".join(sanitized_query.casefold().split())
        fields_key = ",".join(sorted({part.strip() for part in fields.split(",") if part.strip()}))
        cache_key = hashlib.sha256(json.dumps([MAL_SEARCH_CACHE_LOGIC_VERSION, normalized_query, int(limit), fields_key], separators=(",", ":")).encode()).hexdigest()
        now = datetime.now(timezone.utc).replace(microsecond=0)
        now_iso = now.isoformat().replace("+00:00", "Z")
        cache_available = self.config.db_path.parent.exists()
        if cache_available:
            bootstrap_database(self.config.db_path)
        if cache_available and not force_refresh:
            cached = get_mal_anime_search_cache(self.config.db_path, cache_key=cache_key, now=now_iso)
            if cached is not None:
                return cached.response
        encoded_query = urlencode({"q": sanitized_query, "limit": limit, "fields": fields})
        response = self._get_json(
            f"/anime?{encoded_query}",
            headers=self._build_auth_headers(require_user=False),
            error_context=f"MAL API anime search failed for query={sanitized_query!r}",
        )
        negative = not bool(response.get("data"))
        ttl_days = self.config.mal.search_negative_cache_ttl_days if negative else self.config.mal.search_cache_ttl_days
        if cache_available:
            upsert_mal_anime_search_cache(self.config.db_path, cache_key=cache_key, normalized_query=normalized_query,
                result_limit=int(limit), fields=fields_key, logic_version=MAL_SEARCH_CACHE_LOGIC_VERSION,
                status="negative" if negative else "ok", response=response, fetched_at=now_iso,
                expires_at=(now + timedelta(days=max(0, ttl_days))).isoformat().replace("+00:00", "Z"))
        return response

    def get_anime_details(
        self,
        anime_id: int,
        *,
        fields: str = "id,title,num_episodes,my_list_status",
        force_refresh: bool = False,
        require_user: bool = False,
        cache_ttl_days: int | None = None,
    ) -> dict[str, Any]:
        fields_key = ",".join(sorted({part.strip() for part in fields.split(",") if part.strip()}))
        now = datetime.now(timezone.utc).replace(microsecond=0)
        now_iso = now.isoformat().replace("+00:00", "Z")
        cache_available = self.config.db_path.parent.exists()
        if cache_available:
            bootstrap_database(self.config.db_path)
        if cache_available and not force_refresh:
            cached = get_mal_anime_detail_cache(self.config.db_path, mal_anime_id=int(anime_id), fields_key=fields_key,
                                                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION, now=now_iso)
            if cached is None:
                cached = find_covering_mal_anime_detail_cache(self.config.db_path, mal_anime_id=int(anime_id),
                    required_fields=set(fields_key.split(",")), logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION, now=now_iso)
            if cached is not None and cached.status == "ok" and all(field in cached.response for field in fields_key.split(",")):
                return cached.response
        response = self._get_json(
            f"/anime/{anime_id}?{urlencode({'fields': fields})}",
            headers=self._build_auth_headers(require_user=require_user),
            error_context=f"MAL API anime details failed for anime_id={anime_id}",
        )
        ttl = self.config.mal.detail_cache_ttl_days if cache_ttl_days is None else max(0, int(cache_ttl_days))
        if cache_available:
            upsert_mal_anime_detail_cache(self.config.db_path, mal_anime_id=int(anime_id), fields_key=fields_key,
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION, response=response, fetched_at=now_iso,
                expires_at=(now + timedelta(days=ttl)).isoformat().replace("+00:00", "Z"))
        return response


    def iter_my_anime_list_pages(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
        fields: str = "list_status,num_episodes,media_type,status",
        max_pages: int | None = None,
    ):
        if max_pages is None:
            raise ValueError("max_pages is required for MAL anime-list pagination")
        normalized_max_pages = int(max_pages)
        if normalized_max_pages <= 0:
            raise ValueError("max_pages must be positive")
        normalized_limit = min(max(int(limit), 1), 100)
        normalized_status = None
        if status is not None:
            normalized_status = str(status).strip().lower()
            if normalized_status == "all":
                normalized_status = None
            elif normalized_status not in _MAL_LIST_STATUSES:
                raise MalApiError(f"Unsupported MAL anime list status: {status}")
        query = {"limit": normalized_limit, "fields": fields}
        if normalized_status:
            query["status"] = normalized_status
        next_path = f"/users/@me/animelist?{urlencode(query)}"
        pages = 0
        while next_path and pages < normalized_max_pages:
            pages += 1
            payload = self._get_json(
                next_path,
                headers=self._build_auth_headers(require_user=True),
                error_context="MAL API GET /users/@me/animelist failed",
            )
            yield payload
            paging = payload.get("paging") if isinstance(payload, dict) else None
            next_url = paging.get("next") if isinstance(paging, dict) else None
            if isinstance(next_url, str) and next_url.strip():
                next_path = _path_query_from_mal_api_url(next_url.strip(), self.config.mal.base_url)
            else:
                next_path = None

    def get_my_anime_list_page(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
        fields: str = "list_status,num_episodes,media_type,status",
    ) -> dict[str, Any]:
        return next(self.iter_my_anime_list_pages(status=status, limit=limit, fields=fields, max_pages=1))

    def update_my_list_status(
        self,
        anime_id: int,
        *,
        status: str,
        num_watched_episodes: int,
        score: int | None = None,
        start_date: str | None = None,
        finish_date: str | None = None,
    ) -> dict[str, Any]:
        headers = self._build_auth_headers(require_user=True)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        form = {
            "status": status,
            "num_watched_episodes": str(int(num_watched_episodes)),
        }
        if score is not None:
            form["score"] = str(int(score))
        if start_date:
            form["start_date"] = start_date
        if finish_date:
            form["finish_date"] = finish_date
        payload = urlencode(form).encode("utf-8")
        request = Request(
            f"{self.config.mal.base_url}/anime/{anime_id}/my_list_status",
            data=payload,
            headers=headers,
            method="PUT",
        )
        error_context = f"MAL API update my_list_status failed for anime_id={anime_id}"
        try:
            def _send() -> dict[str, Any]:
                with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                    result = _decode_json_object(response.read(), error_context=error_context)
                    record_api_request_event("mal", "update_my_list_status", url=request.full_url, method="PUT", outcome="ok", status_code=getattr(response, "status", None), config=self.config)
                    return result

            return self._request_with_timeout_retry(error_context, _send, operation="update_my_list_status", url=request.full_url, method="PUT")
        except HTTPError as exc:
            _read_http_error_detail(exc)
            raise MalApiError(f"{error_context}: HTTP {exc.code}") from exc
        except URLError as exc:
            raise MalApiError(f"{error_context}: network error") from exc

    def _get_json(self, path_or_url: str, *, headers: dict[str, str], error_context: str) -> dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self.config.mal.base_url}{path_or_url}"
        request = Request(url, headers=headers, method="GET")
        try:
            def _send() -> dict[str, Any]:
                with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                    result = _decode_json_object(response.read(), error_context=error_context)
                    record_api_request_event("mal", "get_json", url=url, method="GET", outcome="ok", status_code=getattr(response, "status", None), config=self.config)
                    return result

            return self._request_with_timeout_retry(error_context, _send, operation="get_json", url=url, method="GET")
        except HTTPError as exc:
            _read_http_error_detail(exc)
            raise MalApiError(f"{error_context}: HTTP {exc.code}") from exc
        except URLError as exc:
            raise MalApiError(f"{error_context}: network error") from exc

    def _post_form(self, url: str, data: bytes) -> TokenResponse:
        # MAL OAuth2 Scheme 1 documents client_id as the Basic auth username
        # and client_secret as the password; public clients send an empty
        # password: https://myanimelist.net/apiconfig/references/authorization
        # Keep client_secret out of the form body so requests do not mix auth schemes.
        credentials = f"{self.secrets.client_id or ''}:{self.secrets.client_secret or ''}"
        basic = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
        request = Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "Authorization": f"Basic {basic}",
            },
            method="POST",
        )
        try:
            def _send() -> dict[str, Any]:
                with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                    result = _decode_json_object(response.read(), error_context="MAL token request failed")
                    record_api_request_event("mal", "token_request", url=url, method="POST", outcome="ok", status_code=getattr(response, "status", None), config=self.config)
                    return result

            raw = self._request_with_timeout_retry("MAL token request failed", _send, operation="token_request", url=url, method="POST")
        except HTTPError as exc:
            _read_http_error_detail(exc)
            raise MalApiError(f"MAL token request failed: HTTP {exc.code}") from exc
        except URLError as exc:
            raise MalApiError("MAL token request failed: network error") from exc
        return TokenResponse(
            access_token=raw["access_token"],
            token_type=raw.get("token_type", "Bearer"),
            expires_in=raw.get("expires_in"),
            refresh_token=raw.get("refresh_token"),
            scope=raw.get("scope"),
            raw=raw,
        )
