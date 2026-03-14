from __future__ import annotations

import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import AppConfig, MalSecrets


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


class MalClient:
    def __init__(self, config: AppConfig, secrets: MalSecrets):
        self.config = config
        self.secrets = secrets

    def _build_auth_headers(self, require_user: bool = False) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.secrets.access_token:
            headers["Authorization"] = f"Bearer {self.secrets.access_token}"
        elif require_user:
            raise MalApiError("MAL access_token is not configured")
        elif self.secrets.client_id:
            headers["X-MAL-CLIENT-ID"] = self.secrets.client_id
        else:
            raise MalApiError("MAL client_id is not configured")
        return headers

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
        if self.secrets.client_secret:
            form["client_secret"] = self.secrets.client_secret
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
        if self.secrets.client_secret:
            form["client_secret"] = self.secrets.client_secret
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

    def search_anime(self, query: str, *, limit: int = 5, fields: str = "id,title,alternative_titles,media_type,status,num_episodes") -> dict[str, Any]:
        encoded_query = urlencode({"q": query, "limit": limit, "fields": fields})
        return self._get_json(
            f"/anime?{encoded_query}",
            headers=self._build_auth_headers(require_user=False),
            error_context=f"MAL API anime search failed for query={query!r}",
        )

    def get_anime_details(self, anime_id: int, *, fields: str = "id,title,num_episodes,my_list_status") -> dict[str, Any]:
        return self._get_json(
            f"/anime/{anime_id}?{urlencode({'fields': fields})}",
            headers=self._build_auth_headers(require_user=True),
            error_context=f"MAL API anime details failed for anime_id={anime_id}",
        )

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
            headers={
                "Authorization": f"Bearer {self.secrets.access_token}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="PUT",
        )
        try:
            with urlopen(request) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {"status": status, "num_episodes_watched": num_watched_episodes}
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise MalApiError(f"MAL API update my_list_status failed for anime_id={anime_id}: HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise MalApiError(f"MAL API update my_list_status failed for anime_id={anime_id}: {exc.reason}") from exc

    def _get_json(self, path_or_url: str, *, headers: dict[str, str], error_context: str) -> dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self.config.mal.base_url}{path_or_url}"
        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise MalApiError(f"{error_context}: HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise MalApiError(f"{error_context}: {exc.reason}") from exc

    def _post_form(self, url: str, data: bytes) -> TokenResponse:
        basic = base64.b64encode(f"{self.secrets.client_id or ''}:{self.secrets.client_secret or ''}".encode("utf-8")).decode("ascii")
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
            with urlopen(request) as response:
                raw = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise MalApiError(f"MAL token request failed: HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise MalApiError(f"MAL token request failed: {exc.reason}") from exc
        return TokenResponse(
            access_token=raw["access_token"],
            token_type=raw.get("token_type", "Bearer"),
            expires_in=raw.get("expires_in"),
            refresh_token=raw.get("refresh_token"),
            scope=raw.get("scope"),
            raw=raw,
        )
