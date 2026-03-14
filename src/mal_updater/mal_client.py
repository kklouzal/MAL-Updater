from __future__ import annotations

import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Any
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

    def generate_pkce_pair(self) -> OAuthPkcePair:
        verifier = secrets.token_urlsafe(64)[:96]
        digest = hashlib.sha256(verifier.encode("utf-8")).digest()
        challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return OAuthPkcePair(code_verifier=verifier, code_challenge=challenge)

    def build_authorization_url(self, code_challenge: str, state: str | None = None) -> str:
        if not self.secrets.client_id:
            raise MalApiError("MAL client_id is not configured")
        query = {
            "response_type": "code",
            "client_id": self.secrets.client_id,
            "redirect_uri": self.config.mal.redirect_uri,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        if state:
            query["state"] = state
        return f"{self.config.mal.auth_url}?{urlencode(query)}"

    def exchange_code(self, code: str, code_verifier: str) -> TokenResponse:
        if not self.secrets.client_id:
            raise MalApiError("MAL client_id is not configured")
        if not self.secrets.client_secret:
            raise MalApiError("MAL client_secret is not configured")
        payload = urlencode(
            {
                "grant_type": "authorization_code",
                "client_id": self.secrets.client_id,
                "client_secret": self.secrets.client_secret,
                "code": code,
                "code_verifier": code_verifier,
                "redirect_uri": self.config.mal.redirect_uri,
            }
        ).encode("utf-8")
        return self._post_form(self.config.mal.token_url, payload)

    def refresh_access_token(self, refresh_token: str | None = None) -> TokenResponse:
        token = refresh_token or self.secrets.refresh_token
        if not token:
            raise MalApiError("MAL refresh_token is not configured")
        if not self.secrets.client_id:
            raise MalApiError("MAL client_id is not configured")
        if not self.secrets.client_secret:
            raise MalApiError("MAL client_secret is not configured")
        payload = urlencode(
            {
                "grant_type": "refresh_token",
                "refresh_token": token,
                "client_id": self.secrets.client_id,
                "client_secret": self.secrets.client_secret,
            }
        ).encode("utf-8")
        return self._post_form(self.config.mal.token_url, payload)

    def get_my_user(self, access_token: str | None = None) -> dict[str, Any]:
        token = access_token or self.secrets.access_token
        if not token:
            raise MalApiError("MAL access_token is not configured")
        request = Request(
            f"{self.config.mal.base_url}/users/@me",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            method="GET",
        )
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))

    def _post_form(self, url: str, data: bytes) -> TokenResponse:
        request = Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        with urlopen(request) as response:
            raw = json.loads(response.read().decode("utf-8"))
        return TokenResponse(
            access_token=raw["access_token"],
            token_type=raw.get("token_type", "Bearer"),
            expires_in=raw.get("expires_in"),
            refresh_token=raw.get("refresh_token"),
            scope=raw.get("scope"),
            raw=raw,
        )
