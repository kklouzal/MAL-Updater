from __future__ import annotations

from collections.abc import Mapping

AUTH_FAILURE_KIND_LABELS = {
    "invalid_grant": "revoked or invalid refresh/auth token",
    "missing_refresh_material": "missing refresh material",
    "malformed_token_payload": "malformed token payload",
    "login_failure": "provider login/bootstrap failure",
    "session_auth_failed": "persisted auth-failed session residue",
    "http_auth": "HTTP auth rejection",
    "generic_auth": "generic auth-style failure",
}

AUTH_FAILURE_REMEDIATION = {
    "invalid_grant": {
        "remediation_kind": "refresh-token-invalidated",
        "detail": "stage fresh auth material because the existing refresh/auth token looks revoked, expired, or otherwise invalid",
    },
    "missing_refresh_material": {
        "remediation_kind": "refresh-material-missing",
        "detail": "restage the missing refresh material before treating unattended auth as healthy again",
    },
    "malformed_token_payload": {
        "remediation_kind": "token-payload-malformed",
        "detail": "replace the malformed persisted auth payload with a freshly bootstrapped token set",
    },
    "login_failure": {
        "remediation_kind": "login-bootstrap-failed",
        "detail": "repeat the auth/bootstrap flow because the previous login/bootstrap attempt did not complete cleanly",
    },
    "session_auth_failed": {
        "remediation_kind": "session-auth-failed",
        "detail": "re-bootstrap the persisted session/auth state before trusting unattended fetches again",
    },
    "http_auth": {
        "remediation_kind": "http-auth-rejected",
        "detail": "refresh the staged auth/session material because the provider is actively rejecting the current credentials or tokens",
    },
    "generic_auth": {
        "remediation_kind": "generic-auth-degraded",
        "detail": "repeat the conservative reauth/rebootstrap flow because persisted auth state still looks degraded",
    },
}

AUTH_STYLE_FAILURE_MARKERS = (
    "http 401",
    "http 403",
    "unauthor",
    "forbidden",
    "auth_failed",
    "invalid_grant",
    "invalid token",
    "expired token",
    "token refresh",
    "refresh token",
    "missing mal refresh material",
    "missing crunchyroll refresh token",
    "missing hidive refresh token",
    "credential_rebootstrap",
    "login failed",
    "login did not return",
    "did not return a json object",
    "did not return both access_token and refresh_token",
    "did not return both authorisationtoken and refreshtoken",
    "did not return authorisationtoken",
    "did not return refreshtoken",
    "bearer",
)

AUTH_STYLE_SESSION_PHASES = {
    "auth_failed",
    "auth_retrying_with_refresh_token",
    "auth_retrying_with_credentials",
}


def classify_auth_style_failure(
    reason: str,
    *,
    session_residue: Mapping[str, object] | None = None,
) -> dict[str, str] | None:
    lowered = reason.lower()
    session_phase = session_residue.get("session_phase") if isinstance(session_residue, Mapping) else None
    session_last_error = session_residue.get("session_last_error") if isinstance(session_residue, Mapping) else None
    lowered_session_error = session_last_error.lower() if isinstance(session_last_error, str) else ""
    combined = "\n".join(part for part in (lowered, lowered_session_error) if part)

    if "invalid_grant" in combined or "revoked" in combined:
        return {
            "kind": "invalid_grant",
            "label": AUTH_FAILURE_KIND_LABELS["invalid_grant"],
        }
    if any(
        marker in combined
        for marker in (
            "missing mal refresh material",
            "missing crunchyroll refresh token",
            "missing hidive refresh token",
            "missing refresh token",
            "missing refresh material",
        )
    ):
        return {
            "kind": "missing_refresh_material",
            "label": AUTH_FAILURE_KIND_LABELS["missing_refresh_material"],
        }
    if any(
        marker in combined
        for marker in (
            "did not return a json object",
            "did not return both access_token and refresh_token",
            "did not return both authorisationtoken and refreshtoken",
            "did not return authorisationtoken",
            "did not return refreshtoken",
            "malformed-token-payload",
            "malformed token payload",
        )
    ):
        return {
            "kind": "malformed_token_payload",
            "label": AUTH_FAILURE_KIND_LABELS["malformed_token_payload"],
        }
    if any(marker in combined for marker in ("http 401", "http 403", "unauthor", "forbidden", "bearer", "invalid token", "expired token")):
        return {
            "kind": "http_auth",
            "label": AUTH_FAILURE_KIND_LABELS["http_auth"],
        }
    if isinstance(session_phase, str) and session_phase in AUTH_STYLE_SESSION_PHASES:
        return {
            "kind": "session_auth_failed",
            "label": AUTH_FAILURE_KIND_LABELS["session_auth_failed"],
        }
    if any(marker in combined for marker in ("login failed", "login did not return", "token refresh", "refresh token", "credential_rebootstrap")):
        return {
            "kind": "login_failure",
            "label": AUTH_FAILURE_KIND_LABELS["login_failure"],
        }
    if any(marker in combined for marker in AUTH_STYLE_FAILURE_MARKERS):
        return {
            "kind": "generic_auth",
            "label": AUTH_FAILURE_KIND_LABELS["generic_auth"],
        }
    return None



def auth_failure_remediation(kind_payload: Mapping[str, object] | None) -> dict[str, str]:
    kind = kind_payload.get("kind") if isinstance(kind_payload, Mapping) else None
    if isinstance(kind, str):
        remediation = AUTH_FAILURE_REMEDIATION.get(kind)
        if isinstance(remediation, dict):
            return dict(remediation)
    return dict(AUTH_FAILURE_REMEDIATION["generic_auth"])



def looks_auth_style_failure(reason: str, *, session_residue: Mapping[str, object] | None = None) -> bool:
    return classify_auth_style_failure(reason, session_residue=session_residue) is not None
