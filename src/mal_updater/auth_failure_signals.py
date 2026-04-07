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



def looks_auth_style_failure(reason: str, *, session_residue: Mapping[str, object] | None = None) -> bool:
    return classify_auth_style_failure(reason, session_residue=session_residue) is not None
