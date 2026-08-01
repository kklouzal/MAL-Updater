from __future__ import annotations

import json
from pathlib import Path

from .auth_failure_signals import AUTH_STYLE_SESSION_PHASES, auth_failure_remediation, classify_auth_style_failure
from .config import AppConfig
from .crunchyroll_auth import resolve_crunchyroll_state_paths
from .hidive_auth import resolve_hidive_state_paths
from .redaction import sanitize_text, sanitize_value


def load_json_dict(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def load_service_state(config: AppConfig) -> dict[str, object] | None:
    payload = load_json_dict(config.service_state_path)
    safe = sanitize_value(payload, max_depth=10, max_items=500, max_string=2_000)
    return safe if isinstance(safe, dict) else None


def provider_auth_session_residue(config: AppConfig, provider: str) -> dict[str, object] | None:
    if provider == "crunchyroll":
        session_path = resolve_crunchyroll_state_paths(config).session_state_path
        phase_key = "crunchyroll_phase"
    elif provider == "hidive":
        session_path = resolve_hidive_state_paths(config).session_state_path
        phase_key = "hidive_phase"
    else:
        return None
    payload = load_json_dict(session_path)
    if not isinstance(payload, dict):
        return None
    phase = payload.get(phase_key)
    last_error = payload.get("last_error")
    residue: dict[str, object] = {}
    if isinstance(phase, str) and phase in AUTH_STYLE_SESSION_PHASES:
        residue["session_phase"] = phase
    if isinstance(last_error, str) and last_error.strip():
        residue["session_last_error"] = sanitize_text(last_error.strip(), max_length=500)
    return residue or None


def describe_auth_failure_kind(kind_payload: dict[str, object] | None) -> str:
    if not isinstance(kind_payload, dict):
        return "auth looks degraded"
    label = kind_payload.get("label")
    if isinstance(label, str) and label.strip() and label.strip().lower() != "none":
        return label.strip()
    return "auth looks degraded"


def task_service_auth_failure(
    service_state: dict[str, object] | None,
    *,
    task_name: str,
    subject_key: str,
    subject_value: str,
    session_residue: dict[str, object] | None = None,
    min_consecutive_failures: int = 2,
) -> dict[str, object] | None:
    if not isinstance(service_state, dict):
        return None
    tasks = service_state.get("tasks")
    if not isinstance(tasks, dict):
        return None
    task_state = tasks.get(task_name)
    if not isinstance(task_state, dict):
        return None
    reason = task_state.get("failure_backoff_reason") or task_state.get("last_error")
    if not isinstance(reason, str) or not reason.strip():
        return None
    consecutive_failures = task_state.get("failure_backoff_consecutive_failures", 0)
    if not isinstance(consecutive_failures, (int, float)) or int(consecutive_failures) < min_consecutive_failures:
        return None
    auth_failure_kind = classify_auth_style_failure(reason, session_residue=session_residue)
    if not isinstance(auth_failure_kind, dict):
        return None
    remediation = auth_failure_remediation(auth_failure_kind)
    payload: dict[str, object] = {
        subject_key: subject_value,
        "reason": sanitize_text(reason.strip(), max_length=500),
        "consecutive_failures": int(consecutive_failures),
        "auth_failure_kind": auth_failure_kind.get("kind"),
        "auth_failure_label": auth_failure_kind.get("label"),
        "auth_remediation_kind": remediation.get("remediation_kind"),
        "auth_remediation_detail": remediation.get("detail"),
    }
    if isinstance(task_state.get("failure_backoff_until"), str):
        payload["failure_backoff_until"] = task_state["failure_backoff_until"]
    if isinstance(task_state.get("failure_backoff_remaining_seconds"), (int, float)):
        payload["failure_backoff_remaining_seconds"] = int(task_state["failure_backoff_remaining_seconds"])
    if isinstance(task_state.get("failure_backoff_class"), str):
        payload["failure_backoff_class"] = task_state["failure_backoff_class"]
    if isinstance(task_state.get("failure_backoff_floor_seconds"), (int, float)):
        payload["failure_backoff_floor_seconds"] = int(task_state["failure_backoff_floor_seconds"])
    if isinstance(session_residue, dict):
        payload.update(session_residue)
    safe_payload = sanitize_value(payload, max_depth=6, max_items=50, max_string=500)
    return safe_payload if isinstance(safe_payload, dict) else None


def provider_service_auth_failure(
    service_state: dict[str, object] | None,
    *,
    provider: str,
    config: AppConfig,
    min_consecutive_failures: int = 2,
) -> dict[str, object] | None:
    session_residue = provider_auth_session_residue(config, provider)
    return task_service_auth_failure(
        service_state,
        task_name=f"sync_fetch_{provider}",
        subject_key="provider",
        subject_value=provider,
        session_residue=session_residue,
        min_consecutive_failures=min_consecutive_failures,
    )


def mal_service_auth_failure(
    service_state: dict[str, object] | None,
    *,
    min_consecutive_failures: int = 2,
) -> dict[str, object] | None:
    return task_service_auth_failure(
        service_state,
        task_name="mal_refresh",
        subject_key="provider",
        subject_value="mal",
        min_consecutive_failures=min_consecutive_failures,
    )


def mal_bootstrap_auth_issue(
    service_state: dict[str, object] | None,
    *,
    min_consecutive_failures: int = 2,
) -> dict[str, object] | None:
    service_failure = mal_service_auth_failure(service_state, min_consecutive_failures=min_consecutive_failures)
    if not isinstance(service_failure, dict):
        return None
    return {
        **service_failure,
        "source": "service_state",
    }


def provider_bootstrap_auth_issue(
    *,
    provider: str,
    config: AppConfig,
    service_state: dict[str, object] | None,
) -> dict[str, object] | None:
    service_failure = provider_service_auth_failure(service_state, provider=provider, config=config)
    if isinstance(service_failure, dict):
        return {
            **service_failure,
            "source": "service_state",
        }

    session_residue = provider_auth_session_residue(config, provider)
    if not isinstance(session_residue, dict):
        return None
    auth_failure_kind = classify_auth_style_failure("", session_residue=session_residue)
    if not isinstance(auth_failure_kind, dict):
        return None

    reason = session_residue.get("session_last_error")
    if not isinstance(reason, str) or not reason.strip():
        session_phase = session_residue.get("session_phase")
        if isinstance(session_phase, str) and session_phase:
            reason = f"session phase {session_phase}"
        else:
            reason = "provider session state looks auth-degraded"

    remediation = auth_failure_remediation(auth_failure_kind)
    payload = {
        "provider": provider,
        "reason": sanitize_text(reason, max_length=500),
        "source": "session_state",
        "auth_failure_kind": auth_failure_kind.get("kind"),
        "auth_failure_label": auth_failure_kind.get("label"),
        "auth_remediation_kind": remediation.get("remediation_kind"),
        "auth_remediation_detail": remediation.get("detail"),
        **session_residue,
    }
    safe_payload = sanitize_value(payload, max_depth=6, max_items=50, max_string=500)
    return safe_payload if isinstance(safe_payload, dict) else None


__all__ = [
    "describe_auth_failure_kind",
    "load_json_dict",
    "load_service_state",
    "mal_bootstrap_auth_issue",
    "mal_service_auth_failure",
    "provider_auth_session_residue",
    "provider_bootstrap_auth_issue",
    "provider_service_auth_failure",
    "task_service_auth_failure",
]
