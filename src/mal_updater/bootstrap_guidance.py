from __future__ import annotations

import importlib.util
import io
import json
import platform
import shlex
import shutil
import stat
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from . import health_report
from . import review_queue_support as _review_queue_support
from . import service_systemd_status
from .auth_remediation import (
    AuthRemediationDescriptor,
    guidance_command_fields as _auth_guidance_command_fields,
    mal_missing_auth_descriptor,
    mal_rebootstrap_auth_descriptor,
    provider_missing_state_descriptor,
    provider_rebootstrap_auth_descriptor,
)
from .config import AppConfig, load_mal_secrets, mal_callback_bind_warning
from .crunchyroll_auth import load_crunchyroll_credentials, resolve_crunchyroll_state_paths
from .hidive_auth import load_hidive_credentials, resolve_hidive_state_paths
from .service_auth_state import (
    load_service_state,
    mal_bootstrap_auth_issue,
    provider_bootstrap_auth_issue,
)
from .service_units import systemd_unit_path_context


def _parse_sqlite_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for parser in (
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
    ):
        try:
            parsed = parser(text)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _runtime_initialization_status(config) -> dict[str, object]:
    required_paths = {
        "config_dir": config.config_dir,
        "secrets_dir": config.secrets_dir,
        "data_dir": config.data_dir,
        "state_dir": config.state_dir,
        "cache_dir": config.cache_dir,
        "db_path": config.db_path,
    }
    missing = [name for name, path in required_paths.items() if not path.exists()]
    return {
        "ready": not missing,
        "missing": missing,
        "command": "PYTHONPATH=src python3 -m mal_updater.cli init",
    }


def _secrets_dir_permission_status(config) -> dict[str, object]:
    secrets_dir = config.secrets_dir
    if not secrets_dir.exists():
        return {
            "exists": False,
            "mode_octal": None,
            "restrictive": None,
            "details": f"Secrets dir {secrets_dir} does not exist yet; run init before staging secrets.",
            "command": "PYTHONPATH=src python3 -m mal_updater.cli init",
        }

    mode = stat.S_IMODE(secrets_dir.stat().st_mode)
    restrictive = mode == 0o700
    details = (
        f"Secrets dir {secrets_dir} permissions are mode 0700."
        if restrictive
        else f"Secrets dir {secrets_dir} is not mode 0700; tighten it to owner-only read/write/execute before staging long-lived credentials/tokens."
    )
    return {
        "exists": True,
        "mode_octal": f"0o{mode:03o}",
        "restrictive": restrictive,
        "details": details,
        "command": f"chmod 700 {shlex.quote(str(secrets_dir))}",
    }


def _guidance_command_fields(
    *,
    command: str | None,
    reason_code: str | None = None,
    automation_safe: bool | None = None,
    requires_auth_interaction: bool | None = None,
    auth_failure_kind: str | None = None,
    auth_remediation_kind: str | None = None,
) -> dict[str, object]:
    return _auth_guidance_command_fields(
        command=command,
        reason_code=reason_code,
        automation_safe=automation_safe,
        requires_auth_interaction=requires_auth_interaction,
        auth_failure_kind=auth_failure_kind,
        auth_remediation_kind=auth_remediation_kind,
    )


def _auth_command_string(command: AuthRemediationDescriptor | str, default: AuthRemediationDescriptor) -> str:
    if isinstance(command, AuthRemediationDescriptor):
        return command.command
    if isinstance(command, str) and command:
        return command
    return default.command


def _provider_from_refresh_command_args(command_args: object) -> str | None:
    if not isinstance(command_args, list) or not command_args:
        return None
    if command_args[0] == "crunchyroll-fetch-snapshot":
        return "crunchyroll"
    if len(command_args) >= 2 and command_args[0] == "sync-source" and isinstance(command_args[1], str):
        return str(command_args[1])
    if command_args[0] == "provider-fetch-snapshot":
        for index, part in enumerate(command_args[:-1]):
            if part == "--provider" and isinstance(command_args[index + 1], str):
                return str(command_args[index + 1])
    return None


def _normalized_provider_fetch_command_args(provider: str, command_args: list[str]) -> list[str]:
    """Return provider-generic fetch args while accepting legacy persisted forms."""
    normalized = ["provider-fetch-snapshot", "--provider", provider]
    if not command_args:
        return normalized
    first = command_args[0]
    if first == "provider-fetch-snapshot":
        index = 1
        while index < len(command_args):
            part = command_args[index]
            if part == "--provider" and index + 1 < len(command_args):
                index += 2
                continue
            normalized.append(part)
            index += 1
        return normalized
    if first == "crunchyroll-fetch-snapshot":
        normalized.extend(command_args[1:])
        return normalized
    if first == "sync-source":
        normalized.extend(command_args[2:])
        return normalized
    normalized.extend(command_args[1:])
    return normalized


def _provider_task_clears_health_refresh_recommendation(
    task_state: dict[str, object],
    *,
    reason_code: str,
    health_mtime: float,
) -> bool:
    last_full_refresh_epoch = task_state.get("last_successful_full_refresh_epoch")
    if isinstance(last_full_refresh_epoch, (int, float)) and float(last_full_refresh_epoch) >= float(health_mtime):
        return True
    if reason_code != "refresh_ingested_snapshot":
        return False
    if task_state.get("last_status") != "ok":
        return False
    last_finished_at = _parse_sqlite_timestamp(task_state.get("last_finished_at"))
    if last_finished_at is not None and last_finished_at.timestamp() >= float(health_mtime):
        return True
    last_run_epoch = task_state.get("last_run_epoch")
    if isinstance(last_run_epoch, (int, float)) and float(last_run_epoch) >= float(health_mtime):
        return True
    return False


def _provider_bootstrap_health_refresh_recommendation(
    config: AppConfig,
    *,
    provider: str,
    service_state: dict[str, object] | None,
) -> dict[str, object] | None:
    path = config.health_latest_json_path
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    maintenance = payload.get("maintenance")
    if not isinstance(maintenance, dict):
        return None
    commands = maintenance.get("recommended_commands")
    if not isinstance(commands, list):
        return None
    try:
        health_mtime = path.stat().st_mtime
    except OSError:
        return None
    task_state = None
    if isinstance(service_state, dict):
        tasks = service_state.get("tasks")
        if isinstance(tasks, dict):
            candidate_state = tasks.get(f"sync_fetch_{provider}")
            if isinstance(candidate_state, dict):
                task_state = candidate_state
    for command in commands:
        if not isinstance(command, dict):
            continue
        reason_code = command.get("reason_code")
        if reason_code not in {"refresh_ingested_snapshot", "refresh_full_snapshot"}:
            continue
        if _provider_from_refresh_command_args(command.get("command_args")) != provider:
            continue
        command_args = command.get("command_args")
        if not isinstance(command_args, list) or not all(isinstance(part, str) for part in command_args):
            continue
        normalized_command_args = _normalized_provider_fetch_command_args(provider, list(command_args))
        if reason_code == "refresh_full_snapshot" and "--full-refresh" not in normalized_command_args:
            normalized_command_args.append("--full-refresh")
        if isinstance(task_state, dict) and _provider_task_clears_health_refresh_recommendation(
            task_state,
            reason_code=str(reason_code),
            health_mtime=health_mtime,
        ):
            continue
        detail = command.get("detail") if isinstance(command.get("detail"), str) else None
        return {
            "reason_code": str(reason_code),
            "command_args": normalized_command_args,
            "command": _review_queue_support._build_review_queue_command(normalized_command_args),
            "detail": detail,
        }
    return None


def _bootstrap_health_review_recommendations(config: AppConfig) -> list[dict[str, object]]:
    path = config.health_latest_json_path
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    maintenance = payload.get("maintenance")
    if not isinstance(maintenance, dict):
        return []
    commands = maintenance.get("recommended_commands")
    if not isinstance(commands, list):
        return []

    review_reason_codes = {
        "refresh_mapping_review_worklist",
        "refresh_mapping_review_queue",
        "refresh_mapping_review_backlog",
    }
    recommendations: list[dict[str, object]] = []
    seen_commands: set[str] = set()
    for command in commands:
        if not isinstance(command, dict):
            continue
        reason_code = command.get("reason_code")
        if reason_code not in review_reason_codes:
            continue
        command_args = command.get("command_args")
        if not isinstance(command_args, list) or not all(isinstance(part, str) for part in command_args):
            continue
        command_string = _review_queue_support._build_review_queue_command(list(command_args))
        if command_string in seen_commands:
            continue
        seen_commands.add(command_string)
        detail = command.get("detail") if isinstance(command.get("detail"), str) else None
        recommendations.append(
            {
                "reason_code": str(reason_code),
                "command_args": list(command_args),
                "command": command_string,
                "detail": detail,
                "automation_safe": command.get("automation_safe") if isinstance(command.get("automation_safe"), bool) else True,
                "requires_auth_interaction": command.get("requires_auth_interaction") if isinstance(command.get("requires_auth_interaction"), bool) else False,
            }
        )
    return recommendations


def _provider_bootstrap_guidance_status(
    *,
    provider_name: str,
    credentials_present: bool,
    session_present: bool,
    transport_ready: bool,
    bootstrap_command: AuthRemediationDescriptor | str,
    auth_issue: dict[str, object] | None = None,
    health_refresh_recommendation: dict[str, object] | None = None,
) -> dict[str, object]:
    title = provider_name.capitalize()
    intended = credentials_present or session_present
    missing_descriptor = provider_missing_state_descriptor(provider_name)
    bootstrap_command_string = _auth_command_string(bootstrap_command, missing_descriptor)

    if not intended:
        return {
            "mode": "not-configured",
            "intended": False,
            "partially_staged": False,
            "ready": False,
            "details": f"{title} is not staged yet, so it is optional until you decide to enable that provider.",
            **_guidance_command_fields(command=None),
        }

    if credentials_present and not session_present:
        return {
            "mode": "credentials-staged-awaiting-bootstrap",
            "intended": True,
            "partially_staged": True,
            "ready": False,
            "details": missing_descriptor.bootstrap_operation_details(),
            **missing_descriptor.bootstrap_guidance_command_fields(bootstrap_command_string),
        }

    if session_present and not credentials_present:
        return {
            "mode": "session-staged-missing-credentials",
            "intended": True,
            "partially_staged": True,
            "ready": False,
            "details": f"{title} session state exists without matching staged credentials; restore/stage credentials before treating this provider as healthy for unattended fetches.",
            **_guidance_command_fields(command=None),
        }

    if credentials_present and session_present and isinstance(auth_issue, dict):
        rebootstrap_descriptor = provider_rebootstrap_auth_descriptor(provider_name, auth_issue)
        return {
            "mode": "auth-degraded-needs-rebootstrap",
            "intended": True,
            "partially_staged": True,
            "ready": False,
            "details": rebootstrap_descriptor.bootstrap_operation_details(),
            **rebootstrap_descriptor.bootstrap_guidance_command_fields(bootstrap_command_string),
            **rebootstrap_descriptor.bootstrap_remediation_fields(),
        }

    if credentials_present and session_present and isinstance(health_refresh_recommendation, dict):
        refresh_command = health_refresh_recommendation.get("command") if isinstance(health_refresh_recommendation.get("command"), str) else None
        refresh_detail = health_refresh_recommendation.get("detail") if isinstance(health_refresh_recommendation.get("detail"), str) else None
        refresh_reason_code = str(health_refresh_recommendation.get("reason_code") or "") or "refresh_full_snapshot"
        if refresh_reason_code == "refresh_ingested_snapshot":
            details = (
                f"{title} credentials and session state are staged, so unattended fetches are bootstrapped, but the latest health artifact still recommends refreshing the ingested provider snapshot before treating cached provider state as current."
            )
            operation_mode = "ready-health-recommends-snapshot-refresh"
        else:
            details = (
                f"{title} credentials and session state are staged, so unattended fetches are bootstrapped, but the latest health artifact still recommends a conservative full refresh before treating cached provider coverage as current."
            )
            operation_mode = "ready-health-recommends-full-refresh"
            refresh_reason_code = "refresh_full_snapshot"
        if refresh_detail:
            details += f" Latest maintenance signal: {refresh_detail}"
        return {
            "mode": operation_mode,
            "intended": True,
            "partially_staged": False,
            "ready": True,
            "details": details,
            **_guidance_command_fields(
                command=refresh_command,
                reason_code=refresh_reason_code,
                automation_safe=True,
                requires_auth_interaction=False,
            ),
        }

    if not transport_ready:
        return {
            "mode": "blocked-missing-transport",
            "intended": True,
            "partially_staged": True,
            "ready": False,
            "details": f"{title} bootstrap state is staged but required transport support is missing on this host, so finish transport setup before relying on unattended fetches.",
            **_guidance_command_fields(command=None),
        }

    if credentials_present and session_present:
        return {
            "mode": "ready-for-unattended",
            "intended": True,
            "partially_staged": False,
            "ready": True,
            "details": f"{title} credentials and session state are staged, so this provider is ready for unattended daemon fetches.",
            **_guidance_command_fields(command=None),
        }

    return {
        "mode": "provider-staged-not-ready",
        "intended": True,
        "partially_staged": True,
        "ready": False,
        "details": f"{title} has some staged bootstrap state but is not fully ready yet; finish provider bootstrap before expecting unattended fetches.",
        **(
            missing_descriptor.bootstrap_guidance_command_fields(bootstrap_command_string)
            if credentials_present
            else _guidance_command_fields(command=None)
        ),
    }


def _mal_bootstrap_guidance_status(
    *,
    client_id_present: bool,
    oauth_present: bool,
    auth_command: AuthRemediationDescriptor | str,
    auth_issue: dict[str, object] | None = None,
) -> dict[str, object]:
    auth_command_string = _auth_command_string(auth_command, mal_missing_auth_descriptor())

    if not client_id_present:
        return {
            "mode": "client-id-missing",
            "ready": False,
            "details": "MyAnimeList client id is not staged yet, so MAL OAuth cannot be completed.",
            **_guidance_command_fields(command=None),
        }

    if not oauth_present:
        missing_descriptor = mal_missing_auth_descriptor()
        return {
            "mode": "oauth-missing",
            "ready": False,
            "details": missing_descriptor.bootstrap_operation_details(),
            **missing_descriptor.bootstrap_guidance_command_fields(auth_command_string),
        }

    if isinstance(auth_issue, dict):
        rebootstrap_descriptor = mal_rebootstrap_auth_descriptor(auth_issue)
        return {
            "mode": "auth-degraded-needs-reauth",
            "ready": False,
            "details": rebootstrap_descriptor.bootstrap_operation_details(),
            **rebootstrap_descriptor.bootstrap_guidance_command_fields(auth_command_string),
            **rebootstrap_descriptor.bootstrap_remediation_fields(),
        }

    return {
        "mode": "ready",
        "ready": True,
        "details": "MyAnimeList client id and OAuth tokens are staged, so MAL auth is ready for unattended sync.",
        **_guidance_command_fields(command=None),
    }


def _bootstrap_operation_mode_status(
    *,
    runtime_initialized: bool,
    python_available: bool,
    systemctl_available: bool,
    mal_oauth_present: bool,
    crunchyroll_credentials_present: bool,
    crunchyroll_session_present: bool,
    crunchyroll_transport_ready: bool,
    hidive_credentials_present: bool,
    hidive_session_present: bool,
) -> dict[str, object]:
    provider_states = {
        "crunchyroll": {
            "credentials_present": crunchyroll_credentials_present,
            "session_present": crunchyroll_session_present,
            "transport_ready": crunchyroll_transport_ready,
        },
        "hidive": {
            "credentials_present": hidive_credentials_present,
            "session_present": hidive_session_present,
            "transport_ready": True,
        },
    }
    intended_provider_count = sum(
        1
        for payload in provider_states.values()
        if payload["credentials_present"] or payload["session_present"]
    )
    ready_provider_count = sum(
        1
        for payload in provider_states.values()
        if payload["credentials_present"] and payload["session_present"] and payload["transport_ready"]
    )
    partially_staged_provider_count = max(0, intended_provider_count - ready_provider_count)
    unattended_ready = runtime_initialized and mal_oauth_present and intended_provider_count > 0 and partially_staged_provider_count == 0

    base_payload = {
        "intended_provider_count": intended_provider_count,
        "ready_provider_count": ready_provider_count,
        "partially_staged_provider_count": partially_staged_provider_count,
        "mal_oauth_present": mal_oauth_present,
        "runtime_initialized": runtime_initialized,
        "unattended_ready": unattended_ready,
    }

    if not python_available:
        return {
            **base_payload,
            "mode": "cli-unavailable",
            "manual_foreground_acceptable": False,
            "daemon_preferred": False,
            "daemon_expected": False,
            "details": "Python is unavailable, so neither foreground CLI operation nor the repo-owned daemon path is usable yet.",
        }

    if not systemctl_available:
        return {
            **base_payload,
            "mode": "manual-only-host",
            "manual_foreground_acceptable": True,
            "daemon_preferred": False,
            "daemon_expected": False,
            "details": "This host does not expose systemctl, so manual foreground CLI operation is the acceptable runtime model here.",
        }

    if not runtime_initialized:
        return {
            **base_payload,
            "mode": "bootstrap-manual-acceptable",
            "manual_foreground_acceptable": True,
            "daemon_preferred": True,
            "daemon_expected": False,
            "details": "Manual foreground CLI operation is acceptable during bootstrap and spot checks; initialize the runtime and install the repo-owned user-systemd daemon once you want unattended background sync.",
        }

    if intended_provider_count == 0 and not mal_oauth_present:
        return {
            **base_payload,
            "mode": "bootstrap-manual-acceptable",
            "manual_foreground_acceptable": True,
            "daemon_preferred": True,
            "daemon_expected": False,
            "details": "Manual foreground CLI operation is acceptable during bootstrap and spot checks; no provider intent is staged yet, so finish MAL/provider bootstrap before treating unattended daemon sync as expected.",
        }

    if not unattended_ready:
        return {
            **base_payload,
            "mode": "bootstrap-provider-staged",
            "manual_foreground_acceptable": True,
            "daemon_preferred": True,
            "daemon_expected": False,
            "details": "Some bootstrap state is already staged, but unattended daemon sync is not ready yet; finish MAL OAuth and any partially staged provider bootstrap before treating the repo-owned user-systemd daemon as the expected background path.",
        }

    return {
        **base_payload,
        "mode": "daemon-expected-for-unattended",
        "manual_foreground_acceptable": True,
        "daemon_preferred": True,
        "daemon_expected": True,
        "details": "MAL auth and all currently intended providers are bootstrapped, so manual foreground CLI runs remain valid for validation/recovery, but the repo-owned user-systemd daemon is the expected path for unattended background sync.",
    }




def build_bootstrap_audit_payload(config: AppConfig) -> dict[str, object]:
    secrets = load_mal_secrets(config)
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    crunchyroll_state = resolve_crunchyroll_state_paths(config)
    hidive_credentials = load_hidive_credentials(config)
    hidive_state = resolve_hidive_state_paths(config)
    runtime_initialization = _runtime_initialization_status(config)
    secrets_dir_permissions = _secrets_dir_permission_status(config)
    automation_installation = service_systemd_status.build_automation_installation_status(
        config.project_root,
        path_context=systemd_unit_path_context(config),
    )
    service_state = load_service_state(config)

    dependency_checks = {
        "python3": shutil.which("python3") is not None,
        "systemctl": shutil.which("systemctl") is not None,
        "curl_cffi": importlib.util.find_spec("curl_cffi") is not None,
    }
    missing_dependencies = [name for name, present in dependency_checks.items() if not present]

    crunchyroll_credentials_present = bool(crunchyroll_credentials.username) and bool(crunchyroll_credentials.password)
    crunchyroll_session_present = crunchyroll_state.refresh_token_path.exists() and crunchyroll_state.device_id_path.exists()
    hidive_credentials_present = bool(hidive_credentials.username) and bool(hidive_credentials.password)
    hidive_session_present = hidive_state.access_token_path.exists() and hidive_state.refresh_token_path.exists()
    mal_app_present = bool(secrets.client_id)
    mal_oauth_present = bool(secrets.access_token) and bool(secrets.refresh_token)
    mal_auth_descriptor = mal_missing_auth_descriptor()
    crunchyroll_bootstrap_descriptor = provider_missing_state_descriptor("crunchyroll")
    hidive_bootstrap_descriptor = provider_missing_state_descriptor("hidive")
    mal_auth_issue = mal_bootstrap_auth_issue(service_state)
    provider_auth_issues = {
        provider: payload
        for provider in ("crunchyroll", "hidive")
        if (payload := provider_bootstrap_auth_issue(provider=provider, config=config, service_state=service_state)) is not None
    }
    provider_health_refresh_recommendations = {
        provider: payload
        for provider in ("crunchyroll", "hidive")
        if (payload := _provider_bootstrap_health_refresh_recommendation(config, provider=provider, service_state=service_state)) is not None
    }
    health_review_recommendations = _bootstrap_health_review_recommendations(config)
    mal_guidance = _mal_bootstrap_guidance_status(
        client_id_present=mal_app_present,
        oauth_present=mal_oauth_present,
        auth_command=mal_auth_descriptor,
        auth_issue=mal_auth_issue,
    )
    crunchyroll_guidance = _provider_bootstrap_guidance_status(
        provider_name="crunchyroll",
        credentials_present=crunchyroll_credentials_present,
        session_present=crunchyroll_session_present,
        transport_ready=dependency_checks["curl_cffi"],
        bootstrap_command=crunchyroll_bootstrap_descriptor,
        auth_issue=provider_auth_issues.get("crunchyroll"),
        health_refresh_recommendation=provider_health_refresh_recommendations.get("crunchyroll"),
    )
    hidive_guidance = _provider_bootstrap_guidance_status(
        provider_name="hidive",
        credentials_present=hidive_credentials_present,
        session_present=hidive_session_present,
        transport_ready=True,
        bootstrap_command=hidive_bootstrap_descriptor,
        auth_issue=provider_auth_issues.get("hidive"),
        health_refresh_recommendation=provider_health_refresh_recommendations.get("hidive"),
    )
    operation_modes = _bootstrap_operation_mode_status(
        runtime_initialized=bool(runtime_initialization["ready"]),
        python_available=dependency_checks["python3"],
        systemctl_available=dependency_checks["systemctl"],
        mal_oauth_present=mal_oauth_present and not isinstance(mal_auth_issue, dict),
        crunchyroll_credentials_present=crunchyroll_credentials_present,
        crunchyroll_session_present=crunchyroll_session_present and not isinstance(provider_auth_issues.get("crunchyroll"), dict),
        crunchyroll_transport_ready=dependency_checks["curl_cffi"],
        hidive_credentials_present=hidive_credentials_present,
        hidive_session_present=hidive_session_present and not isinstance(provider_auth_issues.get("hidive"), dict),
    )

    onboarding_steps: list[dict[str, object]] = []

    def add_onboarding_step(
        *,
        step: str,
        details: str,
        user_action_required: bool,
        command: str | None = None,
        command_args: list[str] | None = None,
        applies_to: str | None = None,
        reason_code: str | None = None,
        automation_safe: bool | None = None,
        requires_auth_interaction: bool | None = None,
        auth_failure_kind: str | None = None,
        auth_remediation_kind: str | None = None,
    ) -> None:
        payload = {
            "step": step,
            "status": "missing",
            "user_action_required": user_action_required,
            "command": command,
            "command_args": command_args or [],
            "applies_to": applies_to,
            "details": details,
            "reason_code": reason_code or step.replace("-", "_"),
        }
        if isinstance(automation_safe, bool):
            payload["automation_safe"] = automation_safe
        if isinstance(requires_auth_interaction, bool):
            payload["requires_auth_interaction"] = requires_auth_interaction
        if isinstance(auth_failure_kind, str) and auth_failure_kind:
            payload["auth_failure_kind"] = auth_failure_kind
        if isinstance(auth_remediation_kind, str) and auth_remediation_kind:
            payload["auth_remediation_kind"] = auth_remediation_kind
        onboarding_steps.append(payload)

    if not runtime_initialization["ready"]:
        missing_runtime = runtime_initialization.get("missing") if isinstance(runtime_initialization.get("missing"), list) else []
        add_onboarding_step(
            step="initialize-runtime",
            details="Create the external runtime directories and SQLite database before staging secrets or running sync commands. Missing: "
            + ", ".join(str(item) for item in missing_runtime),
            user_action_required=True,
            command="PYTHONPATH=src python3 -m mal_updater.cli init",
            command_args=["PYTHONPATH=src", "python3", "-m", "mal_updater.cli", "init"],
            applies_to="runtime",
            automation_safe=True,
            requires_auth_interaction=False,
        )
    if not dependency_checks["python3"]:
        add_onboarding_step(
            step="install-python",
            details="Install Python 3.10+ so the mal-updater console script and CLI can run.",
            user_action_required=True,
            applies_to="host",
        )
    if not mal_app_present:
        add_onboarding_step(
            step="create-mal-app",
            details=f"Create a MyAnimeList API app and record its client id at {secrets.client_id_path}. Configure redirect URI {config.mal.redirect_uri} in the MAL app settings.",
            user_action_required=True,
            applies_to="mal",
        )
    if not mal_oauth_present:
        add_onboarding_step(**mal_auth_descriptor.bootstrap_onboarding_step_fields())
    elif isinstance(mal_auth_issue, dict):
        add_onboarding_step(**mal_rebootstrap_auth_descriptor(mal_auth_issue).bootstrap_onboarding_step_fields())
    if not crunchyroll_credentials_present:
        add_onboarding_step(
            step="stage-crunchyroll-credentials",
            details=f"Store Crunchyroll credentials at {crunchyroll_credentials.username_path} and {crunchyroll_credentials.password_path}.",
            user_action_required=True,
            applies_to="crunchyroll",
        )
    if not crunchyroll_session_present:
        add_onboarding_step(**crunchyroll_bootstrap_descriptor.bootstrap_onboarding_step_fields())
    elif isinstance(provider_auth_issues.get("crunchyroll"), dict):
        add_onboarding_step(
            **provider_rebootstrap_auth_descriptor(
                "crunchyroll",
                provider_auth_issues["crunchyroll"],
            ).bootstrap_onboarding_step_fields()
        )
    if not hidive_credentials_present:
        add_onboarding_step(
            step="stage-hidive-credentials",
            details=f"Store HIDIVE credentials at {hidive_credentials.username_path} and {hidive_credentials.password_path}.",
            user_action_required=True,
            applies_to="hidive",
        )
    if not hidive_session_present:
        add_onboarding_step(**hidive_bootstrap_descriptor.bootstrap_onboarding_step_fields())
    elif isinstance(provider_auth_issues.get("hidive"), dict):
        add_onboarding_step(
            **provider_rebootstrap_auth_descriptor(
                "hidive",
                provider_auth_issues["hidive"],
            ).bootstrap_onboarding_step_fields()
        )

    for provider_name, refresh_payload in provider_health_refresh_recommendations.items():
        refresh_command = refresh_payload.get("command") if isinstance(refresh_payload.get("command"), str) else None
        refresh_command_args = refresh_payload.get("command_args") if isinstance(refresh_payload.get("command_args"), list) else []
        refresh_details = refresh_payload.get("detail") if isinstance(refresh_payload.get("detail"), str) else None
        refresh_reason_code = str(refresh_payload.get("reason_code") or "") or "refresh_full_snapshot"
        provider_title = "Crunchyroll" if provider_name == "crunchyroll" else "HIDIVE" if provider_name == "hidive" else provider_name
        if refresh_reason_code == "refresh_ingested_snapshot":
            step = f"refresh-{provider_name}-snapshot"
            details = f"Run a conservative {provider_title} ingest refresh because the latest health artifact says the cached provider snapshot should be refreshed before it is treated as current."
        else:
            step = f"refresh-{provider_name}-full-snapshot"
            details = f"Run a conservative full-refresh {provider_title} ingest because the latest health artifact still reports partial provider coverage."
            refresh_reason_code = "refresh_full_snapshot"
        if refresh_details:
            details += f" Latest maintenance signal: {refresh_details}"
        add_onboarding_step(
            step=step,
            details=details,
            user_action_required=False,
            command=refresh_command,
            command_args=refresh_command_args,
            applies_to=provider_name,
            reason_code=refresh_reason_code,
            automation_safe=True,
            requires_auth_interaction=False,
        )
    if mal_oauth_present and not isinstance(mal_auth_issue, dict):
        for review_recommendation in health_review_recommendations:
            review_command = review_recommendation.get("command") if isinstance(review_recommendation.get("command"), str) else None
            review_command_args = review_recommendation.get("command_args") if isinstance(review_recommendation.get("command_args"), list) else []
            review_detail = review_recommendation.get("detail") if isinstance(review_recommendation.get("detail"), str) else None
            review_reason_code = str(review_recommendation.get("reason_code") or "") or "refresh_mapping_review_backlog"
            step = review_reason_code.replace("_", "-")
            details = review_detail or "Run the mapping-review maintenance command recommended by the latest health artifact."
            details += " This is carried from the latest health artifact into bootstrap-audit so bootstrap-ready installs still surface review/backlog pressure without requiring a separate health-check read."
            add_onboarding_step(
                step=step,
                details=details,
                user_action_required=False,
                command=review_command,
                command_args=review_command_args,
                applies_to="review_queue",
                reason_code=review_reason_code,
                automation_safe=review_recommendation.get("automation_safe") is True,
                requires_auth_interaction=review_recommendation.get("requires_auth_interaction") is True,
            )
    crunchyroll_intended_for_transport = crunchyroll_credentials_present or crunchyroll_session_present
    if crunchyroll_intended_for_transport and not dependency_checks["curl_cffi"]:
        add_onboarding_step(
            step="install-required-crunchyroll-transport",
            details="Install or refresh the project dependencies (`pip install -e .`) so required curl_cffi browser-TLS transport support is available for Crunchyroll.",
            user_action_required=False,
            command="python3 -m pip install -e .",
            command_args=["python3", "-m", "pip", "install", "-e", "."],
            applies_to="crunchyroll",
            automation_safe=True,
            requires_auth_interaction=False,
        )
    if not dependency_checks["systemctl"]:
        add_onboarding_step(
            step="install-service-manager",
            details="systemctl is unavailable; install/enable a compatible service manager or plan to run `mal-updater service-run` manually in the foreground.",
            user_action_required=True,
            applies_to="automation",
        )
    if isinstance(automation_installation, dict):
        install_script_path = automation_installation.get("install_script_path")
        missing_units = automation_installation.get("missing_required_units") if isinstance(automation_installation.get("missing_required_units"), list) else []
        outdated_units = automation_installation.get("outdated_required_units") if isinstance(automation_installation.get("outdated_required_units"), list) else []
        disabled_services = automation_installation.get("disabled_services") if isinstance(automation_installation.get("disabled_services"), list) else []
        inactive_services = automation_installation.get("inactive_services") if isinstance(automation_installation.get("inactive_services"), list) else []
        if isinstance(install_script_path, str) and install_script_path:
            automation_details = None
            automation_step = None
            if missing_units:
                automation_step = "install-user-systemd-daemon"
                automation_details = (
                    "Install the repo-owned MAL-Updater user-systemd daemon so unattended sync can run in the background. "
                    f"Missing units: {', '.join(str(item) for item in missing_units)}"
                )
            elif outdated_units:
                automation_step = "refresh-user-systemd-daemon"
                automation_details = (
                    "Reinstall/update the repo-owned MAL-Updater user-systemd daemon so the installed units match the current repo version. "
                    f"Outdated units: {', '.join(str(item) for item in outdated_units)}"
                )
            elif disabled_services:
                automation_step = "enable-user-systemd-daemon"
                automation_details = (
                    "Re-run the repo-owned MAL-Updater user-systemd installer so the unattended daemon is enabled for this user. "
                    f"Disabled services: {', '.join(str(item) for item in disabled_services)}"
                )
            elif inactive_services:
                automation_step = "restart-user-systemd-daemon"
                automation_details = (
                    "Re-run the repo-owned MAL-Updater user-systemd installer so the unattended daemon is active in the current user runtime. "
                    f"Inactive services: {', '.join(str(item) for item in inactive_services)}"
                )
            elif automation_installation.get("env_present") is False:
                automation_step = "stage-user-systemd-daemon-env"
                automation_details = (
                    "The repo-owned MAL-Updater user-systemd unit is installed, but the rendered service environment file is missing. "
                    "Re-run the installer to recreate the expected environment file before relying on unattended automation."
                )
            elif automation_installation.get("env_restrictive") is False:
                automation_step = "tighten-user-systemd-daemon-env-permissions"
                env_path = automation_installation.get("env_path")
                automation_details = (
                    "The MAL-Updater user-systemd environment file exists but is not mode 0600. "
                    "Tighten it before storing service env overrides that may include sensitive paths or tokens."
                )
            if automation_step and automation_details:
                command = install_script_path
                command_args = [install_script_path]
                if automation_step == "tighten-user-systemd-daemon-env-permissions" and isinstance(env_path, str):
                    command = f"chmod 600 {shlex.quote(env_path)}"
                    command_args = ["chmod", "600", env_path]
                add_onboarding_step(
                    step=automation_step,
                    details=automation_details,
                    user_action_required=False,
                    command=command,
                    command_args=command_args,
                    applies_to="automation",
                    automation_safe=True,
                    requires_auth_interaction=False,
                )
    if secrets_dir_permissions["exists"] and not secrets_dir_permissions["restrictive"]:
        add_onboarding_step(
            step="tighten-secrets-dir-permissions",
            details=str(secrets_dir_permissions["details"]),
            user_action_required=True,
            command=str(secrets_dir_permissions["command"]),
            command_args=["chmod", "700", str(config.secrets_dir)],
            applies_to="security",
            automation_safe=True,
            requires_auth_interaction=False,
        )

    blocking_steps = [item for item in onboarding_steps if item["user_action_required"]]
    nonblocking_steps = [item for item in onboarding_steps if not item["user_action_required"]]
    actionable_commands = [item for item in onboarding_steps if isinstance(item.get("command"), str)]
    providers = {
        "crunchyroll": {
            "enabled_by_credentials": crunchyroll_credentials_present,
            "credentials_present": crunchyroll_credentials_present,
            "session_present": crunchyroll_session_present,
            "transport_ready": dependency_checks["curl_cffi"],
            "ready": crunchyroll_guidance["ready"],
            "missing": [
                name
                for name, present in (
                    ("credentials", crunchyroll_credentials_present),
                    ("session", crunchyroll_session_present),
                    ("transport", dependency_checks["curl_cffi"]),
                    ("auth", not isinstance(provider_auth_issues.get("crunchyroll"), dict)),
                )
                if not present
            ],
            "bootstrap_command": crunchyroll_bootstrap_descriptor.command,
            "auth_degraded": isinstance(provider_auth_issues.get("crunchyroll"), dict),
            "auth_degradation": provider_auth_issues.get("crunchyroll"),
            "operation_mode": crunchyroll_guidance["mode"],
            "operation_guidance": crunchyroll_guidance,
        },
        "hidive": {
            "enabled_by_credentials": hidive_credentials_present,
            "credentials_present": hidive_credentials_present,
            "session_present": hidive_session_present,
            "transport_ready": True,
            "ready": hidive_guidance["ready"],
            "missing": [
                name
                for name, present in (
                    ("credentials", hidive_credentials_present),
                    ("session", hidive_session_present),
                    ("auth", not isinstance(provider_auth_issues.get("hidive"), dict)),
                )
                if not present
            ],
            "bootstrap_command": hidive_bootstrap_descriptor.command,
            "auth_degraded": isinstance(provider_auth_issues.get("hidive"), dict),
            "auth_degradation": provider_auth_issues.get("hidive"),
            "operation_mode": hidive_guidance["mode"],
            "operation_guidance": hidive_guidance,
        },
    }

    recommended_command = health_report.select_maintenance_command(actionable_commands)
    recommended_automation_command = health_report.select_maintenance_command(actionable_commands, require_automation_safe=True)

    payload = {
        "project_root": str(config.project_root),
        "workspace_root": str(config.workspace_root),
        "runtime_root": str(config.runtime_root),
        "settings_path": str(config.settings_path),
        "runtime_paths": {
            "config_dir": str(config.config_dir),
            "secrets_dir": str(config.secrets_dir),
            "data_dir": str(config.data_dir),
            "state_dir": str(config.state_dir),
            "cache_dir": str(config.cache_dir),
            "db_path": str(config.db_path),
        },
        "runtime_initialization": runtime_initialization,
        "secrets_dir_permissions": secrets_dir_permissions,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
        },
        "dependencies": {
            "checks": dependency_checks,
            "missing": missing_dependencies,
        },
        "credentials": {
            "mal_client_id_present": mal_app_present,
            "mal_access_token_present": bool(secrets.access_token),
            "mal_refresh_token_present": bool(secrets.refresh_token),
            "crunchyroll_username_present": bool(crunchyroll_credentials.username),
            "crunchyroll_password_present": bool(crunchyroll_credentials.password),
            "crunchyroll_refresh_token_present": crunchyroll_state.refresh_token_path.exists(),
            "crunchyroll_device_id_present": crunchyroll_state.device_id_path.exists(),
            "hidive_username_present": bool(hidive_credentials.username),
            "hidive_password_present": bool(hidive_credentials.password),
            "hidive_authorisation_token_present": hidive_state.access_token_path.exists(),
            "hidive_refresh_token_present": hidive_state.refresh_token_path.exists(),
        },
        "services": {
            "installer_script": str(config.project_root / "scripts" / "install_user_systemd_units.sh"),
            "service_manager_available": dependency_checks["systemctl"],
            "service_unit_name": "mal-updater.service",
            "service_unit_names": ["mal-updater.service", "mal-updater-dashboard.service"],
            "service_model": "user-systemd daemon plus optional loopback dashboard",
            "automation_installation": automation_installation,
        },
        "operation_modes": operation_modes,
        "mal": {
            "client_id_present": mal_app_present,
            "oauth_ready": mal_oauth_present,
            "ready": mal_guidance["ready"],
            "auth_degraded": isinstance(mal_auth_issue, dict),
            "auth_degradation": mal_auth_issue,
            "operation_mode": mal_guidance["mode"],
            "operation_guidance": mal_guidance,
            "redirect_uri": config.mal.redirect_uri,
            "bind_host": config.mal.bind_host,
            "non_loopback_callback_ack": config.mal.non_loopback_callback_ack,
            "callback_bind_warning": mal_callback_bind_warning(config.mal),
            "redirect_host": config.mal.redirect_host,
            "redirect_port": config.mal.redirect_port,
            "auth_command": mal_auth_descriptor.command,
        },
        "providers": providers,
        "summary": {
            "blocking_step_count": len(blocking_steps),
            "nonblocking_step_count": len(nonblocking_steps),
            "actionable_command_count": len(actionable_commands),
            "ready_provider_count": sum(1 for provider in providers.values() if provider["ready"]),
            "provider_count": len(providers),
            "intended_provider_count": operation_modes.get("intended_provider_count"),
            "partially_staged_provider_count": operation_modes.get("partially_staged_provider_count"),
            "runtime_initialized": runtime_initialization["ready"],
            "secrets_dir_restrictive": secrets_dir_permissions["restrictive"],
            "automation_installed": automation_installation.get("required_units_installed") if isinstance(automation_installation, dict) else None,
            "automation_current": automation_installation.get("required_units_current") if isinstance(automation_installation, dict) else None,
            "automation_all_tracked_installed": automation_installation.get("all_tracked_units_installed") if isinstance(automation_installation, dict) else None,
            "automation_all_tracked_current": automation_installation.get("all_tracked_units_current") if isinstance(automation_installation, dict) else None,
            "automation_enabled": automation_installation.get("service_enabled") if isinstance(automation_installation, dict) else None,
            "automation_active": automation_installation.get("service_active") if isinstance(automation_installation, dict) else None,
            "operation_mode": operation_modes.get("mode"),
            "manual_foreground_acceptable": operation_modes.get("manual_foreground_acceptable"),
            "daemon_expected": operation_modes.get("daemon_expected"),
        },
        "onboarding_steps": onboarding_steps,
        "recommended_commands": actionable_commands,
        "recommended_command": recommended_command,
        "recommended_automation_command": recommended_automation_command,
        "ready": not onboarding_steps and not missing_dependencies,
    }

    return payload


def render_bootstrap_audit_summary(payload: dict[str, object]) -> str:
    runtime_initialization = payload.get("runtime_initialization") if isinstance(payload.get("runtime_initialization"), dict) else {}
    secrets_dir_permissions = payload.get("secrets_dir_permissions") if isinstance(payload.get("secrets_dir_permissions"), dict) else {}
    dependencies = payload.get("dependencies") if isinstance(payload.get("dependencies"), dict) else {}
    missing_dependencies = dependencies.get("missing") if isinstance(dependencies.get("missing"), list) else []
    providers = payload.get("providers") if isinstance(payload.get("providers"), dict) else {}
    onboarding_steps = payload.get("onboarding_steps") if isinstance(payload.get("onboarding_steps"), list) else []
    output = io.StringIO()
    with redirect_stdout(output):
        print(f"ready={payload['ready']}")
        print(f"runtime_root={payload['runtime_root']}")
        print(f"settings_path={payload['settings_path']}")
        print(f"runtime_initialized={payload['summary']['runtime_initialized']}")
        if runtime_initialization.get("missing"):
            print("runtime_missing=" + ", ".join(str(item) for item in runtime_initialization["missing"]))
        if secrets_dir_permissions.get("mode_octal") is not None:
            print(f"secrets_dir_mode={secrets_dir_permissions['mode_octal']}")
        if secrets_dir_permissions.get("restrictive") is not None:
            print(f"secrets_dir_restrictive={secrets_dir_permissions['restrictive']}")
        if payload["summary"].get("automation_installed") is not None:
            print(f"automation_installed={payload['summary']['automation_installed']}")
        if payload["summary"].get("automation_current") is not None:
            print(f"automation_current={payload['summary']['automation_current']}")
        if payload["summary"].get("automation_all_tracked_installed") is not None:
            print(f"automation_all_tracked_installed={payload['summary']['automation_all_tracked_installed']}")
        if payload["summary"].get("automation_all_tracked_current") is not None:
            print(f"automation_all_tracked_current={payload['summary']['automation_all_tracked_current']}")
        if payload["summary"].get("automation_enabled") is not None:
            print(f"automation_enabled={payload['summary']['automation_enabled']}")
        if payload["summary"].get("automation_active") is not None:
            print(f"automation_active={payload['summary']['automation_active']}")
        print(f"operation_mode={payload['summary']['operation_mode']}")
        print(f"mal_ready={payload['mal']['ready']}")
        print(f"mal_operation_mode={payload['mal']['operation_mode']}")
        if payload["mal"].get("callback_bind_warning"):
            print(f"mal_callback_bind_warning={payload['mal']['callback_bind_warning']}")
        mal_auth_degradation = payload["mal"].get("auth_degradation") if isinstance(payload["mal"].get("auth_degradation"), dict) else {}
        mal_auth_failure_kind = mal_auth_degradation.get("auth_failure_kind") if isinstance(mal_auth_degradation.get("auth_failure_kind"), str) else None
        if mal_auth_failure_kind:
            print(f"mal_auth_failure_kind={mal_auth_failure_kind}")
        mal_auth_remediation_kind = mal_auth_degradation.get("auth_remediation_kind") if isinstance(mal_auth_degradation.get("auth_remediation_kind"), str) else None
        if mal_auth_remediation_kind:
            print(f"mal_auth_remediation_kind={mal_auth_remediation_kind}")
        print(f"manual_foreground_acceptable={payload['summary']['manual_foreground_acceptable']}")
        print(f"daemon_expected={payload['summary']['daemon_expected']}")
        print(f"blocking_step_count={payload['summary']['blocking_step_count']}")
        print(f"nonblocking_step_count={payload['summary']['nonblocking_step_count']}")
        print(f"ready_provider_count={payload['summary']['ready_provider_count']}")
        if payload["summary"].get("intended_provider_count") is not None:
            print(f"intended_provider_count={payload['summary']['intended_provider_count']}")
        if payload["summary"].get("partially_staged_provider_count") is not None:
            print(f"partially_staged_provider_count={payload['summary']['partially_staged_provider_count']}")
        if missing_dependencies:
            print("missing_dependencies=" + ", ".join(missing_dependencies))
        mal_operation_guidance = payload["mal"].get("operation_guidance") if isinstance(payload["mal"].get("operation_guidance"), dict) else {}
        mal_next_command = mal_operation_guidance.get("next_command") if isinstance(mal_operation_guidance.get("next_command"), str) else None
        if isinstance(mal_next_command, str) and mal_next_command:
            print(f"mal_next_command={mal_next_command}")
            mal_next_reason_code = mal_operation_guidance.get("next_command_reason_code")
            if isinstance(mal_next_reason_code, str) and mal_next_reason_code:
                print(f"mal_next_command_reason_code={mal_next_reason_code}")
            if mal_operation_guidance.get("next_command_automation_safe") is not None:
                print(f"mal_next_command_automation_safe={mal_operation_guidance['next_command_automation_safe']}")
            if mal_operation_guidance.get("next_command_requires_auth_interaction") is not None:
                print(f"mal_next_command_requires_auth_interaction={mal_operation_guidance['next_command_requires_auth_interaction']}")
            mal_next_auth_failure_kind = mal_operation_guidance.get("next_command_auth_failure_kind")
            if isinstance(mal_next_auth_failure_kind, str) and mal_next_auth_failure_kind:
                print(f"mal_next_command_auth_failure_kind={mal_next_auth_failure_kind}")
            mal_next_auth_remediation_kind = mal_operation_guidance.get("next_command_auth_remediation_kind")
            if isinstance(mal_next_auth_remediation_kind, str) and mal_next_auth_remediation_kind:
                print(f"mal_next_command_auth_remediation_kind={mal_next_auth_remediation_kind}")
        for provider_name, provider_payload in providers.items():
            print(f"provider_{provider_name}_ready={provider_payload['ready']}")
            operation_mode = provider_payload.get("operation_mode")
            if isinstance(operation_mode, str) and operation_mode:
                print(f"provider_{provider_name}_operation_mode={operation_mode}")
            missing = provider_payload.get("missing") if isinstance(provider_payload.get("missing"), list) else []
            if missing:
                print(f"provider_{provider_name}_missing=" + ", ".join(str(item) for item in missing))
            operation_guidance = provider_payload.get("operation_guidance") if isinstance(provider_payload.get("operation_guidance"), dict) else {}
            next_command = operation_guidance.get("next_command") if isinstance(operation_guidance.get("next_command"), str) else None
            provider_auth_degradation = provider_payload.get("auth_degradation") if isinstance(provider_payload.get("auth_degradation"), dict) else {}
            auth_failure_kind = provider_auth_degradation.get("auth_failure_kind") if isinstance(provider_auth_degradation.get("auth_failure_kind"), str) else None
            if auth_failure_kind:
                print(f"provider_{provider_name}_auth_failure_kind={auth_failure_kind}")
            auth_remediation_kind = provider_auth_degradation.get("auth_remediation_kind") if isinstance(provider_auth_degradation.get("auth_remediation_kind"), str) else None
            if auth_remediation_kind:
                print(f"provider_{provider_name}_auth_remediation_kind={auth_remediation_kind}")
            if next_command:
                print(f"provider_{provider_name}_next_command={next_command}")
                next_reason_code = operation_guidance.get("next_command_reason_code")
                if isinstance(next_reason_code, str) and next_reason_code:
                    print(f"provider_{provider_name}_next_command_reason_code={next_reason_code}")
                if operation_guidance.get("next_command_automation_safe") is not None:
                    print(f"provider_{provider_name}_next_command_automation_safe={operation_guidance['next_command_automation_safe']}")
                if operation_guidance.get("next_command_requires_auth_interaction") is not None:
                    print(f"provider_{provider_name}_next_command_requires_auth_interaction={operation_guidance['next_command_requires_auth_interaction']}")
                next_auth_failure_kind = operation_guidance.get("next_command_auth_failure_kind")
                if isinstance(next_auth_failure_kind, str) and next_auth_failure_kind:
                    print(f"provider_{provider_name}_next_command_auth_failure_kind={next_auth_failure_kind}")
                next_auth_remediation_kind = operation_guidance.get("next_command_auth_remediation_kind")
                if isinstance(next_auth_remediation_kind, str) and next_auth_remediation_kind:
                    print(f"provider_{provider_name}_next_command_auth_remediation_kind={next_auth_remediation_kind}")
        top_command = payload.get("recommended_command") if isinstance(payload.get("recommended_command"), dict) else None
        health_report.emit_recommended_command_summary("maintenance_recommended", top_command)
        top_auto_command = payload.get("recommended_automation_command") if isinstance(payload.get("recommended_automation_command"), dict) else None
        health_report.emit_recommended_command_summary("maintenance_recommended_auto", top_auto_command)
        for item in onboarding_steps:
            print(f"next_step={item['step']}: {item['details']}")
            command = item.get("command")
            if isinstance(command, str) and command:
                print(f"next_command={command}")
    return output.getvalue()


def render_bootstrap_audit_json(payload: dict[str, object], *, trailing_newline: bool = True) -> str:
    text = json.dumps(payload, indent=2, sort_keys=True)
    return text + "\n" if trailing_newline else text
