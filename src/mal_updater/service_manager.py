from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess
from typing import Any

from .config import AppConfig, ensure_directories, load_config
from .persistence import PersistentJsonError, atomic_write_text, read_json_dict_bounded
from .redaction import sanitize_text, sanitize_url, sanitize_value
from .service_runtime import TaskSpec, _planned_fetch_mode, effective_niceness_policy
from .service_systemd_status import build_service_status_payload
from .service_units import SERVICE_UNIT_NAME, render_repo_systemd_unit_template

SERVICE_NAME = SERVICE_UNIT_NAME
_RECENT_LOG_LINES = 20
_RESULT_SNIPPET_LIMIT = 240
_SERVICE_STATUS_JSON_MAX_BYTES = 8 * 1024 * 1024
_SERVICE_LOG_LINE_MAX_BYTES = 16 * 1024


@dataclass(slots=True)
class ServiceCommandResult:
    status: str
    message: str
    details: dict[str, Any] | None = None


def _unit_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / SERVICE_NAME


def _service_env_path() -> Path:
    return Path.home() / ".config" / "mal-updater-service.env"


def _service_env_source_path(config: AppConfig) -> Path:
    return config.project_root / "ops" / "systemd-user" / "mal-updater-service.env.example"


def _run(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=check)


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = read_json_dict_bounded(path, max_bytes=_SERVICE_STATUS_JSON_MAX_BYTES)
    except PersistentJsonError as exc:
        return None, sanitize_text(exc.safe_message, max_length=500)
    if payload is None:
        return None, None
    return payload, None


def _tail_lines(path: Path, *, limit: int = _RECENT_LOG_LINES) -> list[str]:
    if not path.exists():
        return []
    if limit <= 0:
        return []
    recent: deque[str] = deque(maxlen=limit)
    try:
        with path.open("rb") as fh:
            while True:
                raw = fh.readline(_SERVICE_LOG_LINE_MAX_BYTES + 1)
                if not raw:
                    break
                truncated = len(raw) > _SERVICE_LOG_LINE_MAX_BYTES
                if truncated:
                    while raw and not raw.endswith(b"\n"):
                        chunk = fh.readline(_SERVICE_LOG_LINE_MAX_BYTES + 1)
                        if not chunk or chunk.endswith(b"\n"):
                            break
                    raw = raw[:_SERVICE_LOG_LINE_MAX_BYTES] + b"...<truncated>"
                recent.append(raw.decode("utf-8", errors="replace").rstrip("\r\n"))
    except OSError:
        return []
    return [sanitize_text(line, max_length=1_000) for line in recent]


def _snippet(value: object, *, limit: int = _RESULT_SNIPPET_LIMIT) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return sanitize_text(trimmed, max_length=limit)


def _parse_iso_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _summarize_last_result(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    summary: dict[str, Any] = {}
    for field in (
        "status",
        "label",
        "returncode",
        "reason",
        "fetch_mode",
        "full_refresh_reason",
        "deferred_full_refresh_reason",
        "access_token_path",
        "refresh_token_path",
        "delivery_status",
        "request_id",
        "request_url",
        "http_status",
    ):
        field_value = value.get(field)
        if field_value is not None:
            if field == "request_url" and isinstance(field_value, str):
                summary[field] = sanitize_url(field_value, max_length=1_000)
            else:
                summary[field] = sanitize_value(field_value, max_depth=3, max_items=25, max_string=500)
    stdout_snippet = _snippet(value.get("stdout_snippet") or value.get("stdout"))
    stderr_snippet = _snippet(value.get("stderr_snippet") or value.get("stderr"))
    if stdout_snippet is not None:
        summary["stdout_snippet"] = stdout_snippet
    if stderr_snippet is not None:
        summary["stderr_snippet"] = stderr_snippet
    return summary or None


def _shape_health_diagnostic_fields(value: object) -> object:
    """Give trusted health diagnostic markers an explicit non-secret name."""

    if not isinstance(value, dict):
        return value
    shaped = dict(value)
    warnings = shaped.get("warnings")
    if isinstance(warnings, list):
        shaped_warnings: list[object] = []
        for warning in warnings:
            if not isinstance(warning, dict):
                shaped_warnings.append(warning)
                continue
            shaped_warning = dict(warning)
            if "code" in shaped_warning:
                shaped_warning["reason_code"] = shaped_warning.pop("code")
            diagnostics = shaped_warning.get("diagnostics")
            if isinstance(diagnostics, list):
                shaped_warning["diagnostics"] = [
                    {"reason_code": diagnostic.get("code"), **{key: item for key, item in diagnostic.items() if key != "code"}}
                    if isinstance(diagnostic, dict) and "code" in diagnostic
                    else diagnostic
                    for diagnostic in diagnostics
                ]
            shaped_warnings.append(shaped_warning)
        shaped["warnings"] = shaped_warnings
    return shaped


def _restore_health_diagnostic_fields(value: object) -> None:
    """Restore the established health-report shape after sanitization."""

    if not isinstance(value, dict):
        return
    health = value.get("health_latest_summary")
    if not isinstance(health, dict):
        return
    warnings = health.get("warnings")
    if not isinstance(warnings, list):
        return
    for warning in warnings:
        if not isinstance(warning, dict):
            continue
        if "reason_code" in warning:
            warning["code"] = warning.pop("reason_code")
        diagnostics = warning.get("diagnostics")
        if not isinstance(diagnostics, list):
            continue
        for diagnostic in diagnostics:
            if isinstance(diagnostic, dict) and "reason_code" in diagnostic:
                diagnostic["code"] = diagnostic.pop("reason_code")


def _current_planned_fetch_summary(config: AppConfig, task_name: str, task_state: dict[str, Any]) -> dict[str, Any]:
    if not task_name.startswith("sync_fetch_"):
        return {}
    provider = task_name.removeprefix("sync_fetch_")
    spec = TaskSpec(name=task_name, every_seconds=int(task_state.get("every_seconds", 0) or 0), budget_provider=provider)
    now_dt = datetime.now(timezone.utc)
    planned_fetch_mode, planned_full_refresh_reasons, health_request = _planned_fetch_mode(config, spec, task_state, now=now_dt.timestamp())
    if planned_fetch_mode is None:
        return {}
    summary: dict[str, Any] = {"planned_fetch_mode": planned_fetch_mode}
    if isinstance(health_request, dict) and isinstance(health_request.get("reason_code"), str):
        summary["planned_health_request_reason_code"] = str(health_request["reason_code"])
    if planned_full_refresh_reasons:
        summary["planned_full_refresh_reason"] = "+".join(planned_full_refresh_reasons)
        if "periodic_cadence" in planned_full_refresh_reasons:
            anchor_epoch = task_state.get("full_refresh_anchor_epoch")
            interval_seconds = int(config.service.full_refresh_every_seconds)
            if isinstance(anchor_epoch, (int, float)) and anchor_epoch > 0 and interval_seconds > 0:
                due_at = datetime.fromtimestamp(float(anchor_epoch) + float(interval_seconds), tz=timezone.utc)
                summary["planned_full_refresh_due_at"] = due_at.isoformat(timespec="seconds").replace("+00:00", "Z")
                summary["planned_full_refresh_overdue_seconds"] = max(0, int((now_dt - due_at).total_seconds()))
        last_result = task_state.get("last_result")
        deferred_reason = None
        if isinstance(last_result, dict):
            raw_deferred_reason = last_result.get("deferred_full_refresh_reason")
            if isinstance(raw_deferred_reason, str) and raw_deferred_reason:
                deferred_reason = raw_deferred_reason
        if deferred_reason is not None and task_state.get("last_fetch_mode") == "incremental":
            summary["planned_full_refresh_budget_deferred"] = True
            summary["planned_full_refresh_deferred_reason"] = deferred_reason
    return summary


def _derive_task_execution_state(task_state: dict[str, Any], *, now: datetime) -> dict[str, Any] | None:
    if isinstance(task_state.get("running_started_epoch"), (int, float)):
        payload: dict[str, Any] = {
            "execution_state": "running",
            "execution_state_reason": "subprocess_active",
        }
        timeout_seconds = task_state.get("running_timeout_seconds")
        if isinstance(timeout_seconds, (int, float)):
            started_epoch = float(task_state["running_started_epoch"])
            elapsed = max(0, int(now.timestamp() - started_epoch))
            payload["execution_state_elapsed_seconds"] = elapsed
            payload["execution_state_remaining_seconds"] = max(0, int(timeout_seconds) - elapsed)
        return payload

    failure_backoff_until = _parse_iso_timestamp(task_state.get("failure_backoff_until"))
    if failure_backoff_until is not None and failure_backoff_until > now:
        remaining = max(0, int((failure_backoff_until - now).total_seconds()))
        payload: dict[str, Any] = {
            "execution_state": "cooling_down_after_failure",
            "execution_state_reason": "failure_backoff_active",
            "execution_state_remaining_seconds": remaining,
        }
        failure_class = task_state.get("failure_backoff_class")
        if isinstance(failure_class, str) and failure_class:
            payload["execution_state_detail"] = failure_class
        return payload

    budget_backoff_until = _parse_iso_timestamp(task_state.get("budget_backoff_until"))
    if budget_backoff_until is not None and budget_backoff_until > now:
        remaining = max(0, int((budget_backoff_until - now).total_seconds()))
        payload = {
            "execution_state": "cooling_down_for_budget",
            "execution_state_reason": "budget_backoff_active",
            "execution_state_remaining_seconds": remaining,
        }
        budget_level = task_state.get("budget_backoff_level")
        if isinstance(budget_level, str) and budget_level:
            payload["execution_state_detail"] = budget_level
        return payload

    next_due_at = _parse_iso_timestamp(task_state.get("next_due_at"))
    if next_due_at is not None:
        remaining = int((next_due_at - now).total_seconds())
        if remaining <= 0:
            return {
                "execution_state": "due_now",
                "execution_state_reason": "next_due_elapsed",
                "execution_state_remaining_seconds": 0,
            }
        return {
            "execution_state": "waiting_until_due",
            "execution_state_reason": "next_due_pending",
            "execution_state_remaining_seconds": remaining,
        }

    last_status = task_state.get("last_status")
    if isinstance(last_status, str) and last_status:
        return {
            "execution_state": "awaiting_schedule",
            "execution_state_reason": f"last_status_{last_status}",
            "execution_state_remaining_seconds": 0,
        }

    return None


def _summarize_task_state(config: AppConfig, task_name: str, value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    summary: dict[str, Any] = {}
    for field in (
        "last_run_at",
        "last_status",
        "last_skipped_at",
        "last_skip_reason",
        "last_error",
        "last_decision_at",
        "last_started_at",
        "last_finished_at",
        "budget_backoff_level",
        "budget_backoff_until",
        "budget_backoff_cooldown_source",
        "failure_backoff_until",
        "failure_backoff_reason",
        "failure_backoff_class",
        "next_due_at",
        "budget_provider",
        "budget_scope",
        "last_fetch_mode",
        "last_full_refresh_reason",
        "planned_full_refresh_deferred_reason",
        "projected_request_source",
        "projected_request_history_mode",
        "projected_request_percentile_source",
        "running_started_at",
        "running_command",
    ):
        field_value = value.get(field)
        if field_value is not None:
            if field == "request_url" and isinstance(field_value, str):
                summary[field] = sanitize_url(field_value, max_length=1_000)
            else:
                summary[field] = sanitize_value(field_value, max_depth=3, max_items=25, max_string=500)
    if isinstance(value.get("last_run_epoch"), (int, float)):
        summary["last_run_epoch"] = value["last_run_epoch"]
    if isinstance(value.get("every_seconds"), int):
        summary["every_seconds"] = value["every_seconds"]
    if isinstance(value.get("initial_delay_seconds"), int):
        summary["initial_delay_seconds"] = value["initial_delay_seconds"]
    if isinstance(value.get("next_due_epoch"), (int, float)):
        summary["next_due_epoch"] = value["next_due_epoch"]
    if isinstance(value.get("last_duration_seconds"), (int, float)):
        summary["last_duration_seconds"] = float(value["last_duration_seconds"])
    if isinstance(value.get("running_timeout_seconds"), (int, float)):
        summary["running_timeout_seconds"] = int(value["running_timeout_seconds"])
    if isinstance(value.get("running_started_epoch"), (int, float)):
        duration = max(0.0, datetime.now(timezone.utc).timestamp() - float(value["running_started_epoch"]))
        summary["running_duration_seconds"] = round(duration, 3)
    if isinstance(value.get("budget_backoff_remaining_seconds"), (int, float)):
        summary["budget_backoff_remaining_seconds"] = int(value["budget_backoff_remaining_seconds"])
    if isinstance(value.get("budget_backoff_floor_seconds"), (int, float)):
        summary["budget_backoff_floor_seconds"] = int(value["budget_backoff_floor_seconds"])
    if isinstance(value.get("failure_backoff_remaining_seconds"), (int, float)):
        summary["failure_backoff_remaining_seconds"] = int(value["failure_backoff_remaining_seconds"])
    if isinstance(value.get("failure_backoff_consecutive_failures"), (int, float)):
        summary["failure_backoff_consecutive_failures"] = int(value["failure_backoff_consecutive_failures"])
    if isinstance(value.get("failure_backoff_floor_seconds"), (int, float)):
        summary["failure_backoff_floor_seconds"] = int(value["failure_backoff_floor_seconds"])
    if isinstance(value.get("projected_request_count"), (int, float)):
        summary["projected_request_count"] = int(value["projected_request_count"])
    if isinstance(value.get("projected_request_total"), (int, float)):
        summary["projected_request_total"] = int(value["projected_request_total"])
    if isinstance(value.get("projected_request_history_window"), (int, float)):
        summary["projected_request_history_window"] = int(value["projected_request_history_window"])
    if isinstance(value.get("projected_request_history_sample_count"), (int, float)):
        summary["projected_request_history_sample_count"] = int(value["projected_request_history_sample_count"])
    if isinstance(value.get("planned_full_refresh_budget_deferred"), bool):
        summary["planned_full_refresh_budget_deferred"] = value["planned_full_refresh_budget_deferred"]
    if isinstance(value.get("projected_ratio"), (int, float)):
        summary["projected_ratio"] = round(float(value["projected_ratio"]), 6)
    if isinstance(value.get("projected_request_percentile"), (int, float)):
        summary["projected_request_percentile"] = round(float(value["projected_request_percentile"]), 6)
    if isinstance(value.get("last_request_delta"), (int, float)):
        summary["last_request_delta"] = int(value["last_request_delta"])
    now_dt = datetime.now(timezone.utc)
    next_due_at = _parse_iso_timestamp(value.get("next_due_at"))
    if next_due_at is not None:
        summary["next_due_in_seconds"] = max(0, int((next_due_at - now_dt).total_seconds()))
    budget_backoff_until = _parse_iso_timestamp(value.get("budget_backoff_until"))
    if budget_backoff_until is not None:
        summary["budget_backoff_remaining_seconds"] = max(0, int((budget_backoff_until - now_dt).total_seconds()))
    failure_backoff_until = _parse_iso_timestamp(value.get("failure_backoff_until"))
    if failure_backoff_until is not None:
        summary["failure_backoff_remaining_seconds"] = max(0, int((failure_backoff_until - now_dt).total_seconds()))
    execution_state = _derive_task_execution_state(value, now=now_dt)
    if execution_state is not None:
        summary.update(execution_state)
    summary.update(_current_planned_fetch_summary(config, task_name, value))
    last_result = _summarize_last_result(value.get("last_result"))
    if last_result is not None:
        summary["last_result"] = last_result
    return summary or None


def unit_contents(config: AppConfig | None = None) -> str:
    config = config or load_config()
    ensure_directories(config)
    repo = config.project_root
    env_file = _service_env_path()
    python = Path(subprocess.run(["python3", "-c", "import sys; print(sys.executable)"], text=True, capture_output=True, check=True).stdout.strip())
    return render_repo_systemd_unit_template(repo, env_file, python)


def write_unit_file(config: AppConfig | None = None) -> Path:
    config = config or load_config()
    path = _unit_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(unit_contents(config), encoding="utf-8")
    return path


def write_service_env_file_if_missing(config: AppConfig | None = None) -> Path:
    config = config or load_config()
    path = _service_env_path()
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    source_path = _service_env_source_path(config)
    content = source_path.read_text(encoding="utf-8") if source_path.exists() else "# Optional EnvironmentFile for mal-updater.service\n"
    atomic_write_text(path, content, mode=0o600)
    return path


def daemon_reload() -> None:
    _run(["systemctl", "--user", "daemon-reload"])


def service_status() -> dict[str, Any]:
    payload = build_service_status_payload(
        unit_name=SERVICE_NAME,
        unit_path=_unit_path(),
        env_path=_service_env_path(),
        runner=_run,
    )
    safe = sanitize_value(payload, max_depth=6, max_items=100, max_string=1_000)
    return safe if isinstance(safe, dict) else {}


def install_service(*, start_now: bool = True, config: AppConfig | None = None) -> ServiceCommandResult:
    config = config or load_config()
    env = write_service_env_file_if_missing(config)
    unit = write_unit_file(config)
    daemon_reload()
    _run(["systemctl", "--user", "enable", SERVICE_NAME])
    if start_now:
        _run(["systemctl", "--user", "restart", SERVICE_NAME])
    return ServiceCommandResult(status="ok", message="MAL-Updater service installed.", details={"unit_path": str(unit), "env_path": str(env), **service_status()})


def uninstall_service(*, stop_now: bool = True) -> ServiceCommandResult:
    if stop_now:
        _run(["systemctl", "--user", "stop", SERVICE_NAME], check=False)
    _run(["systemctl", "--user", "disable", SERVICE_NAME], check=False)
    unit = _unit_path()
    if unit.exists():
        unit.unlink()
    daemon_reload()
    return ServiceCommandResult(status="ok", message="MAL-Updater service uninstalled.", details=service_status())


def restart_service() -> ServiceCommandResult:
    _run(["systemctl", "--user", "restart", SERVICE_NAME])
    return ServiceCommandResult(status="ok", message="MAL-Updater service restarted.", details=service_status())


def stop_service() -> ServiceCommandResult:
    _run(["systemctl", "--user", "stop", SERVICE_NAME], check=False)
    return ServiceCommandResult(status="ok", message="MAL-Updater service stopped.", details=service_status())


def start_service() -> ServiceCommandResult:
    _run(["systemctl", "--user", "start", SERVICE_NAME])
    return ServiceCommandResult(status="ok", message="MAL-Updater service started.", details=service_status())


def doctor_service(config: AppConfig | None = None) -> dict[str, Any]:
    config = config or load_config()
    ensure_directories(config)
    status = service_status()
    service_state, service_state_error = _read_json(config.service_state_path)
    recent_health, recent_health_error = _read_json(config.health_latest_json_path)

    task_state: dict[str, Any] = {}
    if isinstance(service_state, dict):
        raw_tasks = service_state.get("tasks")
        if isinstance(raw_tasks, dict):
            for task_name, raw_task_state in raw_tasks.items():
                normalized_task_name = str(task_name)
                summary = _summarize_task_state(config, normalized_task_name, raw_task_state)
                if summary is not None:
                    task_state[normalized_task_name] = summary

    payload = {
        **status,
        "service_log_path": str(config.service_log_path),
        "service_log_exists": config.service_log_path.exists(),
        "service_log_tail": _tail_lines(config.service_log_path),
        "service_state_path": str(config.service_state_path),
        "service_state_exists": config.service_state_path.exists(),
        "service_state_parse_error": service_state_error,
        "last_loop_at": service_state.get("last_loop_at") if isinstance(service_state, dict) else None,
        "task_state": task_state,
        "api_request_events_path": str(config.api_request_events_path),
        "api_request_events_exists": config.api_request_events_path.exists(),
        "health_latest_json_path": str(config.health_latest_json_path),
        "health_latest_exists": config.health_latest_json_path.exists(),
        "health_latest_parse_error": recent_health_error,
        "health_latest_summary": sanitize_value(
            _shape_health_diagnostic_fields(recent_health),
            max_depth=8,
            max_items=100,
            max_string=1_000,
        ),
        "niceness_policy": effective_niceness_policy(config),
    }
    if isinstance(service_state, dict) and isinstance(service_state.get("api_usage"), dict):
        payload["api_usage"] = sanitize_value(service_state["api_usage"], max_depth=5, max_items=100, max_string=500)
    safe_payload = sanitize_value(payload, max_depth=10, max_items=200, max_string=1_000)
    _restore_health_diagnostic_fields(safe_payload)
    return safe_payload if isinstance(safe_payload, dict) else {}
