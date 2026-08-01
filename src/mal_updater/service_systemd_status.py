from __future__ import annotations

import json
import os
import stat
from datetime import datetime, timezone
from pathlib import Path
import subprocess
from typing import Any, Callable

from .redaction import sanitize_text, sanitize_value
from .service_units import (
    OPTIONAL_SERVICE_UNIT_NAMES,
    SERVICE_UNIT_NAME,
    TRACKED_SERVICE_UNIT_NAMES,
    is_optional_service_unit,
    normalize_unit_names,
    render_systemd_unit_template_file,
    systemd_unit_path_context,
)

SYSTEMD_USER_UNIT_RUNTIME_PROPERTIES = (
    "ActiveState",
    "SubState",
    "UnitFileState",
    "NextElapseUSecRealtime",
    "LastTriggerUSec",
    "Result",
)

StatusRunner = Callable[..., subprocess.CompletedProcess[str]]
RuntimeReader = Callable[[str], dict[str, object]]


def format_systemd_usec_timestamp(value: str) -> str | None:
    raw = value.strip()
    if not raw or raw == "0":
        return None
    try:
        timestamp = int(raw) / 1_000_000
    except ValueError:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_systemd_enabled_state(value: object) -> bool:
    return isinstance(value, str) and value.strip() == "enabled"


def normalize_systemd_active_state(value: object) -> bool:
    return isinstance(value, str) and value.strip() == "active"


def xdg_config_home() -> Path:
    return Path(os.environ.get("XDG_CONFIG_HOME") or (Path.home() / ".config"))


def user_systemd_unit_dir(config_home: Path | None = None) -> Path:
    return (config_home or xdg_config_home()) / "systemd" / "user"


def user_service_env_path(config_home: Path | None = None) -> Path:
    return (config_home or xdg_config_home()) / "mal-updater-service.env"


def _permission_payload(path: Path, *, expected_mode: int) -> dict[str, object]:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        return {"exists": False, "mode_octal": None, "restrictive": None, "error": None}
    except OSError:
        return {"exists": True, "mode_octal": None, "restrictive": False, "error": "permission inspection failed"}
    return {"exists": True, "mode_octal": f"0o{mode:03o}", "restrictive": mode == expected_mode, "error": None}


def run_systemctl_status_probe(
    command: list[str],
    *,
    runner: StatusRunner | None = None,
) -> tuple[subprocess.CompletedProcess[str] | None, str | None]:
    status_runner = runner or _run_status_command
    try:
        return status_runner(command, check=False), None
    except OSError as exc:
        return None, f"{type(exc).__name__}: {exc}"


def systemctl_status_probe_output(result: subprocess.CompletedProcess[str] | None, error: str | None) -> str:
    if error:
        return sanitize_text(error, max_length=1_000)
    if result is None:
        return ""
    return sanitize_text((result.stdout or "").strip() or (result.stderr or "").strip(), max_length=1_000)


def build_service_status_payload(
    *,
    unit_name: str,
    unit_path: Path,
    env_path: Path,
    runner: StatusRunner | None = None,
) -> dict[str, Any]:
    enabled, enabled_error = run_systemctl_status_probe(["systemctl", "--user", "is-enabled", unit_name], runner=runner)
    active, active_error = run_systemctl_status_probe(["systemctl", "--user", "is-active", unit_name], runner=runner)
    systemctl_errors: dict[str, str] = {}
    if enabled_error:
        systemctl_errors["is_enabled"] = enabled_error
    if active_error:
        systemctl_errors["is_active"] = active_error
    env_permissions = _permission_payload(env_path, expected_mode=0o600)
    payload: dict[str, Any] = {
        "unit_path": str(unit_path),
        "unit_exists": unit_path.exists(),
        "enabled": enabled is not None and enabled.returncode == 0 and normalize_systemd_enabled_state(enabled.stdout),
        "active": active is not None and active.returncode == 0 and normalize_systemd_active_state(active.stdout),
        "enabled_raw": systemctl_status_probe_output(enabled, enabled_error),
        "active_raw": systemctl_status_probe_output(active, active_error),
        "env_path": str(env_path),
        "env_exists": env_permissions["exists"],
        "env_mode_octal": env_permissions["mode_octal"],
        "env_restrictive": env_permissions["restrictive"],
        "env_permission_error": env_permissions["error"],
        "systemctl_available": not systemctl_errors,
        "systemctl_status": "ok" if not systemctl_errors else "unavailable",
    }
    if systemctl_errors:
        payload["systemctl_errors"] = systemctl_errors
        payload["systemctl_error"] = "; ".join(f"{name}: {error}" for name, error in systemctl_errors.items())
    safe = sanitize_value(payload, max_depth=5, max_items=100, max_string=1_000)
    return safe if isinstance(safe, dict) else {}


def read_systemd_user_unit_runtime(
    unit_name: str,
    *,
    runner: StatusRunner | None = None,
) -> dict[str, object]:
    test_runtime_state = os.environ.get("MAL_UPDATER_SYSTEMD_RUNTIME_STATE_JSON")
    if test_runtime_state:
        try:
            payload = json.loads(test_runtime_state)
        except json.JSONDecodeError as exc:
            return {"available": False, "error": f"invalid MAL_UPDATER_SYSTEMD_RUNTIME_STATE_JSON: {exc}"}
        if not isinstance(payload, dict):
            return {"available": False, "error": "MAL_UPDATER_SYSTEMD_RUNTIME_STATE_JSON must decode to an object"}
        unit_payload = payload.get(unit_name) if unit_name in payload else payload
        if not isinstance(unit_payload, dict):
            return {"available": False, "error": f"runtime state override for {unit_name} must be an object"}
        safe = sanitize_value(unit_payload, max_depth=5, max_items=100, max_string=1_000)
        return safe if isinstance(safe, dict) else {"available": False, "error": "runtime state override is invalid"}

    systemctl_runner = runner or subprocess.run
    try:
        result = systemctl_runner(
            [
                "systemctl",
                "--user",
                "show",
                unit_name,
                *(f"--property={name}" for name in SYSTEMD_USER_UNIT_RUNTIME_PROPERTIES),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"available": False, "error": sanitize_text(exc, max_length=1_000)}

    if result.returncode != 0:
        error = (result.stderr or result.stdout or f"systemctl exited with code {result.returncode}").strip()
        return {"available": False, "error": sanitize_text(error, max_length=1_000)}

    runtime = parse_systemctl_show_properties(result.stdout, SYSTEMD_USER_UNIT_RUNTIME_PROPERTIES)
    payload = {
        "available": True,
        "active_state": runtime.get("ActiveState") or None,
        "sub_state": runtime.get("SubState") or None,
        "unit_file_state": runtime.get("UnitFileState") or None,
        "next_elapse_at": format_systemd_usec_timestamp(runtime.get("NextElapseUSecRealtime") or ""),
        "last_trigger_at": format_systemd_usec_timestamp(runtime.get("LastTriggerUSec") or ""),
        "result": runtime.get("Result") or None,
    }
    safe = sanitize_value(payload, max_depth=5, max_items=100, max_string=1_000)
    return safe if isinstance(safe, dict) else {"available": False, "error": "runtime state is invalid"}


def parse_systemctl_show_properties(output: str, properties: tuple[str, ...] = SYSTEMD_USER_UNIT_RUNTIME_PROPERTIES) -> dict[str, str]:
    property_names = set(properties)
    runtime: dict[str, str] = {}
    for line in output.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in property_names:
            runtime[key] = value
    return runtime


def _unit_installation_status(
    *,
    project_root: Path,
    source_path: Path,
    target_path: Path,
    env_path: Path,
    unit_name: str,
    runtime_reader: RuntimeReader,
    path_context: dict[str, str] | None,
) -> dict[str, object]:
    installed = target_path.exists()
    content_matches_repo = False
    if installed:
        try:
            rendered_source = render_systemd_unit_template_file(source_path, project_root, env_path, path_context=path_context)
            content_matches_repo = rendered_source == target_path.read_text(encoding="utf-8")
        except (OSError, ValueError):
            content_matches_repo = False
    runtime_state = runtime_reader(unit_name) if installed else None
    enabled = None
    active = None
    runtime_state_error = None
    if isinstance(runtime_state, dict):
        if runtime_state.get("available") is True:
            enabled = normalize_systemd_enabled_state(runtime_state.get("unit_file_state"))
            active = normalize_systemd_active_state(runtime_state.get("active_state"))
        else:
            runtime_state_error = str(runtime_state.get("error") or "runtime state unavailable")
    return {
        "installed": installed,
        "target_path": str(target_path),
        "source_path": str(source_path),
        "source_exists": source_path.exists(),
        "content_matches_repo": content_matches_repo if installed else None,
        "enabled": enabled,
        "active": active,
        "runtime_state": runtime_state,
        "runtime_state_available": bool(isinstance(runtime_state, dict) and runtime_state.get("available") is True),
        "runtime_state_error": runtime_state_error,
        "optional": is_optional_service_unit(unit_name),
    }


def build_automation_installation_status(
    project_root: Path | str,
    *,
    unit_name: str = SERVICE_UNIT_NAME,
    unit_names: tuple[str, ...] | list[str] | None = None,
    runtime_reader: RuntimeReader | None = None,
    path_context: dict[str, str] | None = None,
) -> dict[str, object] | None:
    project_root = Path(project_root)
    source_dir = project_root / "ops" / "systemd-user"
    script_path = project_root / "scripts" / "install_user_systemd_units.sh"
    if not source_dir.is_dir() or not script_path.exists():
        return None
    config_home = xdg_config_home()
    target_dir = user_systemd_unit_dir(config_home)
    if unit_names is not None:
        selected_unit_names = normalize_unit_names(unit_names)
    elif unit_name != SERVICE_UNIT_NAME:
        selected_unit_names = normalize_unit_names((unit_name,))
    else:
        discovered_units = [SERVICE_UNIT_NAME]
        for optional_name in OPTIONAL_SERVICE_UNIT_NAMES:
            if (source_dir / optional_name).exists() or (target_dir / optional_name).exists():
                discovered_units.append(optional_name)
        selected_unit_names = normalize_unit_names(discovered_units)
    source_paths = {name: source_dir / name for name in selected_unit_names}
    if any(not source_paths[name].exists() for name in selected_unit_names if name not in OPTIONAL_SERVICE_UNIT_NAMES):
        return None

    env_path = user_service_env_path(config_home)
    env_permissions = _permission_payload(env_path, expected_mode=0o600)
    render_path_context = path_context
    if render_path_context is None:
        try:
            from .config import load_config

            render_path_context = systemd_unit_path_context(load_config(project_root))
        except Exception:
            render_path_context = None
    unit_runtime_reader = runtime_reader or read_systemd_user_unit_runtime
    units = {}
    for name in selected_unit_names:
        source_path = source_paths[name]
        target_path = target_dir / name
        if not source_path.exists():
            units[name] = {
                "installed": target_path.exists(),
                "target_path": str(target_path),
                "source_path": str(source_path),
                "source_exists": False,
                "content_matches_repo": None,
                "enabled": None,
                "active": None,
                "runtime_state": None,
                "runtime_state_available": False,
                "runtime_state_error": "source unit template missing",
                "optional": is_optional_service_unit(name),
            }
            continue
        units[name] = _unit_installation_status(
            project_root=project_root,
            source_path=source_path,
            target_path=target_path,
            env_path=env_path,
            unit_name=name,
            runtime_reader=unit_runtime_reader,
            path_context=render_path_context,
        )
    primary_unit = units.get(SERVICE_UNIT_NAME) or next(iter(units.values()))
    required_unit_names = [name for name in selected_unit_names if name not in OPTIONAL_SERVICE_UNIT_NAMES]
    required_units = {name: units[name] for name in required_unit_names}
    required_units_installed = all(bool(unit.get("installed")) for unit in required_units.values())
    required_units_current = required_units_installed and all(bool(unit.get("content_matches_repo")) for unit in required_units.values())
    all_tracked_units_installed = all(bool(unit.get("installed")) for unit in units.values())
    all_tracked_units_current = all_tracked_units_installed and all(bool(unit.get("content_matches_repo")) for unit in units.values())
    missing_tracked_units = [name for name, unit in units.items() if not unit.get("installed")]
    outdated_tracked_units = [name for name, unit in units.items() if unit.get("installed") and not unit.get("content_matches_repo")]
    missing_required_units = [name for name in missing_tracked_units if name not in OPTIONAL_SERVICE_UNIT_NAMES]
    missing_optional_units = [name for name in missing_tracked_units if name in OPTIONAL_SERVICE_UNIT_NAMES]
    outdated_required_units = [name for name in outdated_tracked_units if name not in OPTIONAL_SERVICE_UNIT_NAMES]
    outdated_optional_units = [name for name in outdated_tracked_units if name in OPTIONAL_SERVICE_UNIT_NAMES]
    disabled_services = [name for name, unit in units.items() if unit.get("enabled") is False and name not in OPTIONAL_SERVICE_UNIT_NAMES]
    inactive_services = [name for name, unit in units.items() if unit.get("active") is False and name not in OPTIONAL_SERVICE_UNIT_NAMES]
    optional_disabled_services = [name for name, unit in units.items() if unit.get("enabled") is False and name in OPTIONAL_SERVICE_UNIT_NAMES]
    optional_inactive_services = [name for name, unit in units.items() if unit.get("active") is False and name in OPTIONAL_SERVICE_UNIT_NAMES]
    runtime_errors = {
        name: unit.get("runtime_state_error")
        for name, unit in units.items()
        if unit.get("runtime_state_error")
    }
    payload = {
        "available": True,
        "source_dir": str(source_dir),
        "install_script_path": str(script_path),
        "target_dir": str(target_dir),
        "env_path": str(env_path),
        "env_present": env_permissions["exists"],
        "env_mode_octal": env_permissions["mode_octal"],
        "env_restrictive": env_permissions["restrictive"],
        "env_permission_error": env_permissions["error"],
        "unit_name": unit_name,
        "unit_names": list(selected_unit_names),
        "tracked_unit_names": list(TRACKED_SERVICE_UNIT_NAMES),
        "optional_unit_names": list(OPTIONAL_SERVICE_UNIT_NAMES),
        "unit": primary_unit,
        "units": units,
        "all_units_installed": required_units_installed,
        "all_units_current": required_units_current,
        "all_tracked_units_installed": all_tracked_units_installed,
        "all_tracked_units_current": all_tracked_units_current,
        "required_units_installed": required_units_installed,
        "required_units_current": required_units_current,
        "service_enabled": primary_unit.get("enabled"),
        "service_active": primary_unit.get("active"),
        "runtime_state_available": required_units_installed and all(
            bool(unit.get("runtime_state_available")) for unit in required_units.values()
        ),
        "runtime_state_error": "; ".join(f"{name}: {error}" for name, error in runtime_errors.items()) or None,
        "runtime_state_errors": runtime_errors,
        "missing_units": missing_required_units,
        "missing_tracked_units": missing_tracked_units,
        "missing_required_units": missing_required_units,
        "missing_optional_units": missing_optional_units,
        "outdated_units": outdated_required_units,
        "outdated_tracked_units": outdated_tracked_units,
        "outdated_required_units": outdated_required_units,
        "outdated_optional_units": outdated_optional_units,
        "disabled_services": disabled_services,
        "inactive_services": inactive_services,
        "optional_disabled_services": optional_disabled_services,
        "optional_inactive_services": optional_inactive_services,
        "optional_units_note": "optional dashboard units may be installed while stopped/disabled without making the main daemon unhealthy",
    }
    safe = sanitize_value(payload, max_depth=8, max_items=200, max_string=1_000)
    return safe if isinstance(safe, dict) else None


def _run_status_command(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=check)
