from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import Any

from .config import AppConfig, ensure_directories, load_config

SERVICE_NAME = "mal-updater.service"


@dataclass(slots=True)
class ServiceCommandResult:
    status: str
    message: str
    details: dict[str, Any] | None = None


def _unit_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / SERVICE_NAME


def _service_env_path() -> Path:
    return Path.home() / ".config" / "mal-updater-service.env"


def _run(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=check)


def unit_contents(config: AppConfig | None = None) -> str:
    config = config or load_config()
    ensure_directories(config)
    repo = config.project_root
    env_file = _service_env_path()
    python = Path(subprocess.run(["python3", "-c", "import sys; print(sys.executable)"], text=True, capture_output=True, check=True).stdout.strip())
    return f"""[Unit]
Description=MAL-Updater background sync daemon
After=default.target

[Service]
Type=simple
WorkingDirectory={repo}
Environment=PYTHONPATH={repo / 'src'}
Environment=MAL_UPDATER_WORKSPACE_DIR={config.workspace_root}
EnvironmentFile=-{env_file}
ExecStart={python} -m mal_updater.cli service-run --project-root {repo}
Restart=always
RestartSec=15

[Install]
WantedBy=default.target
"""


def write_unit_file(config: AppConfig | None = None) -> Path:
    config = config or load_config()
    path = _unit_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(unit_contents(config), encoding="utf-8")
    return path


def daemon_reload() -> None:
    _run(["systemctl", "--user", "daemon-reload"])


def service_status() -> dict[str, Any]:
    unit = _unit_path()
    enabled = _run(["systemctl", "--user", "is-enabled", SERVICE_NAME], check=False)
    active = _run(["systemctl", "--user", "is-active", SERVICE_NAME], check=False)
    return {
        "unit_path": str(unit),
        "unit_exists": unit.exists(),
        "enabled": enabled.returncode == 0 and enabled.stdout.strip() == "enabled",
        "active": active.returncode == 0 and active.stdout.strip() == "active",
        "enabled_raw": enabled.stdout.strip() or enabled.stderr.strip(),
        "active_raw": active.stdout.strip() or active.stderr.strip(),
        "env_path": str(_service_env_path()),
        "env_exists": _service_env_path().exists(),
    }


def install_service(*, start_now: bool = True, config: AppConfig | None = None) -> ServiceCommandResult:
    config = config or load_config()
    unit = write_unit_file(config)
    daemon_reload()
    _run(["systemctl", "--user", "enable", SERVICE_NAME])
    if start_now:
        _run(["systemctl", "--user", "restart", SERVICE_NAME])
    return ServiceCommandResult(status="ok", message="MAL-Updater service installed.", details={"unit_path": str(unit), **service_status()})


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
    return {
        **status,
        "service_log_exists": config.service_log_path.exists(),
        "service_state_exists": config.service_state_path.exists(),
        "api_request_events_exists": config.api_request_events_path.exists(),
    }
