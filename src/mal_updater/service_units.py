from __future__ import annotations

import os
from pathlib import Path

SERVICE_UNIT_NAME = "mal-updater.service"
SERVICE_UNIT_TEMPLATE_RELATIVE_PATH = Path("ops") / "systemd-user" / SERVICE_UNIT_NAME

_REPO_ROOT_PLACEHOLDER = "__MAL_UPDATER_REPO_ROOT__"
_SERVICE_ENV_FILE_PLACEHOLDER = "__MAL_UPDATER_SERVICE_ENV_FILE__"
_PYTHON_BIN_PLACEHOLDER = "__MAL_UPDATER_PYTHON_BIN__"


def default_service_python_bin(project_root: Path | str) -> Path:
    return Path(project_root).resolve() / ".venv" / "bin" / "python"


def resolve_service_python_bin(project_root: Path | str, python_bin: Path | str | None = None) -> str:
    if python_bin:
        return str(python_bin)
    env_python_bin = os.environ.get("MAL_UPDATER_SERVICE_PYTHON_BIN")
    if env_python_bin:
        return env_python_bin
    return str(default_service_python_bin(project_root))


def render_systemd_unit_template(
    template_text: str,
    *,
    project_root: Path | str,
    env_path: Path | str,
    python_bin: Path | str,
) -> str:
    """Render a MAL-Updater systemd unit template with deterministic placeholder replacement."""
    resolved_project_root = Path(project_root).resolve()
    return (
        template_text.replace(_REPO_ROOT_PLACEHOLDER, str(resolved_project_root))
        .replace(_SERVICE_ENV_FILE_PLACEHOLDER, str(env_path))
        .replace(_PYTHON_BIN_PLACEHOLDER, str(python_bin))
    )


def render_systemd_unit_template_file(
    source_path: Path | str,
    project_root: Path | str,
    env_path: Path | str,
    python_bin: Path | str | None = None,
) -> str:
    service_python_bin = resolve_service_python_bin(project_root, python_bin)
    return render_systemd_unit_template(
        Path(source_path).read_text(encoding="utf-8"),
        project_root=project_root,
        env_path=env_path,
        python_bin=service_python_bin,
    )


def render_repo_systemd_unit_template(
    project_root: Path | str,
    env_path: Path | str,
    python_bin: Path | str | None = None,
    *,
    unit_name: str = SERVICE_UNIT_NAME,
) -> str:
    if unit_name != SERVICE_UNIT_NAME:
        template_path = Path(project_root) / "ops" / "systemd-user" / unit_name
    else:
        template_path = Path(project_root) / SERVICE_UNIT_TEMPLATE_RELATIVE_PATH
    return render_systemd_unit_template_file(template_path, project_root, env_path, python_bin)
