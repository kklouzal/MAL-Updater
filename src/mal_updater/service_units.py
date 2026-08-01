from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

SERVICE_UNIT_NAME = "mal-updater.service"
DASHBOARD_SERVICE_UNIT_NAME = "mal-updater-dashboard.service"
TRACKED_SERVICE_UNIT_NAMES = (SERVICE_UNIT_NAME, DASHBOARD_SERVICE_UNIT_NAME)
DEFAULT_INSTALL_SERVICE_UNIT_NAMES = (SERVICE_UNIT_NAME,)
OPTIONAL_SERVICE_UNIT_NAMES = (DASHBOARD_SERVICE_UNIT_NAME,)
SERVICE_UNIT_TEMPLATE_RELATIVE_PATH = Path("ops") / "systemd-user" / SERVICE_UNIT_NAME

_REPO_ROOT_PLACEHOLDER = "__MAL_UPDATER_REPO_ROOT__"
_SERVICE_ENV_FILE_PLACEHOLDER = "__MAL_UPDATER_SERVICE_ENV_FILE__"
_PYTHON_BIN_PLACEHOLDER = "__MAL_UPDATER_PYTHON_BIN__"
_RUNTIME_ROOT_PLACEHOLDER = "__MAL_UPDATER_RUNTIME_ROOT__"
_CONFIG_DIR_PLACEHOLDER = "__MAL_UPDATER_CONFIG_DIR__"
_SECRETS_DIR_PLACEHOLDER = "__MAL_UPDATER_SECRETS_DIR__"
_DATA_DIR_PLACEHOLDER = "__MAL_UPDATER_DATA_DIR__"
_STATE_DIR_PLACEHOLDER = "__MAL_UPDATER_STATE_DIR__"
_CACHE_DIR_PLACEHOLDER = "__MAL_UPDATER_CACHE_DIR__"
_DB_DIR_PLACEHOLDER = "__MAL_UPDATER_DB_DIR__"
_READ_WRITE_PATHS_PLACEHOLDER = "__MAL_UPDATER_READ_WRITE_PATHS__"

_PATH_PLACEHOLDERS = (
    _RUNTIME_ROOT_PLACEHOLDER,
    _CONFIG_DIR_PLACEHOLDER,
    _SECRETS_DIR_PLACEHOLDER,
    _DATA_DIR_PLACEHOLDER,
    _STATE_DIR_PLACEHOLDER,
    _CACHE_DIR_PLACEHOLDER,
    _DB_DIR_PLACEHOLDER,
    _READ_WRITE_PATHS_PLACEHOLDER,
)
_UNSUPPORTED_SYSTEMD_PATH_CHARS = re.compile(r"[\x00-\x20\x7f]")


def default_service_python_bin(project_root: Path | str) -> Path:
    return Path(project_root).resolve() / ".venv" / "bin" / "python"


def resolve_service_python_bin(project_root: Path | str, python_bin: Path | str | None = None) -> str:
    if python_bin:
        return str(python_bin)
    env_python_bin = os.environ.get("MAL_UPDATER_SERVICE_PYTHON_BIN")
    if env_python_bin:
        return env_python_bin
    return str(default_service_python_bin(project_root))


def unit_template_relative_path(unit_name: str) -> Path:
    return Path("ops") / "systemd-user" / unit_name


def is_optional_service_unit(unit_name: str) -> bool:
    return unit_name in OPTIONAL_SERVICE_UNIT_NAMES


def normalize_unit_names(unit_names: Iterable[str] | None = None) -> tuple[str, ...]:
    raw_names = tuple(unit_names or TRACKED_SERVICE_UNIT_NAMES)
    normalized: list[str] = []
    seen: set[str] = set()
    for unit_name in raw_names:
        name = str(unit_name).strip()
        if not name or name in seen:
            continue
        normalized.append(name)
        seen.add(name)
    return tuple(normalized)


def _load_config_for_render(project_root: Path | str):
    from .config import load_config

    return load_config(Path(project_root).resolve())


def _systemd_path_value(path: Path | str, *, label: str, allow_root: bool = False) -> str:
    """Return a path value supported by the repo-owned user-unit templates.

    The templates intentionally avoid shell quoting or systemd C-style escaping.
    Keep the contract explicit: rendered paths must be absolute, must not be the
    filesystem root for writable allowlists, and must not contain whitespace or
    control characters that systemd would split into surprising tokens.
    """

    raw_path = Path(path).expanduser()
    if not raw_path.is_absolute():
        raise ValueError(f"{label} must resolve to an absolute path for systemd unit rendering")
    value = str(raw_path)
    if not allow_root and value == "/":
        raise ValueError(f"{label} must not be / in systemd unit rendering")
    if _UNSUPPORTED_SYSTEMD_PATH_CHARS.search(value):
        raise ValueError(
            f"{label} contains whitespace/control characters that are unsupported by the deterministic systemd renderer: {value!r}"
        )
    return value


def _dedupe_write_paths(paths: Iterable[Path | str]) -> list[str]:
    selected: list[Path] = []
    for raw_path in paths:
        value = Path(_systemd_path_value(raw_path, label="ReadWritePaths entry"))
        if any(value == existing or value.is_relative_to(existing) for existing in selected):
            continue
        selected = [existing for existing in selected if not existing.is_relative_to(value)]
        selected.append(value)
    return [str(path) for path in selected]


def systemd_unit_path_context(config: object) -> dict[str, str]:
    """Build the explicit path placeholder contract for systemd unit rendering.

    Callers that already loaded configuration should pass this context into the
    renderer. That keeps script, CLI/service-manager, and tests comparing the
    same resolved runtime/settings view instead of depending on ambient process
    environment at render time.
    """

    runtime_root = getattr(config, "runtime_root")
    config_dir = getattr(config, "config_dir")
    secrets_dir = getattr(config, "secrets_dir")
    data_dir = getattr(config, "data_dir")
    state_dir = getattr(config, "state_dir")
    cache_dir = getattr(config, "cache_dir")
    db_path = getattr(config, "db_path")
    raw_paths = [runtime_root, config_dir, secrets_dir, data_dir, state_dir, cache_dir, Path(db_path).parent]
    write_paths = _dedupe_write_paths(raw_paths)
    individual_values: dict[str, str] = {}
    emitted: list[str] = []
    for placeholder, raw_path in zip(
        (
            _RUNTIME_ROOT_PLACEHOLDER,
            _CONFIG_DIR_PLACEHOLDER,
            _SECRETS_DIR_PLACEHOLDER,
            _DATA_DIR_PLACEHOLDER,
            _STATE_DIR_PLACEHOLDER,
            _CACHE_DIR_PLACEHOLDER,
            _DB_DIR_PLACEHOLDER,
        ),
        raw_paths,
    ):
        value = _systemd_path_value(raw_path, label=placeholder)
        if any(Path(value) == Path(existing) or Path(value).is_relative_to(Path(existing)) for existing in emitted):
            individual_values[placeholder] = ""
            continue
        individual_values[placeholder] = value
        emitted.append(value)
    return {
        **individual_values,
        _READ_WRITE_PATHS_PLACEHOLDER: " ".join(write_paths),
    }


def _render_path_context(project_root: Path | str) -> dict[str, str]:
    """Resolve writable MAL-Updater paths for systemd sandbox allowlists.

    The user-unit templates use ProtectHome=read-only with explicit ReadWritePaths.
    Rendering those paths from the same config loader as the service keeps default
    SQLite/runtime writes and configured external state directories writable.
    """

    return systemd_unit_path_context(_load_config_for_render(project_root))


def render_systemd_unit_template(
    template_text: str,
    *,
    project_root: Path | str,
    env_path: Path | str,
    python_bin: Path | str,
    path_context: dict[str, str] | None = None,
) -> str:
    """Render a MAL-Updater systemd unit template with deterministic placeholder replacement."""
    resolved_project_root = Path(project_root).resolve()
    rendered = (
        template_text.replace(_REPO_ROOT_PLACEHOLDER, _systemd_path_value(resolved_project_root, label=_REPO_ROOT_PLACEHOLDER, allow_root=True))
        .replace(_SERVICE_ENV_FILE_PLACEHOLDER, _systemd_path_value(env_path, label=_SERVICE_ENV_FILE_PLACEHOLDER))
        .replace(_PYTHON_BIN_PLACEHOLDER, _systemd_path_value(python_bin, label=_PYTHON_BIN_PLACEHOLDER))
    )
    replacements = path_context
    if replacements is None:
        replacements = _render_path_context(resolved_project_root) if any(placeholder in rendered for placeholder in _PATH_PLACEHOLDERS) else {}
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    return rendered


def render_systemd_unit_template_file(
    source_path: Path | str,
    project_root: Path | str,
    env_path: Path | str,
    python_bin: Path | str | None = None,
    *,
    path_context: dict[str, str] | None = None,
) -> str:
    service_python_bin = resolve_service_python_bin(project_root, python_bin)
    return render_systemd_unit_template(
        Path(source_path).read_text(encoding="utf-8"),
        project_root=project_root,
        env_path=env_path,
        python_bin=service_python_bin,
        path_context=path_context,
    )


def render_repo_systemd_unit_template(
    project_root: Path | str,
    env_path: Path | str,
    python_bin: Path | str | None = None,
    *,
    unit_name: str = SERVICE_UNIT_NAME,
    path_context: dict[str, str] | None = None,
) -> str:
    template_path = Path(project_root) / unit_template_relative_path(unit_name)
    return render_systemd_unit_template_file(template_path, project_root, env_path, python_bin, path_context=path_context)
