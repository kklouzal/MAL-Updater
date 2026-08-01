from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent
_SRC_ROOT = _REPO_ROOT / "src"

for candidate in (_REPO_ROOT, _SRC_ROOT):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)


import mal_updater.config as _mal_updater_config


_ORIGINAL_LOAD_CONFIG = _mal_updater_config.load_config


_RUNTIME_ENV_NAMES = ("MAL_UPDATER_RUNTIME_ROOT", "MAL_UPDATER_RUNTIME_DIR")
_SETTINGS_ENV_NAMES = ("MAL_UPDATER_SETTINGS_PATH", "MAL_UPDATER_CONFIG")


def _has_runtime_config_override() -> bool:
    return any(os.environ.get(name) for name in (*_RUNTIME_ENV_NAMES, *_SETTINGS_ENV_NAMES))


def _pytest_runtime_isolated_load_config(project_root=None):
    if project_root is None or not _has_runtime_config_override():
        return _ORIGINAL_LOAD_CONFIG(project_root)

    root = Path(project_root).resolve()
    runtime_root = root / ".MAL-Updater"
    settings_path = runtime_root / "config" / "settings.toml"
    previous = {name: os.environ.get(name) for name in (*_RUNTIME_ENV_NAMES, *_SETTINGS_ENV_NAMES)}
    for name in _RUNTIME_ENV_NAMES:
        os.environ[name] = str(runtime_root)
    for name in _SETTINGS_ENV_NAMES:
        os.environ[name] = str(settings_path)
    try:
        return _ORIGINAL_LOAD_CONFIG(project_root)
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


_mal_updater_config.load_config = _pytest_runtime_isolated_load_config


@pytest.fixture(autouse=True)
def _isolate_runtime_env_when_command_supplies_runtime_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep env-gated test commands from sharing one runtime DB across tests.

    Some safety-critical invocations intentionally export a command-level
    ``MAL_UPDATER_RUNTIME_ROOT``/``MAL_UPDATER_SETTINGS_PATH`` (or the CI alias
    names) so tests cannot fall back to an ambient runtime.  Tests with an explicit project root are
    routed through the load_config wrapper above; other tests still need a
    non-ambient fallback, so give every pytest item its own runtime root under
    pytest's tmp path. Bare pytest runs without the override keep the
    historical environment, but they are not the documented hermetic gate.
    """
    if not _has_runtime_config_override():
        return

    runtime_root = tmp_path / ".MAL-Updater"
    settings_path = runtime_root / "config" / "settings.toml"
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    for name in _RUNTIME_ENV_NAMES:
        monkeypatch.setenv(name, str(runtime_root))
    for name in _SETTINGS_ENV_NAMES:
        monkeypatch.setenv(name, str(settings_path))
    monkeypatch.setenv("TMPDIR", "/tmp")
