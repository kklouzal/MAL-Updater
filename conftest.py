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


def _pytest_runtime_isolated_load_config(project_root=None):
    if project_root is None or not (os.environ.get("MAL_UPDATER_RUNTIME_ROOT") or os.environ.get("MAL_UPDATER_SETTINGS_PATH")):
        return _ORIGINAL_LOAD_CONFIG(project_root)

    root = Path(project_root).resolve()
    runtime_root = root / ".MAL-Updater"
    settings_path = runtime_root / "config" / "settings.toml"
    previous_runtime_root = os.environ.get("MAL_UPDATER_RUNTIME_ROOT")
    previous_settings_path = os.environ.get("MAL_UPDATER_SETTINGS_PATH")
    os.environ["MAL_UPDATER_RUNTIME_ROOT"] = str(runtime_root)
    os.environ["MAL_UPDATER_SETTINGS_PATH"] = str(settings_path)
    try:
        return _ORIGINAL_LOAD_CONFIG(project_root)
    finally:
        if previous_runtime_root is None:
            os.environ.pop("MAL_UPDATER_RUNTIME_ROOT", None)
        else:
            os.environ["MAL_UPDATER_RUNTIME_ROOT"] = previous_runtime_root
        if previous_settings_path is None:
            os.environ.pop("MAL_UPDATER_SETTINGS_PATH", None)
        else:
            os.environ["MAL_UPDATER_SETTINGS_PATH"] = previous_settings_path


_mal_updater_config.load_config = _pytest_runtime_isolated_load_config


@pytest.fixture(autouse=True)
def _isolate_runtime_env_when_command_supplies_runtime_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep env-gated test commands from sharing one runtime DB across tests.

    Some safety-critical invocations intentionally export a command-level
    ``MAL_UPDATER_RUNTIME_ROOT``/``MAL_UPDATER_SETTINGS_PATH`` so tests cannot
    fall back to an ambient runtime.  Tests with an explicit project root are
    routed through the load_config wrapper above; other tests still need a
    non-ambient fallback, so give every pytest item its own runtime root under
    pytest's tmp path. Normal test runs without the override keep the
    historical environment.
    """
    if not (os.environ.get("MAL_UPDATER_RUNTIME_ROOT") or os.environ.get("MAL_UPDATER_SETTINGS_PATH")):
        return

    runtime_root = tmp_path / ".MAL-Updater"
    settings_path = runtime_root / "config" / "settings.toml"
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("MAL_UPDATER_RUNTIME_ROOT", str(runtime_root))
    monkeypatch.setenv("MAL_UPDATER_SETTINGS_PATH", str(settings_path))
    monkeypatch.setenv("TMPDIR", "/tmp")
