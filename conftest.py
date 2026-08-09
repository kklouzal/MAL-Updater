from __future__ import annotations

import atexit
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent
_SRC_ROOT = _REPO_ROOT / "src"
_CANONICAL_RUNTIME_ROOT = (_REPO_ROOT / ".MAL-Updater").resolve()
_PYTEST_SESSION_ROOT = Path(tempfile.mkdtemp(prefix="mal-updater-pytest-", dir="/tmp")).resolve()
_PYTEST_RUNTIME_ROOT = (_PYTEST_SESSION_ROOT / ".MAL-Updater").resolve()
_PYTEST_SETTINGS_PATH = _PYTEST_RUNTIME_ROOT / "config" / "settings.toml"
_RUNTIME_ENV_NAMES = ("MAL_UPDATER_RUNTIME_ROOT", "MAL_UPDATER_RUNTIME_DIR")
_SETTINGS_ENV_NAMES = ("MAL_UPDATER_SETTINGS_PATH", "MAL_UPDATER_CONFIG")

atexit.register(shutil.rmtree, _PYTEST_SESSION_ROOT, ignore_errors=True)

for candidate in (_REPO_ROOT, _SRC_ROOT):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)

# Establish a hermetic fallback before test modules are imported or subprocess
# environments are copied. Explicit per-test overrides remain supported.
os.environ["MAL_UPDATER_RUNTIME_ROOT"] = str(_PYTEST_RUNTIME_ROOT)
os.environ["MAL_UPDATER_SETTINGS_PATH"] = str(_PYTEST_SETTINGS_PATH)
os.environ["MAL_UPDATER_PYTEST_FALLBACK_RUNTIME_ROOT"] = str(_PYTEST_RUNTIME_ROOT)
os.environ.pop("MAL_UPDATER_RUNTIME_DIR", None)
os.environ.pop("MAL_UPDATER_CONFIG", None)


import mal_updater.config as _mal_updater_config


_ORIGINAL_LOAD_CONFIG = _mal_updater_config.load_config
_ORIGINAL_SQLITE_CONNECT = sqlite3.connect


def _is_within(path: str | os.PathLike[str], parent: Path) -> bool:
    try:
        Path(path).resolve().relative_to(parent)
    except (OSError, RuntimeError, ValueError):
        return False
    return True


def _guarded_sqlite_connect(database, *args, **kwargs):
    # Reject canonical runtime access before SQLite can read, create, migrate,
    # or mutate anything there. URI mode does not bypass the guard.
    guarded_path = database
    if isinstance(database, str) and database.startswith("file:"):
        guarded_path = database.removeprefix("file:").split("?", 1)[0]
    if isinstance(guarded_path, (str, os.PathLike)) and _is_within(guarded_path, _CANONICAL_RUNTIME_ROOT):
        raise AssertionError(f"pytest blocked canonical MAL-Updater runtime database access: {Path(guarded_path).resolve()}")
    return _ORIGINAL_SQLITE_CONNECT(database, *args, **kwargs)


sqlite3.connect = _guarded_sqlite_connect


def _using_pytest_fallback() -> bool:
    runtime = os.environ.get("MAL_UPDATER_RUNTIME_ROOT") or os.environ.get("MAL_UPDATER_RUNTIME_DIR")
    fallback = os.environ.get("MAL_UPDATER_PYTEST_FALLBACK_RUNTIME_ROOT")
    return bool(runtime and fallback) and Path(runtime).resolve() == Path(fallback).resolve()


def _pytest_runtime_isolated_load_config(project_root=None):
    if project_root is None or not _using_pytest_fallback():
        config = _ORIGINAL_LOAD_CONFIG(project_root)
    else:
        # Tests supplying a temporary project root expect its default layout;
        # do not let the session fallback or workspace-marker discovery route
        # that root back to the checkout runtime.
        root = Path(project_root).resolve()
        runtime_root = root / ".MAL-Updater"
        settings_path = runtime_root / "config" / "settings.toml"
        previous = {name: os.environ.get(name) for name in (*_RUNTIME_ENV_NAMES, *_SETTINGS_ENV_NAMES)}
        os.environ["MAL_UPDATER_RUNTIME_ROOT"] = str(runtime_root)
        os.environ["MAL_UPDATER_SETTINGS_PATH"] = str(settings_path)
        os.environ.pop("MAL_UPDATER_RUNTIME_DIR", None)
        os.environ.pop("MAL_UPDATER_CONFIG", None)
        try:
            config = _ORIGINAL_LOAD_CONFIG(project_root)
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
    if _is_within(config.runtime_root, _CANONICAL_RUNTIME_ROOT) or _is_within(config.db_path, _CANONICAL_RUNTIME_ROOT):
        raise AssertionError(f"pytest resolved canonical MAL-Updater runtime: {config.runtime_root}")
    return config


_mal_updater_config.load_config = _pytest_runtime_isolated_load_config


@pytest.fixture(autouse=True)
def _isolate_runtime_per_test(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Give ambient load_config() calls and copied subprocess envs per-test state."""
    runtime_root = tmp_path / ".MAL-Updater"
    settings_path = runtime_root / "config" / "settings.toml"
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("MAL_UPDATER_RUNTIME_ROOT", str(runtime_root))
    monkeypatch.setenv("MAL_UPDATER_SETTINGS_PATH", str(settings_path))
    monkeypatch.setenv("MAL_UPDATER_PYTEST_FALLBACK_RUNTIME_ROOT", str(runtime_root))
    monkeypatch.delenv("MAL_UPDATER_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("MAL_UPDATER_CONFIG", raising=False)
    monkeypatch.setenv("TMPDIR", "/tmp")
