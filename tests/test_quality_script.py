from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
QUALITY_ENV_KEYS = {"quality_runtime", "quality_settings", "quality_tmp"}


def _parse_quality_output(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        key, _, value = line.partition("=")
        if key:
            values[key] = value
    return values


def test_quality_print_env_prefers_canonical_ambient_over_legacy(tmp_path: Path) -> None:
    canonical_runtime = tmp_path / "canonical-runtime"
    legacy_runtime = tmp_path / "legacy-runtime"
    canonical_settings = tmp_path / "canonical-config" / "settings.toml"
    legacy_settings = tmp_path / "legacy-config" / "settings.toml"
    quality_tmp = tmp_path / "quality-tmp"

    env = {
        "PATH": os.environ.get("PATH", ""),
        "MAL_UPDATER_QUALITY_ALLOW_AMBIENT": "1",
        "MAL_UPDATER_QUALITY_TMP": str(quality_tmp),
        "MAL_UPDATER_RUNTIME_ROOT": str(canonical_runtime),
        "MAL_UPDATER_RUNTIME_DIR": str(legacy_runtime),
        "MAL_UPDATER_SETTINGS_PATH": str(canonical_settings),
        "MAL_UPDATER_CONFIG": str(legacy_settings),
    }
    result = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "quality.sh"), "--print-env"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    values = _parse_quality_output(result.stdout)
    assert result.stderr == ""
    assert set(values) == QUALITY_ENV_KEYS
    assert values["quality_runtime"] == str(canonical_runtime)
    assert values["quality_settings"] == str(canonical_settings)
    assert values["quality_tmp"] == str(quality_tmp / "tmp")
    assert canonical_settings.exists()
    assert not legacy_settings.exists()


def test_quality_print_env_uses_legacy_ambient_when_canonical_unset(tmp_path: Path) -> None:
    legacy_runtime = tmp_path / "legacy-runtime"
    legacy_settings = tmp_path / "legacy-config" / "settings.toml"
    quality_tmp = tmp_path / "quality-tmp"

    env = {
        "PATH": os.environ.get("PATH", ""),
        "MAL_UPDATER_QUALITY_ALLOW_AMBIENT": "1",
        "MAL_UPDATER_QUALITY_TMP": str(quality_tmp),
        "MAL_UPDATER_RUNTIME_DIR": str(legacy_runtime),
        "MAL_UPDATER_CONFIG": str(legacy_settings),
    }
    result = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "quality.sh"), "--print-env"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    values = _parse_quality_output(result.stdout)
    assert result.stderr == ""
    assert set(values) == QUALITY_ENV_KEYS
    assert values["quality_runtime"] == str(legacy_runtime)
    assert values["quality_settings"] == str(legacy_settings)
    assert values["quality_tmp"] == str(quality_tmp / "tmp")
    assert legacy_settings.exists()
