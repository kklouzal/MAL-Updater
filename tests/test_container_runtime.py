from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.container_runtime import daemon_command, initialize_runtime, setup_mode


class ContainerRuntimeTests(unittest.TestCase):
    def test_setup_mode_is_safe_by_default_and_requires_explicit_true(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(setup_mode())
        with patch.dict(os.environ, {"MAL_UPDATER_CONTAINER_ENABLE_DAEMON": "false"}, clear=True):
            self.assertTrue(setup_mode())
        with patch.dict(os.environ, {"MAL_UPDATER_CONTAINER_ENABLE_DAEMON": "true"}, clear=True):
            self.assertFalse(setup_mode())

    def test_initialize_runtime_creates_layout_and_migrated_database(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-updater-container-test-", dir="/tmp") as raw:
            runtime = Path(raw) / "data"
            settings = runtime / "config" / "settings.toml"
            with patch.dict(
                os.environ,
                {"MAL_UPDATER_RUNTIME_ROOT": str(runtime), "MAL_UPDATER_SETTINGS_PATH": str(settings)},
                clear=False,
            ):
                config = initialize_runtime(Path(__file__).resolve().parents[1])
            self.assertTrue(config.db_path.is_file())
            for name in ("config", "secrets", "data", "state", "cache"):
                self.assertTrue((runtime / name).is_dir())

    def test_daemon_command_uses_installed_cli_and_explicit_project_root(self) -> None:
        command = daemon_command(Path("/app"))
        self.assertEqual(["-m", "mal_updater.cli", "--project-root", "/app", "service-run"], command[1:])


if __name__ == "__main__":
    unittest.main()
