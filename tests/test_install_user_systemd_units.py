from __future__ import annotations

import os
import subprocess
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import _render_systemd_unit_template
from mal_updater.config import load_config
from mal_updater.service_units import render_systemd_unit_template, systemd_unit_path_context


class InstallUserSystemdUnitsScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.script_path = self.repo_root / "scripts" / "install_user_systemd_units.sh"
        self.source_dir = self.repo_root / "ops" / "systemd-user"
        self.fake_bin_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.fake_bin_dir.cleanup)
        self.runtime_dir = tempfile.TemporaryDirectory(prefix="mal-updater-install-test-runtime-", dir="/tmp")
        self.addCleanup(self.runtime_dir.cleanup)
        self.settings_path = Path(self.runtime_dir.name) / "config" / "settings.toml"
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings_path.write_text("", encoding="utf-8")
        self.env_patch = patch.dict(
            os.environ,
            {
                "MAL_UPDATER_RUNTIME_ROOT": self.runtime_dir.name,
                "MAL_UPDATER_SETTINGS_PATH": str(self.settings_path),
            },
            clear=False,
        )
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        fake_systemctl = Path(self.fake_bin_dir.name) / "systemctl"
        fake_systemctl.write_text(
            "#!/usr/bin/env bash\n"
            "printf 'fake systemctl %s\\n' \"$*\" >&2\n"
            "exit 1\n",
            encoding="utf-8",
        )
        fake_systemctl.chmod(0o755)

    def _renderer_env(self) -> dict[str, str]:
        return {
            "MAL_UPDATER_RUNTIME_ROOT": self.runtime_dir.name,
            "MAL_UPDATER_SETTINGS_PATH": str(self.settings_path),
        }

    def _render_path_context(self) -> dict[str, str]:
        with patch.dict(os.environ, self._renderer_env(), clear=False):
            return systemd_unit_path_context(load_config())

    def _render_cli_unit(
        self,
        source_path: Path,
        env_target: Path,
        python_bin: Path | None = None,
    ) -> str:
        with patch.dict(os.environ, self._renderer_env(), clear=False):
            os.environ.pop("MAL_UPDATER_SERVICE_PYTHON_BIN", None)
            return _render_systemd_unit_template(
                source_path,
                self.repo_root,
                env_target,
                python_bin,
                path_context=systemd_unit_path_context(load_config()),
            )

    def _run_script(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["PATH"] = f"{self.fake_bin_dir.name}:{env.get('PATH', '')}"
        env.update(self._renderer_env())
        return subprocess.run(
            [str(self.script_path), *args],
            cwd=self.repo_root,
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )

    def test_dry_run_reports_planned_actions_without_writing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--dry-run",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertFalse(target_dir.exists())
            self.assertFalse(env_target.exists())
            self.assertIn("selected_units=mal-updater.service", result.stdout)
            self.assertIn("dashboard_unit_action=skipped", result.stdout)
            self.assertIn("installed_units=mal-updater.service", result.stdout)
            self.assertIn("service_env_action=installed", result.stdout)
            self.assertIn("[dry-run] install -D -m 600", result.stdout)
            self.assertIn("[dry-run] systemctl --user daemon-reload", result.stdout)
            self.assertIn("[dry-run] systemctl --user enable mal-updater.service", result.stdout)

    def test_install_copies_service_unit_and_example_env_without_systemctl_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertTrue(target_dir.is_dir())
            self.assertTrue(env_target.exists())
            self.assertEqual(0o600, stat.S_IMODE(env_target.stat().st_mode))
            self.assertIn("installed_units=mal-updater.service", result.stdout)
            self.assertIn("service_env_action=installed", result.stdout)
            self.assertIn("service_env_mode=0o600", result.stdout)
            self.assertIn("service_env_restrictive=true", result.stdout)
            self.assertIn("service enable skipped (--no-enable)", result.stdout)
            self.assertIn("user-level MAL-Updater systemd service install completed", result.stdout)
            self.assertFalse((target_dir / "mal-updater-dashboard.service").exists())

            copied_path = target_dir / "mal-updater.service"
            self.assertTrue(copied_path.exists())
            rendered = copied_path.read_text(encoding="utf-8")
            self.assertNotIn("__MAL_UPDATER_REPO_ROOT__", rendered)
            self.assertNotIn("__MAL_UPDATER_SERVICE_ENV_FILE__", rendered)
            self.assertNotIn("__MAL_UPDATER_PYTHON_BIN__", rendered)
            self.assertNotIn("__MAL_UPDATER_WORKSPACE_ROOT__", rendered)
            self.assertNotIn("__MAL_UPDATER_RUNTIME_ROOT__", rendered)
            self.assertNotIn("__MAL_UPDATER_CONFIG_DIR__", rendered)
            self.assertNotIn("__MAL_UPDATER_DB_DIR__", rendered)
            self.assertNotIn("__MAL_UPDATER_READ_WRITE_PATHS__", rendered)
            self.assertIn(str(self.repo_root), rendered)
            self.assertIn(f"ExecStart={self.repo_root}/.venv/bin/python -m mal_updater.cli", rendered)
            self.assertIn("UMask=0077", rendered)
            self.assertIn("NoNewPrivileges=true", rendered)
            self.assertIn("PrivateTmp=true", rendered)
            self.assertIn("ProtectSystem=strict", rendered)
            self.assertIn("ProtectHome=read-only", rendered)
            self.assertIn("RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6", rendered)
            self.assertIn(f"ReadWritePaths={self.runtime_dir.name}", rendered)

            expected_env = (self.source_dir / "mal-updater-service.env.example").read_text(encoding="utf-8")
            self.assertEqual(expected_env, env_target.read_text(encoding="utf-8"))

    def test_default_install_targets_follow_xdg_config_home_without_home_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            fake_home = temp_root / "fake-home"
            xdg_config_home = temp_root / "xdg-config"
            target_dir = xdg_config_home / "systemd" / "user"
            env_target = xdg_config_home / "mal-updater-service.env"

            with patch.dict(os.environ, {"HOME": str(fake_home), "XDG_CONFIG_HOME": str(xdg_config_home)}, clear=False):
                result = self._run_script("--no-enable", "--no-daemon-reload")

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn(f"target_dir={target_dir}", result.stdout)
            self.assertIn(f"service_env_target={env_target}", result.stdout)
            self.assertTrue((target_dir / "mal-updater.service").exists())
            self.assertTrue(env_target.exists())
            self.assertEqual(0o600, stat.S_IMODE(env_target.stat().st_mode))
            self.assertFalse((fake_home / ".config" / "systemd" / "user" / "mal-updater.service").exists())
            self.assertFalse((fake_home / ".config" / "mal-updater-service.env").exists())

    def test_canonical_systemd_renderer_matches_cli_alias_for_default_and_custom_python_inputs(self) -> None:
        source_path = self.source_dir / "mal-updater.service"
        template_text = source_path.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"
            default_python = self.repo_root / ".venv" / "bin" / "python"
            custom_python = temp_root / "custom-venv" / "bin" / "python"
            path_context = self._render_path_context()

            rendered_by_alias_default = self._render_cli_unit(source_path, env_target)
            rendered_by_canonical_default = render_systemd_unit_template(
                template_text,
                project_root=self.repo_root,
                env_path=env_target,
                python_bin=default_python,
                path_context=path_context,
            )
            rendered_by_alias_custom = self._render_cli_unit(source_path, env_target, custom_python)
            rendered_by_canonical_custom = render_systemd_unit_template(
                template_text,
                project_root=self.repo_root,
                env_path=env_target,
                python_bin=custom_python,
                path_context=path_context,
            )

            self.assertEqual(rendered_by_canonical_default, rendered_by_alias_default)
            self.assertEqual(rendered_by_canonical_custom, rendered_by_alias_custom)
            self.assertIn(f"ExecStart={default_python} -m mal_updater.cli --project-root {self.repo_root} service-run", rendered_by_alias_default)
            self.assertIn(f"ExecStart={custom_python} -m mal_updater.cli --project-root {self.repo_root} service-run", rendered_by_alias_custom)

    def test_script_and_cli_systemd_rendering_contract_are_equivalent_for_default_install_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            rendered_by_script = (target_dir / "mal-updater.service").read_text(encoding="utf-8")
            rendered_by_cli = self._render_cli_unit(
                self.source_dir / "mal-updater.service",
                env_target,
                self.repo_root / ".venv" / "bin" / "python",
            )

            self.assertEqual(rendered_by_cli, rendered_by_script)
            self.assertIn(f"WorkingDirectory={self.repo_root}", rendered_by_script)
            self.assertIn(f"Environment=PYTHONPATH={self.repo_root}/src", rendered_by_script)
            self.assertIn(f"EnvironmentFile=-{env_target}", rendered_by_script)
            self.assertIn(
                f"ExecStart={self.repo_root}/.venv/bin/python -m mal_updater.cli --project-root {self.repo_root} service-run",
                rendered_by_script,
            )
            self.assertIn("Restart=always", rendered_by_script)
            self.assertIn("UMask=0077", rendered_by_script)
            self.assertIn("ProtectSystem=strict", rendered_by_script)
            self.assertIn("ProtectHome=read-only", rendered_by_script)
            self.assertIn("WantedBy=default.target", rendered_by_script)

    def test_script_and_cli_systemd_rendering_preserve_custom_python_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"
            custom_python = temp_root / "custom-venv" / "bin" / "python"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--service-python-bin",
                str(custom_python),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            rendered_by_script = (target_dir / "mal-updater.service").read_text(encoding="utf-8")
            rendered_by_cli_same_inputs = self._render_cli_unit(
                self.source_dir / "mal-updater.service",
                env_target,
                custom_python,
            )

            self.assertEqual(rendered_by_cli_same_inputs, rendered_by_script)
            self.assertIn(f"ExecStart={custom_python} -m mal_updater.cli --project-root {self.repo_root} service-run", rendered_by_script)
            self.assertNotIn(f"ExecStart={self.repo_root}/.venv/bin/python", rendered_by_script)

    def test_dashboard_unit_requires_explicit_install_and_stays_loopback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--install-dashboard",
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("selected_units=mal-updater.service,mal-updater-dashboard.service", result.stdout)
            self.assertIn("installed_units=mal-updater.service,mal-updater-dashboard.service", result.stdout)
            self.assertIn("dashboard enable skipped", result.stdout)
            dashboard_unit = target_dir / "mal-updater-dashboard.service"
            self.assertTrue(dashboard_unit.exists())
            rendered = dashboard_unit.read_text(encoding="utf-8")
            self.assertIn("dashboard-serve --host 127.0.0.1", rendered)
            self.assertNotIn("--host 0.0.0.0", rendered)
            self.assertIn(f"ExecStart={self.repo_root}/.venv/bin/python -m mal_updater.cli", rendered)
            self.assertIn(f"EnvironmentFile=-{env_target}", rendered)
            self.assertIn("NoNewPrivileges=true", rendered)
            self.assertIn("ProtectHome=read-only", rendered)

    def test_enable_dashboard_is_an_explicit_opt_in_and_does_not_restart_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--enable-dashboard",
                "--start-service",
                "--dry-run",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("[dry-run] systemctl --user enable mal-updater.service", result.stdout)
            self.assertIn("[dry-run] systemctl --user enable mal-updater-dashboard.service", result.stdout)
            self.assertIn("[dry-run] systemctl --user restart mal-updater.service", result.stdout)
            self.assertNotIn("restart mal-updater-dashboard.service", result.stdout)

    def test_existing_service_env_is_left_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"
            env_target.parent.mkdir(parents=True, exist_ok=True)
            env_target.write_text("MAL_UPDATER_SERVICE_LOOP_SLEEP_SECONDS=15\n", encoding="utf-8")

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertEqual("MAL_UPDATER_SERVICE_LOOP_SLEEP_SECONDS=15\n", env_target.read_text(encoding="utf-8"))
            self.assertIn("service env already exists; leaving it untouched", result.stdout)
            self.assertIn("service_env_action=preserved", result.stdout)
            self.assertIn("service_env_restrictive=", result.stdout)

    def test_install_reports_updated_and_unchanged_units_separately(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"
            target_dir.mkdir(parents=True, exist_ok=True)

            unit_path = self.source_dir / "mal-updater.service"
            rendered_unchanged = self._render_cli_unit(
                unit_path,
                env_target,
                self.repo_root / ".venv" / "bin" / "python",
            )
            (target_dir / unit_path.name).write_text(rendered_unchanged, encoding="utf-8")

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("unchanged_units=mal-updater.service", result.stdout)

            (target_dir / unit_path.name).write_text("[Unit]\nDescription=stale copy\n", encoding="utf-8")
            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--service-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )
            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("updated_units=mal-updater.service", result.stdout)


if __name__ == "__main__":
    unittest.main()
