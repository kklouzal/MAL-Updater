from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import _render_systemd_unit_template
from mal_updater.service_units import render_systemd_unit_template


class InstallUserSystemdUnitsScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.script_path = self.repo_root / "scripts" / "install_user_systemd_units.sh"
        self.source_dir = self.repo_root / "ops" / "systemd-user"
        self.fake_bin_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.fake_bin_dir.cleanup)
        fake_systemctl = Path(self.fake_bin_dir.name) / "systemctl"
        fake_systemctl.write_text(
            "#!/usr/bin/env bash\n"
            "printf 'fake systemctl %s\\n' \"$*\" >&2\n"
            "exit 1\n",
            encoding="utf-8",
        )
        fake_systemctl.chmod(0o755)

    def _run_script(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["PATH"] = f"{self.fake_bin_dir.name}:{env.get('PATH', '')}"
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
            self.assertIn("installed_units=mal-updater.service", result.stdout)
            self.assertIn("service_env_action=installed", result.stdout)
            self.assertIn("[dry-run] install -D -m 644", result.stdout)
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
            self.assertIn("installed_units=mal-updater.service", result.stdout)
            self.assertIn("service_env_action=installed", result.stdout)
            self.assertIn("service enable skipped (--no-enable)", result.stdout)
            self.assertIn("user-level MAL-Updater systemd service install completed", result.stdout)

            copied_path = target_dir / "mal-updater.service"
            self.assertTrue(copied_path.exists())
            rendered = copied_path.read_text(encoding="utf-8")
            self.assertNotIn("__MAL_UPDATER_REPO_ROOT__", rendered)
            self.assertNotIn("__MAL_UPDATER_SERVICE_ENV_FILE__", rendered)
            self.assertNotIn("__MAL_UPDATER_PYTHON_BIN__", rendered)
            self.assertNotIn("__MAL_UPDATER_WORKSPACE_ROOT__", rendered)
            self.assertIn(str(self.repo_root), rendered)
            self.assertIn(f"ExecStart={self.repo_root}/.venv/bin/python -m mal_updater.cli", rendered)

            expected_env = (self.source_dir / "mal-updater-service.env.example").read_text(encoding="utf-8")
            self.assertEqual(expected_env, env_target.read_text(encoding="utf-8"))

    def test_canonical_systemd_renderer_matches_cli_alias_for_default_and_custom_python_inputs(self) -> None:
        source_path = self.source_dir / "mal-updater.service"
        template_text = source_path.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"
            default_python = self.repo_root / ".venv" / "bin" / "python"
            custom_python = temp_root / "custom-venv" / "bin" / "python"

            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("MAL_UPDATER_SERVICE_PYTHON_BIN", None)
                rendered_by_alias_default = _render_systemd_unit_template(source_path, self.repo_root, env_target)
            rendered_by_canonical_default = render_systemd_unit_template(
                template_text,
                project_root=self.repo_root,
                env_path=env_target,
                python_bin=default_python,
            )
            rendered_by_alias_custom = _render_systemd_unit_template(source_path, self.repo_root, env_target, custom_python)
            rendered_by_canonical_custom = render_systemd_unit_template(
                template_text,
                project_root=self.repo_root,
                env_path=env_target,
                python_bin=custom_python,
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
            rendered_by_cli = _render_systemd_unit_template(
                self.source_dir / "mal-updater.service",
                self.repo_root,
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
            rendered_by_cli_same_inputs = _render_systemd_unit_template(
                self.source_dir / "mal-updater.service",
                self.repo_root,
                env_target,
                custom_python,
            )

            self.assertEqual(rendered_by_cli_same_inputs, rendered_by_script)
            self.assertIn(f"ExecStart={custom_python} -m mal_updater.cli --project-root {self.repo_root} service-run", rendered_by_script)
            self.assertNotIn(f"ExecStart={self.repo_root}/.venv/bin/python", rendered_by_script)

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

    def test_install_reports_updated_and_unchanged_units_separately(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-service.env"
            target_dir.mkdir(parents=True, exist_ok=True)

            unit_path = self.source_dir / "mal-updater.service"
            rendered_unchanged = _render_systemd_unit_template(
                unit_path,
                self.repo_root,
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
