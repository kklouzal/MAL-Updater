from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


class InstallUserSystemdUnitsScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.script_path = self.repo_root / "scripts" / "install_user_systemd_units.sh"
        self.source_dir = self.repo_root / "ops" / "systemd-user"

    def _run_script(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.script_path), *args],
            cwd=self.repo_root,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_dry_run_reports_planned_actions_without_writing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-health-check.env"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--health-env-target",
                str(env_target),
                "--dry-run",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertFalse(target_dir.exists())
            self.assertFalse(env_target.exists())
            self.assertIn("installed_units=mal-updater-exact-approved-sync.service, mal-updater-exact-approved-sync.timer, mal-updater-health-check.service, mal-updater-health-check.timer", result.stdout)
            self.assertIn("health_env_action=installed", result.stdout)
            self.assertIn("[dry-run] install -D -m 644", result.stdout)
            self.assertIn("[dry-run] systemctl --user daemon-reload", result.stdout)
            self.assertIn("[dry-run] systemctl --user enable --now", result.stdout)

    def test_install_copies_units_and_example_env_without_systemctl_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-health-check.env"

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--health-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertTrue(target_dir.is_dir())
            self.assertTrue(env_target.exists())
            self.assertIn("installed_units=mal-updater-exact-approved-sync.service, mal-updater-exact-approved-sync.timer, mal-updater-health-check.service, mal-updater-health-check.timer", result.stdout)
            self.assertIn("health_env_action=installed", result.stdout)
            self.assertIn("timer enable/start skipped (--no-enable)", result.stdout)
            self.assertIn("user-level MAL-Updater systemd unit install completed", result.stdout)

            for unit_path in sorted(self.source_dir.glob("*.service")) + sorted(self.source_dir.glob("*.timer")):
                copied_path = target_dir / unit_path.name
                self.assertTrue(copied_path.exists(), msg=f"missing copied unit {unit_path.name}")
                rendered = copied_path.read_text(encoding="utf-8")
                self.assertNotIn("__MAL_UPDATER_REPO_ROOT__", rendered)
                self.assertNotIn("__MAL_UPDATER_HEALTH_ENV_FILE__", rendered)
                if unit_path.suffix == ".service":
                    self.assertIn(str(self.repo_root), rendered)

            expected_env = (self.source_dir / "mal-updater-health-check.env.example").read_text(encoding="utf-8")
            self.assertEqual(expected_env, env_target.read_text(encoding="utf-8"))

    def test_existing_health_env_is_left_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-health-check.env"
            env_target.parent.mkdir(parents=True, exist_ok=True)
            env_target.write_text("MAL_UPDATER_HEALTH_STRICT=1\n", encoding="utf-8")

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--health-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertEqual("MAL_UPDATER_HEALTH_STRICT=1\n", env_target.read_text(encoding="utf-8"))
            self.assertIn("health env already exists; leaving it untouched", result.stdout)
            self.assertIn("health_env_action=preserved", result.stdout)

    def test_install_reports_updated_and_unchanged_units_separately(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_dir = temp_root / "systemd" / "user"
            env_target = temp_root / ".MAL-Updater" / "config" / "mal-updater-health-check.env"
            target_dir.mkdir(parents=True, exist_ok=True)

            unchanged_unit = self.source_dir / "mal-updater-exact-approved-sync.service"
            updated_unit = self.source_dir / "mal-updater-exact-approved-sync.timer"
            rendered_unchanged = unchanged_unit.read_text(encoding="utf-8").replace("__MAL_UPDATER_REPO_ROOT__", str(self.repo_root)).replace("__MAL_UPDATER_HEALTH_ENV_FILE__", str(env_target))
            (target_dir / unchanged_unit.name).write_text(rendered_unchanged, encoding="utf-8")
            (target_dir / updated_unit.name).write_text("[Unit]\nDescription=stale copy\n", encoding="utf-8")

            result = self._run_script(
                "--target-dir",
                str(target_dir),
                "--health-env-target",
                str(env_target),
                "--no-enable",
                "--no-daemon-reload",
            )

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("updated_units=mal-updater-exact-approved-sync.timer", result.stdout)
            self.assertIn("unchanged_units=mal-updater-exact-approved-sync.service", result.stdout)
            self.assertIn("installed_units=mal-updater-health-check.service, mal-updater-health-check.timer", result.stdout)
            self.assertEqual(updated_unit.read_text(encoding="utf-8"), (target_dir / updated_unit.name).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
