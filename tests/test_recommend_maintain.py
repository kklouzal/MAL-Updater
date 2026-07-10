from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import AppConfig, ensure_directories
from mal_updater.service_runtime import _provider_fetch_command, _task_specs, maintenance_cycle_plan, run_maintenance_cycle


class RecommendMaintainTests(unittest.TestCase):
    def _config(self, root: Path) -> AppConfig:
        runtime_root = root / ".MAL-Updater"
        return AppConfig(
            project_root=root,
            workspace_root=root,
            runtime_root=runtime_root,
            settings_path=runtime_root / "config" / "settings.toml",
            config_dir=runtime_root / "config",
            secrets_dir=runtime_root / "secrets",
            data_dir=runtime_root / "data",
            state_dir=runtime_root / "state",
            cache_dir=runtime_root / "cache",
            db_path=runtime_root / "data" / "mal_updater.sqlite3",
        )

    def test_plan_orders_provider_mapping_metadata_snapshot_health_and_chunks_crunchyroll(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            with patch("mal_updater.service_runtime._available_source_providers", return_value=["crunchyroll"]):
                plan = maintenance_cycle_plan(
                    config,
                    metadata_limit=7,
                    discovery_target_limit=8,
                    recommendation_limit=9,
                    mapping_limit=10,
                    provider_max_history_pages=2,
                    provider_max_watchlist_pages=3,
                )

        self.assertEqual(
            [step["label"] for step in plan],
            [
                "maintain_provider_refresh_crunchyroll",
                "maintain_safe_mapping_review",
                "maintain_recommend_metadata",
                "maintain_recommend_snapshot",
                "maintain_health",
            ],
        )
        provider_args = plan[0]["args"]
        self.assertIn("--ingest", provider_args)
        self.assertNotIn("--full-refresh", provider_args)
        self.assertEqual(provider_args[provider_args.index("--max-history-pages") + 1], "2")
        self.assertEqual(provider_args[provider_args.index("--max-watchlist-pages") + 1], "3")
        self.assertEqual(plan[1]["args"][-3:], ["--limit", "10", "--exact-approved-only"])
        self.assertIn("--persist-snapshot", plan[3]["args"])

    def test_provider_fetch_command_caps_crunchyroll_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            args = _provider_fetch_command(config, "crunchyroll")

        self.assertEqual(args[args.index("--max-history-pages") + 1], "10")
        self.assertEqual(args[args.index("--max-watchlist-pages") + 1], "2")

    def test_provider_fetch_command_allows_disabling_crunchyroll_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.service.crunchyroll_provider_max_history_pages = 0
            config.service.crunchyroll_provider_max_watchlist_pages = 0
            args = _provider_fetch_command(config, "crunchyroll")

        self.assertNotIn("--max-history-pages", args)
        self.assertNotIn("--max-watchlist-pages", args)

    def test_provider_fetch_command_uses_configured_crunchyroll_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.service.crunchyroll_provider_max_history_pages = 11
            config.service.crunchyroll_provider_max_watchlist_pages = 4
            args = _provider_fetch_command(config, "crunchyroll")

        self.assertEqual(args[args.index("--max-history-pages") + 1], "11")
        self.assertEqual(args[args.index("--max-watchlist-pages") + 1], "4")

    def test_maintenance_plan_uses_service_caps_when_not_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.service.crunchyroll_provider_max_history_pages = 12
            config.service.crunchyroll_provider_max_watchlist_pages = 5
            with patch("mal_updater.service_runtime._available_source_providers", return_value=["crunchyroll"]):
                plan = maintenance_cycle_plan(config)

        provider_args = plan[0]["args"]
        self.assertEqual(provider_args[provider_args.index("--max-history-pages") + 1], "12")
        self.assertEqual(provider_args[provider_args.index("--max-watchlist-pages") + 1], "5")

    def test_maintenance_plan_explicit_zero_disables_service_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            with patch("mal_updater.service_runtime._available_source_providers", return_value=["crunchyroll"]):
                plan = maintenance_cycle_plan(config, provider_max_history_pages=0, provider_max_watchlist_pages=0)

        provider_args = plan[0]["args"]
        self.assertNotIn("--max-history-pages", provider_args)
        self.assertNotIn("--max-watchlist-pages", provider_args)

    def test_run_cycle_keeps_going_and_summarizes_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            ensure_directories(config)
            calls: list[str] = []

            def fake_run(_config: AppConfig, _args: list[str], *, label: str) -> dict[str, object]:
                calls.append(label)
                if label == "maintain_recommend_metadata":
                    return {"status": "error", "label": label, "returncode": 4, "stderr": "boom", "stdout": ""}
                return {"status": "ok", "label": label, "returncode": 0, "stderr": "", "stdout": "{}"}

            with patch("mal_updater.service_runtime._available_source_providers", return_value=[]), patch(
                "mal_updater.service_runtime._run_subprocess", side_effect=fake_run
            ):
                result = run_maintenance_cycle(config, dry_run=False)

        self.assertEqual(
            calls,
            [
                "maintain_safe_mapping_review",
                "maintain_recommend_metadata",
                "maintain_recommend_snapshot",
                "maintain_health",
            ],
        )
        self.assertEqual(result["status"], "partial_error")
        self.assertEqual(len(result["failures"]), 1)
        self.assertEqual(result["failures"][0]["label"], "maintain_recommend_metadata")

    def test_daemon_task_specs_keep_snapshot_materialization_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.service.recommendation_metadata_refresh_every_seconds = 60
            with patch("mal_updater.service_runtime._available_source_providers", return_value=[]):
                names = [spec.name for spec in _task_specs(config)]

        self.assertIn("recommend_metadata_refresh", names)
        self.assertNotIn("recommendation_snapshot", names)

    def test_dry_run_records_plan_without_subprocess(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            ensure_directories(config)
            with patch("mal_updater.service_runtime._available_source_providers", return_value=[]), patch(
                "mal_updater.service_runtime._run_subprocess"
            ) as run_subprocess:
                result = run_maintenance_cycle(config, dry_run=True)
                self.assertTrue(config.service_state_path.exists())

        self.assertEqual(result["status"], "dry_run")
        self.assertFalse(run_subprocess.called)


if __name__ == "__main__":
    unittest.main()
