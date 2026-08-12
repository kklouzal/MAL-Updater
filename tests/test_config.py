from __future__ import annotations

import os
import sqlite3
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

from mal_updater.config import ConfigError, _load_toml_parser, _read_toml_file, ensure_directories, load_config, load_mal_secrets, load_openclaw_recommendations_hook_token


class ConfigLoadingTests(unittest.TestCase):
    def _run_status_with_settings(
        self,
        root: Path,
        runtime_root: Path,
        settings_path: Path,
        *,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
            "MAL_UPDATER_RUNTIME_ROOT": str(runtime_root),
            "MAL_UPDATER_SETTINGS_PATH": str(settings_path),
        }
        env.update(extra_env or {})
        return subprocess.run(
            [os.sys.executable, "-m", "mal_updater.cli", "--project-root", str(root), "status"],
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )

    def test_zero_sync_apply_execute_limit_is_preserved_as_scheduler_disable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "config" / "settings.toml").write_text(
                "[service.task_execute_limits]\nsync_apply = 0\n",
                encoding="utf-8",
            )
            config = load_config(root)
        self.assertEqual(0, config.service.execute_limit_for("sync_apply"))

    def test_defaults_resolve_under_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)

            config = load_config(root)
            secrets = load_mal_secrets(config)

            self.assertEqual(config.settings_path, (root / ".MAL-Updater" / "config" / "settings.toml").resolve())
            self.assertEqual(config.config_dir, (root / ".MAL-Updater" / "config").resolve())
            self.assertEqual(config.secrets_dir, (root / ".MAL-Updater" / "secrets").resolve())
            self.assertEqual(config.data_dir, (root / ".MAL-Updater" / "data").resolve())
            self.assertEqual(config.state_dir, (root / ".MAL-Updater" / "state").resolve())
            self.assertEqual(config.cache_dir, (root / ".MAL-Updater" / "cache").resolve())
            self.assertEqual(config.db_path, (root / ".MAL-Updater" / "data" / "mal_updater.sqlite3").resolve())
            self.assertEqual(config.mal.bind_host, "127.0.0.1")
            self.assertFalse(config.mal.non_loopback_callback_ack)
            self.assertEqual(config.mal.redirect_uri, "http://127.0.0.1:8765/callback")
            self.assertEqual(secrets.client_id_path, (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").resolve())
            self.assertFalse(config.openclaw.recommendations_webhook_enabled)
            self.assertEqual("", config.openclaw.recommendations_webhook_url)
            self.assertEqual(20.0, config.openclaw.recommendations_webhook_timeout_seconds)
            self.assertEqual("fresh", config.openclaw.recommendations_webhook_delivery_mode)
            self.assertEqual(5, config.openclaw.recommendations_webhook_section_limits["continue_next"])
            self.assertEqual(2, config.openclaw.recommendations_webhook_section_limits["resume_backlog"])
            self.assertEqual(60 * 60, config.service.sync_every_seconds)
            self.assertEqual(7 * 24 * 60 * 60, config.service.full_refresh_every_seconds)
            self.assertEqual(60 * 60, config.service.health_every_seconds)
            self.assertEqual(60 * 60, config.service.mal_refresh_every_seconds)
            self.assertEqual(8 * 60 * 60, config.service.mal_list_refresh_every_seconds)
            self.assertEqual(12 * 60 * 60, config.service.recommendation_metadata_refresh_every_seconds)
            self.assertEqual(60 * 60, config.service.recommendation_full_harvest_every_seconds)
            self.assertEqual(45, config.service.recommendation_full_harvest_stale_after_days)
            self.assertEqual(60 * 60, config.service.provider_eligibility_refresh_every_seconds)
            self.assertEqual(60 * 60, config.service.recommend_maintain_every_seconds)
            self.assertEqual(14, config.service.recommendation_snapshot_retention_days)
            self.assertEqual(30, config.service.recommendation_snapshot_min_runs_per_kind)
            self.assertEqual(10_000, config.service.recommendation_snapshot_prune_batch_size)
            self.assertEqual(7 * 24 * 60 * 60, config.service.db_compaction_every_seconds)
            self.assertEqual(30 * 24 * 60 * 60, config.service.db_compaction_min_interval_seconds)
            self.assertEqual(128 * 1024 * 1024, config.service.db_compaction_min_freelist_bytes)
            self.assertEqual(0.10, config.service.db_compaction_min_freelist_ratio)
            self.assertEqual(64 * 1024 * 1024, config.service.db_compaction_free_space_margin_bytes)
            self.assertEqual(24 * 60 * 60, config.service.health_history_retention_every_seconds)
            self.assertEqual(90, config.service.health_history_retention_days)
            self.assertEqual(168, config.service.health_history_min_count)
            self.assertEqual(100, config.service.health_history_prune_batch_size)
            self.assertEqual(16 * 1024 * 1024, config.service.service_log_max_bytes)
            self.assertEqual(5, config.service.service_log_retained_generations)
            self.assertEqual(7 * 24 * 60 * 60, config.service.runtime_retention_audit_every_seconds)
            self.assertEqual(30, config.service.startup_grace_seconds)
            self.assertEqual(30 * 60, config.service.lease_stale_after_seconds)
            self.assertEqual(10, config.service.crunchyroll_provider_max_history_pages)
            self.assertEqual(2, config.service.crunchyroll_provider_max_watchlist_pages)
            self.assertEqual(900, config.service.task_timeout_seconds)
            self.assertEqual(72, config.service.provider_hourly_limits["hidive"])
            self.assertEqual(6, config.service.task_hourly_limits["mal_list_refresh"])
            self.assertEqual(48, config.service.task_hourly_limits["sync_apply"])
            self.assertEqual(16, config.service.task_hourly_limits["recommend_full_harvest"])
            self.assertEqual(72, config.service.task_hourly_limits["recommend_provider_eligibility_crunchyroll"])
            self.assertEqual(1, config.service.task_projected_request_counts["mal_refresh"])
            self.assertEqual(3, config.service.task_projected_request_counts["mal_list_refresh"])
            self.assertEqual(8, config.service.task_projected_request_counts["sync_apply"])
            self.assertEqual(28, config.service.task_projected_request_counts["recommend_provider_eligibility_crunchyroll"])
            self.assertEqual(4, config.service.task_execute_limits["recommend_provider_eligibility_candidates"])
            self.assertEqual(1, config.service.task_execute_limits["recommend_provider_eligibility_queries_per_candidate"])
            self.assertEqual(8, config.service.task_execute_limits["sync_apply"])
            self.assertEqual(2, config.service.task_execute_limits["recommend_full_harvest"])
            self.assertEqual(3, config.service.task_execute_limits["recommend_full_harvest_pages"])
            self.assertEqual(4, config.service.task_projected_request_counts_by_mode["sync_fetch_crunchyroll"]["incremental"])
            self.assertEqual(55, config.service.task_projected_request_counts_by_mode["sync_fetch_crunchyroll"]["full_refresh"])
            self.assertEqual(4, config.service.task_projected_request_counts_by_mode["sync_fetch_hidive"]["incremental"])
            self.assertEqual(71, config.service.task_projected_request_counts_by_mode["sync_fetch_hidive"]["full_refresh"])
            self.assertEqual(7, config.service.projected_request_history_window_for("unknown_task", provider="crunchyroll"))
            self.assertEqual(9, config.service.projected_request_history_window_for("unknown_task", provider="hidive"))
            self.assertEqual(3, config.service.projected_request_history_window_for("mal_refresh"))
            self.assertEqual(3, config.service.projected_request_history_window_for("sync_apply"))
            self.assertEqual(0.9, config.service.task_projected_request_percentiles["sync_apply"])
            self.assertEqual(0.9, config.service.projected_request_percentile_for("sync_apply"))
            self.assertEqual(0.9, config.service.projected_request_percentile_for("unknown_task", provider="crunchyroll"))
            self.assertEqual(0.9, config.service.projected_request_percentile_for("unknown_task", provider="hidive"))
            self.assertEqual(900, config.service.backoff_floor_seconds_for("crunchyroll", level="warn"))
            self.assertEqual(900, config.service.backoff_floor_seconds_for("mal", level="warn", task_name="sync_apply"))
            self.assertEqual(1200, config.service.backoff_floor_seconds_for("hidive", level="critical"))
            self.assertEqual(1800, config.service.backoff_floor_seconds_for("mal", level="critical", task_name="sync_apply"))
            self.assertEqual(7200, config.service.auth_failure_backoff_floor_seconds_for("crunchyroll"))
            self.assertEqual(2400, config.service.auth_failure_backoff_floor_seconds_for("mal", task_name="sync_apply"))
            self.assertEqual("task", config.service.budget_scope_for("mal", task_name="sync_apply"))

    def test_runtime_dir_and_config_aliases_resolve_hermetic_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "project"
            root.mkdir()
            runtime_root = Path(td) / "ci-runtime"
            settings_path = Path(td) / "ci-config" / "settings.toml"
            settings_path.parent.mkdir(parents=True)
            settings_path.write_text("[mal]\nredirect_port = 8766\n", encoding="utf-8")

            env = {
                "PATH": os.environ.get("PATH", ""),
                "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
                "MAL_UPDATER_RUNTIME_ROOT": "",
                "MAL_UPDATER_SETTINGS_PATH": "",
                "MAL_UPDATER_RUNTIME_DIR": str(runtime_root),
                "MAL_UPDATER_CONFIG": str(settings_path),
            }
            result = subprocess.run(
                [os.sys.executable, "-m", "mal_updater.cli", "--project-root", str(root), "status"],
                text=True,
                capture_output=True,
                check=False,
                env=env,
            )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn(f"runtime_root={runtime_root.resolve()}", result.stdout)
        self.assertIn(f"settings_path={settings_path.resolve()}", result.stdout)
        self.assertIn(f"config_dir={(runtime_root / 'config').resolve()}", result.stdout)
        self.assertIn("mal.redirect_uri=http://127.0.0.1:8766/callback", result.stdout)

    def test_env_overrides_service_crunchyroll_provider_caps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            with patch.dict(
                os.environ,
                {
                    "MAL_UPDATER_SERVICE_CRUNCHYROLL_PROVIDER_MAX_HISTORY_PAGES": "8",
                    "MAL_UPDATER_SERVICE_CRUNCHYROLL_PROVIDER_MAX_WATCHLIST_PAGES": "2",
                    "MAL_UPDATER_SERVICE_TASK_TIMEOUT_SECONDS": "123",
                },
            ):
                config = load_config(root)

        self.assertEqual(8, config.service.crunchyroll_provider_max_history_pages)
        self.assertEqual(2, config.service.crunchyroll_provider_max_watchlist_pages)
        self.assertEqual(123, config.service.task_timeout_seconds)

    def test_settings_file_overrides_paths_and_secret_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    completion_threshold = 0.95
                    contract_version = "2.0"

                    [paths]
                    config_dir = "./"
                    secrets_dir = "../private"
                    data_dir = "../var/data"
                    state_dir = "../var/state"
                    cache_dir = "../var/cache"
                    db_path = "../var/custom.sqlite3"

                    [mal]
                    bind_host = "127.0.0.1"
                    redirect_host = "127.0.0.50"
                    redirect_port = 9999

                    [openclaw]
                    recommendations_webhook_enabled = true
                    recommendations_webhook_url = "http://127.0.0.1:18789/hooks/agent"
                    recommendations_webhook_timeout_seconds = 9.5
                    recommendations_webhook_channel = "discord"
                    recommendations_webhook_to = "channel:000000000000000000"
                    recommendations_webhook_delivery_mode = "all"

                    [openclaw.recommendations_webhook_section_limits]
                    continue_next = 7
                    resume_backlog = 4

                    [secret_files]
                    mal_client_id = "ids/client-id.txt"
                    openclaw_hook_token = "tokens/openclaw-hook-token.txt"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            token_file = root / ".MAL-Updater" / "private" / "tokens" / "openclaw-hook-token.txt"
            token_file.parent.mkdir(parents=True, exist_ok=True)
            token_file.write_text("secret-token\n", encoding="utf-8")

            config = load_config(root)
            secrets = load_mal_secrets(config)
            hook_token, hook_token_path = load_openclaw_recommendations_hook_token(config)

            self.assertEqual(config.completion_threshold, 0.95)
            self.assertEqual(config.contract_version, "2.0")
            self.assertEqual(config.config_dir, (root / ".MAL-Updater" / "config").resolve())
            self.assertEqual(config.secrets_dir, (root / ".MAL-Updater" / "private").resolve())
            self.assertEqual(config.data_dir, (root / ".MAL-Updater" / "var" / "data").resolve())
            self.assertEqual(config.state_dir, (root / ".MAL-Updater" / "var" / "state").resolve())
            self.assertEqual(config.cache_dir, (root / ".MAL-Updater" / "var" / "cache").resolve())
            self.assertEqual(config.db_path, (root / ".MAL-Updater" / "var" / "custom.sqlite3").resolve())
            self.assertEqual(config.mal.bind_host, "127.0.0.1")
            self.assertEqual(config.mal.redirect_uri, "http://127.0.0.50:9999/callback")
            self.assertTrue(config.openclaw.recommendations_webhook_enabled)
            self.assertEqual("http://127.0.0.1:18789/hooks/agent", config.openclaw.recommendations_webhook_url)
            self.assertEqual(9.5, config.openclaw.recommendations_webhook_timeout_seconds)
            self.assertEqual("discord", config.openclaw.recommendations_webhook_channel)
            self.assertEqual("channel:000000000000000000", config.openclaw.recommendations_webhook_to)
            self.assertEqual("all", config.openclaw.recommendations_webhook_delivery_mode)
            self.assertEqual(7, config.openclaw.recommendations_webhook_section_limits["continue_next"])
            self.assertEqual(4, config.openclaw.recommendations_webhook_section_limits["resume_backlog"])
            self.assertEqual(secrets.client_id_path, (root / ".MAL-Updater" / "private" / "ids" / "client-id.txt").resolve())
            self.assertEqual(hook_token, "secret-token")
            self.assertEqual(hook_token_path, token_file.resolve())

    def test_settings_file_loads_provider_budget_tables(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    [service]
                    source_provider_hourly_limit = 90
                    source_provider_warn_backoff_floor_seconds = 180
                    source_provider_critical_backoff_floor_seconds = 600
                    source_provider_auth_failure_backoff_floor_seconds = 2400

                    [service.provider_hourly_limits]
                    hidive = 72

                    [service.task_hourly_limits]
                    sync_apply = 24

                    [service.task_execute_limits]
                    sync_apply = 6

                    [service.task_projected_request_counts]
                    mal_refresh = 2
                    sync_apply = 8
                    sync_fetch_hidive = 14

                    [service.task_projected_request_counts_by_mode.sync_fetch_hidive]
                    full_refresh = 60
                    incremental = 5

                    [service.provider_projected_request_history_windows]
                    crunchyroll = 7
                    hidive = 11

                    [service.task_projected_request_history_windows]
                    mal_refresh = 4
                    sync_apply = 3
                    sync_fetch_hidive = 9

                    [service.provider_projected_request_percentiles]
                    crunchyroll = 0.85
                    hidive = 0.95

                    [service.task_projected_request_percentiles]
                    sync_apply = 0.75
                    sync_fetch_hidive = 0.9

                    [service.provider_warn_backoff_floor_seconds]
                    crunchyroll = 900
                    hidive = 300

                    [service.provider_critical_backoff_floor_seconds]
                    crunchyroll = 1800
                    hidive = 1200

                    [service.task_warn_backoff_floor_seconds]
                    sync_apply = 450

                    [service.task_critical_backoff_floor_seconds]
                    sync_apply = 1500

                    [service.provider_auth_failure_backoff_floor_seconds]
                    hidive = 3600

                    [service.task_auth_failure_backoff_floor_seconds]
                    sync_apply = 2400
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            config = load_config(root)

            self.assertEqual(90, config.service.source_provider_hourly_limit)
            self.assertEqual(180, config.service.source_provider_warn_backoff_floor_seconds)
            self.assertEqual(600, config.service.source_provider_critical_backoff_floor_seconds)
            self.assertEqual(2400, config.service.source_provider_auth_failure_backoff_floor_seconds)
            self.assertEqual(72, config.service.provider_hourly_limits["hidive"])
            self.assertEqual(24, config.service.task_hourly_limits["sync_apply"])
            self.assertEqual(2, config.service.task_projected_request_counts["mal_refresh"])
            self.assertEqual(8, config.service.task_projected_request_counts["sync_apply"])
            self.assertEqual(6, config.service.task_execute_limits["sync_apply"])
            self.assertEqual(14, config.service.task_projected_request_counts["sync_fetch_hidive"])
            self.assertEqual(60, config.service.task_projected_request_counts_by_mode["sync_fetch_hidive"]["full_refresh"])
            self.assertEqual(5, config.service.task_projected_request_counts_by_mode["sync_fetch_hidive"]["incremental"])
            self.assertEqual(7, config.service.provider_projected_request_history_windows["crunchyroll"])
            self.assertEqual(11, config.service.provider_projected_request_history_windows["hidive"])
            self.assertEqual(4, config.service.task_projected_request_history_windows["mal_refresh"])
            self.assertEqual(3, config.service.task_projected_request_history_windows["sync_apply"])
            self.assertEqual(9, config.service.task_projected_request_history_windows["sync_fetch_hidive"])
            self.assertEqual(0.85, config.service.provider_projected_request_percentiles["crunchyroll"])
            self.assertEqual(0.95, config.service.provider_projected_request_percentiles["hidive"])
            self.assertEqual(0.75, config.service.task_projected_request_percentiles["sync_apply"])
            self.assertEqual(0.9, config.service.task_projected_request_percentiles["sync_fetch_hidive"])
            self.assertEqual(900, config.service.provider_warn_backoff_floor_seconds["crunchyroll"])
            self.assertEqual(300, config.service.provider_warn_backoff_floor_seconds["hidive"])
            self.assertEqual(450, config.service.task_warn_backoff_floor_seconds["sync_apply"])
            self.assertEqual(1800, config.service.provider_critical_backoff_floor_seconds["crunchyroll"])
            self.assertEqual(1200, config.service.provider_critical_backoff_floor_seconds["hidive"])
            self.assertEqual(1500, config.service.task_critical_backoff_floor_seconds["sync_apply"])
            self.assertEqual(3600, config.service.provider_auth_failure_backoff_floor_seconds["hidive"])
            self.assertEqual(2400, config.service.task_auth_failure_backoff_floor_seconds["sync_apply"])
            self.assertEqual(90, config.service.hourly_limit_for("new-provider"))
            self.assertEqual(72, config.service.hourly_limit_for("hidive"))
            self.assertEqual(24, config.service.hourly_limit_for("mal", task_name="sync_apply"))
            self.assertEqual(4, config.service.projected_request_history_window_for("mal_refresh"))
            self.assertEqual(3, config.service.projected_request_history_window_for("sync_apply"))
            self.assertEqual(0.75, config.service.projected_request_percentile_for("sync_apply"))
            self.assertEqual(11, config.service.projected_request_history_window_for("unknown_task", provider="hidive"))
            self.assertEqual(0.95, config.service.projected_request_percentile_for("unknown_task", provider="hidive"))
            self.assertEqual(5, config.service.projected_request_history_window_for("unknown_task"))
            self.assertIsNone(config.service.projected_request_percentile_for("unknown_task"))
            self.assertEqual(180, config.service.backoff_floor_seconds_for("new-provider", level="warn"))
            self.assertEqual(600, config.service.backoff_floor_seconds_for("new-provider", level="critical"))
            self.assertEqual(450, config.service.backoff_floor_seconds_for("mal", level="warn", task_name="sync_apply"))
            self.assertEqual(1500, config.service.backoff_floor_seconds_for("mal", level="critical", task_name="sync_apply"))
            self.assertEqual(2400, config.service.auth_failure_backoff_floor_seconds_for("new-provider"))
            self.assertEqual(2400, config.service.auth_failure_backoff_floor_seconds_for("mal", task_name="sync_apply"))
            self.assertEqual("task", config.service.budget_scope_for("mal", task_name="sync_apply"))
            self.assertEqual("provider", config.service.budget_scope_for("hidive", task_name="sync_fetch_hidive"))

    def test_non_loopback_oauth_bind_requires_explicit_acknowledgement(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_dir = root / ".MAL-Updater" / "config"
            config_dir.mkdir(parents=True)
            settings_path = config_dir / "settings.toml"
            settings_path.write_text('[mal]\nbind_host = "0.0.0.0"\n', encoding="utf-8")

            with self.assertRaises(ConfigError) as raised:
                load_config(root)

            self.assertIn("non-loopback MAL OAuth callback", raised.exception.safe_message)
            settings_path.write_text('[mal]\nbind_host = "0.0.0.0"\nnon_loopback_callback_ack = true\n', encoding="utf-8")
            config = load_config(root)
            self.assertEqual("0.0.0.0", config.mal.bind_host)
            self.assertTrue(config.mal.non_loopback_callback_ack)

    def test_ensure_directories_tightens_secrets_dir_to_0700(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)

            ensure_directories(config)

            mode = stat.S_IMODE(config.secrets_dir.stat().st_mode)
            self.assertEqual(0o700, mode)

    def test_cli_invalid_env_config_exits_two_without_traceback_or_value(self) -> None:
        cases = (
            ("MAL_UPDATER_MAL_REDIRECT_PORT", "SENTINEL-invalid-config-value"),
            ("MAL_UPDATER_MAL_REDIRECT_PORT", "inf"),
            ("MAL_UPDATER_REQUEST_TIMEOUT_SECONDS", "inf"),
        )
        for env_name, raw_value in cases:
            with self.subTest(env_name=env_name, raw_value=raw_value), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                runtime_root = root / "runtime"
                settings_path = runtime_root / "config" / "settings.toml"
                settings_path.parent.mkdir(parents=True)
                settings_path.write_text("", encoding="utf-8")
                result = self._run_status_with_settings(root, runtime_root, settings_path, extra_env={env_name: raw_value})

                self.assertEqual(2, result.returncode)
                self.assertIn("configuration error:", result.stderr)
                self.assertNotIn("Traceback", result.stderr + result.stdout)
                self.assertNotIn(raw_value, result.stderr + result.stdout)

    def test_cli_invalid_toml_numeric_config_exits_two_without_traceback_or_value(self) -> None:
        cases = (
            ("[mal]\nredirect_port = inf\n", "inf"),
            ('[mal]\nredirect_port = "SENTINEL-invalid-config-value"\n', "SENTINEL-invalid-config-value"),
            ("request_timeout_seconds = inf\n", "inf"),
        )
        for settings_text, raw_value in cases:
            with self.subTest(settings_text=settings_text), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                runtime_root = root / "runtime"
                settings_path = runtime_root / "config" / "settings.toml"
                settings_path.parent.mkdir(parents=True)
                settings_path.write_text(settings_text, encoding="utf-8")

                result = self._run_status_with_settings(root, runtime_root, settings_path)

                self.assertEqual(2, result.returncode)
                self.assertIn("configuration error:", result.stderr)
                self.assertNotIn("Traceback", result.stderr + result.stdout)
                self.assertNotIn(raw_value, result.stderr + result.stdout)

    def test_cli_invalid_toml_table_numeric_config_exits_two_without_traceback_or_value(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runtime_root = root / "runtime"
            settings_path = runtime_root / "config" / "settings.toml"
            settings_path.parent.mkdir(parents=True)
            settings_path.write_text(
                "[service.task_hourly_limits]\nsync_apply = inf\n",
                encoding="utf-8",
            )

            result = self._run_status_with_settings(root, runtime_root, settings_path)

            self.assertEqual(2, result.returncode)
            self.assertIn("configuration error:", result.stderr)
            self.assertNotIn("Traceback", result.stderr + result.stdout)
            self.assertNotIn("inf", result.stderr + result.stdout)

    def test_cli_invalid_toml_config_exits_two_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runtime_root = root / "runtime"
            settings_path = runtime_root / "config" / "settings.toml"
            settings_path.parent.mkdir(parents=True)
            settings_path.write_text("[mal\n", encoding="utf-8")
            result = self._run_status_with_settings(root, runtime_root, settings_path)

            self.assertEqual(2, result.returncode)
            self.assertIn("configuration error:", result.stderr)
            self.assertIn("Invalid TOML", result.stderr)
            self.assertNotIn("Traceback", result.stderr + result.stdout)

    def test_read_toml_file_sanitizes_parser_error_details(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            settings_path = Path(td) / "settings.toml"
            settings_path.write_text('[mal]\nclient_secret = "SUPERSECRET"\nclient_secret = "OTHER"\n', encoding="utf-8")

            with self.assertRaises(ConfigError) as raised:
                _read_toml_file(settings_path)

            message = raised.exception.safe_message
            self.assertIn("Invalid TOML in settings.toml", message)
            self.assertIn("TOMLDecodeError", message)
            self.assertIn("line", message)
            self.assertIn("column", message)
            self.assertNotIn("SUPERSECRET", message)
            self.assertNotIn("OTHER", message)

    def test_settings_example_standard_toml_features_parse_and_preserve_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_dir = root / ".MAL-Updater" / "config"
            config_dir.mkdir(parents=True)
            example_text = (Path(__file__).resolve().parents[1] / "references" / "settings.toml.example").read_text(encoding="utf-8")
            (config_dir / "settings.toml").write_text(example_text, encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "MAL_UPDATER_MAL_REDIRECT_PORT": "9876",
                    "MAL_UPDATER_OPENCLAW_RECOMMENDATIONS_WEBHOOK_DELIVERY_MODE": "all",
                    "MAL_UPDATER_SERVICE_SYNC_EVERY_SECONDS": "1800",
                },
                clear=False,
            ):
                config = load_config(root)

            parsed = _read_toml_file(config.settings_path)

            self.assertEqual("fresh", parsed["openclaw"]["recommendations_webhook_delivery_mode"])
            self.assertEqual(604800, parsed["service"]["full_refresh_every_seconds"])
            self.assertEqual(3600, parsed["service"]["recommendation_full_harvest_every_seconds"])
            self.assertEqual(45, parsed["service"]["recommendation_full_harvest_stale_after_days"])
            self.assertEqual(180, parsed["service"]["crunchyroll_hourly_limit"])
            self.assertEqual(16, parsed["service"]["task_hourly_limits"]["recommend_full_harvest"])
            self.assertEqual(2, parsed["service"]["task_execute_limits"]["recommend_full_harvest"])
            self.assertEqual(3, parsed["service"]["task_execute_limits"]["recommend_full_harvest_pages"])
            self.assertEqual("all", config.openclaw.recommendations_webhook_delivery_mode)
            self.assertEqual(9876, config.mal.redirect_port)
            self.assertEqual(1800, config.service.sync_every_seconds)
            self.assertEqual(604800, config.service.full_refresh_every_seconds)
            self.assertEqual(20, config.service.task_execute_limits["push_recommendations_webhook"])
            self.assertEqual(55, config.service.task_projected_request_counts_by_mode["sync_fetch_crunchyroll"]["full_refresh"])

    def test_tomli_fallback_import_contract_uses_standard_parser(self) -> None:
        calls: list[str] = []
        tomli_parser = object()

        def fake_import(name: str) -> object:
            calls.append(name)
            if name == "tomllib":
                raise ModuleNotFoundError("No module named 'tomllib'", name="tomllib")
            if name == "tomli":
                return tomli_parser
            raise AssertionError(f"unexpected import: {name}")

        self.assertIs(_load_toml_parser(fake_import), tomli_parser)
        self.assertEqual(["tomllib", "tomli"], calls)


if __name__ == "__main__":
    unittest.main()


def test_pytest_blocks_canonical_runtime_before_sqlite_open() -> None:
    canonical_db = Path(__file__).resolve().parents[1] / ".MAL-Updater" / "data" / "mal_updater.sqlite3"
    with pytest.raises(AssertionError, match="blocked canonical MAL-Updater runtime"):
        sqlite3.connect(canonical_db)
