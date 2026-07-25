from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.request_tracking import begin_api_request_context, end_api_request_context, record_api_request_event
from mal_updater.service_runtime import (
    TaskSpec,
    _budget_gate,
    _provider_eligibility_command,
    _provider_fetch_command,
    _task_specs,
    effective_niceness_policy,
    run_pending_tasks,
)


class FinalNicenessIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        (self.root / ".MAL-Updater" / "config").mkdir(parents=True)
        secrets = self.root / ".MAL-Updater" / "secrets"
        secrets.mkdir(parents=True)
        (secrets / "mal_access_token.txt").write_text("mal-token\n", encoding="utf-8")
        (secrets / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (secrets / "crunchyroll_password.txt").write_text("secret\n", encoding="utf-8")
        self.config = load_config(self.root)
        ensure_directories(self.config)

    def test_effective_specs_separate_network_ownership_and_stagger_slow_lanes(self) -> None:
        specs = {spec.name: spec for spec in _task_specs(self.config)}

        self.assertEqual(3600, specs["sync_fetch_crunchyroll"].every_seconds)
        self.assertEqual(28800, specs["mal_list_refresh"].every_seconds)
        self.assertEqual(900, specs["mal_list_refresh"].initial_delay_seconds)
        self.assertEqual(43200, specs["recommend_metadata_refresh"].every_seconds)
        eligibility = specs["recommend_provider_eligibility_crunchyroll"]
        self.assertEqual(3600, eligibility.every_seconds)
        self.assertEqual(2700, eligibility.initial_delay_seconds)
        self.assertEqual("crunchyroll", eligibility.budget_provider)
        self.assertEqual(3600, specs["recommend_maintain"].every_seconds)
        self.assertIsNone(specs["recommend_maintain"].budget_provider)

        command = _provider_eligibility_command(self.config, "crunchyroll")
        self.assertEqual("crunchyroll", command[command.index("--provider") + 1])
        self.assertEqual("1", command[command.index("--limit") + 1])
        self.assertEqual("5", command[command.index("--search-limit") + 1])
        self.assertEqual("1", command[command.index("--queries-per-candidate") + 1])

    def test_cold_policy_is_weekly_page_bounded_for_crunchyroll_and_manual_for_hidive(self) -> None:
        policy = effective_niceness_policy(self.config)
        self.assertEqual(604800, policy["cadences"]["provider_cold_full_seconds"])
        self.assertEqual({"mal": 120, "crunchyroll": 180, "hidive": 72}, policy["provider_hourly_budgets"])
        eligibility = policy["task_policies"]["recommend_provider_eligibility_crunchyroll"]
        self.assertEqual(18, eligibility["task_hourly_limit"])
        self.assertEqual(7, eligibility["projected_requests"])
        self.assertEqual(43200, eligibility["auth_failure_backoff_floor_seconds"])
        self.assertEqual(10, policy["cold_refresh_bounds"]["crunchyroll_max_history_pages"])
        self.assertEqual(2, policy["cold_refresh_bounds"]["crunchyroll_max_watchlist_pages"])
        self.assertFalse(policy["cold_refresh_bounds"]["hidive_unattended_full_refresh"])

        cold = _provider_fetch_command(self.config, "crunchyroll", full_refresh=True)
        self.assertIn("--full-refresh", cold)
        self.assertEqual("10", cold[cold.index("--max-history-pages") + 1])
        self.assertEqual("2", cold[cold.index("--max-watchlist-pages") + 1])

    def test_provider_eligibility_lane_obeys_provider_global_budget_as_well_as_task_budget(self) -> None:
        spec = TaskSpec(
            "recommend_provider_eligibility_crunchyroll",
            86400,
            "crunchyroll",
        )
        # These are attributed to another lane: the eligibility task itself is empty,
        # but the provider-global warn headroom must still suppress it.
        token = begin_api_request_context(task="sync_fetch_crunchyroll", run_id="other-run")
        try:
            for index in range(140):
                record_api_request_event(
                    "crunchyroll",
                    "other-lane",
                    url=f"https://example.invalid/{index}",
                    method="GET",
                    outcome="ok",
                    status_code=200,
                    config=self.config,
                )
        finally:
            end_api_request_context(token)

        allowed, reason, usage = _budget_gate(self.config, spec, {})

        self.assertFalse(allowed)
        self.assertIn("crunchyroll_global_budget_warn", reason or "")
        self.assertEqual(0, usage["task_request_count"])
        self.assertEqual(140, usage["global_request_count"])
        self.assertEqual("task", usage["budget_scope"])

    def test_three_day_scheduler_timeline_has_stable_counts_and_projected_bounds(self) -> None:
        calls: list[tuple[str, list[str]]] = []

        def fake_run(_config, args, *, label):
            calls.append((label, list(args)))
            stdout = ""
            if label == "recommend_provider_eligibility_crunchyroll":
                stdout = json.dumps(
                    {
                        "candidates_considered": 1,
                        "cache_hits": 0,
                        "cache_misses": 0,
                        "provider_searches": 0,
                        "provider_detail_probes": 0,
                        "eligibility_fresh_skips": 1,
                    }
                )
            return {"status": "ok", "label": label, "returncode": 0, "stdout": stdout, "stderr": ""}

        start = 2_000_000_000.0
        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}) as refresh_tokens, patch(
            "mal_updater.service_runtime._run_subprocess", side_effect=fake_run
        ):
            for quarter_hour in range(0, 72 * 4 + 1):
                now = start + quarter_hour * 15 * 60
                with patch("mal_updater.service_runtime.time.time", return_value=now):
                    result = run_pending_tasks(self.config)
                    self.assertEqual("ok", result["status"])

        counts: dict[str, int] = {}
        for label, _args in calls:
            counts[label] = counts.get(label, 0) + 1

        self.assertEqual(73, refresh_tokens.call_count)
        self.assertEqual(73, counts["sync_fetch_crunchyroll"])
        self.assertEqual(73, counts["sync_apply"])
        self.assertEqual(9, counts["mal_list_refresh"])
        self.assertEqual(7, counts["recommend_metadata_refresh"])
        self.assertEqual(72, counts["recommend_provider_eligibility_crunchyroll"])
        self.assertEqual(72, counts["recommend_maintain"])
        self.assertEqual(73, counts["health"])

        fetch_commands = [args for label, args in calls if label == "sync_fetch_crunchyroll"]
        self.assertTrue(all("--full-refresh" not in args for args in fetch_commands))
        apply_commands = [args for label, args in calls if label == "sync_apply"]
        self.assertTrue(all("--exact-approved-only" in args and "--execute" in args for args in apply_commands))
        self.assertTrue(all(args[args.index("--limit") + 1] == "8" for args in apply_commands))

        # Conservative projected request ceilings for this 72h timeline. These
        # are policy/simulation bounds, not assertions about provider limits.
        self.assertLessEqual(refresh_tokens.call_count * 1, 73)
        self.assertLessEqual(counts["sync_fetch_crunchyroll"] * 4, 292)
        self.assertLessEqual(counts["sync_apply"] * 8, 584)
        self.assertLessEqual(counts["mal_list_refresh"] * 3, 27)
        self.assertLessEqual(counts["recommend_metadata_refresh"] * 8, 56)
        self.assertLessEqual(counts["recommend_provider_eligibility_crunchyroll"] * 7, 504)

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        eligibility = state["tasks"]["recommend_provider_eligibility_crunchyroll"]
        self.assertEqual(0, eligibility["last_request_delta"])
        self.assertEqual(1, eligibility["last_result"]["eligibility_fresh_skips"])
        self.assertEqual(0, eligibility["last_result"]["provider_searches"])


if __name__ == "__main__":
    unittest.main()
