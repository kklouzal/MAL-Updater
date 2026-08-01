from __future__ import annotations

import json
import multiprocessing
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mal_updater.config import ensure_directories, load_config
from mal_updater.request_tracking import (
    begin_api_request_context,
    capture_api_event_boundary,
    count_api_events_since,
    end_api_request_context,
    record_api_request_event,
    prune_api_request_events,
    prune_api_request_events_with_diagnostics,
    sanitize_telemetry_url,
    summarize_recent_api_usage,
)


class RequestTrackingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        (self.root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(self.root)
        # Test invocations intentionally provide a unique runtime root.  Keep
        # individual cases isolated within it so process-safe append tests do
        # not share telemetry with earlier cases in this module.
        self.config.state_dir = self.root / ".MAL-Updater" / "state"
        ensure_directories(self.config)

    def _events(self) -> list[dict]:
        return [json.loads(line) for line in self.config.api_request_events_path.read_text(encoding="utf-8").splitlines()]

    def test_v2_event_has_run_sequence_and_redacts_url_error_values(self) -> None:
        token = begin_api_request_context(task="sync_fetch_hidive", run_id="run-123")
        try:
            record_api_request_event(
                "hidive",
                "algolia_search",
                url="https://user:pass@example.invalid/search?q=Private+Title&api_key=public-key&limit=3#fragment",
                method="post",
                outcome="request_error",
                error="Bearer token-value password=hunter2 https://example.invalid/?query=PrivateTitle",
                config=self.config,
            )
            record_api_request_event("hidive", "retry", url="https://example.invalid/retry", method="GET", outcome="ok", config=self.config)
        finally:
            end_api_request_context(token)

        first, second = self._events()
        self.assertEqual(2, first["schema_version"])
        self.assertTrue(first["event_id"])
        self.assertEqual("sync_fetch_hidive", first["task"])
        self.assertEqual("run-123", first["run_id"])
        self.assertEqual(1, first["attempt_sequence"])
        self.assertEqual(2, second["attempt_sequence"])
        serialized = json.dumps(first)
        for secret in ("Private Title", "PrivateTitle", "public-key", "token-value", "hunter2", "user:pass"):
            self.assertNotIn(secret, serialized)
        self.assertIn("q=%3Credacted%3E", first["url"])

    def test_shared_sanitizer_preserves_request_markers_and_removes_sentinels(self) -> None:
        sentinel = "SENTINEL-request-credential-123456789"
        record_api_request_event(
            "mal",
            "refresh",
            url=f"https://user:{sentinel}@example.invalid/token?access_token={sentinel}&page=9#private",
            method="POST",
            outcome="request_error",
            status_code=401,
            error=f'HTTP 401 invalid_grant {{"refresh_token":"{sentinel}"}} Basic {sentinel}',
            config=self.config,
        )
        event = self._events()[0]
        rendered = json.dumps(event)
        self.assertNotIn(sentinel, rendered)
        self.assertIn("HTTP 401 invalid_grant", event["error"])
        self.assertIn("access_token=%3Credacted%3E", event["url"])
        self.assertIn("page=%3Cvalue%3E", event["url"])

    def test_run_boundary_counts_only_matching_attributed_attempts_during_overlap(self) -> None:
        boundary = capture_api_event_boundary(config=self.config)
        token_a = begin_api_request_context(task="sync_apply", run_id="run-a")
        try:
            record_api_request_event("mal", "put", url="https://example.invalid/a", method="PUT", outcome="ok", config=self.config)
        finally:
            end_api_request_context(token_a)
        token_b = begin_api_request_context(task="mal_refresh", run_id="run-b")
        try:
            record_api_request_event("mal", "post", url="https://example.invalid/b", method="POST", outcome="timeout", config=self.config)
        finally:
            end_api_request_context(token_b)

        self.assertEqual(1, count_api_events_since(boundary, provider="mal", task="sync_apply", run_id="run-a", config=self.config))
        self.assertEqual(1, count_api_events_since(boundary, provider="mal", task="mal_refresh", run_id="run-b", config=self.config))
        self.assertEqual(2, count_api_events_since(boundary, provider="mal", config=self.config))

    def test_concurrent_contexts_append_valid_unique_events_without_cross_attribution(self) -> None:
        def write_run(run_number: int) -> None:
            task = f"task-{run_number % 2}"
            token = begin_api_request_context(task=task, run_id=f"run-{run_number}")
            try:
                for attempt in range(5):
                    record_api_request_event(
                        "crunchyroll", "get", url=f"https://example.invalid/{run_number}/{attempt}",
                        method="GET", outcome="ok", config=self.config,
                    )
            finally:
                end_api_request_context(token)

        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(write_run, range(8)))

        events = self._events()
        self.assertEqual(40, len(events))
        self.assertEqual(40, len({event["event_id"] for event in events}))
        self.assertEqual(20, summarize_recent_api_usage(provider="crunchyroll", task="task-0", config=self.config).request_count)
        self.assertEqual(20, summarize_recent_api_usage(provider="crunchyroll", task="task-1", config=self.config).request_count)

    def test_legacy_events_remain_global_and_are_conservative_for_task_scope(self) -> None:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        legacy = {"at": now, "provider": "mal", "operation": "legacy", "url": "https://example.invalid", "method": "GET", "outcome": "ok"}
        self.config.api_request_events_path.write_text(json.dumps(legacy) + "\n", encoding="utf-8")
        token = begin_api_request_context(task="sync_apply", run_id="new-run")
        try:
            record_api_request_event("mal", "new", url="https://example.invalid", method="PUT", outcome="ok", config=self.config)
        finally:
            end_api_request_context(token)

        self.assertEqual(2, summarize_recent_api_usage(provider="mal", config=self.config).request_count)
        self.assertEqual(1, summarize_recent_api_usage(provider="mal", task="sync_apply", config=self.config).request_count)
        self.assertEqual(2, summarize_recent_api_usage(provider="mal", task="sync_apply", include_legacy_in_task=True, config=self.config).request_count)

    def test_rolling_expiry_does_not_change_monotonic_boundary_delta(self) -> None:
        expired_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
        old = {"schema_version": 2, "event_id": "old", "at": expired_at, "provider": "mal", "operation": "old", "url": "https://example.invalid", "method": "GET", "outcome": "ok", "task": "sync_apply", "run_id": "old-run"}
        self.config.api_request_events_path.write_text(json.dumps(old) + "\n", encoding="utf-8")
        boundary = capture_api_event_boundary(config=self.config)
        token = begin_api_request_context(task="sync_apply", run_id="current-run")
        try:
            record_api_request_event("mal", "current", url="https://example.invalid", method="PUT", outcome="request_error", config=self.config)
        finally:
            end_api_request_context(token)

        self.assertEqual(1, summarize_recent_api_usage(provider="mal", window_seconds=3600, config=self.config).request_count)
        self.assertEqual(1, count_api_events_since(boundary, provider="mal", task="sync_apply", run_id="current-run", config=self.config))

    def test_malformed_port_is_sanitized_without_masking_caller_errors(self) -> None:
        self.assertEqual("<invalid-url>", sanitize_telemetry_url("https://example.invalid:not-a-port/path?q=secret"))

    def test_malformed_timestamps_block_pruning_without_losing_original_records(self) -> None:
        naive = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        rows = [
            {"at": naive, "provider": "mal", "operation": "legacy", "outcome": "ok"},
            {"at": "not-a-date", "provider": "mal", "operation": "bad", "outcome": "ok"},
            {"at": "9999-12-31T23:59:59-23:59", "provider": "mal", "operation": "overflow", "outcome": "ok"},
        ]
        original = "".join(json.dumps(row) + "\n" for row in rows)
        self.config.api_request_events_path.write_text(original, encoding="utf-8")
        self.assertEqual(1, summarize_recent_api_usage(provider="mal", config=self.config).request_count)
        report = prune_api_request_events_with_diagnostics(config=self.config)
        self.assertTrue(report.blocked)
        self.assertEqual("blocked_corrupt", report.status)
        self.assertEqual(0, report.actual_removed)
        self.assertEqual(2, report.corrupt_records)
        self.assertEqual(original, self.config.api_request_events_path.read_text(encoding="utf-8"))
        self.assertEqual(0, prune_api_request_events(config=self.config))

    def test_corrupt_jsonl_blocks_prune_and_preserves_valid_neighbors_for_repair(self) -> None:
        sentinel = "SENTINEL-jsonl-credential-123456789"
        old_at = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")
        current_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        current = {
            "schema_version": 2,
            "event_id": "current",
            "at": current_at,
            "provider": "mal",
            "operation": "current",
            "url": "https://example.invalid/current",
            "method": "GET",
            "outcome": "ok",
        }
        old = {**current, "event_id": "old", "at": old_at, "operation": "old"}
        original = (
            json.dumps(old)
            + "\n"
            + f'{{"at":"{current_at}","provider":"mal","refresh_token":"{sentinel}"'
            + "\n"
            + json.dumps(current)
            + "\n"
        )
        self.config.api_request_events_path.write_text(original, encoding="utf-8")

        self.assertEqual(1, summarize_recent_api_usage(provider="mal", config=self.config).request_count)
        report = prune_api_request_events_with_diagnostics(config=self.config)
        self.assertEqual(
            {
                "status": "blocked_corrupt",
                "blocked": True,
                "actual_removed": 0,
                "expired_removed": 0,
                "expired_candidates": 1,
                "corrupt_records": 1,
                "kept_records": 1,
                "scanned_records": 3,
            },
            report.as_dict(),
        )
        self.assertEqual(original, self.config.api_request_events_path.read_text(encoding="utf-8"))
        self.assertIn(sentinel, self.config.api_request_events_path.read_text(encoding="utf-8"))

    def test_healthy_prune_atomically_removes_expired_records_and_keeps_valid_neighbors(self) -> None:
        old_at = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")
        current_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        current = {"event_id": "current", "at": current_at, "provider": "mal", "operation": "current", "outcome": "ok"}
        rows = [
            {"event_id": "old-a", "at": old_at, "provider": "mal", "operation": "old-a", "outcome": "ok"},
            current,
            {"event_id": "old-b", "at": old_at, "provider": "mal", "operation": "old-b", "outcome": "ok"},
        ]
        self.config.api_request_events_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

        report = prune_api_request_events_with_diagnostics(config=self.config)
        self.assertFalse(report.blocked)
        self.assertEqual("ok", report.status)
        self.assertEqual(2, report.actual_removed)
        self.assertEqual(2, report.expired_removed)
        self.assertEqual(1, report.kept_records)
        self.assertEqual(["current"], [event["event_id"] for event in self._events()])

    def test_multiprocess_append_and_prune_preserve_jsonl_integrity(self) -> None:
        # fork is the production Linux execution model and avoids serializing AppConfig.
        if "fork" not in multiprocessing.get_all_start_methods():
            self.skipTest("requires Linux fork")
        ctx = multiprocessing.get_context("fork")

        def append_many() -> None:
            for index in range(40):
                record_api_request_event("mal", "child", url=f"https://example.invalid/{index}", method="GET", outcome="ok", config=self.config)

        def prune_many() -> None:
            for _ in range(20):
                prune_api_request_events(config=self.config)

        writers = [ctx.Process(target=append_many) for _ in range(3)]
        pruner = ctx.Process(target=prune_many)
        for process in [*writers, pruner]:
            process.start()
        for process in [*writers, pruner]:
            process.join(10)
            self.assertEqual(0, process.exitcode)
        events = self._events()
        self.assertEqual(120, len(events))
        self.assertEqual(120, len({event["event_id"] for event in events}))


if __name__ == "__main__":
    unittest.main()
