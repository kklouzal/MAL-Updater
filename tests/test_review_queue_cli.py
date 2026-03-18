from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main as cli_main
from mal_updater.config import ensure_directories, load_config
from mal_updater.db import bootstrap_database, connect, replace_review_queue_entries


class ReviewQueueCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / "config").mkdir()
        self.config = load_config(self.project_root)
        ensure_directories(self.config)
        bootstrap_database(self.config.db_path)

    def test_list_review_queue_summary_reports_decisions_and_top_reasons(self) -> None:
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="mapping_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-1",
                    "severity": "error",
                    "payload": {
                        "decision": "needs_manual_match",
                        "reasons": ["ambiguous_candidates", "same_franchise_tie"],
                    },
                },
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-2",
                    "severity": "warning",
                    "payload": {
                        "decision": "review",
                        "reasons": ["ambiguous_candidates"],
                    },
                },
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--summary",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(2, payload["count"])
        self.assertEqual({"mapping_review": 2}, payload["by_issue_type"])
        self.assertEqual({"error": 1, "warning": 1}, payload["by_severity"])
        self.assertEqual({"needs_manual_match": 1, "review": 1}, payload["by_decision"])
        self.assertEqual("series-1", payload["decision_examples"]["needs_manual_match"][0]["provider_series_id"])
        self.assertEqual("ambiguous_candidates", payload["top_reasons"][0]["reason"])
        self.assertEqual(2, payload["top_reasons"][0]["count"])
        self.assertEqual("series-2", payload["top_reasons"][0]["examples"][0]["provider_series_id"])
        self.assertEqual("series-1", payload["top_reasons"][0]["examples"][1]["provider_series_id"])

    def test_list_review_queue_summary_honors_issue_type_filter(self) -> None:
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="mapping_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-1",
                    "severity": "error",
                    "payload": {"decision": "needs_manual_match", "reasons": ["mapping_reason"]},
                }
            ],
        )
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="sync_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-2",
                    "severity": "warning",
                    "payload": {"decision": "skip", "reasons": ["sync_reason"]},
                }
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--summary",
            "--issue-type",
            "sync_review",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual("sync_review", payload["issue_type_filter"])
        self.assertEqual(1, payload["count"])
        self.assertEqual({"sync_review": 1}, payload["by_issue_type"])
        self.assertEqual({"skip": 1}, payload["by_decision"])
        self.assertEqual("sync_reason", payload["top_reasons"][0]["reason"])

    def test_list_review_queue_summary_examples_include_title_when_available(self) -> None:
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="sync_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-2",
                    "severity": "warning",
                    "payload": {
                        "decision": "skip",
                        "crunchyroll_title": "Title From Sync Proposal",
                        "reasons": ["sync_reason"],
                    },
                }
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--summary",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual("Title From Sync Proposal", payload["decision_examples"]["skip"][0]["title"])
        self.assertEqual("Title From Sync Proposal", payload["top_reasons"][0]["examples"][0]["title"])

    def test_list_review_queue_summary_falls_back_to_provider_series_title(self) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_series(provider, provider_series_id, title, season_title, raw_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "crunchyroll",
                    "series-3",
                    "Base Title",
                    "Season Title From Provider",
                    "{}",
                ),
            )
            conn.commit()

        replace_review_queue_entries(
            self.config.db_path,
            issue_type="mapping_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-3",
                    "severity": "warning",
                    "payload": {
                        "decision": "review",
                        "reasons": ["missing_payload_title"],
                    },
                }
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--summary",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual("Season Title From Provider", payload["decision_examples"]["review"][0]["title"])
        self.assertEqual("Season Title From Provider", payload["top_reasons"][0]["examples"][0]["title"])

    def test_list_review_queue_summary_clusters_related_titles_into_franchise_buckets(self) -> None:
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="mapping_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-10",
                    "severity": "warning",
                    "payload": {
                        "decision": "review",
                        "crunchyroll_title": "Example Show Season 2",
                        "reasons": ["same_franchise_tie"],
                    },
                },
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-11",
                    "severity": "warning",
                    "payload": {
                        "decision": "review",
                        "crunchyroll_title": "Example Show Season 3",
                        "reasons": ["same_franchise_tie"],
                    },
                },
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--summary",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual("example show", payload["top_title_clusters"][0]["cluster"])
        self.assertEqual(2, payload["top_title_clusters"][0]["count"])
        example_titles = {item["title"] for item in payload["top_title_clusters"][0]["examples"]}
        self.assertEqual({"Example Show Season 2", "Example Show Season 3"}, example_titles)

    def test_list_review_queue_summary_groups_repeated_fix_strategies(self) -> None:
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="mapping_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-20",
                    "severity": "warning",
                    "payload": {
                        "decision": "needs_manual_match",
                        "crunchyroll_title": "Show A",
                        "reasons": ["same_franchise_tie", "ambiguous_candidates"],
                    },
                },
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-21",
                    "severity": "warning",
                    "payload": {
                        "decision": "needs_manual_match",
                        "crunchyroll_title": "Show B",
                        "reasons": ["ambiguous_candidates", "same_franchise_tie"],
                    },
                },
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--summary",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            "needs_manual_match | ambiguous_candidates | same_franchise_tie",
            payload["top_fix_strategies"][0]["strategy"],
        )
        self.assertEqual(2, payload["top_fix_strategies"][0]["count"])

    def test_list_review_queue_title_cluster_filter_returns_only_matching_rows(self) -> None:
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="mapping_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-30",
                    "severity": "warning",
                    "payload": {
                        "decision": "review",
                        "crunchyroll_title": "Example Show Season 2",
                        "reasons": ["same_franchise_tie"],
                    },
                },
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-31",
                    "severity": "warning",
                    "payload": {
                        "decision": "review",
                        "crunchyroll_title": "Different Show",
                        "reasons": ["same_franchise_tie"],
                    },
                },
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--title-cluster",
            "example show",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(1, len(payload))
        self.assertEqual("series-30", payload[0]["provider_series_id"])

    def test_list_review_queue_summary_fix_strategy_filter_scopes_summary(self) -> None:
        replace_review_queue_entries(
            self.config.db_path,
            issue_type="mapping_review",
            entries=[
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-40",
                    "severity": "warning",
                    "payload": {
                        "decision": "needs_manual_match",
                        "crunchyroll_title": "Show A",
                        "reasons": ["same_franchise_tie", "ambiguous_candidates"],
                    },
                },
                {
                    "provider": "crunchyroll",
                    "provider_series_id": "series-41",
                    "severity": "warning",
                    "payload": {
                        "decision": "review",
                        "crunchyroll_title": "Show B",
                        "reasons": ["weak_candidates"],
                    },
                },
            ],
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "list-review-queue",
            "--summary",
            "--fix-strategy",
            "needs_manual_match | ambiguous_candidates | same_franchise_tie",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(1, payload["count"])
        self.assertEqual(
            "needs_manual_match | ambiguous_candidates | same_franchise_tie",
            payload["fix_strategy_filter"],
        )
        self.assertEqual({"needs_manual_match": 1}, payload["by_decision"])
        self.assertEqual("series-40", payload["decision_examples"]["needs_manual_match"][0]["provider_series_id"])


if __name__ == "__main__":
    unittest.main()
