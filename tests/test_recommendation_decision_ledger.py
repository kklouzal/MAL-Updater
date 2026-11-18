from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main as cli_main
from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, connect
from mal_updater.recommendation_decision_ledger import (
    RECOMMENDATION_POLICY_ARTIFACT_SHA256,
    insert_recommendation_decision_ledger,
    list_recommendation_decision_ledger_items,
    prune_recommendation_decision_ledger,
)
from mal_updater.recommendations import Recommendation


class RecommendationDecisionLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_records_stable_identity_full_eligibility_explicit_rank_and_exposure(self) -> None:
        run = insert_recommendation_decision_ledger(
            self.config.db_path,
            [
                {
                    "kind": "discovery_candidate",
                    "provider": "crunchyroll",
                    "provider_series_id": "cr-1",
                    "title": "First",
                    "priority": 91,
                    "reasons": ["provider availability"],
                    "context": {
                        "mal_anime_id": 111,
                        "last_verified_at": "2026-08-15T20:00:00Z",
                        "scorecard": {"total": 20.5, "continuity": 10.0},
                    },
                },
                {
                    "kind": "resume_backlog",
                    "provider": "hidive",
                    "provider_series_id": "hi-2",
                    "title": "Second",
                    "priority": 80,
                    "reasons": ["unfinished"],
                    "context": {"last_progress_seen_at": "2026-08-15T19:00:00+00:00"},
                },
            ],
            run_id="decision-test",
            cutoff_at="2026-08-15T21:00:00+00:00",
            output_limit=1,
            selected_item_identities_in_exposure_order=["discovery_candidate:mal:111"],
        )

        self.assertEqual(RECOMMENDATION_POLICY_ARTIFACT_SHA256, run.policy_artifact_sha256)
        self.assertEqual("2026-08-15T20:00:00Z", run.maximum_evidence_at)
        self.assertEqual(2, run.candidate_count)
        self.assertEqual(1, run.selected_count)
        rows = list_recommendation_decision_ledger_items(self.config.db_path, run_id=run.run_id)
        self.assertEqual([1, None], [row.exposure_rank for row in rows])
        self.assertEqual([True, False], [row.selected for row in rows])
        self.assertEqual(["selected", "eligible_not_selected_output_limit"], [row.exposure_state for row in rows])
        self.assertEqual("discovery_candidate:mal:111", rows[0].item_identity)
        self.assertEqual("resume_backlog:provider:hidive:hi-2", rows[1].item_identity)
        self.assertEqual(20.5, rows[0].score)
        self.assertEqual(64, len(rows[0].feature_evidence_payload_hash))

    def test_grouped_cli_records_full_candidate_order_and_emitted_exposure_order(self) -> None:
        candidates = [
            Recommendation("new_season", 100, "next-a", "Next A", None, provider="crunchyroll"),
            Recommendation("discovery_candidate", 99, "discover-c", "Discover C", None, provider="hidive"),
            Recommendation("new_season", 90, "next-b", "Next B", None, provider="crunchyroll"),
        ]
        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "recommend",
            "--limit",
            "3",
        ]
        with (
            patch("sys.argv", argv),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
            patch("mal_updater.cli.build_recommendations", return_value=candidates) as build,
        ):
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        build.assert_called_once_with(
            self.config,
            limit=0,
            require_provider_availability=True,
            include_discovery_candidates_without_actionable_provider_evidence=False,
        )
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            ["next-a", "next-b", "discover-c"],
            [item["provider_series_id"] for section in payload for item in section["items"]],
        )
        with connect(self.config.db_path) as conn:
            run_id = conn.execute(
                "SELECT run_id FROM recommendation_decision_ledger_runs ORDER BY created_at DESC, run_id DESC LIMIT 1"
            ).fetchone()["run_id"]
        rows = list_recommendation_decision_ledger_items(self.config.db_path, run_id=run_id)
        self.assertEqual(["next-a", "discover-c", "next-b"], [row.provider_series_id for row in rows])
        self.assertEqual([1, 2, 3], [row.candidate_ordinal for row in rows])
        self.assertEqual([1, 3, 2], [row.exposure_rank for row in rows])

    def test_rejects_evidence_newer_than_cutoff(self) -> None:
        with self.assertRaisesRegex(ValueError, "newer than cutoff"):
            insert_recommendation_decision_ledger(
                self.config.db_path,
                [
                    {
                        "kind": "resume_backlog",
                        "provider": "hidive",
                        "provider_series_id": "future",
                        "title": "Future",
                        "context": {"last_progress_seen_at": "2026-08-15T22:00:00Z"},
                    }
                ],
                run_id="future-test",
                cutoff_at="2026-08-15T21:00:00Z",
                output_limit=20,
            )

    def _insert_run(self, run_id: str) -> None:
        insert_recommendation_decision_ledger(
            self.config.db_path,
            [{"kind": "resume_backlog", "provider": "hidive", "provider_series_id": run_id, "title": run_id, "priority": 1, "reasons": ["r"]}],
            run_id=run_id,
            cutoff_at="2026-08-15T21:00:00Z",
            output_limit=None,
        )

    def _age_run(self, run_id: str, aged_run_id: str, created_at: str) -> None:
        # Rows are immutable, so an aged copy is inserted with an explicit created_at.
        with connect(self.config.db_path) as conn:
            for table in ("recommendation_decision_ledger_runs", "recommendation_decision_ledger_items"):
                cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
                select = ", ".join("?" if c == "run_id" else ("?" if c == "created_at" else c) for c in cols)
                conn.execute(
                    f"INSERT INTO {table} ({', '.join(cols)}) SELECT {select} FROM {table} WHERE run_id = ?",
                    tuple(aged_run_id if c == "run_id" else created_at for c in cols if c in ("run_id", "created_at")) + (run_id,),
                )
            conn.commit()

    def test_prune_removes_only_whole_runs_past_retention_in_bounded_batches(self) -> None:
        self._insert_run("fresh")
        self._age_run("fresh", "old-1", "2026-01-01 00:00:00")
        self._age_run("fresh", "old-2", "2026-01-02 00:00:00")

        first = prune_recommendation_decision_ledger(self.config.db_path, retention_days=30, batch_runs=1)
        self.assertEqual((1, 1, 1), (first["deleted_runs"], first["deleted_items"], first["remaining_eligible_runs"]))
        second = prune_recommendation_decision_ledger(self.config.db_path, retention_days=30, batch_runs=10)
        self.assertEqual((1, 0), (second["deleted_runs"], second["remaining_eligible_runs"]))
        # A retention below the SQL-enforced floor is clamped, so fresh rows survive.
        floor = prune_recommendation_decision_ledger(self.config.db_path, retention_days=0)
        self.assertEqual(("no_change", 7), (floor["status"], floor["retention_days"]))
        with connect(self.config.db_path) as conn:
            runs = [row[0] for row in conn.execute("SELECT run_id FROM recommendation_decision_ledger_runs")]
            items = conn.execute("SELECT COUNT(*) FROM recommendation_decision_ledger_items").fetchone()[0]
        self.assertEqual(["fresh"], runs)
        self.assertEqual(1, items)

    def test_ledger_rows_are_immutable(self) -> None:
        insert_recommendation_decision_ledger(
            self.config.db_path,
            [
                {
                    "kind": "resume_backlog",
                    "provider": "hidive",
                    "provider_series_id": "immutable",
                    "title": "Immutable",
                }
            ],
            run_id="immutable-test",
            cutoff_at="2026-08-15T21:00:00Z",
            output_limit=20,
        )
        with connect(self.config.db_path) as conn:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
                conn.execute(
                    "UPDATE recommendation_decision_ledger_items SET title = 'changed' WHERE run_id = 'immutable-test'"
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
                conn.execute("DELETE FROM recommendation_decision_ledger_runs WHERE run_id = 'immutable-test'")

    def test_schema_rejects_malformed_direct_timestamps(self) -> None:
        run_values = (
            "direct-invalid",
            "not-a-timestamp",
            "cli:recommend",
            "rank_local_recommendations",
            "policy",
            "1",
            "0" * 64,
            None,
            None,
            0,
            0,
        )
        with connect(self.config.db_path) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO recommendation_decision_ledger_runs (
                        run_id, cutoff_at, surface, objective, policy_id, policy_version,
                        policy_artifact_sha256, maximum_evidence_at, output_limit,
                        candidate_count, selected_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    run_values,
                )

        insert_recommendation_decision_ledger(
            self.config.db_path,
            [],
            run_id="direct-parent",
            cutoff_at="2026-08-15T21:00:00Z",
            output_limit=None,
        )
        with connect(self.config.db_path) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO recommendation_decision_ledger_items (
                        run_id, item_identity, candidate_ordinal, exposure_rank, selected,
                        eligibility_state, exposure_state, kind, title, reasons_json,
                        feature_evidence_payload_hash, maximum_evidence_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "direct-parent", "kind:provider:test:1", 1, 1, 1,
                        "eligible", "selected", "kind", "Title", "[]", "0" * 64,
                        "not-a-timestamp",
                    ),
                )


if __name__ == "__main__":
    unittest.main()
