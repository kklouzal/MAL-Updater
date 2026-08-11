from __future__ import annotations

from datetime import datetime, timezone
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.db import bootstrap_database, connect
from mal_updater.recommendation_snapshot_retention import prune_recommendation_score_snapshots
from mal_updater.service_runtime import run_pending_tasks


class RecommendationSnapshotRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.config = load_config(self.root)
        ensure_directories(self.config)
        bootstrap_database(self.config.db_path)

    def _insert_run(self, run_id: str, generated_at: str, *kinds: str, rows_per_kind: int = 2) -> None:
        with connect(self.config.db_path) as conn:
            for kind in kinds:
                for index in range(rows_per_kind):
                    conn.execute(
                        """
                        INSERT INTO recommendation_score_snapshots(run_id, generated_at, kind, title)
                        VALUES (?, ?, ?, ?)
                        """,
                        (run_id, generated_at, kind, f"{run_id}-{kind}-{index}"),
                    )
            conn.commit()

    def _run_ids(self, kind: str) -> list[str]:
        with connect(self.config.db_path) as conn:
            return [
                str(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT run_id FROM recommendation_score_snapshots WHERE kind = ? ORDER BY run_id",
                    (kind,),
                )
            ]

    def test_prunes_only_old_runs_beyond_each_kind_safety_floor(self) -> None:
        self._insert_run("old-a-1", "2025-01-01T00:00:00Z", "a")
        self._insert_run("old-a-2", "2025-01-02T00:00:00Z", "a")
        self._insert_run("new-a", "2026-07-31T00:00:00Z", "a")
        self._insert_run("old-b", "2025-01-01T00:00:00Z", "b")
        self._insert_run("new-b", "2026-07-31T00:00:00Z", "b")

        report = prune_recommendation_score_snapshots(
            self.config.db_path,
            retention_days=90,
            min_runs_per_kind=2,
            batch_size=100,
            now=datetime(2026, 8, 11, tzinfo=timezone.utc),
        )

        self.assertEqual("pruned", report.status)
        self.assertEqual(2, report.deleted_rows)
        self.assertEqual(["new-a", "old-a-2"], self._run_ids("a"))
        self.assertEqual(["new-b", "old-b"], self._run_ids("b"))

    def test_latest_global_run_is_preserved_and_deletion_is_batched(self) -> None:
        for day in range(1, 5):
            self._insert_run(f"old-{day}", f"2025-01-0{day}T00:00:00Z", "discovery", rows_per_kind=3)
        self._insert_run("latest", "2026-08-10T00:00:00Z", "discovery", rows_per_kind=3)

        first = prune_recommendation_score_snapshots(
            self.config.db_path,
            retention_days=90,
            min_runs_per_kind=1,
            batch_size=4,
            now=datetime(2026, 8, 11, tzinfo=timezone.utc),
        )

        self.assertEqual("latest", first.latest_run_id)
        self.assertEqual(4, first.deleted_rows)
        self.assertEqual(8, first.remaining_eligible_rows)
        self.assertIn("latest", self._run_ids("discovery"))
        self.assertFalse(first.vacuum_performed)

    def test_second_pass_is_idempotent_after_all_eligible_rows_are_removed(self) -> None:
        self._insert_run("old", "2025-01-01T00:00:00Z", "continue")
        self._insert_run("latest", "2026-08-10T00:00:00Z", "continue")
        options = dict(
            retention_days=90,
            min_runs_per_kind=1,
            batch_size=100,
            now=datetime(2026, 8, 11, tzinfo=timezone.utc),
        )

        first = prune_recommendation_score_snapshots(self.config.db_path, **options)
        second = prune_recommendation_score_snapshots(self.config.db_path, **options)

        self.assertEqual(2, first.deleted_rows)
        self.assertEqual("no_change", second.status)
        self.assertEqual(0, second.deleted_rows)
        self.assertEqual(first.rows_after, second.rows_after)

    def test_default_fourteen_day_horizon_drains_existing_excess_in_batches(self) -> None:
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        for day in range(1, 38):
            generated_at = datetime(2026, 7, day, tzinfo=timezone.utc) if day <= 31 else datetime(2026, 8, day - 31, tzinfo=timezone.utc)
            self._insert_run(
                f"discovery-{day:02d}",
                generated_at.isoformat().replace("+00:00", "Z"),
                "discovery",
                rows_per_kind=2,
            )
        # Sparse kinds remain wholly protected by the 30-run floor, regardless of age.
        for day in range(1, 6):
            self._insert_run(f"dubbed-{day}", f"2026-07-0{day}T00:00:00Z", "dubbed", rows_per_kind=1)

        first = prune_recommendation_score_snapshots(
            self.config.db_path,
            batch_size=5,
            now=now,
        )

        self.assertEqual(14, first.retention_days)
        self.assertEqual(30, first.min_runs_per_kind)
        self.assertEqual(14, first.eligible_rows)
        self.assertEqual(5, first.deleted_rows)
        self.assertEqual(9, first.remaining_eligible_rows)
        self.assertEqual([f"dubbed-{day}" for day in range(1, 6)], self._run_ids("dubbed"))

        second = prune_recommendation_score_snapshots(
            self.config.db_path,
            batch_size=100,
            now=now,
        )
        self.assertEqual(9, second.eligible_rows)
        self.assertEqual(9, second.deleted_rows)
        self.assertEqual(0, second.remaining_eligible_rows)
        self.assertEqual(30, len(self._run_ids("discovery")))

    def test_scheduler_exposes_retention_diagnostics_without_vacuum(self) -> None:
        self.config.service.recommendation_snapshot_retention_days = 45
        self.config.service.recommendation_snapshot_min_runs_per_kind = 7
        self.config.service.recommendation_snapshot_prune_batch_size = 321
        diagnostics = {
            "status": "pruned",
            "deleted_rows": 12,
            "remaining_eligible_rows": 4,
            "rows_after": 99,
            "vacuum_performed": False,
        }

        with patch(
            "mal_updater.service_runtime.prune_recommendation_score_snapshots"
        ) as prune, patch("mal_updater.service_runtime._task_specs", return_value=[]):
            prune.return_value.as_dict.return_value = diagnostics
            result = run_pending_tasks(self.config)

        prune.assert_called_once_with(
            self.config.db_path,
            retention_days=45,
            min_runs_per_kind=7,
            batch_size=321,
        )
        self.assertEqual(diagnostics, result["recommendation_snapshot_retention"])
        self.assertIn("recommendation_score_snapshots_pruned=12", self.config.service_log_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
