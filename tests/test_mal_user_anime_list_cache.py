from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.db import (
    abort_mal_user_anime_list_cache_refresh,
    apply_migrations,
    begin_mal_user_anime_list_cache_refresh,
    bootstrap_database,
    connect,
    count_mal_user_anime_list_cache,
    finalize_mal_user_anime_list_cache_refresh,
    get_mal_user_anime_list_cache,
    list_mal_user_anime_list_cache,
    replace_mal_user_anime_list_cache_generation,
    upsert_mal_user_anime_list_cache_generation,
)


def _list_item(anime_id: int, title: str, status: str, *, score: int = 0, watched: int = 0) -> dict:
    return {
        "node": {"id": anime_id, "title": title, "unknown_node_field": {"kept": True}},
        "list_status": {
            "status": status,
            "score": score,
            "num_episodes_watched": watched,
            "start_date": "2024-01-01",
            "finish_date": "2024-01-14" if status == "completed" else None,
            "updated_at": f"2024-02-{anime_id % 28 + 1:02d}T00:00:00+00:00",
            "unknown_status_field": ["kept"],
        },
        "unexpected_future_field": {"kept": True},
    }


class MalUserAnimeListCacheDbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "mal-updater.sqlite3"
        bootstrap_database(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fresh_bootstrap_and_upgrade_apply_user_list_schema_idempotently(self) -> None:
        with connect(self.db_path) as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_cache)")}
            indexes = {row["name"] for row in conn.execute("PRAGMA index_list(mal_user_anime_list_cache)")}
            migrations = {row["version"] for row in conn.execute("SELECT version FROM schema_migrations")}
        self.assertIn("mal_anime_id", columns)
        self.assertIn("list_status", columns)
        self.assertIn("user_score", columns)
        self.assertIn("num_episodes_watched", columns)
        self.assertIn("start_date", columns)
        self.assertIn("finish_date", columns)
        self.assertIn("list_updated_at", columns)
        self.assertIn("node_json", columns)
        self.assertIn("list_status_json", columns)
        self.assertIn("raw_json", columns)
        self.assertIn("fetched_at", columns)
        self.assertIn("last_seen_at", columns)
        self.assertIn("refresh_generation", columns)
        self.assertIn("idx_mal_user_anime_list_cache_status", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_score", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_freshness", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_generation", indexes)
        self.assertIn("007_mal_user_anime_list_cache.sql", migrations)

        db_path = Path(self.temp_dir.name) / "upgrade.sqlite3"
        with connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            for migration in [
                "001_initial.sql",
                "002_mal_metadata_cache.sql",
                "003_mal_recommendation_edges.sql",
                "004_provider_search_cache.sql",
                "004_mal_recommendation_harvest_status.sql",
                "005_recommendation_score_snapshots.sql",
                "006_recommendation_eligibility_evidence.sql",
            ]:
                conn.executescript((Path(__file__).resolve().parents[1] / "migrations" / migration).read_text(encoding="utf-8"))
                conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (migration,))
            conn.commit()
            apply_migrations(conn)
            apply_migrations(conn)
            migration_rows = conn.execute(
                "SELECT COUNT(*) AS n FROM schema_migrations WHERE version = '007_mal_user_anime_list_cache.sql'"
            ).fetchone()["n"]
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_cache)")}
        self.assertEqual(1, migration_rows)
        self.assertIn("last_seen_at", columns)
        self.assertIn("list_status_json", columns)

    def test_upsert_list_get_count_and_preserve_unknown_json_fields(self) -> None:
        summary = replace_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[
                _list_item(10, "Completed", "completed", score=9, watched=12),
                _list_item(20, "Plan", "plan_to_watch"),
                _list_item(20, "Duplicate", "dropped"),
            ],
            refresh_run_id="run-1",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )
        self.assertEqual("ok", summary.status)
        self.assertEqual(2, summary.upserted)
        self.assertEqual({"completed": 1, "plan_to_watch": 1}, summary.by_status)
        self.assertEqual(1, summary.scored)
        self.assertEqual(1, summary.unscored)

        rows = list_mal_user_anime_list_cache(self.db_path)
        self.assertEqual([10, 20], [row.mal_anime_id for row in rows])
        completed = get_mal_user_anime_list_cache(self.db_path, 10)
        self.assertIsNotNone(completed)
        assert completed is not None
        self.assertEqual("Completed", completed.title)
        self.assertEqual("completed", completed.list_status)
        self.assertEqual(9, completed.user_score)
        self.assertEqual(12, completed.num_episodes_watched)
        self.assertEqual("2024-01-01", completed.start_date)
        self.assertEqual("2024-01-14", completed.finish_date)
        self.assertEqual("2024-02-11T00:00:00+00:00", completed.list_updated_at)
        self.assertEqual({"kept": True}, completed.node["unknown_node_field"])
        self.assertEqual(["kept"], completed.list_status_raw["unknown_status_field"])
        self.assertEqual({"kept": True}, completed.raw["unexpected_future_field"])
        self.assertEqual("run-1", completed.refresh_run_id)
        self.assertEqual("2026-07-19T00:00:00Z", completed.fetched_at)
        self.assertEqual("2026-07-19T00:00:00Z", completed.last_seen_at)
        self.assertEqual(2, count_mal_user_anime_list_cache(self.db_path))
        self.assertEqual(1, count_mal_user_anime_list_cache(self.db_path, statuses=["completed"]))
        self.assertEqual([20], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.db_path, statuses=["plan_to_watch"])])

    def test_partial_bounded_or_aborted_generation_never_prunes_absent_rows(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(10, "Existing", "completed", score=8)],
            refresh_run_id="old",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )
        generation = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="partial-run",
            fetched_at="2026-07-19T01:00:00Z",
        )
        partial = upsert_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(20, "New", "watching", score=7, watched=3)],
            refresh_run_id=generation.refresh_run_id,
            generation=generation.generation,
            fetched_at=generation.fetched_at,
        )
        self.assertEqual("upserted", partial.status)
        self.assertTrue(partial.partial)
        self.assertEqual(1, partial.preserved_absent)
        self.assertEqual([10, 20], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.db_path)])

        with self.assertRaises(ValueError):
            finalize_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=generation.refresh_run_id,
                generation=generation.generation,
                proven_complete=False,
                delete_absent=True,
            )
        aborted = abort_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id=generation.refresh_run_id,
            generation=generation.generation,
            error="bounded page run",
        )
        self.assertEqual("aborted", aborted.status)
        self.assertEqual("bounded page run", aborted.error)
        self.assertEqual([10, 20], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.db_path)])

    def test_proven_complete_finalize_can_explicitly_prune_absent_rows(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(10, "Existing", "completed", score=8)],
            refresh_run_id="old",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )
        generation = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="complete-run",
            fetched_at="2026-07-19T01:00:00Z",
        )
        upsert_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(20, "New", "watching", score=7, watched=3)],
            refresh_run_id=generation.refresh_run_id,
            generation=generation.generation,
            fetched_at=generation.fetched_at,
        )
        complete = finalize_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id=generation.refresh_run_id,
            generation=generation.generation,
            proven_complete=True,
            delete_absent=True,
        )
        self.assertEqual("ok", complete.status)
        self.assertEqual(1, complete.pruned)
        self.assertEqual([20], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.db_path)])


if __name__ == "__main__":
    unittest.main()
