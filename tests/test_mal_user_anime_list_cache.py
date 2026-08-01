from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
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
    MalUserAnimeListRefreshConflictError,
    replace_mal_user_anime_list_cache_generation,
    upsert_mal_user_anime_list_cache_generation,
)
from mal_updater.recommendation_metadata import refresh_mal_user_anime_list_cache


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


def _refresh_lifecycle_row(db_path: Path, generation: int):
    with connect(db_path) as conn:
        return conn.execute(
            "SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation = ?",
            (int(generation),),
        ).fetchone()


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
            lifecycle_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_refresh_generations)")}
            indexes = {row["name"] for row in conn.execute("PRAGMA index_list(mal_user_anime_list_cache)")}
            lifecycle_indexes = {row["name"] for row in conn.execute("PRAGMA index_list(mal_user_anime_list_refresh_generations)")}
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
        self.assertIn("priority", columns)
        self.assertIn("is_rewatching", columns)
        self.assertIn("num_times_rewatched", columns)
        self.assertIn("rewatch_value", columns)
        self.assertIn("tag_count", columns)
        self.assertIn("has_comments", columns)
        self.assertNotIn("tags_json", columns)
        self.assertNotIn("comments", columns)
        self.assertIn("idx_mal_user_anime_list_cache_status", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_score", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_freshness", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_generation", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_priority_pref", indexes)
        self.assertIn("idx_mal_user_anime_list_cache_private_text_presence", indexes)
        self.assertIn("generation", lifecycle_columns)
        self.assertIn("refresh_run_id", lifecycle_columns)
        self.assertIn("status", lifecycle_columns)
        self.assertIn("fetched_at", lifecycle_columns)
        self.assertIn("completed_at", lifecycle_columns)
        self.assertIn("idx_mal_user_anime_list_refresh_generations_status", lifecycle_indexes)
        self.assertIn("uq_mal_user_anime_list_refresh_generations_active", lifecycle_indexes)
        self.assertIn("007_mal_user_anime_list_cache.sql", migrations)
        self.assertIn("011_mal_user_anime_list_preference_fields.sql", migrations)
        self.assertIn("019_mal_user_anime_list_refresh_generations.sql", migrations)

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
            lifecycle_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_refresh_generations)")}
        self.assertEqual(1, migration_rows)
        self.assertIn("last_seen_at", columns)
        self.assertIn("list_status_json", columns)
        self.assertIn("priority", columns)
        self.assertIn("has_comments", columns)
        self.assertIn("generation", lifecycle_columns)
        self.assertIn("refresh_run_id", lifecycle_columns)

    def test_concurrent_begins_allocate_unique_monotonic_generations(self) -> None:
        def begin(index: int):
            return begin_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=f"concurrent-{index}",
                fetched_at=f"2026-07-19T00:00:{index:02d}Z",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            allocations = list(executor.map(begin, range(8)))

        generations = sorted(allocation.generation for allocation in allocations)
        self.assertEqual(list(range(1, 9)), generations)
        self.assertEqual(8, len({allocation.refresh_run_id for allocation in allocations}))
        with connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT generation, status, error FROM mal_user_anime_list_refresh_generations ORDER BY generation"
            ).fetchall()
        self.assertEqual(1, sum(row["status"] == "active" for row in rows))
        self.assertEqual(8, next(row["generation"] for row in rows if row["status"] == "active"))
        for row in rows[:-1]:
            self.assertEqual("failed", row["status"])
            self.assertEqual("superseded by a newer MAL user anime list refresh", row["error"])

    def test_begin_keeps_exactly_one_latest_active_and_supersedes_older_active(self) -> None:
        old = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="old-active",
            fetched_at="2026-07-19T00:00:00Z",
        )
        new = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="new-active",
            fetched_at="2026-07-19T00:01:00Z",
        )

        old_lifecycle = _refresh_lifecycle_row(self.db_path, old.generation)
        new_lifecycle = _refresh_lifecycle_row(self.db_path, new.generation)
        self.assertIsNotNone(old_lifecycle)
        self.assertIsNotNone(new_lifecycle)
        assert old_lifecycle is not None
        assert new_lifecycle is not None
        self.assertEqual("failed", old_lifecycle["status"])
        self.assertEqual("superseded by a newer MAL user anime list refresh", old_lifecycle["error"])
        self.assertIsNotNone(old_lifecycle["completed_at"])
        self.assertEqual("active", new_lifecycle["status"])
        with connect(self.db_path) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) AS n FROM mal_user_anime_list_refresh_generations WHERE status = 'active'"
                ).fetchone()["n"],
            )

        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*failed"):
            begin_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=old.refresh_run_id,
                fetched_at="2026-07-19T00:02:00Z",
            )

    def test_duplicate_active_begin_returns_existing_allocation_and_terminal_reuse_rejects(self) -> None:
        first = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="duplicate-run",
            fetched_at="2026-07-19T00:00:00Z",
        )
        duplicate = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="duplicate-run",
            fetched_at="2026-07-19T00:05:00Z",
        )
        self.assertEqual(first, duplicate)

        finalize_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id=first.refresh_run_id,
            generation=first.generation,
            proven_complete=True,
            delete_absent=False,
        )
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*completed"):
            begin_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id="duplicate-run",
                fetched_at="2026-07-19T00:10:00Z",
            )

    def test_generation_identity_active_and_terminal_validation_rejects_bad_operations(self) -> None:
        generation = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="identity-run",
            fetched_at="2026-07-19T00:00:00Z",
        )

        with self.assertRaisesRegex(ValueError, "unknown"):
            upsert_mal_user_anime_list_cache_generation(
                self.db_path,
                items=[_list_item(10, "Unknown", "completed")],
                refresh_run_id=generation.refresh_run_id,
                generation=generation.generation + 100,
                fetched_at=generation.fetched_at,
            )
        with self.assertRaisesRegex(ValueError, "mismatch"):
            upsert_mal_user_anime_list_cache_generation(
                self.db_path,
                items=[_list_item(10, "Mismatch", "completed")],
                refresh_run_id="wrong-run",
                generation=generation.generation,
                fetched_at=generation.fetched_at,
            )

        upsert_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(10, "Valid", "completed")],
            refresh_run_id=generation.refresh_run_id,
            generation=generation.generation,
            fetched_at=generation.fetched_at,
        )
        finalize_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id=generation.refresh_run_id,
            generation=generation.generation,
            proven_complete=True,
            delete_absent=False,
        )

        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*completed"):
            upsert_mal_user_anime_list_cache_generation(
                self.db_path,
                items=[_list_item(10, "Terminal", "completed")],
                refresh_run_id=generation.refresh_run_id,
                generation=generation.generation,
                fetched_at=generation.fetched_at,
            )
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*completed"):
            finalize_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=generation.refresh_run_id,
                generation=generation.generation,
                proven_complete=True,
                delete_absent=False,
            )
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*completed"):
            abort_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=generation.refresh_run_id,
                generation=generation.generation,
                error="late abort",
            )

    def test_database_partial_unique_index_rejects_a_second_active_generation(self) -> None:
        begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="only-active",
            fetched_at="2026-07-19T00:00:00Z",
        )
        with connect(self.db_path) as conn:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "UNIQUE constraint failed"):
                conn.execute(
                    """
                    INSERT INTO mal_user_anime_list_refresh_generations (
                        refresh_run_id, status, fetched_at
                    ) VALUES ('second-active', 'active', '2026-07-19T00:01:00Z')
                    """
                )

    def test_old_generation_upsert_rejects_and_cannot_overwrite_newer_row(self) -> None:
        old = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="old-overlap",
            fetched_at="2026-07-19T00:00:00Z",
        )
        new = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="new-overlap",
            fetched_at="2026-07-19T00:01:00Z",
        )
        upsert_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(10, "New Winner", "watching", score=7)],
            refresh_run_id=new.refresh_run_id,
            generation=new.generation,
            fetched_at=new.fetched_at,
        )
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*failed.*cannot upsert"):
            upsert_mal_user_anime_list_cache_generation(
                self.db_path,
                items=[_list_item(10, "Old Loser", "completed", score=10)],
                refresh_run_id=old.refresh_run_id,
                generation=old.generation,
                fetched_at=old.fetched_at,
            )
        row = get_mal_user_anime_list_cache(self.db_path, 10)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("New Winner", row.title)
        self.assertEqual(new.generation, row.refresh_generation)
        self.assertEqual(new.refresh_run_id, row.refresh_run_id)

    def test_stale_finalize_cannot_prune_and_current_complete_can_prune_older_rows(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(10, "Existing", "completed", score=8)],
            refresh_run_id="old-complete",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )
        stale = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="stale-complete",
            fetched_at="2026-07-19T01:00:00Z",
        )
        upsert_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(30, "Stale", "watching", score=7)],
            refresh_run_id=stale.refresh_run_id,
            generation=stale.generation,
            fetched_at=stale.fetched_at,
        )
        current = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="current-complete",
            fetched_at="2026-07-19T02:00:00Z",
        )
        upsert_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(20, "Current", "plan_to_watch")],
            refresh_run_id=current.refresh_run_id,
            generation=current.generation,
            fetched_at=current.fetched_at,
        )

        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*failed.*cannot finalize"):
            finalize_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=stale.refresh_run_id,
                generation=stale.generation,
                proven_complete=True,
                delete_absent=True,
            )
        self.assertEqual([10, 20, 30], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.db_path)])

        complete = finalize_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id=current.refresh_run_id,
            generation=current.generation,
            proven_complete=True,
            delete_absent=True,
        )
        self.assertEqual("ok", complete.status)
        self.assertEqual(2, complete.pruned)
        self.assertEqual([20], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.db_path)])

    def test_superseded_abort_and_duplicate_begin_reject_without_mutating_lifecycle(self) -> None:
        stale = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="stale-abort",
            fetched_at="2026-07-19T01:00:00Z",
        )
        begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="newer-abort",
            fetched_at="2026-07-19T02:00:00Z",
        )

        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*failed.*cannot abort"):
            abort_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=stale.refresh_run_id,
                generation=stale.generation,
                error="too late",
            )
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*failed"):
            begin_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=stale.refresh_run_id,
                fetched_at="2026-07-19T03:00:00Z",
            )

        lifecycle = _refresh_lifecycle_row(self.db_path, stale.generation)
        self.assertIsNotNone(lifecycle)
        assert lifecycle is not None
        self.assertEqual("failed", lifecycle["status"])
        self.assertEqual("superseded by a newer MAL user anime list refresh", lifecycle["error"])

    def test_privacy_safe_preference_fields_are_typed_while_raw_json_is_retained(self) -> None:
        item = _list_item(30, "Preference Seed", "completed", score=10, watched=12)
        item["list_status"].update(
            {
                "priority": 2,
                "is_rewatching": True,
                "num_times_rewatched": 3,
                "rewatch_value": 5,
                "tags": ["private favorite", "private vibe"],
                "comments": "private note that must not become a typed explanation field",
            }
        )
        summary = replace_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[item],
            refresh_run_id="pref-run",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )

        self.assertEqual(
            {
                "with_priority": 1,
                "with_rewatching": 1,
                "with_num_times_rewatched": 1,
                "with_rewatch_value": 1,
                "with_tags": 1,
                "with_comments": 1,
            },
            summary.preference_counts,
        )
        entry = get_mal_user_anime_list_cache(self.db_path, 30)
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(2, entry.priority)
        self.assertIs(entry.is_rewatching, True)
        self.assertEqual(3, entry.num_times_rewatched)
        self.assertEqual(5, entry.rewatch_value)
        self.assertEqual(2, entry.tag_count)
        self.assertTrue(entry.has_comments)
        self.assertEqual(["private favorite", "private vibe"], entry.list_status_raw["tags"])
        self.assertIn("private note", entry.list_status_raw["comments"])

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
        lifecycle = _refresh_lifecycle_row(self.db_path, generation.generation)
        self.assertIsNotNone(lifecycle)
        assert lifecycle is not None
        self.assertEqual("failed", lifecycle["status"])
        self.assertEqual("bounded page run", lifecycle["error"])
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
        lifecycle = _refresh_lifecycle_row(self.db_path, generation.generation)
        self.assertIsNotNone(lifecycle)
        assert lifecycle is not None
        self.assertEqual("completed", lifecycle["status"])
        self.assertEqual([20], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.db_path)])

    def test_superseded_complete_refresh_fails_closed_without_cache_overwrite_or_prune(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(10, "Existing", "completed", score=8)],
            refresh_run_id="old",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )
        superseded = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="superseded",
            fetched_at="2026-07-19T01:00:00Z",
        )
        current = begin_mal_user_anime_list_cache_refresh(
            self.db_path,
            refresh_run_id="current",
            fetched_at="2026-07-19T02:00:00Z",
        )
        upsert_mal_user_anime_list_cache_generation(
            self.db_path,
            items=[_list_item(30, "Current", "watching", score=7)],
            refresh_run_id=current.refresh_run_id,
            generation=current.generation,
            fetched_at=current.fetched_at,
        )

        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*failed.*cannot upsert"):
            upsert_mal_user_anime_list_cache_generation(
                self.db_path,
                items=[
                    _list_item(10, "Superseded Overwrite", "watching", score=10),
                    _list_item(20, "Superseded New", "completed"),
                ],
                refresh_run_id=superseded.refresh_run_id,
                generation=superseded.generation,
                fetched_at=superseded.fetched_at,
            )
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "terminal.*failed.*cannot finalize"):
            finalize_mal_user_anime_list_cache_refresh(
                self.db_path,
                refresh_run_id=superseded.refresh_run_id,
                generation=superseded.generation,
                proven_complete=True,
                delete_absent=True,
            )

        rows = list_mal_user_anime_list_cache(self.db_path)
        self.assertEqual([10, 30], [row.mal_anime_id for row in rows])
        self.assertEqual("Existing", rows[0].title)
        self.assertEqual("Current", rows[1].title)

    def test_top_level_refresh_superseded_after_collection_returns_controlled_failure(self) -> None:
        project_root = Path(self.temp_dir.name) / "refresh-project"
        config = load_config(project_root)
        bootstrap_database(config.db_path)
        replace_mal_user_anime_list_cache_generation(
            config.db_path,
            items=[_list_item(90, "Old", "completed", score=8)],
            refresh_run_id="old",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )

        def superseding_pages(**_: object):
            yield {"data": [_list_item(100, "Must Not Land", "watching", score=7)], "paging": {}}
            begin_mal_user_anime_list_cache_refresh(
                config.db_path,
                refresh_run_id="overlapping-new-owner",
                fetched_at="2026-07-19T02:00:00Z",
            )

        with patch(
            "mal_updater.recommendation_metadata.MalClient.iter_my_anime_list_pages",
            side_effect=superseding_pages,
        ):
            summary = refresh_mal_user_anime_list_cache(config, max_pages=3, prune_on_complete=True)

        self.assertEqual("failed", summary.status)
        self.assertTrue(summary.partial)
        self.assertEqual(1, summary.pages)
        self.assertEqual(1, summary.items)
        self.assertIn("terminal (failed)", summary.error or "")
        self.assertEqual([90], [row.mal_anime_id for row in list_mal_user_anime_list_cache(config.db_path)])
        lifecycle = _refresh_lifecycle_row(config.db_path, summary.generation)
        self.assertIsNotNone(lifecycle)
        assert lifecycle is not None
        self.assertEqual("failed", lifecycle["status"])
        self.assertEqual("superseded by a newer MAL user anime list refresh", lifecycle["error"])

    def test_top_level_refresh_does_not_swallow_unrelated_runtime_error(self) -> None:
        config = load_config(Path(self.temp_dir.name) / "runtime-error-project")
        bootstrap_database(config.db_path)
        with patch(
            "mal_updater.recommendation_metadata.MalClient.iter_my_anime_list_pages",
            side_effect=RuntimeError("unrelated bug"),
        ):
            with self.assertRaisesRegex(RuntimeError, "unrelated bug"):
                refresh_mal_user_anime_list_cache(config, max_pages=3)


if __name__ == "__main__":
    unittest.main()
