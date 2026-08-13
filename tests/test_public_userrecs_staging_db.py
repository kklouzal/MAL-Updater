from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from mal_updater import db
from mal_updater.db import (
    MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
    MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
    bootstrap_database,
    claim_mal_public_userrecs_sources,
    connect,
    create_or_get_active_mal_public_userrecs_generation,
    discard_mal_public_userrecs_generation,
    get_active_mal_public_userrecs_generation,
    get_mal_public_userrecs_generation,
    get_public_userrecs_diagnostics,
    mark_mal_public_userrecs_generation_ready,
    pause_mal_public_userrecs_generation,
    publish_mal_public_userrecs_generation,
    replace_mal_public_userrecs_staged_page,
    replace_mal_recommendation_edges,
    restart_mal_public_userrecs_generation_after_drift,
    release_mal_public_userrecs_source_claim,
    renew_mal_public_userrecs_source_claim,
    resume_mal_public_userrecs_generation,
    sync_mal_public_userrecs_source_queue,
    schedule_mal_public_userrecs_generation_retry,
)


SOURCE_URL = "https://myanimelist.net/anime/1/Cowboy_Bebop/userrecs"
PAGE2_URL = f"{SOURCE_URL}?p=2"
PAGE3_URL = f"{SOURCE_URL}?p=3"
FETCHED_AT = "2026-07-28T01:00:00Z"


def _edge(target_id: int, title: str, count: int, *, page_url: str = SOURCE_URL) -> dict[str, object]:
    return {
        "target_mal_anime_id": target_id,
        "target_title": title,
        "num_recommendations": count,
        "raw": {"source": "public_mal_userrecs", "page_url": page_url, "unsafe_free_prose": "drop me"},
        "provenance": {"source": "public_mal_userrecs", "page_url": page_url, "unsafe_user": "drop me"},
    }


class PublicUserRecsStagingDbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "mal.sqlite3"
        bootstrap_database(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _published_rows(self, source_id: int = 1) -> list[dict[str, object]]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT target_mal_anime_id, target_title, num_recommendations, harvest_source, complete_harvest, provenance_json
                FROM mal_anime_recommendations
                WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'
                ORDER BY target_mal_anime_id ASC
                """,
                (source_id,),
            ).fetchall()
        return [{key: row[key] for key in row.keys()} for row in rows]

    def _harvest_status(self, source_id: int = 1) -> dict[str, object] | None:
        with connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT status, num_edges, source_type, is_complete, pages_fetched, source_url, last_error, failure_count
                FROM mal_recommendation_harvest_status
                WHERE source_mal_anime_id = ?
                """,
                (source_id,),
            ).fetchone()
        return None if row is None else {key: row[key] for key in row.keys()}

    def _seed_published_official_detail(self) -> None:
        self.assertTrue(
            replace_mal_recommendation_edges(
                self.db_path,
                source_mal_anime_id=1,
                hop_distance=1,
                edges=[{"target_mal_anime_id": 90, "target_title": "Old", "num_recommendations": 2, "raw": {}}],
                source_type=MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
                complete=False,
            )
        )

    def test_migration_catalog_and_repository_package_parity_for_015(self) -> None:
        self.assertIn(db.PUBLIC_USERRECS_STAGING_MIGRATION, db.MIGRATION_FILENAMES)
        self.assertLess(
            db.MIGRATION_FILENAMES.index(db.PUBLIC_USERRECS_STAGING_MIGRATION),
            db.MIGRATION_FILENAMES.index(db.PROVIDER_WATCHLIST_MEMBERSHIP_MIGRATION),
        )
        root_file = Path(__file__).resolve().parents[1] / "migrations" / db.PUBLIC_USERRECS_STAGING_MIGRATION
        packaged = next(migration for migration in db.MIGRATIONS if migration.name == db.PUBLIC_USERRECS_STAGING_MIGRATION)
        self.assertEqual(root_file.read_text(encoding="utf-8"), packaged.read_text(encoding="utf-8"))
        with connect(self.db_path) as conn:
            self.assertIsNotNone(
                conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = ?",
                    (db.PUBLIC_USERRECS_STAGING_MIGRATION,),
                ).fetchone()
            )

    def test_create_or_get_enforces_one_open_generation_per_source(self) -> None:
        first = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        second = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Ignored",
            source_url=PAGE2_URL,
        )

        self.assertEqual(first.generation_id, second.generation_id)
        self.assertEqual(SOURCE_URL, first.cursor_url)
        with connect(self.db_path) as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO mal_public_userrecs_crawl_generations (source_mal_anime_id, source_title, source_url, status)
                    VALUES (1, 'Duplicate', ?, 'active')
                    """,
                    (SOURCE_URL,),
                )

    def test_durable_queue_strict_class_priority_fairness_and_claim_cas(self) -> None:
        sync_mal_public_userrecs_source_queue(
            self.db_path,
            source_mal_anime_ids=[1, 2, 3, 4],
            due_classes={1: "resumable", 2: "never_started", 3: "never_started", 4: "refresh_due"},
        )
        first = claim_mal_public_userrecs_sources(self.db_path, limit=2, claim_token="worker-a")
        self.assertEqual([2, 3], [row.source_mal_anime_id for row in first])
        self.assertEqual(["never_started", "never_started"], [row.queue_class for row in first])
        concurrent = claim_mal_public_userrecs_sources(self.db_path, limit=2, claim_token="worker-b")
        self.assertEqual([1, 4], [row.source_mal_anime_id for row in concurrent])
        self.assertTrue({row.source_mal_anime_id for row in first}.isdisjoint(
            {row.source_mal_anime_id for row in concurrent}
        ))
        for row in concurrent:
            self.assertTrue(release_mal_public_userrecs_source_claim(
                self.db_path,
                source_mal_anime_id=row.source_mal_anime_id,
                claim_token="worker-b",
                queue_class=row.queue_class,
                outcome="deferred",
            ))

        for row in first:
            self.assertTrue(release_mal_public_userrecs_source_claim(
                self.db_path,
                source_mal_anime_id=row.source_mal_anime_id,
                claim_token="worker-a",
                queue_class="resumable",
                outcome="paused",
            ))
        second = claim_mal_public_userrecs_sources(self.db_path, limit=1, claim_token="worker-b")
        self.assertEqual([2], [row.source_mal_anime_id for row in second])

    def test_empty_publish_requires_explicit_terminal_evidence(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="empty",
            next_url=None,
            edges=[],
        )
        with self.assertRaisesRegex(ValueError, "terminal-empty proof"):
            mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=generation.generation_id)

        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="empty",
            next_url=None,
            edges=[],
            terminal_evidence={
                "terminal": True, "explicit_empty": True, "document_complete": True,
                "recommendation_surface": True, "recommendation_row_count": 0,
                "next_candidate_count": 0, "next_links_consistent": True,
            },
        )
        ready = mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=generation.generation_id)
        self.assertTrue(ready.terminal_evidence["explicit_empty"])

    def test_fast_same_class_claims_rotate_by_monotonic_sequence(self) -> None:
        sync_mal_public_userrecs_source_queue(
            self.db_path, source_mal_anime_ids=[1, 2, 3],
            due_classes={1: "resumable", 2: "resumable", 3: "resumable"},
        )
        selected: list[int] = []
        sequences: list[int] = []
        for index in range(4):
            token = f"fast-{index}"
            row = claim_mal_public_userrecs_sources(
                self.db_path, limit=1, claim_token=token, claim_seconds=60
            )[0]
            selected.append(row.source_mal_anime_id)
            sequences.append(row.selection_sequence)
            self.assertTrue(release_mal_public_userrecs_source_claim(
                self.db_path, source_mal_anime_id=row.source_mal_anime_id,
                claim_token=token, queue_class="resumable", outcome="fast_rotation",
            ))
        self.assertEqual([1, 2, 3, 1], selected)
        self.assertEqual(sorted(sequences), sequences)
        self.assertEqual(4, len(set(sequences)))

    def test_repeated_drift_is_bounded_and_quarantines_without_new_generation(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL
        )
        for index in range(db.PUBLIC_USERRECS_MAX_DRIFT_RESTARTS):
            generation = restart_mal_public_userrecs_generation_after_drift(
                self.db_path, generation_id=generation.generation_id, reason=f"drift-{index}"
            )
            self.assertIsNone(generation.quarantined_at)
        quarantined = restart_mal_public_userrecs_generation_after_drift(
            self.db_path, generation_id=generation.generation_id, reason="drift-livelock"
        )
        self.assertEqual("failed", quarantined.status)
        self.assertIsNotNone(quarantined.quarantined_at)
        self.assertEqual("drift-livelock", quarantined.quarantine_reason)
        self.assertIsNone(get_active_mal_public_userrecs_generation(self.db_path, source_mal_anime_id=1))

    def test_claim_fence_rejects_expired_and_stale_worker_mutations(self) -> None:
        sync_mal_public_userrecs_source_queue(
            self.db_path, source_mal_anime_ids=[1], due_classes={1: "never_started"}
        )
        claimed = claim_mal_public_userrecs_sources(
            self.db_path, limit=1, claim_token="worker-a", claim_seconds=60
        )[0]
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL, claim_token="worker-a"
        )
        renewed = renew_mal_public_userrecs_source_claim(
            self.db_path, source_mal_anime_id=1, generation_id=generation.generation_id,
            claim_token="worker-a", expected_revision=generation.generation_revision, claim_seconds=60,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path, generation_id=renewed.generation_id, page_number=1,
            page_url=SOURCE_URL, page_fingerprint="fp", next_url=PAGE2_URL,
            edges=[_edge(10, "Ten", 1)], claim_token="worker-a",
            expected_revision=renewed.generation_revision,
        )
        current = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        with self.assertRaisesRegex(RuntimeError, "stale claim token"):
            pause_mal_public_userrecs_generation(
                self.db_path, generation_id=generation.generation_id, cursor_url=PAGE2_URL,
                claim_token="worker-b", expected_revision=current.generation_revision,
            )
        with connect(self.db_path) as conn:
            conn.execute("UPDATE mal_public_userrecs_source_queue SET claim_expires_at = '2000-01-01' WHERE source_mal_anime_id = 1")
            conn.execute("UPDATE mal_public_userrecs_crawl_generations SET claim_expires_at = '2000-01-01' WHERE generation_id = ?", (generation.generation_id,))
            conn.commit()
        with self.assertRaisesRegex(RuntimeError, "claim expired"):
            pause_mal_public_userrecs_generation(
                self.db_path, generation_id=generation.generation_id, cursor_url=PAGE2_URL,
                claim_token="worker-a", expected_revision=current.generation_revision,
            )
        self.assertEqual("active", get_mal_public_userrecs_generation(
            self.db_path, generation_id=generation.generation_id
        ).status)

    def test_expired_generation_lease_rebinds_to_reclaimer_and_fences_old_worker(self) -> None:
        sync_mal_public_userrecs_source_queue(
            self.db_path, source_mal_anime_ids=[1], due_classes={1: "never_started"}
        )
        claim_mal_public_userrecs_sources(
            self.db_path, limit=1, claim_token="worker-a", claim_seconds=60
        )
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_title="Seed",
            source_url=SOURCE_URL, claim_token="worker-a",
        )
        paused = pause_mal_public_userrecs_generation(
            self.db_path, generation_id=generation.generation_id, cursor_url=SOURCE_URL,
            claim_token="worker-a", expected_revision=generation.generation_revision,
        )
        identity = (
            paused.generation_id, paused.source_mal_anime_id, paused.source_title,
            paused.source_url, paused.status, paused.generation_key, paused.staged_revision,
        )
        with connect(self.db_path) as conn:
            conn.execute(
                "UPDATE mal_public_userrecs_source_queue SET claim_expires_at = '2000-01-01' WHERE source_mal_anime_id = 1"
            )
            conn.execute(
                "UPDATE mal_public_userrecs_crawl_generations SET claim_expires_at = '2000-01-01' WHERE generation_id = ?",
                (generation.generation_id,),
            )
            conn.commit()

        reclaimed = claim_mal_public_userrecs_sources(
            self.db_path, limit=1, claim_token="worker-b", claim_seconds=60
        )
        self.assertEqual([1], [row.source_mal_anime_id for row in reclaimed])
        rebound = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_title="ignored",
            source_url=PAGE2_URL, claim_token="worker-b",
        )
        self.assertEqual(identity, (
            rebound.generation_id, rebound.source_mal_anime_id, rebound.source_title,
            rebound.source_url, rebound.status, rebound.generation_key, rebound.staged_revision,
        ))
        self.assertEqual(paused.generation_revision + 1, rebound.generation_revision)
        resumed = resume_mal_public_userrecs_generation(
            self.db_path, generation_id=rebound.generation_id, claim_token="worker-b",
            expected_revision=rebound.generation_revision,
        )
        progressed = replace_mal_public_userrecs_staged_page(
            self.db_path, generation_id=resumed.generation_id, page_number=1,
            page_url=SOURCE_URL, page_fingerprint="reclaimed", next_url=None,
            edges=[_edge(10, "Ten", 1)], claim_token="worker-b",
            expected_revision=resumed.generation_revision,
        )
        self.assertEqual(1, progressed.edge_count)
        with self.assertRaisesRegex(RuntimeError, "stale claim token"):
            pause_mal_public_userrecs_generation(
                self.db_path, generation_id=generation.generation_id, cursor_url=PAGE2_URL,
                claim_token="worker-a", expected_revision=paused.generation_revision,
            )
        with self.assertRaisesRegex(RuntimeError, "stale claim token"):
            release_mal_public_userrecs_source_claim(
                self.db_path, source_mal_anime_id=1, claim_token="worker-a",
                queue_class="resumable", outcome="stale-release",
                generation_id=generation.generation_id,
            )
        current = get_mal_public_userrecs_generation(
            self.db_path, generation_id=generation.generation_id
        )
        ready = mark_mal_public_userrecs_generation_ready(
            self.db_path, generation_id=generation.generation_id,
            claim_token="worker-b", expected_revision=current.generation_revision,
        )
        with self.assertRaisesRegex(RuntimeError, "stale claim token"):
            publish_mal_public_userrecs_generation(
                self.db_path, generation_id=generation.generation_id,
                claim_token="worker-a", expected_revision=ready.generation_revision,
            )
        with connect(self.db_path) as conn:
            event_types = [str(row["event_type"]) for row in conn.execute(
                "SELECT event_type FROM mal_public_userrecs_claim_events WHERE generation_id = ? ORDER BY id",
                (generation.generation_id,),
            )]
            open_count = int(conn.execute(
                "SELECT COUNT(*) AS count FROM mal_public_userrecs_crawl_generations WHERE source_mal_anime_id = 1 AND status IN ('active', 'paused', 'ready')"
            ).fetchone()["count"])
        self.assertIn("rebind", event_types)
        self.assertEqual(1, open_count)

    def test_live_generation_lease_denies_premature_takeover(self) -> None:
        sync_mal_public_userrecs_source_queue(
            self.db_path, source_mal_anime_ids=[1], due_classes={1: "never_started"}
        )
        claim_mal_public_userrecs_sources(
            self.db_path, limit=1, claim_token="worker-a", claim_seconds=60
        )
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL, claim_token="worker-a"
        )
        with connect(self.db_path) as conn:
            conn.execute(
                "UPDATE mal_public_userrecs_source_queue SET claim_token = 'worker-b', claim_expires_at = datetime('now', '+60 seconds') WHERE source_mal_anime_id = 1"
            )
            conn.commit()
        with self.assertRaisesRegex(RuntimeError, "prior generation lease is still live"):
            create_or_get_active_mal_public_userrecs_generation(
                self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL, claim_token="worker-b"
            )
        current = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertEqual("worker-a", current.claim_token)
        self.assertEqual(generation.generation_revision, current.generation_revision)

    def test_two_direct_reclaimers_cannot_both_claim_and_bind_generation(self) -> None:
        sync_mal_public_userrecs_source_queue(
            self.db_path, source_mal_anime_ids=[1], due_classes={1: "never_started"}
        )
        claim_mal_public_userrecs_sources(self.db_path, limit=1, claim_token="worker-a", claim_seconds=60)
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL, claim_token="worker-a"
        )
        with connect(self.db_path) as conn:
            conn.execute("UPDATE mal_public_userrecs_source_queue SET claim_expires_at = '2000-01-01' WHERE source_mal_anime_id = 1")
            conn.execute(
                "UPDATE mal_public_userrecs_crawl_generations SET claim_expires_at = '2000-01-01' WHERE generation_id = ?",
                (generation.generation_id,),
            )
            conn.commit()

        def reclaim(token: str) -> tuple[str, bool]:
            claimed = claim_mal_public_userrecs_sources(
                self.db_path, limit=1, claim_token=token, claim_seconds=60
            )
            if not claimed:
                return token, False
            rebound = create_or_get_active_mal_public_userrecs_generation(
                self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL, claim_token=token
            )
            return token, rebound.claim_token == token

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(reclaim, ("worker-b", "worker-c")))
        winners = [token for token, bound in results if bound]
        self.assertEqual(1, len(winners))
        current = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertEqual(winners[0], current.claim_token)
        with connect(self.db_path) as conn:
            open_count = int(conn.execute(
                "SELECT COUNT(*) AS count FROM mal_public_userrecs_crawl_generations WHERE source_mal_anime_id = 1 AND status IN ('active', 'paused', 'ready')"
            ).fetchone()["count"])
        self.assertEqual(1, open_count)

    def test_retry_schedule_updates_generation_and_queue_atomically(self) -> None:
        sync_mal_public_userrecs_source_queue(
            self.db_path, source_mal_anime_ids=[1], due_classes={1: "never_started"}
        )
        claim_mal_public_userrecs_sources(self.db_path, limit=1, claim_token="worker", claim_seconds=60)
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL, claim_token="worker"
        )
        paused = pause_mal_public_userrecs_generation(
            self.db_path, generation_id=generation.generation_id, cursor_url=SOURCE_URL,
            claim_token="worker", expected_revision=generation.generation_revision,
        )
        retry_at = "2999-01-01T00:00:00Z"
        scheduled = schedule_mal_public_userrecs_generation_retry(
            self.db_path, generation_id=generation.generation_id,
            retry_class="transient_fetch_failure", next_retry_at=retry_at,
            claim_token="worker", expected_revision=paused.generation_revision,
        )
        with connect(self.db_path) as conn:
            queue = conn.execute("SELECT * FROM mal_public_userrecs_source_queue WHERE source_mal_anime_id = 1").fetchone()
        self.assertEqual(retry_at, scheduled.next_retry_at)
        self.assertEqual("retry_due", queue["queue_class"])
        self.assertEqual(retry_at, queue["next_retry_at"])
        self.assertEqual("transient_fetch_failure", queue["last_error_code"])

    def test_nonadjacent_page_overlap_is_rejected(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path, source_mal_anime_id=1, source_url=SOURCE_URL
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path, generation_id=generation.generation_id, page_number=1,
            page_url=SOURCE_URL, page_fingerprint="one", next_url=PAGE2_URL,
            edges=[_edge(10, "Ten", 1)],
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path, generation_id=generation.generation_id, page_number=2,
            page_url=PAGE2_URL, page_fingerprint="two", next_url=PAGE3_URL,
            edges=[_edge(20, "Twenty", 1, page_url=PAGE2_URL)],
        )
        with self.assertRaisesRegex(ValueError, "overlaps prior generation pages"):
            replace_mal_public_userrecs_staged_page(
                self.db_path, generation_id=generation.generation_id, page_number=3,
                page_url=PAGE3_URL, page_fingerprint="three", next_url=None,
                edges=[_edge(10, "Ten again", 2, page_url=PAGE3_URL)],
            )

    def test_page_replacement_and_pause_resume_cursor_persistence(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        page = replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-1",
            anchor={"target_mal_anime_ids": [10, 11], "ignored_free_prose": "do not persist"},
            next_url=PAGE2_URL,
            edges=[_edge(10, "Ten", 3), _edge(11, "Eleven", 2)],
            fetched_at=FETCHED_AT,
        )
        self.assertEqual(2, page.edge_count)
        self.assertEqual({"target_mal_anime_ids": [10, 11]}, {"target_mal_anime_ids": page.anchor["target_mal_anime_ids"]})
        self.assertNotIn("ignored_free_prose", page.anchor)

        paused = pause_mal_public_userrecs_generation(
            self.db_path,
            generation_id=generation.generation_id,
            cursor_url=PAGE2_URL,
            error="rate limit",
        )
        self.assertEqual("paused", paused.status)
        self.assertEqual(PAGE2_URL, paused.cursor_url)
        resumed = resume_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertEqual("active", resumed.status)
        self.assertEqual(PAGE2_URL, resumed.cursor_url)

        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-1b",
            next_url=PAGE3_URL,
            edges=[_edge(12, "Twelve", 9), _edge(12, "Lower duplicate", 1)],
            fetched_at=FETCHED_AT,
        )
        current = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertIsNotNone(current)
        self.assertEqual(PAGE3_URL, current.cursor_url)
        self.assertEqual(1, current.pages_fetched)
        self.assertEqual(1, current.staged_edge_count)
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT target_mal_anime_id, num_recommendations, raw_json, provenance_json
                FROM mal_public_userrecs_staged_edges
                WHERE generation_id = ?
                ORDER BY target_mal_anime_id
                """,
                (generation.generation_id,),
            ).fetchall()
        self.assertEqual([12], [int(row["target_mal_anime_id"]) for row in rows])
        self.assertEqual([9], [int(row["num_recommendations"]) for row in rows])
        raw = json.loads(rows[0]["raw_json"])
        provenance = json.loads(rows[0]["provenance_json"])
        self.assertEqual("public_mal_userrecs", raw["source"])
        self.assertNotIn("unsafe_free_prose", raw)
        self.assertNotIn("unsafe_user", provenance)

    def test_replacing_non_final_staged_page_keeps_generation_cursor_on_final_page(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-1",
            next_url=PAGE2_URL,
            edges=[_edge(10, "Ten", 1)],
            fetched_at=FETCHED_AT,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=2,
            page_url=PAGE2_URL,
            page_fingerprint="fp-2",
            next_url=None,
            edges=[_edge(20, "Twenty", 2, page_url=PAGE2_URL)],
            fetched_at=FETCHED_AT,
        )

        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-1-replaced",
            next_url=PAGE2_URL,
            edges=[_edge(11, "Eleven", 3)],
            fetched_at=FETCHED_AT,
        )

        current = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertIsNotNone(current)
        self.assertEqual(2, current.pages_fetched)
        self.assertEqual(2, current.staged_edge_count)
        self.assertIsNone(current.cursor_url)
        self.assertEqual(PAGE2_URL, current.last_page_url)
        self.assertEqual("fp-2", current.last_page_fingerprint)
        ready = mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=generation.generation_id)
        self.assertEqual("ready", ready.status)

    def test_drift_restart_discards_old_generation_and_keeps_single_open_generation(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-before-drift",
            next_url=PAGE2_URL,
            edges=[_edge(10, "Ten", 1)],
            fetched_at=FETCHED_AT,
        )

        restarted = restart_mal_public_userrecs_generation_after_drift(
            self.db_path,
            generation_id=generation.generation_id,
            reason="fingerprint changed",
        )

        old = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        active = get_active_mal_public_userrecs_generation(self.db_path, source_mal_anime_id=1)
        self.assertIsNotNone(old)
        self.assertEqual("discarded", old.status)
        self.assertEqual("fingerprint changed", old.last_error)
        self.assertEqual(restarted.generation_id, active.generation_id)
        self.assertNotEqual(generation.generation_id, restarted.generation_id)
        self.assertEqual(SOURCE_URL, restarted.cursor_url)

    def test_explicit_discard_closes_generation_and_audit_is_bounded_and_sanitized(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        long_error = "x" * 1205
        for index in range(105):
            pause_mal_public_userrecs_generation(
                self.db_path,
                generation_id=generation.generation_id,
                cursor_url=f"{SOURCE_URL}?p={index + 2}",
                error=f"{long_error}-{index}",
            )
            resume_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)

        discarded = discard_mal_public_userrecs_generation(
            self.db_path,
            generation_id=generation.generation_id,
            reason=long_error,
        )

        self.assertEqual("discarded", discarded.status)
        self.assertEqual(1000, len(discarded.last_error))
        self.assertIsNone(get_active_mal_public_userrecs_generation(self.db_path, source_mal_anime_id=1))
        with self.assertRaisesRegex(ValueError, "status"):
            publish_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        with connect(self.db_path) as conn:
            audit = conn.execute(
                """
                SELECT COUNT(*) AS event_count, MAX(LENGTH(error)) AS max_error_length
                FROM mal_public_userrecs_crawl_events
                WHERE source_mal_anime_id = 1
                """
            ).fetchone()
        self.assertLessEqual(int(audit["event_count"]), db._PUBLIC_USERRECS_EVENT_LIMIT_PER_SOURCE)
        self.assertEqual(db._PUBLIC_USERRECS_EVENT_LIMIT_PER_SOURCE, int(audit["event_count"]))
        self.assertEqual(1000, int(audit["max_error_length"]))

    def test_mark_ready_requires_contiguous_terminal_coherent_staging(self) -> None:
        missing_first_page = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=missing_first_page.generation_id,
            page_number=2,
            page_url=PAGE2_URL,
            page_fingerprint="fp-2",
            next_url=None,
            edges=[_edge(10, "Ten", 1, page_url=PAGE2_URL)],
            fetched_at=FETCHED_AT,
        )
        with self.assertRaisesRegex(ValueError, "contiguous"):
            mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=missing_first_page.generation_id)
        self.assertEqual(
            "active",
            get_mal_public_userrecs_generation(self.db_path, generation_id=missing_first_page.generation_id).status,
        )

        nonterminal = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=2,
            source_title="Other",
            source_url=SOURCE_URL.replace("/1/", "/2/"),
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=nonterminal.generation_id,
            page_number=1,
            page_url=SOURCE_URL.replace("/1/", "/2/"),
            page_fingerprint="fp-1",
            next_url=PAGE2_URL.replace("/1/", "/2/"),
            edges=[_edge(20, "Twenty", 1)],
            fetched_at=FETCHED_AT,
        )
        with self.assertRaisesRegex(ValueError, "cursor"):
            mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=nonterminal.generation_id)
        self.assertEqual(
            "active",
            get_mal_public_userrecs_generation(self.db_path, generation_id=nonterminal.generation_id).status,
        )

    def test_mark_ready_rejects_stored_next_link_chain_incoherence(self) -> None:
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-1",
            next_url=PAGE3_URL,
            edges=[_edge(10, "Ten", 1)],
            fetched_at=FETCHED_AT,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=2,
            page_url=PAGE2_URL,
            page_fingerprint="fp-2",
            next_url=None,
            edges=[_edge(20, "Twenty", 1, page_url=PAGE2_URL)],
            fetched_at=FETCHED_AT,
        )

        with self.assertRaisesRegex(ValueError, "next-link chain"):
            mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=generation.generation_id)
        self.assertEqual(
            "active",
            get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id).status,
        )

    def test_staging_and_rejected_publish_preserve_existing_published_rows_and_status(self) -> None:
        self._seed_published_official_detail()
        before_rows = self._published_rows()
        before_status = self._harvest_status()
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-terminal",
            next_url=None,
            edges=[_edge(10, "Ten", 6)],
            fetched_at=FETCHED_AT,
        )

        self.assertEqual(before_rows, self._published_rows())
        self.assertEqual(before_status, self._harvest_status())
        with self.assertRaisesRegex(ValueError, "status"):
            publish_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertEqual(before_rows, self._published_rows())
        self.assertEqual(before_status, self._harvest_status())

        with connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE mal_public_userrecs_crawl_generations
                SET status = 'ready', completed_at = CURRENT_TIMESTAMP, cursor_url = ?
                WHERE generation_id = ?
                """,
                (PAGE2_URL, generation.generation_id),
            )
            conn.commit()
        with self.assertRaisesRegex(ValueError, "cursor"):
            publish_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertEqual(before_rows, self._published_rows())
        self.assertEqual(before_status, self._harvest_status())

    def test_ready_generation_publish_is_atomic_updates_status_and_aggregates_duplicate_targets(self) -> None:
        self._seed_published_official_detail()
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-1",
            next_url=PAGE2_URL,
            edges=[_edge(10, "Ten", 2), _edge(20, "Twenty", 8)],
            fetched_at=FETCHED_AT,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=2,
            page_url=PAGE2_URL,
            page_fingerprint="fp-2",
            next_url=None,
            edges=[_edge(11, "Eleven later", 5, page_url=PAGE2_URL), _edge(30, "Thirty", 1, page_url=PAGE2_URL)],
            fetched_at=FETCHED_AT,
        )
        ready = mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=generation.generation_id)
        self.assertEqual("ready", ready.status)

        result = publish_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)

        self.assertEqual(4, result.published_edge_count)
        rows = self._published_rows()
        self.assertEqual([10, 11, 20, 30], [int(row["target_mal_anime_id"]) for row in rows])
        counts = {int(row["target_mal_anime_id"]): int(row["num_recommendations"]) for row in rows}
        self.assertEqual({10: 2, 11: 5, 20: 8, 30: 1}, counts)
        self.assertTrue(all(row["harvest_source"] == MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS for row in rows))
        self.assertTrue(all(int(row["complete_harvest"]) == 1 for row in rows))
        provenance = json.loads(rows[0]["provenance_json"])
        self.assertEqual("public_mal_userrecs", provenance["source"])
        self.assertEqual(generation.generation_id, provenance["generation_id"])
        status = self._harvest_status()
        self.assertEqual("fetched", status["status"])
        self.assertEqual(4, status["num_edges"])
        self.assertEqual(MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS, status["source_type"])
        self.assertEqual(1, status["is_complete"])
        self.assertEqual(2, status["pages_fetched"])
        self.assertEqual(SOURCE_URL, status["source_url"])
        published = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertEqual("published", published.status)

        downgraded = replace_mal_recommendation_edges(
            self.db_path,
            source_mal_anime_id=1,
            hop_distance=1,
            edges=[{"target_mal_anime_id": 99, "target_title": "Official only", "num_recommendations": 1, "raw": {}}],
            source_type=MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
            complete=False,
        )
        self.assertFalse(downgraded)
        self.assertEqual(rows, self._published_rows())

    def test_publish_failure_rolls_back_and_preserves_prior_rows_status_and_ready_generation(self) -> None:
        self._seed_published_official_detail()
        before_rows = self._published_rows()
        before_status = self._harvest_status()
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=1,
            source_title="Cowboy Bebop",
            source_url=SOURCE_URL,
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL,
            page_fingerprint="fp-1",
            next_url=None,
            edges=[_edge(10, "Ten", 7)],
            fetched_at=FETCHED_AT,
        )
        mark_mal_public_userrecs_generation_ready(self.db_path, generation_id=generation.generation_id)
        real_execute = db._execute_public_userrecs_publication_statement

        def fail_on_edge_insert(conn: sqlite3.Connection, statement: str, params: tuple[object, ...] = ()) -> sqlite3.Cursor:
            if "INSERT INTO mal_anime_recommendations" in statement:
                raise RuntimeError("injected publish failure")
            return real_execute(conn, statement, params)

        with mock.patch.object(db, "_execute_public_userrecs_publication_statement", side_effect=fail_on_edge_insert):
            with self.assertRaisesRegex(RuntimeError, "injected publish failure"):
                publish_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)

        self.assertEqual(before_rows, self._published_rows())
        self.assertEqual(before_status, self._harvest_status())
        still_ready = get_mal_public_userrecs_generation(self.db_path, generation_id=generation.generation_id)
        self.assertEqual("ready", still_ready.status)

    def test_public_userrecs_diagnostics_are_read_only_aggregate_and_sanitized(self) -> None:
        with connect(self.db_path) as conn:
            for anime_id, status in ((1, "completed"), (2, "watching")):
                conn.execute(
                    """
                    INSERT INTO mal_user_anime_list_cache (
                        mal_anime_id, title, list_status, node_json, list_status_json, raw_json,
                        refresh_run_id, refresh_generation, fetched_at, last_seen_at
                    ) VALUES (?, ?, ?, '{}', '{}', '{}', 'test-list', 1, '2999-01-01T00:00:00Z', '2999-01-01T00:00:00Z')
                    """,
                    (anime_id, f"Sensitive Seed {anime_id}", status),
                )
            conn.commit()
        replace_mal_recommendation_edges(
            self.db_path,
            source_mal_anime_id=1,
            hop_distance=1,
            edges=[{"target_mal_anime_id": 10, "target_title": "Target", "num_recommendations": 3, "raw": {}}],
            source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
            complete=True,
            pages_fetched=2,
        )
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.db_path,
            source_mal_anime_id=2,
            source_title="Sensitive Seed 2",
            source_url=SOURCE_URL.replace("/1/", "/2/"),
        )
        replace_mal_public_userrecs_staged_page(
            self.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=SOURCE_URL.replace("/1/", "/2/"),
            page_fingerprint="fp-1",
            next_url=PAGE2_URL.replace("/1/", "/2/"),
            edges=[_edge(20, "Twenty", 5)],
            fetched_at="2999-01-01T00:00:00Z",
        )
        pause_mal_public_userrecs_generation(
            self.db_path,
            generation_id=generation.generation_id,
            cursor_url=PAGE2_URL.replace("/1/", "/2/"),
            error="rate limit from https://example.invalid/with/raw/url",
        )

        snapshot = get_public_userrecs_diagnostics(
            self.db_path,
            configured_source_titles_per_hour=2,
            max_pages_per_source_per_run=3,
            stale_after_days=45,
        )

        self.assertEqual("degraded", snapshot["status"])
        self.assertEqual(2, snapshot["policy"]["authorized_source_titles_per_hour"])
        self.assertEqual(2, snapshot["policy"]["configured_source_titles_per_hour"])
        self.assertEqual(3, snapshot["policy"]["max_pages_per_source_per_run"])
        self.assertEqual(45, snapshot["policy"]["stale_horizon_days"])
        self.assertEqual(2, snapshot["positive_seed_count"])
        self.assertEqual(1, snapshot["coverage"]["complete"])
        self.assertEqual(1, snapshot["coverage"]["fresh"])
        self.assertEqual(1, snapshot["coverage"]["unharvested"])
        self.assertEqual(0.5, snapshot["coverage"]["fresh_ratio"])
        self.assertEqual(1, snapshot["open_generations"]["paused"])
        self.assertEqual(1, snapshot["open_generations"]["total"])
        self.assertEqual(1, snapshot["open_generations"]["staged_pages"])
        self.assertEqual(1, snapshot["open_generations"]["staged_edges"])
        self.assertEqual(1, snapshot["hourly_throughput"]["pages_fetched_last_hour"])
        self.assertEqual(1, snapshot["backlog"]["due_sources"])
        self.assertIsNone(snapshot["backlog"]["completion_eta_hours"])
        self.assertEqual(["unknown_pages_per_source"], snapshot["backlog"]["completion_eta_reason_codes"])
        self.assertEqual("rate_limited", snapshot["errors"]["last_error"]["code"])
        encoded = json.dumps(snapshot)
        self.assertNotIn("example.invalid", encoded)
        self.assertNotIn("Sensitive Seed", encoded)

    def test_public_userrecs_diagnostics_degrade_when_staging_schema_is_absent(self) -> None:
        with connect(self.db_path) as conn:
            conn.execute("DROP TABLE mal_public_userrecs_crawl_events")
            conn.execute("DROP TABLE mal_public_userrecs_staged_edges")
            conn.execute("DROP TABLE mal_public_userrecs_staged_pages")
            conn.execute("DROP TABLE mal_public_userrecs_crawl_generations")
            conn.commit()

        snapshot = get_public_userrecs_diagnostics(self.db_path)

        self.assertEqual("unknown", snapshot["status"])
        self.assertIn("public_userrecs_staging_schema_absent", snapshot["reason_codes"])
        self.assertEqual(["unknown_pages_per_source"], snapshot["backlog"]["completion_eta_reason_codes"])


if __name__ == "__main__":
    unittest.main()
