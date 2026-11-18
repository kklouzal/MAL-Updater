from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import (
    MAL_USER_LIST_PAGINATION_LOGIC_VERSION,
    MalUserAnimeListRefreshConflictError,
    bootstrap_database,
    claim_or_create_mal_user_anime_list_traversal,
    connect,
    get_mal_user_anime_list_cache,
    list_mal_user_anime_list_cache,
    merge_mal_user_anime_list_cache_into_metadata,
    reconcile_mal_user_state_after_write,
    replace_mal_user_anime_list_cache_generation,
    upsert_mal_anime_metadata,
)
from mal_updater.mal_client import MalApiError
from mal_updater.recommendation_metadata import (
    MAL_USER_LIST_FIELDS,
    MAL_USER_LIST_MAX_DRIFT_ATTEMPTS_PER_RUN,
    _mal_user_list_initial_url,
    refresh_mal_user_anime_list_cache,
)

ACCOUNT = {"id": 7, "name": "owner"}


def _item(anime_id: int, *, picture_ext: str = "jpg", score: int = 0) -> dict[str, Any]:
    return {
        "node": {
            "id": anime_id,
            "title": f"Item {anime_id}",
            "main_picture": {
                "medium": f"https://cdn.myanimelist.net/images/anime/{anime_id}.{picture_ext}",
                "large": f"https://cdn.myanimelist.net/images/anime/{anime_id}l.{picture_ext}",
            },
            "num_episodes": 12,
            "media_type": "tv",
            "status": "finished_airing",
        },
        "list_status": {"status": "completed", "score": score, "num_episodes_watched": 12, "updated_at": "2026-01-01T00:00:00+00:00"},
    }


class MalUserListRefreshDriftTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        (root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(root)
        bootstrap_database(self.config.db_path)
        replace_mal_user_anime_list_cache_generation(
            self.config.db_path, items=[_item(9999)], refresh_run_id="lkg", fetched_at="2026-08-01T00:00:00Z",
            prune_absent=True,
        )
        self.sleep = patch("mal_updater.recommendation_metadata.time.sleep").start()
        self.addCleanup(patch.stopall)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _run(self, responses: Any, **kwargs: Any):
        requested: list[str] = []

        def get_page(url: str) -> dict:
            requested.append(url)
            response = responses(len(requested)) if callable(responses) else responses
            if isinstance(response, Exception):
                raise response
            return response

        with patch("mal_updater.recommendation_metadata.MalClient.get_my_user", return_value=ACCOUNT), patch(
            "mal_updater.recommendation_metadata.MalClient.get_my_anime_list_page_url", side_effect=get_page
        ):
            summary = refresh_mal_user_anime_list_cache(self.config, max_pages=kwargs.pop("max_pages", 16), **kwargs)
        return summary, requested

    def _latest_generation(self) -> dict[str, Any]:
        with connect(self.config.db_path) as conn:
            return dict(conn.execute(
                "SELECT * FROM mal_user_anime_list_refresh_generations WHERE account_id IS NOT NULL ORDER BY generation DESC LIMIT 1"
            ).fetchone())

    def test_285_item_list_is_one_limit_1000_request_plus_validation(self) -> None:
        payload = {"data": [_item(i) for i in range(1, 286)], "paging": {}}
        summary, requested = self._run(payload, prune_on_complete=True)
        self.assertEqual("ok", summary.status)
        url = _mal_user_list_initial_url(self.config, status=None, page_size=1000)
        self.assertIn("limit=1000", url)
        self.assertEqual([url, url], requested)  # page fetch + page-1/boundary revalidation
        self.assertEqual(285, len(list_mal_user_anime_list_cache(self.config.db_path)))

    def test_main_picture_jpg_webp_flip_is_not_drift_and_is_not_retained(self) -> None:
        def responses(n: int) -> dict:
            ext = "jpg" if n % 2 else "webp"
            return {"data": [_item(i, picture_ext=ext) for i in range(1, 286)], "paging": {}}

        summary, requested = self._run(responses, prune_on_complete=True)
        self.assertEqual("ok", summary.status)
        self.assertEqual(2, len(requested))
        entry = get_mal_user_anime_list_cache(self.config.db_path, 1)
        assert entry is not None
        self.assertNotIn("main_picture", entry.node)
        self.assertNotIn("main_picture", json.dumps(entry.raw))
        self.assertEqual({"id", "title", "num_episodes", "media_type", "status"}, set(entry.node))

    def test_real_change_drift_retries_in_run_then_succeeds(self) -> None:
        def responses(n: int) -> dict:
            # Score edit lands between the first fetch and its revalidation only.
            return {"data": [_item(i, score=7 if n == 1 and i == 5 else 8 if i == 5 else 0) for i in range(1, 286)], "paging": {}}

        summary, requested = self._run(responses, prune_on_complete=True)
        self.assertEqual("ok", summary.status)
        self.assertEqual(4, len(requested))
        self.assertEqual(8, get_mal_user_anime_list_cache(self.config.db_path, 5).user_score)
        self.sleep.assert_called_once()

    def test_persistent_drift_is_bounded_in_run_and_never_quarantines(self) -> None:
        def drifting(n: int) -> dict:
            return {"data": [_item(i, score=n % 10 if i == 5 else 0) for i in range(1, 286)], "paging": {}}

        summary, requested = self._run(drifting, prune_on_complete=True)
        self.assertEqual("partial", summary.status)
        self.assertEqual(2 * MAL_USER_LIST_MAX_DRIFT_ATTEMPTS_PER_RUN, len(requested))
        self.assertIn("drifted in this run", summary.error or "")
        latest = self._latest_generation()
        self.assertEqual("active", latest["status"])
        self.assertIsNone(latest["quarantined_at"])
        self.assertIsNone(latest["claim_token"])
        # Published LKG untouched.
        self.assertEqual([9999], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.config.db_path)])
        # Repeated drifting runs stay non-terminal (no cross-run drift quarantine).
        for _ in range(3):
            summary, _ = self._run(drifting, prune_on_complete=True)
            self.assertEqual("partial", summary.status)
        stable = {"data": [_item(i) for i in range(1, 286)], "paging": {}}
        summary, _ = self._run(stable, prune_on_complete=True)
        self.assertEqual("ok", summary.status)
        self.assertEqual(285, len(list_mal_user_anime_list_cache(self.config.db_path)))

    def test_contract_violation_quarantines_but_auth_failure_does_not(self) -> None:
        summary, _ = self._run(MalApiError("MAL API GET /users/@me/animelist failed: HTTP 401"))
        self.assertEqual("partial", summary.status)
        self.assertIsNone(self._latest_generation()["quarantined_at"])
        bad = {"data": [{"node": {"id": 1, "title": "x"}, "list_status": {"status": "rewatching"}}], "paging": {}}
        summary, _ = self._run(bad)
        self.assertEqual("failed", summary.status)
        latest = self._latest_generation()
        self.assertIsNotNone(latest["quarantined_at"])
        self.assertEqual("contract_violation", latest["quarantine_reason"])
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "explicit safe reinitialization"):
            self._run({"data": [], "paging": {}})

    def _seed_legacy_v3_quarantine(self, reason: str) -> int:
        legacy_query = {"fields": MAL_USER_LIST_FIELDS, "logic_version": "mal-user-list-pagination-v3", "page_size": 100, "statuses": ["all"]}
        text = json.dumps(legacy_query, sort_keys=True, separators=(",", ":"))
        identity = hashlib.sha256(text.encode()).hexdigest()
        generation, _ = claim_or_create_mal_user_anime_list_traversal(
            self.config.db_path, account_id=7, account_name="owner", query_identity=identity, query=legacy_query,
            partitions=[{"partition_key": "all", "requested_status": None, "ordinal": 0, "initial_url": "https://api.myanimelist.net/v2/users/@me/animelist?limit=100"}],
            claim_token="legacy", fetched_at="2026-09-01T00:00:00Z",
        )
        with connect(self.config.db_path) as conn:
            conn.execute(
                """UPDATE mal_user_anime_list_refresh_generations SET logic_version='mal-user-list-pagination-v3',
                   status='failed', quarantined_at='2026-09-01 16:33:30', quarantine_reason=?, error=?, restart_count=3,
                   drift_count=3, claim_token=NULL, claim_expires_at=NULL WHERE generation=?""",
                (reason, reason, generation.generation),
            )
            conn.commit()
        return generation.generation

    def test_legacy_v3_drift_quarantine_resumes_without_operator_action(self) -> None:
        legacy = self._seed_legacy_v3_quarantine("MAL list snapshot drift during validate_interior")
        summary, _ = self._run({"data": [_item(i) for i in range(1, 286)], "paging": {}}, prune_on_complete=True)
        self.assertEqual("ok", summary.status)
        self.assertGreater(summary.generation, legacy)
        self.assertEqual(MAL_USER_LIST_PAGINATION_LOGIC_VERSION, summary.traversal["logic_version"])
        self.assertEqual(0, summary.traversal["restart_count"])

    def test_legacy_v3_contract_quarantine_still_requires_reinitialization(self) -> None:
        self._seed_legacy_v3_quarantine("contract_violation")
        with self.assertRaisesRegex(MalUserAnimeListRefreshConflictError, "explicit safe reinitialization"):
            self._run({"data": [_item(1)], "paging": {}})


class MalUserStateReconcileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "db.sqlite3"
        bootstrap_database(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _metadata(self, anime_id: int, my_list_status: dict | None) -> None:
        raw: dict[str, Any] = {"id": anime_id, "title": f"Show {anime_id}"}
        if my_list_status is not None:
            raw["my_list_status"] = my_list_status
        upsert_mal_anime_metadata(
            self.db_path, mal_anime_id=anime_id, title=f"Show {anime_id}", title_english=None, title_japanese=None,
            alternative_titles=[], media_type=None, status=None, num_episodes=None, mean=None, popularity=None,
            start_season=None, raw=raw,
        )

    def _my_list_status(self, anime_id: int) -> Any:
        with connect(self.db_path) as conn:
            row = conn.execute("SELECT raw_json FROM mal_anime_metadata WHERE mal_anime_id=?", (anime_id,)).fetchone()
        return json.loads(row["raw_json"]).get("my_list_status", "absent")

    def test_confirmed_write_for_new_entry_inserts_cache_row_and_metadata_state(self) -> None:
        self._metadata(42, None)
        reconcile_mal_user_state_after_write(
            self.db_path, mal_anime_id=42, list_status={"status": "watching", "num_episodes_watched": 3, "score": 0},
        )
        entry = get_mal_user_anime_list_cache(self.db_path, 42)
        assert entry is not None
        self.assertEqual(("Show 42", "watching", 3), (entry.title, entry.list_status, entry.num_episodes_watched))
        self.assertEqual("watching", self._my_list_status(42)["status"])
        self.assertEqual(3, self._my_list_status(42)["num_episodes_watched"])

    def test_existing_entry_update_keeps_metadata_coherent(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.db_path, items=[{"node": {"id": 42, "title": "Show 42"}, "list_status": {"status": "watching", "num_episodes_watched": 2}}],
            refresh_run_id="lkg", fetched_at="2026-08-01T00:00:00Z", prune_absent=True,
        )
        self._metadata(42, {"status": "watching", "num_episodes_watched": 2})
        reconcile_mal_user_state_after_write(self.db_path, mal_anime_id=42, list_status={"status": "completed", "num_episodes_watched": 12})
        self.assertEqual(("completed", 12), (self._my_list_status(42)["status"], self._my_list_status(42)["num_episodes_watched"]))

    def test_complete_publication_merge_clears_entries_removed_from_mal(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.db_path, items=[{"node": {"id": 1, "title": "Kept"}, "list_status": {"status": "completed"}}],
            refresh_run_id="lkg", fetched_at="2026-08-01T00:00:00Z", prune_absent=True,
        )
        self._metadata(1, {"status": "completed"})
        self._metadata(2, {"status": "watching"})
        merge_mal_user_anime_list_cache_into_metadata(self.db_path)
        self.assertEqual("watching", self._my_list_status(2)["status"])  # not cleared without complete proof
        merge_mal_user_anime_list_cache_into_metadata(self.db_path, clear_absent=True)
        self.assertIsNone(self._my_list_status(2))
        self.assertEqual("completed", self._my_list_status(1)["status"])


if __name__ == "__main__":
    unittest.main()
