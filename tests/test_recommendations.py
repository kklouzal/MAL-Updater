from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, connect
from mal_updater.recommendations import build_recommendations


class RecommendationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert_series(
        self,
        provider_series_id: str,
        *,
        title: str,
        season_title: str | None = None,
        season_number: int | None = None,
        watchlist_status: str | None = None,
    ) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_series (provider, provider_series_id, title, season_title, season_number, raw_json)
                VALUES ('crunchyroll', ?, ?, ?, ?, '{}')
                """,
                (provider_series_id, title, season_title, season_number),
            )
            if watchlist_status is not None:
                conn.execute(
                    """
                    INSERT INTO provider_watchlist (provider, provider_series_id, status, raw_json)
                    VALUES ('crunchyroll', ?, ?, '{}')
                    """,
                    (provider_series_id, watchlist_status),
                )
            conn.commit()

    def _insert_progress(
        self,
        provider_series_id: str,
        provider_episode_id: str,
        *,
        episode_number: int,
        completion_ratio: float,
        last_watched_at: str,
    ) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_episode_progress (
                    provider,
                    provider_episode_id,
                    provider_series_id,
                    episode_number,
                    completion_ratio,
                    last_watched_at,
                    raw_json
                ) VALUES ('crunchyroll', ?, ?, ?, ?, ?, '{}')
                """,
                (provider_episode_id, provider_series_id, episode_number, completion_ratio, last_watched_at),
            )
            conn.commit()

    def test_new_dubbed_episode_recommendation_detects_unwatched_progress_gap(self) -> None:
        self._insert_series(
            "series-1",
            title="Example Show",
            season_title="Example Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="in_progress",
        )
        self._insert_progress("series-1", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-10T01:00:00Z")
        self._insert_progress("series-1", "ep-2", episode_number=2, completion_ratio=1.0, last_watched_at="2026-03-10T02:00:00Z")
        self._insert_progress("series-1", "ep-3", episode_number=3, completion_ratio=0.2, last_watched_at="2026-03-11T02:00:00Z")

        results = build_recommendations(self.config, limit=0)

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("new_dubbed_episode", item.kind)
        self.assertEqual("series-1", item.provider_series_id)
        self.assertEqual(1, item.context["unwatched_episode_count"])

    def test_new_season_recommendation_detects_completed_predecessor(self) -> None:
        self._insert_series(
            "season-1",
            title="Franchise Show",
            season_title="Franchise Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
        )
        self._insert_progress("season-1", "s1-ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_progress("season-1", "s1-ep-2", episode_number=2, completion_ratio=1.0, last_watched_at="2026-03-01T02:00:00Z")

        self._insert_series(
            "season-2",
            title="Franchise Show",
            season_title="Franchise Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="never_watched",
        )

        results = build_recommendations(self.config, limit=0)

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("new_season", item.kind)
        self.assertEqual("season-2", item.provider_series_id)
        self.assertEqual("season-1", item.context["predecessor_provider_series_id"])

    def test_filters_out_non_english_dub_candidates(self) -> None:
        self._insert_series(
            "series-fr",
            title="Foreign Dub Show",
            season_title="Foreign Dub Show (French Dub)",
            season_number=1,
            watchlist_status="in_progress",
        )
        self._insert_progress("series-fr", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-10T01:00:00Z")
        self._insert_progress("series-fr", "ep-2", episode_number=2, completion_ratio=0.1, last_watched_at="2026-03-10T02:00:00Z")

        results = build_recommendations(self.config, limit=0)

        self.assertEqual([], results)


if __name__ == "__main__":
    unittest.main()
