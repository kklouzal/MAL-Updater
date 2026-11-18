from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mal_updater.config import load_config
from mal_updater.crunchyroll_snapshot import _season_catalog_from_episodes
from mal_updater.db import bootstrap_database
from mal_updater.hidive_snapshot import _season_catalog_from_pages
from mal_updater.season_catalog import (
    ProviderSeasonCatalog,
    get_provider_season_catalog,
    record_provider_season_catalog_failure,
    season_keys_needing_catalog,
    upsert_provider_season_catalog,
)


class SeasonCatalogParsingTests(unittest.TestCase):
    def test_crunchyroll_listing_counts_numbered_episodes_and_original_air_window(self) -> None:
        # Shape of /content/v2/cms/seasons/{id}/episodes (Attack on Titan Final Season).
        payload = {"total": 4, "data": [
            {"episode": "60", "episode_air_date": "2020-12-07T00:00:00+09:00", "season_title": "Final Season"},
            {"episode": "87", "episode_air_date": "2021-03-29T00:00:00+09:00"},
            {"episode": "SP1", "episode_air_date": "2023-03-04T00:00:00+09:00"},
            {"episode": "SP2", "episode_air_date": "2023-11-05T00:00:00+09:00"},
        ]}
        entry = _season_catalog_from_episodes("G69", payload)
        self.assertEqual((2, 4), (entry.numbered_episode_count, entry.total_episode_count))
        self.assertEqual(("2020-12-07", "2021-03-29"), (entry.first_date, entry.last_date))
        self.assertEqual("original_air", entry.date_kind)
        self.assertTrue(entry.complete)

    def test_hidive_pages_use_provider_publish_dates(self) -> None:
        pages = [{"title": "Season 1", "episodeCount": 2, "episodes": [
            {"episodeInformation": {"episodeNumber": 1}, "publishedDate": "2024-01-25T23:10:31Z"},
            {"episodeInformation": {"episodeNumber": 2}, "publishedDate": "2024-02-01T23:10:31Z"},
        ]}]
        entry = _season_catalog_from_pages("20439", pages)
        self.assertEqual("provider_publish", entry.date_kind)
        self.assertEqual(("2024-01-25", "2024-02-01", 2), (entry.first_date, entry.last_date, entry.numbered_episode_count))


class SeasonCatalogRefreshSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        (root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(root)
        bootstrap_database(self.config.db_path)
        # Rows are stamped with the database clock, so "now" is relative to it.
        self.today = datetime.now(timezone.utc)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _needing(self, keys: list[str], *, days_ahead: int) -> list[str]:
        return season_keys_needing_catalog(
            self.config.db_path, provider="crunchyroll", season_keys=keys, now=self.today + timedelta(days=days_ahead)
        )

    def test_only_missing_or_recent_seasons_are_refetched(self) -> None:
        recent_last = (self.today - timedelta(days=8)).strftime("%Y-%m-%d")
        for key, last in (("old", "2020-01-01"), ("airing", recent_last)):
            upsert_provider_season_catalog(self.config.db_path, ProviderSeasonCatalog(
                provider="crunchyroll", provider_season_key=key, title=None, numbered_episode_count=12,
                total_episode_count=12, first_date="2019-10-01", last_date=last, date_kind="original_air",
                complete=True, fetched_at=""))
        keys = ["old", "airing", "new"]
        self.assertEqual(["new"], self._needing(keys, days_ahead=2))
        # An airing season is re-read once its row is a week old.
        self.assertEqual(["airing", "new"], self._needing(keys, days_ahead=8))

    def test_failed_read_defers_the_season_and_keeps_existing_facts(self) -> None:
        upsert_provider_season_catalog(self.config.db_path, ProviderSeasonCatalog(
            provider="crunchyroll", provider_season_key="kept", title="T", numbered_episode_count=12,
            total_episode_count=12, first_date="2019-10-01", last_date=None, date_kind="original_air",
            complete=True, fetched_at=""))
        for key in ("kept", "gone"):
            record_provider_season_catalog_failure(
                self.config.db_path, provider="crunchyroll", provider_season_key=key, date_kind="original_air"
            )
        self.assertEqual([], self._needing(["kept", "gone"], days_ahead=2))
        self.assertEqual(["kept", "gone"], self._needing(["kept", "gone"], days_ahead=8))
        kept = get_provider_season_catalog(self.config.db_path, provider="crunchyroll", provider_season_key="kept")
        gone = get_provider_season_catalog(self.config.db_path, provider="crunchyroll", provider_season_key="gone")
        assert kept is not None and gone is not None
        self.assertEqual(("2019-10-01", 12, True), (kept.first_date, kept.numbered_episode_count, kept.complete))
        self.assertEqual((None, None, False), (gone.first_date, gone.numbered_episode_count, gone.complete))

if __name__ == "__main__":
    unittest.main()
