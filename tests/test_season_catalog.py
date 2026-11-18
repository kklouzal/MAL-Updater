from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from mal_updater.config import load_config
from mal_updater.crunchyroll_snapshot import _season_catalog_from_episodes
from mal_updater.db import bootstrap_database
from mal_updater.hidive_snapshot import _season_catalog_from_pages
from mal_updater.season_catalog import ProviderSeasonCatalog, season_keys_needing_catalog, upsert_provider_season_catalog


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
    def test_only_missing_or_recent_seasons_are_refetched(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            for key, last in (("old", "2020-01-01"), ("airing", "2026-11-10")):
                upsert_provider_season_catalog(config.db_path, ProviderSeasonCatalog(
                    provider="crunchyroll", provider_season_key=key, title=None, numbered_episode_count=12,
                    total_episode_count=12, first_date="2019-10-01", last_date=last, date_kind="original_air",
                    complete=True, fetched_at=""))
            soon = datetime(2026, 11, 20, tzinfo=timezone.utc)
            later = datetime(2026, 12, 20, tzinfo=timezone.utc)
            keys = ["old", "airing", "new"]
            self.assertEqual(["new"], season_keys_needing_catalog(config.db_path, provider="crunchyroll", season_keys=keys, now=soon))
            # An airing season is re-read once its row is a week old.
            self.assertEqual(["airing", "new"], season_keys_needing_catalog(config.db_path, provider="crunchyroll", season_keys=keys, now=later))


if __name__ == "__main__":
    unittest.main()
