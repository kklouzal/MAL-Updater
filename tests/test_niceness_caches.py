from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, connect, upsert_mal_anime_detail_cache
from mal_updater.mal_client import MAL_DETAIL_CACHE_LOGIC_VERSION, MalClient


class NicenessCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "settings.toml").write_text("[mal]\nsearch_cache_ttl_days=14\nsearch_negative_cache_ttl_days=3\n", encoding="utf-8")
        self.config = load_config(self.root)
        bootstrap_database(self.config.db_path)
        self.client = MalClient(self.config, type("Secrets", (), {"client_id": "x", "access_token": None})())

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_migration_adds_additive_cache_tables_and_columns(self) -> None:
        with connect(self.config.db_path) as conn:
            tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            provider_columns = {row["name"] for row in conn.execute("PRAGMA table_info(provider_title_search_cache)")}
            evidence_columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_provider_eligibility_evidence)")}
        self.assertTrue({"mal_anime_search_cache", "mal_anime_detail_cache", "provider_enriched_detail_cache"} <= tables)
        self.assertTrue({"logic_version", "search_limit", "identity_key"} <= provider_columns)
        self.assertTrue({"refresh_status", "failure_count", "next_retry_at", "logic_version"} <= evidence_columns)

    def test_search_positive_and_negative_results_are_persisted_and_force_bypasses(self) -> None:
        with patch.object(self.client, "_get_json", return_value={"data": []}) as get:
            self.assertEqual({"data": []}, self.client.search_anime("  Example  ", limit=5))
            self.assertEqual({"data": []}, self.client.search_anime("Example", limit=5))
            self.client.search_anime("Example", limit=5, force_refresh=True)
        self.assertEqual(2, get.call_count)
        with connect(self.config.db_path) as conn:
            row = conn.execute("SELECT status FROM mal_anime_search_cache").fetchone()
        self.assertEqual("negative", row["status"])

    def test_corrupt_search_json_is_ignored_and_refetched(self) -> None:
        with patch.object(self.client, "_get_json", return_value={"data": []}) as get:
            self.client.search_anime("Broken")
            with connect(self.config.db_path) as conn:
                conn.execute("UPDATE mal_anime_search_cache SET response_json='not-json'")
                conn.commit()
            self.client.search_anime("Broken")
        self.assertEqual(2, get.call_count)

    def test_covering_detail_cache_prevents_narrow_duplicate_get_but_not_incomplete_use(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        upsert_mal_anime_detail_cache(self.config.db_path, mal_anime_id=7,
            fields_key="id,num_episodes,title", logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
            response={"id": 7, "title": "Cached", "num_episodes": 12}, fetched_at=now,
            expires_at="2999-01-01T00:00:00Z")
        with patch.object(self.client, "_get_json", return_value={"id": 7, "title": "Network", "status": "finished_airing"}) as get:
            self.assertEqual("Cached", self.client.get_anime_details(7, fields="id,title")["title"])
            self.assertEqual("Network", self.client.get_anime_details(7, fields="id,title,status")["title"])
        self.assertEqual(1, get.call_count)


if __name__ == "__main__":
    unittest.main()
