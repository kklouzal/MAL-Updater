from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    get_provider_title_search_cache,
    list_covering_mal_anime_detail_cache_nodes,
    upsert_mal_anime_detail_cache,
    upsert_provider_title_search_cache,
)
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

    def test_provider_title_search_cache_full_key_variants_are_isolated(self) -> None:
        first = upsert_provider_title_search_cache(
            self.config.db_path,
            provider="crunchyroll",
            normalized_query="shared query",
            query="Shared Query",
            candidate_mal_anime_id=101,
            candidate_title="Candidate One",
            matches=[{"provider_series_id": "cr-one", "title": "One"}],
            status="ok",
            fetched_at="2026-07-30T00:00:00Z",
            expires_at="2027-07-30T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )
        upsert_provider_title_search_cache(
            self.config.db_path,
            provider="crunchyroll",
            normalized_query="shared query",
            query="Shared Query",
            candidate_mal_anime_id=202,
            candidate_title="Candidate Two",
            matches=[{"provider_series_id": "cr-two", "title": "Two"}],
            status="ok",
            fetched_at="2026-07-30T00:00:00Z",
            expires_at="2026-08-01T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:202",
        )
        upsert_provider_title_search_cache(
            self.config.db_path,
            provider="crunchyroll",
            normalized_query="shared query",
            query="Shared Query",
            candidate_mal_anime_id=303,
            candidate_title="Legacy Candidate",
            matches=[{"provider_series_id": "cr-legacy", "title": "Legacy"}],
            status="ok",
            fetched_at="2026-07-30T00:00:00Z",
            expires_at="2027-07-30T00:00:00Z",
        )
        self.assertEqual("cr-one", first.matches[0]["provider_series_id"])

        refreshed_first = upsert_provider_title_search_cache(
            self.config.db_path,
            provider="crunchyroll",
            normalized_query="shared query",
            query="Shared Query Refreshed",
            candidate_mal_anime_id=101,
            candidate_title="Candidate One Refreshed",
            matches=[{"provider_series_id": "cr-one-refreshed", "title": "One Refreshed"}],
            status="ok",
            fetched_at="2026-08-02T00:00:00Z",
            expires_at="2027-08-02T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )

        with connect(self.config.db_path) as conn:
            rows = conn.execute(
                """
                SELECT candidate_mal_anime_id, logic_version, search_limit, identity_key, matches_json
                FROM provider_title_search_cache
                WHERE provider = 'crunchyroll' AND normalized_query = 'shared query'
                ORDER BY logic_version ASC, search_limit ASC, identity_key ASC
                """
            ).fetchall()
        self.assertEqual(3, len(rows))
        self.assertEqual("cr-one-refreshed", refreshed_first.matches[0]["provider_series_id"])

        exact_first = get_provider_title_search_cache(
            self.config.db_path,
            provider="crunchyroll",
            normalized_query="shared query",
            now="2026-08-02T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )
        self.assertIsNotNone(exact_first)
        assert exact_first is not None
        self.assertEqual("cr-one-refreshed", exact_first.matches[0]["provider_series_id"])
        self.assertIsNone(
            get_provider_title_search_cache(
                self.config.db_path,
                provider="crunchyroll",
                normalized_query="shared query",
                now="2026-08-02T00:00:00Z",
                logic_version="provider-title-v2",
                search_limit=5,
                identity_key="mal:202",
            )
        )
        expired_without_now = get_provider_title_search_cache(
            self.config.db_path,
            provider="crunchyroll",
            normalized_query="shared query",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:202",
        )
        self.assertIsNotNone(expired_without_now)
        assert expired_without_now is not None
        self.assertEqual("cr-two", expired_without_now.matches[0]["provider_series_id"])
        legacy = get_provider_title_search_cache(
            self.config.db_path,
            provider="crunchyroll",
            normalized_query="shared query",
            legacy_lookup=True,
        )
        self.assertIsNotNone(legacy)
        assert legacy is not None
        self.assertEqual("legacy-v1", legacy.logic_version)
        self.assertEqual(10, legacy.search_limit)
        self.assertEqual("", legacy.identity_key)
        with self.assertRaisesRegex(ValueError, "full semantic key"):
            get_provider_title_search_cache(
                self.config.db_path,
                provider="crunchyroll",
                normalized_query="shared query",
            )
        with self.assertRaisesRegex(ValueError, "logic_version, search_limit, and identity_key together"):
            get_provider_title_search_cache(
                self.config.db_path,
                provider="crunchyroll",
                normalized_query="shared query",
                logic_version="provider-title-v2",
            )

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

    def test_covering_detail_cache_listing_dedupes_and_rejects_unusable_rows(self) -> None:
        required_fields = {"id", "title", "alternative_titles", "media_type", "status", "num_episodes", "start_season"}
        valid_node = {
            "id": 52736,
            "title": "Tensei Oujo to Tensai Reijou no Mahou Kakumei",
            "alternative_titles": {
                "en": "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady",
                "synonyms": [],
            },
            "media_type": "tv",
            "status": "finished_airing",
            "num_episodes": 12,
            "start_season": {"year": 2023, "season": "winter"},
        }
        fields_key = ",".join(sorted(required_fields))
        broader_fields_key = ",".join(sorted(required_fields | {"related_anime"}))
        broadest_fields_key = ",".join(sorted(required_fields | {"related_anime", "synopsis"}))
        rows = (
            (52736, broadest_fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", {**valid_node, "title": ""}, "2026-07-26T17:15:00Z", "2999-01-01T00:00:00Z"),
            (52736, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", valid_node, "2026-07-26T17:00:00Z", "2999-01-01T00:00:00Z"),
            (52736, f"{broader_fields_key},ranking", MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", {**valid_node, "title": "Older duplicate"}, "2026-07-26T16:00:00Z", "2999-01-01T00:00:00Z"),
            (1, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", {**valid_node, "id": 1}, "2026-07-26T17:00:00Z", "2026-07-26T17:00:00Z"),
            (2, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "failed", {**valid_node, "id": 2}, "2026-07-26T17:00:00Z", "2999-01-01T00:00:00Z"),
            (3, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", "not-json", "2026-07-26T17:00:00Z", "2999-01-01T00:00:00Z"),
            (4, "id,title", MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", {**valid_node, "id": 4}, "2026-07-26T17:00:00Z", "2999-01-01T00:00:00Z"),
            (5, fields_key, "old-logic", "ok", {**valid_node, "id": 5}, "2026-07-26T17:00:00Z", "2999-01-01T00:00:00Z"),
            (6, fields_key, MAL_DETAIL_CACHE_LOGIC_VERSION, "ok", {**valid_node, "id": 60}, "2026-07-26T17:00:00Z", "2999-01-01T00:00:00Z"),
        )
        with connect(self.config.db_path) as conn:
            for anime_id, fields_key, logic_version, status, response, fetched_at, expires_at in rows:
                response_json = response if isinstance(response, str) else json.dumps(response)
                conn.execute(
                    """
                    INSERT INTO mal_anime_detail_cache (
                        mal_anime_id, fields_key, logic_version, status, response_json, fetched_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (anime_id, fields_key, logic_version, status, response_json, fetched_at, expires_at),
                )
            conn.commit()

        nodes = list_covering_mal_anime_detail_cache_nodes(
            self.config.db_path,
            required_fields=required_fields,
            logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
            now="2026-07-26T17:30:00Z",
        )

        self.assertEqual([valid_node], nodes)


if __name__ == "__main__":
    unittest.main()
