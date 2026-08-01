from __future__ import annotations

import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from mal_updater import db
from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    replace_mal_user_anime_list_cache_generation,
    upsert_mal_anime_metadata,
    upsert_recommendation_provider_eligibility_evidence,
    upsert_series_mapping,
)
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.recommendations import build_recommendations
from mal_updater.sync_planner import build_dry_run_sync_plan, execute_approved_sync, load_provider_series_states
from tests.test_validation_ingestion import sample_snapshot


@contextmanager
def isolated_config():
    with tempfile.TemporaryDirectory(dir="/tmp") as td:
        root = Path(td)
        runtime_root = root / ".MAL-Updater"
        settings_path = runtime_root / "config" / "settings.toml"
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        with patch.dict(
            os.environ,
            {
                "MAL_UPDATER_RUNTIME_ROOT": str(runtime_root),
                "MAL_UPDATER_SETTINGS_PATH": str(settings_path),
                "TMPDIR": "/tmp",
            },
        ):
            yield root, load_config(root)


def _insert_catalog_only_provider_series(config, *, provider_series_id: str, title: str = "Catalog Only") -> None:
    bootstrap_database(config.db_path)
    with connect(config.db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_series(
                provider, provider_series_id, title, season_title, raw_json,
                last_seen_at, catalog_observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "crunchyroll",
                provider_series_id,
                title,
                title,
                json.dumps({"source": "catalog"}, sort_keys=True),
                "2026-01-01T00:00:00Z",
                "2026-01-02T00:00:00Z",
            ),
        )
        conn.commit()


class ProviderSeriesObservationProvenanceTests(unittest.TestCase):
    def test_catalog_only_row_is_excluded_from_sync_plans_until_account_snapshot_promotes_it(self) -> None:
        with isolated_config() as (_root, config):
            _insert_catalog_only_provider_series(config, provider_series_id="series-123", title="Catalog Title")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=123,
                confidence=1.0,
                mapping_source="auto_exact",
                approved_by_user=True,
                notes=None,
            )

            self.assertEqual([], load_provider_series_states(config, limit=0))
            with patch("mal_updater.sync_planner.MalClient.search_anime", side_effect=AssertionError("catalog-only dry-run must not search MAL")) as search_mock, patch(
                "mal_updater.sync_planner.MalClient.get_anime_details",
                side_effect=AssertionError("catalog-only dry-run must not load MAL details"),
            ) as details_mock:
                self.assertEqual(
                    [],
                    build_dry_run_sync_plan(
                        config,
                        limit=0,
                        approved_mappings_only=True,
                        exact_approved_only=True,
                    ),
                )
                self.assertEqual([], execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=True))
            search_mock.assert_not_called()
            details_mock.assert_not_called()

            payload = sample_snapshot()
            payload["series"][0]["title"] = "Account Title"
            ingest_snapshot_payload(payload, config)

            states = load_provider_series_states(config, limit=0)
            self.assertEqual(["series-123"], [state.provider_series_id for state in states])
            self.assertIsNotNone(states[0].account_observed_at)
            self.assertEqual("2026-01-02T00:00:00Z", states[0].catalog_observed_at)
            with patch(
                "mal_updater.sync_planner.MalClient.get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Account Title",
                    "num_episodes": 12,
                    "media_type": "tv",
                    "status": "finished_airing",
                    "my_list_status": None,
                    "alternative_titles": {},
                },
            ) as details_mock:
                proposals = build_dry_run_sync_plan(
                    config,
                    limit=0,
                    approved_mappings_only=True,
                    exact_approved_only=True,
                )
            self.assertEqual(1, len(proposals))
            self.assertEqual("series-123", proposals[0].provider_series_id)
            details_mock.assert_called_once()

    def test_linked_legacy_progress_and_watchlist_rows_remain_planner_visible_after_upgrade(self) -> None:
        with isolated_config() as (_root, config):
            original = db.MIGRATIONS
            migration_index = db.MIGRATION_FILENAMES.index(db.PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION)
            try:
                db.MIGRATIONS = original[:migration_index]
                bootstrap_database(config.db_path)
            finally:
                db.MIGRATIONS = original

            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, last_seen_at)
                    VALUES
                        ('crunchyroll', 'legacy-progress', 'Legacy Progress', '2026-01-01T00:00:00Z'),
                        ('crunchyroll', 'legacy-watchlist', 'Legacy Watchlist', '2026-01-02T00:00:00Z')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO provider_episode_progress(
                        provider, provider_episode_id, provider_series_id,
                        episode_number, completion_ratio, raw_json, last_seen_at
                    ) VALUES (
                        'crunchyroll', 'legacy-episode', 'legacy-progress',
                        1, 1.0, '{}', '2026-01-03T00:00:00Z'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO provider_watchlist(
                        provider, provider_series_id, list_id, provider_item_id,
                        status, raw_json, last_seen_at
                    ) VALUES (
                        'crunchyroll', 'legacy-watchlist', 'default', 'legacy-watchlist',
                        'watching', '{}', '2026-01-04T00:00:00Z'
                    )
                    """
                )
                conn.commit()

            bootstrap_database(config.db_path)

            states = load_provider_series_states(config, limit=0)
            self.assertEqual({"legacy-progress", "legacy-watchlist"}, {state.provider_series_id for state in states})
            by_id = {state.provider_series_id: state for state in states}
            self.assertEqual("2026-01-03T00:00:00Z", by_id["legacy-progress"].account_observed_at)
            self.assertEqual("2026-01-04T00:00:00Z", by_id["legacy-watchlist"].account_observed_at)

    def test_catalog_only_provider_eligibility_evidence_remains_usable_for_recommendations(self) -> None:
        with isolated_config() as (_root, config):
            bootstrap_database(config.db_path)
            _insert_catalog_only_provider_series(config, provider_series_id="catalog-evidence", title="Catalog Evidence Provider")
            self.assertEqual([], load_provider_series_states(config, limit=0))
            upsert_mal_anime_metadata(
                config.db_path,
                mal_anime_id=100,
                title="Seed Show",
                title_english="Seed Show",
                title_japanese=None,
                alternative_titles=[],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=8.0,
                popularity=100,
                start_season={"year": 2023, "season": "spring"},
                raw={"id": 100, "title": "Seed Show", "my_list_status": {"status": "completed", "score": 8}},
            )
            upsert_mal_anime_metadata(
                config.db_path,
                mal_anime_id=200,
                title="Catalog Evidence Target",
                title_english="Catalog Evidence Target",
                title_japanese=None,
                alternative_titles=[],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=7.5,
                popularity=500,
                start_season={"year": 2024, "season": "summer"},
                raw={"id": 200, "title": "Catalog Evidence Target"},
            )
            replace_mal_user_anime_list_cache_generation(
                config.db_path,
                items=[
                    {
                        "node": {"id": 100, "title": "Seed Show"},
                        "list_status": {"status": "completed", "score": 8, "num_episodes_watched": 12},
                    }
                ],
                refresh_run_id="seed-cache",
                fetched_at="2026-07-19T00:00:00Z",
                prune_absent=False,
            )
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO mal_anime_recommendations(
                        source_mal_anime_id, target_mal_anime_id, target_title,
                        num_recommendations, hop_distance, source_kind, raw_json,
                        fetched_at
                    ) VALUES (100, 200, 'Catalog Evidence Target', 12, 1, 'mal_recommendation', '{}', '2026-07-19T00:00:00Z')
                    """
                )
                conn.commit()
            upsert_recommendation_provider_eligibility_evidence(
                config.db_path,
                mal_anime_id=200,
                provider="crunchyroll",
                provider_series_id="catalog-evidence",
                provider_title="Catalog Evidence Provider",
                provider_url="https://example.test/catalog-evidence",
                identity_match_kind="provider_title_search_exact",
                match_confidence=0.9,
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                explicit_dub_evidence_source="provider_audio_locale",
                audio_locales=["en-US", "ja-JP"],
                source_evidence={"source": "catalog_only_test"},
                fetched_at="2026-07-19T00:00:00Z",
                expires_at="2099-01-01T00:00:00Z",
                last_verified_at="2026-07-19T00:00:00Z",
            )

            recommendations = build_recommendations(config, limit=0, require_provider_availability=True)

            matching = [
                item
                for item in recommendations
                if item.kind == "discovery_candidate" and item.context.get("mal_anime_id") == 200
            ]
            self.assertEqual(1, len(matching))
            item = matching[0]
            self.assertEqual("crunchyroll", item.provider)
            self.assertEqual("catalog-evidence", item.provider_series_id)
            self.assertTrue(item.context["watch_now_eligible"])
            self.assertEqual(["crunchyroll"], item.context["available_via_providers"])
            self.assertEqual("catalog_only_test", item.context["provider_eligibility_evidence"][0]["source_evidence"]["source"])


if __name__ == "__main__":
    unittest.main()
