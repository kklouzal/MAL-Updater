from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import AppConfig, MalSecrets, MalSettings
from mal_updater.db import (
    bootstrap_database,
    get_provider_enriched_detail_cache,
    record_provider_enriched_detail_failure,
    upsert_mal_anime_search_cache,
    upsert_provider_enriched_detail_cache,
)
from mal_updater.mal_client import MAL_SEARCH_CACHE_LOGIC_VERSION, MalApiError, MalClient


class PeriodicCacheSwrTests(unittest.TestCase):
    def _config(self, root: Path) -> AppConfig:
        runtime = root / ".MAL-Updater"
        return AppConfig(
            project_root=root,
            workspace_root=root,
            runtime_root=runtime,
            settings_path=runtime / "config" / "settings.toml",
            config_dir=runtime / "config",
            secrets_dir=runtime / "secrets",
            data_dir=runtime / "data",
            state_dir=runtime / "state",
            cache_dir=runtime / "cache",
            db_path=runtime / "data" / "cache.sqlite3",
            mal=MalSettings(request_spacing_seconds=0, request_spacing_jitter_seconds=0),
        )

    def _secrets(self, config: AppConfig) -> MalSecrets:
        return MalSecrets(
            client_id="id",
            client_secret=None,
            access_token=None,
            refresh_token=None,
            client_id_path=config.secrets_dir / "mal_client_id.txt",
            client_secret_path=config.secrets_dir / "mal_client_secret.txt",
            access_token_path=config.secrets_dir / "mal_access_token.txt",
            refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
        )

    def test_due_positive_search_refresh_failure_preserves_last_known_good_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            bootstrap_database(config.db_path)
            client = MalClient(config, self._secrets(config))
            query = "Example"
            import hashlib, json
            key = hashlib.sha256(json.dumps([
                MAL_SEARCH_CACHE_LOGIC_VERSION, query.casefold(), 5,
                "alternative_titles,id,media_type,num_episodes,status,title",
            ], separators=(",", ":")).encode()).hexdigest()
            upsert_mal_anime_search_cache(
                config.db_path,
                cache_key=key,
                normalized_query=query.casefold(),
                result_limit=5,
                fields="alternative_titles,id,media_type,num_episodes,status,title",
                logic_version=MAL_SEARCH_CACHE_LOGIC_VERSION,
                status="ok",
                response={"data": [{"node": {"id": 1, "title": "Example"}}]},
                fetched_at="2020-01-01T00:00:00Z",
                expires_at="2020-05-01T00:00:00Z",
            )
            with patch.object(client, "_get_json", side_effect=MalApiError("refresh failed")):
                with self.assertRaises(MalApiError):
                    client.search_anime(query)
            from mal_updater.db import get_mal_anime_search_cache
            retained = get_mal_anime_search_cache(config.db_path, cache_key=key)
            assert retained is not None
            self.assertEqual("ok", retained.status)
            self.assertEqual(1, retained.response["data"][0]["node"]["id"])

    def test_negative_search_remains_short_ttl_and_zero_disables_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.mal.search_negative_cache_ttl_days = 0
            client = MalClient(config, self._secrets(config))
            with patch.object(client, "_get_json", return_value={"data": []}) as fetch:
                client.search_anime("Absent")
                client.search_anime("Absent")
            self.assertEqual(2, fetch.call_count)

    def test_provider_detail_failure_keeps_prior_payload_and_success_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            bootstrap_database(config.db_path)
            upsert_provider_enriched_detail_cache(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-1",
                logic_version="crunchyroll-detail-v1",
                detail={"provider_series_id": "series-1", "audio_locales": ["en-US"]},
                fetched_at="2026-01-01T00:00:00Z",
                expires_at="2026-05-01T00:00:00Z",
            )
            record_provider_enriched_detail_failure(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-1",
                logic_version="crunchyroll-detail-v1",
                fetched_at="2027-01-01T00:00:00Z",
                next_retry_at="2027-01-01T01:00:00Z",
                expires_at="2027-01-08T00:00:00Z",
                error="temporary",
            )
            row = get_provider_enriched_detail_cache(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-1",
                logic_version="crunchyroll-detail-v1",
            )
            assert row is not None
            self.assertEqual("ok", row.status)
            self.assertEqual("2026-01-01T00:00:00Z", row.fetched_at)
            self.assertEqual(["en-US"], row.response["audio_locales"])
