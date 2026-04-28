from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

from mal_updater.config import AppConfig, MalSecrets, MalSettings
from mal_updater.mal_client import MalClient


class _JsonResponse:
    status = 200

    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> "_JsonResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


class MalClientTests(unittest.TestCase):
    def _config(self, root: Path) -> AppConfig:
        runtime_root = root / ".MAL-Updater"
        return AppConfig(
            project_root=root,
            workspace_root=root,
            runtime_root=runtime_root,
            settings_path=runtime_root / "config" / "settings.toml",
            config_dir=runtime_root / "config",
            secrets_dir=runtime_root / "secrets",
            data_dir=runtime_root / "data",
            state_dir=runtime_root / "state",
            cache_dir=runtime_root / "cache",
            db_path=runtime_root / "data" / "mal_updater.sqlite3",
            mal=MalSettings(request_spacing_seconds=0.0, request_spacing_jitter_seconds=0.0),
        )

    def test_search_anime_strips_dub_noise_before_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token=None,
                    refresh_token=None,
                    client_id_path=config.secrets_dir / "mal_client_id.txt",
                    client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                    access_token_path=config.secrets_dir / "mal_access_token.txt",
                    refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
                ),
            )
            requested_urls: list[str] = []

            def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
                requested_urls.append(request.full_url)
                return _JsonResponse({"data": []})

            with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                client.search_anime("Sword Art Online the Movie -Progressive- Scherzo of Deep Night (English Dub)")

            query = parse_qs(urlparse(requested_urls[0]).query)["q"][0]
            self.assertEqual(query, "Sword Art Online the Movie -Progressive- Scherzo of Deep Night")
            self.assertNotIn("Dub", query)

    def test_search_anime_strips_broader_provider_audio_noise(self) -> None:
        cases = {
            "Example Show (Spanish Dub)": "Example Show",
            "Example Show [German Dub]": "Example Show",
            "Example Show (Latin American Spanish Dub)": "Example Show",
            "Example Show (English Sub)": "Example Show",
            "Example Show - Portuguese Dub": "Example Show",
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token=None,
                    refresh_token=None,
                    client_id_path=config.secrets_dir / "mal_client_id.txt",
                    client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                    access_token_path=config.secrets_dir / "mal_access_token.txt",
                    refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
                ),
            )
            requested_urls: list[str] = []

            def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
                requested_urls.append(request.full_url)
                return _JsonResponse({"data": []})

            with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                for query in cases:
                    client.search_anime(query)

            sanitized_queries = [parse_qs(urlparse(url).query)["q"][0] for url in requested_urls]
            self.assertEqual(sanitized_queries, list(cases.values()))


if __name__ == "__main__":
    unittest.main()
