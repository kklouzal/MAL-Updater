from __future__ import annotations

import base64
import io
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from socket import timeout as SocketTimeout
from urllib.error import HTTPError
from unittest.mock import patch

from mal_updater.config import AppConfig, MalSecrets, MalSettings, DEFAULT_MAL_BASE_URL
from mal_updater.db import bootstrap_database, upsert_mal_anime_detail_cache
from mal_updater.mal_client import MAL_DETAIL_CACHE_LOGIC_VERSION, MalApiError, MalClient


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


class _RawResponse(_JsonResponse):
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body


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

    def _secrets(
        self,
        config: AppConfig,
        *,
        access_token: str | None = None,
        refresh_token: str | None = None,
        client_secret: str | None = None,
    ) -> MalSecrets:
        return MalSecrets(
            client_id="client-id",
            client_secret=client_secret,
            access_token=access_token,
            refresh_token=refresh_token,
            client_id_path=config.secrets_dir / "mal_client_id.txt",
            client_secret_path=config.secrets_dir / "mal_client_secret.txt",
            access_token_path=config.secrets_dir / "mal_access_token.txt",
            refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
        )

    def test_get_anime_suggestions_is_one_bounded_authenticated_get(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config, access_token="access"))
            payload = {"data": [{"node": {"id": 123, "title": "Suggestion"}}], "paging": {"next": "ignored"}}
            with patch.object(client, "_get_json", return_value=payload) as get_json:
                self.assertEqual(payload, client.get_anime_suggestions(limit=1000, fields="id, title"))

            url = get_json.call_args.args[0]
            self.assertEqual("/anime/suggestions", urlparse(url).path)
            self.assertEqual(["100"], parse_qs(urlparse(url).query)["limit"])
            self.assertEqual(["id,title"], parse_qs(urlparse(url).query)["fields"])
            self.assertEqual("Bearer access", get_json.call_args.kwargs["headers"]["Authorization"])
            self.assertNotIn("offset", parse_qs(urlparse(url).query))

    def test_get_anime_suggestions_requires_oauth_and_rejects_bad_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            no_oauth = MalClient(config, self._secrets(config))
            with self.assertRaisesRegex(MalApiError, "access_token"):
                no_oauth.get_anime_suggestions()

            client = MalClient(config, self._secrets(config, access_token="access"))
            malformed = (
                {"data": {}},
                {"data": [], "paging": []},
                {"data": [{"node": {"id": "123", "title": "Bad"}}]},
                {"data": [{"node": {"id": 123, "title": ""}}]},
            )
            for payload in malformed:
                with self.subTest(payload=payload), patch.object(client, "_get_json", return_value=payload):
                    with self.assertRaises(MalApiError):
                        client.get_anime_suggestions()

    def test_token_exchange_and_refresh_failures_are_single_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.mal.retry_max_attempts = 5
            client = MalClient(config, MalSecrets(
                client_id="client-id", client_secret=None, access_token=None, refresh_token="refresh-token",
                client_id_path=config.secrets_dir / "mal_client_id.txt", client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                access_token_path=config.secrets_dir / "mal_access_token.txt", refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
            ))
            effects = [
                HTTPError(config.mal.token_url, 503, "busy", {}, io.BytesIO(b"busy")),
                SocketTimeout("ambiguous"),
            ]
            for effect, call in ((effects[0], lambda: client.exchange_code("one-shot-code", "verifier")), (effects[1], client.refresh_access_token)):
                with patch("mal_updater.mal_client.urlopen", side_effect=effect) as send:
                    with self.assertRaises(MalApiError):
                        call()
                self.assertEqual(1, send.call_count)

    def test_token_requests_use_mal_scheme_one_basic_auth_for_public_and_secret_clients(self) -> None:
        for client_secret in (None, "client-secret"):
            with self.subTest(client_secret=client_secret or "public-empty-password"):
                with tempfile.TemporaryDirectory() as tmp:
                    config = self._config(Path(tmp))
                    client = MalClient(config, self._secrets(config, refresh_token="refresh-token", client_secret=client_secret))
                    requests = []

                    def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
                        requests.append(request)
                        return _JsonResponse(
                            {
                                "access_token": "access-token",
                                "token_type": "Bearer",
                                "expires_in": 3600,
                                "refresh_token": "new-refresh-token",
                            }
                        )

                    with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                        client.exchange_code("auth-code", "verifier")
                        client.refresh_access_token()

                    expected_basic = base64.b64encode(f"client-id:{client_secret or ''}".encode("utf-8")).decode("ascii")
                    self.assertEqual(2, len(requests))
                    for request in requests:
                        self.assertEqual(f"Basic {expected_basic}", request.get_header("Authorization"))
                        body = parse_qs(request.data.decode("utf-8"))
                        self.assertEqual(["client-id"], body["client_id"])
                        self.assertNotIn("client_secret", body)

                    exchange_body = parse_qs(requests[0].data.decode("utf-8"))
                    self.assertEqual(["authorization_code"], exchange_body["grant_type"])
                    self.assertEqual(["auth-code"], exchange_body["code"])
                    self.assertEqual(["verifier"], exchange_body["code_verifier"])
                    self.assertEqual([config.mal.redirect_uri], exchange_body["redirect_uri"])

                    refresh_body = parse_qs(requests[1].data.decode("utf-8"))
                    self.assertEqual(["refresh_token"], refresh_body["grant_type"])
                    self.assertEqual(["refresh-token"], refresh_body["refresh_token"])

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

    def test_timeout_retry_records_each_failed_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=config.secrets_dir / "mal_client_id.txt",
                    client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                    access_token_path=config.secrets_dir / "mal_access_token.txt",
                    refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
                ),
            )
            with patch("mal_updater.mal_client.urlopen", side_effect=SocketTimeout("simulated timeout")):
                with self.assertRaisesRegex(MalApiError, "timeout after 2 attempts"):
                    client.get_my_user()

            events = [json.loads(line) for line in config.api_request_events_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(2, len(events))
            self.assertEqual(["timeout", "timeout"], [event["outcome"] for event in events])
            self.assertEqual(events[0]["attempt_sequence"] + 1, events[1]["attempt_sequence"])

    def test_get_retries_selected_5xx_and_honors_capped_retry_after(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.mal.retry_backoff_base_seconds = 0.0
            config.mal.retry_backoff_jitter_seconds = 0.0
            config.mal.retry_after_cap_seconds = 0.5
            client = MalClient(config, MalSecrets(
                client_id="client-id", client_secret=None, access_token="access-token", refresh_token=None,
                client_id_path=config.secrets_dir / "mal_client_id.txt", client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                access_token_path=config.secrets_dir / "mal_access_token.txt", refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
            ))
            error = HTTPError("https://example.invalid", 503, "busy", {"Retry-After": "10"}, io.BytesIO(b"busy"))
            with patch("mal_updater.mal_client.urlopen", side_effect=[error, _JsonResponse({"id": 1})]) as send, patch("mal_updater.mal_client.time.sleep") as sleep:
                payload = client.get_my_user()
            self.assertEqual({"id": 1}, payload)
            self.assertEqual(2, send.call_count)
            sleep.assert_called_once_with(0.5)

    def test_put_timeout_is_not_retried(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.mal.retry_max_attempts = 5
            client = MalClient(config, MalSecrets(
                client_id="client-id", client_secret=None, access_token="access-token", refresh_token=None,
                client_id_path=config.secrets_dir / "mal_client_id.txt", client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                access_token_path=config.secrets_dir / "mal_access_token.txt", refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
            ))
            with patch("mal_updater.mal_client.urlopen", side_effect=SocketTimeout("ambiguous write")) as send:
                with self.assertRaises(MalApiError):
                    client.update_my_list_status(1, status="watching", num_watched_episodes=1)
            self.assertEqual(1, send.call_count)

    def test_mal_write_error_does_not_reflect_upstream_response_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config, access_token="***"))
            error = HTTPError(
                "https://example.invalid",
                400,
                "bad request",
                {},
                io.BytesIO(b'password=*** Bearer secret-token'),
            )

            with patch("mal_updater.mal_client.urlopen", side_effect=error):
                with self.assertRaises(MalApiError) as raised:
                    client.update_my_list_status(53590, status="watching", num_watched_episodes=10)

            rendered = str(raised.exception)
            self.assertNotIn("hunter2", rendered)
            self.assertNotIn("secret-token", rendered)
            self.assertEqual("MAL API update my_list_status failed for anime_id=53590: HTTP 400", rendered)

    def test_update_my_list_status_rejects_empty_or_malformed_json_response(self) -> None:
        cases = (
            (b"", "empty response body"),
            (b"not-json password=hunter2", "malformed JSON response"),
            (b"[]", "JSON response must be an object"),
        )
        for body, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
                config = self._config(Path(tmp))
                client = MalClient(config, self._secrets(config, access_token="access-token"))
                with patch("mal_updater.mal_client.urlopen", return_value=_RawResponse(body)):
                    with self.assertRaisesRegex(MalApiError, expected) as raised:
                        client.update_my_list_status(53590, status="watching", num_watched_episodes=10)
                self.assertNotIn("hunter2", str(raised.exception))

    def test_live_user_detail_revalidation_requires_access_token_before_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config, access_token=None))

            with patch(
                "mal_updater.mal_client.urlopen",
                side_effect=AssertionError("authenticated live-state read must fail before network without a token"),
            ) as send:
                with self.assertRaisesRegex(MalApiError, "MAL access_token is not configured"):
                    client.get_anime_details(53590, force_refresh=True, require_user=True)

            send.assert_not_called()

    def test_public_anime_detail_leaves_omitted_user_state_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config))
            payload = {
                "id": 53590,
                "title": "Example",
                "start_season": None,
                "broadcast": None,
            }
            with patch.object(client, "_get_json", return_value=payload):
                actual = client.get_anime_details(
                    53590,
                    fields="id,title,start_season,broadcast,my_list_status",
                    force_refresh=True,
                )
            self.assertEqual(payload, actual)
            self.assertNotIn("my_list_status", actual)

    def test_anime_detail_canonicalizes_omitted_requested_list_status_as_unlisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config, access_token="access-token"))
            for anime_id in (5114, 54918):
                with self.subTest(anime_id=anime_id):
                    payload = {"id": anime_id, "title": "Not Yet Listed", "num_episodes": 12}
                    with patch.object(client, "_get_json", return_value=payload):
                        actual = client.get_anime_details(
                            anime_id,
                            fields="id,title,num_episodes,my_list_status",
                            force_refresh=True,
                            require_user=True,
                        )
                    self.assertIsNone(actual["my_list_status"])
                    self.assertNotIn("my_list_status", payload)

    def test_anime_detail_canonicalizes_omitted_nullable_rank_but_rejects_malformed_present_rank(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config))
            with patch.object(client, "_get_json", return_value={"id": 54915, "title": "No Rank"}):
                actual = client.get_anime_details(54915, fields="id,title,rank", force_refresh=True)
            self.assertIsNone(actual["rank"])
            for malformed in ("1", 0, -1, True, {}, []):
                with self.subTest(malformed=malformed), patch.object(
                    client, "_get_json", return_value={"id": 54915, "title": "Bad Rank", "rank": malformed}
                ):
                    with self.assertRaisesRegex(MalApiError, "malformed rank"):
                        client.get_anime_details(54915, fields="id,title,rank", force_refresh=True)

    def test_anime_detail_canonicalizes_omitted_broadcast_for_public_and_authenticated_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            for access_token, expected_user_state in ((None, False), ("***", True)):
                with self.subTest(authenticated=expected_user_state):
                    client = MalClient(config, self._secrets(config, access_token=access_token))
                    payload = {"id": 54915, "title": "No Broadcast"}
                    with patch.object(client, "_get_json", return_value=payload):
                        actual = client.get_anime_details(
                            54915,
                            fields="id,title,broadcast,my_list_status",
                            force_refresh=True,
                        )
                    self.assertIsNone(actual["broadcast"])
                    self.assertEqual(expected_user_state, "my_list_status" in actual)
                    if expected_user_state:
                        self.assertIsNone(actual["my_list_status"])
                    self.assertNotIn("broadcast", payload)
                    self.assertNotIn("my_list_status", payload)

    def test_public_detail_cache_preserves_unknown_user_state_and_nullable_catalog_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            bootstrap_database(config.db_path)
            client = MalClient(config, self._secrets(config))
            payload = {"id": 54915, "title": "Public"}
            with patch.object(client, "_get_json", return_value=payload) as get_json:
                first = client.get_anime_details(
                    54915, fields="id,title,broadcast,rank,my_list_status", force_refresh=True
                )
                second = client.get_anime_details(54915, fields="id,title,broadcast,rank,my_list_status")
            get_json.assert_called_once()
            for actual in (first, second):
                self.assertIsNone(actual["broadcast"])
                self.assertIsNone(actual["rank"])
                self.assertNotIn("my_list_status", actual)

    def test_covering_cache_without_broadcast_does_not_invent_null_for_an_unrequested_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            bootstrap_database(config.db_path)
            upsert_mal_anime_detail_cache(
                config.db_path,
                mal_anime_id=54915,
                fields_key="id,title",
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                response={"id": 54915, "title": "Nukitashi the Animation"},
                fetched_at="2999-01-01T00:00:00Z",
                expires_at="2999-01-02T00:00:00Z",
            )
            client = MalClient(config, self._secrets(config))
            with patch.object(
                client,
                "_get_json",
                return_value={"id": 54915, "title": "Nukitashi the Animation"},
            ) as get_json:
                actual = client.get_anime_details(54915, fields="id,title,broadcast")
            get_json.assert_called_once()
            self.assertIsNone(actual["broadcast"])

    def test_authenticated_detail_does_not_reuse_public_cache_without_user_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            bootstrap_database(config.db_path)
            fields_key = "broadcast,id,my_list_status,rank,title"
            upsert_mal_anime_detail_cache(
                config.db_path,
                mal_anime_id=54915,
                fields_key=fields_key,
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                response={"id": 54915, "title": "Public", "broadcast": None, "rank": None},
                fetched_at="2999-01-01T00:00:00Z",
                expires_at="2999-01-02T00:00:00Z",
            )
            client = MalClient(config, self._secrets(config, access_token="***"))
            with patch.object(client, "_get_json", return_value={"id": 54915, "title": "Authenticated"}) as get_json:
                actual = client.get_anime_details(54915, fields="id,title,broadcast,rank,my_list_status")
                cached = client.get_anime_details(54915, fields="id,title,broadcast,rank,my_list_status")
            get_json.assert_called_once()
            for response in (actual, cached):
                self.assertIsNone(response["broadcast"])
                self.assertIsNone(response["rank"])
                self.assertIsNone(response["my_list_status"])

    def test_anime_detail_rejects_missing_or_malformed_requested_contract_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config))
            with patch.object(client, "_get_json", return_value={"id": 53590, "title": "Example"}):
                with self.assertRaisesRegex(MalApiError, "lacks requested fields"):
                    client.get_anime_details(53590, fields="id,title,synopsis", force_refresh=True)
            for malformed in ("friday", [], 1, False):
                with self.subTest(malformed_broadcast=malformed), patch.object(
                    client,
                    "_get_json",
                    return_value={"id": 53590, "title": "Example", "broadcast": malformed},
                ):
                    with self.assertRaisesRegex(MalApiError, "malformed broadcast"):
                        client.get_anime_details(53590, fields="id,title,broadcast", force_refresh=True)
            with patch.object(client, "_get_json", return_value={"id": 53590, "title": "Example", "recommendations": None}):
                with self.assertRaisesRegex(MalApiError, "malformed recommendations"):
                    client.get_anime_details(53590, fields="id,title,recommendations", force_refresh=True)
            authenticated = MalClient(config, self._secrets(config, access_token="access"))
            with patch.object(
                authenticated,
                "_get_json",
                return_value={"id": 53590, "title": "Example", "my_list_status": "completed"},
            ):
                with self.assertRaisesRegex(MalApiError, "malformed my_list_status"):
                    authenticated.get_anime_details(
                        53590, fields="id,title,my_list_status", force_refresh=True
                    )

    def test_update_my_list_status_requires_nonblank_access_token_before_side_effects(self) -> None:
        for access_token in (None, "", " \t\n"):
            with self.subTest(access_token=access_token):
                with tempfile.TemporaryDirectory() as tmp:
                    config = self._config(Path(tmp))
                    client = MalClient(config, self._secrets(config, access_token=access_token))

                    with patch.object(client, "_pace_request", side_effect=AssertionError("pacing should not run without a token")) as pace, patch(
                        "mal_updater.mal_client.Request", side_effect=AssertionError("request should not be constructed without a token")
                    ) as build_request, patch(
                        "mal_updater.mal_client.urlopen", side_effect=AssertionError("urlopen should not run without a token")
                    ) as send, patch(
                        "mal_updater.mal_client.record_api_request_event", side_effect=AssertionError("telemetry should not be recorded without a token")
                    ) as telemetry:
                        with self.assertRaisesRegex(MalApiError, "MAL access_token is not configured"):
                            client.update_my_list_status(1, status="watching", num_watched_episodes=1)

                    pace.assert_not_called()
                    build_request.assert_not_called()
                    send.assert_not_called()
                    telemetry.assert_not_called()
                    self.assertFalse(config.api_request_events_path.exists())

    def test_update_my_list_status_uses_valid_access_token_for_put(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config, access_token="access-token"))
            requests = []

            def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
                requests.append(request)
                return _JsonResponse({"status": "watching", "num_episodes_watched": 3})

            with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                payload = client.update_my_list_status(99, status="watching", num_watched_episodes=3)

            self.assertEqual({"status": "watching", "num_episodes_watched": 3}, payload)
            self.assertEqual(1, len(requests))
            request = requests[0]
            self.assertEqual("PUT", request.get_method())
            self.assertEqual(f"{config.mal.base_url}/anime/99/my_list_status", request.full_url)
            self.assertEqual("Bearer access-token", request.get_header("Authorization"))
            self.assertNotEqual("Bearer None", request.get_header("Authorization"))
            self.assertEqual("application/x-www-form-urlencoded", request.get_header("Content-type"))
            self.assertEqual({"status": ["watching"], "num_watched_episodes": ["3"]}, parse_qs(request.data.decode("utf-8")))

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

    def test_search_anime_skips_queries_over_mal_limit(self) -> None:
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

            with patch("mal_updater.mal_client.urlopen", side_effect=AssertionError("invalid long query should not be sent")):
                result = client.search_anime(
                    "The Magical Revolution of the Reincarnated Princess and the Genius Young Lady"
                )

            self.assertEqual({"data": []}, result)

    def test_my_anime_list_paginates_with_user_auth_and_clamps_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=config.secrets_dir / "mal_client_id.txt",
                    client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                    access_token_path=config.secrets_dir / "mal_access_token.txt",
                    refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
                ),
            )
            requested_urls: list[str] = []
            requested_auth: list[str | None] = []

            def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
                requested_urls.append(request.full_url)
                requested_auth.append(request.headers.get("Authorization"))
                if len(requested_urls) == 1:
                    return _JsonResponse({"data": [{"node": {"id": 1, "title": "One"}}], "paging": {"next": f"{config.mal.base_url}/users/@me/animelist?offset=100&limit=100&fields=list_status"}})
                return _JsonResponse({"data": [{"node": {"id": 2, "title": "Two"}}], "paging": {}})

            with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                pages = list(client.iter_my_anime_list_pages(status="completed", limit=500, fields="list_status", max_pages=2))

            self.assertEqual(2, len(pages))
            self.assertEqual(2, len(requested_urls))
            self.assertEqual(
                "https://api.myanimelist.net/v2/users/@me/animelist?offset=100&limit=100&fields=list_status",
                requested_urls[1],
            )
            first_query = parse_qs(urlparse(requested_urls[0]).query)
            self.assertEqual(["completed"], first_query["status"])
            self.assertEqual(["100"], first_query["limit"])
            self.assertEqual(["Bearer access-token", "Bearer access-token"], requested_auth)

    def test_my_anime_list_paginates_with_custom_base_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.mal = MalSettings(
                base_url="https://proxy.example/mal-api/v2",
                request_spacing_seconds=0.0,
                request_spacing_jitter_seconds=0.0,
            )
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
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
                if len(requested_urls) == 1:
                    return _JsonResponse({"data": [{"node": {"id": 1, "title": "One"}}], "paging": {"next": "https://proxy.example/mal-api/v2/users/@me/animelist?offset=100&limit=100"}})
                return _JsonResponse({"data": [{"node": {"id": 2, "title": "Two"}}], "paging": {}})

            with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                pages = list(client.iter_my_anime_list_pages(limit=100, max_pages=2))

            self.assertEqual(2, len(pages))
            self.assertEqual(2, len(requested_urls))
            self.assertEqual(
                "https://proxy.example/mal-api/v2/users/@me/animelist?offset=100&limit=100",
                requested_urls[1],
            )

    def test_my_anime_list_rejects_next_url_outside_mal_origin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=config.secrets_dir / "mal_client_id.txt",
                    client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                    access_token_path=config.secrets_dir / "mal_access_token.txt",
                    refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
                ),
            )

            def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
                return _JsonResponse({"data": [], "paging": {"next": "https://evil.example/users/@me/animelist?offset=100"}})

            with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                with self.assertRaises(MalApiError):
                    list(client.iter_my_anime_list_pages(max_pages=1))

    def test_my_anime_list_requires_explicit_positive_max_pages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=config.secrets_dir / "mal_client_id.txt",
                    client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                    access_token_path=config.secrets_dir / "mal_access_token.txt",
                    refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
                ),
            )
            with self.assertRaises(ValueError):
                list(client.iter_my_anime_list_pages())  # type: ignore[call-arg]
            with self.assertRaises(ValueError):
                list(client.iter_my_anime_list_pages(max_pages=0))

    def test_my_anime_list_rejects_unsafe_next_urls(self) -> None:
        unsafe_next_urls = [
            "http://api.myanimelist.net/v2/users/@me/animelist?offset=100",
            "https://token@api.myanimelist.net/v2/users/@me/animelist?offset=100",
            f"{DEFAULT_MAL_BASE_URL.replace('api.myanimelist.net', 'evil.example')}/users/@me/animelist?offset=100",
            "https://api.myanimelist.net/v2/anime?offset=100",
            "https://api.myanimelist.net/v2/users/@me/animelist/../anime?offset=100",
        ]
        for next_url in unsafe_next_urls:
            with self.subTest(next_url=next_url):
                with tempfile.TemporaryDirectory() as tmp:
                    config = self._config(Path(tmp))
                    client = MalClient(
                        config,
                        MalSecrets(
                            client_id="client-id",
                            client_secret=None,
                            access_token="access-token",
                            refresh_token=None,
                            client_id_path=config.secrets_dir / "mal_client_id.txt",
                            client_secret_path=config.secrets_dir / "mal_client_secret.txt",
                            access_token_path=config.secrets_dir / "mal_access_token.txt",
                            refresh_token_path=config.secrets_dir / "mal_refresh_token.txt",
                        ),
                    )

                    def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
                        return _JsonResponse({"data": [], "paging": {"next": next_url}})

                    with patch("mal_updater.mal_client.urlopen", fake_urlopen):
                        with self.assertRaises(MalApiError):
                            list(client.iter_my_anime_list_pages(max_pages=2))

    def test_search_and_detail_contract_failures_do_not_replace_caches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            config.db_path.parent.mkdir(parents=True, exist_ok=True)
            client = MalClient(config, self._secrets(config))
            with patch.object(client, "_get_json", return_value={}):
                with self.assertRaisesRegex(MalApiError, "typed data list"):
                    client.search_anime("valid", force_refresh=True)
                with self.assertRaisesRegex(MalApiError, "identity"):
                    client.get_anime_details(7, fields="id,title,related_anime", force_refresh=True)
            from mal_updater.db import get_mal_anime_search_cache, get_mal_anime_detail_cache
            self.assertIsNone(get_mal_anime_detail_cache(config.db_path, mal_anime_id=7, fields_key="id,related_anime,title", logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION))

    def test_detail_requires_matching_id_title_and_requested_container_types(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config))
            malformed = {"id": 8, "title": "Wrong", "related_anime": []}
            with patch.object(client, "_get_json", return_value=malformed):
                with self.assertRaisesRegex(MalApiError, "identity"):
                    client.get_anime_details(7, fields="id,title,related_anime", force_refresh=True)
            malformed = {"id": 7, "title": "Seven", "related_anime": {}}
            with patch.object(client, "_get_json", return_value=malformed):
                with self.assertRaisesRegex(MalApiError, "malformed related_anime"):
                    client.get_anime_details(7, fields="id,title,related_anime", force_refresh=True)

    def test_generic_list_iterator_rejects_repeated_and_backward_cursors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            client = MalClient(config, self._secrets(config, access_token="token"))
            repeated = f"{config.mal.base_url}/users/@me/animelist?limit=100&fields=list_status%2Cnum_episodes%2Cmedia_type%2Cstatus"
            with patch.object(client, "_get_json", return_value={"data": [], "paging": {"next": repeated}}):
                with self.assertRaisesRegex(MalApiError, "repeated"):
                    list(client.iter_my_anime_list_pages(max_pages=2))


if __name__ == "__main__":
    unittest.main()
