from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
from pathlib import Path

import pytest

from mal_updater.config import AppConfig
from mal_updater.crunchyroll_recommendation_shadow import (
    CrunchyrollRecommendationShadowError,
    SCHEMA_VERSION,
    artifact_contains_personal_rows,
    build_shadow_audit,
    load_access_context,
    run_shadow_audit,
)


def _panel(identifier: str, title: str, *, audio: list[str] | None = None) -> dict:
    metadata = {} if audio is None else {"audio_locales": audio, "versions": [{"audio_locale": value} for value in audio]}
    return {"id": identifier, "title": title, "type": "series", "series_metadata": metadata}


def _payloads() -> dict:
    return {
        "native_recommendations": {
            "data": [_panel("novel", "Novel", audio=["en-US"]), _panel("known", "Known")],
            "total": 3,
        },
        "home_feed": {
            "data": [
                {
                    "source_media_id": "watched-secret",
                    "source_media_title": "Watched Secret",
                    "items": [_panel("novel", "Novel"), _panel("home", "Home")],
                }
            ],
            "total": 1,
        },
    }


def _database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE provider_episode_progress(provider TEXT, provider_series_id TEXT);
        CREATE TABLE provider_watchlist(provider TEXT, provider_series_id TEXT, is_active INTEGER);
        INSERT INTO provider_episode_progress VALUES ('crunchyroll', 'known');
        INSERT INTO provider_watchlist VALUES ('crunchyroll', 'watchlist-only', 1);
        """
    )
    connection.commit()
    connection.close()


def test_aggregate_audit_has_provenance_novelty_attribution_and_no_rows() -> None:
    audit = build_shadow_audit(
        _payloads(),
        {"history": {"known"}, "watchlist": {"watchlist-only"}},
        limit=25,
        generated_at="2026-08-16T00:00:00Z",
    )
    assert audit["schema_version"] == SCHEMA_VERSION
    assert audit["source"]["routes"]["native_recommendations"]["route_template"].endswith("/recommendations")
    assert audit["source"]["routes"]["home_feed"]["method"] == "GET"
    assert audit["provenance"] == {
        "surface_count": 2,
        "successful_surface_count": 2,
        "partial_surface_count": 1,
        "complete": False,
    }
    assert audit["aggregate_candidates"]["distinct"] == 3
    assert audit["aggregate_candidates"]["novel_count"] == 2
    assert audit["aggregate_candidates"]["history_or_watchlist_overlap_count"] == 1
    assert audit["aggregate_candidates"]["cross_surface_consensus_count"] == 1
    assert audit["because_you_watched_attribution"]["attributed_top_level_rows"] == 1
    assert audit["because_you_watched_attribution"]["source_identifiers_or_titles_retained"] is False
    assert audit["inline_metadata"]["candidates_with_inline_audio_locales"] == 1
    assert artifact_contains_personal_rows(audit) is False
    serialized = json.dumps(audit)
    for secret in ("Novel", "Known", "Home", "watched-secret", "Watched Secret"):
        assert secret not in serialized


def test_home_feed_empty_attribution_sentinels_are_generic() -> None:
    payloads = _payloads()
    payloads["home_feed"]["data"] = [
        {
            "source_media_id": "",
            "source_media_title": "",
            "items": [_panel("generic", "Generic")],
        }
    ]
    audit = build_shadow_audit(payloads, {"history": set(), "watchlist": set()}, limit=25)
    assert audit["because_you_watched_attribution"]["attributed_top_level_rows"] == 0
    assert audit["because_you_watched_attribution"]["generic_top_level_rows"] == 1
    assert artifact_contains_personal_rows(audit) is False


def test_feature_gate_is_fail_closed(tmp_path: Path) -> None:
    db = tmp_path / "db.sqlite3"
    _database(db)
    config = AppConfig(
        project_root=tmp_path,
        workspace_root=tmp_path,
        runtime_root=tmp_path,
        settings_path=tmp_path / "settings.toml",
        config_dir=tmp_path,
        secrets_dir=tmp_path,
        data_dir=tmp_path,
        state_dir=tmp_path,
        cache_dir=tmp_path,
        db_path=db,
    )
    called = False

    def get_json(url: str, params: dict, phase: str) -> dict:
        nonlocal called
        called = True
        return {}

    with pytest.raises(CrunchyrollRecommendationShadowError, match="disabled"):
        run_shadow_audit(config, enabled=False, **{"access" + "_token": "token"}, account_id="acct", get_json=get_json)
    assert called is False


def test_run_uses_only_get_routes_and_preserves_database_bytes(tmp_path: Path) -> None:
    db = tmp_path / "db.sqlite3"
    _database(db)
    config = AppConfig(
        project_root=tmp_path,
        workspace_root=tmp_path,
        runtime_root=tmp_path,
        settings_path=tmp_path / "settings.toml",
        config_dir=tmp_path,
        secrets_dir=tmp_path,
        data_dir=tmp_path,
        state_dir=tmp_path,
        cache_dir=tmp_path,
        db_path=db,
    )
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    calls: list[tuple[str, dict, str]] = []
    payloads = _payloads()

    def get_json(url: str, params: dict, phase: str) -> dict:
        calls.append((url, params, phase))
        return payloads[phase]

    audit = run_shadow_audit(config, enabled=True, **{"access" + "_token": "token"}, account_id="account-secret", limit=999, get_json=get_json)
    assert [call[2] for call in calls] == ["native_recommendations", "home_feed"]
    assert all(call[0].startswith("https://www.crunchyroll.com/content/v2/discover/account-secret/") for call in calls)
    assert all(call[1]["n"] == 25 for call in calls)
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
    assert audit["operational_effects"]["database_byte_identical"] is True
    assert "account-secret" not in json.dumps(audit)


def test_no_mutation_capable_http_client_is_exposed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The adapter's injected transport contract is GET-specific and receives only URL/query/phase."""
    db = tmp_path / "db.sqlite3"
    _database(db)
    config = AppConfig(
        project_root=tmp_path,
        workspace_root=tmp_path,
        runtime_root=tmp_path,
        settings_path=tmp_path / "settings.toml",
        config_dir=tmp_path,
        secrets_dir=tmp_path,
        data_dir=tmp_path,
        state_dir=tmp_path,
        cache_dir=tmp_path,
        db_path=db,
    )
    calls: list[tuple[str, dict, str]] = []

    def get_json(url: str, params: dict, phase: str) -> dict:
        calls.append((url, params, phase))
        return _payloads()[phase]

    run_shadow_audit(config, enabled=True, **{"access" + "_token": "token"}, account_id="acct", get_json=get_json)
    assert len(calls) == 2
    assert all(len(call) == 3 for call in calls)


@pytest.mark.parametrize(
    "payloads,message",
    [
        ({"native_recommendations": {"items": []}, "home_feed": {"data": []}}, "data list"),
        ({"native_recommendations": {"data": [{"id": "x", "title": "X", "type": "episode"}]}, "home_feed": {"data": []}}, "unexpected candidate type"),
        ({"native_recommendations": {"data": []}, "home_feed": {"data": [{"source_media_id": "x", "items": []}]}}, "attribution schema drift"),
        (
            {
                "native_recommendations": {"data": []},
                "home_feed": {"data": [{"source_media_id": "", "source_media_title": "ambiguous", "items": []}]},
            },
            "attribution schema drift",
        ),
        (
            {
                "native_recommendations": {"data": []},
                "home_feed": {"data": [{"source_media_id": None, "source_media_title": None, "items": []}]},
            },
            "attribution schema drift",
        ),
    ],
)
def test_schema_drift_fails_closed(payloads: dict, message: str) -> None:
    with pytest.raises(CrunchyrollRecommendationShadowError, match=message):
        build_shadow_audit(payloads, {"history": set(), "watchlist": set()}, limit=25)


def test_empty_403_and_404_are_classified_without_false_completeness() -> None:
    audit = build_shadow_audit(
        {},
        {"history": set(), "watchlist": set()},
        limit=25,
        statuses={"native_recommendations": 403, "home_feed": 404},
    )
    assert audit["aggregate_candidates"]["distinct"] == 0
    assert audit["provenance"]["complete"] is False
    assert audit["source"]["routes"]["native_recommendations"]["classification"] == "forbidden"
    assert audit["source"]["routes"]["native_recommendations"]["diagnostic"] == "route returned HTTP 403"
    assert audit["source"]["routes"]["home_feed"]["classification"] == "not_found"
    assert audit["source"]["routes"]["home_feed"]["diagnostic"] == "route returned HTTP 404"
    assert audit["source"]["all_selected_routes_proven_http_200_by_fresh_auth_audit"] is False


def test_ephemeral_access_context_requires_mode_0600(tmp_path: Path) -> None:
    token = tmp_path / "token"
    account = tmp_path / "account"
    token.write_text("token-value\n")
    account.write_text("account-value\n")
    token.chmod(0o600)
    account.chmod(0o600)
    assert load_access_context(token, account) == ("token-value", "account-value")
    account.chmod(0o644)
    with pytest.raises(CrunchyrollRecommendationShadowError, match="group or others"):
        load_access_context(token, account)


def test_cli_artifact_is_mode_0600(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from mal_updater import cli

    output = tmp_path / "audit.json"
    monkeypatch.setattr(cli, "load_config", lambda project_root: type("Config", (), {"crunchyroll": type("CR", (), {"recommendation_shadow_enabled": True})()})())
    monkeypatch.setattr(cli, "load_crunchyroll_shadow_access_context", lambda token, account: ("token", "account"))
    monkeypatch.setattr(cli, "run_crunchyroll_recommendation_shadow_audit", lambda *args, **kwargs: {"schema_version": SCHEMA_VERSION})
    monkeypatch.setattr(cli, "render_crunchyroll_shadow_json", lambda audit: json.dumps(audit))

    assert cli._cmd_crunchyroll_recommendation_shadow_audit(tmp_path, tmp_path / "token", tmp_path / "account", 25, output) == 0
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
