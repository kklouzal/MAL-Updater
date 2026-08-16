from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .config import AppConfig, _read_secret_file
from .crunchyroll_snapshot import CrunchyrollSnapshotError, _CrunchyrollRequestPacer, _authorized_json_get


SCHEMA_VERSION = "crunchyroll-native-recommendation-shadow-audit-v1"
ROUTES = (
    ("native_recommendations", "/content/v2/discover/{account}/recommendations"),
    ("home_feed", "/content/v2/discover/{account}/home_feed"),
)


class CrunchyrollRecommendationShadowError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _read_only_provider_cohorts(db_path: Path) -> dict[str, set[str]]:
    if not db_path.is_file():
        raise FileNotFoundError(f"operational database does not exist: {db_path}")
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only = ON")
        history = {
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT provider_series_id FROM provider_episode_progress WHERE provider = 'crunchyroll'"
            )
        }
        watchlist = {
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT provider_series_id FROM provider_watchlist WHERE provider = 'crunchyroll' AND COALESCE(is_active, 1) = 1"
            )
        }
    finally:
        connection.close()
    return {"history": history, "watchlist": watchlist}


def load_access_context(access_token_file: Path, account_id_file: Path) -> tuple[str, str]:
    for label, path in (("access token", access_token_file), ("account id", account_id_file)):
        if not path.is_file():
            raise CrunchyrollRecommendationShadowError(f"{label} file does not exist: {path}")
        if path.stat().st_mode & 0o077:
            raise CrunchyrollRecommendationShadowError(f"{label} file must not be accessible by group or others: {path}")
    access_token = _read_secret_file(access_token_file)
    account_id = _read_secret_file(account_id_file)
    if not access_token or not account_id:
        raise CrunchyrollRecommendationShadowError("ephemeral access token and account id files must both be non-empty")
    return access_token, account_id


def _status_from_error(exc: Exception) -> int | None:
    text = str(exc)
    for status in (403, 404):
        if f"HTTP {status}" in text:
            return status
    return None


def _validated_data(payload: Any, *, surface: str, limit: int) -> tuple[list[dict[str, Any]], int, bool]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise CrunchyrollRecommendationShadowError(f"{surface} schema drift: expected object with data list")
    rows = payload["data"]
    total = payload.get("total", len(rows))
    if isinstance(total, bool) or not isinstance(total, int) or total < len(rows):
        raise CrunchyrollRecommendationShadowError(f"{surface} schema drift: invalid total")
    if len(rows) > limit:
        raise CrunchyrollRecommendationShadowError(f"{surface} exceeded requested bound")
    if any(not isinstance(row, dict) for row in rows):
        raise CrunchyrollRecommendationShadowError(f"{surface} schema drift: non-object row")
    return rows, total, total > len(rows)


def _series_panel(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    raw_panel = value.get("panel")
    panel: dict[str, Any] = raw_panel if isinstance(raw_panel, dict) else value
    if str(panel.get("type") or "").casefold() not in {"series", "movie_listing"}:
        return None
    identifier = panel.get("id")
    title = panel.get("title")
    if not isinstance(identifier, str) or not identifier or not isinstance(title, str) or not title.strip():
        raise CrunchyrollRecommendationShadowError("candidate schema drift: series rows require string id and title")
    return panel


def _native_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        panel = _series_panel(row)
        if panel is None:
            raise CrunchyrollRecommendationShadowError("native_recommendations schema drift: unexpected candidate type")
        candidates.append(panel)
    return candidates


def _home_candidates(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Counter[str]]:
    candidates: list[dict[str, Any]] = []
    attribution: Counter[str] = Counter()
    for row in rows:
        source_id = row.get("source_media_id")
        source_title = row.get("source_media_title")
        if source_id is not None or source_title is not None:
            if not isinstance(source_id, str) or not source_id or not isinstance(source_title, str) or not source_title.strip():
                raise CrunchyrollRecommendationShadowError("home_feed attribution schema drift")
            attribution["because_you_watched_rows"] += 1
        else:
            attribution["generic_rows"] += 1
        values: list[Any] = [row]
        items = row.get("items")
        if items is not None:
            if not isinstance(items, list):
                raise CrunchyrollRecommendationShadowError("home_feed schema drift: items must be a list")
            values.extend(items)
        data = row.get("data")
        if data is not None:
            if not isinstance(data, list):
                raise CrunchyrollRecommendationShadowError("home_feed schema drift: nested data must be a list")
            values.extend(data)
        for value in values:
            panel = _series_panel(value)
            if panel is not None:
                candidates.append(panel)
    return candidates, attribution


def _candidate_ids(candidates: list[dict[str, Any]]) -> set[str]:
    return {str(candidate["id"]) for candidate in candidates}


def _audio_aggregates(candidates: list[dict[str, Any]]) -> dict[str, int]:
    with_audio = 0
    locale_occurrences = 0
    inline_version_entries = 0
    for candidate in candidates:
        metadata = candidate.get("series_metadata")
        if metadata is None:
            continue
        if not isinstance(metadata, dict):
            raise CrunchyrollRecommendationShadowError("candidate schema drift: series_metadata must be an object")
        locales = metadata.get("audio_locales")
        if locales is not None:
            if not isinstance(locales, list) or any(not isinstance(value, str) or not value for value in locales):
                raise CrunchyrollRecommendationShadowError("candidate schema drift: audio_locales must contain strings")
            if locales:
                with_audio += 1
                locale_occurrences += len(locales)
        versions = metadata.get("versions")
        if versions is not None:
            if not isinstance(versions, list) or any(not isinstance(value, dict) for value in versions):
                raise CrunchyrollRecommendationShadowError("candidate schema drift: versions must contain objects")
            inline_version_entries += len(versions)
    return {
        "candidates_with_inline_audio_locales": with_audio,
        "inline_audio_locale_occurrences": locale_occurrences,
        "inline_version_entries": inline_version_entries,
    }


def build_shadow_audit(
    payloads: dict[str, Any],
    cohorts: dict[str, set[str]],
    *,
    limit: int,
    generated_at: str | None = None,
    statuses: dict[str, int] | None = None,
) -> dict[str, Any]:
    normalized_limit = min(max(int(limit), 1), 25)
    statuses = statuses or {name: 200 for name, _ in ROUTES}
    surfaces: dict[str, dict[str, Any]] = {}
    surface_ids: dict[str, set[str]] = {}
    all_candidates: list[dict[str, Any]] = []
    attribution: Counter[str] = Counter()
    for name, route in ROUTES:
        status = statuses.get(name)
        if status in {403, 404}:
            surfaces[name] = {
                "route_template": route,
                "method": "GET",
                "status": status,
                "classification": "forbidden" if status == 403 else "not_found",
                "diagnostic": "route returned HTTP 403" if status == 403 else "route returned HTTP 404",
                "returned_distinct": 0,
                "complete": False,
            }
            surface_ids[name] = set()
            continue
        if status != 200:
            raise CrunchyrollRecommendationShadowError(f"{name} failed closed with unexpected status {status}")
        rows, total, partial = _validated_data(payloads.get(name), surface=name, limit=normalized_limit)
        if name == "native_recommendations":
            candidates = _native_candidates(rows)
        else:
            candidates, row_attribution = _home_candidates(rows)
            attribution.update(row_attribution)
        ids = _candidate_ids(candidates)
        surface_ids[name] = ids
        all_candidates.extend(candidates)
        surfaces[name] = {
            "route_template": route,
            "method": "GET",
            "status": 200,
            "classification": "valid_empty_sample" if not rows else "ok",
            "returned_rows": len(rows),
            "returned_distinct_candidates": len(ids),
            "advertised_total": total,
            "partial": partial,
            "complete": not partial,
        }
    distinct_candidate_ids: set[str] = set().union(*surface_ids.values()) if surface_ids else set()
    history = set(cohorts.get("history", set()))
    watchlist = set(cohorts.get("watchlist", set()))
    known = history | watchlist
    overlap_count = len(distinct_candidate_ids & known)
    consensus_count = len(surface_ids.get("native_recommendations", set()) & surface_ids.get("home_feed", set()))
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or _utc_now(),
        "mode": "manual_feature_gated_get_only_non_persisting_shadow",
        "source": {
            "provider": "crunchyroll",
            "routes": surfaces,
            "requested_limit_per_surface": normalized_limit,
            "all_selected_routes_proven_http_200_by_fresh_auth_audit": all(
                item["status"] == 200 for item in surfaces.values()
            ),
        },
        "provenance": {
            "surface_count": len(surfaces),
            "successful_surface_count": sum(item["status"] == 200 for item in surfaces.values()),
            "partial_surface_count": sum(item.get("partial") is True for item in surfaces.values()),
            "complete": bool(surfaces) and all(item.get("complete") is True for item in surfaces.values()),
        },
        "aggregate_candidates": {
            "distinct": len(distinct_candidate_ids),
            "cross_surface_consensus_count": consensus_count,
            "cross_surface_consensus_ratio": _ratio(consensus_count, len(distinct_candidate_ids)),
            "history_or_watchlist_overlap_count": overlap_count,
            "history_or_watchlist_overlap_ratio": _ratio(overlap_count, len(distinct_candidate_ids)),
            "novel_count": len(distinct_candidate_ids - known),
            "novel_ratio": _ratio(len(distinct_candidate_ids - known), len(distinct_candidate_ids)),
        },
        "because_you_watched_attribution": {
            "supported_by_retained_safe_schema": True,
            "attributed_top_level_rows": attribution["because_you_watched_rows"],
            "generic_top_level_rows": attribution["generic_rows"],
            "source_identifiers_or_titles_retained": False,
        },
        "inline_metadata": _audio_aggregates(all_candidates),
        "unsupported_diagnostics": {
            "multiprofile": "not_queried_prior_403",
            "subscription": "not_queried_prior_403",
            "custom_lists": "not_queried_prior_404",
            "standalone_versions": "not_queried_prior_404",
            "calendar": "not_queried_out_of_scope",
        },
        "privacy": {
            "aggregate_only": True,
            "raw_payload_retained": False,
            "candidate_identifiers_retained": False,
            "candidate_titles_retained": False,
            "account_or_identity_values_retained": False,
            "access_token_retained": False,
        },
        "operational_effects": {
            "recommendation_rows_persisted": 0,
            "recommendation_state_mutated": False,
            "provider_mutations": 0,
            "mal_mutations": 0,
        },
    }


def run_shadow_audit(
    config: AppConfig,
    *,
    enabled: bool,
    access_token: str,
    account_id: str,
    limit: int = 25,
    get_json: Callable[[str, dict[str, Any], str], Any] | None = None,
) -> dict[str, Any]:
    if not enabled:
        raise CrunchyrollRecommendationShadowError("Crunchyroll recommendation shadow adapter is disabled; pass the explicit manual enable flag")
    normalized_limit = min(max(int(limit), 1), 25)
    cohorts = _read_only_provider_cohorts(config.db_path)
    before = hashlib.sha256(config.db_path.read_bytes()).hexdigest()
    if get_json is None:
        pacer = _CrunchyrollRequestPacer(
            spacing_seconds=max(0.0, float(config.crunchyroll.request_spacing_seconds)),
            jitter_seconds=max(0.0, float(config.crunchyroll.request_spacing_jitter_seconds)),
            retry_max_attempts=max(1, int(config.crunchyroll.retry_max_attempts)),
            retry_backoff_base_seconds=float(config.crunchyroll.retry_backoff_base_seconds),
            retry_backoff_jitter_seconds=float(config.crunchyroll.retry_backoff_jitter_seconds),
            retry_after_cap_seconds=float(config.crunchyroll.retry_after_cap_seconds),
        )

        def get_json(url: str, params: dict[str, Any], phase: str) -> Any:
            return _authorized_json_get(
                url,
                access_token=access_token,
                timeout_seconds=float(config.request_timeout_seconds),
                params=params,
                pacer=pacer,
                phase=phase,
                config=config,
            )

    payloads: dict[str, Any] = {}
    statuses: dict[str, int] = {}
    for name, route in ROUTES:
        url = "https://www.crunchyroll.com" + route.format(account=account_id)
        try:
            payloads[name] = get_json(url, {"n": normalized_limit, "locale": config.crunchyroll.locale}, name)
            statuses[name] = 200
        except CrunchyrollSnapshotError as exc:
            status = _status_from_error(exc)
            if status not in {403, 404}:
                raise CrunchyrollRecommendationShadowError(f"{name} GET failed") from exc
            statuses[name] = status
    audit = build_shadow_audit(payloads, cohorts, limit=normalized_limit, statuses=statuses)
    after = hashlib.sha256(config.db_path.read_bytes()).hexdigest()
    if before != after:
        raise CrunchyrollRecommendationShadowError("operational database changed during read-only shadow audit")
    audit["operational_effects"]["database_byte_identical"] = True
    return audit


def artifact_contains_personal_rows(payload: dict[str, Any]) -> bool:
    forbidden = {"id", "title", "source_media_id", "source_media_title", "account_id", "access_token", "data", "items", "panel"}

    def walk(value: Any) -> bool:
        if isinstance(value, dict):
            return any(key in forbidden or walk(child) for key, child in value.items())
        if isinstance(value, list):
            return any(walk(child) for child in value)
        return False

    return walk(payload)


def render_json(payload: dict[str, Any]) -> str:
    if artifact_contains_personal_rows(payload):
        raise CrunchyrollRecommendationShadowError("privacy guard rejected non-aggregate artifact")
    return json.dumps(payload, indent=2, sort_keys=True)
