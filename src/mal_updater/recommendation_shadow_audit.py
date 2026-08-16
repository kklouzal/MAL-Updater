from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable

from .mal_client import MalApiError


SHADOW_AUDIT_SCHEMA_VERSION = "mal-suggestions-shadow-audit-v1"
QUALITY_FIELDS = "mean,num_scoring_users,num_list_users,popularity,rank,media_type,status,num_episodes,rating,nsfw,start_season"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_only_connection(db_path: Path) -> sqlite3.Connection:
    if not db_path.is_file():
        raise FileNotFoundError(f"operational database does not exist: {db_path}")
    conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def load_shadow_cohorts(db_path: Path) -> dict[str, set[int]]:
    """Load comparison identities without migrating or writing the operational DB."""
    with _read_only_connection(db_path) as conn:
        mapped = {int(row[0]) for row in conn.execute("SELECT DISTINCT mal_anime_id FROM mal_series_mapping")}
        listed = {int(row[0]) for row in conn.execute("SELECT mal_anime_id FROM mal_user_anime_list_cache")}
        latest_run = conn.execute(
            "SELECT run_id FROM recommendation_score_snapshots ORDER BY generated_at DESC, id DESC LIMIT 1"
        ).fetchone()
        recommended: set[int] = set()
        discovery: set[int] = set()
        if latest_run is not None:
            rows = conn.execute(
                "SELECT mal_anime_id, kind FROM recommendation_score_snapshots "
                "WHERE run_id = ? AND mal_anime_id IS NOT NULL",
                (latest_run["run_id"],),
            )
            for row in rows:
                anime_id = int(row["mal_anime_id"])
                recommended.add(anime_id)
                if row["kind"] == "discovery_candidate":
                    discovery.add(anime_id)
    return {
        "mapped": mapped,
        "listed": listed,
        "recommended": recommended,
        "discovery_recommended": discovery,
    }


def _typed_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _distribution(values: Iterable[Any]) -> dict[str, int | float | None]:
    typed = sorted(value for item in values if (value := _typed_number(item)) is not None)
    if not typed:
        return {"observed": 0, "minimum": None, "median": None, "maximum": None}

    def clean(value: float) -> int | float:
        return int(value) if value.is_integer() else round(value, 4)

    return {
        "observed": len(typed),
        "minimum": clean(typed[0]),
        "median": clean(float(median(typed))),
        "maximum": clean(typed[-1]),
    }


def _ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _validated_nodes(payload: dict[str, Any], *, limit: int) -> list[dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, list):
        raise MalApiError("MAL anime suggestions response lacks a typed data list")
    paging = payload.get("paging")
    if paging is not None and not isinstance(paging, dict):
        raise MalApiError("MAL anime suggestions response has malformed paging")
    if len(data) > limit:
        raise MalApiError("MAL anime suggestions response exceeds the requested first-page limit")
    nodes: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in data:
        node = item.get("node") if isinstance(item, dict) else None
        anime_id = node.get("id") if isinstance(node, dict) else None
        title = node.get("title") if isinstance(node, dict) else None
        if (
            not isinstance(node, dict)
            or isinstance(anime_id, bool)
            or not isinstance(anime_id, int)
            or anime_id <= 0
            or not isinstance(title, str)
            or not title.strip()
        ):
            raise MalApiError("MAL anime suggestions response contains an invalid node")
        if anime_id in seen:
            continue
        seen.add(anime_id)
        nodes.append(node)
    return nodes


def build_mal_suggestions_shadow_audit(
    payload: dict[str, Any],
    cohorts: dict[str, set[int]],
    *,
    limit: int,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Reduce a personalized first page to privacy-safe aggregate evaluation only."""
    normalized_limit = min(max(int(limit), 1), 100)
    nodes = _validated_nodes(payload, limit=normalized_limit)
    suggestion_ids = {int(node["id"]) for node in nodes}
    mapped = set(cohorts.get("mapped", set()))
    listed = set(cohorts.get("listed", set()))
    recommended = set(cohorts.get("recommended", set()))
    discovery = set(cohorts.get("discovery_recommended", set()))

    overlaps = {
        "mapped": len(suggestion_ids & mapped),
        "listed": len(suggestion_ids & listed),
        "recommended": len(suggestion_ids & recommended),
        "discovery_recommended": len(suggestion_ids & discovery),
    }
    counts: Counter[str] = Counter()
    for node in nodes:
        counts[f"media_type:{node.get('media_type') or 'unknown'}"] += 1
        counts[f"status:{node.get('status') or 'unknown'}"] += 1
        counts[f"rating:{node.get('rating') or 'unknown'}"] += 1
        counts[f"nsfw:{node.get('nsfw') or 'unknown'}"] += 1

    return {
        "schema_version": SHADOW_AUDIT_SCHEMA_VERSION,
        "generated_at": generated_at or _utc_now(),
        "mode": "manual_get_only_non_persisting_shadow",
        "source": {
            "surface": "GET /v2/anime/suggestions",
            "page": 1,
            "requested_limit": normalized_limit,
            "returned_distinct": len(nodes),
            "additional_pages_followed": 0,
        },
        "privacy": {
            "aggregate_only": True,
            "raw_payload_retained": False,
            "identifiers_retained": False,
            "titles_retained": False,
        },
        "cohort_sizes": {name: len(values) for name, values in sorted(cohorts.items())},
        "overlap": {
            name: {"count": count, "ratio_of_suggestions": _ratio(count, len(nodes))}
            for name, count in overlaps.items()
        },
        "novelty": {
            "vs_list_count": len(suggestion_ids - listed),
            "vs_list_ratio": _ratio(len(suggestion_ids - listed), len(nodes)),
            "vs_current_recommendations_count": len(suggestion_ids - recommended),
            "vs_current_recommendations_ratio": _ratio(len(suggestion_ids - recommended), len(nodes)),
            "vs_list_and_recommendations_count": len(suggestion_ids - listed - recommended),
            "vs_list_and_recommendations_ratio": _ratio(len(suggestion_ids - listed - recommended), len(nodes)),
        },
        "candidate_quality_inputs": {
            "mean": _distribution(node.get("mean") for node in nodes),
            "num_scoring_users": _distribution(node.get("num_scoring_users") for node in nodes),
            "num_list_users": _distribution(node.get("num_list_users") for node in nodes),
            "popularity": _distribution(node.get("popularity") for node in nodes),
            "rank": _distribution(node.get("rank") for node in nodes),
            "num_episodes": _distribution(node.get("num_episodes") for node in nodes),
            "categorical_counts": dict(sorted(counts.items())),
            "interpretation": "Descriptive inputs only; correlated popularity/adoption fields are not summed or used for production ranking.",
        },
        "operational_effects": {
            "candidate_rows_persisted": 0,
            "recommendation_state_mutated": False,
            "mal_provider_data_mutated": False,
        },
    }
