from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from .config import AppConfig, load_mal_secrets
from .db import (
    MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
    MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
    MalAnimeMetadata,
    MalPublicUserRecsCrawlGeneration,
    MalUserAnimeListRefreshConflictError,
    MalUserAnimeListRefreshSummary,
    MAL_USER_LIST_PAGINATION_LOGIC_VERSION,
    abort_mal_user_anime_list_cache_refresh,
    begin_mal_user_anime_list_cache_refresh,
    connect,
    count_mal_user_anime_list_cache,
    create_or_get_active_mal_public_userrecs_generation,
    claim_mal_public_userrecs_sources,
    claim_or_create_mal_user_anime_list_traversal,
    checkpoint_mal_user_anime_list_page,
    checkpoint_mal_user_anime_list_revalidation,
    finish_mal_user_anime_list_cache_refresh,
    get_active_mal_public_userrecs_generation,
    get_mal_user_anime_list_refresh_diagnostics,
    get_mal_user_anime_list_traversal,
    get_mal_anime_metadata_map,
    list_active_mal_public_userrecs_generations,
    list_mal_public_userrecs_staged_pages,
    list_mal_user_anime_list_cache,
    merge_mal_user_anime_list_cache_into_metadata,
    list_series_mappings,
    mark_mal_public_userrecs_generation_ready,
    pause_mal_public_userrecs_generation,
    persist_mal_user_anime_list_identity_assertion,
    publish_mal_public_userrecs_generation,
    publish_mal_user_anime_list_staging,
    release_mal_public_userrecs_source_claim,
    record_mal_recommendation_harvest_attempt_error,
    record_mal_user_anime_list_request_failure,
    record_mal_recommendation_harvest_failure,
    record_mal_public_userrecs_revalidation,
    renew_mal_public_userrecs_source_claim,
    replace_mal_anime_relations,
    restart_or_quarantine_mal_user_anime_list_traversal,
    replace_mal_public_userrecs_staged_page,
    replace_mal_recommendation_edges,
    upsert_mal_anime_metadata,
    restart_mal_public_userrecs_generation_after_drift,
    resume_mal_public_userrecs_generation,
    schedule_mal_public_userrecs_generation_retry,
    sync_mal_public_userrecs_source_queue,
    load_validated_mal_user_anime_list_staging,
    release_mal_user_anime_list_traversal_claim,
    select_mal_user_anime_list_partition_work,
)
from .mal_client import MalApiError, MalClient
from .periodic_evidence_lifecycle import periodic_evidence_is_due
from .mal_user_recommendations import (
    DEFAULT_PUBLIC_USER_RECS_MAX_BODY_BYTES,
    DEFAULT_PUBLIC_USER_RECS_MAX_PAGES,
    PublicMalUserRecommendationsClient,
    PublicMalUserRecommendationsError,
    build_public_user_recs_url,
    validate_public_user_recs_url,
)

DETAIL_FIELD_NAMES = (
    "id",
    "title",
    "alternative_titles",
    "main_picture",
    "synopsis",
    "media_type",
    "status",
    "num_episodes",
    "mean",
    "rank",
    "popularity",
    "num_list_users",
    "num_scoring_users",
    "rating",
    "average_episode_duration",
    "start_date",
    "end_date",
    "broadcast",
    "pictures",
    "background",
    "nsfw",
    "statistics",
    "start_season",
    "source",
    "genres",
    "studios",
    "related_anime",
    "related_manga",
    "recommendations",
    "my_list_status",
)
DETAIL_FIELDS = ",".join(DETAIL_FIELD_NAMES)
DISCOVERY_DETAIL_FIELDS = ",".join(field for field in DETAIL_FIELD_NAMES if field not in {"related_anime", "recommendations"})
DEFAULT_HARVEST_STALE_AFTER_DAYS = 120
DEFAULT_METADATA_STALE_AFTER_DAYS = 120
DEFAULT_HOT_METADATA_STALE_AFTER_DAYS = 120
DEFAULT_WARM_METADATA_STALE_AFTER_DAYS = 120
DEFAULT_COLD_METADATA_STALE_AFTER_DAYS = 120
DEFAULT_FULL_USER_RECOMMENDATION_HARVEST_STALE_AFTER_DAYS = 120
MAL_USER_LIST_POSITIVE_SEED_STATUSES = frozenset({"completed", "watching", "on_hold"})
MAL_USER_LIST_SUPPRESSION_STATUSES = frozenset({"completed", "watching", "on_hold", "dropped", "plan_to_watch"})
MAL_USER_LIST_STATUS_PREFERENCE_FIELDS = (
    "priority",
    "is_rewatching",
    "num_times_rewatched",
    "rewatch_value",
    "tags",
    "comments",
)
MAL_USER_LIST_FIELD_NAMES = (
    "list_status",
    *MAL_USER_LIST_STATUS_PREFERENCE_FIELDS,
    "num_episodes",
    "media_type",
    "status",
)
MAL_USER_LIST_FIELDS = ",".join(MAL_USER_LIST_FIELD_NAMES)
HARVEST_RETRY_STATUSES = frozenset({"unharvested", "stale", "failed"})
HARVEST_RETRY_ORDER = {"unharvested": 0, "failed": 1, "stale": 2}

# Official MAL API v2 anime detail/search fields do not expose character or voice-actor
# credits. Keep recommendation metadata on official catalog/list signals rather than
# scraping MAL pages or introducing an unofficial dependency for that fragile surface.
CHARACTER_VOICE_ACTOR_CAPABILITY_NOTE = (
    "Official MAL API v2 anime metadata does not expose character/voice-actor credits; "
    "recommendation metadata intentionally avoids scraping or unofficial services."
)


@dataclass(slots=True)
class _DiscoveredTargetStats:
    title: str | None = None
    supporting_sources: int = 0
    total_recommendation_votes: int = 0
    best_single_source_votes: int = 0
    cross_seed_support_votes: int = 0

    def observe(self, *, title: str | None, num_recommendations: int) -> None:
        self.supporting_sources += 1
        if title and not self.title:
            self.title = title
        votes = max(num_recommendations, 0)
        self.total_recommendation_votes += votes
        self.best_single_source_votes = max(self.best_single_source_votes, votes)
        self.cross_seed_support_votes = max(self.total_recommendation_votes - self.best_single_source_votes, 0)


@dataclass(slots=True)
class _SeedRefreshState:
    mal_anime_id: int
    eligible: bool
    metadata_status: str
    harvest_status: str
    harvest_fetched_at: str | None
    harvest_edge_count: int


@dataclass(slots=True)
class MetadataRefreshFailure:
    mal_anime_id: int
    stage: str
    error: str

    def as_dict(self) -> dict[str, Any]:
        return {"mal_anime_id": self.mal_anime_id, "stage": self.stage, "error": self.error}


@dataclass(slots=True)
class MetadataRefreshSummary:
    considered: int
    refreshed: int
    discovery_considered: int = 0
    discovery_refreshed: int = 0
    failures: list[MetadataRefreshFailure] | None = None
    eligible_seed_count: int = 0
    harvest_unharvested: int = 0
    harvest_stale: int = 0
    harvest_failed: int = 0
    harvested_edge_count: int = 0
    target_hydration_skip_reasons: dict[str, int] = field(default_factory=dict)
    fresh_skipped: int = 0
    refresh_tiers: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        failures = self.failures or []
        return {
            "considered": self.considered,
            "refreshed": self.refreshed,
            "eligible_seed_count": self.eligible_seed_count,
            "harvest_unharvested": self.harvest_unharvested,
            "harvest_stale": self.harvest_stale,
            "harvest_failed": self.harvest_failed,
            "harvested_edge_count": self.harvested_edge_count,
            "discovery_considered": self.discovery_considered,
            "discovery_refreshed": self.discovery_refreshed,
            "target_hydration_skip_reasons": dict(self.target_hydration_skip_reasons),
            "fresh_skipped": self.fresh_skipped,
            "refresh_tiers": dict(self.refresh_tiers),
            "official_detail_fields": list(DETAIL_FIELD_NAMES),
            "typed_detail_fields": [
                "rank",
                "num_list_users",
                "num_scoring_users",
                "rating",
                "average_episode_duration",
                "start_date",
                "end_date",
                "broadcast_day",
                "broadcast_time",
                "broadcast_timezone",
                "nsfw",
            ],
            "failed": len(failures),
            "failures": [failure.as_dict() for failure in failures],
        }


@dataclass(slots=True)
class FullUserRecommendationHarvestFailure:
    mal_anime_id: int
    title: str | None
    error: str
    pages_fetched: int = 0
    source_url: str | None = None
    generation_id: int | None = None
    paused: bool = False
    drift_restart: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "mal_anime_id": self.mal_anime_id,
            "title": self.title,
            "error": self.error,
            "pages_fetched": self.pages_fetched,
            "source_url": self.source_url,
            "generation_id": self.generation_id,
            "paused": self.paused,
            "drift_restart": self.drift_restart,
        }


@dataclass(slots=True)
class FullUserRecommendationHarvestSummary:
    status: str
    seed_count: int
    considered: int
    harvested: int
    failed: int
    skipped_fresh: int
    total_edges: int
    forced: bool
    stale_after_days: int
    max_pages: int
    failures: list[FullUserRecommendationHarvestFailure] = field(default_factory=list)
    harvested_sources: list[dict[str, Any]] = field(default_factory=list)
    paused_sources: list[dict[str, Any]] = field(default_factory=list)
    restarted_sources: list[dict[str, Any]] = field(default_factory=list)
    queue_counts: dict[str, int] = field(default_factory=dict)
    selected_classes: list[str] = field(default_factory=list)
    quarantined_sources: list[dict[str, Any]] = field(default_factory=list)
    fetch_attempted: int = 0
    fetch_succeeded: int = 0
    fetch_failed: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "seed_count": self.seed_count,
            "considered": self.considered,
            "harvested": self.harvested,
            "failed": self.failed,
            "paused": len(self.paused_sources),
            "drift_restarted": len(self.restarted_sources),
            "skipped_fresh": self.skipped_fresh,
            "total_edges": self.total_edges,
            "forced": self.forced,
            "stale_after_days": self.stale_after_days,
            "max_pages": self.max_pages,
            "failures": [failure.as_dict() for failure in self.failures],
            "harvested_sources": list(self.harvested_sources),
            "paused_sources": list(self.paused_sources),
            "restarted_sources": list(self.restarted_sources),
            "quarantined": len(self.quarantined_sources),
            "quarantined_sources": list(self.quarantined_sources),
            "queue_counts": dict(self.queue_counts),
            "selected_classes": list(self.selected_classes),
            "fetch_attempted": self.fetch_attempted,
            "fetch_succeeded": self.fetch_succeeded,
            "fetch_failed": self.fetch_failed,
            "semantics": {
                "source": "public_mal_userrecs_html",
                "complete_when_no_next_link": False,
                "max_pages": "per-source per-run network-request budget including continuation and final page-1/boundary revalidation; zero performs no fetch",
                "terminal_requires": "complete recognized userrecs document, unambiguous validated pagination, coherent staged chain, and final snapshot validation",
                "partial_failure_preserves_existing_edges": True,
                "staged_pages_publish_atomically": True,
                "source_priority": ["never_started", "resumable", "retry_due", "refresh_due"],
                "fairness": "durable collision-free monotonic selection sequence inside each strict class; never-started consumes capacity before open or refresh work",
                "concurrency": "durable expiring source claims fence every orchestrated generation mutation by token, source/generation identity, and revision",
                "retained_fields": ["target_mal_anime_id", "target_title", "num_recommendations"],
                "privacy": "recommendation prose and usernames are not persisted",
            },
        }


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_stale(fetched_at: str | None, *, stale_after_days: int) -> bool:
    parsed = _parse_timestamp(fetched_at)
    if parsed is None:
        return True
    threshold = datetime.now(timezone.utc) - timedelta(days=max(int(stale_after_days), 0))
    return parsed < threshold


def _metadata_age_sort_value(fetched_at: str | None) -> tuple[int, str]:
    if not fetched_at:
        return (0, "")
    value = str(fetched_at).strip()
    if not value:
        return (0, "")
    parsed = _parse_timestamp(value)
    if parsed is None:
        return (1, value)
    return (1, parsed.isoformat())


def _metadata_status(metadata: MalAnimeMetadata | None, *, stale_after_days: int) -> str:
    if metadata is None:
        return "missing"
    return "stale" if _is_stale(metadata.fetched_at, stale_after_days=stale_after_days) else "fresh"


def _has_my_list_status(metadata: MalAnimeMetadata | None) -> bool:
    my_list_status = _my_list_status_payload(metadata)
    if not isinstance(my_list_status, dict):
        return False
    status = my_list_status.get("status")
    return isinstance(status, str) and bool(status.strip())


def _has_positive_my_list_status(metadata: MalAnimeMetadata | None) -> bool:
    status = _my_list_status_value(metadata)
    return status in MAL_USER_LIST_POSITIVE_SEED_STATUSES


def _my_list_status_payload(metadata: MalAnimeMetadata | None) -> Any:
    if metadata is None:
        return None
    return metadata.raw.get("my_list_status") if isinstance(metadata.raw, dict) else None


def _my_list_status_value(metadata: MalAnimeMetadata | None) -> str | None:
    my_list_status = _my_list_status_payload(metadata)
    status = my_list_status.get("status") if isinstance(my_list_status, dict) else None
    if not isinstance(status, str):
        return None
    normalized = status.strip().lower()
    return normalized or None


def _load_mapped_seed_states(
    db_path: Path,
    *,
    mapped_anime_ids: set[int],
    metadata_by_id: dict[int, MalAnimeMetadata],
    positive_mal_list_seed_ids: set[int] | None,
    harvest_stale_after_days: int,
    metadata_stale_after_days: int,
) -> dict[int, _SeedRefreshState]:
    if not mapped_anime_ids:
        return {}
    placeholders = ", ".join("?" for _ in mapped_anime_ids)
    params = [int(anime_id) for anime_id in sorted(mapped_anime_ids)]
    with connect(db_path) as conn:
        listed_rows = conn.execute(
            f"""
            SELECT
                m.mal_anime_id,
                MAX(CASE WHEN w.provider_series_id IS NOT NULL OR p.provider_episode_id IS NOT NULL THEN 1 ELSE 0 END) AS listed
            FROM mal_series_mapping m
            LEFT JOIN provider_watchlist w
                ON w.provider = m.provider AND w.provider_series_id = m.provider_series_id
            LEFT JOIN provider_episode_progress p
                ON p.provider = m.provider AND p.provider_series_id = m.provider_series_id
            WHERE m.mal_anime_id IN ({placeholders})
            GROUP BY m.mal_anime_id
            """,
            params,
        ).fetchall()
        status_rows = conn.execute(
            f"""
            SELECT source_mal_anime_id, status, num_edges, fetched_at, source_type, is_complete
            FROM mal_recommendation_harvest_status
            WHERE source_mal_anime_id IN ({placeholders})
            """,
            params,
        ).fetchall()
        edge_rows = conn.execute(
            f"""
            SELECT source_mal_anime_id, COUNT(*) AS edge_count, MAX(fetched_at) AS fetched_at
            FROM mal_anime_recommendations
            WHERE source_kind = 'mal_recommendation' AND source_mal_anime_id IN ({placeholders})
            GROUP BY source_mal_anime_id
            """,
            params,
        ).fetchall()

    listed_by_id = {int(row["mal_anime_id"]): bool(row["listed"]) for row in listed_rows}
    harvest_by_id = {int(row["source_mal_anime_id"]): row for row in status_rows}
    edges_by_id = {int(row["source_mal_anime_id"]): row for row in edge_rows}
    states: dict[int, _SeedRefreshState] = {}
    for anime_id in sorted(mapped_anime_ids):
        metadata = metadata_by_id.get(anime_id)
        status_row = harvest_by_id.get(anime_id)
        edge_row = edges_by_id.get(anime_id)
        fetched_at = str(status_row["fetched_at"]) if status_row and status_row["fetched_at"] else None
        if fetched_at is None and edge_row and edge_row["fetched_at"]:
            fetched_at = str(edge_row["fetched_at"])
        edge_count = 0
        if status_row is not None:
            edge_count = int(status_row["num_edges"] or 0)
        elif edge_row is not None:
            edge_count = int(edge_row["edge_count"] or 0)

        has_complete_public_harvest = (
            status_row is not None
            and int(status_row["is_complete"] or 0) == 1
            and str(status_row["source_type"] or "") == MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS
        )
        if has_complete_public_harvest:
            # The separate public-MAL cold path owns complete-harvest freshness.
            # The ordinary 12h official-detail metadata lane must never chase a
            # stale/failed full-harvest attempt by overwriting complete edges with
            # the official API detail surface's practical top-10 subset.
            harvest_status = "fresh"
        elif status_row is None and edge_row is None:
            harvest_status = "unharvested"
        else:
            stored_status = str(status_row["status"] or "fetched") if status_row is not None else "fetched"
            if stored_status != "fetched":
                harvest_status = "failed"
            elif _is_stale(fetched_at, stale_after_days=harvest_stale_after_days):
                harvest_status = "stale"
            else:
                harvest_status = "fresh"

        states[anime_id] = _SeedRefreshState(
            mal_anime_id=anime_id,
            eligible=bool(listed_by_id.get(anime_id)) or anime_id in (positive_mal_list_seed_ids or set()) or _has_positive_my_list_status(metadata),
            metadata_status=_metadata_status(metadata, stale_after_days=metadata_stale_after_days),
            harvest_status=harvest_status,
            harvest_fetched_at=fetched_at,
            harvest_edge_count=edge_count,
        )
    return states


def _rank_refresh_ids(anime_ids: list[int], metadata_by_id: dict[int, Any], seed_states: dict[int, _SeedRefreshState] | None = None) -> list[int]:
    def _priority(anime_id: int) -> tuple[int, tuple[int, str], tuple[int, str], int]:
        state = seed_states.get(anime_id) if seed_states is not None else None
        metadata_age = _metadata_age_sort_value(getattr(metadata_by_id.get(anime_id), "fetched_at", None))
        if state is None:
            return (1, metadata_age, (1, ""), anime_id)
        harvest_age = _metadata_age_sort_value(state.harvest_fetched_at)
        if state.eligible and state.harvest_status in HARVEST_RETRY_STATUSES:
            harvest_order = HARVEST_RETRY_ORDER.get(state.harvest_status, 3)
            return (0, (harvest_order, harvest_age[1]), metadata_age, anime_id)
        if state.metadata_status in {"missing", "stale"}:
            metadata_order = 0 if state.metadata_status == "missing" else 1
            return (1, (metadata_order, metadata_age[1]), harvest_age, anime_id)
        return (2, metadata_age, harvest_age, anime_id)

    return sorted(anime_ids, key=_priority)


def _record_harvest_failure(db_path: Path, *, source_mal_anime_id: int, error: str | None = None) -> None:
    record_mal_recommendation_harvest_failure(
        db_path,
        source_mal_anime_id=source_mal_anime_id,
        source_type=MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
        error=error,
    )


def _metadata_payload_from_details(db_path: Path, *, mal_anime_id: int, details: dict[str, Any]) -> None:
    alternative_titles = details.get("alternative_titles") or {}
    aliases: list[str] = []
    if isinstance(alternative_titles, dict):
        for key in ("en", "ja"):
            value = alternative_titles.get(key)
            if isinstance(value, str) and value.strip():
                aliases.append(value.strip())
        synonyms = alternative_titles.get("synonyms")
        if isinstance(synonyms, list):
            for value in synonyms:
                if isinstance(value, str) and value.strip():
                    aliases.append(value.strip())

    upsert_mal_anime_metadata(
        db_path,
        mal_anime_id=mal_anime_id,
        title=str(details.get("title") or mal_anime_id),
        title_english=alternative_titles.get("en") if isinstance(alternative_titles, dict) else None,
        title_japanese=alternative_titles.get("ja") if isinstance(alternative_titles, dict) else None,
        alternative_titles=aliases,
        media_type=str(details.get("media_type")) if details.get("media_type") else None,
        status=str(details.get("status")) if details.get("status") else None,
        num_episodes=int(details["num_episodes"]) if isinstance(details.get("num_episodes"), int) else None,
        mean=float(details["mean"]) if isinstance(details.get("mean"), (float, int)) else None,
        popularity=int(details["popularity"]) if isinstance(details.get("popularity"), int) else None,
        start_season=details.get("start_season") if isinstance(details.get("start_season"), dict) else None,
        raw=details,
    )


def _record_target_skip(target_hydration_skip_reasons: dict[str, int], reason: str) -> None:
    target_hydration_skip_reasons[reason] = target_hydration_skip_reasons.get(reason, 0) + 1



def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _payload_has_next_page(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    paging = payload.get("paging")
    if not isinstance(paging, dict):
        return False
    next_url = paging.get("next")
    return isinstance(next_url, str) and bool(next_url.strip())


def _normalized_user_list_statuses(statuses: list[str] | tuple[str, ...] | None) -> list[str | None]:
    if not statuses:
        return [None]
    normalized: list[str | None] = []
    for status in statuses:
        value = str(status).strip().lower()
        if value == "all":
            return [None]
        if value not in MAL_USER_LIST_SUPPRESSION_STATUSES:
            raise ValueError(f"Unsupported MAL anime list status: {status}")
        if value not in normalized:
            normalized.append(value)
    return normalized or [None]


def _mal_user_list_query_identity(*, statuses: list[str | None], page_size: int) -> tuple[str, dict[str, Any]]:
    query = {
        "statuses": ["all" if value is None else value for value in statuses],
        "page_size": min(max(int(page_size), 1), 100),
        "fields": MAL_USER_LIST_FIELDS,
        "logic_version": MAL_USER_LIST_PAGINATION_LOGIC_VERSION,
    }
    encoded = __import__("json").dumps(query, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest(), query


def _mal_user_list_initial_url(config: AppConfig, *, status: str | None, page_size: int) -> str:
    query: dict[str, Any] = {"limit": min(max(int(page_size), 1), 100), "fields": MAL_USER_LIST_FIELDS}
    if status is not None:
        query["status"] = status
    return f"{config.mal.base_url}/users/@me/animelist?{urlencode(query)}"


def _validate_mal_user_list_cursor(
    config: AppConfig, *, url: str, expected_status: str | None, page_size: int,
    expected_offset: int | None = None,
) -> str:
    parsed = urlparse(str(url))
    base = urlparse(config.mal.base_url)
    if parsed.scheme != "https" or parsed.scheme != base.scheme or parsed.netloc != base.netloc or parsed.username or parsed.password:
        raise ValueError("MAL anime-list next cursor is foreign or not HTTPS")
    base_path = base.path.rstrip("/")
    expected_path = f"{base_path}/users/@me/animelist" if base_path else "/users/@me/animelist"
    if parsed.path != expected_path or parsed.fragment:
        raise ValueError("MAL anime-list next cursor has a conflicting path/fragment")
    query = parse_qs(parsed.query, keep_blank_values=True)
    if set(query) - {"limit", "fields", "status", "offset", "ranking_type", "nsfw"}:
        raise ValueError("MAL anime-list next cursor has unexpected query keys")
    for key, values in query.items():
        if len(values) != 1:
            raise ValueError(f"MAL anime-list next cursor repeats {key}")
    if query.get("limit", [None])[0] != str(min(max(int(page_size), 1), 100)):
        raise ValueError("MAL anime-list next cursor changes page limit")
    if query.get("fields", [None])[0] != MAL_USER_LIST_FIELDS:
        raise ValueError("MAL anime-list next cursor changes requested fields")
    actual_status = query.get("status", [None])[0]
    if actual_status != expected_status:
        raise ValueError("MAL anime-list next cursor changes status partition")
    offset = query.get("offset", ["0"])[0]
    if not str(offset).isdigit():
        raise ValueError("MAL anime-list next cursor has a malformed offset")
    if expected_offset is not None and int(offset) != int(expected_offset):
        raise ValueError("MAL anime-list next cursor skips, repeats, or moves backward across the expected offset boundary")
    return str(url)


def _parse_mal_user_list_payload(
    config: AppConfig, *, payload: dict[str, Any], page_url: str, expected_status: str | None,
    page_size: int, account_id: int, account_name: str,
) -> tuple[list[dict[str, Any]], str | None, str, dict[str, Any], bool, bool]:
    if not isinstance(payload, dict) or set(payload) - {"data", "paging"}:
        raise ValueError("MAL anime-list response document is malformed or has unexpected top-level fields")
    if "data" not in payload or "paging" not in payload or not isinstance(payload["data"], list) or not isinstance(payload["paging"], dict):
        raise ValueError("MAL anime-list response lacks complete data/paging structure")
    paging = payload["paging"]
    if set(paging) - {"previous", "next"}:
        raise ValueError("MAL anime-list paging object has unexpected fields")
    if "next" not in paging:
        raise ValueError("MAL anime-list response is ambiguous because paging.next is missing")
    next_raw = paging["next"]
    if next_raw is not None and (not isinstance(next_raw, str) or not next_raw.strip()):
        raise ValueError("MAL anime-list paging.next is malformed")
    items: list[dict[str, Any]] = []
    ordered: list[tuple[int, int | None, str | None]] = []
    for raw in payload["data"]:
        if not isinstance(raw, dict) or not isinstance(raw.get("node"), dict) or not isinstance(raw.get("list_status"), dict):
            raise ValueError("MAL anime-list data row is structurally incomplete")
        node = raw["node"]
        list_status = raw["list_status"]
        anime_id = node.get("id")
        title = node.get("title")
        status = list_status.get("status")
        if isinstance(anime_id, bool) or not isinstance(anime_id, int) or anime_id <= 0 or not isinstance(title, str) or not title.strip():
            raise ValueError("MAL anime-list data row lacks valid anime identity")
        if expected_status is not None and status != expected_status:
            raise ValueError("MAL anime-list response row conflicts with requested status partition")
        if status not in MAL_USER_LIST_SUPPRESSION_STATUSES:
            raise ValueError("MAL anime-list response row has unsupported status")
        rank = raw.get("ranking", {}).get("rank") if isinstance(raw.get("ranking"), dict) else None
        if rank is not None and (isinstance(rank, bool) or not isinstance(rank, int) or rank <= 0):
            raise ValueError("MAL anime-list response row has malformed rank")
        ordered.append((anime_id, rank, list_status.get("updated_at") if isinstance(list_status.get("updated_at"), str) else None))
        items.append(raw)
    ids = [item[0] for item in ordered]
    if len(ids) != len(set(ids)):
        raise ValueError("MAL anime-list page repeats an anime id")
    current_query = parse_qs(urlparse(page_url).query, keep_blank_values=True)
    current_offset_text = current_query.get("offset", ["0"])[0]
    if not str(current_offset_text).isdigit():
        raise ValueError("MAL anime-list current page has a malformed offset")
    current_offset = int(current_offset_text)
    # MAL's offset is the index of the next range. A non-terminal page must be
    # full; otherwise the server's offset contract is ambiguous and publishing
    # would risk a skipped interior range.
    expected_page_size = min(max(int(page_size), 1), 100)
    if next_raw is not None and len(items) != expected_page_size:
        raise ValueError(
            "MAL anime-list non-terminal page was short; expected exactly "
            f"{expected_page_size} distinct valid rows, got {len(items)}"
        )
    next_url = None if next_raw is None else _validate_mal_user_list_cursor(
        config,
        url=next_raw.strip(),
        expected_status=expected_status,
        page_size=page_size,
        expected_offset=current_offset + expected_page_size,
    )
    canonical = __import__("json").dumps(ordered, ensure_ascii=False, separators=(",", ":"))
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    anchor = {
        "account_id": account_id, "account_name": account_name, "status": expected_status,
        "page_url": page_url, "next_url": next_url, "row_count": len(items), "first": ordered[:2], "last": ordered[-2:],
    }
    terminal_explicit = next_url is None
    empty_proven = terminal_explicit and not items
    return items, next_url, fingerprint, anchor, terminal_explicit, empty_proven


def refresh_mal_user_anime_list_cache(
    config: AppConfig,
    *,
    statuses: list[str] | tuple[str, ...] | None = None,
    page_size: int = 100,
    max_pages: int | None = 25,
    prune_on_complete: bool = False,
) -> MalUserAnimeListRefreshSummary:
    """Durably refresh MAL @me with fair partition rotation and fail-closed publication."""
    normalized_max_pages = 25 if max_pages is None else max(0, int(max_pages))
    normalized_statuses = _normalized_user_list_statuses(statuses)
    if normalized_max_pages == 0:
        # Hard zero-network/no-mutation contract; diagnostics read only.
        diagnostics = get_mal_user_anime_list_refresh_diagnostics(config.db_path)
        return MalUserAnimeListRefreshSummary(
            status="partial", refresh_run_id="", generation=0, partial=True,
            error="max_pages=0: no MAL request or refresh-state mutation performed", traversal=diagnostics,
        )
    client = MalClient(config, load_mal_secrets(config))
    attempts = 1  # Account identity is a budgeted network attempt.
    try:
        account = client.get_my_user(max_attempts=1)
    except (MalApiError, TimeoutError) as exc:
        return MalUserAnimeListRefreshSummary(
            status="failed", refresh_run_id="", generation=0, pages=attempts, partial=True,
            error=str(exc), traversal=get_mal_user_anime_list_refresh_diagnostics(config.db_path),
        )
    account_id = account.get("id") if isinstance(account, dict) else None
    account_name = account.get("name") if isinstance(account, dict) else None
    if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id <= 0 or not isinstance(account_name, str) or not account_name.strip():
        raise MalApiError("authenticated MAL account response is incomplete")
    query_identity, query = _mal_user_list_query_identity(statuses=normalized_statuses, page_size=page_size)
    partitions = [
        {
            "partition_key": "all" if status is None else status,
            "requested_status": status,
            "ordinal": ordinal,
            "initial_url": _mal_user_list_initial_url(config, status=status, page_size=page_size),
        }
        for ordinal, status in enumerate(normalized_statuses)
    ]
    token = str(uuid.uuid4())
    generation, _ = claim_or_create_mal_user_anime_list_traversal(
        config.db_path, account_id=account_id, account_name=account_name, query_identity=query_identity,
        query=query, partitions=partitions, claim_token=token, fetched_at=_now_iso(),
    )
    failure: str | None = None
    while attempts < normalized_max_pages:
        selected = select_mal_user_anime_list_partition_work(
            config.db_path, generation=generation.generation, claim_token=token
        )
        if selected is None:
            break
        generation, partition, work_kind, page_url = selected
        attempts += 1  # persisted before invocation by selection.
        try:
            payload = client.get_my_anime_list_page_url(page_url)
            parsed = _parse_mal_user_list_payload(
                config, payload=payload, page_url=page_url, expected_status=partition.requested_status,
                page_size=page_size, account_id=account_id, account_name=account_name,
            )
            items, next_url, fingerprint, anchor, terminal_explicit, empty_proven = parsed
            if work_kind == "page":
                generation, _ = checkpoint_mal_user_anime_list_page(
                    config.db_path, generation=generation.generation, partition_key=partition.partition_key,
                    claim_token=token, expected_revision=generation.revision, page_url=page_url, next_url=next_url,
                    items=items, fingerprint=fingerprint, anchor=anchor, terminal_explicit=terminal_explicit,
                    empty_proven=empty_proven, fetched_at=_now_iso(),
                )
            else:
                generation, _ = checkpoint_mal_user_anime_list_revalidation(
                    config.db_path, generation=generation.generation, partition_key=partition.partition_key,
                    claim_token=token, expected_revision=generation.revision, kind=work_kind,
                    page_url=page_url, fingerprint=fingerprint, anchor=anchor,
                )
        except (MalApiError, TimeoutError) as exc:
            retry_class = "auth_or_contract" if "HTTP 401" in str(exc) or "HTTP 403" in str(exc) else "ordinary_retry"
            quarantine = retry_class == "auth_or_contract"
            generation = record_mal_user_anime_list_request_failure(
                config.db_path, generation=generation.generation, partition_key=partition.partition_key,
                claim_token=token, expected_revision=generation.revision, retry_class=retry_class, error=str(exc),
                next_retry_at=None if quarantine else (datetime.now(timezone.utc)+timedelta(minutes=15)).replace(microsecond=0).isoformat().replace("+00:00","Z"),
                quarantine=quarantine,
            )
            failure = str(exc)
            break
        except ValueError as exc:
            generation = record_mal_user_anime_list_request_failure(
                config.db_path, generation=generation.generation, partition_key=partition.partition_key,
                claim_token=token, expected_revision=generation.revision, retry_class="snapshot_drift_or_contract",
                error=str(exc), next_retry_at=None,
            )
            generation = restart_or_quarantine_mal_user_anime_list_traversal(
                config.db_path, generation=generation.generation, claim_token=token,
                expected_revision=generation.revision, reason=str(exc),
            )
            failure = str(exc)
            break
    # Bind publication to the authenticated account. The initial identity
    # response is stable evidence only for a no-page run; after any list call,
    # revalidate under the same hard request budget before publication.
    if failure is None:
        generation, identity_partitions = get_mal_user_anime_list_traversal(
            config.db_path, generation=generation.generation
        )
        complete = bool(identity_partitions) and all(
            part.terminal and part.terminal_explicit and part.page1_validated_at and part.boundary_validated_at
            for part in identity_partitions
        )
        if complete and attempts < normalized_max_pages:
            attempts += 1
            try:
                current_account = client.get_my_user(max_attempts=1)
                if current_account.get("id") != account_id or current_account.get("name") != account_name:
                    raise ValueError("authenticated MAL account identity changed before publication")
                generation = persist_mal_user_anime_list_identity_assertion(
                    config.db_path, generation=generation.generation, claim_token=token,
                    expected_revision=generation.revision, account_id=account_id,
                    account_name=account_name, nonce=str(uuid.uuid4()),
                )
            except (MalApiError, TimeoutError, ValueError) as exc:
                failure = str(exc)
        elif complete:
            failure = "request budget exhausted before authenticated account identity revalidation"
    generation, current_partitions = get_mal_user_anime_list_traversal(config.db_path, generation=generation.generation)
    all_validated = bool(current_partitions) and all(
        part.terminal and part.terminal_explicit and part.page1_validated_at and part.boundary_validated_at
        for part in current_partitions
    )
    identity_confirmed = failure is None
    if all_validated and identity_confirmed:
        try:
            generation, _items, _proof = load_validated_mal_user_anime_list_staging(
                config.db_path, generation=generation.generation, claim_token=token, expected_revision=generation.revision,
            )
            summary = publish_mal_user_anime_list_staging(
                config.db_path, generation=generation.generation, claim_token=token, expected_revision=generation.revision,
                delete_absent=bool(normalized_statuses == [None] and prune_on_complete),
            )
            summary.metadata_rows_with_my_list_status = merge_mal_user_anime_list_cache_into_metadata(config.db_path)
            summary.traversal = get_mal_user_anime_list_refresh_diagnostics(config.db_path)
            return summary
        except (ValueError, MalUserAnimeListRefreshConflictError, __import__("json").JSONDecodeError) as exc:
            failure = str(exc)
    # Final validation is a distinct budgeted boundary: if all page requests used
    # the budget, publication waits for a later invocation and LKG remains intact.
    if generation.quarantined_at is None and generation.claim_token == token:
        try:
            release_mal_user_anime_list_traversal_claim(
                config.db_path, generation=generation.generation, claim_token=token, expected_revision=generation.revision,
            )
        except MalUserAnimeListRefreshConflictError:
            pass
    diagnostics = get_mal_user_anime_list_refresh_diagnostics(config.db_path)
    return MalUserAnimeListRefreshSummary(
        status="failed" if generation.quarantined_at else "partial",
        refresh_run_id=generation.refresh_run_id, generation=generation.generation,
        pages=max(0, generation.requests_attempted), items=sum(part.item_count for part in current_partitions),
        preserved_absent=count_mal_user_anime_list_cache(config.db_path), partial=True,
        error=failure or "network budget exhausted before final validated publication; published MAL list LKG preserved",
        traversal=diagnostics,
    )

def _full_harvest_status_rows(db_path: Path, source_ids: set[int]) -> dict[int, dict[str, Any]]:
    if not source_ids:
        return {}
    placeholders = ", ".join("?" for _ in source_ids)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                source_mal_anime_id,
                status,
                num_edges,
                fetched_at,
                source_type,
                is_complete,
                pages_fetched,
                source_url,
                last_attempted_at,
                last_error,
                failure_count
            FROM mal_recommendation_harvest_status
            WHERE source_mal_anime_id IN ({placeholders})
            """,
            [int(source_id) for source_id in sorted(source_ids)],
        ).fetchall()
    return {int(row["source_mal_anime_id"]): {key: row[key] for key in row.keys()} for row in rows}


def _full_harvest_candidate_status(
    row: dict[str, Any] | None,
    *,
    stale_after_days: int,
    source_mal_anime_id: int | None = None,
) -> str:
    if row is None:
        return "unharvested"
    if str(row.get("status") or "") == "failed":
        return "failed"
    if not bool(row.get("is_complete")) or str(row.get("source_type") or "") != MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS:
        return "unharvested"
    if source_mal_anime_id is None:
        return "stale" if _is_stale(row.get("fetched_at"), stale_after_days=stale_after_days) else "fresh"
    return "stale" if periodic_evidence_is_due(
        successful_at=row.get("fetched_at"),
        surface="complete_public_userrecs_harvest",
        identity={"source_mal_anime_id": int(source_mal_anime_id)},
        target_days=stale_after_days,
        jitter_days=min(15, stale_after_days),
    ) else "fresh"


def _full_harvest_rank_key(entry: Any, status_row: dict[str, Any] | None, *, stale_after_days: int) -> tuple[int, tuple[int, str], int]:
    status = _full_harvest_candidate_status(
        status_row,
        stale_after_days=stale_after_days,
        source_mal_anime_id=int(entry.mal_anime_id),
    )
    status_order = {"unharvested": 0, "failed": 1, "stale": 2, "fresh": 3}.get(status, 4)
    if status == "failed":
        age = _metadata_age_sort_value(status_row.get("last_attempted_at") if status_row else None)
    else:
        age = _metadata_age_sort_value(status_row.get("fetched_at") if status_row else None)
    return (status_order, age, int(entry.mal_anime_id))


def _full_userrecs_start_url(config: AppConfig, *, source_mal_anime_id: int, source_title: str | None) -> str:
    return validate_public_user_recs_url(
        build_public_user_recs_url(
            config.mal.public_base_url,
            source_mal_anime_id=int(source_mal_anime_id),
            source_title=source_title,
        ),
        public_base_url=config.mal.public_base_url,
        source_mal_anime_id=int(source_mal_anime_id),
    )


def _public_userrecs_generation_drift_reason(
    db_path: Path,
    *,
    generation: MalPublicUserRecsCrawlGeneration,
) -> str | None:
    pages = list_mal_public_userrecs_staged_pages(db_path, generation_id=generation.generation_id)
    if not pages:
        if generation.status == "ready":
            return "ready generation has no staged pages"
        if generation.cursor_url is None:
            return "stored cursor_url is missing without staged pages"
        if generation.pages_fetched != 0:
            return "stored pages_fetched references missing staged pages"
        if generation.last_page_url or generation.last_page_fingerprint:
            return "stored last-page metadata exists without staged pages"
        return None
    actual_numbers = [int(page.page_number) for page in pages]
    expected_numbers = list(range(1, len(pages) + 1))
    if actual_numbers != expected_numbers:
        return f"staged pages are not contiguous from page 1: {actual_numbers!r}"
    if generation.pages_fetched != len(pages):
        return "stored pages_fetched does not match staged page count"
    last_page = pages[-1]
    if generation.last_page_url != last_page.page_url:
        return "stored last_page_url does not match final staged page"
    if generation.last_page_fingerprint != last_page.page_fingerprint:
        return "stored last_page_fingerprint does not match final staged page"
    if generation.cursor_url != last_page.next_url:
        return "stored cursor_url does not match final staged page next_url"
    for previous, current in zip(pages, pages[1:]):
        if previous.next_url != current.page_url:
            return "staged next-link chain no longer matches stored page order"
    return None


def _public_userrecs_fetched_page_drift_reason(
    db_path: Path,
    *,
    generation: MalPublicUserRecsCrawlGeneration,
    fetched_page: Any,
) -> str | None:
    pages = list_mal_public_userrecs_staged_pages(db_path, generation_id=generation.generation_id)
    page_urls = {str(page.page_url) for page in pages if page.page_url}
    final_url = str(fetched_page.final_url)
    next_url = fetched_page.next_url
    if final_url in page_urls:
        return "fetched page URL already exists in this staged generation"
    if next_url is not None and str(next_url) in (page_urls | {final_url}):
        return "fetched page next_url loops into this staged generation"
    if pages:
        last = pages[-1]
        if generation.cursor_url != last.next_url:
            return "stored cursor_url changed before staging fetched page"
        previous_ids: set[int] = set()
        for page in pages:
            previous_ids.update(int(value) for value in (page.anchor.get("target_mal_anime_ids") or []))
        fetched_ids = set((getattr(fetched_page, "anchor", {}) or {}).get("target_mal_anime_ids") or [])
        overlap = sorted(previous_ids & fetched_ids)
        if overlap:
            return f"fetched page overlaps a prior staged page target: {overlap[:10]!r}"
    return None


def _public_userrecs_revalidate_generation(
    config: AppConfig,
    *,
    generation: MalPublicUserRecsCrawlGeneration,
    client: Any,
    max_body_bytes: int,
    remaining_requests: int,
    claim_token: str,
    fetch_counts: dict[str, int],
) -> tuple[str | None, int, MalPublicUserRecsCrawlGeneration]:
    """Re-read every staged page because MAL exposes no immutable snapshot token."""
    pages = list_mal_public_userrecs_staged_pages(config.db_path, generation_id=generation.generation_id)
    if not pages:
        return None, 0, generation
    checks = pages
    if remaining_requests < len(checks):
        return "max_pages cannot cover required final snapshot validation", 0, generation
    fetched_count = 0
    validation_parts: list[str] = []
    for staged in checks:
        generation = renew_mal_public_userrecs_source_claim(
            config.db_path,
            source_mal_anime_id=generation.source_mal_anime_id,
            generation_id=generation.generation_id,
            claim_token=claim_token,
            expected_revision=generation.generation_revision,
        )
        try:
            # The budget is an attempt budget, not a successful-response
            # counter. Charge it before entering code that may raise.
            fetched_count += 1
            fetch_counts["attempted"] += 1
            try:
                fetch_kwargs = {"page_url": staged.page_url, "max_body_bytes": max_body_bytes}
                if isinstance(client, PublicMalUserRecommendationsClient):
                    fetched = client.fetch_page(
                        generation.source_mal_anime_id,
                        page_url=staged.page_url,
                        max_body_bytes=max_body_bytes,
                        max_attempts=1,
                    )
                else:
                    fetched = client.fetch_page(generation.source_mal_anime_id, **fetch_kwargs)
            except BaseException:
                fetch_counts["failed"] += 1
                raise
        except (PublicMalUserRecommendationsError, TimeoutError, ValueError) as exc:
            return f"snapshot revalidation failed at page {staged.page_number}: {exc}", fetched_count, generation
        fetch_counts["succeeded"] += 1
        if fetched.final_url != staged.page_url:
            return f"snapshot revalidation final URL drifted at page {staged.page_number}", fetched_count, generation
        if fetched.page_fingerprint != staged.page_fingerprint:
            return f"snapshot revalidation fingerprint/anchor drift at page {staged.page_number}", fetched_count, generation
        if fetched.next_url != staged.next_url:
            return f"snapshot revalidation next-link drift at page {staged.page_number}", fetched_count, generation
        validation_parts.append(f"{staged.page_number}:{fetched.page_fingerprint}:{fetched.next_url or ''}")
    validation_fingerprint = hashlib.sha256("|".join(validation_parts).encode()).hexdigest()
    generation = record_mal_public_userrecs_revalidation(
        config.db_path,
        generation_id=generation.generation_id,
        checked_boundary=True,
        validation_fingerprint=validation_fingerprint,
        claim_token=claim_token,
        expected_revision=generation.generation_revision,
    )
    return None, fetched_count, generation


def _public_userrecs_pause_source(
    config: AppConfig,
    *,
    generation: MalPublicUserRecsCrawlGeneration,
    cursor_url: str | None,
    error: str | None,
    claim_token: str,
) -> MalPublicUserRecsCrawlGeneration:
    if generation.status in {"active", "paused"}:
        return pause_mal_public_userrecs_generation(
            config.db_path,
            generation_id=generation.generation_id,
            cursor_url=cursor_url,
            error=error,
            claim_token=claim_token,
            expected_revision=generation.generation_revision,
        )
    return generation


def refresh_full_user_recommendation_harvest(
    config: AppConfig,
    *,
    limit: int | None = None,
    force_refresh: bool = False,
    stale_after_days: int = DEFAULT_FULL_USER_RECOMMENDATION_HARVEST_STALE_AFTER_DAYS,
    max_pages: int = DEFAULT_PUBLIC_USER_RECS_MAX_PAGES,
    max_body_bytes: int = DEFAULT_PUBLIC_USER_RECS_MAX_BODY_BYTES,
    client: Any | None = None,
) -> FullUserRecommendationHarvestSummary:
    """Resumable cold path for complete public MAL user-recommendation aggregates.

    Seeds come only from the cached official MAL @me anime-list positive states
    (completed/watching/on_hold). Provider-only mappings are intentionally not a
    full-harvest source of truth. Each run fetches at most ``max_pages`` per
    selected source, stages page data in an open generation, and publishes only
    after a terminal page proves that the staged generation is coherent.
    """
    stale_after_days = max(1, int(stale_after_days))
    normalized_max_pages = max(0, int(max_pages))
    normalized_max_body_bytes = max(1024, int(max_body_bytes))
    # Hard zero means no attributable database mutation at all. Do not merge
    # metadata, synchronize/claim the queue, or advance fairness/outcomes.
    if normalized_max_pages == 0:
        positive_entries = list_mal_user_anime_list_cache(
            config.db_path, statuses=MAL_USER_LIST_POSITIVE_SEED_STATUSES
        )
        return FullUserRecommendationHarvestSummary(
            status="partial" if positive_entries else "ok",
            seed_count=len(positive_entries), considered=0, harvested=0, failed=0,
            skipped_fresh=0, total_edges=0, forced=bool(force_refresh),
            stale_after_days=stale_after_days, max_pages=0,
            paused_sources=[
                {"mal_anime_id": int(entry.mal_anime_id), "reason": "zero_request_budget"}
                for entry in positive_entries
            ],
        )
    merge_mal_user_anime_list_cache_into_metadata(config.db_path)
    positive_entries = list_mal_user_anime_list_cache(config.db_path, statuses=MAL_USER_LIST_POSITIVE_SEED_STATUSES)
    source_ids = {int(entry.mal_anime_id) for entry in positive_entries}
    status_rows = _full_harvest_status_rows(config.db_path, source_ids)
    entries_by_source = {int(entry.mal_anime_id): entry for entry in positive_entries}
    open_generations = {
        int(generation.source_mal_anime_id): generation
        for generation in list_active_mal_public_userrecs_generations(config.db_path, source_mal_anime_ids=source_ids)
    }
    due_classes: dict[int, str] = {}
    for source_id in sorted(source_ids):
        status = _full_harvest_candidate_status(status_rows.get(source_id), stale_after_days=stale_after_days, source_mal_anime_id=source_id)
        if source_id in open_generations and open_generations[source_id].retry_class:
            due_classes[source_id] = "retry_due"
        elif source_id in open_generations:
            due_classes[source_id] = "resumable"
        elif status == "unharvested":
            due_classes[source_id] = "never_started"
        elif status == "failed":
            due_classes[source_id] = "retry_due"
        elif status == "stale" or force_refresh:
            due_classes[source_id] = "refresh_due"
        else:
            due_classes[source_id] = "fresh"
    queue_rows = sync_mal_public_userrecs_source_queue(
        config.db_path,
        source_mal_anime_ids=source_ids,
        due_classes=due_classes,
    )
    queue_counts = dict(Counter(row.queue_class for row in queue_rows if row.eligible))
    skipped_fresh = queue_counts.get("fresh", 0)
    capacity = len(source_ids) if limit is None else max(0, int(limit))
    claim_token = uuid.uuid4().hex
    claimed_rows = claim_mal_public_userrecs_sources(
        config.db_path,
        limit=capacity,
        claim_token=claim_token,
    )
    selected_entries = [entries_by_source[row.source_mal_anime_id] for row in claimed_rows if row.source_mal_anime_id in entries_by_source]
    selected_classes = [row.queue_class for row in claimed_rows if row.source_mal_anime_id in entries_by_source]
    harvest_client = client or PublicMalUserRecommendationsClient(config)
    failures: list[FullUserRecommendationHarvestFailure] = []
    harvested_sources: list[dict[str, Any]] = []
    paused_sources: list[dict[str, Any]] = []
    restarted_sources: list[dict[str, Any]] = []
    quarantined_sources: list[dict[str, Any]] = []
    harvested = 0
    total_edges = 0
    fetch_counts = {"attempted": 0, "succeeded": 0, "failed": 0}

    for entry in selected_entries:
        source_id = int(entry.mal_anime_id)
        source_title = entry.title
        source_url = _full_userrecs_start_url(config, source_mal_anime_id=source_id, source_title=source_title)
        pages_this_run = 0
        generation = create_or_get_active_mal_public_userrecs_generation(
            config.db_path,
            source_mal_anime_id=source_id,
            source_title=source_title,
            source_url=source_url,
            cursor_url=source_url,
            claim_token=claim_token,
        )

        queue_outcome = "resumable"
        queue_class = "resumable"
        queue_retry_at: str | None = None
        queue_error_code: str | None = None
        resume_revalidated = False

        while True:
            drift_reason = _public_userrecs_generation_drift_reason(config.db_path, generation=generation)
            if drift_reason is not None:
                generation = restart_mal_public_userrecs_generation_after_drift(
                    config.db_path,
                    generation_id=generation.generation_id,
                    reason=drift_reason,
                    cursor_url=source_url,
                    claim_token=claim_token,
                    expected_revision=generation.generation_revision,
                )
                if generation.quarantined_at is not None:
                    quarantined_sources.append({
                        "mal_anime_id": source_id,
                        "generation_id": generation.generation_id,
                        "reason": generation.quarantine_reason or drift_reason,
                    })
                    queue_outcome = "quarantined_drift_livelock"
                    queue_class = "quarantined"
                    queue_error_code = "pagination_drift"
                    break
                restarted_sources.append(
                    {
                        "mal_anime_id": source_id,
                        "title": source_title,
                        "generation_id": generation.generation_id,
                        "reason": drift_reason,
                        "cursor_url": generation.cursor_url,
                    }
                )
                if pages_this_run >= normalized_max_pages:
                    paused = _public_userrecs_pause_source(
                        config,
                        generation=generation,
                        cursor_url=generation.cursor_url or source_url,
                        error="drift restart deferred until next run because max_pages was reached",
                        claim_token=claim_token,
                    )
                    paused_sources.append(
                        {
                            "mal_anime_id": source_id,
                            "title": source_title,
                            "generation_id": paused.generation_id,
                            "pages_fetched": paused.pages_fetched,
                            "cursor_url": paused.cursor_url,
                            "reason": "max_pages",
                        }
                    )
                    break
                continue

            if generation.status == "ready":
                if generation.pages_fetched > 0 and not resume_revalidated:
                    drift_reason, checked_pages, generation = _public_userrecs_revalidate_generation(
                        config,
                        generation=generation,
                        client=harvest_client,
                        max_body_bytes=normalized_max_body_bytes,
                        remaining_requests=normalized_max_pages - pages_this_run,
                        claim_token=claim_token,
                        fetch_counts=fetch_counts,
                    )
                    pages_this_run += checked_pages
                    resume_revalidated = True
                    if drift_reason is not None:
                        if drift_reason.startswith("max_pages cannot cover"):
                            paused = _public_userrecs_pause_source(
                                config, generation=generation, cursor_url=generation.cursor_url,
                                error=drift_reason, claim_token=claim_token,
                            )
                            paused_sources.append({
                                "mal_anime_id": source_id, "title": source_title,
                                "generation_id": paused.generation_id,
                                "pages_fetched": paused.pages_fetched, "cursor_url": paused.cursor_url,
                                "reason": "final_validation_budget",
                            })
                            queue_outcome = "paused_final_validation_budget"
                            break
                        generation = restart_mal_public_userrecs_generation_after_drift(
                            config.db_path,
                            generation_id=generation.generation_id,
                            reason=drift_reason,
                            cursor_url=source_url,
                            claim_token=claim_token,
                            expected_revision=generation.generation_revision,
                        )
                        restarted_sources.append({
                            "mal_anime_id": source_id,
                            "title": source_title,
                            "generation_id": generation.generation_id,
                            "reason": drift_reason,
                            "cursor_url": generation.cursor_url,
                        })
                        continue
                try:
                    publication = publish_mal_public_userrecs_generation(
                        config.db_path, generation_id=generation.generation_id,
                        claim_token=claim_token, expected_revision=generation.generation_revision,
                    )
                except (PublicMalUserRecommendationsError, TimeoutError, ValueError, RuntimeError) as exc:
                    error = str(exc)
                    record_mal_recommendation_harvest_failure(
                        config.db_path,
                        source_mal_anime_id=source_id,
                        source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                        error=error,
                        pages_fetched=generation.pages_fetched,
                        source_url=generation.source_url or source_url,
                    )
                    failures.append(
                        FullUserRecommendationHarvestFailure(
                            mal_anime_id=source_id,
                            title=source_title,
                            error=error,
                            pages_fetched=generation.pages_fetched,
                            source_url=generation.source_url or source_url,
                            generation_id=generation.generation_id,
                        )
                    )
                    break
                harvested += 1
                total_edges += publication.published_edge_count
                harvested_sources.append(
                    {
                        "mal_anime_id": source_id,
                        "title": source_title,
                        "edge_count": publication.published_edge_count,
                        "pages_fetched": publication.pages_fetched,
                        "source_url": generation.source_url or source_url,
                        "generation_id": publication.generation_id,
                    }
                )
                queue_outcome = "published"
                queue_class = "fresh"
                break

            if generation.status == "paused":
                if generation.pages_fetched > 0 and not resume_revalidated:
                    drift_reason, checked_pages, generation = _public_userrecs_revalidate_generation(
                        config,
                        generation=generation,
                        client=harvest_client,
                        max_body_bytes=normalized_max_body_bytes,
                        remaining_requests=normalized_max_pages - pages_this_run,
                        claim_token=claim_token,
                        fetch_counts=fetch_counts,
                    )
                    pages_this_run += checked_pages
                    resume_revalidated = True
                    if drift_reason is not None:
                        if drift_reason.startswith("max_pages cannot cover"):
                            paused_sources.append({
                                "mal_anime_id": source_id, "title": source_title,
                                "generation_id": generation.generation_id,
                                "pages_fetched": generation.pages_fetched, "cursor_url": generation.cursor_url,
                                "reason": "resume_validation_budget",
                            })
                            queue_outcome = "paused_resume_validation_budget"
                            break
                        generation = restart_mal_public_userrecs_generation_after_drift(
                            config.db_path,
                            generation_id=generation.generation_id,
                            reason=drift_reason,
                            cursor_url=source_url,
                            claim_token=claim_token,
                            expected_revision=generation.generation_revision,
                        )
                        restarted_sources.append({
                            "mal_anime_id": source_id,
                            "title": source_title,
                            "generation_id": generation.generation_id,
                            "reason": drift_reason,
                            "cursor_url": generation.cursor_url,
                        })
                        continue
                generation = resume_mal_public_userrecs_generation(
                    config.db_path, generation_id=generation.generation_id,
                    claim_token=claim_token, expected_revision=generation.generation_revision,
                )

            if generation.cursor_url is None:
                try:
                    generation = mark_mal_public_userrecs_generation_ready(
                        config.db_path, generation_id=generation.generation_id,
                        claim_token=claim_token, expected_revision=generation.generation_revision,
                    )
                    continue
                except ValueError as exc:
                    error = str(exc)
                    generation = _public_userrecs_pause_source(
                        config,
                        generation=generation,
                        cursor_url=generation.cursor_url,
                        error=error,
                        claim_token=claim_token,
                    )
                    record_mal_recommendation_harvest_failure(
                        config.db_path,
                        source_mal_anime_id=source_id,
                        source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                        error=error,
                        pages_fetched=generation.pages_fetched,
                        source_url=generation.source_url or source_url,
                    )
                    failures.append(
                        FullUserRecommendationHarvestFailure(
                            mal_anime_id=source_id,
                            title=source_title,
                            error=error,
                            pages_fetched=generation.pages_fetched,
                            source_url=generation.source_url or source_url,
                            generation_id=generation.generation_id,
                            paused=True,
                        )
                    )
                    break

            if pages_this_run >= normalized_max_pages:
                paused = _public_userrecs_pause_source(
                    config,
                    generation=generation,
                    cursor_url=generation.cursor_url,
                    error="max_pages reached; staged generation paused with next-page cursor",
                    claim_token=claim_token,
                )
                paused_sources.append(
                    {
                        "mal_anime_id": source_id,
                        "title": source_title,
                        "generation_id": paused.generation_id,
                        "pages_fetched": paused.pages_fetched,
                        "cursor_url": paused.cursor_url,
                        "reason": "max_pages",
                    }
                )
                queue_outcome = "paused_page_budget"
                break

            try:
                cursor_url = validate_public_user_recs_url(
                    generation.cursor_url,
                    public_base_url=config.mal.public_base_url,
                    source_mal_anime_id=source_id,
                )
            except PublicMalUserRecommendationsError as exc:
                drift_reason = f"stored cursor URL failed validation: {exc}"
                generation = restart_mal_public_userrecs_generation_after_drift(
                    config.db_path,
                    generation_id=generation.generation_id,
                    reason=drift_reason,
                    cursor_url=source_url,
                    claim_token=claim_token,
                    expected_revision=generation.generation_revision,
                )
                restarted_sources.append(
                    {
                        "mal_anime_id": source_id,
                        "title": source_title,
                        "generation_id": generation.generation_id,
                        "reason": drift_reason,
                        "cursor_url": generation.cursor_url,
                    }
                )
                if generation.quarantined_at is not None:
                    quarantined_sources.append({
                        "mal_anime_id": source_id,
                        "generation_id": generation.generation_id,
                        "reason": generation.quarantine_reason or drift_reason,
                    })
                    queue_outcome = "quarantined_drift_livelock"
                    queue_class = "quarantined"
                    queue_error_code = "pagination_drift"
                    break
                if pages_this_run >= normalized_max_pages:
                    paused = _public_userrecs_pause_source(
                        config,
                        generation=generation,
                        cursor_url=generation.cursor_url or source_url,
                        error="drift restart deferred until next run because max_pages was reached",
                        claim_token=claim_token,
                    )
                    paused_sources.append(
                        {
                            "mal_anime_id": source_id,
                            "title": source_title,
                            "generation_id": paused.generation_id,
                            "pages_fetched": paused.pages_fetched,
                            "cursor_url": paused.cursor_url,
                            "reason": "drift_restart",
                        }
                    )
                    break
                continue
            try:
                generation = renew_mal_public_userrecs_source_claim(
                    config.db_path, source_mal_anime_id=source_id,
                    generation_id=generation.generation_id, claim_token=claim_token,
                    expected_revision=generation.generation_revision,
                )
                pages_this_run += 1
                fetch_counts["attempted"] += 1
                try:
                    fetch_kwargs = {"page_url": cursor_url, "max_body_bytes": normalized_max_body_bytes}
                    if isinstance(harvest_client, PublicMalUserRecommendationsClient):
                        fetched_page = harvest_client.fetch_page(
                            source_id,
                            page_url=cursor_url,
                            max_body_bytes=normalized_max_body_bytes,
                            max_attempts=1,
                        )
                    else:
                        fetched_page = harvest_client.fetch_page(source_id, **fetch_kwargs)
                except BaseException:
                    fetch_counts["failed"] += 1
                    raise
            except (PublicMalUserRecommendationsError, TimeoutError, ValueError) as exc:
                error = str(exc)
                generation = _public_userrecs_pause_source(
                    config,
                    generation=generation,
                    cursor_url=cursor_url,
                    error=error,
                    claim_token=claim_token,
                )
                record_mal_recommendation_harvest_attempt_error(
                    config.db_path,
                    source_mal_anime_id=source_id,
                    source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                    error=error,
                    pages_fetched=generation.pages_fetched,
                    source_url=generation.source_url or source_url,
                )
                failures.append(
                    FullUserRecommendationHarvestFailure(
                        mal_anime_id=source_id,
                        title=source_title,
                        error=error,
                        pages_fetched=generation.pages_fetched,
                        source_url=generation.source_url or source_url,
                        generation_id=generation.generation_id,
                        paused=True,
                    )
                )
                queue_outcome = "retryable_failure"
                queue_class = "retry_due"
                queue_error_code = "fetch_failure"
                retry_delay_seconds = min(24 * 60 * 60, 15 * 60 * (2 ** min(generation.attempt_count, 6)))
                queue_retry_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=retry_delay_seconds)
                ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                generation = schedule_mal_public_userrecs_generation_retry(
                    config.db_path,
                    generation_id=generation.generation_id,
                    retry_class="transient_fetch_failure",
                    next_retry_at=queue_retry_at,
                    claim_token=claim_token,
                    expected_revision=generation.generation_revision,
                )
                break

            fetch_counts["succeeded"] += 1

            fetched_drift_reason = _public_userrecs_fetched_page_drift_reason(
                config.db_path,
                generation=generation,
                fetched_page=fetched_page,
            )
            if fetched_drift_reason is not None:
                generation = restart_mal_public_userrecs_generation_after_drift(
                    config.db_path,
                    generation_id=generation.generation_id,
                    reason=fetched_drift_reason,
                    cursor_url=source_url,
                    claim_token=claim_token,
                    expected_revision=generation.generation_revision,
                )
                restarted_sources.append(
                    {
                        "mal_anime_id": source_id,
                        "title": source_title,
                        "generation_id": generation.generation_id,
                        "reason": fetched_drift_reason,
                        "cursor_url": generation.cursor_url,
                    }
                )
                if generation.quarantined_at is not None:
                    quarantined_sources.append({
                        "mal_anime_id": source_id,
                        "generation_id": generation.generation_id,
                        "reason": generation.quarantine_reason or fetched_drift_reason,
                    })
                    queue_outcome = "quarantined_drift_livelock"
                    queue_class = "quarantined"
                    queue_error_code = "pagination_drift"
                    break
                if pages_this_run >= normalized_max_pages:
                    paused = _public_userrecs_pause_source(
                        config,
                        generation=generation,
                        cursor_url=generation.cursor_url or source_url,
                        error="drift restart deferred until next run because max_pages was reached",
                        claim_token=claim_token,
                    )
                    paused_sources.append(
                        {
                            "mal_anime_id": source_id,
                            "title": source_title,
                            "generation_id": paused.generation_id,
                            "pages_fetched": paused.pages_fetched,
                            "cursor_url": paused.cursor_url,
                            "reason": "drift_restart",
                        }
                    )
                    break
                continue

            page_number = generation.pages_fetched + 1
            edge_payloads = fetched_page.edge_payloads(
                source_url=generation.source_url or source_url,
                page_count=page_number,
            )
            replace_mal_public_userrecs_staged_page(
                config.db_path,
                generation_id=generation.generation_id,
                page_number=page_number,
                page_url=fetched_page.final_url,
                page_fingerprint=fetched_page.page_fingerprint,
                anchor=fetched_page.anchor,
                next_url=fetched_page.next_url,
                edges=edge_payloads,
                terminal_evidence={
                    **(getattr(fetched_page, "terminal_evidence", {}) or {}),
                    "page_number": page_number,
                    "page_fingerprint": fetched_page.page_fingerprint,
                },
                claim_token=claim_token,
                expected_revision=generation.generation_revision,
            )
            # Publication always performs a separate final page-1/boundary
            # validation; staging is never treated as equivalent proof.
            resume_revalidated = False
            current_generation = get_active_mal_public_userrecs_generation(
                config.db_path, source_mal_anime_id=source_id
            )
            if current_generation is None:
                failures.append(
                    FullUserRecommendationHarvestFailure(
                        mal_anime_id=source_id,
                        title=source_title,
                        error="public userrecs generation disappeared after staging",
                        pages_fetched=page_number,
                        source_url=source_url,
                    )
                )
                break
            generation = current_generation

            if fetched_page.next_url is None:
                try:
                    generation = mark_mal_public_userrecs_generation_ready(
                        config.db_path, generation_id=generation.generation_id,
                        claim_token=claim_token, expected_revision=generation.generation_revision,
                    )
                    continue
                except ValueError as exc:
                    error = str(exc)
                    generation = _public_userrecs_pause_source(
                        config,
                        generation=generation,
                        cursor_url=generation.cursor_url,
                        error=error,
                        claim_token=claim_token,
                    )
                    record_mal_recommendation_harvest_failure(
                        config.db_path,
                        source_mal_anime_id=source_id,
                        source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                        error=error,
                        pages_fetched=generation.pages_fetched,
                        source_url=generation.source_url or source_url,
                    )
                    failures.append(
                        FullUserRecommendationHarvestFailure(
                            mal_anime_id=source_id,
                            title=source_title,
                            error=error,
                            pages_fetched=generation.pages_fetched,
                            source_url=generation.source_url or source_url,
                            generation_id=generation.generation_id,
                            paused=True,
                        )
                    )
                    break

            if pages_this_run >= normalized_max_pages:
                paused = _public_userrecs_pause_source(
                    config,
                    generation=generation,
                    cursor_url=fetched_page.next_url,
                    error="max_pages reached; staged generation paused with next-page cursor",
                    claim_token=claim_token,
                )
                paused_sources.append(
                    {
                        "mal_anime_id": source_id,
                        "title": source_title,
                        "generation_id": paused.generation_id,
                        "pages_fetched": paused.pages_fetched,
                        "cursor_url": paused.cursor_url,
                        "reason": "max_pages",
                    }
                )
                queue_outcome = "paused_page_budget"
                break

        if queue_class != "quarantined":
            release_mal_public_userrecs_source_claim(
                config.db_path,
                source_mal_anime_id=source_id,
                claim_token=claim_token,
                queue_class=queue_class,
                outcome=queue_outcome,
                generation_id=generation.generation_id,
                next_retry_at=queue_retry_at,
                error_code=queue_error_code,
            )

    if failures and (harvested or paused_sources):
        status = "partial"
    elif failures:
        status = "failed"
    elif paused_sources:
        status = "partial"
    else:
        status = "ok"
    return FullUserRecommendationHarvestSummary(
        status=status,
        seed_count=len(positive_entries),
        considered=len(selected_entries),
        harvested=harvested,
        failed=len(failures),
        skipped_fresh=skipped_fresh,
        total_edges=total_edges,
        forced=bool(force_refresh),
        stale_after_days=stale_after_days,
        max_pages=normalized_max_pages,
        failures=failures,
        harvested_sources=harvested_sources,
        paused_sources=paused_sources,
        restarted_sources=restarted_sources,
        queue_counts=queue_counts,
        selected_classes=selected_classes,
        quarantined_sources=quarantined_sources,
        fetch_attempted=fetch_counts["attempted"],
        fetch_succeeded=fetch_counts["succeeded"],
        fetch_failed=fetch_counts["failed"],
    )

def refresh_recommendation_metadata(
    config: AppConfig,
    *,
    limit: int | None = None,
    include_discovery_targets: bool = False,
    discovery_target_limit: int | None = None,
    harvest_stale_after_days: int = DEFAULT_HARVEST_STALE_AFTER_DAYS,
    metadata_stale_after_days: int = DEFAULT_METADATA_STALE_AFTER_DAYS,
    hot_stale_after_days: int = DEFAULT_HOT_METADATA_STALE_AFTER_DAYS,
    warm_stale_after_days: int = DEFAULT_WARM_METADATA_STALE_AFTER_DAYS,
    cold_stale_after_days: int = DEFAULT_COLD_METADATA_STALE_AFTER_DAYS,
    force_refresh: bool = False,
) -> MetadataRefreshSummary:
    mappings = list_series_mappings(config.db_path, approved_only=False)
    merge_mal_user_anime_list_cache_into_metadata(config.db_path)
    metadata_by_id = get_mal_anime_metadata_map(config.db_path)
    mapped_anime_ids = {int(mapping.mal_anime_id) for mapping in mappings}
    cached_list_entries = list_mal_user_anime_list_cache(config.db_path)
    positive_list_seed_ids = {
        entry.mal_anime_id
        for entry in cached_list_entries
        if (entry.list_status or "").strip().lower() in MAL_USER_LIST_POSITIVE_SEED_STATUSES
    }
    seed_anime_ids = mapped_anime_ids | positive_list_seed_ids
    seed_states = _load_mapped_seed_states(
        config.db_path,
        mapped_anime_ids=seed_anime_ids,
        metadata_by_id=metadata_by_id,
        positive_mal_list_seed_ids=positive_list_seed_ids,
        harvest_stale_after_days=harvest_stale_after_days,
        metadata_stale_after_days=metadata_stale_after_days,
    )
    eligible_seed_count = sum(1 for state in seed_states.values() if state.eligible)
    harvest_unharvested = sum(1 for state in seed_states.values() if state.eligible and state.harvest_status == "unharvested")
    harvest_stale = sum(1 for state in seed_states.values() if state.eligible and state.harvest_status == "stale")
    harvest_failed = sum(1 for state in seed_states.values() if state.eligible and state.harvest_status == "failed")
    ranked_ids = _rank_refresh_ids(sorted(seed_anime_ids), metadata_by_id, seed_states)
    refresh_tiers: dict[str, int] = {"hot": 0, "warm": 0, "cold": 0, "retry": 0}
    anime_ids: list[int] = []
    for anime_id in ranked_ids:
        state = seed_states.get(anime_id)
        metadata = metadata_by_id.get(anime_id)
        list_status = _my_list_status_value(metadata)
        if list_status == "watching":
            tier, horizon = "hot", hot_stale_after_days
        elif list_status in {"plan_to_watch", "on_hold"} or anime_id in mapped_anime_ids:
            tier, horizon = "warm", warm_stale_after_days
        else:
            tier, horizon = "cold", cold_stale_after_days
        needs_retry = metadata is None or state is None or state.harvest_status in HARVEST_RETRY_STATUSES
        metadata_due = periodic_evidence_is_due(
            successful_at=metadata.fetched_at if metadata else None,
            surface="recommendation_metadata",
            identity={"mal_anime_id": int(anime_id), "tier": tier},
            target_days=horizon,
            jitter_days=min(15, max(0, int(horizon))),
        )
        if force_refresh or needs_retry or metadata_due:
            anime_ids.append(anime_id)
            refresh_tiers["retry" if needs_retry else tier] += 1
    fresh_skipped = len(ranked_ids) - len(anime_ids)
    if limit is not None and limit > 0:
        anime_ids = anime_ids[:limit]

    client = MalClient(config, load_mal_secrets(config))
    if force_refresh:
        original_get_anime_details = client.get_anime_details
        def _forced_get_anime_details(anime_id: int, *, fields: str = "id,title,num_episodes,my_list_status") -> dict[str, Any]:
            return original_get_anime_details(anime_id, fields=fields, force_refresh=True)
        client.get_anime_details = _forced_get_anime_details  # type: ignore[assignment]
    refreshed = 0
    harvested_edge_count = 0
    failures: list[MetadataRefreshFailure] = []
    discovered_targets: dict[int, _DiscoveredTargetStats] = {}
    for anime_id in anime_ids:
        try:
            details = client.get_anime_details(anime_id, fields=DETAIL_FIELDS)
        except (MalApiError, TimeoutError) as exc:
            _record_harvest_failure(config.db_path, source_mal_anime_id=anime_id, error=str(exc))
            failures.append(MetadataRefreshFailure(mal_anime_id=anime_id, stage="mapped_metadata", error=str(exc)))
            continue
        _metadata_payload_from_details(config.db_path, mal_anime_id=anime_id, details=details)
        relations_payload: list[dict[str, Any]] = []
        for relation in details.get("related_anime") or []:
            if not isinstance(relation, dict):
                continue
            node = relation.get("node") or {}
            if not isinstance(node, dict) or not isinstance(node.get("id"), int):
                continue
            relation_type = relation.get("relation_type")
            if not isinstance(relation_type, str) or not relation_type:
                continue
            relations_payload.append(
                {
                    "related_mal_anime_id": int(node["id"]),
                    "relation_type": relation_type,
                    "relation_type_formatted": relation.get("relation_type_formatted"),
                    "related_title": node.get("title") if isinstance(node.get("title"), str) else None,
                    "raw": relation,
                }
            )
        replace_mal_anime_relations(config.db_path, mal_anime_id=anime_id, relations=relations_payload)

        recommendation_edges: list[dict[str, Any]] = []
        for rec in details.get("recommendations") or []:
            if not isinstance(rec, dict):
                continue
            node = rec.get("node") or {}
            if not isinstance(node, dict) or not isinstance(node.get("id"), int):
                continue
            target_id = int(node["id"])
            target_title = node.get("title") if isinstance(node.get("title"), str) else None
            num_recs = int(rec["num_recommendations"]) if isinstance(rec.get("num_recommendations"), int) else 0
            recommendation_edges.append(
                {
                    "target_mal_anime_id": target_id,
                    "target_title": target_title,
                    "num_recommendations": num_recs if num_recs > 0 else None,
                    "raw": rec,
                }
            )
            discovered_targets.setdefault(target_id, _DiscoveredTargetStats()).observe(
                title=target_title,
                num_recommendations=num_recs,
            )
        replaced_edges = replace_mal_recommendation_edges(
            config.db_path,
            source_mal_anime_id=anime_id,
            hop_distance=1,
            edges=recommendation_edges,
            source_type=MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
            complete=False,
        )
        if replaced_edges:
            harvested_edge_count += len(recommendation_edges)
        refreshed += 1

    discovery_considered = 0
    discovery_refreshed = 0
    target_hydration_skip_reasons: dict[str, int] = {}
    if include_discovery_targets and discovered_targets:
        ranked_targets = sorted(
            discovered_targets.items(),
            key=lambda item: (
                _metadata_age_sort_value(getattr(metadata_by_id.get(item[0]), "fetched_at", None)),
                -item[1].supporting_sources,
                -item[1].total_recommendation_votes,
                -item[1].cross_seed_support_votes,
                item[1].best_single_source_votes,
                item[0],
            ),
        )
        hydratable_targets: list[tuple[int, _DiscoveredTargetStats]] = []
        for target_id, info in ranked_targets:
            if target_id in seed_anime_ids:
                _record_target_skip(target_hydration_skip_reasons, "already_mapped")
                continue
            if _has_my_list_status(metadata_by_id.get(target_id)):
                _record_target_skip(target_hydration_skip_reasons, "already_listed")
                continue
            hydratable_targets.append((target_id, info))
        if discovery_target_limit is not None and discovery_target_limit > 0:
            hydratable_targets = hydratable_targets[:discovery_target_limit]
        discovery_considered = len(hydratable_targets)
        for target_id, _info in hydratable_targets:
            try:
                details = client.get_anime_details(
                    target_id,
                    fields=DISCOVERY_DETAIL_FIELDS,
                )
            except (MalApiError, TimeoutError) as exc:
                failures.append(MetadataRefreshFailure(mal_anime_id=target_id, stage="discovery_metadata", error=str(exc)))
                continue
            _metadata_payload_from_details(config.db_path, mal_anime_id=target_id, details=details)
            discovery_refreshed += 1
    merge_mal_user_anime_list_cache_into_metadata(config.db_path)
    return MetadataRefreshSummary(
        considered=len(anime_ids),
        refreshed=refreshed,
        discovery_considered=discovery_considered,
        discovery_refreshed=discovery_refreshed,
        failures=failures,
        eligible_seed_count=eligible_seed_count,
        harvest_unharvested=harvest_unharvested,
        harvest_stale=harvest_stale,
        harvest_failed=harvest_failed,
        harvested_edge_count=harvested_edge_count,
        target_hydration_skip_reasons=target_hydration_skip_reasons,
        fresh_skipped=fresh_skipped,
        refresh_tiers=refresh_tiers,
    )
