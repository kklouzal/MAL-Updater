from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import uuid
from pathlib import Path
from typing import Any

from .config import AppConfig, load_mal_secrets
from .db import (
    MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
    MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
    MalAnimeMetadata,
    MalUserAnimeListRefreshSummary,
    abort_mal_user_anime_list_cache_refresh,
    begin_mal_user_anime_list_cache_refresh,
    connect,
    finalize_mal_user_anime_list_cache_refresh,
    get_mal_anime_metadata_map,
    list_mal_user_anime_list_cache,
    merge_mal_user_anime_list_cache_into_metadata,
    list_series_mappings,
    record_mal_recommendation_harvest_failure,
    replace_mal_anime_relations,
    replace_mal_recommendation_edges,
    upsert_mal_anime_metadata,
    upsert_mal_user_anime_list_cache_generation,
)
from .mal_client import MalApiError, MalClient
from .mal_user_recommendations import (
    DEFAULT_PUBLIC_USER_RECS_MAX_BODY_BYTES,
    DEFAULT_PUBLIC_USER_RECS_MAX_PAGES,
    PublicMalUserRecommendationsClient,
    PublicMalUserRecommendationsError,
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
DEFAULT_HARVEST_STALE_AFTER_DAYS = 30
DEFAULT_METADATA_STALE_AFTER_DAYS = 14
DEFAULT_HOT_METADATA_STALE_AFTER_DAYS = 3
DEFAULT_WARM_METADATA_STALE_AFTER_DAYS = 14
DEFAULT_COLD_METADATA_STALE_AFTER_DAYS = 90
DEFAULT_FULL_USER_RECOMMENDATION_HARVEST_STALE_AFTER_DAYS = 45
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "mal_anime_id": self.mal_anime_id,
            "title": self.title,
            "error": self.error,
            "pages_fetched": self.pages_fetched,
            "source_url": self.source_url,
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "seed_count": self.seed_count,
            "considered": self.considered,
            "harvested": self.harvested,
            "failed": self.failed,
            "skipped_fresh": self.skipped_fresh,
            "total_edges": self.total_edges,
            "forced": self.forced,
            "stale_after_days": self.stale_after_days,
            "max_pages": self.max_pages,
            "failures": [failure.as_dict() for failure in self.failures],
            "harvested_sources": list(self.harvested_sources),
            "semantics": {
                "source": "public_mal_userrecs_html",
                "complete_when_no_next_link": True,
                "partial_failure_preserves_existing_edges": True,
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


def refresh_mal_user_anime_list_cache(
    config: AppConfig,
    *,
    statuses: list[str] | tuple[str, ...] | None = None,
    page_size: int = 100,
    max_pages: int | None = 25,
    prune_on_complete: bool = False,
) -> MalUserAnimeListRefreshSummary:
    """Refresh the official read-only MAL @me anime list cache generation-safely.

    Bounded partial page runs upsert seen rows and retain all older rows; failures
    do not alter the existing cache. Absent rows are pruned only when the caller
    explicitly opts in and MAL pagination reaches a terminal page.
    """
    max_pages = 25 if max_pages is None or max_pages <= 0 else int(max_pages)
    normalized_statuses = _normalized_user_list_statuses(statuses)
    all_statuses = normalized_statuses == [None]
    refresh_run_id = str(uuid.uuid4())
    fetched_at = _now_iso()
    refresh = begin_mal_user_anime_list_cache_refresh(
        config.db_path,
        refresh_run_id=refresh_run_id,
        fetched_at=fetched_at,
    )
    client = MalClient(config, load_mal_secrets(config))
    collected: list[dict[str, Any]] = []
    by_status: Counter[str] = Counter()
    scored = 0
    unscored = 0
    pages = 0
    last_payload: dict[str, Any] | None = None
    try:
        for index, status in enumerate(normalized_statuses):
            status_pages = 0
            remaining_pages = max(max_pages - pages, 0)
            if remaining_pages <= 0:
                break
            for payload in client.iter_my_anime_list_pages(
                status=status,
                limit=page_size,
                fields=MAL_USER_LIST_FIELDS,
                max_pages=remaining_pages,
            ):
                pages += 1
                status_pages += 1
                last_payload = payload
                data = payload.get("data") if isinstance(payload, dict) else None
                if not isinstance(data, list):
                    continue
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    collected.append(item)
                    list_status = item.get("list_status") if isinstance(item.get("list_status"), dict) else {}
                    status_value = list_status.get("status")
                    if isinstance(status_value, str) and status_value.strip():
                        by_status[status_value.strip().lower()] += 1
                    score = list_status.get("score")
                    if isinstance(score, int) and score > 0:
                        scored += 1
                    else:
                        unscored += 1
            reached_budget_before_terminal = pages >= max_pages and _payload_has_next_page(last_payload)
            statuses_left_unfetched = pages >= max_pages and index < len(normalized_statuses) - 1
            if reached_budget_before_terminal or statuses_left_unfetched:
                summary = upsert_mal_user_anime_list_cache_generation(
                    config.db_path,
                    items=collected,
                    refresh_run_id=refresh.refresh_run_id,
                    generation=refresh.generation,
                    fetched_at=refresh.fetched_at,
                )
                summary.status = "partial"
                summary.pages = pages
                summary.scored = scored
                summary.unscored = unscored
                summary.by_status = dict(by_status)
                summary.partial = True
                summary.error = "max_pages reached before MAL anime list pagination completed; seen rows upserted and absent rows retained"
                summary.metadata_rows_with_my_list_status = merge_mal_user_anime_list_cache_into_metadata(config.db_path)
                return summary
    except (MalApiError, TimeoutError, ValueError) as exc:
        summary = abort_mal_user_anime_list_cache_refresh(
            config.db_path,
            refresh_run_id=refresh.refresh_run_id,
            generation=refresh.generation,
            error=str(exc),
        )
        summary.status = "failed"
        summary.pages = pages
        summary.items = len(collected)
        summary.scored = scored
        summary.unscored = unscored
        summary.by_status = dict(by_status)
        summary.partial = True
        return summary
    upsert = upsert_mal_user_anime_list_cache_generation(
        config.db_path,
        items=collected,
        refresh_run_id=refresh.refresh_run_id,
        generation=refresh.generation,
        fetched_at=refresh.fetched_at,
    )
    summary = finalize_mal_user_anime_list_cache_refresh(
        config.db_path,
        refresh_run_id=refresh.refresh_run_id,
        generation=refresh.generation,
        proven_complete=True,
        delete_absent=bool(all_statuses and prune_on_complete),
    )
    summary.metadata_rows_with_my_list_status = merge_mal_user_anime_list_cache_into_metadata(config.db_path)
    summary.pages = pages
    summary.items = upsert.items
    summary.upserted = upsert.upserted
    summary.scored = scored
    summary.unscored = unscored
    summary.by_status = dict(by_status)
    return summary


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


def _full_harvest_candidate_status(row: dict[str, Any] | None, *, stale_after_days: int) -> str:
    if row is None:
        return "unharvested"
    if str(row.get("status") or "") == "failed":
        return "failed"
    if not bool(row.get("is_complete")) or str(row.get("source_type") or "") != MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS:
        return "unharvested"
    return "stale" if _is_stale(row.get("fetched_at"), stale_after_days=stale_after_days) else "fresh"


def _full_harvest_rank_key(entry: Any, status_row: dict[str, Any] | None, *, stale_after_days: int) -> tuple[int, tuple[int, str], int]:
    status = _full_harvest_candidate_status(status_row, stale_after_days=stale_after_days)
    status_order = {"unharvested": 0, "failed": 1, "stale": 2, "fresh": 3}.get(status, 4)
    if status == "failed":
        age = _metadata_age_sort_value(status_row.get("last_attempted_at") if status_row else None)
    else:
        age = _metadata_age_sort_value(status_row.get("fetched_at") if status_row else None)
    return (status_order, age, int(entry.mal_anime_id))


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
    """Bounded cold path for complete public MAL user-recommendation aggregates.

    Seeds come only from the cached official MAL @me anime-list positive states
    (completed/watching/on_hold). Provider-only mappings are intentionally not a
    full-harvest source of truth. Each source is atomically replaced only after
    public MAL pagination reaches a terminal page; malformed/looped/truncated
    pages are recorded as failed attempts and preserve existing graph data.
    """
    stale_after_days = max(1, int(stale_after_days))
    normalized_max_pages = max(1, int(max_pages))
    normalized_max_body_bytes = max(1024, int(max_body_bytes))
    merge_mal_user_anime_list_cache_into_metadata(config.db_path)
    positive_entries = list_mal_user_anime_list_cache(config.db_path, statuses=MAL_USER_LIST_POSITIVE_SEED_STATUSES)
    source_ids = {int(entry.mal_anime_id) for entry in positive_entries}
    status_rows = _full_harvest_status_rows(config.db_path, source_ids)
    ranked_entries = sorted(
        positive_entries,
        key=lambda entry: _full_harvest_rank_key(entry, status_rows.get(int(entry.mal_anime_id)), stale_after_days=stale_after_days),
    )
    stale_or_missing_entries = [
        entry
        for entry in ranked_entries
        if force_refresh
        or _full_harvest_candidate_status(status_rows.get(int(entry.mal_anime_id)), stale_after_days=stale_after_days)
        != "fresh"
    ]
    skipped_fresh = len(ranked_entries) - len(stale_or_missing_entries)
    selected_entries = stale_or_missing_entries
    if limit is not None and limit > 0:
        selected_entries = selected_entries[: int(limit)]

    harvest_client = client or PublicMalUserRecommendationsClient(config)
    failures: list[FullUserRecommendationHarvestFailure] = []
    harvested_sources: list[dict[str, Any]] = []
    harvested = 0
    total_edges = 0
    for entry in selected_entries:
        try:
            result = harvest_client.harvest(
                int(entry.mal_anime_id),
                source_title=entry.title,
                max_pages=normalized_max_pages,
                max_body_bytes=normalized_max_body_bytes,
            )
        except PublicMalUserRecommendationsError as exc:
            record_mal_recommendation_harvest_failure(
                config.db_path,
                source_mal_anime_id=int(entry.mal_anime_id),
                source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                error=str(exc),
            )
            failures.append(
                FullUserRecommendationHarvestFailure(
                    mal_anime_id=int(entry.mal_anime_id),
                    title=entry.title,
                    error=str(exc),
                )
            )
            continue
        if not result.complete or result.partial or result.status != "ok":
            error = result.error or "public MAL userrecs harvest did not prove completeness"
            record_mal_recommendation_harvest_failure(
                config.db_path,
                source_mal_anime_id=int(entry.mal_anime_id),
                source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                error=error,
                pages_fetched=result.pages_fetched,
                source_url=result.source_url,
            )
            failures.append(
                FullUserRecommendationHarvestFailure(
                    mal_anime_id=int(entry.mal_anime_id),
                    title=entry.title,
                    error=error,
                    pages_fetched=result.pages_fetched,
                    source_url=result.source_url,
                )
            )
            continue
        edge_payloads = [
            edge.as_edge_payload(source_url=result.source_url or "", page_count=result.pages_fetched)
            for edge in result.edges
        ]
        replaced = replace_mal_recommendation_edges(
            config.db_path,
            source_mal_anime_id=int(entry.mal_anime_id),
            hop_distance=1,
            edges=edge_payloads,
            source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
            complete=True,
            pages_fetched=result.pages_fetched,
            source_url=result.source_url,
        )
        if replaced:
            harvested += 1
            total_edges += len(edge_payloads)
            harvested_sources.append(
                {
                    "mal_anime_id": int(entry.mal_anime_id),
                    "title": entry.title,
                    "edge_count": len(edge_payloads),
                    "pages_fetched": result.pages_fetched,
                    "source_url": result.source_url,
                }
            )

    if failures and harvested:
        status = "partial"
    elif failures:
        status = "failed"
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
        if force_refresh or needs_retry or _is_stale(metadata.fetched_at if metadata else None, stale_after_days=horizon):
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
        client.get_anime_details = _forced_get_anime_details  # type: ignore[method-assign]
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
