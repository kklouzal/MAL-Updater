from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import uuid
from pathlib import Path
from typing import Any

from .config import AppConfig, load_mal_secrets
from .db import (
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
    replace_mal_anime_relations,
    replace_mal_recommendation_edges,
    upsert_mal_anime_metadata,
    upsert_mal_user_anime_list_cache_generation,
)
from .mal_client import MalApiError, MalClient

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
    "statistics",
    "start_season",
    "source",
    "genres",
    "studios",
    "related_anime",
    "recommendations",
    "my_list_status",
)
DETAIL_FIELDS = ",".join(DETAIL_FIELD_NAMES)
DISCOVERY_DETAIL_FIELDS = ",".join(field for field in DETAIL_FIELD_NAMES if field not in {"related_anime", "recommendations"})
DEFAULT_HARVEST_STALE_AFTER_DAYS = 14
DEFAULT_METADATA_STALE_AFTER_DAYS = 14
MAL_USER_LIST_POSITIVE_SEED_STATUSES = frozenset({"completed", "watching", "on_hold"})
MAL_USER_LIST_SUPPRESSION_STATUSES = frozenset({"completed", "watching", "on_hold", "dropped", "plan_to_watch"})
MAL_USER_LIST_FIELDS = "list_status,num_episodes,media_type,status"
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
            "failed": len(failures),
            "failures": [failure.as_dict() for failure in failures],
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
            SELECT source_mal_anime_id, status, num_edges, fetched_at
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

        if status_row is None and edge_row is None:
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


def _record_harvest_failure(db_path: Path, *, source_mal_anime_id: int) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO mal_recommendation_harvest_status (source_mal_anime_id, status, num_edges, fetched_at)
            VALUES (
                ?,
                'failed',
                COALESCE((SELECT COUNT(*) FROM mal_anime_recommendations WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'), 0),
                CURRENT_TIMESTAMP
            )
            ON CONFLICT(source_mal_anime_id) DO UPDATE SET
                status = 'failed',
                num_edges = COALESCE((SELECT COUNT(*) FROM mal_anime_recommendations WHERE source_mal_anime_id = excluded.source_mal_anime_id AND source_kind = 'mal_recommendation'), mal_recommendation_harvest_status.num_edges),
                fetched_at = CURRENT_TIMESTAMP
            """,
            (int(source_mal_anime_id), int(source_mal_anime_id)),
        )
        conn.commit()


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

def refresh_recommendation_metadata(
    config: AppConfig,
    *,
    limit: int | None = None,
    include_discovery_targets: bool = False,
    discovery_target_limit: int | None = None,
    harvest_stale_after_days: int = DEFAULT_HARVEST_STALE_AFTER_DAYS,
    metadata_stale_after_days: int = DEFAULT_METADATA_STALE_AFTER_DAYS,
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
    anime_ids = _rank_refresh_ids(sorted(seed_anime_ids), metadata_by_id, seed_states)
    if limit is not None and limit > 0:
        anime_ids = anime_ids[:limit]

    client = MalClient(config, load_mal_secrets(config))
    refreshed = 0
    harvested_edge_count = 0
    failures: list[MetadataRefreshFailure] = []
    discovered_targets: dict[int, _DiscoveredTargetStats] = {}
    for anime_id in anime_ids:
        try:
            details = client.get_anime_details(anime_id, fields=DETAIL_FIELDS)
        except (MalApiError, TimeoutError) as exc:
            _record_harvest_failure(config.db_path, source_mal_anime_id=anime_id)
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
        replace_mal_recommendation_edges(
            config.db_path,
            source_mal_anime_id=anime_id,
            hop_distance=1,
            edges=recommendation_edges,
        )
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
    )
