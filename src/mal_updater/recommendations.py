from __future__ import annotations

from collections import defaultdict
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .config import AppConfig
from .db import get_mal_anime_metadata_map, get_mal_anime_relations_map, get_mal_recommendation_edges_map, list_series_mappings
from .mapping import normalize_title
from .sync_planner import ProviderSeriesState, load_provider_series_states

_ENGLISH_DUB_RE = re.compile(r"\benglish dub\b|\(dub\)", re.IGNORECASE)
_FOREIGN_DUB_RE = re.compile(r"\b(?:french|german|spanish|portuguese|italian|arabic|hindi) dub\b", re.IGNORECASE)
_SEASON_NUMBER_RE = re.compile(r"\bseason\s*(\d+)\b", re.IGNORECASE)
_ORDINAL_SEASON_RE = re.compile(
    r"\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d+(?:st|nd|rd|th))\s+season\b",
    re.IGNORECASE,
)
_FINAL_SEASON_RE = re.compile(r"\bfinal\s+season\b", re.IGNORECASE)
_PART_RE = re.compile(r"\bpart\s*(\d+)\b", re.IGNORECASE)
_ROMAN_END_RE = re.compile(r"\b([ivx]{1,5})\b(?=\s*(?:\(|$))", re.IGNORECASE)

_ORDINALS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
}

_ROMAN = {
    "i": 1,
    "ii": 2,
    "iii": 3,
    "iv": 4,
    "v": 5,
    "vi": 6,
    "vii": 7,
    "viii": 8,
    "ix": 9,
    "x": 10,
}

_FRESH_DUBBED_EPISODE_WINDOW_DAYS = 21

_SEASON_ORDER = {
    "winter": 0,
    "spring": 1,
    "summer": 2,
    "fall": 3,
}


@dataclass(slots=True)
class Recommendation:
    kind: str
    priority: int
    provider_series_id: str
    title: str
    season_title: str | None
    provider: str | None = None
    reasons: list[str] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)

    def available_providers(self) -> list[str]:
        raw = self.context.get("available_via_providers")
        if isinstance(raw, list):
            providers = sorted({value for value in raw if isinstance(value, str) and value.strip()})
            if providers:
                return providers
        if isinstance(self.provider, str) and self.provider.strip():
            return [self.provider]
        return ["unknown"]

    def as_dict(self) -> dict[str, Any]:
        providers = self.available_providers()
        return {
            "kind": self.kind,
            "priority": self.priority,
            "provider": self.provider,
            "providers": providers,
            "provider_count": len(providers),
            "multi_provider": len(providers) > 1,
            "provider_label": _format_provider_label(providers),
            "provider_series_id": self.provider_series_id,
            "title": self.title,
            "season_title": self.season_title,
            "reasons": self.reasons,
            "context": self.context,
        }


@dataclass(slots=True, frozen=True)
class RecommendationSectionDefinition:
    key: str
    title: str
    kinds: tuple[str, ...]
    description: str


_RECOMMENDATION_SECTIONS: tuple[RecommendationSectionDefinition, ...] = (
    RecommendationSectionDefinition(
        key="continue_next",
        title="Continue next",
        kinds=("new_season",),
        description="Later-season continuations that look ready because an earlier installment appears completed.",
    ),
    RecommendationSectionDefinition(
        key="fresh_dubbed_episodes",
        title="Fresh dubbed episodes",
        kinds=("new_dubbed_episode",),
        description="Recent contiguous tail gaps that look like actual new dubbed episode availability.",
    ),
    RecommendationSectionDefinition(
        key="discovery_candidates",
        title="Discovery candidates",
        kinds=("discovery_candidate",),
        description="Unmapped titles supported by cached MAL recommendation-graph evidence.",
    ),
    RecommendationSectionDefinition(
        key="resume_backlog",
        title="Resume backlog",
        kinds=("resume_backlog",),
        description="Older tail gaps that look more like backlog continuation than fresh release alerts.",
    ),
)


_PROVIDER_DISPLAY_NAMES = {
    "crunchyroll": "Crunchyroll",
    "hidive": "HIDIVE",
    "mal": "MyAnimeList",
    "unknown": "Unknown",
}


def _format_provider_label(providers: list[str]) -> str:
    names = [_PROVIDER_DISPLAY_NAMES.get(provider, provider.replace("_", " ").title()) for provider in providers]
    return " + ".join(names)


def _section_provider_metadata(items: list[Recommendation]) -> dict[str, Any]:
    provider_counts: dict[str, int] = {}
    multi_provider_item_count = 0
    for item in items:
        providers = item.available_providers()
        if len(providers) > 1:
            multi_provider_item_count += 1
        for provider in providers:
            provider_counts[provider] = provider_counts.get(provider, 0) + 1
    providers = sorted(provider_counts)
    mixed = len(providers) > 1
    payload: dict[str, Any] = {
        "providers": providers,
        "provider_counts": provider_counts,
        "provider_label": _format_provider_label(providers),
        "mixed_providers": mixed,
        "multi_provider_item_count": multi_provider_item_count,
    }
    if mixed:
        payload["mixed_provider_note"] = "This section contains recommendations available across multiple providers."
    return payload


def _availability_priority_bonus(provider_count: int) -> int:
    if provider_count <= 1:
        return 0
    return min((provider_count - 1) * 3, 9)



def _recommendation_sort_key(item: Recommendation) -> tuple[int, int, str, str]:
    return (-item.priority, -len(item.available_providers()), item.title.lower(), item.provider_series_id)


def group_recommendations(items: list[Recommendation]) -> list[dict[str, Any]]:
    items_by_kind: dict[str, list[Recommendation]] = defaultdict(list)
    for item in items:
        items_by_kind[item.kind].append(item)

    sections: list[dict[str, Any]] = []
    consumed_kinds: set[str] = set()
    for section in _RECOMMENDATION_SECTIONS:
        section_items: list[Recommendation] = []
        for kind in section.kinds:
            section_items.extend(items_by_kind.get(kind, []))
            consumed_kinds.add(kind)
        if not section_items:
            continue
        section_items.sort(key=_recommendation_sort_key)
        sections.append(
            {
                "key": section.key,
                "title": section.title,
                "description": section.description,
                "kinds": list(section.kinds),
                "count": len(section_items),
                **_section_provider_metadata(section_items),
                "items": [item.as_dict() for item in section_items],
            }
        )

    remaining_items: list[Recommendation] = []
    for item in items:
        if item.kind not in consumed_kinds:
            remaining_items.append(item)
    if remaining_items:
        remaining_items.sort(key=_recommendation_sort_key)
        sections.append(
            {
                "key": "other",
                "title": "Other",
                "description": "Recommendation kinds that do not yet have a dedicated named section.",
                "kinds": sorted({item.kind for item in remaining_items}),
                "count": len(remaining_items),
                **_section_provider_metadata(remaining_items),
                "items": [item.as_dict() for item in remaining_items],
            }
        )
    return sections


def build_recommendations(config: AppConfig, limit: int | None = 20) -> list[Recommendation]:
    states = load_provider_series_states(config, limit=None)
    state_by_id = {(state.provider, state.provider_series_id): state for state in states}
    mapping_by_series = {
        (mapping.provider, mapping.provider_series_id): int(mapping.mal_anime_id)
        for mapping in list_series_mappings(config.db_path, approved_only=False)
    }
    metadata_by_id = get_mal_anime_metadata_map(config.db_path)
    relations_by_id = get_mal_anime_relations_map(config.db_path)
    recommendation_edges_by_id = get_mal_recommendation_edges_map(config.db_path)

    recommendations: list[Recommendation] = []
    recommendations.extend(
        _build_relation_backed_new_season_recommendations(
            states,
            state_by_id=state_by_id,
            mapping_by_series=mapping_by_series,
            metadata_by_id=metadata_by_id,
            relations_by_id=relations_by_id,
        )
    )
    recommendations.extend(_build_new_season_recommendations(states))
    recommendations.extend(
        _build_discovery_recommendations(
            states,
            mapping_by_series=mapping_by_series,
            metadata_by_id=metadata_by_id,
            relations_by_id=relations_by_id,
            recommendation_edges_by_id=recommendation_edges_by_id,
        )
    )
    recommendations.extend(_build_new_episode_recommendations(states))
    recommendations = _dedupe_recommendations(recommendations)
    recommendations = _merge_cross_provider_recommendations(recommendations, mapping_by_series=mapping_by_series)
    recommendations.sort(key=_recommendation_sort_key)
    if limit is None or limit <= 0:
        return recommendations
    return recommendations[:limit]


def _build_new_episode_recommendations(states: list[ProviderSeriesState]) -> list[Recommendation]:
    items: list[Recommendation] = []
    for state in states:
        if not _is_english_dub_series(state):
            continue
        if state.completed_episode_count <= 0:
            continue
        if state.max_episode_number is None:
            continue

        tail_gap = _contiguous_tail_gap(state)
        if tail_gap is None or tail_gap <= 0:
            continue

        days_since_watch = _days_since(state.last_watched_at)
        is_recent = days_since_watch is not None and days_since_watch <= _FRESH_DUBBED_EPISODE_WINDOW_DAYS
        kind = "new_dubbed_episode" if is_recent else "resume_backlog"
        reasons = [
            "English dub is available",
            f"{tail_gap} contiguous episode(s) appear beyond your completed tail progress",
        ]
        if kind == "resume_backlog":
            reasons.append("this looks more like a backlog continuation than a fresh release alert")
        if state.last_watched_at:
            reasons.append(f"most recent watch activity: {state.last_watched_at}")
        priority = _episode_recommendation_priority(state, tail_gap, kind)
        items.append(
            Recommendation(
                kind=kind,
                priority=priority,
                provider=state.provider,
                provider_series_id=state.provider_series_id,
                title=state.title,
                season_title=state.season_title,
                reasons=reasons,
                context={
                    "provider": state.provider,
                    "completed_episode_count": state.completed_episode_count,
                    "max_episode_number": state.max_episode_number,
                    "max_completed_episode_number": state.max_completed_episode_number,
                    "contiguous_tail_gap": tail_gap,
                    "watchlist_status": state.watchlist_status,
                    "last_watched_at": state.last_watched_at,
                    "days_since_last_watch": days_since_watch,
                },
            )
        )
    return items


def _contiguous_tail_gap(state: ProviderSeriesState) -> int | None:
    if state.max_episode_number is None or state.max_completed_episode_number is None:
        return None
    if state.max_completed_episode_number >= state.max_episode_number:
        return None
    if state.completed_episode_count != state.max_completed_episode_number:
        return None
    return state.max_episode_number - state.max_completed_episode_number


def _episode_recommendation_priority(state: ProviderSeriesState, tail_gap: int, kind: str) -> int:
    base = 55 if kind == "new_dubbed_episode" else 30
    priority = base - min(tail_gap, 10)
    if tail_gap == 1:
        priority += 10
    if state.watchlist_status == "in_progress":
        priority += 12
    elif state.watchlist_status == "fully_watched":
        priority -= 20
    priority += _freshness_boost(state.last_watched_at, kind)
    return priority


def _days_since(last_watched_at: str | None) -> int | None:
    if not last_watched_at:
        return None
    try:
        watched = datetime.fromisoformat(last_watched_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    now = datetime.now(timezone.utc)
    return max((now - watched).days, 0)


def _freshness_boost(last_watched_at: str | None, kind: str) -> int:
    days = _days_since(last_watched_at)
    if days is None:
        return -8 if kind == "new_dubbed_episode" else -2
    if days <= 7:
        return 10 if kind == "new_dubbed_episode" else 3
    if days <= 30:
        return 6 if kind == "new_dubbed_episode" else 1
    if days <= 90:
        return 2 if kind == "new_dubbed_episode" else 0
    if days <= 365:
        return -4 if kind == "new_dubbed_episode" else -2
    return -12 if kind == "new_dubbed_episode" else -4


def _dedupe_recommendations(items: list[Recommendation]) -> list[Recommendation]:
    best: dict[tuple[str, str, str], Recommendation] = {}
    for item in items:
        key = (item.kind, item.provider or "", item.provider_series_id)
        existing = best.get(key)
        if existing is None or item.priority > existing.priority:
            best[key] = item
    return list(best.values())


_CROSS_PROVIDER_MERGE_KINDS = frozenset({"new_season", "new_dubbed_episode", "resume_backlog"})


def _merge_cross_provider_recommendations(
    items: list[Recommendation], *, mapping_by_series: dict[tuple[str, str], int]
) -> list[Recommendation]:
    grouped: dict[tuple[str, int], list[Recommendation]] = defaultdict(list)
    passthrough: list[Recommendation] = []
    for item in items:
        provider = item.provider
        if provider is None or provider == "mal" or item.kind not in _CROSS_PROVIDER_MERGE_KINDS:
            passthrough.append(item)
            continue
        mal_anime_id = mapping_by_series.get((provider, item.provider_series_id))
        if mal_anime_id is None:
            passthrough.append(item)
            continue
        grouped[(item.kind, mal_anime_id)].append(item)

    merged: list[Recommendation] = list(passthrough)
    for _, bucket in grouped.items():
        if len(bucket) == 1:
            merged.extend(bucket)
            continue
        bucket.sort(key=lambda item: (-item.priority, item.provider or "", item.provider_series_id))
        primary = bucket[0]
        alternates = bucket[1:]
        merged_reasons = list(primary.reasons)
        for alt in alternates:
            for reason in alt.reasons:
                if reason not in merged_reasons:
                    merged_reasons.append(reason)
        merged_context = dict(primary.context)
        available_via_providers = sorted({item.provider for item in bucket if item.provider})
        availability_priority_bonus = _availability_priority_bonus(len(available_via_providers))
        merged_context["cross_provider_merged"] = True
        merged_context["available_via_providers"] = available_via_providers
        merged_context["availability_priority_bonus"] = availability_priority_bonus
        merged_context["alternate_provider_series"] = [
            {
                "provider": item.provider,
                "provider_series_id": item.provider_series_id,
                "title": item.title,
                "season_title": item.season_title,
                "priority": item.priority,
            }
            for item in alternates
        ]
        if availability_priority_bonus > 0:
            provider_label = _format_provider_label(available_via_providers)
            merged_reasons.append(f"available via multiple providers: {provider_label}")
        merged.append(
            Recommendation(
                kind=primary.kind,
                priority=primary.priority + availability_priority_bonus,
                provider=primary.provider,
                provider_series_id=primary.provider_series_id,
                title=primary.title,
                season_title=primary.season_title,
                reasons=merged_reasons,
                context=merged_context,
            )
        )
    return merged


def _metadata_named_list_values(meta: Any, field: str) -> set[str]:
    if meta is None or not isinstance(getattr(meta, "raw", None), dict):
        return set()
    values: set[str] = set()
    for item in meta.raw.get(field) or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if isinstance(name, str) and name.strip():
            values.add(name.strip())
    return values


def _metadata_genre_names(meta: Any) -> set[str]:
    return _metadata_named_list_values(meta, "genres")


def _metadata_studio_names(meta: Any) -> set[str]:
    return _metadata_named_list_values(meta, "studios")


def _metadata_source_value(meta: Any) -> str | None:
    raw = meta.raw if meta is not None and isinstance(getattr(meta, "raw", None), dict) else None
    value = raw.get("source") if isinstance(raw, dict) else None
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _discovery_metadata_affinity_bonus(
    *,
    genre_overlap_score: int,
    studio_overlap_score: int,
    source_overlap_score: int,
    popularity: int | None,
) -> tuple[int, int]:
    matched_dimensions = sum(
        1
        for score in (genre_overlap_score, studio_overlap_score, source_overlap_score)
        if score > 0
    )
    if matched_dimensions <= 1:
        return 0, matched_dimensions
    bonus = 2
    if matched_dimensions >= 3:
        bonus += 2
    if genre_overlap_score >= 6:
        bonus += 1
    if popularity is not None:
        if popularity <= 500:
            bonus += 1
        elif popularity <= 2000:
            bonus += 0
    return min(bonus, 6), matched_dimensions


def _discovery_metadata_quality_bonus(
    *,
    matched_dimensions: int,
    mean: float | None,
    popularity: int | None,
) -> tuple[int, str | None, str | None]:
    if matched_dimensions <= 1:
        return 0, None, None
    bonus = 0
    mean_band: str | None = None
    popularity_band: str | None = None
    if mean is not None:
        if mean >= 8.6:
            bonus += 2
            mean_band = "elite"
        elif mean >= 8.2:
            bonus += 1
            mean_band = "strong"
    if popularity is not None and popularity <= 300:
        bonus += 1
        popularity_band = "broad"
    return min(bonus, 3), mean_band, popularity_band


def _discovery_catalog_quality_bonus(*, mean: float | None, popularity: int | None) -> tuple[int, str | None, str | None]:
    bonus = 0
    mean_band: str | None = None
    popularity_band: str | None = None
    if mean is not None:
        if mean >= 8.6:
            bonus += 2
            mean_band = "elite"
        elif mean >= 8.3:
            bonus += 1
            mean_band = "strong"
    if popularity is not None:
        if popularity <= 150:
            bonus += 2
            popularity_band = "very_broad"
        elif popularity <= 400:
            bonus += 1
            popularity_band = "broad"
    return min(bonus, 3), mean_band, popularity_band


def _metadata_start_season(meta: Any) -> dict[str, Any] | None:
    start_season = getattr(meta, "start_season", None) if meta is not None else None
    if not isinstance(start_season, dict):
        return None
    year = start_season.get("year")
    season = start_season.get("season")
    if not isinstance(year, int):
        return None
    if not isinstance(season, str):
        return None
    season = season.strip().lower()
    if season not in _SEASON_ORDER:
        return None
    return {"year": year, "season": season}


def _start_season_sort_key(start_season: dict[str, Any] | None) -> tuple[int, int] | None:
    if not isinstance(start_season, dict):
        return None
    year = start_season.get("year")
    season = start_season.get("season")
    if not isinstance(year, int):
        return None
    if not isinstance(season, str):
        return None
    season_index = _SEASON_ORDER.get(season.strip().lower())
    if season_index is None:
        return None
    return (year, season_index)


def _format_start_season(start_season: dict[str, Any] | None) -> str | None:
    sort_key = _start_season_sort_key(start_season)
    if sort_key is None:
        return None
    return f"{str(start_season['season']).strip().title()} {start_season['year']}"


def _discovery_candidate_freshness_profile(start_season: dict[str, Any] | None) -> tuple[int, str | None, int | None, int]:
    sort_key = _start_season_sort_key(start_season)
    if sort_key is None:
        return 0, None, None, 0
    current = datetime.now(timezone.utc)
    current_key = (current.year, (current.month - 1) // 3)
    age_in_seasons = (current_key[0] - sort_key[0]) * 4 + (current_key[1] - sort_key[1])
    if age_in_seasons <= 0:
        return 6, "current_or_upcoming", age_in_seasons, 0
    if age_in_seasons <= 8:
        return 5, "recent_two_years", age_in_seasons, 0
    if age_in_seasons <= 16:
        return 3, "recent_four_years", age_in_seasons, 0
    if age_in_seasons <= 28:
        return 1, "modern_catalog", age_in_seasons, 0
    if age_in_seasons <= 48:
        return 0, "aging_catalog", age_in_seasons, 2
    if age_in_seasons <= 80:
        return 0, "older_catalog", age_in_seasons, 4
    return 0, "legacy_catalog", age_in_seasons, 6


def _discovery_seed_recency_bonus(days_since_watch: int | None) -> int:
    if days_since_watch is None:
        return 0
    if days_since_watch <= 30:
        return 3
    if days_since_watch <= 90:
        return 2
    if days_since_watch <= 180:
        return 1
    return 0


def _discovery_seed_staleness_profile(days_since_watch: int | None) -> tuple[float, int]:
    if days_since_watch is None:
        return 1.0, 0
    if days_since_watch <= 180:
        return 1.0, 0
    if days_since_watch <= 365:
        return 0.9, 1
    if days_since_watch <= 730:
        return 0.75, 2
    return 0.6, 3


def _metadata_my_list_status(meta: Any) -> dict[str, Any] | None:
    if meta is None or not isinstance(getattr(meta, "raw", None), dict):
        return None
    my_list_status = meta.raw.get("my_list_status")
    return my_list_status if isinstance(my_list_status, dict) else None


def _discovery_seed_status_value(meta: Any) -> str | None:
    my_list_status = _metadata_my_list_status(meta)
    if not isinstance(my_list_status, dict):
        return None
    raw_status = my_list_status.get("status")
    if not isinstance(raw_status, str):
        return None
    status = raw_status.strip().lower()
    return status or None


def _discovery_seed_score_bonus(meta: Any) -> tuple[int, int | None]:
    my_list_status = _metadata_my_list_status(meta)
    if not isinstance(my_list_status, dict):
        return 0, None
    raw_score = my_list_status.get("score")
    if not isinstance(raw_score, int) or raw_score <= 0:
        return 0, None
    if raw_score >= 9:
        return 3, raw_score
    if raw_score >= 8:
        return 2, raw_score
    if raw_score >= 7:
        return 1, raw_score
    return 0, raw_score


def _discovery_seed_score_penalty(meta: Any) -> tuple[int, int | None]:
    my_list_status = _metadata_my_list_status(meta)
    if not isinstance(my_list_status, dict):
        return 0, None
    raw_score = my_list_status.get("score")
    if not isinstance(raw_score, int) or raw_score <= 0:
        return 0, None
    if raw_score <= 3:
        return 3, raw_score
    if raw_score == 4:
        return 2, raw_score
    if raw_score == 5:
        return 1, raw_score
    return 0, raw_score


def _discovery_seed_score_is_neutral(meta: Any) -> tuple[bool, int | None]:
    my_list_status = _metadata_my_list_status(meta)
    if not isinstance(my_list_status, dict):
        return False, None
    raw_score = my_list_status.get("score")
    if not isinstance(raw_score, int) or raw_score <= 0:
        return False, None
    return raw_score == 6, raw_score


def _discovery_seed_completion_bonus(state: ProviderSeriesState) -> int:
    if state.watchlist_status == "fully_watched":
        if state.completed_episode_count >= 24:
            return 2
        if state.completed_episode_count >= 12:
            return 1
        return 0
    if state.watchlist_status == "in_progress" and state.completed_episode_count >= 12:
        return 1
    return 0


def _normalized_title_aliases(*values: str | None) -> set[str]:
    aliases: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        normalized = normalize_title(value)
        if normalized:
            aliases.add(normalized)
    return aliases


def _metadata_title_aliases(meta: Any) -> set[str]:
    if meta is None:
        return set()
    raw = meta.raw if isinstance(getattr(meta, "raw", None), dict) else {}
    aliases = _normalized_title_aliases(
        getattr(meta, "title", None),
        getattr(meta, "title_english", None),
        getattr(meta, "title_japanese", None),
    )
    for alt in getattr(meta, "alternative_titles", []) or []:
        aliases.update(_normalized_title_aliases(alt))
    if isinstance(raw, dict):
        alternative_titles = raw.get("alternative_titles")
        if isinstance(alternative_titles, dict):
            aliases.update(_normalized_title_aliases(alternative_titles.get("en"), alternative_titles.get("ja")))
            synonyms = alternative_titles.get("synonyms")
            if isinstance(synonyms, list):
                for synonym in synonyms:
                    aliases.update(_normalized_title_aliases(synonym))
    return aliases


def _provider_catalog_title_aliases(states: list[ProviderSeriesState]) -> set[str]:
    aliases: set[str] = set()
    for state in states:
        aliases.update(_normalized_title_aliases(state.title, state.season_title))
    return aliases


_DISCOVERY_FRANCHISE_RELATION_TYPES = frozenset(
    {
        "sequel",
        "prequel",
        "parent_story",
        "side_story",
        "alternative_setting",
        "alternative_version",
        "spin_off",
        "summary",
        "full_story",
    }
)


def _build_discovery_recommendations(
    states: list[ProviderSeriesState],
    *,
    mapping_by_series: dict[tuple[str, str], int],
    metadata_by_id: dict[int, Any],
    relations_by_id: dict[int, list[Any]],
    recommendation_edges_by_id: dict[int, list[Any]],
) -> list[Recommendation]:
    seed_weights: dict[int, int] = {}
    seed_recent_activity_bonus: dict[int, int] = {}
    seed_recent_activity_days: dict[int, int] = {}
    seed_staleness_penalty: dict[int, int] = {}
    seed_scores: dict[int, int] = {}
    seed_penalty_scores: dict[int, int] = {}
    seed_quality_bonus: dict[int, int] = {}
    seed_quality_penalty: dict[int, int] = {}
    dropped_seed_ids: set[int] = set()
    disliked_seed_ids: set[int] = set()
    positive_quality_seed_ids: set[int] = set()
    neutral_seed_ids: set[int] = set()
    neutral_seed_scores: dict[int, int] = {}
    for state in states:
        mal_anime_id = mapping_by_series.get((state.provider, state.provider_series_id))
        if mal_anime_id is None:
            continue
        days_since_watch = _days_since(state.last_watched_at)
        if state.watchlist_status == "fully_watched":
            seed_weights[mal_anime_id] = max(seed_weights.get(mal_anime_id, 0), 3)
        elif state.watchlist_status == "in_progress" and (state.completed_episode_count >= 3 or days_since_watch in range(0, 91)):
            seed_weights[mal_anime_id] = max(seed_weights.get(mal_anime_id, 0), 2)
        meta = metadata_by_id.get(mal_anime_id)
        score_bonus, seed_score = _discovery_seed_score_bonus(meta)
        score_penalty, seed_penalty_score = _discovery_seed_score_penalty(meta)
        is_neutral_seed, neutral_seed_score = _discovery_seed_score_is_neutral(meta)
        completion_bonus = _discovery_seed_completion_bonus(state)
        seed_status = _discovery_seed_status_value(meta)
         
        if seed_status == "dropped":
            dropped_seed_ids.add(mal_anime_id)
        if seed_penalty_score is not None and seed_penalty_score <= 4:
            disliked_seed_ids.add(mal_anime_id)
        quality_bonus = score_bonus + completion_bonus
        if quality_bonus > seed_quality_bonus.get(mal_anime_id, 0):
            seed_quality_bonus[mal_anime_id] = quality_bonus
        if quality_bonus > 0:
            positive_quality_seed_ids.add(mal_anime_id)
        if score_penalty > seed_quality_penalty.get(mal_anime_id, 0):
            seed_quality_penalty[mal_anime_id] = score_penalty
        if is_neutral_seed:
            neutral_seed_ids.add(mal_anime_id)
            if neutral_seed_score is not None:
                neutral_seed_scores[mal_anime_id] = neutral_seed_score
        if seed_score is not None and seed_score > seed_scores.get(mal_anime_id, 0):
            seed_scores[mal_anime_id] = seed_score
        if seed_penalty_score is not None:
            current_penalty_score = seed_penalty_scores.get(mal_anime_id)
            if current_penalty_score is None or seed_penalty_score < current_penalty_score:
                seed_penalty_scores[mal_anime_id] = seed_penalty_score
        recency_bonus = _discovery_seed_recency_bonus(days_since_watch)
        if recency_bonus > seed_recent_activity_bonus.get(mal_anime_id, 0):
            seed_recent_activity_bonus[mal_anime_id] = recency_bonus
        _, staleness_penalty = _discovery_seed_staleness_profile(days_since_watch)
        if staleness_penalty > seed_staleness_penalty.get(mal_anime_id, 0):
            seed_staleness_penalty[mal_anime_id] = staleness_penalty
        if days_since_watch is not None:
            current_days = seed_recent_activity_days.get(mal_anime_id)
            if current_days is None or days_since_watch < current_days:
                seed_recent_activity_days[mal_anime_id] = days_since_watch

    seed_genre_weights: dict[str, int] = {}
    seed_studio_weights: dict[str, int] = {}
    seed_source_weights: dict[str, int] = {}
    for mal_anime_id, weight in seed_weights.items():
        meta = metadata_by_id.get(mal_anime_id)
        for genre in _metadata_genre_names(meta):
            seed_genre_weights[genre] = seed_genre_weights.get(genre, 0) + weight
        for studio in _metadata_studio_names(meta):
            seed_studio_weights[studio] = seed_studio_weights.get(studio, 0) + weight
        source = _metadata_source_value(meta)
        if source is not None:
            seed_source_weights[source] = seed_source_weights.get(source, 0) + weight

    candidate_scores: dict[int, dict[str, Any]] = {}
    watched_ids = set(seed_weights)
    mapped_ids = set(mapping_by_series.values())
    provider_catalog_aliases = _provider_catalog_title_aliases(states)
    direct_franchise_relation_targets_by_source: dict[int, set[int]] = {}
    globally_related_franchise_targets: set[int] = set()
    for source_id in seed_weights:
        direct_targets = {
            relation.related_mal_anime_id
            for relation in relations_by_id.get(source_id, [])
            if relation.relation_type in _DISCOVERY_FRANCHISE_RELATION_TYPES
        }
        direct_franchise_relation_targets_by_source[source_id] = direct_targets
        globally_related_franchise_targets.update(direct_targets)
    for source_id, weight in seed_weights.items():
        for edge in recommendation_edges_by_id.get(source_id, [])[:15]:
            target_id = edge.target_mal_anime_id
            if target_id in watched_ids or target_id in mapped_ids:
                continue
            if target_id in globally_related_franchise_targets:
                continue
            if target_id in direct_franchise_relation_targets_by_source.get(source_id, set()):
                continue
            bucket = candidate_scores.setdefault(
                target_id,
                {
                    "supporting_sources": set(),
                    "raw_score": 0.0,
                    "votes": 0,
                    "votes_by_source": {},
                    "title": edge.target_title,
                    "seed_recent_activity_bonus": 0,
                    "seed_recent_activity_days": {},
                    "seed_staleness_penalty": 0,
                    "stale_supporting_seed_ids": set(),
                    "seed_quality_bonus": 0,
                    "seed_quality_penalty": 0,
                    "supporting_seed_scores": {},
                    "penalized_seed_scores": {},
                    "dropped_supporting_seed_ids": set(),
                    "disliked_supporting_seed_ids": set(),
                    "positive_quality_supporting_seed_ids": set(),
                    "neutral_supporting_seed_ids": set(),
                    "neutral_supporting_seed_scores": {},
                },
            )
            votes = edge.num_recommendations or 0
            bucket["supporting_sources"].add(source_id)
            bucket["seed_recent_activity_bonus"] += seed_recent_activity_bonus.get(source_id, 0)
            bucket["seed_staleness_penalty"] += seed_staleness_penalty.get(source_id, 0)
            bucket["seed_quality_bonus"] += seed_quality_bonus.get(source_id, 0)
            bucket["seed_quality_penalty"] += seed_quality_penalty.get(source_id, 0)
            if source_id in seed_recent_activity_days:
                bucket["seed_recent_activity_days"][source_id] = seed_recent_activity_days[source_id]
            if source_id in seed_scores:
                bucket["supporting_seed_scores"][source_id] = seed_scores[source_id]
            if source_id in seed_penalty_scores:
                bucket["penalized_seed_scores"][source_id] = seed_penalty_scores[source_id]
            if seed_staleness_penalty.get(source_id, 0) > 0:
                bucket["stale_supporting_seed_ids"].add(source_id)
            if source_id in dropped_seed_ids:
                bucket["dropped_supporting_seed_ids"].add(source_id)
            if source_id in disliked_seed_ids:
                bucket["disliked_supporting_seed_ids"].add(source_id)
            if source_id in positive_quality_seed_ids:
                bucket["positive_quality_supporting_seed_ids"].add(source_id)
            if source_id in neutral_seed_ids:
                bucket["neutral_supporting_seed_ids"].add(source_id)
            if source_id in neutral_seed_scores:
                bucket["neutral_supporting_seed_scores"][source_id] = neutral_seed_scores[source_id]
            votes_by_source = bucket["votes_by_source"]
            votes_by_source[source_id] = votes_by_source.get(source_id, 0) + votes
            bucket["votes"] += votes
            staleness_scale, _ = _discovery_seed_staleness_profile(seed_recent_activity_days.get(source_id))
            bucket["raw_score"] += weight * min(votes, 40) * staleness_scale

    items: list[Recommendation] = []
    for target_id, bucket in candidate_scores.items():
        meta = metadata_by_id.get(target_id)
        support_count = len(bucket["supporting_sources"])
        if support_count <= 0:
            continue
        if meta is not None and meta.media_type not in (None, "tv", "movie", "ova", "ona", "special"):
            continue
        if meta is not None:
            my_list_status = meta.raw.get("my_list_status") if isinstance(meta.raw, dict) else None
            if isinstance(my_list_status, dict):
                status_value = my_list_status.get("status")
                watched_count = my_list_status.get("num_episodes_watched")
                if status_value in {"completed", "watching", "on_hold", "dropped", "plan_to_watch"}:
                    continue
                if isinstance(watched_count, int) and watched_count > 0:
                    continue
        candidate_title_aliases = _metadata_title_aliases(meta)
        candidate_title_aliases.update(_normalized_title_aliases(bucket.get("title")))
        if candidate_title_aliases & provider_catalog_aliases:
            continue
        mean = meta.mean if meta is not None else None
        popularity = meta.popularity if meta is not None else None
        votes_by_source = bucket.get("votes_by_source") or {}
        best_single_source_votes = max((int(value) for value in votes_by_source.values()), default=0)
        cross_seed_support_votes = max(bucket["votes"] - best_single_source_votes, 0)
        popularity_bonus = 0
        if popularity is not None:
            if popularity <= 100:
                popularity_bonus = 6
            elif popularity <= 500:
                popularity_bonus = 3
            elif popularity <= 2000:
                popularity_bonus = 1
        candidate_genres = _metadata_genre_names(meta)
        shared_genres = sorted(candidate_genres & set(seed_genre_weights), key=lambda genre: (-seed_genre_weights[genre], genre))
        genre_overlap_score = sum(seed_genre_weights[genre] for genre in shared_genres)
        genre_bonus = min(genre_overlap_score * 3, 18)
        candidate_studios = _metadata_studio_names(meta)
        shared_studios = sorted(candidate_studios & set(seed_studio_weights), key=lambda studio: (-seed_studio_weights[studio], studio))
        studio_overlap_score = sum(seed_studio_weights[studio] for studio in shared_studios)
        studio_bonus = min(studio_overlap_score * 2, 10)
        candidate_source = _metadata_source_value(meta)
        source_overlap_score = seed_source_weights.get(candidate_source, 0) if candidate_source is not None else 0
        source_bonus = min(source_overlap_score * 2, 6)
        metadata_affinity_bonus, metadata_match_dimensions = _discovery_metadata_affinity_bonus(
            genre_overlap_score=genre_overlap_score,
            studio_overlap_score=studio_overlap_score,
            source_overlap_score=source_overlap_score,
            popularity=popularity,
        )
        metadata_quality_bonus, metadata_mean_band, metadata_popularity_band = _discovery_metadata_quality_bonus(
            matched_dimensions=metadata_match_dimensions,
            mean=mean,
            popularity=popularity,
        )
        catalog_quality_bonus, catalog_mean_band, catalog_popularity_band = _discovery_catalog_quality_bonus(
            mean=mean,
            popularity=popularity,
        )
        start_season = _metadata_start_season(meta)
        start_season_label = _format_start_season(start_season)
        freshness_bonus, freshness_bucket, catalog_age_in_seasons, freshness_penalty = _discovery_candidate_freshness_profile(start_season)
        recent_seed_activity_bonus = min(int(bucket.get("seed_recent_activity_bonus", 0)), 6)
        seed_quality_bonus = min(int(bucket.get("seed_quality_bonus", 0)), 6)
        seed_quality_penalty = min(int(bucket.get("seed_quality_penalty", 0)), 6)
        recent_seed_activity_days = [
            int(value) for value in (bucket.get("seed_recent_activity_days") or {}).values() if isinstance(value, int)
        ]
        freshest_supporting_seed_days = min(recent_seed_activity_days) if recent_seed_activity_days else None
        supporting_seed_scores = {
            int(source_id): int(score)
            for source_id, score in (bucket.get("supporting_seed_scores") or {}).items()
            if isinstance(source_id, int) and isinstance(score, int) and score > 0
        }
        penalized_seed_scores = {
            int(source_id): int(score)
            for source_id, score in (bucket.get("penalized_seed_scores") or {}).items()
            if isinstance(source_id, int) and isinstance(score, int) and score > 0
        }
        stale_supporting_seed_ids = sorted(
            int(source_id)
            for source_id in (bucket.get("stale_supporting_seed_ids") or set())
            if isinstance(source_id, int)
        )
        dropped_supporting_seed_ids = sorted(
            int(source_id)
            for source_id in (bucket.get("dropped_supporting_seed_ids") or set())
            if isinstance(source_id, int)
        )
        disliked_supporting_seed_ids = sorted(
            int(source_id)
            for source_id in (bucket.get("disliked_supporting_seed_ids") or set())
            if isinstance(source_id, int)
        )
        positive_quality_supporting_seed_ids = sorted(
            int(source_id)
            for source_id in (bucket.get("positive_quality_supporting_seed_ids") or set())
            if isinstance(source_id, int)
        )
        neutral_supporting_seed_ids = sorted(
            int(source_id)
            for source_id in (bucket.get("neutral_supporting_seed_ids") or set())
            if isinstance(source_id, int)
        )
        neutral_supporting_seed_scores = {
            int(source_id): int(score)
            for source_id, score in (bucket.get("neutral_supporting_seed_scores") or {}).items()
            if isinstance(source_id, int) and isinstance(score, int) and score > 0
        }
        best_supporting_seed_score = max(supporting_seed_scores.values(), default=None)
        lowest_supporting_seed_score = min(penalized_seed_scores.values(), default=None)
        all_support_is_dropped = support_count > 0 and len(dropped_supporting_seed_ids) >= support_count
        all_support_is_disliked = support_count > 0 and len(disliked_supporting_seed_ids) >= support_count
        if all_support_is_dropped:
            continue
        if all_support_is_disliked and not positive_quality_supporting_seed_ids:
            continue
        stale_supporting_seed_count = len(stale_supporting_seed_ids)
        stale_support_ratio = stale_supporting_seed_count / support_count if support_count > 0 else 0.0
        stale_support_penalty = min(int(bucket.get("seed_staleness_penalty", 0)), 6)
        negative_supporting_seed_ids = sorted({*dropped_supporting_seed_ids, *disliked_supporting_seed_ids})
        negative_supporting_seed_count = len(negative_supporting_seed_ids)
        negative_support_ratio = (
            negative_supporting_seed_count / support_count if support_count > 0 else 0.0
        )
        neutral_supporting_seed_count = len(neutral_supporting_seed_ids)
        neutral_support_ratio = (
            neutral_supporting_seed_count / support_count if support_count > 0 else 0.0
        )
        mixed_signal_penalty = 0
        if 0 < negative_supporting_seed_count < support_count:
            if negative_support_ratio >= (2 / 3):
                mixed_signal_penalty = 5
            elif negative_support_ratio >= 0.5:
                mixed_signal_penalty = 3
            elif negative_support_ratio >= (1 / 3):
                mixed_signal_penalty = 1
        neutral_support_penalty = 0
        if neutral_supporting_seed_count > 0 and negative_supporting_seed_count < support_count:
            if neutral_support_ratio >= (2 / 3):
                neutral_support_penalty = 4
            elif neutral_support_ratio >= 0.5:
                neutral_support_penalty = 2
            elif neutral_support_ratio > 0:
                neutral_support_penalty = 1
        base_effective_supporting_seed_count = max(
            support_count - negative_supporting_seed_count - neutral_supporting_seed_count,
            1,
        )
        stale_consensus_discount = 0
        if base_effective_supporting_seed_count > 1 and stale_supporting_seed_count > 0:
            if stale_support_ratio >= 1.0:
                stale_consensus_discount = 2
            elif stale_support_ratio >= (2 / 3):
                stale_consensus_discount = 1
        stale_consensus_discount = min(stale_consensus_discount, base_effective_supporting_seed_count - 1)
        effective_supporting_seed_count = max(base_effective_supporting_seed_count - stale_consensus_discount, 1)
        effective_votes_by_source: dict[int, int] = {}
        for source_id, value in votes_by_source.items():
            if not isinstance(source_id, int):
                continue
            votes = int(value)
            if source_id in negative_supporting_seed_ids or source_id in neutral_supporting_seed_ids:
                continue
            staleness_scale, _ = _discovery_seed_staleness_profile(
                (bucket.get("seed_recent_activity_days") or {}).get(source_id)
            )
            effective_votes_by_source[source_id] = max(int(round(votes * staleness_scale)), 0)
        effective_total_support_votes = sum(effective_votes_by_source.values())
        effective_best_single_source_votes = max(effective_votes_by_source.values(), default=0)
        effective_cross_seed_support_votes = max(
            effective_total_support_votes - effective_best_single_source_votes,
            0,
        )
        support_balance_bonus = min(effective_cross_seed_support_votes // 5, 8)
        priority = int(min(bucket["raw_score"] / 8.0, 60)) + effective_supporting_seed_count * 12 + int(mean or 0)
        priority += popularity_bonus + genre_bonus + studio_bonus + source_bonus + metadata_affinity_bonus + metadata_quality_bonus + catalog_quality_bonus + support_balance_bonus + freshness_bonus + recent_seed_activity_bonus + seed_quality_bonus
        priority -= freshness_penalty
        priority -= stale_support_penalty
        priority -= seed_quality_penalty
        priority -= mixed_signal_penalty
        priority -= neutral_support_penalty
        reasons = [
            f"recommended by {support_count} watched/mapped seed title(s)",
        ]
        if bucket["votes"]:
            reasons.append(f"aggregated MAL recommendation votes: {bucket['votes']}")
        if cross_seed_support_votes > 0:
            reasons.append(f"cross-seed consensus beyond the strongest seed: {cross_seed_support_votes} vote(s)")
        if cross_seed_support_votes > effective_cross_seed_support_votes:
            reasons.append(
                f"support spread counted conservatively after neutral/stale seed weighting ({effective_cross_seed_support_votes}/{cross_seed_support_votes} effective cross-seed vote(s))"
            )
        if shared_genres:
            reasons.append("shared seed genres: " + ", ".join(shared_genres[:3]))
        if shared_studios:
            reasons.append("shared seed studios: " + ", ".join(shared_studios[:2]))
        if source_overlap_score > 0 and candidate_source is not None:
            reasons.append(f"shared seed source material: {candidate_source}")
        if metadata_affinity_bonus > 0:
            reasons.append(
                f"metadata-rich seed alignment across {metadata_match_dimensions} dimensions counted as an extra tie-break"
            )
        if metadata_quality_bonus > 0:
            quality_fragments: list[str] = []
            if metadata_mean_band is not None:
                quality_fragments.append(f"{metadata_mean_band} MAL mean")
            if metadata_popularity_band is not None:
                quality_fragments.append(f"{metadata_popularity_band} catalog adoption")
            if quality_fragments:
                reasons.append(
                    "metadata-rich tie-break also favored stronger MAL quality/adoption signals ("
                    + ", ".join(quality_fragments)
                    + ")"
                )
        if catalog_quality_bonus > 0:
            catalog_quality_fragments: list[str] = []
            if catalog_mean_band is not None:
                catalog_quality_fragments.append(f"{catalog_mean_band} MAL mean")
            if catalog_popularity_band is not None:
                catalog_quality_fragments.append(
                    "very broad catalog adoption" if catalog_popularity_band == "very_broad" else "broad catalog adoption"
                )
            if catalog_quality_fragments:
                reasons.append(
                    "global catalog quality/adoption calibration slightly favored this candidate ("
                    + ", ".join(catalog_quality_fragments)
                    + ")"
                )
        if freshness_bonus > 0 and start_season_label is not None:
            reasons.append(f"recent MAL start season: {start_season_label}")
        if recent_seed_activity_bonus > 0 and freshest_supporting_seed_days is not None:
            reasons.append(f"supported by recently active seed watch history ({freshest_supporting_seed_days} day(s) since last watch)")
        if freshness_penalty > 0 and start_season_label is not None:
            reasons.append(f"older MAL catalog title received modest age decay ({start_season_label})")
        if stale_support_penalty > 0:
            reasons.append(
                f"older supporting seed activity counted conservatively ({stale_supporting_seed_count}/{support_count} supporting seed title(s) were stale)"
            )
        if stale_consensus_discount > 0:
            reasons.append(
                f"stale-heavy multi-seed consensus counted less strongly ({stale_supporting_seed_count}/{support_count} supporting seed title(s) were stale)"
            )
        if seed_quality_bonus > 0:
            if best_supporting_seed_score is not None:
                reasons.append(f"backed by higher-confidence seed taste signals (best supporting seed MAL score: {best_supporting_seed_score})")
            else:
                reasons.append("backed by stronger seed engagement signals (completion depth across supporting seeds)")
        if seed_quality_penalty > 0 and lowest_supporting_seed_score is not None:
            reasons.append(
                f"tempered by low-confidence/disliked seed support (lowest supporting seed MAL score: {lowest_supporting_seed_score})"
            )
        if mixed_signal_penalty > 0:
            reasons.append(
                f"mixed-signal support decay applied ({negative_supporting_seed_count}/{support_count} supporting seed title(s) were dropped/disliked)"
            )
        if neutral_support_penalty > 0:
            reasons.append(
                f"explicit neutral seed support counted conservatively ({neutral_supporting_seed_count}/{support_count} supporting seed title(s) carried a neutral MAL score)"
            )
        if mean is not None:
            reasons.append(f"MAL mean score: {mean}")
        if meta is None:
            reasons.append("full MAL metadata for this discovery candidate is not cached yet")
        title = meta.title if meta is not None else (bucket.get("title") or f"MAL anime {target_id}")
        items.append(
            Recommendation(
                kind="discovery_candidate",
                priority=priority,
                provider="mal",
                provider_series_id=f"mal:{target_id}",
                title=title,
                season_title=None,
                reasons=reasons,
                context={
                    "mal_anime_id": target_id,
                    "supporting_source_count": support_count,
                    "base_effective_supporting_seed_count": base_effective_supporting_seed_count,
                    "stale_consensus_discount": stale_consensus_discount,
                    "effective_supporting_seed_count": effective_supporting_seed_count,
                    "supporting_mal_anime_ids": sorted(bucket["supporting_sources"]),
                    "aggregated_recommendation_votes": bucket["votes"],
                    "best_single_source_votes": best_single_source_votes,
                    "cross_seed_support_votes": cross_seed_support_votes,
                    "effective_best_single_source_votes": effective_best_single_source_votes,
                    "effective_cross_seed_support_votes": effective_cross_seed_support_votes,
                    "support_balance_bonus": support_balance_bonus,
                    "shared_genres": shared_genres,
                    "genre_overlap_score": genre_overlap_score,
                    "shared_studios": shared_studios,
                    "studio_overlap_score": studio_overlap_score,
                    "source": candidate_source,
                    "source_overlap_score": source_overlap_score,
                    "metadata_match_dimensions": metadata_match_dimensions,
                    "metadata_affinity_bonus": metadata_affinity_bonus,
                    "metadata_quality_bonus": metadata_quality_bonus,
                    "metadata_mean_band": metadata_mean_band,
                    "metadata_popularity_band": metadata_popularity_band,
                    "catalog_quality_bonus": catalog_quality_bonus,
                    "catalog_mean_band": catalog_mean_band,
                    "catalog_popularity_band": catalog_popularity_band,
                    "start_season": start_season,
                    "start_season_label": start_season_label,
                    "freshness_bucket": freshness_bucket,
                    "freshness_bonus": freshness_bonus,
                    "freshness_penalty": freshness_penalty,
                    "catalog_age_in_seasons": catalog_age_in_seasons,
                    "recent_seed_activity_bonus": recent_seed_activity_bonus,
                    "seed_quality_bonus": seed_quality_bonus,
                    "seed_quality_penalty": seed_quality_penalty,
                    "supporting_seed_scores": supporting_seed_scores,
                    "penalized_seed_scores": penalized_seed_scores,
                    "best_supporting_seed_score": best_supporting_seed_score,
                    "lowest_supporting_seed_score": lowest_supporting_seed_score,
                    "stale_supporting_seed_ids": stale_supporting_seed_ids,
                    "stale_supporting_seed_count": stale_supporting_seed_count,
                    "stale_support_ratio": stale_support_ratio,
                    "stale_support_penalty": stale_support_penalty,
                    "dropped_supporting_seed_ids": dropped_supporting_seed_ids,
                    "dropped_supporting_seed_count": len(dropped_supporting_seed_ids),
                    "disliked_supporting_seed_ids": disliked_supporting_seed_ids,
                    "disliked_supporting_seed_count": len(disliked_supporting_seed_ids),
                    "negative_supporting_seed_ids": negative_supporting_seed_ids,
                    "negative_supporting_seed_count": negative_supporting_seed_count,
                    "negative_support_ratio": negative_support_ratio,
                    "mixed_signal_penalty": mixed_signal_penalty,
                    "positive_quality_supporting_seed_ids": positive_quality_supporting_seed_ids,
                    "positive_quality_supporting_seed_count": len(positive_quality_supporting_seed_ids),
                    "neutral_supporting_seed_ids": neutral_supporting_seed_ids,
                    "neutral_supporting_seed_count": neutral_supporting_seed_count,
                    "neutral_supporting_seed_scores": neutral_supporting_seed_scores,
                    "neutral_support_ratio": neutral_support_ratio,
                    "neutral_support_penalty": neutral_support_penalty,
                    "freshest_supporting_seed_days": freshest_supporting_seed_days,
                    "mean": mean,
                    "popularity": popularity,
                    "media_type": meta.media_type if meta is not None else None,
                    "num_episodes": meta.num_episodes if meta is not None else None,
                    "metadata_cached": meta is not None,
                },
            )
        )
    return items


def _build_relation_backed_new_season_recommendations(
    states: list[ProviderSeriesState],
    *,
    state_by_id: dict[tuple[str, str], ProviderSeriesState],
    mapping_by_series: dict[tuple[str, str], int],
    metadata_by_id: dict[int, Any],
    relations_by_id: dict[int, list[Any]],
) -> list[Recommendation]:
    items: list[Recommendation] = []
    series_by_anime_id: dict[int, list[ProviderSeriesState]] = defaultdict(list)
    for series_key, anime_id in mapping_by_series.items():
        state = state_by_id.get(series_key)
        if state is not None:
            series_by_anime_id.setdefault(anime_id, []).append(state)
    sequel_relation_types = {"sequel"}
    predecessor_relation_types = {"prequel", "parent_story"}
    for state in states:
        current_anime_id = mapping_by_series.get((state.provider, state.provider_series_id))
        if current_anime_id is None:
            continue
        if not _is_english_dub_series(state):
            continue
        relations = relations_by_id.get(current_anime_id, [])
        best_predecessor = None
        for relation in relations:
            if relation.relation_type not in predecessor_relation_types:
                continue
            predecessor_candidates = series_by_anime_id.get(relation.related_mal_anime_id, [])
            predecessor_state = next((item for item in predecessor_candidates if _series_counts_as_completed(item)), None)
            if predecessor_state is None:
                continue
            best_predecessor = predecessor_state
            break
        if best_predecessor is not None and state.completed_episode_count <= 0:
            title_hint = metadata_by_id.get(current_anime_id).title if current_anime_id in metadata_by_id else state.title
            items.append(
                Recommendation(
                    kind="new_season",
                    priority=110,
                    provider=state.provider,
                    provider_series_id=state.provider_series_id,
                    title=state.title,
                    season_title=state.season_title,
                    reasons=[
                        "English dub is available",
                        f"MAL relation metadata links this title as a continuation after {best_predecessor.season_title or best_predecessor.title}",
                    ],
                    context={
                        "provider": state.provider,
                        "relation_backed": True,
                        "mal_anime_id": current_anime_id,
                        "metadata_title": title_hint,
                        "predecessor_provider_series_id": best_predecessor.provider_series_id,
                        "predecessor_title": best_predecessor.season_title or best_predecessor.title,
                    },
                )
            )
            continue

        if state.completed_episode_count > 0:
            for relation in relations:
                if relation.relation_type not in sequel_relation_types:
                    continue
                sequel_candidates = series_by_anime_id.get(relation.related_mal_anime_id, [])
                for sequel_state in sequel_candidates:
                    if not _is_english_dub_series(sequel_state):
                        continue
                    if sequel_state.completed_episode_count > 0:
                        continue
                    title_hint = metadata_by_id.get(relation.related_mal_anime_id).title if relation.related_mal_anime_id in metadata_by_id else sequel_state.title
                    items.append(
                        Recommendation(
                            kind="new_season",
                            priority=112,
                            provider=sequel_state.provider,
                            provider_series_id=sequel_state.provider_series_id,
                            title=sequel_state.title,
                            season_title=sequel_state.season_title,
                            reasons=[
                                "English dub is available",
                                f"MAL relation metadata links this as a sequel to {state.season_title or state.title}",
                            ],
                            context={
                                "provider": sequel_state.provider,
                                "relation_backed": True,
                                "mal_anime_id": relation.related_mal_anime_id,
                                "metadata_title": title_hint,
                                "predecessor_provider_series_id": state.provider_series_id,
                                "predecessor_title": state.season_title or state.title,
                                "predecessor_provider": state.provider,
                            },
                        )
                    )
    return items


def _build_new_season_recommendations(states: list[ProviderSeriesState]) -> list[Recommendation]:
    items: list[Recommendation] = []
    by_franchise: dict[str, list[tuple[int, ProviderSeriesState]]] = {}
    for state in states:
        if not _is_english_dub_series(state):
            continue
        installment = _series_installment_index(state)
        if installment is None:
            continue
        key = normalize_title(state.title)
        if not key:
            continue
        by_franchise.setdefault(key, []).append((installment, state))

    for entries in by_franchise.values():
        entries.sort(key=lambda item: (item[0], item[1].title.lower(), item[1].provider_series_id))
        for installment, state in entries:
            if installment <= 1:
                continue
            predecessor = _find_best_completed_predecessor(entries, installment)
            if predecessor is None:
                continue
            if predecessor.provider_series_id == state.provider_series_id:
                continue
            reasons = [
                "English dub is available",
                f"a later season appears available after completing {predecessor.season_title or predecessor.title}",
            ]
            if state.watchlist_status:
                reasons.append(f"{state.provider.title()} watchlist status: {state.watchlist_status}")
            priority = 100 - min(max(installment - 1, 0), 10)
            if state.completed_episode_count <= 0:
                priority += 5
            items.append(
                Recommendation(
                    kind="new_season",
                    priority=priority,
                    provider=state.provider,
                    provider_series_id=state.provider_series_id,
                    title=state.title,
                    season_title=state.season_title,
                    reasons=reasons,
                    context={
                        "provider": state.provider,
                        "installment_index": installment,
                        "predecessor_provider_series_id": predecessor.provider_series_id,
                        "predecessor_title": predecessor.season_title or predecessor.title,
                        "predecessor_completed_episode_count": predecessor.completed_episode_count,
                        "predecessor_max_episode_number": predecessor.max_episode_number,
                        "watchlist_status": state.watchlist_status,
                    },
                )
            )
    return items


def _find_best_completed_predecessor(
    entries: list[tuple[int, ProviderSeriesState]], current_installment: int
) -> ProviderSeriesState | None:
    best: tuple[int, ProviderSeriesState] | None = None
    for installment, state in entries:
        if installment >= current_installment:
            continue
        if not _series_counts_as_completed(state):
            continue
        if best is None or installment > best[0]:
            best = (installment, state)
    return None if best is None else best[1]


def _series_counts_as_completed(state: ProviderSeriesState) -> bool:
    if state.watchlist_status == "fully_watched":
        return True
    if state.max_episode_number is None or state.max_episode_number <= 0:
        return False
    return state.completed_episode_count >= state.max_episode_number


def _is_english_dub_series(state: ProviderSeriesState) -> bool:
    haystacks = [value for value in (state.season_title, state.title) if value]
    if not haystacks:
        return False
    joined = " ".join(haystacks)
    if _FOREIGN_DUB_RE.search(joined):
        return False
    return bool(_ENGLISH_DUB_RE.search(joined))


def _has_explicit_season_style_evidence(text: str) -> bool:
    return bool(
        _SEASON_NUMBER_RE.search(text) or _ORDINAL_SEASON_RE.search(text) or _FINAL_SEASON_RE.search(text)
    )


def _series_installment_index(state: ProviderSeriesState) -> int | None:
    candidates: list[int] = []
    if state.season_number is not None and state.season_number > 0:
        candidates.append(int(state.season_number))
    for text in (state.season_title, state.title):
        if not text:
            continue
        season_match = _SEASON_NUMBER_RE.search(text)
        if season_match:
            candidates.append(int(season_match.group(1)))
        ordinal_match = _ORDINAL_SEASON_RE.search(text)
        if ordinal_match:
            raw = ordinal_match.group(1).lower()
            if raw in _ORDINALS:
                candidates.append(_ORDINALS[raw])
            else:
                digits = re.sub(r"\D+", "", raw)
                if digits:
                    candidates.append(int(digits))
        part_match = _PART_RE.search(text)
        if part_match and _has_explicit_season_style_evidence(text):
            candidates.append(int(part_match.group(1)))
        roman_match = _ROMAN_END_RE.search(text)
        if roman_match:
            roman_value = _ROMAN.get(roman_match.group(1).lower())
            if roman_value:
                candidates.append(roman_value)
    positive = [value for value in candidates if value > 0]
    if positive:
        return max(positive)
    return 1 if (state.title or state.season_title) else None
