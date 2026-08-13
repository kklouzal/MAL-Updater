from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any, Callable, Protocol

from .config import AppConfig, DEFAULT_SERVICE_TASK_EXECUTE_LIMITS
from .db import (
    connect,
    delete_recommendation_provider_eligibility_evidence,
    get_mal_anime_metadata_map,
    get_provider_title_search_cache,
    get_provider_enriched_detail_cache,
    get_recommendation_provider_enrichment_cursor,
    record_provider_enriched_detail_failure,
    record_recommendation_provider_enrichment_attempt,
    get_recommendation_provider_eligibility_evidence,
    list_recommendation_provider_enrichment_attempts,
    list_series_mappings,
    list_recommendation_provider_eligibility_evidence_for_mal_ids,
    get_recommendation_provider_eligibility_lifecycle_counts,
    record_recommendation_provider_eligibility_negative_scope,
    replace_review_queue_entries,
    upsert_provider_title_search_cache,
    upsert_provider_enriched_detail_cache,
    upsert_recommendation_provider_eligibility_evidence,
    update_recommendation_provider_enrichment_attempt_outcome,
)
from .mapping import extract_provider_mapping_evidence, merge_provider_mapping_evidence, normalize_title
from .recommendation_actionability import (
    PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND,
    PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND,
    STRICT_PROVIDER_ELIGIBILITY_PROVIDERS,
    is_strict_provider_eligibility_actionable,
    provider_audio_locales_have_english,
    strict_provider_last_verified_at_for_persistence,
)
from .recommendations import Recommendation, build_recommendations
from .periodic_evidence_lifecycle import periodic_evidence_is_due
from .provider_eligibility_lifecycle import (
    PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
    ProviderEligibilityProcessLease,
    provider_eligibility_refresh_due_at,
    provider_eligibility_refresh_schedule_key,
)

PROVIDER_SEARCH_CACHE_TTL_DAYS = 365
PROVIDER_SEARCH_CACHE_LOGIC_VERSION = "provider-title-v2"
PROVIDER_DETAIL_CACHE_LOGIC_VERSION = "crunchyroll-detail-v1"
PROVIDER_ELIGIBILITY_LOGIC_VERSION = "provider-eligibility-v1"
PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS = 7
PROVIDER_ENRICHMENT_TRAVERSAL_SAFETY_FACTOR = 1.25
PROVIDER_NO_MATCH_SERIES_ID = "__provider_search_no_match__"
DISCOVERY_PROVIDER_SEARCH_REVIEW_ISSUE = "discovery_provider_search_match_review"
DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS = STRICT_PROVIDER_ELIGIBILITY_PROVIDERS
VERIFIED_PROVIDER_SEARCH_IDENTITY_KINDS = frozenset({
    PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND,
    PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND,
})
PROVIDER_SEARCH_IDENTITY_CONFIDENCE = {
    PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND: 0.9,
    PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND: 0.88,
}
_EXACT_APPROVED_MAPPING_SOURCES = frozenset({"auto_exact", "user_exact"})
_LEGACY_PROVIDER_ELIGIBILITY_LOGIC_VERSIONS = frozenset({"legacy-v1"})
_PENDING_PROVIDER_ELIGIBILITY_REVIEW_STATUSES = frozenset({"unknown", "review-needed"})


class ProviderTitleSearchClient(Protocol):
    slug: str

    def search_title(self, config: AppConfig, query: str, *, limit: int = 10) -> list[Any]:
        ...


@dataclass(slots=True)
class EnrichmentSummary:
    candidates_considered: int = 0
    queries_selected: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    provider_searches: int = 0
    strong_matches: int = 0
    ambiguous_matches: int = 0
    providers_skipped: list[str] = field(default_factory=list)
    provider_search_failures: int = 0
    provider_detail_probes: int = 0
    provider_detail_failures: int = 0
    eligibility_fresh_skips: int = 0
    eligibility_expired_retries: int = 0
    eligibility_retry_backoff_skips: int = 0
    legacy_eligibility_rows_reconciled: int = 0
    legacy_eligibility_rows_deleted: int = 0
    failure_details: list[dict[str, str]] = field(default_factory=list)
    eligibility_evidence_upserted: int = 0
    verified_eligibility_evidence_upserted: int = 0
    exact_verified_identities_no_review: int = 0
    aggregate_shells_verified_no_review: int = 0
    franchise_shell_verified_matches: int = 0
    franchise_shell_verified_identities_no_review: int = 0
    review_entries_written: int = 0
    review_entries_resolved: int = 0
    dry_run_review_entries: int = 0
    selected_candidates: list[dict[str, Any]] = field(default_factory=list)
    provider_cursor_states: dict[str, dict[str, Any]] = field(default_factory=dict)
    selection_class_counts: dict[str, int] = field(default_factory=dict)
    selection_skip_counts: dict[str, int] = field(default_factory=dict)
    eligibility_due: int = 0
    eligibility_overdue: int = 0
    eligibility_failed: int = 0
    eligibility_backoff: int = 0
    eligibility_preserved_positive: int = 0
    eligibility_contradicted: int = 0
    eligibility_invalidated: int = 0
    lease_busy: int = 0
    refresh_policy: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidates_considered": self.candidates_considered,
            "queries_selected": self.queries_selected,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "provider_searches": self.provider_searches,
            "strong_matches": self.strong_matches,
            "ambiguous_matches": self.ambiguous_matches,
            "providers_skipped": sorted(set(self.providers_skipped)),
            "provider_search_failures": self.provider_search_failures,
            "provider_detail_probes": self.provider_detail_probes,
            "provider_detail_failures": self.provider_detail_failures,
            "eligibility_fresh_skips": self.eligibility_fresh_skips,
            "eligibility_expired_retries": self.eligibility_expired_retries,
            "eligibility_retry_backoff_skips": self.eligibility_retry_backoff_skips,
            "legacy_eligibility_rows_reconciled": self.legacy_eligibility_rows_reconciled,
            "legacy_eligibility_rows_deleted": self.legacy_eligibility_rows_deleted,
            "failure_details": self.failure_details,
            "eligibility_evidence_upserted": self.eligibility_evidence_upserted,
            "verified_eligibility_evidence_upserted": self.verified_eligibility_evidence_upserted,
            "exact_verified_identities_no_review": self.exact_verified_identities_no_review,
            "aggregate_shells_verified_no_review": self.aggregate_shells_verified_no_review,
            "franchise_shell_verified_matches": self.franchise_shell_verified_matches,
            "franchise_shell_verified_identities_no_review": self.franchise_shell_verified_identities_no_review,
            "review_entries_written": self.review_entries_written,
            "review_entries_resolved": self.review_entries_resolved,
            "dry_run_review_entries": self.dry_run_review_entries,
            "selected_candidates": self.selected_candidates,
            "provider_cursor_states": self.provider_cursor_states,
            "selection_class_counts": dict(sorted(self.selection_class_counts.items())),
            "selection_skip_counts": dict(sorted(self.selection_skip_counts.items())),
            "eligibility_due": self.eligibility_due,
            "eligibility_overdue": self.eligibility_overdue,
            "eligibility_failed": self.eligibility_failed,
            "eligibility_backoff": self.eligibility_backoff,
            "eligibility_preserved_positive": self.eligibility_preserved_positive,
            "eligibility_contradicted": self.eligibility_contradicted,
            "eligibility_invalidated": self.eligibility_invalidated,
            "lease_busy": self.lease_busy,
            "refresh_policy": dict(self.refresh_policy),
            "cache_ttl_days": PROVIDER_SEARCH_CACHE_TTL_DAYS,
            "eligibility_evidence_ttl_days": PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS,
        }


@dataclass(slots=True)
class TargetTitleAlias:
    text: str
    normalized: str
    source: str
    substantive: bool


@dataclass(slots=True)
class ProviderSearchCandidateDecision:
    kind: str
    selected: list[dict[str, Any]]
    reasons: tuple[str, ...] = ()
    suppress_reason: str | None = None

    def __iter__(self):
        # Preserve the old direct-call unpacking contract:
        # kind, selected = classify_provider_matches(query, matches)
        yield self.kind
        yield self.selected


@dataclass(slots=True)
class AggregateShellVerification:
    match: dict[str, Any]
    identity_match_kind: str
    child: dict[str, Any]
    child_title: str
    child_identity: dict[str, Any]
    child_titles: tuple[dict[str, Any], ...]
    child_episode_count: int | None
    target_episode_count: int | None
    parent_episode_count: int | None
    parent_season_count: int | None
    parent_launch_year: int | None
    target_start_year: int | None
    identity_match_reasons: tuple[str, ...]
    verification_reasons: tuple[str, ...]


def select_english_provider_search_queries(meta: Any) -> list[str]:
    """Return conservative provider search queries, preferring English aliases.

    MAL's main title is often romaji while Crunchyroll/HIDIVE search indexes the
    localized/provider title.  Prefer explicit English fields and English-like
    synonyms first; only fall back to the MAL main title when no better alias is
    available.
    """
    queries: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        if not isinstance(value, str):
            return
        value = value.strip()
        normalized = normalize_title(value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        queries.append(value)

    raw = meta.raw if isinstance(getattr(meta, "raw", None), dict) else {}
    add(getattr(meta, "title_english", None))
    alt = raw.get("alternative_titles") if isinstance(raw, dict) else None
    if isinstance(alt, dict):
        add(alt.get("en"))
        synonyms = alt.get("synonyms")
        if isinstance(synonyms, list):
            for synonym in synonyms:
                # Skip obvious Japanese-script aliases.  Latin provider aliases
                # such as "Blade Dance of the Elementalers" are precisely the
                # recall gap this path is intended to close.
                if isinstance(synonym, str) and synonym.isascii():
                    add(synonym)
    if not queries:
        add(getattr(meta, "title", None))
    return queries


_PRIMARY_TITLE_ALIAS_SOURCES = frozenset({"title", "title_english", "title_japanese", "raw_alternative_titles.en", "raw_alternative_titles.ja"})
_WEAK_LEXICAL_STOPWORDS = frozenset({
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "does",
    "for",
    "from",
    "in",
    "is",
    "ni",
    "no",
    "not",
    "of",
    "on",
    "the",
    "to",
    "wa",
    "with",
    "wo",
})
_FRANCHISE_SHELL_TOKENS = frozenset({"series", "collection", "franchise", "bundle"})
_PROVIDER_CHILD_CONTAINER_KEYS = ("children", "child_titles", "child_seasons", "seasons", "season_details", "installments")
_PROVIDER_CHILD_TITLE_FIELDS = ("title", "season_title", "name", "display_title", "promo_title")
_PROVIDER_CHILD_EPISODE_COUNT_FIELDS = ("episode_count", "number_of_episodes", "num_episodes", "episodes_count", "total_episodes")


def _looks_acronym_like(value: str, normalized: str) -> bool:
    letters = [ch for ch in value if ch.isalpha()]
    if not letters:
        return False
    uppercase_letters = sum(1 for ch in letters if ch.isupper())
    normalized_letters = "".join(ch for ch in normalized if ch.isalpha())
    if len(normalized_letters) <= 4 and not any(ch in "aeiou" for ch in normalized_letters):
        return True
    return len(normalized_letters) <= 5 and uppercase_letters >= max(2, len(letters) - 1)


def _is_substantive_target_alias(value: str, normalized: str, source: str) -> bool:
    if not normalized:
        return False
    if source in _PRIMARY_TITLE_ALIAS_SOURCES:
        return True
    if _looks_acronym_like(value, normalized):
        return False
    tokens = normalized.split()
    compact = "".join(tokens)
    if len(tokens) >= 3:
        return True
    if len(tokens) == 2:
        return len(compact) >= 12
    return len(compact) >= 10


def build_target_title_family(meta: Any) -> list[TargetTitleAlias]:
    aliases: list[TargetTitleAlias] = []
    by_normalized: dict[str, int] = {}

    def add(value: Any, source: str) -> None:
        if not isinstance(value, str):
            return
        text = value.strip()
        normalized = normalize_title(text)
        if not normalized:
            return
        alias = TargetTitleAlias(
            text=text,
            normalized=normalized,
            source=source,
            substantive=_is_substantive_target_alias(text, normalized, source),
        )
        existing_index = by_normalized.get(normalized)
        if existing_index is None:
            by_normalized[normalized] = len(aliases)
            aliases.append(alias)
        elif alias.substantive and not aliases[existing_index].substantive:
            aliases[existing_index] = alias

    add(getattr(meta, "title", None), "title")
    add(getattr(meta, "title_english", None), "title_english")
    add(getattr(meta, "title_japanese", None), "title_japanese")

    stored_alternatives = getattr(meta, "alternative_titles", None)
    if isinstance(stored_alternatives, list):
        for alternative in stored_alternatives:
            add(alternative, "stored_alternative_titles")

    raw = meta.raw if isinstance(getattr(meta, "raw", None), dict) else {}
    raw_alternatives = raw.get("alternative_titles") if isinstance(raw, dict) else None
    if isinstance(raw_alternatives, dict):
        add(raw_alternatives.get("en"), "raw_alternative_titles.en")
        add(raw_alternatives.get("ja"), "raw_alternative_titles.ja")
        synonyms = raw_alternatives.get("synonyms")
        if isinstance(synonyms, list):
            for synonym in synonyms:
                add(synonym, "raw_alternative_titles.synonyms")
    return aliases


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _match_to_dict(match: Any) -> dict[str, Any]:
    if isinstance(match, dict):
        raw = dict(match)
    else:
        raw = {
            name: getattr(match, name)
            for name in (
                "provider_series_id",
                "id",
                "title",
                "season_title",
                "url",
                "audio_locales",
                "catalog_status",
                "detail_evidence_source",
                *_PROVIDER_CHILD_CONTAINER_KEYS,
                "raw",
            )
            if hasattr(match, name)
        }
    provider_raw = raw.get("raw") if isinstance(raw.get("raw"), dict) else raw
    provider_series_id = raw.get("provider_series_id") or raw.get("id")
    title = raw.get("title") or raw.get("name")
    season_title = raw.get("season_title")
    catalog_status = raw.get("catalog_status") or provider_raw.get("catalog_status")
    return {
        "provider_series_id": str(provider_series_id) if provider_series_id is not None else None,
        "title": str(title) if title is not None else None,
        "season_title": str(season_title) if season_title is not None else None,
        "url": raw.get("url"),
        "audio_locales": raw.get("audio_locales") if isinstance(raw.get("audio_locales"), list) else [],
        "catalog_status": catalog_status if catalog_status in {"present", "absent", "unknown"} else None,
        "detail_evidence_source": raw.get("detail_evidence_source") or provider_raw.get("catalog_evidence_source"),
        **{
            key: raw[key]
            for key in _PROVIDER_CHILD_CONTAINER_KEYS
            if isinstance(raw.get(key), list)
        },
        "raw": provider_raw,
    }


def _unwrap_raw_layers(value: Any) -> list[dict[str, Any]]:
    """Return nested provider raw dictionaries, outermost first.

    Cached search rows have existed in more than one normalized shape.  In
    particular, earlier runs stored the normalized match itself under ``raw``
    with the actual Crunchyroll payload one layer deeper.  Shell verification
    needs to inspect both without depending on one historical cache shape.
    """
    layers: list[dict[str, Any]] = []
    current = value
    seen_ids: set[int] = set()
    while isinstance(current, dict) and id(current) not in seen_ids:
        seen_ids.add(id(current))
        layers.append(current)
        current = current.get("raw")
    return layers


def _raw_lookup(match: dict[str, Any], key: str) -> Any:
    if key in match:
        return match.get(key)
    for layer in _unwrap_raw_layers(match.get("raw")):
        if key in layer:
            return layer.get(key)
        metadata = layer.get("series_metadata")
        if isinstance(metadata, dict) and key in metadata:
            return metadata.get(key)
    return None


def _int_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _provider_episode_count(match: dict[str, Any]) -> int | None:
    return _int_value(_raw_lookup(match, "episode_count"))


def _provider_season_count(match: dict[str, Any]) -> int | None:
    return _int_value(_raw_lookup(match, "season_count"))


def _provider_launch_year(match: dict[str, Any]) -> int | None:
    return _int_value(_raw_lookup(match, "series_launch_year"))


def _target_episode_count(meta: Any) -> int | None:
    value = _int_value(getattr(meta, "num_episodes", None))
    if value is not None:
        return value
    raw = meta.raw if isinstance(getattr(meta, "raw", None), dict) else {}
    return _int_value(raw.get("num_episodes"))


def _target_start_year(meta: Any) -> int | None:
    start_season = getattr(meta, "start_season", None)
    if not isinstance(start_season, dict):
        raw = meta.raw if isinstance(getattr(meta, "raw", None), dict) else {}
        start_season = raw.get("start_season") if isinstance(raw.get("start_season"), dict) else None
    if not isinstance(start_season, dict):
        return None
    return _int_value(start_season.get("year"))


def _coerce_provider_child_items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("data", "items", *_PROVIDER_CHILD_CONTAINER_KEYS):
            items = value.get(key)
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
    return []


def _provider_child_items_from_match(match: dict[str, Any]) -> list[dict[str, Any]]:
    children: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for layer in [match, *_unwrap_raw_layers(match.get("raw"))]:
        for key in _PROVIDER_CHILD_CONTAINER_KEYS:
            for child in _coerce_provider_child_items(layer.get(key)):
                child_id = str(child.get("id") or child.get("season_id") or child.get("provider_child_id") or "")
                child_title = str(child.get("title") or child.get("season_title") or child.get("name") or "")
                dedupe_key = (child_id, normalize_title(child_title))
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                children.append(child)
    return children


def _child_nested_dicts(child: dict[str, Any]) -> list[dict[str, Any]]:
    nested = [child]
    for key in ("season_metadata", "series_metadata", "metadata", "raw"):
        value = child.get(key)
        if isinstance(value, dict):
            nested.append(value)
    return nested


def _provider_child_title_norms(child: dict[str, Any]) -> list[tuple[str, str, str]]:
    norms: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for layer in _child_nested_dicts(child):
        for field_name in _PROVIDER_CHILD_TITLE_FIELDS:
            value = layer.get(field_name)
            normalized = normalize_title(value) if isinstance(value, str) else ""
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            norms.append((field_name, str(value), normalized))
    return norms


def _provider_child_id(child: dict[str, Any]) -> str | None:
    for layer in _child_nested_dicts(child):
        value = layer.get("id") or layer.get("season_id") or layer.get("provider_child_id")
        if value:
            return str(value)
    return None


def _provider_child_series_id(child: dict[str, Any]) -> str | None:
    for layer in _child_nested_dicts(child):
        value = layer.get("series_id") or layer.get("provider_series_id")
        if value:
            return str(value)
    return None


def _provider_child_season_number(child: dict[str, Any]) -> int | None:
    for layer in _child_nested_dicts(child):
        value = _int_value(layer.get("season_number"))
        if value is not None:
            return value
    return None


def _provider_child_episode_count(child: dict[str, Any]) -> int | None:
    for layer in _child_nested_dicts(child):
        for field_name in _PROVIDER_CHILD_EPISODE_COUNT_FIELDS:
            value = _int_value(layer.get(field_name))
            if value is not None:
                return value
    return None


def _provider_child_audio_locales(child: dict[str, Any]) -> list[Any]:
    for layer in _child_nested_dicts(child):
        locales = layer.get("audio_locales")
        if isinstance(locales, list) and locales:
            return list(locales)
    return []


def _provider_child_titles_payload(children: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    child_titles: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for child in children:
        child_id = _provider_child_id(child)
        series_id = _provider_child_series_id(child)
        episode_count = _provider_child_episode_count(child)
        season_number = _provider_child_season_number(child)
        audio_locales = _provider_child_audio_locales(child)
        for field_name, title, normalized in _provider_child_title_norms(child):
            key = (str(child_id or ""), field_name, normalized)
            if key in seen:
                continue
            seen.add(key)
            payload: dict[str, Any] = {
                "title": title,
                "normalized_title": normalized,
                "field": field_name,
            }
            if child_id is not None:
                payload["id"] = child_id
            if series_id is not None:
                payload["series_id"] = series_id
            if season_number is not None:
                payload["season_number"] = season_number
            if episode_count is not None:
                payload["episode_count"] = episode_count
            if audio_locales:
                payload["audio_locales"] = audio_locales
            child_titles.append(payload)
    return tuple(child_titles)


def _provider_child_identity_payload(child: dict[str, Any], field_name: str, title: str, normalized: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "title": title,
        "normalized_title": normalized,
        "field": field_name,
    }
    child_id = _provider_child_id(child)
    if child_id is not None:
        payload["id"] = child_id
    series_id = _provider_child_series_id(child)
    if series_id is not None:
        payload["series_id"] = series_id
    season_number = _provider_child_season_number(child)
    if season_number is not None:
        payload["season_number"] = season_number
    episode_count = _provider_child_episode_count(child)
    if episode_count is not None:
        payload["episode_count"] = episode_count
    audio_locales = _provider_child_audio_locales(child)
    if audio_locales:
        payload["audio_locales"] = audio_locales
    return payload


def _fetch_provider_children_if_available(
    provider: ProviderTitleSearchClient,
    config: AppConfig,
    match: dict[str, Any],
    *,
    provider_session: Any | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    cached_children = _provider_child_items_from_match(match)
    if cached_children:
        return cached_children, False
    provider_slug = str(getattr(provider, "slug", ""))
    provider_series_id = str(match.get("provider_series_id") or "")
    current = _utc_now()
    current_iso = _iso(current)
    if provider_slug == "crunchyroll" and provider_series_id:
        cached = get_provider_enriched_detail_cache(config.db_path, provider=provider_slug,
            provider_series_id=provider_series_id, logic_version=PROVIDER_DETAIL_CACHE_LOGIC_VERSION)
        detail_target_days = max(0, int(getattr(config.mal, "provider_detail_cache_ttl_days", 120)))
        if cached is not None:
            children = _provider_child_items_from_match(cached.response)
            retry_backoff_active = bool(cached.next_retry_at and cached.next_retry_at > current_iso)
            refresh_due = periodic_evidence_is_due(
                successful_at=cached.fetched_at,
                surface="provider_detail",
                identity={"provider": provider_slug, "provider_series_id": provider_series_id, "logic_version": PROVIDER_DETAIL_CACHE_LOGIC_VERSION},
                now=current,
                target_days=detail_target_days,
                jitter_days=min(15, detail_target_days),
            )
            if children and (retry_backoff_active or not refresh_due):
                return children, False
            if cached.status == "failed" and retry_backoff_active:
                return [], False
    children_func = getattr(provider, "fetch_search_result_children", None)
    if not callable(children_func):
        return [], False
    fetched = children_func(config, match, session=provider_session) if provider_session is not None else children_func(config, match)
    children = _coerce_provider_child_items(fetched)
    if provider_slug == "crunchyroll" and provider_series_id and children:
        ttl = max(0, int(getattr(config.mal, "provider_detail_cache_ttl_days", 30)))
        upsert_provider_enriched_detail_cache(config.db_path, provider=provider_slug,
            provider_series_id=provider_series_id, logic_version=PROVIDER_DETAIL_CACHE_LOGIC_VERSION,
            detail={**match, "children": children}, fetched_at=current_iso,
            expires_at=_iso(current + timedelta(days=ttl)))
    return children, True


def _aggregate_shell_parent_metadata_reasons(
    match: dict[str, Any],
    meta: Any,
) -> tuple[bool, list[str], int | None, int | None, int | None, int | None]:
    target_episode_count = _target_episode_count(meta)
    parent_episode_count = _provider_episode_count(match)
    parent_season_count = _provider_season_count(match)
    parent_launch_year = _provider_launch_year(match)
    target_start_year = _target_start_year(meta)
    reasons: list[str] = []

    aggregate_parent = False
    if parent_season_count is not None and parent_season_count > 1:
        aggregate_parent = True
        reasons.append(f"provider_season_count={parent_season_count}")
    if target_episode_count is not None and parent_episode_count is not None and parent_episode_count > target_episode_count:
        aggregate_parent = True
        reasons.append(f"provider_episode_count_exceeds_target={parent_episode_count}>{target_episode_count}")
    if not aggregate_parent:
        return False, reasons, target_episode_count, parent_episode_count, parent_season_count, parent_launch_year
    if target_episode_count is not None and parent_episode_count is not None and parent_episode_count < target_episode_count:
        return False, reasons, target_episode_count, parent_episode_count, parent_season_count, parent_launch_year
    if target_start_year is not None and parent_launch_year is not None:
        if target_start_year != parent_launch_year:
            return False, reasons, target_episode_count, parent_episode_count, parent_season_count, parent_launch_year
        reasons.append(f"provider_launch_year_matches_target={parent_launch_year}")
    elif target_start_year is not None or parent_launch_year is not None:
        return False, reasons, target_episode_count, parent_episode_count, parent_season_count, parent_launch_year
    return True, reasons, target_episode_count, parent_episode_count, parent_season_count, parent_launch_year


def _verify_aggregate_shell_match(
    query: str,
    match: dict[str, Any],
    meta: Any,
    title_family: list[TargetTitleAlias],
    children: list[dict[str, Any]],
) -> AggregateShellVerification | None:
    identity_match_reasons = _plausible_target_overlap_reasons(query, match, title_family)
    if "franchise_shell_overlap" not in identity_match_reasons:
        return None

    aliases = _aliases_by_normalized(title_family)
    parent_ok, parent_reasons, target_episode_count, parent_episode_count, parent_season_count, parent_launch_year = _aggregate_shell_parent_metadata_reasons(match, meta)
    target_start_year = _target_start_year(meta)

    child_titles = _provider_child_titles_payload(children)
    child_identity_candidates: list[tuple[dict[str, Any], str, str, str, int | None, TargetTitleAlias, list[str]]] = []
    for child in children:
        child_episode_count = _provider_child_episode_count(child)
        for field_name, child_title, normalized in _provider_child_title_norms(child):
            alias = aliases.get(normalized)
            if alias is None or not alias.substantive:
                continue
            verification_reasons = [
                f"provider_child_{field_name}_exact_mal_alias:{alias.source}",
                *parent_reasons,
            ]
            if target_episode_count is not None and child_episode_count is not None:
                if target_episode_count != child_episode_count:
                    continue
                verification_reasons.append(f"provider_child_episode_count_matches_target={child_episode_count}")
            elif parent_ok:
                verification_reasons.append("provider_parent_aggregate_metadata_supports_child_identity")
            child_identity_candidates.append((child, field_name, child_title, normalized, child_episode_count, alias, verification_reasons))

    if not child_identity_candidates:
        return None
    # More than one distinct exact child title is a conflicting identity signal;
    # keep the franchise shell in review instead of choosing by rank/order.
    if len({normalized for _child, _field, _title, normalized, _count, _alias, _reasons in child_identity_candidates}) > 1:
        return None
    child, child_field_name, child_title, child_normalized, child_episode_count, _alias, verification_reasons = child_identity_candidates[0]
    return AggregateShellVerification(
        match=match,
        identity_match_kind=PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND,
        child=child,
        child_title=child_title,
        child_identity=_provider_child_identity_payload(child, child_field_name, child_title, child_normalized),
        child_titles=child_titles,
        child_episode_count=child_episode_count,
        target_episode_count=target_episode_count,
        parent_episode_count=parent_episode_count,
        parent_season_count=parent_season_count,
        parent_launch_year=parent_launch_year,
        target_start_year=target_start_year,
        identity_match_reasons=identity_match_reasons,
        verification_reasons=tuple(verification_reasons),
    )


def _aggregate_shell_evidence_payload(verification: AggregateShellVerification) -> dict[str, Any]:
    return {
        "identity_match_kind": verification.identity_match_kind,
        "identity_match_reasons": list(verification.identity_match_reasons),
        "verification_reasons": list(verification.verification_reasons),
        "child_title": verification.child_title,
        "child_identity": verification.child_identity,
        "child_titles": list(verification.child_titles),
        "child_episode_count": verification.child_episode_count,
        "target_episode_count": verification.target_episode_count,
        "parent_episode_count": verification.parent_episode_count,
        "parent_season_count": verification.parent_season_count,
        "parent_launch_year": verification.parent_launch_year,
        "target_start_year": verification.target_start_year,
        "child": verification.child,
    }


def _match_with_aggregate_shell_evidence(match: dict[str, Any], verification: AggregateShellVerification) -> dict[str, Any]:
    enriched = dict(match)
    child_audio_locales = _provider_child_audio_locales(verification.child)
    if child_audio_locales:
        enriched["audio_locales"] = child_audio_locales
    enriched["identity_evidence"] = _aggregate_shell_evidence_payload(verification)
    return enriched


def _is_english_dub_match(match: dict[str, Any]) -> bool:
    """Return True only for explicit provider audio-locale evidence.

    Provider search/title fields are not reliable dub evidence: HIDIVE Algolia
    currently exposes no audio/dub contract, and Crunchyroll discover search can
    return empty audio_locales. Keep title-only markers such as "English Dub"
    out of availability gating so unknown rows are not promoted as dubbed.
    """
    return provider_audio_locales_have_english(_audio_locales(match))


def _audio_locales(match: dict[str, Any]) -> list[Any]:
    return match.get("audio_locales") if isinstance(match.get("audio_locales"), list) else []


def _explicit_dub_evidence_source(provider: str, match: dict[str, Any]) -> str | None:
    if not _is_english_dub_match(match):
        return None
    return "provider_audio_locale" if provider == "crunchyroll" else "provider_audio_tag"


def _english_dub_status_from_match(provider: str, match: dict[str, Any]) -> str:
    if _explicit_dub_evidence_source(provider, match) is not None:
        return "present"
    # Crunchyroll CMS/search audio_locales and HIDIVE Algolia Audio|... tags are
    # explicit provider audio contracts. If one of those contracts is present but
    # lacks English, record a conservative negative instead of pretending no
    # provider evidence was checked. Empty/missing contracts remain unknown.
    locales = _audio_locales(match)
    if locales:
        return "absent"
    return "unknown"


def _identity_text(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _match_identity(match: dict[str, Any]) -> tuple[str, ...]:
    raw = match.get("raw") if isinstance(match.get("raw"), dict) else {}
    return (
        str(match.get("provider_series_id") or "").strip(),
        *(_identity_text(match.get(key)) for key in ("title", "season_title", "url", "catalog_status", "detail_evidence_source")),
        *(_identity_text(raw.get(key)) for key in ("type", "availability_status")),
    )


def _dedupe_provider_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: dict[tuple[str, ...], dict[str, Any]] = {}
    for match in matches:
        key = _match_identity(match)
        existing = seen.get(key)
        if existing is None:
            cloned = dict(match)
            if isinstance(cloned.get("audio_locales"), list):
                cloned["audio_locales"] = list(cloned["audio_locales"])
            seen[key] = cloned
            deduped.append(cloned)
            continue
        _merge_selected_match_evidence(existing, match)
    return deduped


def _review_match_identities(payload: dict[str, Any]) -> tuple[tuple[str, ...], ...]:
    match = payload.get("match")
    if isinstance(match, dict):
        return (_match_identity(match),)
    matches = payload.get("matches")
    if isinstance(matches, list):
        return tuple(_match_identity(item) for item in matches if isinstance(item, dict))
    return ()


def _discovery_review_entry_key(entry: dict[str, Any]) -> tuple[str, str, str, str, str, str, tuple[tuple[str, ...], ...]]:
    payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
    mal_id = payload.get("mal_anime_id")
    return (
        DISCOVERY_PROVIDER_SEARCH_REVIEW_ISSUE,
        str(entry.get("provider") or ""),
        str(mal_id) if mal_id is not None else "",
        _identity_text(payload.get("decision")),
        str(entry.get("provider_series_id") or ""),
        _identity_text(payload.get("query")),
        _review_match_identities(payload),
    )


def _merge_audio_locales(existing: list[Any], incoming: list[Any]) -> list[Any]:
    merged: list[Any] = []
    seen: set[str] = set()
    for locale in [*existing, *incoming]:
        if locale is None:
            continue
        key = str(locale).strip().lower().replace("_", "-")
        if key in seen:
            continue
        seen.add(key)
        merged.append(locale)
    return merged


def _preferred_english_dub_status(existing: Any, incoming: Any) -> Any:
    rank = {"unknown": 1, "absent": 2, "present": 3}
    return incoming if rank.get(str(incoming), 0) > rank.get(str(existing), 0) else existing


def _merge_selected_match_evidence(existing: Any, incoming: Any) -> None:
    if isinstance(existing, dict) and isinstance(incoming, dict) and _match_identity(existing) == _match_identity(incoming):
        existing["audio_locales"] = _merge_audio_locales(_audio_locales(existing), _audio_locales(incoming))
    elif isinstance(existing, list) and isinstance(incoming, list) and len(existing) == len(incoming):
        for existing_item, incoming_item in zip(existing, incoming):
            _merge_selected_match_evidence(existing_item, incoming_item)


def _merge_duplicate_discovery_review_entry(existing: dict[str, Any], duplicate: dict[str, Any]) -> None:
    existing_payload = existing.get("payload") if isinstance(existing.get("payload"), dict) else {}
    duplicate_payload = duplicate.get("payload") if isinstance(duplicate.get("payload"), dict) else {}
    if "provider_search_match_reasons" in existing_payload or "provider_search_match_reasons" in duplicate_payload:
        existing_payload["provider_search_match_reasons"] = _merge_text_list(
            existing_payload.get("provider_search_match_reasons") if isinstance(existing_payload.get("provider_search_match_reasons"), list) else [],
            duplicate_payload.get("provider_search_match_reasons") if isinstance(duplicate_payload.get("provider_search_match_reasons"), list) else [],
        )
    if "audio_locales" in existing_payload or "audio_locales" in duplicate_payload:
        existing_payload["audio_locales"] = _merge_audio_locales(
            existing_payload.get("audio_locales") if isinstance(existing_payload.get("audio_locales"), list) else [],
            duplicate_payload.get("audio_locales") if isinstance(duplicate_payload.get("audio_locales"), list) else [],
        )
    if "english_dub_status" in existing_payload or "english_dub_status" in duplicate_payload:
        existing_payload["english_dub_status"] = _preferred_english_dub_status(
            existing_payload.get("english_dub_status"), duplicate_payload.get("english_dub_status")
        )
    if not existing_payload.get("explicit_dub_evidence_source") and duplicate_payload.get("explicit_dub_evidence_source"):
        existing_payload["explicit_dub_evidence_source"] = duplicate_payload["explicit_dub_evidence_source"]
    _merge_selected_match_evidence(existing_payload.get("match"), duplicate_payload.get("match"))
    _merge_selected_match_evidence(existing_payload.get("matches"), duplicate_payload.get("matches"))


def _dedupe_discovery_review_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: dict[tuple[str, str, str, str, str, str, tuple[tuple[str, ...], ...]], dict[str, Any]] = {}
    for entry in entries:
        key = _discovery_review_entry_key(entry)
        existing = seen.get(key)
        if existing is None:
            seen[key] = entry
            deduped.append(entry)
            continue
        _merge_duplicate_discovery_review_entry(existing, entry)
    return deduped


def _merge_text_list(existing: list[Any], incoming: list[Any]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*existing, *incoming]:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        merged.append(text)
    return merged


def _review_entry_payload(entry: dict[str, Any]) -> dict[str, Any]:
    return entry.get("payload") if isinstance(entry.get("payload"), dict) else {}


def _review_entry_target_key(entry: dict[str, Any]) -> tuple[str, str]:
    payload = _review_entry_payload(entry)
    mal_id = payload.get("mal_anime_id")
    return (str(entry.get("provider") or ""), str(mal_id) if mal_id is not None else "")


def _review_entry_decision(entry: dict[str, Any]) -> str:
    return str(_review_entry_payload(entry).get("decision") or "")


def _coalesce_discovery_review_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    coalesced: list[dict[str, Any]] = []
    strong_by_identity: dict[tuple[str, str, str], dict[str, Any]] = {}
    strong_ids_by_target: dict[tuple[str, str], set[str]] = {}
    for entry in entries:
        if _review_entry_decision(entry) == "strong_provider_search_candidate_no_auto_link":
            target_key = _review_entry_target_key(entry)
            provider_series_id = str(entry.get("provider_series_id") or "").strip()
            if provider_series_id:
                strong_ids_by_target.setdefault(target_key, set()).add(provider_series_id)
                identity_key = (*target_key, provider_series_id)
                existing = strong_by_identity.get(identity_key)
                if existing is not None:
                    _merge_duplicate_discovery_review_entry(existing, entry)
                    continue
                strong_by_identity[identity_key] = entry
        coalesced.append(entry)

    filtered: list[dict[str, Any]] = []
    for entry in coalesced:
        if _review_entry_decision(entry) == "ambiguous_no_auto_link" and len(strong_ids_by_target.get(_review_entry_target_key(entry), set())) == 1:
            continue
        filtered.append(entry)
    return filtered


def _discovery_review_dub_payload(provider: str, match: dict[str, Any]) -> dict[str, Any]:
    return {
        "english_dub_status": _english_dub_status_from_match(provider, match),
        "audio_locales": list(_audio_locales(match)),
        "explicit_dub_evidence_source": _explicit_dub_evidence_source(provider, match),
    }


def _catalog_status_from_match(match: dict[str, Any]) -> str:
    status = str(match.get("catalog_status") or "").strip().lower()
    if status in {"present", "absent"}:
        return status
    raw = match.get("raw") if isinstance(match.get("raw"), dict) else {}
    raw_status = str(raw.get("catalog_status") or "").strip().lower()
    if raw_status in {"present", "absent"}:
        return raw_status
    # A normalized series-like provider search result is a catalog-presence
    # signal. This does not verify MAL<->provider identity; review remains gated.
    return "present"


def _provider_detail_needed(provider: ProviderTitleSearchClient, match: dict[str, Any]) -> bool:
    provider_slug = str(getattr(provider, "slug", "")).strip().lower()
    if provider_slug == "crunchyroll":
        return not _audio_locales(match)
    return False


def _fetch_provider_detail_if_available(
    provider: ProviderTitleSearchClient,
    config: AppConfig,
    match: dict[str, Any],
    *,
    now: datetime | None = None,
    force_refresh: bool = False,
    provider_session: Any | None = None,
    provider_session_factory: Callable[[], Any | None] | None = None,
) -> tuple[dict[str, Any], bool]:
    detail_func = getattr(provider, "fetch_search_result_detail", None)
    if not callable(detail_func) or not _provider_detail_needed(provider, match):
        return match, False
    provider_slug = str(getattr(provider, "slug", ""))
    provider_series_id = str(match.get("provider_series_id") or "")
    current = now or _utc_now()
    current_iso = _iso(current)
    if provider_slug == "crunchyroll" and provider_series_id and not force_refresh:
        cached = get_provider_enriched_detail_cache(config.db_path, provider=provider_slug,
            provider_series_id=provider_series_id, logic_version=PROVIDER_DETAIL_CACHE_LOGIC_VERSION)
        detail_target_days = max(0, int(getattr(config.mal, "provider_detail_cache_ttl_days", 120)))
        if cached is not None and cached.status == "ok":
            retry_backoff_active = bool(cached.next_retry_at and cached.next_retry_at > current_iso)
            refresh_due = periodic_evidence_is_due(
                successful_at=cached.fetched_at,
                surface="provider_detail",
                identity={"provider": provider_slug, "provider_series_id": provider_series_id, "logic_version": PROVIDER_DETAIL_CACHE_LOGIC_VERSION},
                now=current,
                target_days=detail_target_days,
                jitter_days=min(15, detail_target_days),
            )
            if retry_backoff_active or not refresh_due:
                return _match_to_dict(cached.response), False
        if cached is not None and cached.status == "failed" and cached.next_retry_at and cached.next_retry_at > current_iso:
            return match, False
    if provider_session is None and provider_session_factory is not None:
        provider_session = provider_session_factory()
    try:
        enriched = detail_func(config, match, session=provider_session) if provider_session is not None else detail_func(config, match)
    except Exception as exc:
        if provider_slug == "crunchyroll" and provider_series_id:
            prior = get_provider_enriched_detail_cache(config.db_path, provider=provider_slug,
                provider_series_id=provider_series_id, logic_version=PROVIDER_DETAIL_CACHE_LOGIC_VERSION)
            failure_count = (prior.failure_count if prior else 0) + 1
            delay_hours = min(24, 2 ** min(failure_count - 1, 5))
            record_provider_enriched_detail_failure(config.db_path, provider=provider_slug,
                provider_series_id=provider_series_id, logic_version=PROVIDER_DETAIL_CACHE_LOGIC_VERSION,
                fetched_at=current_iso, next_retry_at=_iso(current + timedelta(hours=delay_hours)),
                expires_at=_iso(current + timedelta(days=7)), error=str(exc))
        raise
    if enriched is None:
        return match, True
    normalized = _match_to_dict(enriched)
    if provider_slug == "crunchyroll" and provider_series_id:
        ttl = max(0, int(getattr(config.mal, "provider_detail_cache_ttl_days", 30)))
        upsert_provider_enriched_detail_cache(config.db_path, provider=provider_slug,
            provider_series_id=provider_series_id, logic_version=PROVIDER_DETAIL_CACHE_LOGIC_VERSION,
            detail=normalized, fetched_at=current_iso, expires_at=_iso(current + timedelta(days=ttl)))
    return normalized, True


def _upsert_search_eligibility_evidence(
    config: AppConfig,
    *,
    provider: str,
    mal_anime_id: int,
    candidate_title: str,
    query: str,
    match: dict[str, Any],
    mapping: Any | None,
    search_identity_match_kind: str,
    fetched_at: str,
    expires_at: str,
) -> tuple[bool, bool, bool]:
    """Persist normalized provider eligibility evidence without auto-approving title search identity."""
    provider_series_id = match.get("provider_series_id")
    if provider not in DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS or not provider_series_id:
        return False, False, False
    approved_identity = bool(
        mapping is not None
        and getattr(mapping, "approved_by_user", False)
        and int(getattr(mapping, "mal_anime_id", -1)) == int(mal_anime_id)
    )
    verified_search_identity = search_identity_match_kind in VERIFIED_PROVIDER_SEARCH_IDENTITY_KINDS
    identity_match_kind = "approved_mapping" if approved_identity else search_identity_match_kind
    match_confidence = getattr(mapping, "confidence", None) if approved_identity else PROVIDER_SEARCH_IDENTITY_CONFIDENCE.get(search_identity_match_kind)
    explicit_source = _explicit_dub_evidence_source(provider, match)
    catalog_status = _catalog_status_from_match(match)
    english_dub_status = _english_dub_status_from_match(provider, match)
    verified_identity = approved_identity or verified_search_identity
    audio_locales = match.get("audio_locales") if isinstance(match.get("audio_locales"), list) else []
    evidence_candidate = {
        "provider": provider,
        "identity_match_kind": identity_match_kind,
        "review_status": "verified" if verified_identity else "review-needed",
        "catalog_status": catalog_status,
        "english_dub_status": english_dub_status,
        "audio_locales": audio_locales,
        "expires_at": expires_at,
    }
    last_verified_at = strict_provider_last_verified_at_for_persistence(evidence_candidate, verified_at=fetched_at, now=fetched_at)
    verified_actionable = is_strict_provider_eligibility_actionable({**evidence_candidate, "last_verified_at": last_verified_at}, now=fetched_at)

    existing = get_recommendation_provider_eligibility_evidence(
        config.db_path,
        mal_anime_id=int(mal_anime_id),
        provider=provider,
        provider_series_id=str(provider_series_id),
    )
    if verified_identity and existing is not None and existing.fetched_at == fetched_at:
        catalog_status = _preferred_english_dub_status(existing.catalog_status, catalog_status)
        english_dub_status = _preferred_english_dub_status(existing.english_dub_status, english_dub_status)
        audio_locales = _merge_audio_locales(existing.audio_locales, audio_locales)
        if explicit_source is None:
            explicit_source = existing.explicit_dub_evidence_source
        evidence_candidate.update(
            {
                "catalog_status": catalog_status,
                "english_dub_status": english_dub_status,
                "audio_locales": audio_locales,
            }
        )
        last_verified_at = strict_provider_last_verified_at_for_persistence(evidence_candidate, verified_at=fetched_at, now=fetched_at)
        if last_verified_at is None and is_strict_provider_eligibility_actionable(existing, now=fetched_at):
            last_verified_at = existing.last_verified_at
        verified_actionable = is_strict_provider_eligibility_actionable({**evidence_candidate, "last_verified_at": last_verified_at}, now=fetched_at)
    upsert_recommendation_provider_eligibility_evidence(
        config.db_path,
        mal_anime_id=int(mal_anime_id),
        provider=provider,
        provider_series_id=str(provider_series_id),
        provider_title=match.get("title") or match.get("season_title"),
        provider_url=match.get("url"),
        identity_match_kind=identity_match_kind,
        match_confidence=match_confidence,
        review_status="verified" if verified_identity else "review-needed",
        catalog_status=catalog_status,
        english_dub_status=english_dub_status,
        explicit_dub_evidence_source=explicit_source,
        audio_locales=audio_locales,
        source_evidence={
            "source": "bounded_provider_title_search",
            "query": query,
            "candidate_mal_anime_id": int(mal_anime_id),
            "candidate_title": candidate_title,
            "provider": provider,
            "provider_series_id": str(provider_series_id),
            "match": match,
            "approved_mapping": bool(approved_identity),
            "catalog_status": catalog_status,
            "catalog_evidence_source": match.get("detail_evidence_source") or ("provider_title_search_result" if catalog_status == "present" else "provider_title_search_result_negative"),
            "english_dub_status": english_dub_status,
            "explicit_dub_evidence_source": explicit_source,
            "mapping_source": getattr(mapping, "mapping_source", None) if mapping is not None else None,
            "mapping_confidence": getattr(mapping, "confidence", None) if mapping is not None else None,
            "search_identity_match_kind": search_identity_match_kind,
            "identity_evidence": match.get("identity_evidence") if isinstance(match.get("identity_evidence"), dict) else None,
        },
        fetched_at=fetched_at,
        expires_at=expires_at,
        last_verified_at=last_verified_at,
        refresh_status="ok",
        failure_count=0,
        next_retry_at=None,
        logic_version=PROVIDER_ELIGIBILITY_LOGIC_VERSION,
        verification_outcome="positive" if verified_actionable else "unknown",
        refresh_due_at=(
            provider_eligibility_refresh_due_at(
                successful_verified_at=fetched_at,
                mal_anime_id=int(mal_anime_id),
                provider=provider,
                provider_series_id=str(provider_series_id),
                target_days=config.service.provider_eligibility_refresh_target_days,
                jitter_days=config.service.provider_eligibility_refresh_jitter_days,
            )
            if verified_actionable
            else None
        ),
        refresh_schedule_version=PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
        refresh_schedule_key=provider_eligibility_refresh_schedule_key(
            mal_anime_id=int(mal_anime_id),
            provider=provider,
            provider_series_id=str(provider_series_id),
        ),
        last_successful_positive_at=last_verified_at if verified_actionable else None,
    )
    return True, verified_identity, verified_actionable


def _record_eligibility_refresh_failure(
    config: AppConfig,
    evidence: Any,
    *,
    now: datetime,
) -> None:
    failure_count = max(0, int(evidence.failure_count)) + 1
    delay_hours = min(24, 2 ** min(failure_count - 1, 5))
    upsert_recommendation_provider_eligibility_evidence(
        config.db_path,
        mal_anime_id=evidence.mal_anime_id,
        provider=evidence.provider,
        provider_series_id=evidence.provider_series_id,
        provider_title=evidence.provider_title,
        provider_url=evidence.provider_url,
        identity_match_kind=evidence.identity_match_kind,
        match_confidence=evidence.match_confidence,
        review_status=evidence.review_status,
        catalog_status=evidence.catalog_status,
        english_dub_status=evidence.english_dub_status,
        explicit_dub_evidence_source=evidence.explicit_dub_evidence_source,
        audio_locales=evidence.audio_locales,
        source_evidence=evidence.source_evidence,
        fetched_at=evidence.fetched_at,
        expires_at=evidence.expires_at,
        last_verified_at=evidence.last_verified_at,
        refresh_status="failed",
        failure_count=failure_count,
        next_retry_at=_iso(now + timedelta(hours=delay_hours)),
        logic_version=PROVIDER_ELIGIBILITY_LOGIC_VERSION,
    )


def _record_selection_refresh_failures(
    config: AppConfig,
    selection: ProviderEnrichmentCandidate,
    *,
    now: datetime,
) -> int:
    preserved = 0
    for evidence in selection.due_evidence:
        _record_eligibility_refresh_failure(config, evidence, now=now)
        if evidence.last_successful_positive_at and not evidence.invalidated_at:
            preserved += 1
    return preserved


def _decode_json_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _decode_json_list(value: Any) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return []
    return decoded if isinstance(decoded, list) else []


def _current_provider_eligibility_rows_for_reconciliation(
    config: AppConfig,
    provider_series_keys: Any,
) -> list[dict[str, Any]]:
    normalized_keys = sorted(
        {
            (str(provider).strip().lower(), str(provider_series_id).strip())
            for provider, provider_series_id in provider_series_keys
            if str(provider).strip() and str(provider_series_id).strip()
        }
    )
    if not normalized_keys:
        return []
    conditions = " OR ".join("(e.provider = ? AND e.provider_series_id = ?)" for _ in normalized_keys)
    params: list[object] = []
    for provider, provider_series_id in normalized_keys:
        params.extend([provider, provider_series_id])
    with connect(config.db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                e.*,
                s.title AS current_provider_title,
                s.season_title AS current_provider_season_title,
                s.season_number AS current_provider_season_number,
                s.raw_json AS current_provider_series_raw_json
            FROM recommendation_provider_eligibility_evidence e
            LEFT JOIN provider_series s
                ON s.provider = e.provider
               AND s.provider_series_id = e.provider_series_id
            WHERE {conditions}
            ORDER BY e.provider ASC, e.provider_series_id ASC, e.mal_anime_id ASC
            """,
            params,
        ).fetchall()
    prepared: list[dict[str, Any]] = []
    for row in rows:
        prepared.append(
            {
                "mal_anime_id": int(row["mal_anime_id"]),
                "provider": str(row["provider"]),
                "provider_series_id": str(row["provider_series_id"]),
                "provider_title": row["provider_title"],
                "provider_url": row["provider_url"],
                "identity_match_kind": str(row["identity_match_kind"]),
                "match_confidence": None if row["match_confidence"] is None else float(row["match_confidence"]),
                "review_status": str(row["review_status"]),
                "catalog_status": str(row["catalog_status"]),
                "english_dub_status": str(row["english_dub_status"]),
                "explicit_dub_evidence_source": row["explicit_dub_evidence_source"],
                "audio_locales": _decode_json_list(row["audio_locales_json"]),
                "source_evidence": _decode_json_object(row["source_evidence_json"]),
                "fetched_at": str(row["fetched_at"]),
                "expires_at": str(row["expires_at"]),
                "last_verified_at": row["last_verified_at"],
                "refresh_status": str(row["refresh_status"]),
                "failure_count": int(row["failure_count"]),
                "next_retry_at": row["next_retry_at"],
                "logic_version": str(row["logic_version"]),
                "current_provider_title": row["current_provider_title"],
                "current_provider_season_title": row["current_provider_season_title"],
                "current_provider_season_number": row["current_provider_season_number"],
                "current_provider_series_raw": _decode_json_object(row["current_provider_series_raw_json"]),
            }
        )
    return prepared


def _approved_mappings_by_series(mappings_by_series: dict[tuple[str, str], Any]) -> dict[tuple[str, str], Any]:
    return {
        key: mapping
        for key, mapping in mappings_by_series.items()
        if bool(getattr(mapping, "approved_by_user", False))
    }


def _exact_approved_mappings_by_series(mappings_by_series: dict[tuple[str, str], Any]) -> dict[tuple[str, str], Any]:
    return {
        key: mapping
        for key, mapping in _approved_mappings_by_series(mappings_by_series).items()
        if str(getattr(mapping, "mapping_source", "")).strip() in _EXACT_APPROVED_MAPPING_SOURCES
    }


def _deterministic_child_identity_payload(source_evidence: dict[str, Any]) -> dict[str, Any] | None:
    identity_evidence = source_evidence.get("identity_evidence")
    if not isinstance(identity_evidence, dict):
        match = source_evidence.get("match")
        if isinstance(match, dict) and isinstance(match.get("identity_evidence"), dict):
            identity_evidence = match["identity_evidence"]
    if not isinstance(identity_evidence, dict):
        return None
    child_identity = identity_evidence.get("child_identity")
    return child_identity if isinstance(child_identity, dict) and child_identity else None


def _has_current_verified_child_identity(row: dict[str, Any]) -> bool:
    return (
        str(row.get("identity_match_kind") or "") == PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND
        and str(row.get("review_status") or "").strip().lower() == "verified"
        and _deterministic_child_identity_payload(row.get("source_evidence") if isinstance(row.get("source_evidence"), dict) else {}) is not None
    )


def _provider_mapping_evidence_for_eligibility_row(row: dict[str, Any]) -> Any:
    evidence_items = []
    provider_raw = row.get("current_provider_series_raw")
    if isinstance(provider_raw, dict) and provider_raw:
        evidence_items.append(extract_provider_mapping_evidence(provider_raw))
    source_evidence = row.get("source_evidence") if isinstance(row.get("source_evidence"), dict) else {}
    if source_evidence:
        evidence_items.append(extract_provider_mapping_evidence(source_evidence))
        match = source_evidence.get("match")
        if isinstance(match, dict):
            evidence_items.append(extract_provider_mapping_evidence(match))
        identity_evidence = source_evidence.get("identity_evidence")
        if isinstance(identity_evidence, dict):
            evidence_items.append(extract_provider_mapping_evidence({"identity_evidence": identity_evidence}))
    if not evidence_items:
        return extract_provider_mapping_evidence({})
    return merge_provider_mapping_evidence(*evidence_items)


def _is_aggregate_provider_parent_row(row: dict[str, Any], meta: Any | None) -> bool:
    provider_evidence = _provider_mapping_evidence_for_eligibility_row(row)
    provider_episode_count = provider_evidence.episode_count
    target_episode_count = _target_episode_count(meta) if meta is not None else None
    if provider_episode_count is None or target_episode_count is None or target_episode_count <= 0:
        return False
    if provider_episode_count <= target_episode_count:
        return False
    season_count = provider_evidence.season_count
    return season_count is not None and season_count >= 3


def _eligibility_record_value(record: Any, key: str) -> Any:
    if isinstance(record, dict):
        return record.get(key)
    return getattr(record, key, None)


def _is_legacy_or_pending_provider_eligibility_record(record: Any) -> bool:
    review_status = str(_eligibility_record_value(record, "review_status") or "").strip().lower()
    logic_version = str(_eligibility_record_value(record, "logic_version") or "").strip()
    return (
        review_status in _PENDING_PROVIDER_ELIGIBILITY_REVIEW_STATUSES
        or logic_version in _LEGACY_PROVIDER_ELIGIBILITY_LOGIC_VERSIONS
    )


def _legacy_eligibility_row_needs_approved_mapping_rewrite(row: dict[str, Any]) -> bool:
    return (
        str(row.get("identity_match_kind") or "") != "approved_mapping"
        or str(row.get("review_status") or "").strip().lower() != "verified"
        or str(row.get("logic_version") or "") in _LEGACY_PROVIDER_ELIGIBILITY_LOGIC_VERSIONS
    )


def _delete_legacy_provider_eligibility_row(config: AppConfig, row: dict[str, Any]) -> int:
    return delete_recommendation_provider_eligibility_evidence(
        config.db_path,
        mal_anime_id=int(row["mal_anime_id"]),
        provider=str(row["provider"]),
        provider_series_id=str(row["provider_series_id"]),
    )


def _rewrite_legacy_provider_eligibility_row_as_approved_mapping(
    config: AppConfig,
    row: dict[str, Any],
    mapping: Any,
) -> None:
    upsert_recommendation_provider_eligibility_evidence(
        config.db_path,
        mal_anime_id=int(row["mal_anime_id"]),
        provider=str(row["provider"]),
        provider_series_id=str(row["provider_series_id"]),
        provider_title=row.get("provider_title"),
        provider_url=row.get("provider_url"),
        identity_match_kind="approved_mapping",
        match_confidence=getattr(mapping, "confidence", None) if getattr(mapping, "confidence", None) is not None else row.get("match_confidence"),
        review_status="verified",
        catalog_status=str(row.get("catalog_status") or "unknown"),
        english_dub_status=str(row.get("english_dub_status") or "unknown"),
        explicit_dub_evidence_source=row.get("explicit_dub_evidence_source"),
        audio_locales=list(row.get("audio_locales") or []),
        source_evidence=dict(row.get("source_evidence") or {}),
        fetched_at=str(row["fetched_at"]),
        expires_at=str(row["expires_at"]),
        last_verified_at=row.get("last_verified_at"),
        refresh_status=str(row.get("refresh_status") or "ok"),
        failure_count=int(row.get("failure_count") or 0),
        next_retry_at=row.get("next_retry_at"),
        logic_version=PROVIDER_ELIGIBILITY_LOGIC_VERSION,
    )


def _reconcile_legacy_provider_eligibility_evidence(
    config: AppConfig,
    summary: EnrichmentSummary,
    *,
    metadata: dict[int, Any],
    mappings_by_series: dict[tuple[str, str], Any],
) -> None:
    approved_mappings = _approved_mappings_by_series(mappings_by_series)
    exact_approved_mappings = _exact_approved_mappings_by_series(mappings_by_series)
    if not approved_mappings:
        return
    for row in _current_provider_eligibility_rows_for_reconciliation(config, approved_mappings.keys()):
        key = (str(row["provider"]), str(row["provider_series_id"]))
        approved_mapping = approved_mappings.get(key)
        if approved_mapping is None:
            continue
        if int(getattr(approved_mapping, "mal_anime_id")) != int(row["mal_anime_id"]):
            summary.legacy_eligibility_rows_deleted += _delete_legacy_provider_eligibility_row(config, row)
            continue
        if not _is_legacy_or_pending_provider_eligibility_record(row):
            continue
        child_identity = _has_current_verified_child_identity(row)
        if _is_aggregate_provider_parent_row(row, metadata.get(int(row["mal_anime_id"]))) and not child_identity:
            summary.legacy_eligibility_rows_deleted += _delete_legacy_provider_eligibility_row(config, row)
            continue
        if child_identity:
            continue
        exact_mapping = exact_approved_mappings.get(key)
        if exact_mapping is None:
            continue
        if not _legacy_eligibility_row_needs_approved_mapping_rewrite(row):
            continue
        _rewrite_legacy_provider_eligibility_row_as_approved_mapping(config, row, exact_mapping)
        summary.legacy_eligibility_rows_reconciled += 1


def _provider_title_norms(match: dict[str, Any]) -> list[tuple[str, str]]:
    norms: list[tuple[str, str]] = []
    seen: set[str] = set()
    for field_name in ("title", "season_title"):
        normalized = normalize_title(match.get(field_name))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        norms.append((field_name, normalized))
    return norms


def _aliases_by_normalized(title_family: list[TargetTitleAlias]) -> dict[str, TargetTitleAlias]:
    return {alias.normalized: alias for alias in title_family}


def _exact_target_alias_reasons(query: str, match: dict[str, Any], title_family: list[TargetTitleAlias]) -> tuple[str, ...]:
    aliases = _aliases_by_normalized(title_family)
    query_normalized = normalize_title(query)
    reasons: list[str] = []
    seen: set[str] = set()
    for field_name, provider_normalized in _provider_title_norms(match):
        alias = aliases.get(provider_normalized)
        if alias is None:
            continue
        reason = f"{field_name}_exact_mal_alias:{alias.source}"
        if reason not in seen:
            seen.add(reason)
            reasons.append(reason)
        if query_normalized and provider_normalized == query_normalized:
            query_reason = "provider_title_exact_current_query"
            if query_reason not in seen:
                seen.add(query_reason)
                reasons.append(query_reason)
    return tuple(reasons)


def _provider_result_identity(match: dict[str, Any]) -> str:
    provider_series_id = str(match.get("provider_series_id") or "").strip()
    if provider_series_id:
        return provider_series_id
    return "|".join(_match_identity(match))


def _provider_match_preference(match: dict[str, Any]) -> tuple[int, int, int]:
    return (
        1 if _is_english_dub_match(match) else 0,
        len(_audio_locales(match)),
        1 if _catalog_status_from_match(match) == "present" else 0,
    )


def _meaningful_tokens(normalized: str) -> set[str]:
    return {token for token in normalized.split() if len(token) > 1 and token not in _WEAK_LEXICAL_STOPWORDS}


def _has_meaningful_title_overlap(provider_normalized: str, target_normalized: str) -> bool:
    if not provider_normalized or not target_normalized or provider_normalized == target_normalized:
        return False
    provider_tokens = _meaningful_tokens(provider_normalized)
    target_tokens = _meaningful_tokens(target_normalized)
    if not provider_tokens or not target_tokens:
        return False
    overlap = provider_tokens & target_tokens
    if len(overlap) >= 3:
        return True
    return len(overlap) >= 2 and len(overlap) >= max(2, min(len(provider_tokens), len(target_tokens)) // 2)


def _franchise_shell_reason(match: dict[str, Any]) -> str | None:
    for _field_name, normalized in _provider_title_norms(match):
        if _meaningful_tokens(normalized) & _FRANCHISE_SHELL_TOKENS:
            return "franchise_shell_overlap"
    return None


def _plausible_target_overlap_reasons(query: str, match: dict[str, Any], title_family: list[TargetTitleAlias]) -> tuple[str, ...]:
    comparison_aliases = [alias for alias in title_family if alias.substantive]
    query_normalized = normalize_title(query)
    if query_normalized and all(alias.normalized != query_normalized for alias in comparison_aliases):
        comparison_aliases.append(TargetTitleAlias(text=query, normalized=query_normalized, source="current_query", substantive=False))
    reasons: list[str] = []
    seen: set[str] = set()
    for field_name, provider_normalized in _provider_title_norms(match):
        for alias in comparison_aliases:
            if _has_meaningful_title_overlap(provider_normalized, alias.normalized):
                shell_reason = _franchise_shell_reason(match)
                reason = shell_reason or f"{field_name}_lexical_overlap_mal_alias:{alias.source}"
                if reason not in seen:
                    seen.add(reason)
                    reasons.append(reason)
    return tuple(reasons)


def _query_title_family(query: str) -> list[TargetTitleAlias]:
    normalized = normalize_title(query)
    if not normalized:
        return []
    return [TargetTitleAlias(text=query.strip(), normalized=normalized, source="current_query", substantive=True)]


def _coerce_title_family(query: str, target: Any | None) -> list[TargetTitleAlias]:
    if target is None:
        return _query_title_family(query)
    if isinstance(target, list):
        if all(isinstance(alias, TargetTitleAlias) for alias in target):
            return target
        aliases: list[TargetTitleAlias] = []
        for value in target:
            if not isinstance(value, str):
                continue
            normalized = normalize_title(value)
            if normalized:
                aliases.append(TargetTitleAlias(text=value.strip(), normalized=normalized, source="provided_alias", substantive=True))
        return aliases
    return build_target_title_family(target)


def classify_provider_matches(
    query: str,
    matches: list[dict[str, Any]],
    target: Any | None = None,
) -> ProviderSearchCandidateDecision:
    title_family = _coerce_title_family(query, target)
    exact_by_identity: dict[str, tuple[dict[str, Any], tuple[str, ...]]] = {}
    for match in matches:
        reasons = _exact_target_alias_reasons(query, match, title_family)
        if reasons:
            identity = _provider_result_identity(match)
            existing = exact_by_identity.get(identity)
            if existing is None or _provider_match_preference(match) > _provider_match_preference(existing[0]):
                exact_by_identity[identity] = (match, reasons)
    exact_matches = list(exact_by_identity.values())
    if exact_matches:
        if len(exact_matches) == 1:
            selected, reasons = max(exact_matches, key=lambda item: _provider_match_preference(item[0]))
            return ProviderSearchCandidateDecision(kind="strong", selected=[selected], reasons=reasons)
        reasons = ["multiple_exact_mal_alias_provider_ids"]
        for _match, match_reasons in exact_matches:
            for reason in match_reasons:
                if reason not in reasons:
                    reasons.append(reason)
        return ProviderSearchCandidateDecision(
            kind="ambiguous",
            selected=[match for match, _reasons in exact_matches],
            reasons=tuple(reasons),
        )

    plausible: list[dict[str, Any]] = []
    plausible_reasons: list[str] = []
    for match in matches:
        reasons = _plausible_target_overlap_reasons(query, match, title_family)
        if not reasons:
            continue
        plausible.append(match)
        for reason in reasons:
            if reason not in plausible_reasons:
                plausible_reasons.append(reason)
    if plausible:
        return ProviderSearchCandidateDecision(kind="ambiguous", selected=plausible, reasons=tuple(plausible_reasons))
    return ProviderSearchCandidateDecision(
        kind="none",
        selected=[],
        reasons=(),
        suppress_reason="no_provider_title_match_in_mal_title_family",
    )


def _search_identity_match_kind(query: str, match: dict[str, Any], target: Any | None = None) -> str:
    if target is not None and _exact_target_alias_reasons(query, match, _coerce_title_family(query, target)):
        return PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND
    q = normalize_title(query)
    if q and (normalize_title(match.get("title")) == q or normalize_title(match.get("season_title")) == q):
        return PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND
    return "provider_title_search"


def _has_unique_exact_mal_title_family_reason(decision: ProviderSearchCandidateDecision) -> bool:
    """Return True for a unique exact match against MAL title-family metadata.

    A provider title that only matches the current search query is deliberately
    not enough here; this suppresses review only when the classifier matched an
    exact MAL alias/title-family reason.
    """
    if decision.kind != "strong" or len(decision.selected) != 1:
        return False
    return any(
        (reason.startswith("title_exact_mal_alias:") or reason.startswith("season_title_exact_mal_alias:"))
        and not reason.endswith(":current_query")
        for reason in decision.reasons
    )


def _append_provider_search_review_entry(
    review_entries: list[dict[str, Any]],
    *,
    provider: str,
    provider_series_id: Any,
    mal_id: int,
    candidate_title: str,
    query: str,
    match: dict[str, Any],
    decision: ProviderSearchCandidateDecision,
) -> None:
    review_entries.append({
        "provider": provider,
        "provider_series_id": str(provider_series_id),
        "severity": "info",
        "payload": {
            "mal_anime_id": mal_id,
            "candidate_title": candidate_title,
            "query": query,
            "match": match,
            "decision": "strong_provider_search_candidate_no_auto_link",
            "provider_search_match_reasons": list(decision.reasons),
            **_discovery_review_dub_payload(provider, match),
        },
    })


def _upsert_exact_identity_or_append_review(
    config: AppConfig,
    summary: EnrichmentSummary,
    review_entries: list[dict[str, Any]],
    *,
    provider: str,
    provider_series_id: Any,
    mal_id: int,
    candidate_title: str,
    query: str,
    match: dict[str, Any],
    mapping: Any | None,
    decision: ProviderSearchCandidateDecision,
    title_family: list[TargetTitleAlias],
    fetched_at: str,
    expires_at: str,
) -> None:
    persisted, verified_identity, verified_actionable = _upsert_search_eligibility_evidence(
        config,
        provider=provider,
        mal_anime_id=mal_id,
        candidate_title=candidate_title,
        query=query,
        match=match,
        mapping=mapping,
        search_identity_match_kind=_search_identity_match_kind(query, match, title_family),
        fetched_at=fetched_at,
        expires_at=expires_at,
    )
    if persisted:
        summary.eligibility_evidence_upserted += 1
    if verified_actionable:
        summary.verified_eligibility_evidence_upserted += 1
    if persisted and verified_identity and _has_unique_exact_mal_title_family_reason(decision):
        summary.exact_verified_identities_no_review += 1
        return
    _append_provider_search_review_entry(
        review_entries,
        provider=provider,
        provider_series_id=provider_series_id,
        mal_id=mal_id,
        candidate_title=candidate_title,
        query=query,
        match=match,
        decision=decision,
    )


def _upsert_aggregate_shell_identity(
    config: AppConfig,
    summary: EnrichmentSummary,
    *,
    provider: str,
    mal_id: int,
    candidate_title: str,
    query: str,
    verification: AggregateShellVerification,
    mapping: Any | None,
    fetched_at: str,
    expires_at: str,
) -> bool:
    match = _match_with_aggregate_shell_evidence(verification.match, verification)
    provider_series_id = match.get("provider_series_id")
    if not provider_series_id:
        return False
    persisted, verified_identity, verified_actionable = _upsert_search_eligibility_evidence(
        config,
        provider=provider,
        mal_anime_id=mal_id,
        candidate_title=candidate_title,
        query=query,
        match=match,
        mapping=mapping,
        search_identity_match_kind=verification.identity_match_kind,
        fetched_at=fetched_at,
        expires_at=expires_at,
    )
    if not persisted or not verified_identity:
        return False
    _ensure_provider_series(config, provider=provider, match=match)
    summary.eligibility_evidence_upserted += 1
    if verified_actionable:
        summary.verified_eligibility_evidence_upserted += 1
    summary.franchise_shell_verified_matches += 1
    summary.franchise_shell_verified_identities_no_review += 1
    summary.aggregate_shells_verified_no_review += 1
    summary.strong_matches += 1
    return True


def _verified_aggregate_shell_candidates(
    config: AppConfig,
    summary: EnrichmentSummary,
    *,
    provider: ProviderTitleSearchClient,
    query: str,
    decision: ProviderSearchCandidateDecision,
    meta: Any,
    title_family: list[TargetTitleAlias],
    child_probe_cache: dict[tuple[str, str], list[dict[str, Any]]],
    child_probe_failures: set[tuple[str, str]],
    provider_session: Any | None = None,
) -> list[AggregateShellVerification]:
    if "franchise_shell_overlap" not in decision.reasons:
        return []
    verified: list[AggregateShellVerification] = []
    verified_identities: set[str] = set()
    for selected_match in decision.selected:
        match = selected_match
        children: list[dict[str, Any]] = []
        children_attempted = False
        provider_series_id = str(match.get("provider_series_id") or "").strip()
        probe_key = (str(provider.slug), provider_series_id) if provider_series_id else None
        try:
            embedded_children = _provider_child_items_from_match(match)
            if embedded_children:
                children = embedded_children
            elif probe_key is not None and probe_key in child_probe_cache:
                children = child_probe_cache[probe_key]
            elif probe_key is not None and probe_key in child_probe_failures:
                children = []
            else:
                children, children_attempted = _fetch_provider_children_if_available(provider, config, match, provider_session=provider_session)
                if probe_key is not None and children_attempted:
                    child_probe_cache[probe_key] = children
            if children_attempted:
                summary.provider_detail_probes += 1
        except Exception as exc:  # child-season probes are optional; unresolved shells stay review-gated
            if probe_key is not None:
                child_probe_failures.add(probe_key)
            summary.provider_detail_failures += 1
            if len(summary.failure_details) < 10:
                summary.failure_details.append({"provider": str(provider.slug), "query": query, "detail_error": str(exc)})
        verification = _verify_aggregate_shell_match(query, match, meta, title_family, children)
        if verification is not None:
            verification_identity = _provider_result_identity(verification.match)
            if verification_identity in verified_identities:
                continue
            verified_identities.add(verification_identity)
            verified.append(verification)
    return verified



def _candidate_mal_id(item: Recommendation) -> int | None:
    value = item.context.get("mal_anime_id")
    return value if isinstance(value, int) else None


@dataclass(slots=True)
class ProviderEnrichmentCandidate:
    item: Recommendation
    mal_id: int
    rank: int
    rank_key: dict[str, Any]
    selection_class: str
    cursor_wrapped: bool = False
    previous_attempted_at: str | None = None
    previous_attempt_count: int = 0
    due_evidence: tuple[Any, ...] = ()


_DUE_SELECTION_CLASSES = frozenset({
    "failed_retry_due",
    "expired_refresh_due",
    "stale_refresh_due",
    "logic_refresh_due",
})

_SELECTION_CLASS_PRIORITY = {
    "force_refresh": 0,
    "uncovered": 1,
    "mapping_refresh_due": 2,
    "failed_retry_due": 3,
    "expired_refresh_due": 4,
    "stale_refresh_due": 5,
    "logic_refresh_due": 6,
}


def _effective_evidence_refresh_horizon(
    config: AppConfig, *, candidate_population: int, candidates_per_run: int
) -> timedelta:
    """Keep evidence fresh long enough for this lane to traverse its ranked universe."""
    cadence_seconds = max(0, int(config.service.provider_eligibility_refresh_every_seconds))
    capacity = max(0, int(candidates_per_run))
    floor = timedelta(days=PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS)
    if cadence_seconds <= 0 or capacity <= 0 or candidate_population <= 0:
        return floor
    traversal_seconds = ceil(candidate_population / capacity) * cadence_seconds
    return max(floor, timedelta(seconds=ceil(traversal_seconds * PROVIDER_ENRICHMENT_TRAVERSAL_SAFETY_FACTOR)))


def _candidate_rank_key(item: Recommendation, *, mal_id: int, rank: int) -> dict[str, Any]:
    return {
        "rank": int(rank),
        "mal_anime_id": int(mal_id),
        "priority": int(getattr(item, "priority", 0) or 0),
        "kind": str(getattr(item, "kind", "") or ""),
    }


def _evidence_is_stale(evidence: Any) -> bool:
    return any(
        str(getattr(evidence, field_name, "")).strip().lower() == "stale"
        for field_name in ("review_status", "catalog_status", "english_dub_status")
    )


def _provider_selection_class(
    evidence_rows: list[Any],
    *,
    now: str,
    force_refresh: bool,
) -> tuple[str | None, str | None, tuple[Any, ...]]:
    if force_refresh:
        return "force_refresh", None, tuple(evidence_rows)
    if not evidence_rows:
        return "uncovered", None, ()
    failed_due: list[Any] = []
    failed_backoff: list[Any] = []
    stale_due: list[Any] = []
    expired_due: list[Any] = []
    logic_due: list[Any] = []
    fresh_current: list[Any] = []
    for evidence in evidence_rows:
        refresh_status = str(getattr(evidence, "refresh_status", "")).strip().lower()
        next_retry_at = getattr(evidence, "next_retry_at", None)
        if refresh_status == "failed":
            if next_retry_at is not None and str(next_retry_at) > now:
                failed_backoff.append(evidence)
                continue
            failed_due.append(evidence)
            continue
        refresh_due_at = getattr(evidence, "refresh_due_at", None)
        if refresh_due_at is not None and str(refresh_due_at) <= now:
            expired_due.append(evidence)
            continue
        if str(getattr(evidence, "logic_version", "")) not in {
            PROVIDER_ELIGIBILITY_LOGIC_VERSION,
            PROVIDER_DETAIL_CACHE_LOGIC_VERSION,
        }:
            logic_due.append(evidence)
            continue
        if _evidence_is_stale(evidence):
            stale_due.append(evidence)
            continue
        if (
            str(getattr(evidence, "verification_outcome", "")) != "positive"
            and getattr(evidence, "refresh_due_at", None) is None
            and str(getattr(evidence, "expires_at", "")) <= now
        ):
            expired_due.append(evidence)
            continue
        fresh_current.append(evidence)

    if failed_due:
        return "failed_retry_due", None, tuple(failed_due)
    if stale_due:
        return "stale_refresh_due", None, tuple(stale_due)
    if expired_due:
        return "expired_refresh_due", None, tuple(expired_due)
    if logic_due:
        return "logic_refresh_due", None, tuple(logic_due)
    if failed_backoff and not fresh_current:
        return None, "retry_backoff", ()
    return None, "fresh_covered", ()


def _append_summary_counter(target: dict[str, int], key: str) -> None:
    target[key] = target.get(key, 0) + 1


def _provider_candidate_title(item: Recommendation, meta: Any | None) -> str | None:
    title = getattr(meta, "title", None) if meta is not None else None
    if isinstance(title, str) and title.strip():
        return title.strip()
    item_title = getattr(item, "title", None)
    if isinstance(item_title, str) and item_title.strip():
        return item_title.strip()
    return None


def _select_provider_enrichment_candidates(
    config: AppConfig,
    *,
    provider_slug: str,
    candidates: list[Recommendation],
    metadata: dict[int, Any],
    mappings_by_series: dict[tuple[str, str], Any],
    candidate_limit: int,
    now: str,
    force_refresh: bool,
    summary: EnrichmentSummary,
) -> list[ProviderEnrichmentCandidate]:
    limit = max(0, int(candidate_limit))
    cursor = get_recommendation_provider_enrichment_cursor(config.db_path, provider=provider_slug)
    provider_state: dict[str, Any] = {
        "cursor_before_mal_anime_id": cursor.cursor_mal_anime_id if cursor is not None else None,
        "cursor_before_generation": cursor.cursor_generation if cursor is not None else 0,
        "candidate_count": len(candidates),
        "eligible_count": 0,
        "selected_mal_anime_ids": [],
        "selected_classes": [],
        "wrapped": False,
        "cursor_missing": False,
    }
    summary.provider_cursor_states[provider_slug] = provider_state
    if limit <= 0 or not candidates:
        provider_state["exhausted"] = True
        return []

    candidate_ids = [mal_id for item in candidates if (mal_id := _candidate_mal_id(item)) is not None]
    evidence_by_mal_id: dict[int, list[Any]] = {}
    if provider_slug in DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS and candidate_ids:
        for evidence in list_recommendation_provider_eligibility_evidence_for_mal_ids(
            config.db_path,
            candidate_ids,
            provider=provider_slug,
        ):
            evidence_by_mal_id.setdefault(evidence.mal_anime_id, []).append(evidence)
    attempts_by_mal_id = {
        attempt.mal_anime_id: attempt
        for attempt in list_recommendation_provider_enrichment_attempts(
            config.db_path,
            provider=provider_slug,
            mal_anime_ids=candidate_ids,
        )
    }

    ranked: list[ProviderEnrichmentCandidate] = []
    eligible_class_counts: dict[str, int] = {}
    cursor_rank: int | None = None
    cursor_mal_id = cursor.cursor_mal_anime_id if cursor is not None else None
    for rank, item in enumerate(candidates):
        mal_id = _candidate_mal_id(item)
        if mal_id is None:
            _append_summary_counter(summary.selection_skip_counts, "missing_mal_id")
            continue
        if cursor_mal_id is not None and mal_id == cursor_mal_id:
            cursor_rank = rank
        if metadata.get(mal_id) is None:
            _append_summary_counter(summary.selection_skip_counts, "missing_metadata")
            continue
        evidence_rows = evidence_by_mal_id.get(mal_id, [])
        selection_class, skip_reason, due_evidence = _provider_selection_class(
            evidence_rows,
            now=now,
            force_refresh=force_refresh,
        )
        if selection_class is None and skip_reason == "fresh_covered":
            for evidence in evidence_rows:
                mapping = mappings_by_series.get((provider_slug, str(evidence.provider_series_id)))
                if (
                    mapping is not None
                    and bool(getattr(mapping, "approved_by_user", False))
                    and int(getattr(mapping, "mal_anime_id", -1)) == int(mal_id)
                    and str(getattr(evidence, "identity_match_kind", "")) != "approved_mapping"
                    and _is_legacy_or_pending_provider_eligibility_record(evidence)
                ):
                    selection_class = "mapping_refresh_due"
                    skip_reason = None
                    due_evidence = (evidence,)
                    break
        if selection_class is None:
            _append_summary_counter(summary.selection_skip_counts, skip_reason or "not_due")
            if skip_reason == "fresh_covered":
                summary.eligibility_fresh_skips += 1
            elif skip_reason == "retry_backoff":
                summary.eligibility_retry_backoff_skips += 1
            continue
        _append_summary_counter(eligible_class_counts, selection_class)
        attempt = attempts_by_mal_id.get(mal_id)
        ranked.append(
            ProviderEnrichmentCandidate(
                item=item,
                mal_id=mal_id,
                rank=rank,
                rank_key=_candidate_rank_key(item, mal_id=mal_id, rank=rank),
                selection_class=selection_class,
                previous_attempted_at=attempt.attempted_at if attempt is not None else None,
                previous_attempt_count=attempt.attempt_count if attempt is not None else 0,
                due_evidence=due_evidence,
            )
        )

    provider_state["eligible_count"] = len(ranked)
    provider_state["eligible_class_counts"] = dict(sorted(eligible_class_counts.items()))
    if not ranked:
        provider_state["exhausted"] = True
        return []

    def traversal_order(rows: list[ProviderEnrichmentCandidate]) -> list[ProviderEnrichmentCandidate]:
        if cursor_rank is None:
            return sorted(
                rows,
                key=lambda candidate: (
                    1 if candidate.previous_attempted_at is not None else 0,
                    candidate.previous_attempted_at or "",
                    candidate.rank,
                    candidate.mal_id,
                ),
            )
        return [candidate for candidate in rows if candidate.rank > cursor_rank] + [
            candidate for candidate in rows if candidate.rank <= cursor_rank
        ]

    def due_order_key(candidate: ProviderEnrichmentCandidate) -> tuple[Any, ...]:
        timestamps = [
            str(getattr(evidence, "next_retry_at", None) or getattr(evidence, "refresh_due_at", None) or "")
            for evidence in candidate.due_evidence
        ]
        identities = sorted(
            str(getattr(evidence, "provider_series_id", "")) for evidence in candidate.due_evidence
        )
        return (
            _SELECTION_CLASS_PRIORITY.get(candidate.selection_class, 99),
            min(timestamps, default=""),
            candidate.mal_id,
            identities[0] if identities else "",
        )

    uncovered = traversal_order([candidate for candidate in ranked if candidate.selection_class == "uncovered"])
    due = sorted(
        [candidate for candidate in ranked if candidate.selection_class != "uncovered"],
        key=due_order_key,
    )
    # Strict uncovered-first: refresh work receives no reserved slot while
    # eligible never-covered candidates can fill capacity. Retry/safety
    # backoff remains enforced before candidates reach either list.
    selected = (uncovered + due)[:limit]
    if cursor_rank is None and cursor_mal_id is not None:
        provider_state["cursor_missing"] = True
    if cursor_rank is not None and any(candidate.rank <= cursor_rank for candidate in selected):
        provider_state["wrapped"] = True
        for candidate in selected:
            if candidate.rank <= cursor_rank:
                candidate.cursor_wrapped = True

    provider_state["selected_mal_anime_ids"] = [candidate.mal_id for candidate in selected]
    provider_state["selected_classes"] = [candidate.selection_class for candidate in selected]
    if selected:
        provider_state["cursor_after_mal_anime_id"] = selected[-1].mal_id
    return selected


_PROVIDER_ENRICHMENT_AUTHORIZED_CANDIDATES_PER_PROVIDER_HOUR = 2
_PROVIDER_ENRICHMENT_RECENT_SAMPLE_LIMIT = 5


def _diagnostic_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_nonnegative_count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _safe_metadata_ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(max(0, int(numerator)) / denominator, 6)


def _provider_enrichment_policy(config: AppConfig) -> dict[str, Any]:
    candidate_limit = config.service.execute_limit_for("recommend_provider_eligibility_candidates")
    if candidate_limit is None:
        candidate_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_provider_eligibility_candidates", 4)
    search_limit = config.service.execute_limit_for("recommend_provider_eligibility_search_results")
    if search_limit is None:
        search_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_provider_eligibility_search_results", 5)
    query_limit = config.service.execute_limit_for("recommend_provider_eligibility_queries_per_candidate")
    if query_limit is None:
        query_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_provider_eligibility_queries_per_candidate", 1)
    every_seconds = max(0, int(config.service.provider_eligibility_refresh_every_seconds))
    configured_per_hour: float | None = None
    if every_seconds > 0:
        configured_per_hour = round(max(0, int(candidate_limit)) * 3600 / every_seconds, 6)
    return {
        "authorized_candidates_per_provider_hour": _PROVIDER_ENRICHMENT_AUTHORIZED_CANDIDATES_PER_PROVIDER_HOUR,
        "configured_candidates_per_provider_run": max(0, int(candidate_limit)),
        "configured_every_seconds": every_seconds,
        "configured_candidates_per_provider_hour": configured_per_hour,
        "configured_candidates_per_provider_hour_source": "task_execute_limit_and_cadence",
        "search_results_per_query": max(1, int(search_limit)),
        "queries_per_candidate": max(1, int(query_limit)),
        "query_policy": "local MAL metadata English/preferred-title queries only; no provider query is issued by diagnostics",
        "search_policy": "runtime lane may issue bounded provider title searches; diagnostics only rank local due candidates",
        "read_only_selection": True,
        "network_io": False,
        "writes": False,
        "provider_search_cache_ttl_days": PROVIDER_SEARCH_CACHE_TTL_DAYS,
        "eligibility_evidence_ttl_days": PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS,
        "refresh_target_days": max(0, int(config.service.provider_eligibility_refresh_target_days)),
        "refresh_jitter_days": max(0, int(config.service.provider_eligibility_refresh_jitter_days)),
        "refresh_schedule_version": PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
        "refresh_window_days": {
            "minimum": max(0, int(config.service.provider_eligibility_refresh_target_days) - int(config.service.provider_eligibility_refresh_jitter_days)),
            "maximum": int(config.service.provider_eligibility_refresh_target_days) + int(config.service.provider_eligibility_refresh_jitter_days),
        },
    }


def _provider_cursor_diagnostic(cursor: Any) -> dict[str, Any]:
    if cursor is None:
        return {
            "present": False,
            "cursor_mal_anime_id": None,
            "cursor_rank_key": None,
            "cursor_generation": 0,
            "wrapped_at": None,
            "last_attempted_mal_anime_id": None,
            "last_attempted_rank_key": None,
            "last_attempted_at": None,
            "last_selection_class": None,
            "last_outcome": None,
            "updated_at": None,
        }
    return {
        "present": True,
        "cursor_mal_anime_id": cursor.cursor_mal_anime_id,
        "cursor_rank_key": cursor.cursor_rank_key,
        "cursor_generation": cursor.cursor_generation,
        "wrapped_at": cursor.wrapped_at,
        "last_attempted_mal_anime_id": cursor.last_attempted_mal_anime_id,
        "last_attempted_rank_key": cursor.last_attempted_rank_key,
        "last_attempted_at": cursor.last_attempted_at,
        "last_selection_class": cursor.last_selection_class,
        "last_outcome": cursor.last_outcome,
        "updated_at": cursor.updated_at,
    }


def _provider_attempt_diagnostics(provider: str, attempts: list[Any], *, now: datetime) -> dict[str, Any]:
    one_hour_cutoff = now - timedelta(hours=1)
    day_cutoff = now - timedelta(hours=24)
    parsed_attempts: list[tuple[Any, datetime | None]] = [(attempt, _diagnostic_timestamp(attempt.attempted_at)) for attempt in attempts]
    last_hour = [attempt for attempt, parsed in parsed_attempts if parsed is not None and parsed >= one_hour_cutoff]
    last_24h = [(attempt, parsed) for attempt, parsed in parsed_attempts if parsed is not None and parsed >= day_cutoff]
    outcome_counts = Counter(str(attempt.last_outcome or "unknown") for attempt, _parsed in last_24h)
    latest_samples: list[dict[str, Any]] = []
    for attempt, _parsed in sorted(
        last_24h,
        key=lambda item: (item[1] or datetime.min.replace(tzinfo=timezone.utc), int(item[0].mal_anime_id)),
        reverse=True,
    )[:_PROVIDER_ENRICHMENT_RECENT_SAMPLE_LIMIT]:
        latest_samples.append(
            {
                "provider": provider,
                "mal_anime_id": int(attempt.mal_anime_id),
                "selection_class": attempt.selection_class,
                "attempted_at": attempt.attempted_at,
                "attempt_count_for_candidate": int(attempt.attempt_count),
                "last_outcome": attempt.last_outcome,
                "rank_key": attempt.rank_key,
            }
        )
    return {
        "distinct_candidates_attempted_last_hour": len(last_hour),
        "throughput_label": "distinct candidates from overwrite-per-candidate attempt table, not event count",
        "oldest_attempted_at_last_hour": min((attempt.attempted_at for attempt in last_hour), default=None),
        "recent_24h_outcome_counts": dict(sorted(outcome_counts.items())),
        "recent_24h_latest_samples": latest_samples,
    }


def _provider_selection_eta(
    *,
    due_count: int,
    selected_count: int,
    attempts_last_hour: int,
    oldest_attempted_at_last_hour: str | None,
    policy: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    candidate_limit = _safe_nonnegative_count(policy.get("configured_candidates_per_provider_run"))
    configured_hour_raw = policy.get("configured_candidates_per_provider_hour")
    configured_hour = int(configured_hour_raw) if isinstance(configured_hour_raw, (int, float)) else 0
    authorized = _safe_nonnegative_count(policy.get("authorized_candidates_per_provider_hour"))
    effective_hourly_capacity = max(0, min(authorized, configured_hour) if configured_hour else authorized)
    if due_count <= 0:
        return {"eta_seconds": 0, "reason_code": "no_due_candidates"}
    if candidate_limit <= 0 or effective_hourly_capacity <= 0:
        return {"eta_seconds": None, "reason_code": "provider_enrichment_disabled"}
    if selected_count <= 0:
        return {"eta_seconds": None, "reason_code": "no_current_limit_selection"}
    if attempts_last_hour < effective_hourly_capacity:
        return {"eta_seconds": 0, "reason_code": "hourly_candidate_capacity_available"}
    oldest = _diagnostic_timestamp(oldest_attempted_at_last_hour)
    if oldest is None:
        return {"eta_seconds": None, "reason_code": "hourly_candidate_window_reset_unknown"}
    reset_at = oldest + timedelta(hours=1)
    return {
        "eta_seconds": max(0, int((reset_at - now).total_seconds())),
        "reason_code": "hourly_candidate_capacity_saturated",
        "basis": "oldest distinct candidate attempt in the last hour",
    }


def unknown_provider_enrichment_diagnostics(
    *,
    reason: str,
    provider_slugs: list[str] | tuple[str, ...] | None = None,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_policy = dict(policy or {})
    providers = {
        str(provider): {
            "status": "unknown",
            "reason_codes": [str(reason or "unknown")],
            "policy": resolved_policy,
            "candidates": {
                "ranked_total": None,
                "with_mal_anime_id": None,
                "metadata_available": None,
                "metadata_missing": None,
                "metadata_coverage_ratio": None,
            },
            "due": {
                "total": None,
                "by_class": {},
                "current_limit_selection_count": 0,
                "current_limit_selection_by_class": {},
                "current_limit_selected_mal_anime_ids": [],
                "selection_eta": {"eta_seconds": None, "reason_code": str(reason or "unknown")},
            },
            "cursor": _provider_cursor_diagnostic(None),
            "attempts": {
                "distinct_candidates_attempted_last_hour": 0,
                "throughput_label": "distinct candidates from overwrite-per-candidate attempt table, not event count",
                "oldest_attempted_at_last_hour": None,
                "recent_24h_outcome_counts": {},
                "recent_24h_latest_samples": [],
            },
            "availability_completion_eta": {"eta_seconds": None, "reason_code": str(reason or "unknown")},
        }
        for provider in (provider_slugs or [])
    }
    return {
        "status": "unknown",
        "reason_codes": [str(reason or "unknown")],
        "policy": resolved_policy,
        "providers": providers,
    }


def build_provider_enrichment_diagnostics(
    config: AppConfig,
    *,
    provider_slugs: list[str] | tuple[str, ...],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return read-only provider enrichment lane diagnostics.

    This deliberately reuses the in-process ranking/selection logic over local
    SQLite state only. It does not instantiate provider clients, call provider
    networks, update cursors, write review rows, or refresh caches.
    """
    policy = _provider_enrichment_policy(config)
    providers = sorted({str(provider).strip().lower() for provider in provider_slugs if str(provider).strip()})
    if not providers:
        return unknown_provider_enrichment_diagnostics(reason="no_configured_credentialed_providers", policy=policy)

    current = now or _utc_now()
    current_iso = _iso(current)
    candidate_limit = _safe_nonnegative_count(policy.get("configured_candidates_per_provider_run"))
    try:
        metadata = get_mal_anime_metadata_map(config.db_path)
        mappings_by_series = {
            (mapping.provider, mapping.provider_series_id): mapping
            for mapping in list_series_mappings(config.db_path, approved_only=False)
        }
        ranked_candidates = [
            item
            for item in build_recommendations(
                config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
                read_only=True,
            )
            if item.kind == "discovery_candidate"
        ]
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "no such table" in message or "no such column" in message:
            return unknown_provider_enrichment_diagnostics(
                reason="provider_enrichment_schema_unavailable",
                provider_slugs=providers,
                policy=policy,
            )
        raise

    candidate_ids = [_candidate_mal_id(item) for item in ranked_candidates]
    candidates_with_mal_id = [candidate_id for candidate_id in candidate_ids if candidate_id is not None]
    metadata_available = sum(1 for candidate_id in candidates_with_mal_id if candidate_id in metadata)
    candidate_totals = {
        "ranked_total": len(ranked_candidates),
        "with_mal_anime_id": len(candidates_with_mal_id),
        "metadata_available": metadata_available,
        "metadata_missing": max(0, len(candidates_with_mal_id) - metadata_available),
        "metadata_coverage_ratio": _safe_metadata_ratio(metadata_available, len(candidates_with_mal_id)),
        "read_only_ranking": True,
    }

    provider_payloads: dict[str, Any] = {}
    overall_reasons: set[str] = set()
    overall_status = "ok"
    for provider_slug in providers:
        try:
            all_due_summary = EnrichmentSummary()
            all_due = _select_provider_enrichment_candidates(
                config,
                provider_slug=provider_slug,
                candidates=ranked_candidates,
                metadata=metadata,
                mappings_by_series=mappings_by_series,
                candidate_limit=max(candidate_limit, len(ranked_candidates)),
                now=current_iso,
                force_refresh=False,
                summary=all_due_summary,
            )
            current_summary = EnrichmentSummary()
            selected = _select_provider_enrichment_candidates(
                config,
                provider_slug=provider_slug,
                candidates=ranked_candidates,
                metadata=metadata,
                mappings_by_series=mappings_by_series,
                candidate_limit=candidate_limit,
                now=current_iso,
                force_refresh=False,
                summary=current_summary,
            )
            attempts = list_recommendation_provider_enrichment_attempts(config.db_path, provider=provider_slug)
            cursor = get_recommendation_provider_enrichment_cursor(config.db_path, provider=provider_slug)
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "no such table" in message or "no such column" in message:
                provider_payloads[provider_slug] = unknown_provider_enrichment_diagnostics(
                    reason="provider_enrichment_schema_unavailable",
                    provider_slugs=[provider_slug],
                    policy=policy,
                )["providers"][provider_slug]
                overall_reasons.add("provider_enrichment_schema_unavailable")
                overall_status = "unknown"
                continue
            raise

        all_state = all_due_summary.provider_cursor_states.get(provider_slug, {})
        current_state = current_summary.provider_cursor_states.get(provider_slug, {})
        due_by_class = Counter(candidate.selection_class for candidate in all_due)
        selected_by_class = Counter(candidate.selection_class for candidate in selected)
        attempt_payload = _provider_attempt_diagnostics(provider_slug, attempts, now=current)
        selection_eta = _provider_selection_eta(
            due_count=len(all_due),
            selected_count=len(selected),
            attempts_last_hour=attempt_payload["distinct_candidates_attempted_last_hour"],
            oldest_attempted_at_last_hour=attempt_payload["oldest_attempted_at_last_hour"],
            policy=policy,
            now=current,
        )
        refresh_horizon = timedelta(days=max(0, int(config.service.provider_eligibility_refresh_target_days)))
        traversal_runs = ceil(len(ranked_candidates) / candidate_limit) if candidate_limit > 0 else None
        traversal_seconds = (
            traversal_runs * int(policy.get("configured_every_seconds") or 0)
            if traversal_runs is not None
            else None
        )

        reason_codes: list[str] = []
        configured_hour = policy.get("configured_candidates_per_provider_hour")
        if policy.get("configured_every_seconds") == 0 or candidate_limit <= 0:
            status = "disabled"
            reason_codes.append("provider_enrichment_disabled")
        elif len(ranked_candidates) == 0:
            status = "unknown"
            reason_codes.append("no_current_discovery_candidates")
        elif len(all_due) > 0:
            status = "backlog"
            reason_codes.append("due_candidates_present")
        else:
            status = "ok"
            reason_codes.append("no_due_candidates_current_rank")
        if isinstance(configured_hour, (int, float)) and configured_hour > _PROVIDER_ENRICHMENT_AUTHORIZED_CANDIDATES_PER_PROVIDER_HOUR:
            reason_codes.append("configured_candidate_rate_exceeds_authorized")
            if status == "ok":
                status = "degraded"
        if attempt_payload["distinct_candidates_attempted_last_hour"] > _PROVIDER_ENRICHMENT_AUTHORIZED_CANDIDATES_PER_PROVIDER_HOUR:
            reason_codes.append("recent_candidate_rate_exceeds_authorized")
            status = "degraded"
        if attempt_payload["recent_24h_outcome_counts"].get("provider_search_failure"):
            reason_codes.append("recent_provider_search_failures")
            if status == "ok":
                status = "degraded"
        for reason in reason_codes:
            overall_reasons.add(reason)
        if status in {"unknown", "degraded"}:
            overall_status = status
        elif status in {"backlog", "disabled"} and overall_status == "ok":
            overall_status = status

        provider_payloads[provider_slug] = {
            "status": status,
            "reason_codes": reason_codes,
            "policy": policy,
            "candidates": dict(candidate_totals),
            "due": {
                "total": len(all_due),
                "by_class": dict(sorted(due_by_class.items())),
                "current_limit_selection_count": len(selected),
                "current_limit_selection_by_class": dict(sorted(selected_by_class.items())),
                "current_limit_selected_mal_anime_ids": [candidate.mal_id for candidate in selected[:_PROVIDER_ENRICHMENT_RECENT_SAMPLE_LIMIT]],
                "selection_eta": selection_eta,
                "selection_skip_counts": dict(sorted(current_summary.selection_skip_counts.items())),
                "fresh_current_skip_count": int(current_summary.eligibility_fresh_skips),
                "retry_backoff_skip_count": int(current_summary.eligibility_retry_backoff_skips),
                "cursor_wrapped": bool(current_state.get("wrapped")),
                "cursor_missing": bool(current_state.get("cursor_missing")),
                "eligible_count_check": _safe_nonnegative_count(all_state.get("eligible_count")),
                "eligible_class_counts_check": dict(all_state.get("eligible_class_counts") or {}),
                "initial_coverage_backlog": int(due_by_class.get("uncovered", 0)),
                "refresh_backlog": sum(int(due_by_class.get(name, 0)) for name in _DUE_SELECTION_CLASSES),
                "retry_backoff": int(current_summary.selection_skip_counts.get("retry_backoff", 0)),
            },
            "cursor": _provider_cursor_diagnostic(cursor),
            "attempts": attempt_payload,
            "sustainability": {
                "authorized_candidates_per_provider_hour": _PROVIDER_ENRICHMENT_AUTHORIZED_CANDIDATES_PER_PROVIDER_HOUR,
                "configured_candidates_per_provider_hour": configured_hour,
                "distinct_candidates_attempted_last_hour": attempt_payload["distinct_candidates_attempted_last_hour"],
                "within_authorized_rate": attempt_payload["distinct_candidates_attempted_last_hour"] <= _PROVIDER_ENRICHMENT_AUTHORIZED_CANDIDATES_PER_PROVIDER_HOUR,
                "configured_exceeds_authorized": None if not isinstance(configured_hour, (int, float)) else configured_hour > _PROVIDER_ENRICHMENT_AUTHORIZED_CANDIDATES_PER_PROVIDER_HOUR,
                "candidate_population": len(ranked_candidates),
                "traversal_runs": traversal_runs,
                "traversal_seconds": traversal_seconds,
                "effective_refresh_horizon_seconds": int(refresh_horizon.total_seconds()),
                "refresh_horizon_floor_days": PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS,
                "recirculation_risk": bool(
                    traversal_seconds is not None and traversal_seconds >= refresh_horizon.total_seconds()
                ),
            },
            "availability_completion_eta": {
                "eta_seconds": None,
                "reason_code": "provider_availability_completion_depends_on_provider_search_results_review_and_future_candidates"
                if ranked_candidates
                else "no_current_discovery_candidates",
            },
            "lifecycle": get_recommendation_provider_eligibility_lifecycle_counts(
                config.db_path, provider=provider_slug, now=current_iso
            ),
        }

    return {
        "status": overall_status,
        "reason_codes": sorted(overall_reasons) or ["no_due_candidates_current_rank"],
        "policy": policy,
        "providers": provider_payloads,
    }


def _ensure_provider_series(config: AppConfig, *, provider: str, match: dict[str, Any]) -> None:
    provider_series_id = str(match["provider_series_id"])
    title = str(match.get("title") or provider_series_id)
    season_number = _int_value(_raw_lookup(match, "season_number"))
    with connect(config.db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_series (
                provider,
                provider_series_id,
                title,
                season_title,
                season_number,
                raw_json,
                catalog_observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(provider, provider_series_id) DO UPDATE SET
                title = CASE
                    WHEN provider_series.account_observed_at IS NULL THEN excluded.title
                    ELSE provider_series.title
                END,
                season_title = CASE
                    WHEN provider_series.account_observed_at IS NULL THEN excluded.season_title
                    ELSE provider_series.season_title
                END,
                season_number = CASE
                    WHEN provider_series.account_observed_at IS NULL THEN excluded.season_number
                    ELSE provider_series.season_number
                END,
                raw_json = CASE
                    WHEN provider_series.account_observed_at IS NULL THEN excluded.raw_json
                    ELSE provider_series.raw_json
                END,
                last_seen_at = CASE
                    WHEN provider_series.account_observed_at IS NULL THEN CURRENT_TIMESTAMP
                    ELSE provider_series.last_seen_at
                END,
                catalog_observed_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                provider_series_id,
                title,
                match.get("season_title"),
                season_number,
                json.dumps(match, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()


def enrich_discovery_provider_availability(
    config: AppConfig,
    *,
    providers: list[ProviderTitleSearchClient],
    candidate_limit: int = 2,
    search_limit: int = 5,
    queries_per_candidate: int = 1,
    now: datetime | None = None,
    persist_review_queue: bool = True,
    force_refresh: bool = False,
) -> EnrichmentSummary:
    current = now or _utc_now()
    fetched_at = _iso(current)
    expires_at = _iso(current + timedelta(days=PROVIDER_SEARCH_CACHE_TTL_DAYS))
    summary = EnrichmentSummary()
    metadata = get_mal_anime_metadata_map(config.db_path)
    mappings_by_series = {
        (mapping.provider, mapping.provider_series_id): mapping
        for mapping in list_series_mappings(config.db_path, approved_only=False)
    }
    _reconcile_legacy_provider_eligibility_evidence(
        config,
        summary,
        metadata=metadata,
        mappings_by_series=mappings_by_series,
    )
    candidates = [
        r
        for r in build_recommendations(
            config,
            limit=0,
            require_provider_availability=False,
            include_discovery_candidates_without_actionable_provider_evidence=True,
        )
        if r.kind == "discovery_candidate"
    ]
    # Legacy expires_at remains a short diagnostic/cache timestamp. Lifecycle
    # scheduling is independently persisted in refresh_due_at and is never
    # derived from population size, rank, batch size, or traversal duration.
    eligibility_expires_at = _iso(current + timedelta(days=PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS))
    summary.refresh_policy = {
        "target_days": max(0, int(config.service.provider_eligibility_refresh_target_days)),
        "jitter_days": max(0, int(config.service.provider_eligibility_refresh_jitter_days)),
        "window_days": [
            max(0, int(config.service.provider_eligibility_refresh_target_days) - int(config.service.provider_eligibility_refresh_jitter_days)),
            int(config.service.provider_eligibility_refresh_target_days) + int(config.service.provider_eligibility_refresh_jitter_days),
        ],
        "schedule_version": PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
        "capacity_per_provider_run": max(0, int(candidate_limit)),
    }
    acquired_leases: list[ProviderEligibilityProcessLease] = []
    available_providers: list[ProviderTitleSearchClient] = []
    for provider_client in providers:
        provider_slug = str(getattr(provider_client, "slug", provider_client.__class__.__name__)).strip().lower()
        lease = ProviderEligibilityProcessLease(config.service_leases_dir, provider_slug)
        if not lease.try_acquire():
            summary.lease_busy += 1
            summary.provider_cursor_states[provider_slug] = {"status": "skipped", "reason": "lease_busy"}
            continue
        acquired_leases.append(lease)
        available_providers.append(provider_client)
    providers = available_providers
    review_entries: list[dict[str, Any]] = []
    child_probe_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    child_probe_failures: set[tuple[str, str]] = set()
    provider_sessions: dict[str, Any] = {}

    def release_leases() -> None:
        while acquired_leases:
            acquired_leases.pop().release()

    def request_session(provider: ProviderTitleSearchClient) -> Any | None:
        slug = str(getattr(provider, "slug", ""))
        if slug in provider_sessions:
            return provider_sessions[slug]
        factory = getattr(provider, "create_request_session", None)
        session = factory(config) if callable(factory) else None
        provider_sessions[slug] = session
        return session

    def process_selection(provider: ProviderTitleSearchClient, selection: ProviderEnrichmentCandidate) -> None:
        provider_slug = str(getattr(provider, "slug", provider.__class__.__name__)).strip().lower()
        meta = metadata.get(selection.mal_id)
        candidate_title = _provider_candidate_title(selection.item, meta)
        cursor = record_recommendation_provider_enrichment_attempt(
            config.db_path,
            provider=provider_slug,
            mal_anime_id=selection.mal_id,
            rank_key=selection.rank_key,
            selection_class=selection.selection_class,
            attempted_at=fetched_at,
            wrapped=selection.cursor_wrapped,
            outcome="selected",
        )
        provider_state = summary.provider_cursor_states.setdefault(provider_slug, {})
        provider_state["cursor_after_generation"] = cursor.cursor_generation
        provider_state["cursor_after_mal_anime_id"] = selection.mal_id
        selected_payload: dict[str, Any] = {
            "provider": provider_slug,
            "mal_anime_id": selection.mal_id,
            "candidate_title": candidate_title,
            "rank": selection.rank,
            "rank_key": selection.rank_key,
            "selection_class": selection.selection_class,
            "cursor_wrapped": selection.cursor_wrapped,
            "previous_attempted_at": selection.previous_attempted_at,
            "previous_attempt_count": selection.previous_attempt_count,
        }
        summary.selected_candidates.append(selected_payload)
        summary.candidates_considered += 1
        _append_summary_counter(summary.selection_class_counts, selection.selection_class)
        if selection.selection_class in _DUE_SELECTION_CLASSES:
            summary.eligibility_expired_retries += 1
        outcome = "selected"
        due_failure_recorded = False

        def record_due_failures_once() -> None:
            nonlocal due_failure_recorded
            if due_failure_recorded:
                return
            due_failure_recorded = True
            summary.eligibility_preserved_positive += _record_selection_refresh_failures(
                config, selection, now=current
            )

        try:
            if meta is None:
                outcome = "missing_metadata"
                return
            title_family = build_target_title_family(meta)
            queries = select_english_provider_search_queries(meta)
            if queries_per_candidate > 0:
                queries = queries[:queries_per_candidate]
            if not queries:
                outcome = "no_queries"
                return
            summary.queries_selected += len(queries)
            for query in queries:
                normalized_query = normalize_title(query)
                identity_key = f"mal:{selection.mal_id}"
                bypass_cache = force_refresh or selection.selection_class in _DUE_SELECTION_CLASSES
                cached = None if bypass_cache else get_provider_title_search_cache(
                    config.db_path, provider=provider_slug, normalized_query=normalized_query, now=fetched_at,
                    logic_version=PROVIDER_SEARCH_CACHE_LOGIC_VERSION, search_limit=search_limit,
                    identity_key=identity_key,
                )
                searched_provider = False
                if cached is not None:
                    summary.cache_hits += 1
                    matches = _dedupe_provider_matches(cached.matches)
                else:
                    summary.cache_misses += 1
                    try:
                        session = request_session(provider)
                        raw_matches = provider.search_title(config, query, limit=search_limit, session=session) if session is not None else provider.search_title(config, query, limit=search_limit)
                    except Exception as exc:  # provider/auth/network errors must not stale-out good evidence
                        if selection.selection_class in _DUE_SELECTION_CLASSES:
                            record_due_failures_once()
                        summary.provider_search_failures += 1
                        outcome = "provider_search_failure"
                        if len(summary.failure_details) < 10:
                            summary.failure_details.append({"provider": provider_slug, "query": query, "error": str(exc)})
                        continue
                    searched_provider = True
                    summary.provider_searches += 1
                    matches = _dedupe_provider_matches([_match_to_dict(match) for match in raw_matches])
                    upsert_provider_title_search_cache(
                        config.db_path,
                        provider=provider_slug,
                        normalized_query=normalized_query,
                        query=query,
                        candidate_mal_anime_id=selection.mal_id,
                        candidate_title=meta.title,
                        matches=matches,
                        status="ok",
                        fetched_at=fetched_at,
                        expires_at=expires_at,
                        logic_version=PROVIDER_SEARCH_CACHE_LOGIC_VERSION,
                        search_limit=search_limit,
                        identity_key=identity_key,
                    )
                decision = classify_provider_matches(query, matches, title_family)
                if decision.kind == "strong" and decision.selected:
                    outcome = "strong_match"
                    match = decision.selected[0]
                    provider_series_id = match.get("provider_series_id")
                    if provider_series_id:
                        existing_evidence = None
                        if provider_slug in DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS:
                            existing_evidence = get_recommendation_provider_eligibility_evidence(
                                config.db_path, mal_anime_id=int(selection.mal_id), provider=provider_slug,
                                provider_series_id=str(provider_series_id),
                            )
                        if (not force_refresh and cached is not None and existing_evidence is not None
                                and existing_evidence.fetched_at != fetched_at
                                and existing_evidence.refresh_status == "ok"
                                and existing_evidence.logic_version == PROVIDER_ELIGIBILITY_LOGIC_VERSION
                                and is_strict_provider_eligibility_actionable(existing_evidence, now=fetched_at)
                                and not bool(mappings_by_series.get((provider_slug, str(provider_series_id))))):
                            summary.eligibility_fresh_skips += 1
                            outcome = "fresh_eligibility_skip"
                            continue
                        if cached is not None and existing_evidence is not None and existing_evidence.expires_at <= fetched_at:
                            # Search-cache identity is reusable for 365d, but availability is not.
                            # Re-query this bounded title before issuing a new current eligibility claim.
                            if (
                                not force_refresh
                                and existing_evidence.refresh_status == "failed"
                                and existing_evidence.logic_version == PROVIDER_ELIGIBILITY_LOGIC_VERSION
                                and existing_evidence.next_retry_at is not None
                                and existing_evidence.next_retry_at > fetched_at
                            ):
                                summary.eligibility_retry_backoff_skips += 1
                                outcome = "retry_backoff_skip"
                                continue
                            summary.eligibility_expired_retries += 1
                            try:
                                session = request_session(provider)
                                raw_matches = provider.search_title(config, query, limit=search_limit, session=session) if session is not None else provider.search_title(config, query, limit=search_limit)
                            except Exception as exc:
                                _record_eligibility_refresh_failure(config, existing_evidence, now=current)
                                summary.provider_search_failures += 1
                                outcome = "provider_search_failure"
                                if len(summary.failure_details) < 10:
                                    summary.failure_details.append({"provider": provider_slug, "query": query, "error": str(exc)})
                                continue
                            searched_provider = True
                            summary.provider_searches += 1
                            matches = _dedupe_provider_matches([_match_to_dict(value) for value in raw_matches])
                            decision = classify_provider_matches(query, matches, title_family)
                            if decision.kind != "strong" or not decision.selected:
                                outcome = "searched_no_current_strong_match"
                                continue
                            match = decision.selected[0]
                            provider_series_id = match.get("provider_series_id")
                            if not provider_series_id:
                                outcome = "strong_match_missing_provider_series_id"
                                continue
                        try:
                            match, detail_attempted = _fetch_provider_detail_if_available(
                                provider, config, match, now=current, force_refresh=force_refresh,
                                provider_session=provider_sessions.get(str(getattr(provider, "slug", ""))),
                                provider_session_factory=(lambda provider=provider: request_session(provider))
                                if _provider_detail_needed(provider, match) else None,
                            )
                            if detail_attempted:
                                summary.provider_detail_probes += 1
                        except Exception as exc:  # incomplete detail refresh must not overwrite stronger evidence
                            summary.provider_detail_failures += 1
                            if len(summary.failure_details) < 10:
                                summary.failure_details.append({"provider": provider_slug, "query": query, "detail_error": str(exc)})
                            if existing_evidence is not None and existing_evidence.last_successful_positive_at and not existing_evidence.invalidated_at:
                                _record_eligibility_refresh_failure(config, existing_evidence, now=current)
                                summary.eligibility_preserved_positive += 1
                                outcome = "provider_detail_failure_preserved_positive"
                                continue
                        provider_series_id = match.get("provider_series_id") or provider_series_id
                        _ensure_provider_series(config, provider=provider_slug, match=match)
                        summary.strong_matches += 1
                        _upsert_exact_identity_or_append_review(
                            config,
                            summary,
                            review_entries,
                            provider=provider_slug,
                            provider_series_id=provider_series_id,
                            mal_id=selection.mal_id,
                            candidate_title=meta.title,
                            query=query,
                            match=match,
                            mapping=mappings_by_series.get((provider_slug, str(provider_series_id))),
                            decision=decision,
                            title_family=title_family,
                            fetched_at=fetched_at,
                            expires_at=eligibility_expires_at,
                        )
                elif decision.kind == "ambiguous":
                    outcome = "ambiguous_match"
                    verified_shells = _verified_aggregate_shell_candidates(
                        config,
                        summary,
                        provider=provider,
                        query=query,
                        decision=decision,
                        meta=meta,
                        title_family=title_family,
                        child_probe_cache=child_probe_cache,
                        child_probe_failures=child_probe_failures,
                        provider_session=request_session(provider),
                    )
                    if len(verified_shells) == 1:
                        verification = verified_shells[0]
                        provider_series_id = str(verification.match.get("provider_series_id") or "")
                        if provider_series_id and _upsert_aggregate_shell_identity(
                            config,
                            summary,
                            provider=provider_slug,
                            mal_id=selection.mal_id,
                            candidate_title=meta.title,
                            query=query,
                            verification=verification,
                            mapping=mappings_by_series.get((provider_slug, provider_series_id)),
                            fetched_at=fetched_at,
                            expires_at=eligibility_expires_at,
                        ):
                            continue
                    summary.ambiguous_matches += 1
                    review_entries.append({
                        "provider": provider_slug,
                        "provider_series_id": None,
                        "severity": "warning",
                        "payload": {
                            "mal_anime_id": selection.mal_id,
                            "candidate_title": meta.title,
                            "query": query,
                            "matches": decision.selected,
                            "decision": "ambiguous_no_auto_link",
                            "provider_search_match_reasons": list(decision.reasons),
                        },
                    })
                elif outcome not in {"provider_search_failure", "strong_match", "ambiguous_match"}:
                    outcome = "searched_no_match" if searched_provider else "cache_hit_no_match"
                    if provider_slug in DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS and searched_provider:
                        # A successful bounded search with no acceptable identity is
                        # useful negative coverage, not an auto-link. Persist it so
                        # cursor wraps do not immediately recirculate this candidate.
                        negative_due_at = provider_eligibility_refresh_due_at(
                            successful_verified_at=fetched_at,
                            mal_anime_id=selection.mal_id,
                            provider=provider_slug,
                            provider_series_id=PROVIDER_NO_MATCH_SERIES_ID,
                            target_days=config.service.provider_eligibility_refresh_target_days,
                            jitter_days=config.service.provider_eligibility_refresh_jitter_days,
                        )
                        contradicted = record_recommendation_provider_eligibility_negative_scope(
                            config.db_path,
                            mal_anime_id=selection.mal_id,
                            provider=provider_slug,
                            provider_series_id=PROVIDER_NO_MATCH_SERIES_ID,
                            attempted_at=fetched_at,
                            expires_at=eligibility_expires_at,
                            refresh_due_at=negative_due_at,
                            refresh_schedule_version=PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
                            refresh_schedule_key=provider_eligibility_refresh_schedule_key(
                                mal_anime_id=selection.mal_id,
                                provider=provider_slug,
                                provider_series_id=PROVIDER_NO_MATCH_SERIES_ID,
                            ),
                            invalidation_reason="successful_affirmative_no_match",
                            source_evidence={
                                "source": "provider_title_search",
                                "result": "no_acceptable_match",
                                "query": query,
                                "search_limit": search_limit,
                            },
                            logic_version=PROVIDER_ELIGIBILITY_LOGIC_VERSION,
                        )
                        summary.eligibility_evidence_upserted += 1
                        summary.eligibility_contradicted += contradicted
        except Exception:
            record_due_failures_once()
            raise
        finally:
            update_recommendation_provider_enrichment_attempt_outcome(
                config.db_path,
                provider=provider_slug,
                mal_anime_id=selection.mal_id,
                outcome=outcome,
            )
            selected_payload["outcome"] = outcome

    try:
        for provider in providers:
            provider_slug = str(getattr(provider, "slug", provider.__class__.__name__)).strip().lower()
            if not callable(getattr(provider, "search_title", None)):
                summary.providers_skipped.append(provider_slug)
                continue
            selected = _select_provider_enrichment_candidates(
                config,
                provider_slug=provider_slug,
                candidates=candidates,
                metadata=metadata,
                mappings_by_series=mappings_by_series,
                candidate_limit=candidate_limit,
                now=fetched_at,
                force_refresh=force_refresh,
                summary=summary,
            )
            for selection in selected:
                process_selection(provider, selection)
            if provider_slug in DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS:
                lifecycle_counts = get_recommendation_provider_eligibility_lifecycle_counts(
                    config.db_path, provider=provider_slug, now=fetched_at
                )
                summary.eligibility_due += lifecycle_counts["due"]
                summary.eligibility_overdue += lifecycle_counts["overdue"]
                summary.eligibility_failed += lifecycle_counts["failed"]
                summary.eligibility_backoff += lifecycle_counts["backoff"]
                summary.eligibility_preserved_positive += lifecycle_counts["preserved_positive"]
                summary.eligibility_invalidated += lifecycle_counts["invalidated"]
    finally:
        release_leases()
    review_entries = _dedupe_discovery_review_entries(_coalesce_discovery_review_entries(review_entries))
    should_refresh_review_queue = bool(
        review_entries
        or summary.exact_verified_identities_no_review
        or summary.franchise_shell_verified_identities_no_review
    )
    if persist_review_queue and should_refresh_review_queue:
        result = replace_review_queue_entries(config.db_path, issue_type=DISCOVERY_PROVIDER_SEARCH_REVIEW_ISSUE, entries=review_entries)
        summary.review_entries_written = result["inserted"]
        summary.review_entries_resolved = result["resolved"]
    elif review_entries:
        summary.dry_run_review_entries = len(review_entries)
    return summary
