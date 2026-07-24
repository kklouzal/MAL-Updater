from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from .config import AppConfig
from .db import (
    connect,
    get_mal_anime_metadata_map,
    get_provider_title_search_cache,
    get_recommendation_provider_eligibility_evidence,
    list_series_mappings,
    replace_review_queue_entries,
    upsert_provider_title_search_cache,
    upsert_recommendation_provider_eligibility_evidence,
)
from .mapping import normalize_title
from .recommendations import Recommendation, build_recommendations

PROVIDER_SEARCH_CACHE_TTL_DAYS = 365
PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS = 7
DISCOVERY_PROVIDER_SEARCH_REVIEW_ISSUE = "discovery_provider_search_match_review"
DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS = frozenset({"crunchyroll", "hidive"})
PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND = "provider_title_search_exact"
PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND = "provider_franchise_shell_child_match"
VERIFIED_PROVIDER_SEARCH_IDENTITY_KINDS = frozenset({
    PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND,
    PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND,
})
PROVIDER_SEARCH_IDENTITY_CONFIDENCE = {
    PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND: 0.9,
    PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND: 0.88,
}


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
) -> tuple[list[dict[str, Any]], bool]:
    cached_children = _provider_child_items_from_match(match)
    if cached_children:
        return cached_children, False
    children_func = getattr(provider, "fetch_search_result_children", None)
    if not callable(children_func):
        return [], False
    fetched = children_func(config, match)
    return _coerce_provider_child_items(fetched), True


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
    locales = {str(v).strip().lower().replace("_", "-") for v in match.get("audio_locales") or [] if v is not None}
    return bool(locales & {"en", "en-us", "en-gb"})


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
) -> tuple[dict[str, Any], bool]:
    detail_func = getattr(provider, "fetch_search_result_detail", None)
    if not callable(detail_func) or not _provider_detail_needed(provider, match):
        return match, False
    enriched = detail_func(config, match)
    if enriched is None:
        return match, True
    return _match_to_dict(enriched), True


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
    verified_actionable = verified_identity and catalog_status == "present" and english_dub_status == "present"
    audio_locales = match.get("audio_locales") if isinstance(match.get("audio_locales"), list) else []
    last_verified_at = fetched_at if verified_actionable else None

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
        verified_actionable = verified_identity and catalog_status == "present" and english_dub_status == "present"
        if last_verified_at is None:
            last_verified_at = existing.last_verified_at
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
    )
    return True, verified_identity, verified_actionable


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
                children, children_attempted = _fetch_provider_children_if_available(provider, config, match)
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


def _ensure_provider_series(config: AppConfig, *, provider: str, match: dict[str, Any]) -> None:
    provider_series_id = str(match["provider_series_id"])
    title = str(match.get("title") or provider_series_id)
    with connect(config.db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_series (provider, provider_series_id, title, season_title, raw_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_series_id) DO UPDATE SET
                title = excluded.title,
                season_title = excluded.season_title,
                raw_json = excluded.raw_json,
                last_seen_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                provider_series_id,
                title,
                match.get("season_title"),
                json.dumps(match, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()


def enrich_discovery_provider_availability(
    config: AppConfig,
    *,
    providers: list[ProviderTitleSearchClient],
    candidate_limit: int = 25,
    search_limit: int = 10,
    now: datetime | None = None,
    persist_review_queue: bool = True,
) -> EnrichmentSummary:
    current = now or _utc_now()
    fetched_at = _iso(current)
    expires_at = _iso(current + timedelta(days=PROVIDER_SEARCH_CACHE_TTL_DAYS))
    eligibility_expires_at = _iso(current + timedelta(days=PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS))
    summary = EnrichmentSummary()
    metadata = get_mal_anime_metadata_map(config.db_path)
    mappings_by_series = {
        (mapping.provider, mapping.provider_series_id): mapping
        for mapping in list_series_mappings(config.db_path, approved_only=False)
    }
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
    review_entries: list[dict[str, Any]] = []
    child_probe_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    child_probe_failures: set[tuple[str, str]] = set()
    for item in candidates[:candidate_limit]:
        summary.candidates_considered += 1
        mal_id = _candidate_mal_id(item)
        meta = metadata.get(mal_id) if mal_id is not None else None
        if meta is None:
            continue
        title_family = build_target_title_family(meta)
        queries = select_english_provider_search_queries(meta)
        if not queries:
            continue
        summary.queries_selected += len(queries)
        for provider in providers:
            if not hasattr(provider, "search_title"):
                summary.providers_skipped.append(getattr(provider, "slug", provider.__class__.__name__))
                continue
            for query in queries:
                normalized_query = normalize_title(query)
                cached = get_provider_title_search_cache(config.db_path, provider=provider.slug, normalized_query=normalized_query, now=fetched_at)
                if cached is not None:
                    summary.cache_hits += 1
                    matches = _dedupe_provider_matches(cached.matches)
                else:
                    summary.cache_misses += 1
                    try:
                        raw_matches = provider.search_title(config, query, limit=search_limit)
                    except Exception as exc:  # provider/auth/network errors must not stale-out good evidence
                        summary.provider_search_failures += 1
                        if len(summary.failure_details) < 10:
                            summary.failure_details.append({"provider": str(provider.slug), "query": query, "error": str(exc)})
                        continue
                    summary.provider_searches += 1
                    matches = _dedupe_provider_matches([_match_to_dict(match) for match in raw_matches])
                    upsert_provider_title_search_cache(
                        config.db_path,
                        provider=provider.slug,
                        normalized_query=normalized_query,
                        query=query,
                        candidate_mal_anime_id=mal_id,
                        candidate_title=meta.title,
                        matches=matches,
                        status="ok",
                        fetched_at=fetched_at,
                        expires_at=expires_at,
                    )
                decision = classify_provider_matches(query, matches, title_family)
                if decision.kind == "strong" and decision.selected:
                    match = decision.selected[0]
                    provider_series_id = match.get("provider_series_id")
                    if provider_series_id:
                        try:
                            match, detail_attempted = _fetch_provider_detail_if_available(provider, config, match)
                            if detail_attempted:
                                summary.provider_detail_probes += 1
                        except Exception as exc:  # detail enrichment is optional; keep the search hit evidence
                            summary.provider_detail_failures += 1
                            if len(summary.failure_details) < 10:
                                summary.failure_details.append({"provider": str(provider.slug), "query": query, "detail_error": str(exc)})
                        provider_series_id = match.get("provider_series_id") or provider_series_id
                        _ensure_provider_series(config, provider=provider.slug, match=match)
                        summary.strong_matches += 1
                        _upsert_exact_identity_or_append_review(
                            config,
                            summary,
                            review_entries,
                            provider=provider.slug,
                            provider_series_id=provider_series_id,
                            mal_id=mal_id,
                            candidate_title=meta.title,
                            query=query,
                            match=match,
                            mapping=mappings_by_series.get((provider.slug, str(provider_series_id))),
                            decision=decision,
                            title_family=title_family,
                            fetched_at=fetched_at,
                            expires_at=eligibility_expires_at,
                        )
                elif decision.kind == "ambiguous":
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
                    )
                    if len(verified_shells) == 1:
                        verification = verified_shells[0]
                        provider_series_id = str(verification.match.get("provider_series_id") or "")
                        if provider_series_id and _upsert_aggregate_shell_identity(
                            config,
                            summary,
                            provider=provider.slug,
                            mal_id=mal_id,
                            candidate_title=meta.title,
                            query=query,
                            verification=verification,
                            mapping=mappings_by_series.get((provider.slug, provider_series_id)),
                            fetched_at=fetched_at,
                            expires_at=eligibility_expires_at,
                        ):
                            continue
                    summary.ambiguous_matches += 1
                    review_entries.append({
                        "provider": provider.slug,
                        "provider_series_id": None,
                        "severity": "warning",
                        "payload": {
                            "mal_anime_id": mal_id,
                            "candidate_title": meta.title,
                            "query": query,
                            "matches": decision.selected,
                            "decision": "ambiguous_no_auto_link",
                            "provider_search_match_reasons": list(decision.reasons),
                        },
                    })
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
