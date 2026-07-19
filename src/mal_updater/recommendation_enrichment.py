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
    failure_details: list[dict[str, str]] = field(default_factory=list)
    eligibility_evidence_upserted: int = 0
    verified_eligibility_evidence_upserted: int = 0
    review_entries_written: int = 0
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
            "failure_details": self.failure_details,
            "eligibility_evidence_upserted": self.eligibility_evidence_upserted,
            "verified_eligibility_evidence_upserted": self.verified_eligibility_evidence_upserted,
            "review_entries_written": self.review_entries_written,
            "dry_run_review_entries": self.dry_run_review_entries,
            "cache_ttl_days": PROVIDER_SEARCH_CACHE_TTL_DAYS,
            "eligibility_evidence_ttl_days": PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS,
        }


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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _match_to_dict(match: Any) -> dict[str, Any]:
    if isinstance(match, dict):
        raw = dict(match)
    else:
        raw = {name: getattr(match, name) for name in ("provider_series_id", "id", "title", "season_title", "url", "audio_locales") if hasattr(match, name)}
    provider_series_id = raw.get("provider_series_id") or raw.get("id")
    title = raw.get("title") or raw.get("name")
    season_title = raw.get("season_title")
    return {
        "provider_series_id": str(provider_series_id) if provider_series_id is not None else None,
        "title": str(title) if title is not None else None,
        "season_title": str(season_title) if season_title is not None else None,
        "url": raw.get("url"),
        "audio_locales": raw.get("audio_locales") if isinstance(raw.get("audio_locales"), list) else [],
        "raw": raw,
    }


def _is_english_dub_match(match: dict[str, Any]) -> bool:
    """Return True only for explicit provider audio-locale evidence.

    Provider search/title fields are not reliable dub evidence: HIDIVE Algolia
    currently exposes no audio/dub contract, and Crunchyroll discover search can
    return empty audio_locales. Keep title-only markers such as "English Dub"
    out of availability gating so unknown rows are not promoted as dubbed.
    """
    locales = {str(v).strip().lower().replace("_", "-") for v in match.get("audio_locales") or [] if v is not None}
    return bool(locales & {"en", "en-us", "en-gb"})


def _explicit_dub_evidence_source(provider: str, match: dict[str, Any]) -> str | None:
    if not _is_english_dub_match(match):
        return None
    return "provider_audio_locale" if provider == "crunchyroll" else "provider_audio_tag"


def _upsert_search_eligibility_evidence(
    config: AppConfig,
    *,
    provider: str,
    mal_anime_id: int,
    candidate_title: str,
    query: str,
    match: dict[str, Any],
    mapping: Any | None,
    fetched_at: str,
    expires_at: str,
) -> tuple[bool, bool]:
    """Persist normalized provider eligibility evidence without auto-approving title search identity."""
    provider_series_id = match.get("provider_series_id")
    if provider not in DISCOVERY_PROVIDER_ELIGIBILITY_PROVIDERS or not provider_series_id:
        return False, False
    approved_identity = bool(
        mapping is not None
        and getattr(mapping, "approved_by_user", False)
        and int(getattr(mapping, "mal_anime_id", -1)) == int(mal_anime_id)
    )
    explicit_source = _explicit_dub_evidence_source(provider, match)
    verified_actionable = approved_identity and explicit_source is not None
    upsert_recommendation_provider_eligibility_evidence(
        config.db_path,
        mal_anime_id=int(mal_anime_id),
        provider=provider,
        provider_series_id=str(provider_series_id),
        provider_title=match.get("title") or match.get("season_title"),
        provider_url=match.get("url"),
        identity_match_kind="approved_mapping" if approved_identity else "provider_title_search",
        match_confidence=(getattr(mapping, "confidence", None) if approved_identity else None),
        review_status="verified" if approved_identity else "review-needed",
        catalog_status="present" if approved_identity else "unknown",
        english_dub_status="present" if verified_actionable else "unknown",
        explicit_dub_evidence_source=explicit_source,
        audio_locales=match.get("audio_locales") if isinstance(match.get("audio_locales"), list) else [],
        source_evidence={
            "source": "bounded_provider_title_search",
            "query": query,
            "candidate_mal_anime_id": int(mal_anime_id),
            "candidate_title": candidate_title,
            "provider": provider,
            "provider_series_id": str(provider_series_id),
            "match": match,
            "approved_mapping": bool(approved_identity),
            "mapping_source": getattr(mapping, "mapping_source", None) if mapping is not None else None,
            "mapping_confidence": getattr(mapping, "confidence", None) if mapping is not None else None,
        },
        fetched_at=fetched_at,
        expires_at=expires_at,
        last_verified_at=fetched_at if verified_actionable else None,
    )
    return True, verified_actionable


def classify_provider_matches(query: str, matches: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    q = normalize_title(query)
    exact = [m for m in matches if normalize_title(m.get("title")) == q or normalize_title(m.get("season_title")) == q]
    if len(exact) == 1:
        return "strong", exact
    if len(exact) > 1:
        return "ambiguous", exact
    near = [m for m in matches if q and (q in normalize_title(m.get("title")) or q in normalize_title(m.get("season_title")))]
    if len(near) == 1:
        return "strong", near
    if near or len(matches) > 1:
        return "ambiguous", near or matches
    return "none", []


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
    for item in candidates[:candidate_limit]:
        summary.candidates_considered += 1
        mal_id = _candidate_mal_id(item)
        meta = metadata.get(mal_id) if mal_id is not None else None
        if meta is None:
            continue
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
                    matches = cached.matches
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
                    matches = [_match_to_dict(match) for match in raw_matches]
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
                kind, selected = classify_provider_matches(query, matches)
                if kind == "strong" and selected:
                    match = selected[0]
                    provider_series_id = match.get("provider_series_id")
                    if provider_series_id:
                        _ensure_provider_series(config, provider=provider.slug, match=match)
                        summary.strong_matches += 1
                        persisted, verified = _upsert_search_eligibility_evidence(
                            config,
                            provider=provider.slug,
                            mal_anime_id=mal_id,
                            candidate_title=meta.title,
                            query=query,
                            match=match,
                            mapping=mappings_by_series.get((provider.slug, str(provider_series_id))),
                            fetched_at=fetched_at,
                            expires_at=eligibility_expires_at,
                        )
                        if persisted:
                            summary.eligibility_evidence_upserted += 1
                        if verified:
                            summary.verified_eligibility_evidence_upserted += 1
                        review_entries.append({
                            "provider": provider.slug,
                            "provider_series_id": str(provider_series_id),
                            "severity": "info",
                            "payload": {"mal_anime_id": mal_id, "candidate_title": meta.title, "query": query, "match": match, "decision": "strong_provider_search_candidate_no_auto_link"},
                        })
                        if _is_english_dub_match(match):
                            review_entries.append({
                                "provider": provider.slug,
                                "provider_series_id": str(provider_series_id),
                                "severity": "info",
                                "payload": {"mal_anime_id": mal_id, "query": query, "match": match, "decision": "strong_english_dub_evidence"},
                            })
                elif kind == "ambiguous":
                    summary.ambiguous_matches += 1
                    review_entries.append({
                        "provider": provider.slug,
                        "provider_series_id": None,
                        "severity": "warning",
                        "payload": {"mal_anime_id": mal_id, "candidate_title": meta.title, "query": query, "matches": selected, "decision": "ambiguous_no_auto_link"},
                    })
    if review_entries and persist_review_queue:
        result = replace_review_queue_entries(config.db_path, issue_type=DISCOVERY_PROVIDER_SEARCH_REVIEW_ISSUE, entries=review_entries)
        summary.review_entries_written = result["inserted"]
    elif review_entries:
        summary.dry_run_review_entries = len(review_entries)
    return summary
