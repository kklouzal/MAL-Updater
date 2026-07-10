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
    replace_review_queue_entries,
    upsert_provider_title_search_cache,
    upsert_series_mapping,
)
from .mapping import normalize_title
from .recommendations import Recommendation, build_recommendations

PROVIDER_SEARCH_CACHE_TTL_DAYS = 365
DISCOVERY_PROVIDER_SEARCH_REVIEW_ISSUE = "discovery_provider_search_match_review"


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
    review_entries_written: int = 0

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
            "review_entries_written": self.review_entries_written,
            "cache_ttl_days": PROVIDER_SEARCH_CACHE_TTL_DAYS,
        }


def select_english_provider_search_queries(meta: Any) -> list[str]:
    """Return English-only provider search queries; never Japanese/romaji MAL titles."""
    queries: list[str] = []
    raw = meta.raw if isinstance(getattr(meta, "raw", None), dict) else {}
    for value in (getattr(meta, "title_english", None),):
        if isinstance(value, str) and normalize_title(value) and value not in queries:
            queries.append(value)
    alt = raw.get("alternative_titles") if isinstance(raw, dict) else None
    if isinstance(alt, dict):
        en = alt.get("en")
        if isinstance(en, str) and normalize_title(en) and en not in queries:
            queries.append(en)
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
    locales = [str(v).lower() for v in match.get("audio_locales") or []]
    joined = " ".join(locales + [str(match.get("title") or ""), str(match.get("season_title") or "")]).lower()
    return "en-us" in locales or "en" in locales or "english dub" in joined


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
) -> EnrichmentSummary:
    current = now or _utc_now()
    fetched_at = _iso(current)
    expires_at = _iso(current + timedelta(days=PROVIDER_SEARCH_CACHE_TTL_DAYS))
    summary = EnrichmentSummary()
    metadata = get_mal_anime_metadata_map(config.db_path)
    candidates = [r for r in build_recommendations(config, limit=0, require_provider_availability=False) if r.kind == "discovery_candidate" and not r.context.get("availability_visible")]
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
            for query in queries[:1]:
                normalized_query = normalize_title(query)
                cached = get_provider_title_search_cache(config.db_path, provider=provider.slug, normalized_query=normalized_query, now=fetched_at)
                if cached is not None:
                    summary.cache_hits += 1
                    matches = cached.matches
                else:
                    summary.cache_misses += 1
                    raw_matches = provider.search_title(config, query, limit=search_limit)
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
                        upsert_series_mapping(
                            config.db_path,
                            provider=provider.slug,
                            provider_series_id=str(provider_series_id),
                            mal_anime_id=int(mal_id),
                            confidence=0.92,
                            mapping_source="reverse_provider_title_search",
                            approved_by_user=False,
                            notes=f"Auto-discovered by English-title provider search: {query}",
                        )
                        summary.strong_matches += 1
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
    if review_entries:
        result = replace_review_queue_entries(config.db_path, issue_type=DISCOVERY_PROVIDER_SEARCH_REVIEW_ISSUE, entries=review_entries)
        summary.review_entries_written = result["inserted"]
    return summary
