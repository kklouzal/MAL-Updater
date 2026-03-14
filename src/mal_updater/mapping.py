from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from .mal_client import MalApiError, MalClient

_TITLE_CLEANUPS = [
    re.compile(r"\(english dub\)", re.IGNORECASE),
    re.compile(r"\(dub\)", re.IGNORECASE),
    re.compile(r"\benglish dub\b", re.IGNORECASE),
    re.compile(r"\bseason\s+\d+\b", re.IGNORECASE),
    re.compile(r"\bpart\s+\d+\b", re.IGNORECASE),
]
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class SeriesMappingInput:
    provider: str
    provider_series_id: str
    title: str
    season_title: str | None = None
    season_number: int | None = None


@dataclass(slots=True)
class MappingCandidate:
    mal_anime_id: int
    title: str
    alternative_titles: list[str]
    media_type: str | None
    status: str | None
    num_episodes: int | None
    score: float
    matched_query: str
    match_reasons: list[str]
    raw: dict[str, Any]


@dataclass(slots=True)
class MappingResult:
    series: SeriesMappingInput
    status: str
    confidence: float
    chosen_candidate: MappingCandidate | None
    candidates: list[MappingCandidate]
    rationale: list[str]



def normalize_title(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    lowered = normalized.lower().replace("’", "'")
    for pattern in _TITLE_CLEANUPS:
        lowered = pattern.sub(" ", lowered)
    lowered = lowered.replace("&", " and ")
    lowered = _NON_ALNUM_RE.sub(" ", lowered)
    return " ".join(lowered.split())


def _search_query_cleanup(value: str) -> str:
    cleaned = value.replace("’", "'")
    for pattern in _TITLE_CLEANUPS:
        cleaned = pattern.sub(" ", cleaned)
    return " ".join(cleaned.split()).strip()


def _fallback_queries(query: str) -> list[str]:
    variants: list[str] = []
    for delimiter in (":", "-", "("):
        if delimiter in query:
            shortened = query.split(delimiter, 1)[0].strip()
            if shortened and shortened not in variants:
                variants.append(shortened)
    words = query.split()
    if len(words) > 8:
        shortened = " ".join(words[:8]).strip()
        if shortened and shortened not in variants:
            variants.append(shortened)
    return variants


def build_search_queries(series: SeriesMappingInput) -> list[str]:
    queries: list[str] = []
    for value in (series.season_title, series.title):
        if not value:
            continue
        cleaned = _search_query_cleanup(value)
        if cleaned and cleaned not in queries:
            queries.append(cleaned)
    return queries or [_search_query_cleanup(series.title)]


def _extract_titles_from_node(node: dict[str, Any]) -> list[str]:
    titles = [str(node.get("title") or "")]
    alternative_titles = node.get("alternative_titles") or {}
    if isinstance(alternative_titles, dict):
        for key in ("synonyms", "en", "ja"):
            value = alternative_titles.get(key)
            if isinstance(value, list):
                titles.extend(str(item) for item in value if item)
            elif value:
                titles.append(str(value))
    return [title for title in titles if title]


def _score_candidate(query: str, node: dict[str, Any]) -> tuple[float, list[str]]:
    titles = _extract_titles_from_node(node)
    query_norm = normalize_title(query)
    best_ratio = 0.0
    best_title = ""
    for title in titles:
        title_norm = normalize_title(title)
        if not title_norm:
            continue
        ratio = SequenceMatcher(None, query_norm, title_norm).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_title = title
    reasons: list[str] = []
    score = best_ratio
    best_norm = normalize_title(best_title)
    if best_norm == query_norm and best_norm:
        score = max(score, 0.995)
        reasons.append("exact_normalized_title")
    elif query_norm and best_norm and (query_norm in best_norm or best_norm in query_norm):
        score += 0.03
        reasons.append("substring_title_match")
    media_type = node.get("media_type")
    if media_type == "movie":
        score -= 0.05
        reasons.append("movie_penalty")
    score = max(0.0, min(score, 1.0))
    return score, reasons


def map_series(client: MalClient, series: SeriesMappingInput, limit: int = 5) -> MappingResult:
    queries = build_search_queries(series)
    by_id: dict[int, MappingCandidate] = {}
    attempted_queries: list[str] = []
    for query in queries:
        query_variants = [query, *_fallback_queries(query)]
        for variant in query_variants:
            if not variant or variant in attempted_queries:
                continue
            attempted_queries.append(variant)
            try:
                response = client.search_anime(variant, limit=limit)
            except MalApiError:
                continue
            for entry in response.get("data", []):
                node = entry.get("node") or {}
                anime_id = node.get("id")
                if anime_id is None:
                    continue
                score, reasons = _score_candidate(query, node)
                alternative_titles = _extract_titles_from_node(node)[1:]
                candidate = MappingCandidate(
                    mal_anime_id=int(anime_id),
                    title=str(node.get("title") or ""),
                    alternative_titles=alternative_titles,
                    media_type=node.get("media_type"),
                    status=node.get("status"),
                    num_episodes=node.get("num_episodes"),
                    score=score,
                    matched_query=variant,
                    match_reasons=reasons,
                    raw=node,
                )
                previous = by_id.get(candidate.mal_anime_id)
                if previous is None or candidate.score > previous.score:
                    by_id[candidate.mal_anime_id] = candidate
    candidates = sorted(by_id.values(), key=lambda item: (-item.score, item.title.lower(), item.mal_anime_id))
    top = candidates[0] if candidates else None
    second = candidates[1] if len(candidates) > 1 else None
    rationale: list[str] = []
    if not top:
        return MappingResult(series=series, status="no_candidates", confidence=0.0, chosen_candidate=None, candidates=[], rationale=["MAL search returned no candidates"])
    margin = top.score - (second.score if second else 0.0)
    rationale.append(f"top_score={top.score:.3f}")
    rationale.append(f"margin={margin:.3f}")
    rationale.extend(top.match_reasons)
    if top.score >= 0.99 and margin >= 0.05:
        status = "exact"
    elif top.score >= 0.90 and margin >= 0.05:
        status = "strong"
    elif top.score >= 0.78:
        status = "ambiguous"
    else:
        status = "weak"
    return MappingResult(series=series, status=status, confidence=top.score, chosen_candidate=top, candidates=candidates[:limit], rationale=rationale)
