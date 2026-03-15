from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

_AUTO_APPROVAL_BLOCKERS = (
    "season_number_mismatch=",
    "installment_hint_conflict=",
    "episode_evidence_exceeds_candidate_count=",
    "completed_evidence_exceeds_candidate_count=",
)

from .mal_client import MalApiError, MalClient

_TITLE_CLEANUPS = [
    re.compile(r"\(english dub\)", re.IGNORECASE),
    re.compile(r"\(dub\)", re.IGNORECASE),
    re.compile(r"\benglish dub\b", re.IGNORECASE),
    re.compile(r"\bfrench dub\b", re.IGNORECASE),
    re.compile(r"\bbroadcast version\b", re.IGNORECASE),
    re.compile(r"\buncensored\b", re.IGNORECASE),
    re.compile(r"\bseason\s+\d+\b", re.IGNORECASE),
    re.compile(r"\bpart\s+\d+\b", re.IGNORECASE),
]
_QUERY_CLEANUPS = _TITLE_CLEANUPS[:3]
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_SEARCH_SPACING_REWRITES = (
    (re.compile(r"\bseason\s*(\d+)\b", re.IGNORECASE), r"Season \1"),
    (re.compile(r"\bpart\s*(\d+)\b", re.IGNORECASE), r"Part \1"),
    (re.compile(r"\bcour\s*(\d+)\b", re.IGNORECASE), r"Cour \1"),
)
_AUXILIARY_TITLE_PATTERNS = (
    (re.compile(r"\bpv\b", re.IGNORECASE), "pv"),
    (re.compile(r"\bpromo\b", re.IGNORECASE), "promo"),
    (re.compile(r"\bcommercial\b", re.IGNORECASE), "commercial"),
    (re.compile(r"\bcm\b", re.IGNORECASE), "cm"),
    (re.compile(r"\brecaps?\b", re.IGNORECASE), "recap"),
    (re.compile(r"\bextras?\b", re.IGNORECASE), "extra"),
    (re.compile(r"\bpicture\s+drama\b", re.IGNORECASE), "picture_drama"),
    (re.compile(r"\brelay\s+pvs?\b", re.IGNORECASE), "relay_pv"),
)
_ROMAN_TOKEN_RE = re.compile(r"\b(i|ii|iii|iv|v|vi|vii|viii|ix|x)\b", re.IGNORECASE)
_ORDINAL_SEASON_RE = re.compile(
    r"\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s+season\b",
    re.IGNORECASE,
)
_ORDINAL_COUR_RE = re.compile(
    r"\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d+(?:st|nd|rd|th))\s+cour\b",
    re.IGNORECASE,
)
_COUR_NUMBER_RE = re.compile(r"\bcour\s*(\d+)\b", re.IGNORECASE)
_FINAL_SEASON_RE = re.compile(r"\b(?:the\s+)?final\s+season\b", re.IGNORECASE)
_SEASON_RANGE_RE = re.compile(r"\bseasons?\s+\d+\s*[-/]\s*\d+\b", re.IGNORECASE)
_STANDALONE_SEASON_RE = re.compile(r"^season\s+\d+$", re.IGNORECASE)
_STANDALONE_PART_RE = re.compile(r"^part\s+\d+$", re.IGNORECASE)
_STANDALONE_COUR_RE = re.compile(
    r"^(?:cour\s+\d+|(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d+(?:st|nd|rd|th))\s+cour)$",
    re.IGNORECASE,
)
_STANDALONE_FINAL_SEASON_RE = re.compile(r"^final\s+season(?:\s+part\s+\d+)?$", re.IGNORECASE)

_ORDINAL_TO_NUMBER = {
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

_ROMAN_TO_NUMBER = {
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

_NUMBER_TO_ORDINAL = {
    1: "1st",
    2: "2nd",
    3: "3rd",
    4: "4th",
    5: "5th",
    6: "6th",
    7: "7th",
    8: "8th",
    9: "9th",
    10: "10th",
}

_NUMBER_TO_ROMAN = {
    1: "I",
    2: "II",
    3: "III",
    4: "IV",
    5: "V",
    6: "VI",
    7: "VII",
    8: "VIII",
    9: "IX",
    10: "X",
}


@dataclass(slots=True)
class SeriesMappingInput:
    provider: str
    provider_series_id: str
    title: str
    season_title: str | None = None
    season_number: int | None = None
    max_episode_number: int | None = None
    completed_episode_count: int | None = None


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



def _normalize_with_cleanup_patterns(value: str | None, cleanup_patterns: list[re.Pattern[str]]) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    lowered = normalized.lower().replace("’", "'")
    for pattern in cleanup_patterns:
        lowered = pattern.sub(" ", lowered)
    lowered = lowered.replace("&", " and ")
    lowered = _NON_ALNUM_RE.sub(" ", lowered)
    return " ".join(lowered.split())


def normalize_title(value: str | None) -> str:
    return _normalize_with_cleanup_patterns(value, _TITLE_CLEANUPS)


def normalize_title_strict(value: str | None) -> str:
    return _normalize_with_cleanup_patterns(value, _QUERY_CLEANUPS)


def _search_query_cleanup(value: str) -> str:
    cleaned = unicodedata.normalize("NFKC", value).replace("’", "'")
    for pattern in _QUERY_CLEANUPS:
        cleaned = pattern.sub(" ", cleaned)
    for pattern, replacement in _SEARCH_SPACING_REWRITES:
        cleaned = pattern.sub(replacement, cleaned)
    return " ".join(cleaned.split()).strip()


def _season_title_needs_base_title(title: str, season_title: str) -> bool:
    title_norm = normalize_title(title)
    season_norm = _search_query_cleanup(season_title).lower()
    if not season_norm or not title_norm:
        return False
    if title_norm in normalize_title(season_title):
        return False
    return bool(
        _STANDALONE_SEASON_RE.fullmatch(season_norm)
        or _STANDALONE_PART_RE.fullmatch(season_norm)
        or _STANDALONE_COUR_RE.fullmatch(season_norm)
        or _STANDALONE_FINAL_SEASON_RE.fullmatch(season_norm)
    )


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


def _season_number_query_variants(title: str, season_number: int | None) -> list[str]:
    if not title or season_number is None or season_number < 2:
        return []
    variants = [
        f"{title} Season {season_number}",
        f"{title} {_NUMBER_TO_ORDINAL.get(season_number, f'{season_number}th')} Season",
        f"{title} {season_number}",
    ]
    roman = _NUMBER_TO_ROMAN.get(season_number)
    if roman:
        variants.append(f"{title} {roman}")
    return variants


def build_search_queries(series: SeriesMappingInput) -> list[str]:
    queries: list[str] = []

    def add_query(value: str | None) -> None:
        if not value:
            return
        raw = " ".join(str(value).split()).strip()
        if raw and raw not in queries:
            queries.append(raw)
        cleaned = _search_query_cleanup(raw)
        if cleaned and cleaned not in queries:
            queries.append(cleaned)

    add_query(series.season_title)
    if series.season_title and _season_title_needs_base_title(series.title, series.season_title):
        add_query(f"{series.title} {series.season_title}")
    for variant in _season_number_query_variants(series.title, series.season_number):
        add_query(variant)
    add_query(series.title)
    return queries or [_search_query_cleanup(series.title)]


def _extract_season_number(value: str | None) -> int | None:
    if not value:
        return None
    if _SEASON_RANGE_RE.search(value):
        return None
    match = re.search(r"\bseason\s*(\d+)\b", value, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _extract_part_number(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\bpart\s*(\d+)\b", value, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _extract_roman_installment_number(value: str | None) -> int | None:
    if not value:
        return None
    for match in _ROMAN_TOKEN_RE.finditer(value):
        number = _ROMAN_TO_NUMBER.get(match.group(1).lower())
        if number is not None and number >= 2:
            return number
    return None


def _extract_ordinal_season_number(value: str | None) -> int | None:
    if not value:
        return None
    match = _ORDINAL_SEASON_RE.search(value)
    if not match:
        return None
    return _ORDINAL_TO_NUMBER.get(match.group(1).lower())


def _parse_ordinal_token(value: str) -> int | None:
    lowered = value.lower()
    if lowered in _ORDINAL_TO_NUMBER:
        return _ORDINAL_TO_NUMBER[lowered]
    match = re.fullmatch(r"(\d+)(?:st|nd|rd|th)", lowered)
    if match:
        return int(match.group(1))
    return None


def _extract_cour_number(value: str | None) -> int | None:
    if not value:
        return None
    match = _COUR_NUMBER_RE.search(value)
    if match:
        return int(match.group(1))
    match = _ORDINAL_COUR_RE.search(value)
    if not match:
        return None
    return _parse_ordinal_token(match.group(1))


def _extract_title_hints(value: str | None) -> set[str]:
    hints: set[str] = set()
    if not value:
        return hints
    season_number = _extract_season_number(value)
    if season_number is not None:
        hints.add(f"season:{season_number}")
    ordinal_season = _extract_ordinal_season_number(value)
    if ordinal_season is not None:
        hints.add(f"season:{ordinal_season}")
    part_number = _extract_part_number(value)
    if part_number is not None:
        hints.add(f"part:{part_number}")
        hints.add(f"split:{part_number}")
    cour_number = _extract_cour_number(value)
    if cour_number is not None:
        hints.add(f"cour:{cour_number}")
        hints.add(f"split:{cour_number}")
    roman_number = _extract_roman_installment_number(value)
    if roman_number is not None:
        hints.add(f"roman:{roman_number}")
    if _FINAL_SEASON_RE.search(value):
        hints.add("final")
    return hints


def _candidate_title_hints(node: dict[str, Any]) -> set[str]:
    hints: set[str] = set()
    for title in _extract_titles_from_node(node):
        hints.update(_extract_title_hints(title))
    return hints


def _title_has_auxiliary_marker(value: str | None) -> str | None:
    if not value:
        return None
    for pattern, label in _AUXILIARY_TITLE_PATTERNS:
        if pattern.search(value):
            return label
    return None


def _candidate_auxiliary_markers(node: dict[str, Any]) -> set[str]:
    markers: set[str] = set()
    for title in _extract_titles_from_node(node):
        marker = _title_has_auxiliary_marker(title)
        if marker:
            markers.add(marker)
    return markers


def _has_non_base_installment_hint(hints: set[str]) -> bool:
    for hint in hints:
        if hint == "final":
            return True
        if hint.startswith(("season:", "part:", "roman:", "cour:", "split:")):
            try:
                if int(hint.split(":", 1)[1]) > 1:
                    return True
            except ValueError:
                continue
    return False


def _provider_title_hints(series: SeriesMappingInput) -> set[str]:
    hints: set[str] = set()
    for value in (series.season_title, series.title):
        hints.update(_extract_title_hints(value))
    return hints


def _provider_auxiliary_markers(series: SeriesMappingInput) -> set[str]:
    return {
        marker
        for value in (series.season_title, series.title)
        for marker in [_title_has_auxiliary_marker(value)]
        if marker
    }


def _provider_episode_numbering_may_be_aggregated(
    series: SeriesMappingInput,
    provider_hints: set[str],
    candidate_hints: set[str],
    candidate_num_episodes: int,
) -> bool:
    if series.max_episode_number is None or series.max_episode_number <= candidate_num_episodes:
        return False
    if series.completed_episode_count is None or series.completed_episode_count > candidate_num_episodes:
        return False
    if not _has_non_base_installment_hint(provider_hints):
        return False
    if not candidate_hints:
        return False
    shared_installment_hints = provider_hints & candidate_hints
    if not shared_installment_hints:
        return False
    return True


def _candidate_season_numbers(node: dict[str, Any]) -> set[int]:
    numbers: set[int] = set()
    for hint in _candidate_title_hints(node):
        if hint.startswith("season:"):
            numbers.add(int(hint.split(":", 1)[1]))
    return numbers


def _provider_season_number(series: SeriesMappingInput) -> tuple[int | None, str | None]:
    title_season_number = _extract_season_number(series.season_title)
    metadata_season_number = series.season_number
    if title_season_number is not None and metadata_season_number is not None and title_season_number != metadata_season_number:
        return (
            title_season_number,
            f"provider_season_metadata_conflict=metadata:{metadata_season_number};title:{title_season_number}",
        )
    if title_season_number is not None:
        return title_season_number, None
    return metadata_season_number, None


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


def _score_candidate(series: SeriesMappingInput, query: str, node: dict[str, Any]) -> tuple[float, list[str]]:
    titles = _extract_titles_from_node(node)
    query_norm = normalize_title(query)
    query_strict_norm = normalize_title_strict(query)
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
    best_strict_norm = normalize_title_strict(best_title)
    if best_strict_norm == query_strict_norm and best_strict_norm:
        score = max(score, 0.995)
        reasons.append("exact_normalized_title")
    elif query_strict_norm and best_strict_norm and (
        query_strict_norm in best_strict_norm or best_strict_norm in query_strict_norm
    ):
        score += 0.03
        reasons.append("substring_title_match")

    provider_season_number, provider_season_conflict_reason = _provider_season_number(series)
    if provider_season_conflict_reason:
        reasons.append(provider_season_conflict_reason)
    candidate_season_numbers = _candidate_season_numbers(node)
    if provider_season_number is not None and candidate_season_numbers:
        if provider_season_number in candidate_season_numbers:
            score += 0.05
            reasons.append(f"season_number_match={provider_season_number}")
        else:
            score -= 0.08
            reasons.append(
                "season_number_mismatch="
                f"provider:{provider_season_number};candidate:{','.join(str(number) for number in sorted(candidate_season_numbers))}"
            )

    provider_hints = _provider_title_hints(series)
    candidate_hints = _candidate_title_hints(node)
    shared_hints = sorted(provider_hints & candidate_hints)
    conflicting_hints: list[str] = []

    provider_has_non_base_installment_hint = _has_non_base_installment_hint(provider_hints)
    candidate_has_non_base_installment_hint = _has_non_base_installment_hint(candidate_hints)
    if provider_has_non_base_installment_hint and not candidate_hints:
        score -= 0.06
        reasons.append("candidate_missing_installment_hint")
    elif not provider_has_non_base_installment_hint and candidate_has_non_base_installment_hint:
        score -= 0.06
        reasons.append("candidate_extra_installment_hint")

    provider_parts = {hint for hint in provider_hints if hint.startswith("part:")}
    candidate_parts = {hint for hint in candidate_hints if hint.startswith("part:")}
    if provider_parts and candidate_parts:
        if provider_parts & candidate_parts:
            reasons.append(f"part_hint_match={','.join(sorted(provider_parts & candidate_parts))}")
            score += 0.04
        else:
            conflicting_hints.extend(sorted(provider_parts | candidate_parts))

    provider_splits = {hint for hint in provider_hints if hint.startswith("split:")}
    candidate_splits = {hint for hint in candidate_hints if hint.startswith("split:")}
    if provider_splits and candidate_splits:
        if provider_splits & candidate_splits:
            reasons.append(f"split_installment_match={','.join(sorted(provider_splits & candidate_splits))}")
            score += 0.06
        else:
            conflicting_hints.extend(sorted(provider_splits | candidate_splits))

    provider_romans = {hint for hint in provider_hints if hint.startswith("roman:")}
    candidate_romans = {hint for hint in candidate_hints if hint.startswith("roman:")}
    if provider_romans and candidate_romans:
        if provider_romans & candidate_romans:
            reasons.append(f"roman_installment_match={','.join(sorted(provider_romans & candidate_romans))}")
            score += 0.04
        else:
            conflicting_hints.extend(sorted(provider_romans | candidate_romans))

    if "final" in provider_hints and candidate_hints:
        if "final" in candidate_hints:
            reasons.append("final_season_hint_match")
            score += 0.04
        elif any(hint.startswith(("season:", "roman:")) for hint in candidate_hints):
            conflicting_hints.append("final")

    non_season_shared_hints = [hint for hint in shared_hints if not hint.startswith("season:")]
    if non_season_shared_hints:
        reasons.append(f"installment_hint_match={','.join(non_season_shared_hints)}")

    if conflicting_hints:
        penalty = 0.08
        if any(hint.startswith(("part:", "cour:", "split:")) for hint in conflicting_hints):
            penalty = 0.16
        score -= penalty
        reasons.append(f"installment_hint_conflict={','.join(conflicting_hints)}")

    provider_auxiliary_markers = _provider_auxiliary_markers(series)
    candidate_auxiliary_markers = _candidate_auxiliary_markers(node)
    extra_auxiliary_markers = sorted(candidate_auxiliary_markers - provider_auxiliary_markers)
    if extra_auxiliary_markers:
        score -= 0.08
        reasons.append(f"candidate_auxiliary_content={','.join(extra_auxiliary_markers)}")

    candidate_num_episodes = node.get("num_episodes")
    if isinstance(candidate_num_episodes, int) and candidate_num_episodes > 0:
        if series.max_episode_number is not None and series.max_episode_number > candidate_num_episodes:
            if _provider_episode_numbering_may_be_aggregated(series, provider_hints, candidate_hints, candidate_num_episodes):
                score -= 0.03
                reasons.append(
                    f"aggregated_episode_numbering_suspected={series.max_episode_number}>{candidate_num_episodes}"
                )
            else:
                score -= 0.12
                reasons.append(
                    f"episode_evidence_exceeds_candidate_count={series.max_episode_number}>{candidate_num_episodes}"
                )
        elif series.completed_episode_count is not None and series.completed_episode_count > candidate_num_episodes:
            score -= 0.12
            reasons.append(
                f"completed_evidence_exceeds_candidate_count={series.completed_episode_count}>{candidate_num_episodes}"
            )

    media_type = node.get("media_type")
    if (
        media_type == "special"
        and not provider_auxiliary_markers
        and (series.completed_episode_count or 0) > 1
    ):
        score -= 0.10
        reasons.append("special_penalty_for_multi_episode_series")
    elif (
        media_type in {"ova", "ona"}
        and candidate_num_episodes == 1
        and not provider_auxiliary_markers
        and (series.completed_episode_count or 0) > 1
    ):
        score -= 0.06
        reasons.append(f"single_episode_{media_type}_penalty_for_multi_episode_series")
    if media_type == "movie":
        if best_strict_norm == query_strict_norm and best_strict_norm:
            reasons.append("movie_type_allowed_for_exact_title")
        else:
            score -= 0.05
            reasons.append("movie_penalty")
    score = max(0.0, min(score, 1.0))
    return score, reasons


def _candidate_positive_signal_count(candidate: MappingCandidate) -> int:
    return sum(
        1
        for reason in candidate.match_reasons
        if reason == "exact_normalized_title"
        or reason.startswith(
            (
                "season_number_match=",
                "part_hint_match=",
                "split_installment_match=",
                "roman_installment_match=",
                "installment_hint_match=",
            )
        )
        or reason == "final_season_hint_match"
        or reason == "movie_type_allowed_for_exact_title"
    )


def _candidate_penalty_count(candidate: MappingCandidate) -> int:
    penalty_prefixes = (
        "season_number_mismatch=",
        "installment_hint_conflict=",
        "candidate_missing_installment_hint",
        "candidate_extra_installment_hint",
        "candidate_auxiliary_content=",
        "episode_evidence_exceeds_candidate_count=",
        "completed_evidence_exceeds_candidate_count=",
    )
    penalty_reasons = sum(1 for reason in candidate.match_reasons if reason.startswith(penalty_prefixes))
    if candidate.media_type in {"special", "pv"}:
        penalty_reasons += 1
    return penalty_reasons


def _candidate_sort_key(candidate: MappingCandidate) -> tuple[float, int, int, int, int, str, int]:
    return (
        candidate.score,
        int("exact_normalized_title" in candidate.match_reasons),
        _candidate_positive_signal_count(candidate),
        -_candidate_penalty_count(candidate),
        len(normalize_title_strict(candidate.matched_query)),
        candidate.title.lower(),
        -candidate.mal_anime_id,
    )


def _candidate_is_explainably_weaker(series: SeriesMappingInput, candidate: MappingCandidate) -> bool:
    weaker_prefixes = (
        "season_number_mismatch=",
        "installment_hint_conflict=",
        "candidate_missing_installment_hint",
        "candidate_extra_installment_hint",
        "candidate_auxiliary_content=",
        "episode_evidence_exceeds_candidate_count=",
        "completed_evidence_exceeds_candidate_count=",
    )
    if any(reason.startswith(weaker_prefixes) for reason in candidate.match_reasons):
        return True
    if candidate.media_type in {"special", "pv"}:
        return True
    if candidate.media_type in {"ova", "ona"} and candidate.num_episodes == 1 and (series.completed_episode_count or 0) > 1:
        return True
    return False


def _supports_exact_classification(series: SeriesMappingInput, top: MappingCandidate, second: MappingCandidate | None) -> bool:
    if second is None:
        return top.score >= 0.99
    if top.score >= 0.99 and top.score - second.score >= 0.05:
        return True
    if "exact_normalized_title" not in top.match_reasons:
        return False

    reasons = list(top.match_reasons)
    if any(reason.startswith(_AUTO_APPROVAL_BLOCKERS) for reason in reasons):
        return False

    top_query_norm = normalize_title_strict(top.matched_query)
    base_query_norm = normalize_title_strict(series.title)
    second_query_norm = normalize_title_strict(second.matched_query)
    has_specific_installment_context = (series.season_number or 0) >= 2 and top_query_norm != base_query_norm
    if not has_specific_installment_context or top.score < 0.99:
        return False
    if _candidate_is_explainably_weaker(series, second):
        return True
    if "exact_normalized_title" not in second.match_reasons:
        return True
    if second_query_norm == base_query_norm and top_query_norm != base_query_norm:
        return True
    return False


def should_auto_approve_mapping(result: MappingResult) -> bool:
    if result.status != "exact" or result.chosen_candidate is None:
        return False
    if "exact_normalized_title" not in result.chosen_candidate.match_reasons:
        return False

    provider_season_number, _ = _provider_season_number(result.series)
    candidate_season_numbers = _candidate_season_numbers(result.chosen_candidate.raw)
    if provider_season_number is not None and candidate_season_numbers and candidate_season_numbers != {provider_season_number}:
        return False

    reasons = [*result.rationale, *result.chosen_candidate.match_reasons]
    if any(reason.startswith(_AUTO_APPROVAL_BLOCKERS) for reason in reasons):
        return False
    return True


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
                score, reasons = _score_candidate(series, query, node)
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
    candidates = sorted(by_id.values(), key=_candidate_sort_key, reverse=True)
    top = candidates[0] if candidates else None
    second = candidates[1] if len(candidates) > 1 else None
    rationale: list[str] = []
    if not top:
        return MappingResult(series=series, status="no_candidates", confidence=0.0, chosen_candidate=None, candidates=[], rationale=["MAL search returned no candidates"])
    margin = top.score - (second.score if second else 0.0)
    rationale.append(f"top_score={top.score:.3f}")
    rationale.append(f"margin={margin:.3f}")
    rationale.extend(top.match_reasons)
    if _supports_exact_classification(series, top, second):
        status = "exact"
    elif top.score >= 0.90 and margin >= 0.05:
        status = "strong"
    elif top.score >= 0.78:
        status = "ambiguous"
    else:
        status = "weak"
    return MappingResult(series=series, status=status, confidence=top.score, chosen_candidate=top, candidates=candidates[:limit], rationale=rationale)
