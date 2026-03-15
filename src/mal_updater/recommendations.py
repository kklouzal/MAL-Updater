from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .config import AppConfig
from .mapping import normalize_title
from .sync_planner import ProviderSeriesState, load_provider_series_states

_ENGLISH_DUB_RE = re.compile(r"\benglish dub\b|\(dub\)", re.IGNORECASE)
_FOREIGN_DUB_RE = re.compile(r"\b(?:french|german|spanish|portuguese|italian|arabic|hindi) dub\b", re.IGNORECASE)
_SEASON_NUMBER_RE = re.compile(r"\bseason\s*(\d+)\b", re.IGNORECASE)
_ORDINAL_SEASON_RE = re.compile(
    r"\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d+(?:st|nd|rd|th))\s+season\b",
    re.IGNORECASE,
)
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


@dataclass(slots=True)
class Recommendation:
    kind: str
    priority: int
    provider_series_id: str
    title: str
    season_title: str | None
    reasons: list[str] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "priority": self.priority,
            "provider_series_id": self.provider_series_id,
            "title": self.title,
            "season_title": self.season_title,
            "reasons": self.reasons,
            "context": self.context,
        }


def build_recommendations(config: AppConfig, limit: int | None = 20) -> list[Recommendation]:
    states = load_provider_series_states(config, limit=None)
    recommendations: list[Recommendation] = []
    recommendations.extend(_build_new_season_recommendations(states))
    recommendations.extend(_build_new_episode_recommendations(states))
    recommendations.sort(key=lambda item: (-item.priority, item.title.lower(), item.provider_series_id))
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
        if state.completed_episode_count >= state.max_episode_number:
            continue

        unwatched_count = state.max_episode_number - state.completed_episode_count
        reasons = [
            "English dub is available",
            f"{unwatched_count} episode(s) are available beyond completed progress",
        ]
        if state.last_watched_at:
            reasons.append(f"most recent watch activity: {state.last_watched_at}")
        priority = 70 - min(unwatched_count, 10)
        if unwatched_count == 1:
            priority += 8
        items.append(
            Recommendation(
                kind="new_dubbed_episode",
                priority=priority,
                provider_series_id=state.provider_series_id,
                title=state.title,
                season_title=state.season_title,
                reasons=reasons,
                context={
                    "completed_episode_count": state.completed_episode_count,
                    "max_episode_number": state.max_episode_number,
                    "unwatched_episode_count": unwatched_count,
                    "watchlist_status": state.watchlist_status,
                    "last_watched_at": state.last_watched_at,
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
                reasons.append(f"Crunchyroll watchlist status: {state.watchlist_status}")
            priority = 100 - min(max(installment - 1, 0), 10)
            if state.completed_episode_count <= 0:
                priority += 5
            items.append(
                Recommendation(
                    kind="new_season",
                    priority=priority,
                    provider_series_id=state.provider_series_id,
                    title=state.title,
                    season_title=state.season_title,
                    reasons=reasons,
                    context={
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
        if part_match and "season" in text.lower():
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
