from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .config import AppConfig
from .db import get_mal_anime_metadata_map, get_mal_anime_relations_map, list_series_mappings
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
    state_by_id = {state.provider_series_id: state for state in states}
    mapping_by_series = {
        mapping.provider_series_id: int(mapping.mal_anime_id)
        for mapping in list_series_mappings(config.db_path, provider="crunchyroll", approved_only=False)
    }
    metadata_by_id = get_mal_anime_metadata_map(config.db_path)
    relations_by_id = get_mal_anime_relations_map(config.db_path)

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
    recommendations.extend(_build_new_episode_recommendations(states))
    recommendations = _dedupe_recommendations(recommendations)
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

        tail_gap = _contiguous_tail_gap(state)
        if tail_gap is None or tail_gap <= 0:
            continue

        days_since_watch = _days_since(state.last_watched_at)
        is_recent = days_since_watch is not None and days_since_watch <= 90
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
                provider_series_id=state.provider_series_id,
                title=state.title,
                season_title=state.season_title,
                reasons=reasons,
                context={
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
    best: dict[tuple[str, str], Recommendation] = {}
    for item in items:
        key = (item.kind, item.provider_series_id)
        existing = best.get(key)
        if existing is None or item.priority > existing.priority:
            best[key] = item
    return list(best.values())


def _build_relation_backed_new_season_recommendations(
    states: list[ProviderSeriesState],
    *,
    state_by_id: dict[str, ProviderSeriesState],
    mapping_by_series: dict[str, int],
    metadata_by_id: dict[int, Any],
    relations_by_id: dict[int, list[Any]],
) -> list[Recommendation]:
    items: list[Recommendation] = []
    series_by_anime_id = {anime_id: state_by_id[sid] for sid, anime_id in mapping_by_series.items() if sid in state_by_id}
    sequel_relation_types = {"sequel"}
    predecessor_relation_types = {"prequel", "parent_story"}
    for state in states:
        current_anime_id = mapping_by_series.get(state.provider_series_id)
        if current_anime_id is None:
            continue
        if not _is_english_dub_series(state):
            continue
        relations = relations_by_id.get(current_anime_id, [])
        best_predecessor = None
        for relation in relations:
            if relation.relation_type not in predecessor_relation_types:
                continue
            predecessor_state = series_by_anime_id.get(relation.related_mal_anime_id)
            if predecessor_state is None or not _series_counts_as_completed(predecessor_state):
                continue
            best_predecessor = predecessor_state
            break
        if best_predecessor is not None and state.completed_episode_count <= 0:
            title_hint = metadata_by_id.get(current_anime_id).title if current_anime_id in metadata_by_id else state.title
            items.append(
                Recommendation(
                    kind="new_season",
                    priority=110,
                    provider_series_id=state.provider_series_id,
                    title=state.title,
                    season_title=state.season_title,
                    reasons=[
                        "English dub is available",
                        f"MAL relation metadata links this title as a continuation after {best_predecessor.season_title or best_predecessor.title}",
                    ],
                    context={
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
                sequel_state = series_by_anime_id.get(relation.related_mal_anime_id)
                if sequel_state is None or not _is_english_dub_series(sequel_state):
                    continue
                if sequel_state.completed_episode_count > 0:
                    continue
                title_hint = metadata_by_id.get(relation.related_mal_anime_id).title if relation.related_mal_anime_id in metadata_by_id else sequel_state.title
                items.append(
                    Recommendation(
                        kind="new_season",
                        priority=112,
                        provider_series_id=sequel_state.provider_series_id,
                        title=sequel_state.title,
                        season_title=sequel_state.season_title,
                        reasons=[
                            "English dub is available",
                            f"MAL relation metadata links this as a sequel to {state.season_title or state.title}",
                        ],
                        context={
                            "relation_backed": True,
                            "mal_anime_id": relation.related_mal_anime_id,
                            "metadata_title": title_hint,
                            "predecessor_provider_series_id": state.provider_series_id,
                            "predecessor_title": state.season_title or state.title,
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
