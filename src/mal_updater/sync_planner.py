from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import AppConfig, load_mal_secrets
from .db import (
    MalUserAnimeListCacheEntry,
    PersistedSeriesMapping,
    connect,
    get_watch_confirmation_coverage,
    get_mal_user_anime_list_cache,
    get_series_mapping,
    list_recommendation_provider_eligibility_evidence_for_provider_series_keys,
    mal_anime_relations_from_related,
    reconcile_mal_user_state_after_write,
    replace_mal_anime_relations,
    replace_review_queue_entries,
    upsert_series_mapping,
    upsert_watch_confirmation_provenance,
)
from .mal_client import MalApiError, MalClient
from .mapping import (
    MappingCandidate,
    SeriesMappingInput,
    extract_provider_mapping_evidence,
    map_series,
    merge_provider_mapping_evidence,
    should_auto_approve_mapping,
)
from .season_catalog import get_provider_season_catalog
from .season_mappings import get_provider_season_mapping, upsert_provider_season_mapping


EXACT_APPROVED_MAPPING_SOURCES = frozenset({"auto_exact", "user_exact"})
MAL_USER_LIST_STATUSES = frozenset({"completed", "watching", "on_hold", "dropped", "plan_to_watch"})
MAPPING_REVIEW_HEURISTICS_REVISION = "2026-07-24b"
AUTO_CLASSIFIED_PROVIDER_SHELL_DECISION = "auto_classified_provider_shell"
AUTO_CLASSIFIED_AGGREGATE_PROGRESS_DECISION = "auto_classified_aggregate_progress"
AUTO_CLASSIFIED_AMBIGUOUS_EXACT_TITLE_DECISION = "auto_classified_ambiguous_exact_title"
MAPPING_REVIEW_NO_QUEUE_DECISIONS = frozenset(
    {
        "preserved",
        "auto_approved",
        "auto_classified_bundle",
        AUTO_CLASSIFIED_PROVIDER_SHELL_DECISION,
        AUTO_CLASSIFIED_AGGREGATE_PROGRESS_DECISION,
        AUTO_CLASSIFIED_AMBIGUOUS_EXACT_TITLE_DECISION,
    }
)


def _is_approved_mapping_eligible(persisted: PersistedSeriesMapping | None, *, exact_approved_only: bool = False) -> bool:
    if not (persisted and persisted.approved_by_user):
        return False
    if exact_approved_only and persisted.mapping_source not in EXACT_APPROVED_MAPPING_SOURCES:
        return False
    return True


@dataclass(slots=True)
class ProviderSeriesState:
    provider: str
    provider_series_id: str
    title: str
    season_title: str | None
    season_number: int | None
    progress_rows: int
    max_episode_number: int | None
    completed_episode_count: int
    max_completed_episode_number: int | None
    watchlist_status: str | None
    last_watched_at: str | None
    completion_audit: dict[str, Any] = field(default_factory=dict)
    last_progress_seen_at: str | None = None
    last_series_seen_at: str | None = None
    verified_mal_anime_id: int | None = None
    verified_identity_kind: str | None = None
    provider_episode_count: int | None = None
    provider_season_count: int | None = None
    provider_start_year: int | None = None
    provider_start_year_is_trustworthy: bool = False
    verified_identity_evidence: dict[str, Any] | None = None
    account_observed_at: str | None = None
    catalog_observed_at: str | None = None
    # Season-split planning: a provider series whose progress spans several
    # canonical seasons yields one state per season (MAL lists seasons as
    # separate entries). provider_season_key is None for a whole-series state.
    provider_season_key: str | None = None
    season_split: bool = False
    season_unit_rank: int = 0
    min_episode_number: int | None = None
    completed_unnumbered_count: int = 0
    # The same evidence counted by position within the provider season
    # (None when the unit has no ordinals, e.g. a whole-series state).
    ordinal_completed_episode_count: int | None = None
    ordinal_max_completed_episode_number: int | None = None
    ordinal_max_episode_number: int | None = None
    ordinal_min_episode_number: int | None = None
    # "absolute" when an earlier provider season of the series maps to the
    # same MAL entry (the entry spans seasons), "ordinal" when it maps to a
    # different one; None when unknown.
    numbering_hint: str | None = None
    completed_episode_numbers: tuple[int, ...] = ()
    ordinal_completed_episode_numbers: tuple[int, ...] | None = None


@dataclass(slots=True)
class EpisodeProgressState:
    provider: str
    provider_episode_id: str
    episode_number: int | None
    completion_ratio: float | None
    playback_position_ms: int | None
    duration_ms: int | None
    last_watched_at: str | None
    progress_source_surface: str | None = None
    progress_observation_kind: str | None = None
    completion_assertion: str | None = None
    normalization_logic_version: str | None = None
    last_seen_at: str | None = None
    provider_season_key: str | None = None
    season_number: int | None = None
    season_title: str | None = None
    episode_label: str | None = None
    fully_watched: bool | None = None
    season_episode_ordinal: int | None = None


@dataclass(slots=True)
class SyncProposal:
    provider_series_id: str
    provider_title: str
    mapping_status: str
    confidence: float
    mal_anime_id: int | None
    mal_title: str | None
    current_my_list_status: dict[str, Any] | None
    proposed_my_list_status: dict[str, Any] | None
    decision: str
    mapping_source: str | None = None
    persisted_mapping_approved: bool = False
    completion_audit: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    provider: str = "crunchyroll"
    provider_season_key: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "provider_series_id": self.provider_series_id,
            "provider_season_key": self.provider_season_key,
            "provider_title": self.provider_title,
            "crunchyroll_title": self.provider_title,
            "mapping_status": self.mapping_status,
            "confidence": self.confidence,
            "mal_anime_id": self.mal_anime_id,
            "mal_title": self.mal_title,
            "current_my_list_status": self.current_my_list_status,
            "proposed_my_list_status": self.proposed_my_list_status,
            "decision": self.decision,
            "mapping_source": self.mapping_source,
            "persisted_mapping_approved": self.persisted_mapping_approved,
            "completion_audit": self.completion_audit,
            "reasons": self.reasons,
        }


@dataclass(slots=True)
class MappingReviewItem:
    provider: str
    provider_series_id: str
    title: str
    season_title: str | None
    existing_mapping: PersistedSeriesMapping | None
    suggested_mal_anime_id: int | None
    suggested_mal_title: str | None
    mapping_status: str
    confidence: float
    decision: str
    mapper_revision: str = MAPPING_REVIEW_HEURISTICS_REVISION
    reasons: list[str] = field(default_factory=list)
    candidates: list[dict[str, Any]] = field(default_factory=list)
    bundle_companion_candidate: dict[str, Any] | None = None
    bundle_companion_candidates: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "provider_series_id": self.provider_series_id,
            "title": self.title,
            "season_title": self.season_title,
            "existing_mapping": None
            if not self.existing_mapping
            else {
                "provider": self.existing_mapping.provider,
                "provider_series_id": self.existing_mapping.provider_series_id,
                "mal_anime_id": self.existing_mapping.mal_anime_id,
                "confidence": self.existing_mapping.confidence,
                "mapping_source": self.existing_mapping.mapping_source,
                "approved_by_user": self.existing_mapping.approved_by_user,
                "notes": self.existing_mapping.notes,
                "created_at": self.existing_mapping.created_at,
                "updated_at": self.existing_mapping.updated_at,
            },
            "suggested_mal_anime_id": self.suggested_mal_anime_id,
            "suggested_mal_title": self.suggested_mal_title,
            "mapping_status": self.mapping_status,
            "confidence": self.confidence,
            "decision": self.decision,
            "mapper_revision": self.mapper_revision,
            "reasons": self.reasons,
            "candidates": self.candidates,
            "bundle_companion_candidate": self.bundle_companion_candidate,
            "bundle_companion_candidates": self.bundle_companion_candidates,
        }


@dataclass(slots=True)
class ApplyResult:
    provider: str
    provider_series_id: str
    mal_anime_id: int
    mal_title: str | None
    applied: bool
    proposal_decision: str
    requested_status: dict[str, Any] | None
    response_status: dict[str, Any] | None
    reasons: list[str] = field(default_factory=list)
    provider_season_key: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "provider_series_id": self.provider_series_id,
            "provider_season_key": self.provider_season_key,
            "mal_anime_id": self.mal_anime_id,
            "mal_title": self.mal_title,
            "applied": self.applied,
            "proposal_decision": self.proposal_decision,
            "requested_status": self.requested_status,
            "response_status": self.response_status,
            "reasons": self.reasons,
        }


def load_provider_series_states(
    config: AppConfig,
    limit: int | None = None,
    provider: str | None = None,
    provider_series_ids: list[str] | None = None,
    *,
    account_observed_only: bool = True,
    split_seasons: bool = False,
) -> list[ProviderSeriesState]:
    if limit is not None and limit <= 0:
        limit = None
    normalized_provider_series_ids = sorted({value for value in (provider_series_ids or []) if isinstance(value, str) and value.strip()})
    series_query = """
        SELECT
            s.provider,
            s.provider_series_id,
            s.title,
            s.season_title,
            s.season_number,
            s.raw_json,
            s.last_seen_at,
            s.account_observed_at,
            s.catalog_observed_at,
            w.watchlist_status AS watchlist_status
        FROM provider_series s
        LEFT JOIN (
            SELECT provider, provider_series_id, MIN(status) AS watchlist_status
            FROM provider_watchlist
            WHERE is_active = 1
            GROUP BY provider, provider_series_id
        ) w ON w.provider = s.provider AND w.provider_series_id = s.provider_series_id
        WHERE 1=1
    """
    progress_query = """
        SELECT
            provider,
            provider_series_id,
            provider_episode_id,
            episode_number,
            completion_ratio,
            playback_position_ms,
            duration_ms,
            last_watched_at,
            progress_source_surface,
            progress_observation_kind,
            completion_assertion,
            normalization_logic_version,
            last_seen_at,
            provider_season_key,
            season_number,
            season_title,
            episode_label,
            fully_watched,
            season_episode_ordinal
        FROM provider_episode_progress
        WHERE 1=1
    """
    series_params: list[object] = []
    progress_params: list[object] = []
    if account_observed_only:
        series_query += " AND s.account_observed_at IS NOT NULL"
    if provider:
        series_query += " AND s.provider = ?"
        progress_query += " AND provider = ?"
        series_params.append(provider)
        progress_params.append(provider)
    if normalized_provider_series_ids:
        placeholders = ", ".join("?" for _ in normalized_provider_series_ids)
        series_query += f" AND s.provider_series_id IN ({placeholders})"
        progress_query += f" AND provider_series_id IN ({placeholders})"
        series_params.extend(normalized_provider_series_ids)
        progress_params.extend(normalized_provider_series_ids)
    series_query += " ORDER BY s.title ASC"
    progress_query += " ORDER BY provider_series_id ASC, episode_number ASC, last_watched_at ASC, provider_episode_id ASC"
    with connect(config.db_path) as conn:
        series_rows = conn.execute(series_query, series_params).fetchall()
        progress_rows = conn.execute(progress_query, progress_params).fetchall()
        first_episode_by_season = {
            (str(item["provider"]), str(item["provider_season_key"])): int(item["first_episode_number"])
            for item in conn.execute(
                "SELECT provider, provider_season_key, first_episode_number FROM provider_season_catalog "
                "WHERE first_episode_number IS NOT NULL"
            )
        }

    verified_identity_by_series: dict[tuple[str, str], Any] = {}
    conflicting_verified_identity_keys: set[tuple[str, str]] = set()
    for evidence in list_recommendation_provider_eligibility_evidence_for_provider_series_keys(
        config.db_path,
        ((row["provider"], row["provider_series_id"]) for row in series_rows),
    ):
        if evidence.review_status != "verified" or evidence.identity_match_kind not in {
            "provider_title_search_exact",
            "provider_franchise_shell_child_match",
        }:
            continue
        key = (evidence.provider, evidence.provider_series_id)
        candidate = (evidence.mal_anime_id, evidence.identity_match_kind)
        previous = verified_identity_by_series.get(key)
        previous_candidate = (previous.mal_anime_id, previous.identity_match_kind) if previous is not None else None
        if previous is None and key not in conflicting_verified_identity_keys:
            verified_identity_by_series[key] = evidence
        elif previous_candidate != candidate:
            verified_identity_by_series.pop(key, None)
            conflicting_verified_identity_keys.add(key)

    progress_by_series: dict[tuple[str, str], list[EpisodeProgressState]] = {}
    for row in progress_rows:
        label = row["episode_label"]
        # Specials carry a non-numeric label but may reuse low episode numbers
        # (Crunchyroll SP1 -> episode_number 1); they are never MAL episodes.
        is_special = isinstance(label, str) and bool(label) and not label.isdigit()
        progress_by_series.setdefault((row["provider"], row["provider_series_id"]), []).append(
            EpisodeProgressState(
                provider=row["provider"],
                provider_episode_id=row["provider_episode_id"],
                episode_number=None if is_special else row["episode_number"],
                completion_ratio=row["completion_ratio"],
                playback_position_ms=row["playback_position_ms"],
                duration_ms=row["duration_ms"],
                last_watched_at=row["last_watched_at"],
                progress_source_surface=row["progress_source_surface"],
                progress_observation_kind=row["progress_observation_kind"],
                completion_assertion=row["completion_assertion"],
                normalization_logic_version=row["normalization_logic_version"],
                last_seen_at=row["last_seen_at"],
                provider_season_key=row["provider_season_key"],
                season_number=row["season_number"],
                season_title=row["season_title"],
                episode_label=row["episode_label"],
                fully_watched=None if row["fully_watched"] is None else bool(row["fully_watched"]),
                season_episode_ordinal=None if is_special else row["season_episode_ordinal"],
            )
        )

    states: list[tuple[str | None, str, ProviderSeriesState]] = []
    for row in series_rows:
        series_progress = progress_by_series.get((row["provider"], row["provider_series_id"]), [])
        verified_identity = verified_identity_by_series.get((row["provider"], row["provider_series_id"]))
        provider_evidence = extract_provider_mapping_evidence(_load_series_raw_json(row["raw_json"]))
        verified_identity_evidence = provider_evidence.identity_evidence if isinstance(provider_evidence.identity_evidence, dict) else None
        if verified_identity is not None and isinstance(verified_identity.source_evidence, dict):
            raw_identity_evidence = verified_identity.source_evidence.get("identity_evidence")
            if isinstance(raw_identity_evidence, dict):
                verified_identity_evidence = raw_identity_evidence
                provider_evidence = merge_provider_mapping_evidence(
                    provider_evidence,
                    extract_provider_mapping_evidence({"identity_evidence": raw_identity_evidence}),
                )
        season_groups = _season_progress_groups(series_progress) if split_seasons else None
        units: list[tuple[str | None, list[EpisodeProgressState]]]
        if season_groups and len(season_groups) > 1:
            units = list(season_groups.items())
        elif season_groups:
            # One canonical season: plan it against the series mapping, without
            # unkeyed legacy rows that may belong to other seasons.
            units = [(None, next(iter(season_groups.values())))]
        else:
            units = [(None, series_progress)]
        built: list[tuple[str | None, str, ProviderSeriesState]] = []
        keyed_units = bool(season_groups)
        for season_key, unit_progress in units:
            summary = _summarize_episode_progress(unit_progress, config)
            # In-season numbering needs an ordinal on every numbered row: rows
            # stored before ordinals were captured carry only the provider
            # number, and mixing the two numberings would corrupt both views.
            numbered = [row for row in unit_progress if row.episode_number is not None]
            # In-season position: from the catalog's first episode number when
            # known (sequence numbers also count interleaved recaps), or the
            # episode number itself when numbering starts at 1, else the
            # provider sequence number.
            unit_first = (
                first_episode_by_season.get((unit_progress[0].provider, str(unit_progress[0].provider_season_key)))
                if unit_progress and unit_progress[0].provider_season_key
                else None
            )
            if unit_first is None and numbered and min(int(row.episode_number or 0) for row in numbered) <= 1:
                # Numbering already starts at the season's first episode, so it
                # is the in-season position; sequence numbers would be shifted
                # by interleaved recaps (NieR episode 18.5).
                unit_first = 1
            if keyed_units and numbered and unit_first is not None and all(
                row.episode_number is not None and row.episode_number >= unit_first for row in numbered
            ):
                ordinal_rows = [
                    replace(row, episode_number=row.episode_number - unit_first + 1 if row.episode_number is not None else None)
                    for row in unit_progress
                ]
            elif keyed_units and numbered and all(row.season_episode_ordinal is not None for row in numbered):
                ordinal_rows = [
                    replace(row, episode_number=row.season_episode_ordinal if row.episode_number is not None else None)
                    for row in unit_progress
                ]
            else:
                ordinal_rows = None
            ordinal_summary = _summarize_episode_progress(ordinal_rows, config) if ordinal_rows is not None else None
            season_split = season_key is not None
            unit_season_number, unit_season_title = (
                _season_identity(unit_progress) if season_split else (row["season_number"], row["season_title"])
            )
            sort_key = summary["last_watched_at"] or row["account_observed_at"]
            built.append(
                (
                    sort_key,
                    row["title"],
                    ProviderSeriesState(
                        provider=row["provider"],
                        provider_series_id=row["provider_series_id"],
                        title=row["title"],
                        season_title=unit_season_title,
                        season_number=unit_season_number,
                        progress_rows=summary["progress_rows"],
                        max_episode_number=summary["max_episode_number"],
                        completed_episode_count=summary["completed_episode_count"],
                        max_completed_episode_number=summary["max_completed_episode_number"],
                        # A series-level watchlist entry says nothing about which
                        # season to add, so split season states never carry it.
                        watchlist_status=None if season_split else row["watchlist_status"],
                        last_watched_at=summary["last_watched_at"],
                        completion_audit=summary["completion_audit"],
                        last_progress_seen_at=summary["last_progress_seen_at"],
                        last_series_seen_at=row["account_observed_at"],
                        verified_mal_anime_id=verified_identity.mal_anime_id if verified_identity else None,
                        verified_identity_kind=verified_identity.identity_match_kind if verified_identity else None,
                        verified_identity_evidence=verified_identity_evidence,
                        provider_episode_count=None if season_split else provider_evidence.episode_count,
                        provider_season_count=provider_evidence.season_count,
                        provider_start_year=None if season_split else provider_evidence.start_year,
                        provider_start_year_is_trustworthy=False if season_split else provider_evidence.start_year_is_trustworthy,
                        account_observed_at=row["account_observed_at"],
                        catalog_observed_at=row["catalog_observed_at"],
                        provider_season_key=season_key,
                        season_split=season_split,
                        min_episode_number=summary["min_episode_number"],
                        completed_unnumbered_count=summary["completed_unnumbered_count"],
                        ordinal_completed_episode_count=ordinal_summary["completed_episode_count"] if ordinal_summary else None,
                        ordinal_max_completed_episode_number=ordinal_summary["max_completed_episode_number"] if ordinal_summary else None,
                        ordinal_max_episode_number=ordinal_summary["max_episode_number"] if ordinal_summary else None,
                        ordinal_min_episode_number=ordinal_summary["min_episode_number"] if ordinal_summary else None,
                        completed_episode_numbers=summary["completed_episode_numbers"],
                        ordinal_completed_episode_numbers=ordinal_summary["completed_episode_numbers"] if ordinal_summary else None,
                    ),
                )
            )
        if len(built) > 1:
            built.sort(key=lambda item: item[2].last_watched_at or "", reverse=True)
            for rank, item in enumerate(built):
                item[2].season_unit_rank = rank
        states.extend(built)

    states.sort(key=lambda item: (item[0] is not None, item[0] or "", item[1]), reverse=True)
    ordered = [item[2] for item in states]
    if limit is not None:
        return ordered[:limit]
    return ordered


def _load_series_raw_json(value: Any) -> Any:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _summarize_episode_progress(rows: list[EpisodeProgressState], config: AppConfig) -> dict[str, Any]:
    completed_episode_numbers: set[int] = set()
    completed_episode_ids_without_number: set[str] = set()
    max_episode_number: int | None = None
    max_completed_episode_number: int | None = None
    last_watched_at: str | None = None
    last_progress_seen_at: str | None = None
    min_episode_number: int | None = None
    completion_reason_counts: dict[str, int] = {
        "provider_fully_watched": 0,
        "provider_progress_cleared": 0,
        "ratio_threshold": 0,
        "credits_window": 0,
        "later_episode_evidence": 0,
        "next_episode_started_after_runtime": 0,
    }
    completion_reason_examples: dict[str, list[str]] = {
        "provider_fully_watched": [],
        "provider_progress_cleared": [],
        "ratio_threshold": [],
        "credits_window": [],
        "later_episode_evidence": [],
        "next_episode_started_after_runtime": [],
    }
    incomplete_examples: list[str] = []
    uncertain_reason_counts: dict[str, int] = {
        "completion_unknown_history_membership": 0,
        "legacy_unproven_hidive_synthetic_completion": 0,
    }

    for row in rows:
        if row.last_seen_at and (last_progress_seen_at is None or row.last_seen_at > last_progress_seen_at):
            last_progress_seen_at = row.last_seen_at
        completion_reason = _completion_reason(row, rows, config)
        uncertainty_reason = None if completion_reason is not None else _completion_uncertainty_reason(row)
        if uncertainty_reason is not None:
            uncertain_reason_counts[uncertainty_reason] += 1
        else:
            # Completion-uncertain rows remain visible in activity/audit counts,
            # but cannot extend a completion-dependent tail or its freshness.
            if row.episode_number is not None:
                max_episode_number = row.episode_number if max_episode_number is None else max(max_episode_number, row.episode_number)
                min_episode_number = row.episode_number if min_episode_number is None else min(min_episode_number, row.episode_number)
            if row.last_watched_at and (last_watched_at is None or row.last_watched_at > last_watched_at):
                last_watched_at = row.last_watched_at
        if completion_reason is None:
            if len(incomplete_examples) < 5:
                incomplete_examples.append(_episode_audit_label(row))
            continue
        completion_reason_counts[completion_reason] = completion_reason_counts.get(completion_reason, 0) + 1
        examples = completion_reason_examples.setdefault(completion_reason, [])
        if len(examples) < 5:
            examples.append(_episode_audit_label(row))
        if row.episode_number is not None:
            completed_episode_numbers.add(row.episode_number)
            max_completed_episode_number = (
                row.episode_number if max_completed_episode_number is None else max(max_completed_episode_number, row.episode_number)
            )
        else:
            completed_episode_ids_without_number.add(row.provider_episode_id)

    # Unnumbered items (PVs, recaps, OVAs, movies attached to a series) are not
    # MAL episodes of the mapped entry. Only a unit with no numbered episodes at
    # all (a standalone movie or one-shot) counts its completion as one episode.
    numbered_rows_present = any(row.episode_number is not None for row in rows)
    completed_episode_count = len(completed_episode_numbers)
    if not numbered_rows_present and completed_episode_ids_without_number:
        completed_episode_count = 1
    return {
        "progress_rows": len(rows),
        "max_episode_number": max_episode_number,
        "min_episode_number": min_episode_number,
        "completed_episode_count": completed_episode_count,
        "completed_unnumbered_count": len(completed_episode_ids_without_number),
        "completed_episode_numbers": tuple(sorted(completed_episode_numbers)),
        "max_completed_episode_number": max_completed_episode_number,
        "last_watched_at": last_watched_at,
        "last_progress_seen_at": last_progress_seen_at,
        "completion_audit": {
            "completed_by": completion_reason_counts,
            "completed_examples": completion_reason_examples,
            "incomplete_examples": incomplete_examples,
            "completion_uncertain_by": uncertain_reason_counts,
        },
    }


def _completion_uncertainty_reason(row: EpisodeProgressState) -> str | None:
    if row.provider != "hidive":
        return None
    if row.progress_observation_kind == "history_membership" or (
        row.completion_assertion == "unknown"
        and row.progress_source_surface == "hidive_history"
        and row.playback_position_ms is None
    ):
        return "completion_unknown_history_membership"
    if (
        row.progress_source_surface is None
        and row.progress_observation_kind is None
        and row.completion_assertion is None
        and row.normalization_logic_version is None
        and row.completion_ratio == 1.0
        and row.duration_ms is not None
        and row.duration_ms > 0
        and row.playback_position_ms == row.duration_ms
    ):
        return "legacy_unproven_hidive_synthetic_completion"
    return None


def _completion_reason(row: EpisodeProgressState, all_rows: list[EpisodeProgressState], config: AppConfig) -> str | None:
    if row.fully_watched is True:
        return "provider_fully_watched"
    if row.provider == "hidive" and row.progress_observation_kind == "explicit_completed":
        # HIDIVE clears an episode's resume point once it is finished.
        return "provider_progress_cleared"
    if _completion_uncertainty_reason(row) is not None:
        return _history_completion_inference(row, all_rows)
    completion_ratio = row.completion_ratio
    if completion_ratio is not None and completion_ratio >= config.completion_threshold:
        return "ratio_threshold"

    remaining_ms = _remaining_ms(row)
    if remaining_ms is not None and 0 <= remaining_ms <= config.credits_skip_window_seconds * 1000:
        return "credits_window"

    if completion_ratio is None or completion_ratio < 0.85:
        return None
    if row.episode_number is None or row.last_watched_at is None:
        return None

    if any(
        _completion_uncertainty_reason(later) is None
        and later.episode_number is not None
        and later.episode_number > row.episode_number
        and later.last_watched_at is not None
        and later.last_watched_at > row.last_watched_at
        for later in all_rows
    ):
        return "later_episode_evidence"
    return None


# Same bar as the later-episode rule for measured progress (ratio >= 0.85):
# HIDIVE autoplays the next episode when the end credits start, about 87-88%
# into an episode (observed 2026-11-18 across 335 consecutive-episode gaps).
_NEXT_EPISODE_RUNTIME_FRACTION = 0.85


def _history_completion_inference(row: EpisodeProgressState, all_rows: list[EpisodeProgressState]) -> str | None:
    """Infer completion of a membership-only history row from the next episode.

    HIDIVE history proves an episode was started, not finished. When episode
    N+1 of the same season was started at least 90% of episode N's runtime
    after N was started, N was played through (or skipped forward past its end).
    """
    if row.progress_observation_kind != "history_membership" and not (
        row.completion_assertion == "unknown" and row.progress_source_surface == "hidive_history"
    ):
        return None
    if row.episode_number is None or row.duration_ms is None or row.duration_ms <= 0:
        return None
    if row.provider_season_key is None:
        # Without season identity, "episode N+1" may belong to another season.
        return None
    started = _parse_iso_utc(row.last_watched_at)
    if started is None:
        return None
    for later in all_rows:
        if later.episode_number != row.episode_number + 1:
            continue
        if later.provider_season_key != row.provider_season_key:
            continue
        later_started = _parse_iso_utc(later.last_watched_at)
        if later_started is None:
            continue
        if (later_started - started).total_seconds() * 1000 >= row.duration_ms * _NEXT_EPISODE_RUNTIME_FRACTION:
            return "next_episode_started_after_runtime"
    return None


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _season_progress_groups(rows: list[EpisodeProgressState]) -> dict[str, list[EpisodeProgressState]]:
    """Group rows by canonical provider season; rows lacking a key are excluded.

    Unkeyed rows predate season capture. Once any row carries a key, the full
    history refetch has re-observed the series, so unkeyed leftovers cannot be
    attributed to a season and are not counted toward any MAL entry.
    """
    groups: dict[str, list[EpisodeProgressState]] = {}
    for row in rows:
        if row.provider_season_key:
            groups.setdefault(row.provider_season_key, []).append(row)
    return groups


def _season_identity(rows: list[EpisodeProgressState]) -> tuple[int | None, str | None]:
    """Season number/title for a canonical season unit.

    Rows of one key can come from several dub listings of the same season
    (Crunchyroll collapses dubs onto the original season). The lowest season
    ordinal is preferred because dub listings sort after their original;
    ties fall back to the most recently watched row.
    """
    latest_first = sorted(rows, key=lambda row: row.last_watched_at or "", reverse=True)
    numbered = [row for row in latest_first if row.season_number is not None]
    # min() keeps the first (most recent) row among equal season ordinals.
    chosen = min(numbered, key=lambda row: row.season_number or 0) if numbered else latest_first[0]
    return chosen.season_number, chosen.season_title


def _episode_audit_label(row: EpisodeProgressState) -> str:
    episode_fragment = f"ep{row.episode_number}" if row.episode_number is not None else row.provider_episode_id
    ratio_fragment = "unknown"
    if row.completion_ratio is not None:
        ratio_fragment = f"{row.completion_ratio:.3f}"
    remaining_ms = _remaining_ms(row)
    remaining_fragment = "unknown"
    if remaining_ms is not None:
        remaining_fragment = str(remaining_ms)
    return f"{episode_fragment}@ratio={ratio_fragment},remaining_ms={remaining_fragment}"


def _remaining_ms(row: EpisodeProgressState) -> int | None:
    if row.duration_ms is None or row.playback_position_ms is None:
        return None
    return row.duration_ms - row.playback_position_ms


def _auto_approve_mapping(config: AppConfig, state: ProviderSeriesState, mapping_confidence: float, mal_anime_id: int) -> PersistedSeriesMapping:
    return upsert_series_mapping(
        config.db_path,
        provider=state.provider,
        provider_series_id=state.provider_series_id,
        mal_anime_id=mal_anime_id,
        confidence=mapping_confidence,
        mapping_source="auto_exact",
        approved_by_user=True,
        notes="auto-approved: exact normalized title with no contradictory season/episode evidence",
    )


def _build_series_mapping_input(state: ProviderSeriesState) -> SeriesMappingInput:
    if state.season_split:
        # A season is matched on its own, so its evidence is in-season.
        state = _numbering_view(state, None)
    return SeriesMappingInput(
        provider=state.provider,
        provider_series_id=state.provider_series_id,
        title=state.title,
        season_title=state.season_title,
        season_number=state.season_number,
        max_episode_number=state.max_episode_number,
        completed_episode_count=state.completed_episode_count,
        max_completed_episode_number=state.max_completed_episode_number,
        provider_episode_count=state.provider_episode_count,
        provider_season_count=state.provider_season_count,
        provider_start_year=state.provider_start_year,
        provider_start_year_is_trustworthy=state.provider_start_year_is_trustworthy,
        verified_mal_anime_id=state.verified_mal_anime_id,
        verified_identity_kind=state.verified_identity_kind,
        verified_identity_evidence=state.verified_identity_evidence,
    )


def build_mapping_review(
    config: AppConfig,
    limit: int | None = 20,
    mapping_limit: int = 5,
    provider_series_ids: list[str] | None = None,
) -> list[MappingReviewItem]:
    states = load_provider_series_states(config, limit=limit, provider_series_ids=provider_series_ids)
    client = MalClient(config, load_mal_secrets(config))
    items: list[MappingReviewItem] = []
    for state in states:
        existing = get_series_mapping(config.db_path, state.provider, state.provider_series_id)
        if existing and existing.approved_by_user:
            items.append(
                MappingReviewItem(
                    provider=state.provider,
                    provider_series_id=state.provider_series_id,
                    title=state.title,
                    season_title=state.season_title,
                    existing_mapping=existing,
                    suggested_mal_anime_id=existing.mal_anime_id,
                    suggested_mal_title=None,
                    mapping_status="approved",
                    confidence=float(existing.confidence or 1.0),
                    decision="preserved",
                    reasons=["using_user_approved_mapping"],
                    candidates=[],
                )
            )
            continue

        mapping = map_series(client, _build_series_mapping_input(state), limit=mapping_limit)
        reasons = list(mapping.rationale)
        if existing:
            reasons.append(
                f"existing_mapping={existing.mal_anime_id}:{existing.mapping_source}:approved={int(existing.approved_by_user)}"
            )

        effective_mapping = existing
        effective_status = mapping.status
        decision = "needs_review"
        if mapping.is_deterministic_multi_entry_bundle():
            decision = "auto_classified_bundle"
            reasons.append("auto_classified_multi_entry_bundle_non_actionable")
        elif mapping.has_deterministic_aggregate_progress_classification():
            decision = AUTO_CLASSIFIED_AGGREGATE_PROGRESS_DECISION
            reasons.append("auto_classified_aggregate_progress_non_actionable")
        elif mapping.has_deterministic_ambiguous_exact_title_classification():
            decision = AUTO_CLASSIFIED_AMBIGUOUS_EXACT_TITLE_DECISION
            reasons.append("auto_classified_ambiguous_exact_title_non_actionable")
        elif mapping.has_provider_aggregate_shell_protection():
            if mapping.has_deterministic_provider_shell_classification():
                decision = AUTO_CLASSIFIED_PROVIDER_SHELL_DECISION
                reasons.append("auto_classified_provider_shell_non_actionable")
            reasons.append("provider_aggregate_shell_non_actionable")
        elif should_auto_approve_mapping(mapping) and mapping.chosen_candidate:
            effective_mapping = _auto_approve_mapping(config, state, mapping.confidence, mapping.chosen_candidate.mal_anime_id)
            effective_status = "approved"
            decision = "auto_approved"
            reasons.append("auto_approved_exact_unique_match")
        elif mapping.status in {"exact", "strong"} and mapping.chosen_candidate:
            decision = "ready_for_approval"
        elif mapping.status == "no_candidates":
            decision = "needs_manual_match"

        items.append(
            MappingReviewItem(
                provider=state.provider,
                provider_series_id=state.provider_series_id,
                title=state.title,
                season_title=state.season_title,
                existing_mapping=effective_mapping,
                suggested_mal_anime_id=mapping.chosen_candidate.mal_anime_id if mapping.chosen_candidate else None,
                suggested_mal_title=mapping.chosen_candidate.title if mapping.chosen_candidate else None,
                mapping_status=effective_status,
                confidence=mapping.confidence,
                decision=decision,
                reasons=reasons,
                candidates=[
                    {
                        "mal_anime_id": candidate.mal_anime_id,
                        "title": candidate.title,
                        "score": candidate.score,
                        "matched_query": candidate.matched_query,
                        "match_reasons": candidate.match_reasons,
                        "media_type": candidate.media_type,
                    }
                    for candidate in mapping.candidates
                ],
                bundle_companion_candidate=None
                if not mapping.bundle_companion_candidate
                else {
                    "mal_anime_id": mapping.bundle_companion_candidate.mal_anime_id,
                    "title": mapping.bundle_companion_candidate.title,
                    "score": mapping.bundle_companion_candidate.score,
                    "matched_query": mapping.bundle_companion_candidate.matched_query,
                    "match_reasons": mapping.bundle_companion_candidate.match_reasons,
                    "media_type": mapping.bundle_companion_candidate.media_type,
                    "num_episodes": mapping.bundle_companion_candidate.num_episodes,
                },
                bundle_companion_candidates=[
                    {
                        "mal_anime_id": candidate.mal_anime_id,
                        "title": candidate.title,
                        "score": candidate.score,
                        "matched_query": candidate.matched_query,
                        "match_reasons": candidate.match_reasons,
                        "media_type": candidate.media_type,
                        "num_episodes": candidate.num_episodes,
                    }
                    for candidate in (mapping.bundle_companion_candidates or [])
                ],
            )
        )
    return items


def persist_mapping_review_queue(config: AppConfig, items: list[MappingReviewItem]) -> dict[str, int]:
    queue_entries = []
    for item in items:
        if item.decision in MAPPING_REVIEW_NO_QUEUE_DECISIONS:
            continue
        severity = "error" if item.decision == "needs_manual_match" else "warning"
        queue_entries.append(
            {
                "provider": item.provider,
                "provider_series_id": item.provider_series_id,
                "severity": severity,
                "payload": item.as_dict(),
            }
        )
    return replace_review_queue_entries(config.db_path, issue_type="mapping_review", entries=queue_entries)


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_detail_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _completion_decision_for_provenance(
    *,
    provider_watched_episodes: int,
    mal_num_episodes: int | None,
    watchlist_status: str | None,
    progress_rows: int,
) -> tuple[bool, str, str]:
    if mal_num_episodes is not None and mal_num_episodes > 0 and provider_watched_episodes >= mal_num_episodes:
        return True, "provider_completed_known_mal_episode_count", "complete"
    if provider_watched_episodes > 0:
        return False, "provider_progress_without_known_mal_completion", "partial"
    if progress_rows > 0:
        return False, "provider_progress_without_completed_episode", "partial"
    if watchlist_status:
        return False, "provider_watchlist_without_progress", "watchlist_only"
    return False, "no_provider_watch_evidence", "none"


def _watch_identity_key(state: ProviderSeriesState, mal_anime_id: int | None) -> str:
    if mal_anime_id is not None:
        return f"mal:{mal_anime_id}"
    identity = state.verified_identity_evidence if isinstance(state.verified_identity_evidence, dict) else None
    if identity:
        for key in ("series_guid", "series_id", "id", "slug"):
            value = identity.get(key)
            if isinstance(value, str) and value.strip():
                return f"{state.provider}:{key}:{value.strip()}"
    return f"{state.provider}:{state.provider_series_id}"


def _persist_watch_confirmation_provenance_snapshot(
    config: AppConfig,
    state: ProviderSeriesState,
    proposal: SyncProposal,
    *,
    detail: dict[str, Any] | None,
    mapping_reasons: list[str] | None = None,
) -> None:
    if state.season_unit_rank > 0:
        # Provenance is one row per provider series; a season-split series
        # records its most recently watched season only.
        return
    mal_num_episodes = _coerce_detail_int((detail or {}).get("num_episodes"))
    # Record the episode counts in the numbering the decision used.
    state = _numbering_view(state, mal_num_episodes)
    provider_watched_episodes, _, _ = _provider_watched_episodes(state, mal_num_episodes)
    confirmed_complete, completion_decision, completion_status = _completion_decision_for_provenance(
        provider_watched_episodes=provider_watched_episodes,
        mal_num_episodes=mal_num_episodes,
        watchlist_status=state.watchlist_status,
        progress_rows=state.progress_rows,
    )
    generated_at = _utc_iso_now()
    last_evidence_at = max(
        [value for value in (state.last_watched_at, state.last_progress_seen_at, state.last_series_seen_at) if isinstance(value, str)],
        default=None,
    )
    completion_audit = state.completion_audit if isinstance(state.completion_audit, dict) else {}
    completed_by = completion_audit.get("completed_by") if isinstance(completion_audit.get("completed_by"), dict) else {}
    completed_examples = completion_audit.get("completed_examples") if isinstance(completion_audit.get("completed_examples"), dict) else {}
    incomplete_examples = completion_audit.get("incomplete_examples") if isinstance(completion_audit.get("incomplete_examples"), list) else []
    mapping_audit = {
        "mapping_status": proposal.mapping_status,
        "mapping_source": proposal.mapping_source,
        "mapping_confidence": proposal.confidence,
        "mapping_approved": proposal.persisted_mapping_approved,
        "reasons": list(mapping_reasons or []),
    }
    decision_audit = {
        "decision": proposal.decision,
        "current_my_list_status": proposal.current_my_list_status,
        "proposed_my_list_status": proposal.proposed_my_list_status,
        "reasons": list(proposal.reasons),
    }
    progress_audit = {
        "provider": state.provider,
        "provider_series_id": state.provider_series_id,
        "provider_season_key": state.provider_season_key,
        "watchlist_status": state.watchlist_status,
        "progress_rows": state.progress_rows,
        "completed_episode_count": state.completed_episode_count,
        "max_episode_number": state.max_episode_number,
        "max_completed_episode_number": state.max_completed_episode_number,
        "provider_watched_episodes": provider_watched_episodes,
        "last_watched_at": state.last_watched_at,
        "last_progress_seen_at": state.last_progress_seen_at,
        "last_series_seen_at": state.last_series_seen_at,
        "completion_audit": completion_audit,
        "diagnostic_reason_counts": {
            key: int(value)
            for key, value in (completion_audit.get("completion_uncertain_by") or {}).items()
            if isinstance(key, str) and isinstance(value, int) and value > 0
        },
    }
    upsert_watch_confirmation_provenance(
        config.db_path,
        provider=state.provider,
        provider_series_id=state.provider_series_id,
        identity_key=_watch_identity_key(state, proposal.mal_anime_id),
        mal_anime_id=proposal.mal_anime_id,
        source_title=state.title,
        season_title=state.season_title,
        mapped_mal_title=proposal.mal_title or ((detail or {}).get("title") if isinstance((detail or {}).get("title"), str) else None),
        progress_rows=state.progress_rows,
        completed_episode_count=state.completed_episode_count,
        max_episode_number=state.max_episode_number,
        max_completed_episode_number=state.max_completed_episode_number,
        provider_watched_episodes=provider_watched_episodes,
        mal_num_episodes=mal_num_episodes,
        confirmed_complete=confirmed_complete,
        completion_decision=completion_decision,
        completion_status=completion_status,
        completion_threshold=config.completion_threshold,
        credits_skip_window_seconds=config.credits_skip_window_seconds,
        last_watched_at=state.last_watched_at,
        last_progress_seen_at=state.last_progress_seen_at,
        last_series_seen_at=state.last_series_seen_at,
        last_evidence_at=last_evidence_at,
        mapping_source=proposal.mapping_source,
        mapping_confidence=proposal.confidence,
        mapping_approved=proposal.persisted_mapping_approved,
        verified_identity_kind=state.verified_identity_kind,
        verified_identity=state.verified_identity_evidence,
        completed_by=completed_by,
        completed_examples=completed_examples,
        incomplete_examples=incomplete_examples,
        thresholds={
            "completion_threshold": config.completion_threshold,
            "credits_skip_window_seconds": config.credits_skip_window_seconds,
        },
        progress_audit=progress_audit,
        mapping_audit=mapping_audit,
        decision_audit=decision_audit,
        generated_at=generated_at,
    )


def _materialize_hidive_dry_run_confirmations(
    config: AppConfig,
    planned_states: list[ProviderSeriesState],
    proposals: list[SyncProposal],
    details_by_series: dict[tuple[str, str], dict[str, Any] | None],
) -> dict[str, Any]:
    """Materialize all HIDIVE progress-series evidence after a completed plan."""
    # Season-split series contribute their most recently watched season
    # (rank 0); provenance is one row per provider series.
    proposal_by_series = {
        state.provider_series_id: proposal
        for state, proposal in zip(planned_states, proposals)
        if state.provider == "hidive" and state.season_unit_rank == 0
    }
    state_by_series = {
        state.provider_series_id: state
        for state in planned_states
        if state.provider == "hidive" and state.progress_rows > 0 and state.season_unit_rank == 0
    }
    # A bounded plan may omit progress-bearing series. Fill only missing rows for
    # those series, preserving any richer prior confirmation snapshot.
    for state in load_provider_series_states(
        config,
        limit=None,
        provider="hidive",
        account_observed_only=False,
    ):
        if state.progress_rows <= 0:
            continue
        if state.provider_series_id not in state_by_series:
            with connect(config.db_path) as conn:
                already_materialized = conn.execute(
                    "SELECT 1 FROM watch_confirmation_provenance WHERE provider='hidive' AND provider_series_id=?",
                    (state.provider_series_id,),
                ).fetchone()
            if already_materialized is not None:
                continue
            persisted = get_series_mapping(config.db_path, "hidive", state.provider_series_id)
            reasons = ["dry_run_confirmation_coverage_materialization", "not_evaluated_by_bounded_dry_run"]
            proposal = SyncProposal(
                provider="hidive",
                provider_series_id=state.provider_series_id,
                provider_title=state.title,
                mapping_status="approved" if persisted and persisted.approved_by_user else "unreviewed",
                confidence=float(persisted.confidence or 0.0) if persisted else 0.0,
                mal_anime_id=persisted.mal_anime_id if persisted else None,
                mal_title=None,
                current_my_list_status=None,
                proposed_my_list_status=None,
                decision="review",
                mapping_source=persisted.mapping_source if persisted else None,
                persisted_mapping_approved=bool(persisted and persisted.approved_by_user),
                completion_audit=state.completion_audit,
                reasons=reasons,
            )
            _persist_watch_confirmation_provenance_snapshot(config, state, proposal, detail=None, mapping_reasons=reasons)
        else:
            proposal = proposal_by_series[state.provider_series_id]
            _persist_watch_confirmation_provenance_snapshot(
                config,
                state,
                proposal,
                detail=details_by_series.get((state.provider, state.provider_series_id)),
                mapping_reasons=proposal.reasons,
            )
    return get_watch_confirmation_coverage(config.db_path, provider="hidive")


def build_dry_run_sync_plan(
    config: AppConfig,
    limit: int | None = 20,
    mapping_limit: int = 5,
    approved_mappings_only: bool = False,
    exact_approved_only: bool = False,
    provider: str | None = None,
) -> list[SyncProposal]:
    states = load_provider_series_states(config, limit=limit, provider=provider, split_seasons=True)
    client = MalClient(config, load_mal_secrets(config))
    # One slot per state, filled in order; resolved states are planned after
    # every season target is known, since numbering depends on sibling seasons.
    slots: list[list[tuple[SyncProposal, ProviderSeriesState]]] = []
    resolved: list[tuple[int, ProviderSeriesState, str, float, int, str | None, bool, list[str]]] = []
    # Keyed per provider series for confirmation provenance, which records the
    # most recently watched season (rank 0) of a season-split series.
    details_by_series: dict[tuple[str, str], dict[str, Any] | None] = {}
    search_budget = [max(0, int(mapping_limit))]
    for state in states:
        persisted = get_series_mapping(config.db_path, state.provider, state.provider_series_id)
        if approved_mappings_only and not _is_approved_mapping_eligible(persisted, exact_approved_only=exact_approved_only):
            reasons = ["approved_mappings_only_enabled"]
            if exact_approved_only:
                reasons.append("exact_approved_only_enabled")
            if not (persisted and persisted.approved_by_user):
                reasons.append("no_user_approved_mapping")
            else:
                reasons.append(f"mapping_source_not_exact={persisted.mapping_source}")
            proposal = SyncProposal(
                provider_series_id=state.provider_series_id,
                provider_title=state.title,
                mapping_status="unapproved",
                confidence=0.0,
                mal_anime_id=persisted.mal_anime_id if persisted else None,
                mal_title=None,
                current_my_list_status=None,
                proposed_my_list_status=None,
                decision="review",
                mapping_source=persisted.mapping_source if persisted else None,
                persisted_mapping_approved=False,
                reasons=reasons,
                provider=state.provider,
                provider_season_key=state.provider_season_key,
            )
            slots.append([(proposal, state)])
            if state.season_unit_rank == 0:
                details_by_series[(state.provider, state.provider_series_id)] = None
            continue

        mapping_status, confidence, chosen_anime_id, mapping_source, approved, mapping_reasons = _resolve_mapping_for_sync(
            config,
            client,
            state,
            persisted,
            mapping_limit=mapping_limit,
            allow_live_search=not approved_mappings_only,
        )
        if chosen_anime_id is not None and state.season_split:
            if approved:
                season_target = _resolve_season_target(config, client, state, search_budget=search_budget)
                chosen_anime_id = season_target.mal_anime_id
                mapping_source = season_target.mapping_source
                mapping_reasons = mapping_reasons + season_target.reasons
            else:
                chosen_anime_id = None
                mapping_reasons = mapping_reasons + ["season_split_requires_approved_series_mapping"]
        if chosen_anime_id is None:
            proposal = SyncProposal(
                provider_series_id=state.provider_series_id,
                provider_title=state.title,
                mapping_status=mapping_status,
                confidence=confidence,
                mal_anime_id=persisted.mal_anime_id if persisted else None,
                mal_title=None,
                current_my_list_status=None,
                proposed_my_list_status=None,
                decision="review",
                mapping_source=mapping_source,
                persisted_mapping_approved=approved,
                reasons=mapping_reasons,
                provider=state.provider,
                provider_season_key=state.provider_season_key,
            )
            slots.append([(proposal, state)])
            if state.season_unit_rank == 0:
                details_by_series[(state.provider, state.provider_series_id)] = None
            continue

        resolved.append((len(slots), state, mapping_status, confidence, chosen_anime_id, mapping_source, approved, mapping_reasons))
        slots.append([])

    hints = _season_numbering_hints([(item[1], item[4]) for item in resolved])
    for slot, state, mapping_status, confidence, chosen_anime_id, mapping_source, approved, mapping_reasons in resolved:
        hint = hints.get((state.provider, state.provider_series_id, state.provider_season_key))
        if hint is not None:
            state = replace(state, numbering_hint=hint)
        try:
            detail = client.get_anime_details(
                chosen_anime_id,
                fields="id,title,num_episodes,media_type,status,my_list_status,alternative_titles",
            )
        except MalApiError as exc:
            proposal = SyncProposal(
                provider_series_id=state.provider_series_id,
                provider_title=state.title,
                mapping_status=mapping_status,
                confidence=confidence,
                mal_anime_id=chosen_anime_id,
                mal_title=None,
                current_my_list_status=None,
                proposed_my_list_status=None,
                decision="review",
                mapping_source=mapping_source,
                persisted_mapping_approved=approved,
                reasons=mapping_reasons + [f"mal_details_lookup_failed:{exc}"],
                provider=state.provider,
                provider_season_key=state.provider_season_key,
            )
            slots[slot].append((proposal, state))
            if state.season_unit_rank == 0:
                details_by_series[(state.provider, state.provider_series_id)] = None
            continue
        units = (
            _split_cour_units(
                config,
                client,
                state,
                _ApplyTarget(chosen_anime_id, mapping_source, confidence, mapping_reasons),
                first_detail=detail if isinstance(detail, dict) else None,
            )
            if approved
            else [(state, _ApplyTarget(chosen_anime_id, mapping_source, confidence, mapping_reasons))]
        )
        for unit_state, unit_target in units:
            assert unit_target.mal_anime_id is not None
            unit_detail = detail if unit_target.mal_anime_id == chosen_anime_id else _cached_detail(client, unit_target.mal_anime_id)[0]
            if unit_detail is None:
                continue
            proposal = _plan_status_update(
                unit_state,
                unit_detail,
                mapping_status,
                confidence,
                mapping_source=mapping_source,
                persisted_mapping_approved=approved,
                extra_reasons=unit_target.reasons,
            )
            slots[slot].append((proposal, unit_state))
            if unit_state.season_unit_rank == 0:
                details_by_series[(state.provider, state.provider_series_id)] = unit_detail
    proposals = [proposal for entries in slots for proposal, _ in entries]
    proposal_states = [unit_state for entries in slots for _, unit_state in entries]
    if provider in {None, "hidive"}:
        coverage = _materialize_hidive_dry_run_confirmations(config, proposal_states, proposals, details_by_series)
        if not coverage["invariant_satisfied"]:
            raise RuntimeError("HIDIVE progress confirmation coverage invariant failed")
    return proposals


def persist_sync_review_queue(config: AppConfig, proposals: list[SyncProposal]) -> dict[str, int]:
    queue_entries = []
    for proposal in proposals:
        if proposal.decision == "propose_update":
            continue
        severity = "warning"
        if proposal.decision == "review":
            severity = "error"
        queue_entries.append(
            {
                "provider": proposal.provider,
                "provider_series_id": proposal.provider_series_id,
                "severity": severity,
                "payload": proposal.as_dict(),
            }
        )
    return replace_review_queue_entries(config.db_path, issue_type="sync_review", entries=queue_entries)


SEASON_MAPPING_RESOLVER_VERSION = "2026-11-19.4"
# Provider season titles naming non-TV content; such a season never
# auto-matches a TV entry (e.g. "Attack on Titan OADs" -> the TV season).
_AUXILIARY_SEASON_TITLE_RE = re.compile(r"\b(?:ova|ovas|oad|oads|oav|oavs|specials?|movies?|film|recaps?|extras?)\b", re.IGNORECASE)
SEASON_MAPPING_RETRY_SECONDS = 7 * 24 * 60 * 60
_MAL_SYNC_DETAIL_FIELDS = "id,title,num_episodes,media_type,status,my_list_status,alternative_titles"


@dataclass(slots=True)
class _ApplyTarget:
    mal_anime_id: int | None
    mapping_source: str | None
    confidence: float
    reasons: list[str]


_SPLIT_COUR_MEDIA_TYPES = frozenset({"tv", "ona"})
_SPLIT_COUR_MIN_OVERFLOW = 2


def _catalog_detail(client: MalClient, mal_anime_id: int) -> dict[str, Any] | None:
    try:
        detail = client.get_anime_details(mal_anime_id, fields=_MAL_SYNC_DETAIL_FIELDS)
    except MalApiError:
        return None
    return detail if isinstance(detail, dict) else None


def _relation_rows(config: AppConfig, client: MalClient, mal_anime_id: int) -> list[tuple[int, str]]:
    """Return (related id, relation type) for an entry, fetching once if unknown."""
    with connect(config.db_path) as conn:
        rows = conn.execute(
            "SELECT related_mal_anime_id, relation_type FROM mal_anime_relations WHERE mal_anime_id = ?",
            (int(mal_anime_id),),
        ).fetchall()
    if not rows:
        try:
            detail = client.get_anime_details(int(mal_anime_id), fields="id,related_anime")
        except MalApiError:
            return []
        relations = mal_anime_relations_from_related(detail.get("related_anime") if isinstance(detail, dict) else None)
        if not relations:
            return []
        replace_mal_anime_relations(config.db_path, mal_anime_id=int(mal_anime_id), relations=relations)
        rows = [(item["related_mal_anime_id"], item["relation_type"]) for item in relations]
    return [(int(row[0]), str(row[1])) for row in rows]


def _direct_sequel_id(config: AppConfig, client: MalClient, mal_anime_id: int) -> int | None:
    """Return the single direct MAL sequel of an entry (relations cached locally)."""
    sequels = {related for related, kind in _relation_rows(config, client, mal_anime_id) if kind == "sequel"}
    return next(iter(sequels)) if len(sequels) == 1 else None


_FRANCHISE_MAX_HOPS = 12
# Relation lookups per franchise walk. Lookups are cached locally, so the
# bound only matters for a first walk of a large franchise; a truncated walk
# makes the corroboration below decline rather than guess.
_FRANCHISE_MAX_LOOKUPS = 40


def _mal_franchise_ids(config: AppConfig, client: MalClient, anchor_id: int) -> tuple[set[int], bool]:
    """Entries reachable from anchor through prequel/sequel links, and whether the walk finished."""
    frontier, seen, lookups = [int(anchor_id)], {int(anchor_id)}, 0
    for _ in range(_FRANCHISE_MAX_HOPS):
        next_frontier: list[int] = []
        for mal_id in frontier:
            if lookups >= _FRANCHISE_MAX_LOOKUPS:
                return seen, False
            lookups += 1
            for related, kind in _relation_rows(config, client, mal_id):
                if kind in ("sequel", "prequel") and related not in seen:
                    seen.add(related)
                    next_frontier.append(related)
        if not next_frontier:
            return seen, True
        frontier = next_frontier
    return seen, False


_CATALOG_DATE_TOLERANCE_DAYS = 3
_CATALOG_VETO_DAYS = 45
# MAL sometimes files an OVA release as an ONA (e.g. Slime "Coleus no Yume",
# Crunchyroll "OVA Season 2"); date uniqueness still decides the match.
_AUXILIARY_MEDIA_TYPES = frozenset({"ova", "ona", "special", "tv_special", "movie"})
# "other" is left out: on MAL it is mostly commercials, music videos and
# collaborations, which only add detail lookups.
_CATALOG_LEAF_RELATIONS = frozenset({"side_story", "parent_story"})
# Each candidate costs one (cached) MAL detail lookup. A larger pool declines:
# uniqueness of the date match cannot be shown on a truncated pool.
_CATALOG_MAX_CANDIDATES = 40


@dataclass(frozen=True, slots=True)
class _DatedEntry:
    mal_anime_id: int
    media_type: str | None
    num_episodes: int | None
    start: datetime | None
    end: datetime | None


def _parse_day(value: Any) -> datetime | None:
    """Parse a full YYYY-MM-DD date (MAL omits the day for imprecise dates)."""
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d")
    except ValueError:
        return None


def _dated_entry(client: MalClient, mal_anime_id: int) -> _DatedEntry | None:
    try:
        detail = client.get_anime_details(int(mal_anime_id), fields="id,title,num_episodes,media_type,start_date,end_date")
    except MalApiError:
        return None
    if not isinstance(detail, dict):
        return None
    return _DatedEntry(
        mal_anime_id=int(mal_anime_id),
        media_type=_coerce_optional_str(detail.get("media_type")),
        num_episodes=_coerce_detail_int(detail.get("num_episodes")),
        start=_parse_day(detail.get("start_date")),
        end=_parse_day(detail.get("end_date")),
    )


def _catalog_contradicts(client: MalClient, catalog: Any, mal_anime_id: int) -> bool:
    """A title match whose MAL start date is far from the season's original broadcast.

    A multi-episode season whose episodes all carry one date is a batch drop
    (e.g. a dub catalog dated by its release, The Promised Neverland season 2
    is all 2022-03-01), not a broadcast window, so it vetoes nothing.
    """
    if catalog is None or catalog.date_kind != "original_air":
        return False
    first = _parse_day(catalog.first_date)
    if first is None or ((catalog.numbered_episode_count or 0) > 1 and catalog.first_date == catalog.last_date):
        return False
    entry = _dated_entry(client, mal_anime_id)
    if entry is None or entry.start is None:
        return False
    # A provider season can be part of a long MAL entry (D.Gray-man "Season
    # 3-4" is the second half of one 103-episode entry), and single episode
    # dates can be wrong (a 2011 typo in a 2021 season). Only a season with
    # neither end of its window inside the entry's broadcast span contradicts.
    margin = timedelta(days=_CATALOG_VETO_DAYS)
    span_start = entry.start - margin
    span_end = (entry.end or entry.start) + margin
    last = _parse_day(catalog.last_date) or first
    return not (span_start <= first <= span_end or span_start <= last <= span_end)


def _catalog_dated_candidate(
    config: AppConfig,
    client: MalClient,
    state: ProviderSeriesState,
    result: Any,
    catalog: Any,
    *,
    auxiliary_season: bool,
) -> Any | None:
    """Match a season to the one MAL entry whose dates fit its catalog window.

    Sibling seasons often share title stems and episode counts but never their
    broadcast window. Candidates are the matcher's results plus the series'
    MAL franchise (prequel/sequel chain) and its side-story leaves, restricted
    to the season's media family (TV/ONA, or OVA/special/movie for extras
    collections). See _catalog_entry_for_family for the date rules.
    """
    if catalog is None:
        return None
    first = _parse_day(catalog.first_date)
    if first is None:
        return None
    by_id = {int(candidate.mal_anime_id): candidate for candidate in result.candidates[:6]}
    anchor = get_series_mapping(config.db_path, state.provider, state.provider_series_id)
    if anchor is not None and anchor.approved_by_user:
        franchise, _complete = _mal_franchise_ids(config, client, anchor.mal_anime_id)
        for mal_id in franchise:
            by_id.setdefault(int(mal_id), None)
            if not auxiliary_season:
                continue
            # MAL links OVAs/specials as side stories, outside the
            # prequel/sequel chain; they are candidates, not walked further.
            for related, kind in _relation_rows(config, client, mal_id):
                if kind in _CATALOG_LEAF_RELATIONS:
                    by_id.setdefault(int(related), None)
    if len(by_id) > _CATALOG_MAX_CANDIDATES:
        return None
    family = _AUXILIARY_MEDIA_TYPES if auxiliary_season else _SPLIT_COUR_MEDIA_TYPES
    entries = [
        entry
        for entry in (_dated_entry(client, mal_id) for mal_id in by_id)
        if entry is not None and entry.media_type in family and entry.start is not None
    ]
    chosen_entry = _catalog_entry_for_family(config, client, state, catalog, first, entries, family) if entries else None
    if chosen_entry is None:
        return None
    candidate = by_id.get(chosen_entry.mal_anime_id)
    if candidate is not None:
        return candidate
    return MappingCandidate(
        mal_anime_id=chosen_entry.mal_anime_id,
        title="",
        alternative_titles=[],
        media_type=chosen_entry.media_type,
        status=None,
        num_episodes=chosen_entry.num_episodes,
        score=0.0,
        matched_query="season_catalog_dates",
        match_reasons=["season_catalog_dates"],
        raw={},
    )


def _without_same_week_onas(entries: list[_DatedEntry]) -> list[_DatedEntry]:
    """Drop ONAs starting within the date tolerance of a TV entry.

    Chibi/mini ONA spin-offs often start within days of their TV season and
    share its episode count (Hataraku Maou-sama!! and Chiichai! Maou-sama!!),
    so the TV entry is preferred.
    """
    tolerance = timedelta(days=_CATALOG_DATE_TOLERANCE_DAYS)
    tv_starts = [entry.start for entry in entries if entry.media_type == "tv" and entry.start is not None]
    return [
        entry
        for entry in entries
        if entry.media_type != "ona"
        or entry.start is None
        or not any(abs(entry.start - start) <= tolerance for start in tv_starts)
    ]


def _catalog_entry_for_family(
    config: AppConfig,
    client: MalClient,
    state: ProviderSeriesState,
    catalog: Any,
    first: datetime,
    entries: list[_DatedEntry],
    family: frozenset[str],
) -> _DatedEntry | None:
    """Pick the entry whose dates fit the season window (entries all have a start).

    original_air (Crunchyroll): exactly one entry is nearest to the season's
    first broadcast within the tolerance; its end must also match, or its
    direct sequel must end with the season (split cour, Part 1 is returned),
    or the season's watched positions must fit the entry (provider extras
    appended, e.g. OVAs numbered 12-13 after an 11-episode TV season).
    provider_publish (HIDIVE): the provider releases on or after the original
    broadcast, so the latest entry started by the first publish date is taken
    when its episode count equals the season's. A back-catalog season can be
    published after a later sibling aired, so an equal-count sibling started
    by then makes the date ambiguous unless the publish date matches the
    entry's start (a simulcast).
    """
    tolerance = timedelta(days=_CATALOG_DATE_TOLERANCE_DAYS)
    entries = _without_same_week_onas(entries)
    if catalog.date_kind == "original_air":
        starts = sorted(
            (entry for entry in entries if entry.start is not None and abs(entry.start - first) <= tolerance),
            key=lambda entry: abs((entry.start or first) - first),
        )
        if not starts:
            return None
        if len(starts) > 1 and abs((starts[0].start or first) - first) == abs((starts[1].start or first) - first):
            return None
        entry = starts[0]
        last = _parse_day(catalog.last_date)
        if last is None or entry.end is None or abs(entry.end - last) <= tolerance:
            return entry
        if last < entry.end - tolerance:
            return None
        sequel_id = _direct_sequel_id(config, client, entry.mal_anime_id)
        sequel = _dated_entry(client, sequel_id) if sequel_id is not None else None
        if sequel is not None and sequel.media_type in family and sequel.end is not None and abs(sequel.end - last) <= tolerance:
            return entry
        view = _numbering_view(state, None)
        watched_max = int(view.max_completed_episode_number or view.max_episode_number or 0)
        if entry.num_episodes and watched_max and watched_max <= entry.num_episodes:
            return entry
        return None
    count = catalog.numbered_episode_count or catalog.total_episode_count
    started = sorted(
        (entry for entry in entries if entry.start is not None and entry.start <= first + tolerance),
        key=lambda entry: entry.start or datetime.min,
    )
    if not started or not count:
        return None
    latest = started[-1]
    if len(started) > 1 and started[-2].start == latest.start:
        return None
    if latest.num_episodes != count:
        return None
    simulcast = latest.start is not None and abs(latest.start - first) <= tolerance
    if not simulcast and any(entry.num_episodes == count for entry in started[:-1]):
        return None
    return latest


def _franchise_count_corroborated_candidate(
    config: AppConfig, client: MalClient, state: ProviderSeriesState, result: Any
) -> Any | None:
    """Accept the matcher's top candidate when independent evidence agrees.

    Title wording often keeps a correct season match below auto-approval
    ("strong"). The top candidate is accepted when it is a TV/ONA entry in
    the same MAL franchise as the series' approved mapping and its episode
    count equals the number of episodes completed in this season (positions
    1..N, i.e. the season was watched through).

    Sibling seasons often share an episode count (two 12-episode seasons),
    so the count alone cannot tell them apart. The candidate is declined
    when another TV/ONA candidate of the franchise has the same count, when
    another provider season of the series already maps to it, or when it is
    the series anchor while this is a later provider season.
    """
    candidate = result.chosen_candidate or (result.candidates[0] if result.candidates else None)
    if candidate is None or candidate.media_type not in _SPLIT_COUR_MEDIA_TYPES or not candidate.num_episodes:
        return None
    episodes = int(candidate.num_episodes)
    candidate_id = int(candidate.mal_anime_id)
    view = _numbering_view(state, None)
    if (
        view.min_episode_number not in (None, 1)
        or view.completed_episode_count != episodes
        or int(view.max_completed_episode_number or 0) != episodes
    ):
        return None
    anchor = get_series_mapping(config.db_path, state.provider, state.provider_series_id)
    if anchor is None or not anchor.approved_by_user:
        return None
    if candidate_id == int(anchor.mal_anime_id) and state.season_number is not None and int(state.season_number) > 1:
        return None
    with connect(config.db_path) as conn:
        claimed = conn.execute(
            """
            SELECT 1 FROM provider_season_mappings
            WHERE provider = ? AND provider_series_id = ? AND provider_season_key != ?
              AND approved = 1 AND mal_anime_id = ?
            LIMIT 1
            """,
            (state.provider, state.provider_series_id, state.provider_season_key, candidate_id),
        ).fetchone()
    if claimed is not None:
        return None
    franchise, complete = _mal_franchise_ids(config, client, anchor.mal_anime_id)
    if candidate_id not in franchise:
        return None
    for other in result.candidates:
        if (
            int(other.mal_anime_id) != candidate_id
            and other.media_type in _SPLIT_COUR_MEDIA_TYPES
            and other.num_episodes
            and int(other.num_episodes) == episodes
            and (not complete or int(other.mal_anime_id) in franchise)
        ):
            return None
    return candidate


def _split_cour_first_part(
    config: AppConfig, client: MalClient, state: ProviderSeriesState, candidates: list[Any]
) -> Any | None:
    """Pick Part 1 when a provider season spans a MAL entry and its direct sequel.

    Providers often publish a split-cour season as one season (e.g. 30
    episodes) while MAL lists Part 1 and Part 2. Accept the first part only when
    the top two candidates are prequel/sequel of each other, both are TV/ONA,
    and the season's episode evidence exceeds Part 1 but fits both parts.
    """
    top = [candidate for candidate in candidates[:2] if candidate.media_type in _SPLIT_COUR_MEDIA_TYPES]
    if len(top) != 2 or any(not candidate.num_episodes for candidate in top):
        return None
    state = _numbering_view(state, None)
    if state.min_episode_number not in (None, 1):
        # Numbering continued from an earlier season (Season 2 = 13-24) also
        # exceeds Part 1; only a season numbered from 1 spans Part 1 into Part 2.
        return None
    evidence = max(int(state.max_episode_number or 0), state.completed_episode_count)
    for first, second in (top, top[::-1]):
        if _direct_sequel_id(config, client, first.mal_anime_id) != second.mal_anime_id:
            continue
        if int(first.num_episodes) < evidence <= int(first.num_episodes) + int(second.num_episodes):
            return first
    return None


def _resolve_season_target(
    config: AppConfig,
    client: MalClient,
    state: ProviderSeriesState,
    *,
    search_budget: list[int],
) -> _ApplyTarget:
    """Resolve one canonical provider season of a season-split series to a MAL entry.

    The series-level mapping identifies the franchise only; each season is
    matched with the same mapping engine and auto-approval rules used for
    series. Unresolved seasons are recorded and retried on a cadence.
    """
    assert state.provider_season_key is not None
    season_label = f"season={state.provider_season_key}"
    stored = get_provider_season_mapping(
        config.db_path,
        provider=state.provider,
        provider_series_id=state.provider_series_id,
        provider_season_key=state.provider_season_key,
    )
    # User approvals are final; automatic matches are re-resolved when the
    # resolver logic changes.
    stored_is_current = stored is not None and (
        stored.mapping_source == "user_exact" or stored.resolver_version == SEASON_MAPPING_RESOLVER_VERSION
    )
    if stored is not None and stored.approved and stored.mal_anime_id is not None and (stored_is_current or search_budget[0] <= 0):
        return _ApplyTarget(
            stored.mal_anime_id,
            stored.mapping_source,
            float(stored.confidence or 1.0),
            [f"season_mapping={stored.mapping_source}", season_label],
        )
    if stored is not None and stored.resolver_version == SEASON_MAPPING_RESOLVER_VERSION:
        resolved_at = _sqlite_utc(stored.resolved_at)
        catalog_row = get_provider_season_catalog(
            config.db_path, provider=state.provider, provider_season_key=state.provider_season_key
        )
        # New catalog evidence since the last attempt warrants an immediate retry.
        catalog_newer = (
            catalog_row is not None
            and catalog_row.first_date is not None
            and resolved_at is not None
            and (_sqlite_utc(catalog_row.fetched_at) or resolved_at) > resolved_at
        )
        if (
            not catalog_newer
            and resolved_at is not None
            and (datetime.now(timezone.utc) - resolved_at).total_seconds() < SEASON_MAPPING_RETRY_SECONDS
        ):
            return _ApplyTarget(None, None, 0.0, ["season_mapping_unresolved", season_label])
    if search_budget[0] <= 0:
        return _ApplyTarget(None, None, 0.0, ["season_mapping_pending_search_budget", season_label])
    search_budget[0] -= 1
    result = map_series(client, _build_series_mapping_input(state))
    if not result.candidates and stored is not None and stored.approved and stored.mal_anime_id is not None:
        # Re-resolving a stale automatic match found no candidates at all: a
        # failed or empty search (map_series tolerates MAL search errors) is
        # no evidence against the match, so keep it and retry on a later run.
        return _ApplyTarget(
            stored.mal_anime_id,
            stored.mapping_source,
            float(stored.confidence or 1.0),
            [f"season_mapping={stored.mapping_source}", season_label, "season_mapping_reresolve_inconclusive"],
        )
    chosen = result.chosen_candidate
    candidates = [
        {"mal_anime_id": candidate.mal_anime_id, "title": candidate.title, "media_type": candidate.media_type}
        for candidate in result.candidates[:5]
    ]
    auxiliary_season = bool(state.season_title and _AUXILIARY_SEASON_TITLE_RE.search(state.season_title))
    if auxiliary_season and chosen is not None and chosen.media_type == "tv":
        chosen = None
    catalog = get_provider_season_catalog(
        config.db_path, provider=state.provider, provider_season_key=state.provider_season_key
    )
    accepted_by: str | None = None
    if chosen is not None and should_auto_approve_mapping(result):
        if _catalog_contradicts(client, catalog, chosen.mal_anime_id):
            chosen = None
        else:
            accepted_by = "mapping_engine_auto_approval"
    if accepted_by is None and chosen is not None:
        split_first = _split_cour_first_part(config, client, state, list(result.candidates))
        if split_first is not None:
            chosen, accepted_by = split_first, "split_cour_first_part"
    if accepted_by is None:
        dated = _catalog_dated_candidate(config, client, state, result, catalog, auxiliary_season=auxiliary_season)
        if dated is not None:
            chosen, accepted_by = dated, f"season_catalog_{catalog.date_kind if catalog else 'dates'}"
    if accepted_by is None and chosen is not None:
        corroborated = _franchise_count_corroborated_candidate(config, client, state, result)
        if corroborated is not None:
            chosen, accepted_by = corroborated, "franchise_episode_count_corroborated"
    if (
        accepted_by is None
        and stored is not None
        and stored.approved
        and stored.mapping_source == "auto_exact"
        and stored.mal_anime_id is not None
        and not _catalog_contradicts(client, catalog, stored.mal_anime_id)
    ):
        # Re-checking an earlier automatic match that the current rules cannot
        # confirm is not evidence against it; only contradicting dates demote it.
        # It is restamped with the current resolver so it is not re-searched.
        upsert_provider_season_mapping(
            config.db_path,
            provider=state.provider,
            provider_series_id=state.provider_series_id,
            provider_season_key=state.provider_season_key,
            mal_anime_id=stored.mal_anime_id,
            mapping_source="auto_exact",
            confidence=stored.confidence,
            season_title=state.season_title,
            season_number=state.season_number,
            resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
            detail={**stored.detail, "kept_unconfirmed_by_resolver": SEASON_MAPPING_RESOLVER_VERSION},
        )
        return _ApplyTarget(
            stored.mal_anime_id,
            stored.mapping_source,
            float(stored.confidence or 1.0),
            [f"season_mapping={stored.mapping_source}", season_label, "season_mapping_reresolve_unconfirmed_kept"],
        )
    if chosen is not None and accepted_by is not None:
        upsert_provider_season_mapping(
            config.db_path,
            provider=state.provider,
            provider_series_id=state.provider_series_id,
            provider_season_key=state.provider_season_key,
            mal_anime_id=chosen.mal_anime_id,
            mapping_source="auto_exact",
            confidence=float(result.confidence),
            season_title=state.season_title,
            season_number=state.season_number,
            resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
            detail={"rationale": list(result.rationale), "accepted_by": accepted_by, "candidates": candidates},
        )
        reasons = ["season_mapping=auto_exact", season_label, f"season_mapping_accepted_by={accepted_by}"]
        return _ApplyTarget(chosen.mal_anime_id, "auto_exact", float(result.confidence), reasons)
    upsert_provider_season_mapping(
        config.db_path,
        provider=state.provider,
        provider_series_id=state.provider_series_id,
        provider_season_key=state.provider_season_key,
        mal_anime_id=chosen.mal_anime_id if chosen is not None else None,
        mapping_source="unresolved",
        confidence=float(result.confidence),
        season_title=state.season_title,
        season_number=state.season_number,
        resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
        detail={"status": result.status, "rationale": list(result.rationale), "candidates": candidates},
    )
    return _ApplyTarget(None, None, 0.0, ["season_mapping_unresolved", f"season_mapping_status={result.status}", season_label])


def _sqlite_utc(value: str | None) -> datetime | None:
    """Parse SQLite CURRENT_TIMESTAMP text (UTC, 'YYYY-MM-DD HH:MM:SS')."""
    if not value:
        return None
    return _parse_iso_utc(value.replace(" ", "T") + ("" if value.endswith("Z") else "Z"))


def _apply_target(
    config: AppConfig,
    client: MalClient,
    state: ProviderSeriesState,
    persisted: PersistedSeriesMapping,
    *,
    search_budget: list[int],
) -> _ApplyTarget:
    if state.season_split:
        return _resolve_season_target(config, client, state, search_budget=search_budget)
    return _ApplyTarget(persisted.mal_anime_id, persisted.mapping_source, float(persisted.confidence or 1.0), [])


def _apply_priority_key(state: ProviderSeriesState) -> tuple[int, float]:
    """Progress-bearing series first (most recently watched first), then watchlist-only."""
    watched_at = _parse_iso_utc(state.last_watched_at)
    if watched_at is not None:
        return (0, -watched_at.timestamp())
    return (1, 0.0)


def execute_approved_sync(
    config: AppConfig,
    limit: int | None = 20,
    mapping_limit: int = 5,
    exact_approved_only: bool = False,
    dry_run: bool = True,
    providers: list[str] | None = None,
) -> list[ApplyResult]:
    """Plan every approved provider series (season-split) and apply up to ``limit`` updates.

    Planning uses cached MAL state, which costs no request for cached entries;
    only proposals that need a write are revalidated live and written, and
    ``limit`` bounds those. Seasons needing a new MAL search are bounded by
    ``mapping_limit``.
    """
    provider_filter = set(providers) if providers else None
    states = [
        state
        for state in load_provider_series_states(config, limit=None, split_seasons=True)
        if provider_filter is None or state.provider in provider_filter
    ]
    states.sort(key=_apply_priority_key)
    client = MalClient(config, load_mal_secrets(config))
    results: list[ApplyResult] = []
    search_budget = [max(0, int(mapping_limit))]
    # None or a non-positive limit means unbounded (the CLI maps --limit 0 to a
    # full scan; the service disables a zero limit before calling).
    live_budget = None if limit is None or int(limit) <= 0 else int(limit)

    def preview_result(state: ProviderSeriesState, target: _ApplyTarget, preview: SyncProposal, extra: list[str]) -> ApplyResult:
        return ApplyResult(
            provider=state.provider,
            provider_series_id=state.provider_series_id,
            provider_season_key=state.provider_season_key,
            mal_anime_id=int(target.mal_anime_id or 0),
            mal_title=preview.mal_title,
            applied=False,
            proposal_decision=preview.decision,
            requested_status=preview.proposed_my_list_status,
            response_status=preview.current_my_list_status,
            reasons=preview.reasons + extra,
        )

    # Phase 1: resolve every unit's MAL target, then plan each from cached MAL
    # state (no write budget used).
    targeted: list[tuple[ProviderSeriesState, _ApplyTarget, list[str]]] = []
    for state in states:
        persisted = get_series_mapping(config.db_path, state.provider, state.provider_series_id)
        if persisted is None or not _is_approved_mapping_eligible(persisted, exact_approved_only=exact_approved_only):
            continue
        base_reasons = ["using_user_approved_mapping", *(["exact_approved_only_enabled"] if exact_approved_only else [])]
        target = _apply_target(config, client, state, persisted, search_budget=search_budget)
        if target.mal_anime_id is None:
            results.append(
                ApplyResult(
                    provider=state.provider,
                    provider_series_id=state.provider_series_id,
                    provider_season_key=state.provider_season_key,
                    mal_anime_id=persisted.mal_anime_id,
                    mal_title=None,
                    applied=False,
                    proposal_decision="review",
                    requested_status=None,
                    response_status=None,
                    reasons=base_reasons + target.reasons,
                )
            )
            continue
        if exact_approved_only and target.mapping_source not in EXACT_APPROVED_MAPPING_SOURCES:
            continue
        targeted.append((state, target, base_reasons))

    hints = _season_numbering_hints([(state, target.mal_anime_id) for state, target, _ in targeted])
    candidates: list[tuple[ProviderSeriesState, _ApplyTarget, SyncProposal | None, list[str], dict[str, Any] | None]] = []
    for state, target, base_reasons in targeted:
        assert target.mal_anime_id is not None
        hint = hints.get((state.provider, state.provider_series_id, state.provider_season_key))
        if hint is not None:
            state = replace(state, numbering_hint=hint)
        first_detail, first_lookup_reasons = _cached_detail(client, target.mal_anime_id)
        target = _ApplyTarget(target.mal_anime_id, target.mapping_source, target.confidence, base_reasons + target.reasons)
        for unit_state, unit_target in _split_cour_units(config, client, state, target, first_detail=first_detail):
            if unit_target.mal_anime_id == target.mal_anime_id:
                unit_detail, lookup_reasons = first_detail, first_lookup_reasons
            else:
                assert unit_target.mal_anime_id is not None
                unit_detail, lookup_reasons = _cached_detail(client, unit_target.mal_anime_id)
            preview = _preview_from_detail(unit_state, unit_target, unit_detail)
            candidates.append((unit_state, unit_target, preview, lookup_reasons, unit_detail))

    # Phase 2: several provider units can map to one MAL entry (a long
    # MAL entry split into provider seasons, or a split-cour overflow); only
    # the most advanced proposal per entry proceeds.
    best_by_mal: dict[int, int] = {}
    for index, (_, unit_target, preview, _, _) in enumerate(candidates):
        if preview is None or preview.decision != "propose_update":
            continue
        mal_id = int(unit_target.mal_anime_id or 0)
        current_best = best_by_mal.get(mal_id)
        if current_best is None or _proposal_rank(preview) > _proposal_rank(candidates[current_best][2]):
            best_by_mal[mal_id] = index

    deferred = 0
    for index, (state, target, preview, lookup_reasons, cached_detail) in enumerate(candidates):
        if preview is not None:
            if preview.decision == "propose_update" and best_by_mal.get(int(target.mal_anime_id or 0)) != index:
                results.append(preview_result(state, target, preview, ["superseded_by_more_advanced_unit_for_same_mal_entry"]))
                continue
            if preview.decision != "propose_update" and not dry_run:
                _persist_watch_confirmation_provenance_snapshot(
                    config, state, preview, detail=cached_detail, mapping_reasons=preview.reasons
                )
            if preview.decision != "propose_update" or dry_run:
                results.append(
                    preview_result(state, target, preview, ["executor_dry_run"] if dry_run and preview.decision == "propose_update" else [])
                )
                continue
        elif dry_run:
            error = any(reason.startswith("mal_details_lookup_failed:") for reason in lookup_reasons)
            results.append(
                ApplyResult(
                    provider=state.provider,
                    provider_series_id=state.provider_series_id,
                    provider_season_key=state.provider_season_key,
                    mal_anime_id=int(target.mal_anime_id or 0),
                    mal_title=None,
                    applied=False,
                    proposal_decision="error" if error else "review",
                    requested_status=None,
                    response_status=None,
                    reasons=target.reasons + (lookup_reasons or ["mal_cached_detail_unusable"]),
                )
            )
            continue
        if live_budget is not None and live_budget <= 0:
            deferred += 1
            continue
        if live_budget is not None:
            live_budget -= 1
        results.append(_revalidate_and_apply(config, client, state, target, base_reasons=target.reasons))
    if deferred:
        results.append(
            ApplyResult(
                provider="all",
                provider_series_id="",
                mal_anime_id=0,
                mal_title=None,
                applied=False,
                proposal_decision="deferred",
                requested_status=None,
                response_status=None,
                reasons=[f"write_limit_reached deferred_proposals={deferred}"],
            )
        )
    return results


_STATUS_RANK = {"plan_to_watch": 0, "watching": 1, "on_hold": 1, "dropped": 1, "completed": 2}


def _proposal_rank(proposal: SyncProposal | None) -> tuple[int, int]:
    requested = (proposal.proposed_my_list_status if proposal else None) or {}
    return (_STATUS_RANK.get(str(requested.get("status")), 0), int(requested.get("num_watched_episodes") or 0))


def _cached_detail(client: MalClient, mal_anime_id: int) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        detail = client.get_anime_details(mal_anime_id, fields=_MAL_SYNC_DETAIL_FIELDS)
    except MalApiError as exc:
        return None, [f"mal_details_lookup_failed:{exc}"]
    if not isinstance(detail, dict) or "my_list_status" not in detail:
        return None, []
    if _validated_live_mal_user_state(detail, expected_anime_id=mal_anime_id)[1] is not None:
        # Unusable cached shape: only the authoritative live read decides.
        return None, []
    return detail, []


def _preview_from_detail(
    state: ProviderSeriesState, target: _ApplyTarget, detail: dict[str, Any] | None
) -> SyncProposal | None:
    if detail is None:
        return None
    return _plan_status_update(
        state,
        detail,
        mapping_status="approved",
        confidence=target.confidence,
        mapping_source=target.mapping_source,
        persisted_mapping_approved=True,
        extra_reasons=target.reasons,
    )


def _split_cour_units(
    config: AppConfig,
    client: MalClient,
    state: ProviderSeriesState,
    target: _ApplyTarget,
    *,
    first_detail: dict[str, Any] | None = None,
) -> list[tuple[ProviderSeriesState, _ApplyTarget]]:
    """Split one provider unit across a MAL entry and its direct sequel.

    Applies when the unit is numbered from 1, at least two completed episodes
    lie past the entry's episode count, and the entry's single direct TV/ONA
    sequel can hold them (MAL Part 1 / Part 2 of one provider season). Each
    completed episode goes to the part it belongs to, so a skipped or
    unfinished episode does not block the split.
    """
    assert target.mal_anime_id is not None
    # Only catalog fields (episode count, media type) are needed here, so the
    # entry need not be on the user's list.
    detail = first_detail if first_detail is not None else _catalog_detail(client, target.mal_anime_id)
    first_count = _coerce_detail_int((detail or {}).get("num_episodes"))
    state = _numbering_view(state, first_count)
    numbers = state.completed_episode_numbers
    if not numbers:
        return [(state, target)]
    first_numbers = [number for number in numbers if first_count and number <= first_count]
    second_numbers = [number - int(first_count or 0) for number in numbers if first_count and number > first_count]
    if (
        not first_count
        or state.min_episode_number not in (None, 1)
        # A single episode past MAL's count is far more often an extra the
        # provider numbers into the season (MAL lists it as an OVA) than a
        # Part 2; such a unit stays in review until more of Part 2 is watched.
        or len(second_numbers) < _SPLIT_COUR_MIN_OVERFLOW
    ):
        return [(state, target)]
    sequel_id = _direct_sequel_id(config, client, target.mal_anime_id)
    if sequel_id is None:
        return [(state, target)]
    sequel_detail = _catalog_detail(client, sequel_id)
    sequel_count = _coerce_detail_int((sequel_detail or {}).get("num_episodes"))
    if (
        sequel_detail is None
        or str(sequel_detail.get("media_type") or "") not in _SPLIT_COUR_MEDIA_TYPES
        or not sequel_count
        or max(second_numbers) > sequel_count
    ):
        return [(state, target)]
    max_episode = int(state.max_episode_number or max(numbers))
    # The derived parts are already in the chosen numbering; clearing the
    # alternative keeps a later numbering choice from swapping in the whole
    # unit's in-season counts.
    resolved_numbering: dict[str, Any] = {
        "ordinal_completed_episode_count": None,
        "ordinal_max_completed_episode_number": None,
        "ordinal_max_episode_number": None,
        "ordinal_min_episode_number": None,
        "ordinal_completed_episode_numbers": None,
    }
    units: list[tuple[ProviderSeriesState, int]] = []
    if first_numbers:
        units.append(
            (
                replace(
                    state,
                    completed_episode_count=len(first_numbers),
                    max_completed_episode_number=max(first_numbers),
                    max_episode_number=min(max_episode, first_count),
                    min_episode_number=1,
                    completed_episode_numbers=tuple(first_numbers),
                    # The unit's last watch may be a Part 2 episode; Part 1's
                    # finish date is unknown, so it is not filled.
                    last_watched_at=None,
                    **resolved_numbering,
                ),
                target.mal_anime_id,
            )
        )
    units.append(
        (
            replace(
                state,
                completed_episode_count=len(second_numbers),
                max_completed_episode_number=max(second_numbers),
                max_episode_number=max(0, max_episode - first_count),
                # Started-but-unfinished Part 2 rows are not known per part,
                # so the first completed Part 2 episode bounds the minimum:
                # completing Part 2 from its last episodes alone must still
                # rest on observed episodes (see _provider_watched_episodes).
                min_episode_number=min(second_numbers),
                completed_episode_numbers=tuple(second_numbers),
                # Provenance is one row per provider series; the first part keeps it.
                season_unit_rank=state.season_unit_rank + (1000 if first_numbers else 0),
                **resolved_numbering,
            ),
            sequel_id,
        )
    )
    split_reason = f"split_cour_overflow first_mal={target.mal_anime_id} sequel_mal={sequel_id} first_episodes={first_count}"
    return [
        (unit_state, _ApplyTarget(mal_id, target.mapping_source, target.confidence, target.reasons + [split_reason]))
        for unit_state, mal_id in units
    ]


def _revalidate_and_apply(
    config: AppConfig,
    client: MalClient,
    state: ProviderSeriesState,
    target: _ApplyTarget,
    *,
    base_reasons: list[str],
) -> ApplyResult:
    assert target.mal_anime_id is not None
    mal_anime_id = target.mal_anime_id

    def result(**kwargs: Any) -> ApplyResult:
        return ApplyResult(
            provider=state.provider,
            provider_series_id=state.provider_series_id,
            provider_season_key=state.provider_season_key,
            mal_anime_id=mal_anime_id,
            **kwargs,
        )

    def review_proposal(*, mal_title: str | None, current: dict[str, Any] | None, reasons: list[str]) -> SyncProposal:
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            provider_title=state.title,
            mapping_status="approved",
            confidence=target.confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current,
            proposed_my_list_status=None,
            decision="review",
            mapping_source=target.mapping_source,
            persisted_mapping_approved=True,
            completion_audit=state.completion_audit,
            reasons=reasons,
            provider=state.provider,
            provider_season_key=state.provider_season_key,
        )

    try:
        detail = client.get_anime_details(
            mal_anime_id,
            fields=_MAL_SYNC_DETAIL_FIELDS,
            force_refresh=True,
            require_user=True,
        )
    except MalApiError as exc:
        error_reasons = [*base_reasons, f"mal_live_state_revalidation_failed:{exc}"]
        _persist_watch_confirmation_provenance_snapshot(
            config, state, review_proposal(mal_title=None, current=None, reasons=error_reasons), detail=None, mapping_reasons=error_reasons
        )
        return result(mal_title=None, applied=False, proposal_decision="error", requested_status=None, response_status=None, reasons=error_reasons)

    live_status, validation_reason = _validated_live_mal_user_state(detail, expected_anime_id=mal_anime_id)
    if validation_reason is not None:
        error_reasons = [*base_reasons, "executor_revalidated_live_mal_state", validation_reason, "mal_live_user_state_invalid_fail_closed"]
        proposal = review_proposal(
            mal_title=_coerce_optional_str(detail.get("title")) if isinstance(detail, dict) else None,
            current=None,
            reasons=error_reasons,
        )
        _persist_watch_confirmation_provenance_snapshot(
            config, state, proposal, detail=detail if isinstance(detail, dict) else None, mapping_reasons=error_reasons
        )
        return result(mal_title=proposal.mal_title, applied=False, proposal_decision="error", requested_status=None, response_status=None, reasons=error_reasons)
    detail = {**detail, "my_list_status": live_status}
    evidence_reasons = _mal_user_list_cache_conflict_reasons(get_mal_user_anime_list_cache(config.db_path, mal_anime_id), live_status)
    if evidence_reasons:
        proposal = review_proposal(
            mal_title=_coerce_optional_str(detail.get("title")),
            current=live_status if isinstance(live_status, dict) else None,
            reasons=[*base_reasons, "executor_revalidated_live_mal_state", *evidence_reasons, "conflicting_mal_user_state_evidence_fail_closed"],
        )
        _persist_watch_confirmation_provenance_snapshot(config, state, proposal, detail=detail, mapping_reasons=proposal.reasons)
        return result(
            mal_title=proposal.mal_title,
            applied=False,
            proposal_decision="review",
            requested_status=None,
            response_status=proposal.current_my_list_status,
            reasons=proposal.reasons,
        )

    proposal = _plan_status_update(
        state,
        detail,
        mapping_status="approved",
        confidence=target.confidence,
        mapping_source=target.mapping_source,
        persisted_mapping_approved=True,
        extra_reasons=[*base_reasons, "executor_revalidated_live_mal_state"],
    )
    _persist_watch_confirmation_provenance_snapshot(config, state, proposal, detail=detail, mapping_reasons=proposal.reasons)
    if proposal.decision != "propose_update":
        return result(
            mal_title=proposal.mal_title,
            applied=False,
            proposal_decision=proposal.decision,
            requested_status=proposal.proposed_my_list_status,
            response_status=proposal.current_my_list_status,
            reasons=proposal.reasons,
        )
    requested = proposal.proposed_my_list_status or {}
    reasons = list(proposal.reasons)
    try:
        response = client.update_my_list_status(
            mal_anime_id,
            status=str(requested["status"]),
            num_watched_episodes=int(requested.get("num_watched_episodes") or 0),
            score=_coerce_optional_int(requested.get("score")),
            start_date=_coerce_optional_str(requested.get("start_date")),
            finish_date=_coerce_optional_str(requested.get("finish_date")),
        )
        response_status = _validated_mal_write_result(response, requested=requested)
    except MalApiError as exc:
        reasons.append(f"mal_update_failed:{exc}")
        return result(
            mal_title=proposal.mal_title,
            applied=False,
            proposal_decision="error",
            requested_status=proposal.proposed_my_list_status,
            response_status=None,
            reasons=reasons,
        )
    reconcile_mal_user_state_after_write(
        config.db_path, mal_anime_id=mal_anime_id, list_status=response_status, title=proposal.mal_title
    )
    reasons.extend(["applied_to_mal", "reconciled_local_mal_user_state_caches"])
    return result(
        mal_title=proposal.mal_title,
        applied=True,
        proposal_decision=proposal.decision,
        requested_status=proposal.proposed_my_list_status,
        response_status=response_status,
        reasons=reasons,
    )


def _safe_mal_list_status_result(response: dict[str, Any]) -> dict[str, Any]:
    """Keep task output/cache reconciliation to validated, non-textual sync fields."""
    allowed_fields = (
        "status",
        "num_episodes_watched",
        "score",
        "start_date",
        "finish_date",
        "updated_at",
    )
    return {field: response[field] for field in allowed_fields if field in response}


def _validated_live_mal_user_state(
    detail: Any,
    *,
    expected_anime_id: int,
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate the authenticated detail shape before it can authorize a write."""
    if not isinstance(detail, dict):
        return None, "mal_live_detail_invalid:not_object"
    anime_id = detail.get("id")
    if isinstance(anime_id, bool) or not isinstance(anime_id, int) or anime_id != int(expected_anime_id):
        return None, "mal_live_detail_invalid:anime_id_mismatch_or_missing"
    if not isinstance(detail.get("title"), str) or not detail["title"].strip():
        return None, "mal_live_detail_invalid:title_missing_or_invalid"
    if "my_list_status" not in detail:
        return None, "mal_live_detail_invalid:my_list_status_missing_or_invalid"
    if detail["my_list_status"] is None:
        return None, None
    if not isinstance(detail["my_list_status"], dict):
        return None, "mal_live_detail_invalid:my_list_status_missing_or_invalid"
    status = detail["my_list_status"]
    list_status = status.get("status")
    if not isinstance(list_status, str) or list_status not in MAL_USER_LIST_STATUSES:
        return None, "mal_live_detail_invalid:my_list_status_status_invalid"
    watched = status.get("num_episodes_watched")
    if isinstance(watched, bool) or not isinstance(watched, int) or watched < 0:
        return None, "mal_live_detail_invalid:my_list_status_progress_invalid"
    score = status.get("score")
    if score is not None and (isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 10):
        return None, "mal_live_detail_invalid:my_list_status_score_invalid"
    for field_name in ("start_date", "finish_date", "updated_at"):
        if field_name in status and status[field_name] is not None and not isinstance(status[field_name], str):
            return None, f"mal_live_detail_invalid:my_list_status_{field_name}_invalid"
    return _safe_mal_list_status_result(status), None


def _validated_mal_write_result(response: Any, *, requested: dict[str, Any]) -> dict[str, Any]:
    """Require the PUT response to confirm the requested status and progress."""
    if not isinstance(response, dict):
        raise MalApiError("MAL update response is not an object")
    status = response.get("status")
    watched = response.get("num_episodes_watched")
    if not isinstance(status, str) or status not in MAL_USER_LIST_STATUSES:
        raise MalApiError("MAL update response has invalid status")
    if isinstance(watched, bool) or not isinstance(watched, int) or watched < 0:
        raise MalApiError("MAL update response has invalid progress")
    if status != requested.get("status") or watched != int(requested.get("num_watched_episodes") or 0):
        raise MalApiError("MAL update response does not confirm requested status and progress")
    score = response.get("score")
    if score is not None and (isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 10):
        raise MalApiError("MAL update response has invalid score")
    for field_name in ("start_date", "finish_date", "updated_at"):
        if field_name in response and response[field_name] is not None and not isinstance(response[field_name], str):
            raise MalApiError(f"MAL update response has invalid {field_name}")
    return _safe_mal_list_status_result(response)


def _mal_user_list_cache_conflict_reasons(
    cached: MalUserAnimeListCacheEntry | None,
    live_status: dict[str, Any] | None,
) -> list[str]:
    """Return only fail-closed conflicts; a successful live read remains authoritative."""
    if cached is None:
        return []
    cached_watched = int(cached.num_episodes_watched or 0)
    live_watched = int((live_status or {}).get("num_episodes_watched") or 0)
    cached_status = _coerce_optional_str(cached.list_status)
    live_list_status = _coerce_optional_str((live_status or {}).get("status"))
    reasons: list[str] = []
    if cached_watched > live_watched:
        reasons.append(f"mal_user_list_cache_progress_ahead_of_live_detail cached={cached_watched} live={live_watched}")
    if cached_status == "completed" and live_list_status != "completed":
        reasons.append(
            "mal_user_list_cache_completed_conflicts_with_live_detail "
            f"cached={cached_status} live={live_list_status or 'missing'}"
        )
    return reasons


def _resolve_mapping_for_sync(
    config: AppConfig,
    client: MalClient,
    state: ProviderSeriesState,
    persisted: PersistedSeriesMapping | None,
    *,
    mapping_limit: int,
    allow_live_search: bool,
) -> tuple[str, float, int | None, str | None, bool, list[str]]:
    if persisted and persisted.approved_by_user:
        return (
            "approved",
            float(persisted.confidence or 1.0),
            persisted.mal_anime_id,
            persisted.mapping_source,
            True,
            ["using_user_approved_mapping"],
        )
    if not allow_live_search:
        return ("unapproved", 0.0, None, persisted.mapping_source if persisted else None, False, ["live_search_disabled"])

    mapping = map_series(client, _build_series_mapping_input(state), limit=mapping_limit)
    mapping_reasons = list(mapping.rationale)
    mapping_source: str | None = persisted.mapping_source if persisted else None
    if persisted:
        mapping_reasons.append(
            f"existing_mapping={persisted.mal_anime_id}:{persisted.mapping_source}:approved={int(persisted.approved_by_user)}"
        )
    if mapping.is_deterministic_multi_entry_bundle():
        mapping_reasons.append("auto_classified_multi_entry_bundle_non_actionable")
        return (mapping.status, mapping.confidence, None, mapping_source, False, mapping_reasons)
    if mapping.has_deterministic_aggregate_progress_classification():
        mapping_reasons.append("auto_classified_aggregate_progress_non_actionable")
        return (mapping.status, mapping.confidence, None, mapping_source, False, mapping_reasons)
    if mapping.has_deterministic_ambiguous_exact_title_classification():
        mapping_reasons.append("auto_classified_ambiguous_exact_title_non_actionable")
        return (mapping.status, mapping.confidence, None, mapping_source, False, mapping_reasons)
    if mapping.has_provider_aggregate_shell_protection():
        if mapping.has_deterministic_provider_shell_classification():
            mapping_reasons.append("auto_classified_provider_shell_non_actionable")
        mapping_reasons.append("provider_aggregate_shell_non_actionable")
        return (mapping.status, mapping.confidence, None, mapping_source, False, mapping_reasons)
    if should_auto_approve_mapping(mapping) and mapping.chosen_candidate:
        persisted = _auto_approve_mapping(config, state, mapping.confidence, mapping.chosen_candidate.mal_anime_id)
        mapping_reasons.append("auto_approved_exact_unique_match")
        return (
            "approved",
            mapping.confidence,
            persisted.mal_anime_id,
            persisted.mapping_source,
            True,
            mapping_reasons,
        )
    if mapping.status not in {"exact", "strong"} or not mapping.chosen_candidate:
        if mapping.candidates:
            mapping_reasons.append(
                "top_candidates="
                + ", ".join(f"{candidate.mal_anime_id}:{candidate.title}:{candidate.score:.3f}" for candidate in mapping.candidates[:3])
            )
        return (mapping.status, mapping.confidence, None, mapping_source, False, mapping_reasons)
    return (mapping.status, mapping.confidence, mapping.chosen_candidate.mal_anime_id, "live_search", False, mapping_reasons)


def _plan_status_update(
    state: ProviderSeriesState,
    detail: dict[str, Any],
    mapping_status: str,
    confidence: float,
    *,
    mapping_source: str | None,
    persisted_mapping_approved: bool,
    extra_reasons: list[str] | None = None,
) -> SyncProposal:
    proposal = _plan_status_update_for_state(
        state,
        detail,
        mapping_status,
        confidence,
        mapping_source=mapping_source,
        persisted_mapping_approved=persisted_mapping_approved,
        extra_reasons=extra_reasons,
    )
    proposal.provider_season_key = state.provider_season_key
    return proposal


def _plan_status_update_for_state(
    state: ProviderSeriesState,
    detail: dict[str, Any],
    mapping_status: str,
    confidence: float,
    *,
    mapping_source: str | None,
    persisted_mapping_approved: bool,
    extra_reasons: list[str] | None = None,
) -> SyncProposal:
    mal_title = str(detail.get("title") or "")
    mal_anime_id = int(detail["id"])
    current_status = detail.get("my_list_status") or None
    num_episodes = detail.get("num_episodes")
    # Report the episode counts in the numbering the decision uses.
    state = _numbering_view(
        state,
        _coerce_detail_int(num_episodes),
        current_watched=_coerce_optional_int((current_status or {}).get("num_episodes_watched")),
    )
    provider_watched_episodes, watched_reason, watched_review_reason = _provider_watched_episodes(
        state,
        _coerce_detail_int(num_episodes),
        current_watched=_coerce_detail_int((current_status or {}).get("num_episodes_watched")),
    )
    reasons: list[str] = [
        "merge_policy=missing_data_only",
        "completion_policy=ratio>=0.95_or_remaining<=120s_or_later_episode_progress_with_ratio>=0.85",
        "progress_dedup=distinct_episode_number_when_available",
        "missing_meaningful_rules=status:none_missing;progress:missing_only_when_status_absent;score:0_or_null_missing;dates:null_or_empty_missing",
        f"completed_episode_count={state.completed_episode_count}",
        f"max_completed_episode_number={state.max_completed_episode_number}",
        f"progress_rows={state.progress_rows}",
        "completion_audit="
        + ",".join(
            [
                f"ratio={state.completion_audit.get('completed_by', {}).get('ratio_threshold', 0)}",
                f"credits={state.completion_audit.get('completed_by', {}).get('credits_window', 0)}",
                f"follow_on={state.completion_audit.get('completed_by', {}).get('later_episode_evidence', 0)}",
                f"history_unknown={state.completion_audit.get('completion_uncertain_by', {}).get('completion_unknown_history_membership', 0)}",
                f"legacy_hidive_unproven={state.completion_audit.get('completion_uncertain_by', {}).get('legacy_unproven_hidive_synthetic_completion', 0)}",
            ]
        ),
    ]
    for uncertainty_reason, count in state.completion_audit.get("completion_uncertain_by", {}).items():
        if count:
            reasons.append(f"{uncertainty_reason}={count}")
    if extra_reasons:
        reasons = list(extra_reasons) + reasons
    if state.provider_season_key is not None:
        reasons.append(f"provider_season_key={state.provider_season_key}")
    if state.completed_unnumbered_count:
        reasons.append(f"unnumbered_items_not_counted={state.completed_unnumbered_count}")
    if watched_reason:
        reasons.append(watched_reason)
    if watched_review_reason:
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            provider_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="review",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            completion_audit=state.completion_audit,
            reasons=reasons + [watched_review_reason],
            provider=state.provider,
        )

    proposed_status: dict[str, Any] | None = None
    if provider_watched_episodes > 0:
        proposed_status = {
            "status": "watching",
            "num_watched_episodes": provider_watched_episodes,
        }
        reasons.append("provider_completed_episode_evidence_present")
        if num_episodes and provider_watched_episodes >= int(num_episodes):
            proposed_status["status"] = "completed"
            reasons.append("provider_completion_reached_known_episode_count")
    elif state.progress_rows > 0:
        reasons.append("partial_provider_activity_without_completed_episode")
    elif state.watchlist_status:
        proposed_status = {"status": "plan_to_watch", "num_watched_episodes": 0}
        reasons.append("watchlist_only")

    if proposed_status is None:
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            provider_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="skip",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            completion_audit=state.completion_audit,
            reasons=reasons + ["no_actionable_provider_state"],
            provider=state.provider,
        )

    current_watched = int((current_status or {}).get("num_episodes_watched") or 0)
    current_list_status = _coerce_optional_str((current_status or {}).get("status"))
    proposed_watched = int(proposed_status.get("num_watched_episodes") or 0)

    if (
        current_list_status not in (None, "plan_to_watch")
        and proposed_watched == 0
        and proposed_status["status"] == "plan_to_watch"
    ):
        # A provider watchlist entry only ever adds a missing title; it never
        # competes with a status the user already set on MAL.
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            provider_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="skip",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            completion_audit=state.completion_audit,
            reasons=reasons + ["mal_entry_exists_watchlist_only"],
            provider=state.provider,
        )

    if current_list_status == "plan_to_watch" and proposed_watched > 0:
        reasons.append("override_plan_to_watch_due_to_provider_watch_evidence")

    allowed_forward_status_changes = {
        ("plan_to_watch", "watching"),
        ("plan_to_watch", "completed"),
        ("watching", "completed"),
    }
    proposed_list_status = str(proposed_status["status"])
    if (
        current_list_status
        and current_list_status != "completed"
        and proposed_list_status != current_list_status
        and (current_list_status, proposed_list_status) not in allowed_forward_status_changes
    ):
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            provider_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="review",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            completion_audit=state.completion_audit,
            reasons=reasons
            + [f"refusing_destructive_mal_status_change current={current_list_status} proposed={proposed_list_status}"],
            provider=state.provider,
        )

    if current_watched > proposed_watched:
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            provider_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="skip",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            completion_audit=state.completion_audit,
            reasons=reasons + [f"refusing_to_decrease_mal_progress current={current_watched} proposed={proposed_watched}"],
            provider=state.provider,
        )

    if current_watched == proposed_watched and current_list_status == proposed_status["status"]:
        field_merge = _build_missing_field_merge(state, current_status, proposed_status["status"], current_watched)
        if not field_merge:
            return SyncProposal(
                provider_series_id=state.provider_series_id,
                provider_title=state.title,
                mapping_status=mapping_status,
                confidence=confidence,
                mal_anime_id=mal_anime_id,
                mal_title=mal_title,
                current_my_list_status=current_status,
                proposed_my_list_status=None,
                decision="skip",
                mapping_source=mapping_source,
                persisted_mapping_approved=persisted_mapping_approved,
                completion_audit=state.completion_audit,
                reasons=reasons + ["mal_already_matches_or_exceeds_proposal"],
                provider=state.provider,
            )
        proposed_status = {
            "status": proposed_status["status"],
            "num_watched_episodes": proposed_watched,
        }
        proposed_status.update(field_merge)
        reasons.extend(_summarize_field_merge(field_merge))
    else:
        field_merge = _build_missing_field_merge(state, current_status, proposed_status["status"], proposed_watched)
        proposed_status.update(field_merge)
        reasons.extend(_summarize_field_merge(field_merge))

    if current_list_status == "completed" and proposed_status["status"] != "completed":
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            provider_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="skip",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            completion_audit=state.completion_audit,
            reasons=reasons + ["refusing_to_downgrade_completed_mal_entry"],
            provider=state.provider,
        )

    if current_status:
        reasons.append("would_update_existing_mal_entry")
    else:
        reasons.append("would_create_new_mal_entry")

    return SyncProposal(
        provider_series_id=state.provider_series_id,
        provider_title=state.title,
        mapping_status=mapping_status,
        confidence=confidence,
        mal_anime_id=mal_anime_id,
        mal_title=mal_title,
        current_my_list_status=current_status,
        proposed_my_list_status=proposed_status,
        decision="propose_update",
        mapping_source=mapping_source,
        persisted_mapping_approved=persisted_mapping_approved,
        completion_audit=state.completion_audit,
        reasons=reasons,
        provider=state.provider,
    )


def _season_numbering_hints(
    targets: list[tuple[ProviderSeriesState, int | None]]
) -> dict[tuple[str, str, str | None], str]:
    """Derive per-season numbering from how sibling seasons resolved.

    If the nearest earlier provider season of the same series resolved to the
    same MAL entry, that entry spans provider seasons and absolute numbers
    apply; a different entry means each season is its own MAL entry. Several
    units can share a season number (e.g. dub and sub seasons); the hint is
    given only when every unit of the nearest earlier season agrees.
    """
    by_series: dict[tuple[str, str], list[tuple[ProviderSeriesState, int]]] = {}
    for state, mal_id in targets:
        if state.season_split and mal_id is not None and state.season_number is not None:
            by_series.setdefault((state.provider, state.provider_series_id), []).append((state, int(mal_id)))
    hints: dict[tuple[str, str, str | None], str] = {}
    for units in by_series.values():
        targets_by_season: dict[int, set[int]] = {}
        for state, mal_id in units:
            targets_by_season.setdefault(int(state.season_number or 0), set()).add(mal_id)
        for state, mal_id in units:
            earlier = [number for number in targets_by_season if number < int(state.season_number or 0)]
            if not earlier:
                continue
            previous = targets_by_season[max(earlier)]
            key = (state.provider, state.provider_series_id, state.provider_season_key)
            if previous == {mal_id}:
                hints[key] = "absolute"
            elif mal_id not in previous:
                hints[key] = "ordinal"
    return hints


def _numbering_view(
    state: ProviderSeriesState, mal_num_episodes: int | None, *, current_watched: int | None = None
) -> ProviderSeriesState:
    """Choose absolute or in-season episode numbering for one MAL target.

    Provider episode numbers can be absolute across a series (Attack on Titan
    Final Season is 60-87) while MAL lists that season separately, or one MAL
    entry can span several provider seasons with continuous numbering
    (D.Gray-man, 103 episodes over four provider seasons). The absolute
    numbers are used when they fit within the MAL entry; otherwise the
    position within the season is. With an unknown MAL count the in-season
    position is used: it can only under-report, and writes never decrease.
    """
    if state.ordinal_completed_episode_count is None:
        return state
    absolute_max = int(state.max_completed_episode_number or state.max_episode_number or 0)
    if state.numbering_hint == "absolute":
        return state
    if state.numbering_hint is None:
        if mal_num_episodes and mal_num_episodes > 0:
            if absolute_max and absolute_max <= mal_num_episodes:
                return state
        elif (
            current_watched is not None
            and state.min_episode_number is not None
            and int(state.min_episode_number) > 1
            and current_watched >= int(state.min_episode_number) - 1
            and current_watched > int(state.ordinal_max_completed_episode_number or 0)
        ):
            # Unknown MAL count (still airing): MAL already records the
            # episodes before this season, so the entry spans provider seasons.
            return state
    return replace(
        state,
        completed_episode_count=state.ordinal_completed_episode_count,
        max_completed_episode_number=state.ordinal_max_completed_episode_number,
        max_episode_number=state.ordinal_max_episode_number,
        min_episode_number=state.ordinal_min_episode_number,
        completed_episode_numbers=state.ordinal_completed_episode_numbers or (),
        ordinal_completed_episode_count=None,
        ordinal_max_completed_episode_number=None,
        ordinal_max_episode_number=None,
        ordinal_min_episode_number=None,
        ordinal_completed_episode_numbers=None,
    )


def _provider_watched_episodes(
    state: ProviderSeriesState, mal_num_episodes: int | None, *, current_watched: int | None = None
) -> tuple[int, str | None, str | None]:
    """Return (watched, info_reason, review_reason) for a MAL entry of known size.

    The highest completed episode number stands in for episodes watched
    elsewhere, but provider numbering can continue across seasons (a second
    season numbered 13-24). When that exceeds the MAL episode count the count
    is rebased on the first episode number seen; evidence that still exceeds
    MAL's count points at a wrong mapping or aggregation and goes to review,
    as does a completion that rests only on unobserved lower-numbered
    episodes. Numbers within MAL's count are not rebased: the same numbering
    is correct for a MAL entry spanning several provider seasons (e.g. a
    long-running series MAL lists as one entry).
    """
    state = _numbering_view(state, mal_num_episodes, current_watched=current_watched)
    count = state.completed_episode_count
    max_number = int(state.max_completed_episode_number or 0)
    min_number = int(state.min_episode_number or 1)
    watched = max(count, max_number)
    if not mal_num_episodes or mal_num_episodes <= 0 or watched < mal_num_episodes:
        return watched, None, None
    if watched == mal_num_episodes:
        if count < mal_num_episodes and min_number > 1 and (current_watched or 0) < min_number - 1:
            # Completion would rest on unobserved episodes below the first
            # observed number. With numbering that continues from an earlier
            # season (season 2 = 13-25 on a 13-episode MAL entry) episode 13
            # is the first episode, not the last, so do not claim completion
            # unless MAL already records the episodes before it. The distinct
            # count of observed completed episodes is right under either
            # reading, so that is what is written.
            return count, (
                f"provider_completion_rests_on_unobserved_episodes_counted_observed min_episode={min_number} "
                f"max_completed_episode={max_number} completed={count} mal_num_episodes={mal_num_episodes}"
            ), None
        return watched, None, None
    if count > mal_num_episodes:
        return mal_num_episodes, None, (
            f"provider_completed_episodes_exceed_mal_episode_count completed={count} mal_num_episodes={mal_num_episodes}"
        )
    offset = max(0, min_number - 1)
    rebased = max(count, max_number - offset)
    if rebased > mal_num_episodes:
        return mal_num_episodes, None, (
            f"provider_episode_numbering_exceeds_mal_episode_count max_completed_episode={max_number} "
            f"mal_num_episodes={mal_num_episodes}"
        )
    return rebased, f"provider_episode_numbering_rebased offset={offset}", None


def _build_missing_field_merge(
    state: ProviderSeriesState,
    current_status: dict[str, Any] | None,
    derived_status: str,
    derived_watched: int,
) -> dict[str, Any]:
    current = current_status or {}
    merged: dict[str, Any] = {}

    if _is_missing_mal_status(current.get("status")):
        merged["status"] = derived_status
    if _is_missing_mal_progress(current):
        merged["num_watched_episodes"] = derived_watched
    if _is_missing_mal_score(current.get("score")):
        # Provider snapshots do not currently provide a trustworthy rating for sync planning.
        pass
    if derived_status == "completed" and _is_missing_mal_date(current.get("finish_date")) and state.last_watched_at:
        merged["finish_date"] = _iso_datetime_to_date(state.last_watched_at)
    if derived_watched > 0 and _is_missing_mal_date(current.get("start_date")):
        # We only know last_watched_at today, not the true start date, so do not guess.
        pass
    return merged


def _summarize_field_merge(field_merge: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for field in ("status", "num_watched_episodes", "score", "start_date", "finish_date"):
        if field in field_merge:
            reasons.append(f"fill_missing_{field}")
        else:
            reasons.append(f"preserve_meaningful_{field}")
    return reasons


def _is_missing_mal_status(value: Any) -> bool:
    return _coerce_optional_str(value) is None


def _is_missing_mal_progress(current_status: dict[str, Any]) -> bool:
    status = _coerce_optional_str(current_status.get("status"))
    if status is None:
        return True
    return False


def _is_missing_mal_score(value: Any) -> bool:
    score = _coerce_optional_int(value)
    return score is None or score <= 0


def _is_missing_mal_date(value: Any) -> bool:
    return _coerce_optional_str(value) is None


def _iso_datetime_to_date(value: str) -> str:
    return value[:10]


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
