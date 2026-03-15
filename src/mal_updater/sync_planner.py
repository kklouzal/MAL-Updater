from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config import AppConfig, load_mal_secrets
from .db import PersistedSeriesMapping, connect, get_series_mapping, replace_review_queue_entries
from .mal_client import MalClient
from .mapping import SeriesMappingInput, map_series


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


@dataclass(slots=True)
class EpisodeProgressState:
    provider_episode_id: str
    episode_number: int | None
    completion_ratio: float | None
    playback_position_ms: int | None
    duration_ms: int | None
    last_watched_at: str | None


@dataclass(slots=True)
class SyncProposal:
    provider_series_id: str
    crunchyroll_title: str
    mapping_status: str
    confidence: float
    mal_anime_id: int | None
    mal_title: str | None
    current_my_list_status: dict[str, Any] | None
    proposed_my_list_status: dict[str, Any] | None
    decision: str
    mapping_source: str | None = None
    persisted_mapping_approved: bool = False
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider_series_id": self.provider_series_id,
            "crunchyroll_title": self.crunchyroll_title,
            "mapping_status": self.mapping_status,
            "confidence": self.confidence,
            "mal_anime_id": self.mal_anime_id,
            "mal_title": self.mal_title,
            "current_my_list_status": self.current_my_list_status,
            "proposed_my_list_status": self.proposed_my_list_status,
            "decision": self.decision,
            "mapping_source": self.mapping_source,
            "persisted_mapping_approved": self.persisted_mapping_approved,
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
    reasons: list[str] = field(default_factory=list)
    candidates: list[dict[str, Any]] = field(default_factory=list)

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
            "reasons": self.reasons,
            "candidates": self.candidates,
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "provider_series_id": self.provider_series_id,
            "mal_anime_id": self.mal_anime_id,
            "mal_title": self.mal_title,
            "applied": self.applied,
            "proposal_decision": self.proposal_decision,
            "requested_status": self.requested_status,
            "response_status": self.response_status,
            "reasons": self.reasons,
        }


def load_provider_series_states(config: AppConfig, limit: int | None = None, provider: str = "crunchyroll") -> list[ProviderSeriesState]:
    series_query = """
        SELECT
            s.provider,
            s.provider_series_id,
            s.title,
            s.season_title,
            s.season_number,
            s.last_seen_at,
            w.status AS watchlist_status
        FROM provider_series s
        LEFT JOIN provider_watchlist w
            ON w.provider = s.provider AND w.provider_series_id = s.provider_series_id
        WHERE s.provider = ?
        ORDER BY s.title ASC
    """
    progress_query = """
        SELECT
            provider_series_id,
            provider_episode_id,
            episode_number,
            completion_ratio,
            playback_position_ms,
            duration_ms,
            last_watched_at
        FROM provider_episode_progress
        WHERE provider = ?
        ORDER BY provider_series_id ASC, episode_number ASC, last_watched_at ASC, provider_episode_id ASC
    """
    with connect(config.db_path) as conn:
        series_rows = conn.execute(series_query, [provider]).fetchall()
        progress_rows = conn.execute(progress_query, [provider]).fetchall()

    progress_by_series: dict[str, list[EpisodeProgressState]] = {}
    for row in progress_rows:
        progress_by_series.setdefault(row["provider_series_id"], []).append(
            EpisodeProgressState(
                provider_episode_id=row["provider_episode_id"],
                episode_number=row["episode_number"],
                completion_ratio=row["completion_ratio"],
                playback_position_ms=row["playback_position_ms"],
                duration_ms=row["duration_ms"],
                last_watched_at=row["last_watched_at"],
            )
        )

    states: list[tuple[str | None, str, ProviderSeriesState]] = []
    for row in series_rows:
        series_progress = progress_by_series.get(row["provider_series_id"], [])
        summary = _summarize_episode_progress(series_progress, config)
        sort_key = summary["last_watched_at"] or row["last_seen_at"]
        states.append(
            (
                sort_key,
                row["title"],
                ProviderSeriesState(
                    provider=row["provider"],
                    provider_series_id=row["provider_series_id"],
                    title=row["title"],
                    season_title=row["season_title"],
                    season_number=row["season_number"],
                    progress_rows=summary["progress_rows"],
                    max_episode_number=summary["max_episode_number"],
                    completed_episode_count=summary["completed_episode_count"],
                    max_completed_episode_number=summary["max_completed_episode_number"],
                    watchlist_status=row["watchlist_status"],
                    last_watched_at=summary["last_watched_at"],
                ),
            )
        )

    states.sort(key=lambda item: (item[0] is not None, item[0] or "", item[1]), reverse=True)
    ordered = [item[2] for item in states]
    if limit is not None:
        return ordered[:limit]
    return ordered


def _summarize_episode_progress(rows: list[EpisodeProgressState], config: AppConfig) -> dict[str, Any]:
    completed_episode_numbers: set[int] = set()
    completed_episode_ids_without_number: set[str] = set()
    max_episode_number: int | None = None
    max_completed_episode_number: int | None = None
    last_watched_at: str | None = None

    for row in rows:
        if row.episode_number is not None:
            max_episode_number = row.episode_number if max_episode_number is None else max(max_episode_number, row.episode_number)
        if row.last_watched_at and (last_watched_at is None or row.last_watched_at > last_watched_at):
            last_watched_at = row.last_watched_at
        if not _episode_counts_as_completed(row, rows, config):
            continue
        if row.episode_number is not None:
            completed_episode_numbers.add(row.episode_number)
            max_completed_episode_number = (
                row.episode_number if max_completed_episode_number is None else max(max_completed_episode_number, row.episode_number)
            )
        else:
            completed_episode_ids_without_number.add(row.provider_episode_id)

    return {
        "progress_rows": len(rows),
        "max_episode_number": max_episode_number,
        "completed_episode_count": len(completed_episode_numbers) + len(completed_episode_ids_without_number),
        "max_completed_episode_number": max_completed_episode_number,
        "last_watched_at": last_watched_at,
    }


def _episode_counts_as_completed(row: EpisodeProgressState, all_rows: list[EpisodeProgressState], config: AppConfig) -> bool:
    completion_ratio = row.completion_ratio
    if completion_ratio is not None and completion_ratio >= config.completion_threshold:
        return True

    remaining_ms = _remaining_ms(row)
    if remaining_ms is not None and 0 <= remaining_ms <= config.credits_skip_window_seconds * 1000:
        return True

    if completion_ratio is None or completion_ratio < 0.85:
        return False
    if row.episode_number is None or row.last_watched_at is None:
        return False

    return any(
        later.episode_number is not None
        and later.episode_number > row.episode_number
        and later.last_watched_at is not None
        and later.last_watched_at > row.last_watched_at
        for later in all_rows
    )


def _remaining_ms(row: EpisodeProgressState) -> int | None:
    if row.duration_ms is None or row.playback_position_ms is None:
        return None
    return row.duration_ms - row.playback_position_ms


def build_mapping_review(config: AppConfig, limit: int | None = 20, mapping_limit: int = 5) -> list[MappingReviewItem]:
    states = load_provider_series_states(config, limit=limit)
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

        mapping = map_series(
            client,
            SeriesMappingInput(
                provider=state.provider,
                provider_series_id=state.provider_series_id,
                title=state.title,
                season_title=state.season_title,
                season_number=state.season_number,
                max_episode_number=state.max_episode_number,
                completed_episode_count=state.completed_episode_count,
            ),
            limit=mapping_limit,
        )
        reasons = list(mapping.rationale)
        if existing:
            reasons.append(
                f"existing_mapping={existing.mal_anime_id}:{existing.mapping_source}:approved={int(existing.approved_by_user)}"
            )
        if mapping.status in {"exact", "strong"} and mapping.chosen_candidate:
            decision = "ready_for_approval"
        elif mapping.status == "no_candidates":
            decision = "needs_manual_match"
        else:
            decision = "needs_review"
        items.append(
            MappingReviewItem(
                provider=state.provider,
                provider_series_id=state.provider_series_id,
                title=state.title,
                season_title=state.season_title,
                existing_mapping=existing,
                suggested_mal_anime_id=mapping.chosen_candidate.mal_anime_id if mapping.chosen_candidate else None,
                suggested_mal_title=mapping.chosen_candidate.title if mapping.chosen_candidate else None,
                mapping_status=mapping.status,
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
            )
        )
    return items


def persist_mapping_review_queue(config: AppConfig, items: list[MappingReviewItem]) -> dict[str, int]:
    queue_entries = []
    for item in items:
        if item.decision in {"preserved", "ready_for_approval"}:
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


def build_dry_run_sync_plan(
    config: AppConfig,
    limit: int | None = 20,
    mapping_limit: int = 5,
    approved_mappings_only: bool = False,
) -> list[SyncProposal]:
    states = load_provider_series_states(config, limit=limit)
    client = MalClient(config, load_mal_secrets(config))
    proposals: list[SyncProposal] = []
    for state in states:
        persisted = get_series_mapping(config.db_path, state.provider, state.provider_series_id)
        if approved_mappings_only and not (persisted and persisted.approved_by_user):
            proposals.append(
                SyncProposal(
                    provider_series_id=state.provider_series_id,
                    crunchyroll_title=state.title,
                    mapping_status="unapproved",
                    confidence=0.0,
                    mal_anime_id=persisted.mal_anime_id if persisted else None,
                    mal_title=None,
                    current_my_list_status=None,
                    proposed_my_list_status=None,
                    decision="review",
                    mapping_source=persisted.mapping_source if persisted else None,
                    persisted_mapping_approved=False,
                    reasons=["approved_mappings_only_enabled", "no_user_approved_mapping"],
                )
            )
            continue

        mapping_status, confidence, chosen_anime_id, mapping_source, approved, mapping_reasons = _resolve_mapping_for_sync(
            config,
            client,
            state,
            persisted,
            mapping_limit=mapping_limit,
            allow_live_search=not approved_mappings_only,
        )
        if chosen_anime_id is None:
            proposals.append(
                SyncProposal(
                    provider_series_id=state.provider_series_id,
                    crunchyroll_title=state.title,
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
                )
            )
            continue

        detail = client.get_anime_details(
            chosen_anime_id,
            fields="id,title,num_episodes,media_type,status,my_list_status,alternative_titles",
        )
        proposals.append(
            _plan_status_update(
                state,
                detail,
                mapping_status,
                confidence,
                mapping_source=mapping_source,
                persisted_mapping_approved=approved,
                extra_reasons=mapping_reasons,
            )
        )
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
                "provider": "crunchyroll",
                "provider_series_id": proposal.provider_series_id,
                "severity": severity,
                "payload": proposal.as_dict(),
            }
        )
    return replace_review_queue_entries(config.db_path, issue_type="sync_review", entries=queue_entries)


def execute_approved_sync(
    config: AppConfig,
    limit: int | None = 20,
    mapping_limit: int = 5,
    dry_run: bool = True,
) -> list[ApplyResult]:
    states = load_provider_series_states(config, limit=limit)
    client = MalClient(config, load_mal_secrets(config))
    results: list[ApplyResult] = []
    for state in states:
        persisted = get_series_mapping(config.db_path, state.provider, state.provider_series_id)
        if not (persisted and persisted.approved_by_user):
            continue
        detail = client.get_anime_details(
            persisted.mal_anime_id,
            fields="id,title,num_episodes,media_type,status,my_list_status,alternative_titles",
        )
        proposal = _plan_status_update(
            state,
            detail,
            mapping_status="approved",
            confidence=float(persisted.confidence or 1.0),
            mapping_source=persisted.mapping_source,
            persisted_mapping_approved=True,
            extra_reasons=["using_user_approved_mapping", "executor_revalidated_live_mal_state"],
        )
        if proposal.decision != "propose_update":
            results.append(
                ApplyResult(
                    provider=state.provider,
                    provider_series_id=state.provider_series_id,
                    mal_anime_id=persisted.mal_anime_id,
                    mal_title=proposal.mal_title,
                    applied=False,
                    proposal_decision=proposal.decision,
                    requested_status=proposal.proposed_my_list_status,
                    response_status=proposal.current_my_list_status,
                    reasons=proposal.reasons,
                )
            )
            continue
        response_status = proposal.current_my_list_status
        applied = False
        reasons = list(proposal.reasons)
        if dry_run:
            reasons.append("executor_dry_run")
        else:
            requested = proposal.proposed_my_list_status or {}
            response = client.update_my_list_status(
                persisted.mal_anime_id,
                status=str(requested["status"]),
                num_watched_episodes=int(requested.get("num_watched_episodes") or 0),
                score=_coerce_optional_int(requested.get("score")),
                start_date=_coerce_optional_str(requested.get("start_date")),
                finish_date=_coerce_optional_str(requested.get("finish_date")),
            )
            response_status = response
            applied = True
            reasons.append("applied_to_mal")
        results.append(
            ApplyResult(
                provider=state.provider,
                provider_series_id=state.provider_series_id,
                mal_anime_id=persisted.mal_anime_id,
                mal_title=proposal.mal_title,
                applied=applied,
                proposal_decision=proposal.decision,
                requested_status=proposal.proposed_my_list_status,
                response_status=response_status,
                reasons=reasons,
            )
        )
    return results


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

    mapping = map_series(
        client,
        SeriesMappingInput(
            provider=state.provider,
            provider_series_id=state.provider_series_id,
            title=state.title,
            season_title=state.season_title,
            season_number=state.season_number,
            max_episode_number=state.max_episode_number,
            completed_episode_count=state.completed_episode_count,
        ),
        limit=mapping_limit,
    )
    mapping_reasons = list(mapping.rationale)
    mapping_source: str | None = persisted.mapping_source if persisted else None
    if persisted:
        mapping_reasons.append(
            f"existing_mapping={persisted.mal_anime_id}:{persisted.mapping_source}:approved={int(persisted.approved_by_user)}"
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
    mal_title = str(detail.get("title") or "")
    mal_anime_id = int(detail["id"])
    current_status = detail.get("my_list_status") or None
    num_episodes = detail.get("num_episodes")
    crunchyroll_watched_episodes = max(state.completed_episode_count, int(state.max_completed_episode_number or 0))
    reasons: list[str] = [
        "merge_policy=missing_data_only",
        "completion_policy=ratio>=0.95_or_remaining<=120s_or_later_episode_progress_with_ratio>=0.85",
        "progress_dedup=distinct_episode_number_when_available",
        "missing_meaningful_rules=status:none_missing;progress:missing_only_when_status_absent;score:0_or_null_missing;dates:null_or_empty_missing",
        f"completed_episode_count={state.completed_episode_count}",
        f"max_completed_episode_number={state.max_completed_episode_number}",
        f"progress_rows={state.progress_rows}",
    ]
    if extra_reasons:
        reasons = list(extra_reasons) + reasons

    proposed_status: dict[str, Any] | None = None
    if crunchyroll_watched_episodes > 0:
        proposed_status = {
            "status": "watching",
            "num_watched_episodes": crunchyroll_watched_episodes,
        }
        reasons.append("crunchyroll_completed_episode_evidence_present")
        if num_episodes and crunchyroll_watched_episodes >= int(num_episodes):
            proposed_status["status"] = "completed"
            reasons.append("crunchyroll_completion_reached_known_episode_count")
    elif state.progress_rows > 0:
        reasons.append("partial_crunchyroll_activity_without_completed_episode")
    elif state.watchlist_status:
        proposed_status = {"status": "plan_to_watch", "num_watched_episodes": 0}
        reasons.append("watchlist_only")

    if proposed_status is None:
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            crunchyroll_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="skip",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            reasons=reasons + ["no_actionable_crunchyroll_state"],
        )

    current_watched = int((current_status or {}).get("num_episodes_watched") or 0)
    current_list_status = _coerce_optional_str((current_status or {}).get("status"))
    proposed_watched = int(proposed_status.get("num_watched_episodes") or 0)

    if current_list_status == "plan_to_watch" and proposed_watched > 0:
        reasons.append("override_plan_to_watch_due_to_crunchyroll_watch_evidence")

    if current_watched > proposed_watched:
        return SyncProposal(
            provider_series_id=state.provider_series_id,
            crunchyroll_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="skip",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            reasons=reasons + [f"refusing_to_decrease_mal_progress current={current_watched} proposed={proposed_watched}"],
        )

    if current_watched == proposed_watched and current_list_status == proposed_status["status"]:
        field_merge = _build_missing_field_merge(state, current_status, proposed_status["status"], current_watched)
        if not field_merge:
            return SyncProposal(
                provider_series_id=state.provider_series_id,
                crunchyroll_title=state.title,
                mapping_status=mapping_status,
                confidence=confidence,
                mal_anime_id=mal_anime_id,
                mal_title=mal_title,
                current_my_list_status=current_status,
                proposed_my_list_status=None,
                decision="skip",
                mapping_source=mapping_source,
                persisted_mapping_approved=persisted_mapping_approved,
                reasons=reasons + ["mal_already_matches_or_exceeds_proposal"],
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
            crunchyroll_title=state.title,
            mapping_status=mapping_status,
            confidence=confidence,
            mal_anime_id=mal_anime_id,
            mal_title=mal_title,
            current_my_list_status=current_status,
            proposed_my_list_status=None,
            decision="skip",
            mapping_source=mapping_source,
            persisted_mapping_approved=persisted_mapping_approved,
            reasons=reasons + ["refusing_to_downgrade_completed_mal_entry"],
        )

    if current_status:
        reasons.append("would_update_existing_mal_entry")
    else:
        reasons.append("would_create_new_mal_entry")

    return SyncProposal(
        provider_series_id=state.provider_series_id,
        crunchyroll_title=state.title,
        mapping_status=mapping_status,
        confidence=confidence,
        mal_anime_id=mal_anime_id,
        mal_title=mal_title,
        current_my_list_status=current_status,
        proposed_my_list_status=proposed_status,
        decision="propose_update",
        mapping_source=mapping_source,
        persisted_mapping_approved=persisted_mapping_approved,
        reasons=reasons,
    )


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
        # Crunchyroll snapshot does not provide a trustworthy rating yet.
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
