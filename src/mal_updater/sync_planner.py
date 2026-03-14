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
    query = """
        SELECT
            s.provider,
            s.provider_series_id,
            s.title,
            s.season_title,
            s.season_number,
            COUNT(DISTINCT p.provider_episode_id) AS progress_rows,
            MAX(p.episode_number) AS max_episode_number,
            COUNT(DISTINCT CASE WHEN p.completion_ratio >= ? THEN p.provider_episode_id END) AS completed_episode_count,
            MAX(CASE WHEN p.completion_ratio >= ? THEN p.episode_number END) AS max_completed_episode_number,
            MAX(p.last_watched_at) AS last_watched_at,
            w.status AS watchlist_status
        FROM provider_series s
        LEFT JOIN provider_episode_progress p
            ON p.provider = s.provider AND p.provider_series_id = s.provider_series_id
        LEFT JOIN provider_watchlist w
            ON w.provider = s.provider AND w.provider_series_id = s.provider_series_id
        WHERE s.provider = ?
        GROUP BY s.provider, s.provider_series_id, s.title, s.season_title, s.season_number, w.status
        ORDER BY COALESCE(MAX(p.last_watched_at), s.last_seen_at) DESC, s.title ASC
    """
    params: list[Any] = [config.completion_threshold, config.completion_threshold, provider]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    with connect(config.db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        ProviderSeriesState(
            provider=row["provider"],
            provider_series_id=row["provider_series_id"],
            title=row["title"],
            season_title=row["season_title"],
            season_number=row["season_number"],
            progress_rows=int(row["progress_rows"] or 0),
            max_episode_number=row["max_episode_number"],
            completed_episode_count=int(row["completed_episode_count"] or 0),
            max_completed_episode_number=row["max_completed_episode_number"],
            watchlist_status=row["watchlist_status"],
            last_watched_at=row["last_watched_at"],
        )
        for row in rows
    ]


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
            response = client.update_my_list_status(
                persisted.mal_anime_id,
                status=str(proposal.proposed_my_list_status["status"]),
                num_watched_episodes=int(proposal.proposed_my_list_status.get("num_watched_episodes") or 0),
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
        if num_episodes and crunchyroll_watched_episodes >= int(num_episodes):
            proposed_status["status"] = "completed"
            reasons.append("crunchyroll_completion_reached_known_episode_count")
    elif state.progress_rows > 0:
        proposed_status = {"status": "watching", "num_watched_episodes": 0}
        reasons.append("started_but_no_completed_episodes")
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
    current_list_status = (current_status or {}).get("status")
    proposed_watched = int(proposed_status.get("num_watched_episodes") or 0)

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
