from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config import AppConfig, load_mal_secrets
from .db import connect
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



def build_dry_run_sync_plan(config: AppConfig, limit: int | None = 20, mapping_limit: int = 5) -> list[SyncProposal]:
    states = load_provider_series_states(config, limit=limit)
    client = MalClient(config, load_mal_secrets(config))
    proposals: list[SyncProposal] = []
    for state in states:
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
        if mapping.status not in {"exact", "strong"} or not mapping.chosen_candidate:
            reasons = list(mapping.rationale)
            if mapping.candidates:
                reasons.append(
                    "top_candidates="
                    + ", ".join(f"{candidate.mal_anime_id}:{candidate.title}:{candidate.score:.3f}" for candidate in mapping.candidates[:3])
                )
            proposals.append(
                SyncProposal(
                    provider_series_id=state.provider_series_id,
                    crunchyroll_title=state.title,
                    mapping_status=mapping.status,
                    confidence=mapping.confidence,
                    mal_anime_id=mapping.chosen_candidate.mal_anime_id if mapping.chosen_candidate else None,
                    mal_title=mapping.chosen_candidate.title if mapping.chosen_candidate else None,
                    current_my_list_status=None,
                    proposed_my_list_status=None,
                    decision="review",
                    reasons=reasons,
                )
            )
            continue

        detail = client.get_anime_details(
            mapping.chosen_candidate.mal_anime_id,
            fields="id,title,num_episodes,media_type,status,my_list_status,alternative_titles",
        )
        current_status = detail.get("my_list_status") or None
        proposal = _plan_status_update(state, detail, mapping.status, mapping.confidence)
        proposals.append(proposal)
    return proposals



def _plan_status_update(
    state: ProviderSeriesState,
    detail: dict[str, Any],
    mapping_status: str,
    confidence: float,
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
        reasons=reasons,
    )
