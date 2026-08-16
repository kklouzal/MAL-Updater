from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import AppConfig
from .db import bootstrap_database, connect
from .provider_snapshot import snapshot_to_dict
from .provider_types import ProviderFetchResult
from .snapshot_authority import has_hidive_snapshot_authority
from .validation import validate_snapshot_payload
from .evaluation.events import capture_provider_observations


@dataclass(slots=True)
class IngestionSummary:
    provider: str
    contract_version: str
    series_count: int
    progress_count: int
    watchlist_count: int
    sync_run_id: int | None = None
    diagnostics: list[dict[str, Any]] | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "provider": self.provider,
            "contract_version": self.contract_version,
            "series_count": self.series_count,
            "progress_count": self.progress_count,
            "watchlist_count": self.watchlist_count,
            "sync_run_id": self.sync_run_id,
        }
        if self.diagnostics:
            payload["diagnostics"] = self.diagnostics
        return payload


def ingest_snapshot_payload(
    payload: Any,
    config: AppConfig,
    *,
    mode: str = "ingest_snapshot",
) -> IngestionSummary:
    return _ingest_snapshot_payload(payload, config, mode=mode)


def ingest_provider_fetch_result(
    result: ProviderFetchResult,
    config: AppConfig,
    *,
    mode: str,
) -> IngestionSummary:
    """Ingest an in-process provider result, preserving internal producer authority."""
    return _ingest_snapshot_payload(
        snapshot_to_dict(result.snapshot),
        config,
        mode=mode,
        hidive_producer_authenticated=has_hidive_snapshot_authority(result._ingestion_authority),
    )


def _ingest_snapshot_payload(
    payload: Any,
    config: AppConfig,
    *,
    mode: str,
    hidive_producer_authenticated: bool = False,
) -> IngestionSummary:
    snapshot = validate_snapshot_payload(payload)
    bootstrap_database(config.db_path)

    provider = snapshot.provider
    contract_version = snapshot.contract_version

    with connect(config.db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        sync_run_id = _insert_sync_run(conn, provider, contract_version, mode)
        try:
            _upsert_series(conn, provider, snapshot.series)
            _upsert_progress(conn, provider, snapshot.progress)
            capture_provider_observations(conn, snapshot=snapshot, sync_run_id=sync_run_id)
            account_id_hint = snapshot.account_id_hint.strip() if snapshot.account_id_hint else None
            account_conflict = _watchlist_account_conflict(
                conn, provider=provider, account_id_hint=account_id_hint,
            )
            if account_conflict:
                membership_deactivation = {
                    "code": "watchlist_membership_generation",
                    "surface": "watchlist",
                    "severity": "warning",
                    "generation": sync_run_id,
                    "complete": False,
                    "deactivated": 0,
                    "upserted": 0,
                    "reason": "account_mismatch",
                }
            else:
                _upsert_watchlist(
                    conn, provider, snapshot.watchlist,
                    generation=sync_run_id, account_id_hint=account_id_hint,
                )
                membership_deactivation = _deactivate_absent_watchlist_memberships(
                    conn, provider=provider, generation=sync_run_id,
                    account_id_hint=account_id_hint, mode=mode, raw=snapshot.raw,
                    hidive_producer_authenticated=hidive_producer_authenticated,
                )
                membership_deactivation["upserted"] = len(snapshot.watchlist)
            diagnostics = _snapshot_diagnostics_from_raw(
                snapshot.raw,
                hidive_producer_authenticated=hidive_producer_authenticated,
            )
            diagnostics.append(membership_deactivation)
            summary = IngestionSummary(
                provider=provider,
                contract_version=contract_version,
                series_count=len(snapshot.series),
                progress_count=len(snapshot.progress),
                watchlist_count=len(snapshot.watchlist),
                sync_run_id=sync_run_id,
                diagnostics=diagnostics,
            )
            _complete_sync_run(conn, sync_run_id, "completed", summary.as_dict())
            conn.commit()
            return summary
        except Exception as exc:
            conn.rollback()
            _insert_failed_sync_run(conn, provider, contract_version, mode, exc)
            conn.commit()
            raise


def ingest_snapshot_file(path: Path, config: AppConfig, *, mode: str = "ingest_snapshot") -> IngestionSummary:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ingest_snapshot_payload(payload, config, mode=mode)


def _insert_sync_run(conn, provider: str, contract_version: str, mode: str) -> int:
    cursor = conn.execute(
        """
        INSERT INTO sync_runs(provider, contract_version, mode, status)
        VALUES (?, ?, ?, ?)
        """,
        (provider, contract_version, mode, "running"),
    )
    return int(cursor.lastrowid)


def _insert_failed_sync_run(conn, provider: str, contract_version: str, mode: str, exc: Exception) -> int:
    sync_run_id = _insert_sync_run(conn, provider, contract_version, mode)
    _complete_sync_run(conn, sync_run_id, "failed", {"error": str(exc)})
    return sync_run_id


def _complete_sync_run(conn, sync_run_id: int, status: str, summary: dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE sync_runs
        SET status = ?, completed_at = CURRENT_TIMESTAMP, summary_json = ?
        WHERE id = ?
        """,
        (status, json.dumps(summary, sort_keys=True), sync_run_id),
    )


def _entry_json(entry: Any) -> str:
    return json.dumps(asdict(entry), sort_keys=True)


def _snapshot_diagnostics_from_raw(
    raw: dict[str, Any], *, hidive_producer_authenticated: bool = False
) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    raw_diagnostics = raw.get("diagnostics")
    if isinstance(raw_diagnostics, list):
        for item in raw_diagnostics:
            if not isinstance(item, dict):
                continue
            code = item.get("code")
            if not isinstance(code, str) or not code:
                continue
            diagnostics.append({key: value for key, value in item.items() if key != "sensitive"})
    raw_authority = raw.get("surface_authority")
    authority: dict[str, Any] = raw_authority if isinstance(raw_authority, dict) else {}
    producer_evidence = _hidive_producer_evidence(raw, authenticated=hidive_producer_authenticated)
    hidive_authority_present = bool(authority) and isinstance(raw.get("sync_boundary_mode"), str)
    hot_boundary_proven = False
    if hidive_authority_present:
        hot_boundary_proven = bool(
            producer_evidence["producer_authenticated"]
            and raw.get("sync_boundary_mode") == "hot"
            and raw.get("sync_boundary_account_status") == "account_match"
            and raw.get("sync_boundary_usable") is True
            and authority.get("account_identity_proven") is True
            and authority.get("history_front_boundary_complete") is True
            and raw.get("history_front_boundary_complete") is True
        )
        diagnostics.append(
            {
                "code": "hidive_surface_authority",
                "surface": "provider_snapshot",
                "severity": "info",
                "sync_boundary_mode": raw.get("sync_boundary_mode"),
                "sync_boundary_account_status": raw.get("sync_boundary_account_status"),
                "account_identity_proven": authority.get("account_identity_proven") is True,
                "history_front_boundary_complete": authority.get("history_front_boundary_complete") is True,
                "history_full_complete": authority.get("history_full_complete") is True,
                "continue_complete": authority.get("continue_complete") is True,
                "watchlist_complete": authority.get("watchlist_complete") is True,
                "producer_authenticated": producer_evidence["producer_authenticated"],
                # A hot snapshot deliberately reuses the account-bound watchlist
                # generation instead of traversing it again.  That authority is
                # safe only after the matching boundary and current history front
                # marker are both proven.
                "watchlist_authority_safe": authority.get("watchlist_complete") is True or hot_boundary_proven,
            }
        )
    documented_partial_codes = {
        str(item.get("code"))
        for item in diagnostics
        if isinstance(item, dict)
    }
    # A proven hot boundary can satisfy the history-front objective on page 1,
    # so that concrete run does not emit history_partial_unpageable.  The
    # history diagnostic is therefore allowed, not required; Continue Watching
    # remains the actual partial contributor and must still be explicit.
    expected_partial_codes = {"continue_watching_partial_unpageable"}
    allowed_partial_codes = expected_partial_codes | {"history_partial_unpageable"}
    partial_fully_explained = bool(
        hot_boundary_proven
        and expected_partial_codes.issubset(documented_partial_codes)
        and documented_partial_codes.issubset(allowed_partial_codes | {"hidive_surface_authority"})
        and raw.get("partial") is True
        and raw.get("history_full_complete") is False
        and raw.get("continue_complete") is False
        and raw.get("continue_partial") is True
        and authority.get("history_full_complete") is False
        and authority.get("continue_complete") is False
    )
    if raw.get("partial") is True and not partial_fully_explained:
        diagnostics.append({"code": "snapshot_partial", "surface": "provider_snapshot", "severity": "warning"})
    if raw.get("history_non_advancing_detected") is True:
        diagnostics.append({"code": "history_pagination_non_advancing", "surface": "history", "severity": "warning"})
    if raw.get("continue_partial") is True:
        diagnostics.append({"code": "continue_watching_partial_unpageable", "surface": "continue_watching", "severity": "info"})
    if raw.get("custom_watchlist_partial") is True:
        diagnostics.append({"code": "custom_watchlist_partial", "surface": "watchlists", "severity": "warning"})

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    for item in diagnostics:
        key = (str(item.get("code")), str(item.get("surface")) if item.get("surface") is not None else None)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _upsert_series(conn, provider: str, series_entries: list[Any]) -> None:
    for entry in series_entries:
        conn.execute(
            """
            INSERT INTO provider_series (
                provider,
                provider_series_id,
                title,
                season_title,
                season_number,
                raw_json,
                first_seen_at,
                last_seen_at,
                account_observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(provider, provider_series_id) DO UPDATE SET
                title = excluded.title,
                season_title = excluded.season_title,
                season_number = excluded.season_number,
                raw_json = excluded.raw_json,
                last_seen_at = CURRENT_TIMESTAMP,
                account_observed_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                entry.provider_series_id,
                entry.title,
                entry.season_title,
                entry.season_number,
                _entry_json(entry),
            ),
        )


def _upsert_progress(conn, provider: str, progress_entries: list[Any]) -> None:
    for entry in progress_entries:
        conn.execute(
            """
            INSERT INTO provider_episode_progress (
                provider,
                provider_episode_id,
                provider_series_id,
                episode_number,
                episode_title,
                playback_position_ms,
                duration_ms,
                completion_ratio,
                last_watched_at,
                audio_locale,
                subtitle_locale,
                rating,
                progress_source_surface,
                progress_observation_kind,
                completion_assertion,
                normalization_logic_version,
                raw_json,
                first_seen_at,
                last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(provider, provider_episode_id) DO UPDATE SET
                provider_series_id = excluded.provider_series_id,
                episode_number = excluded.episode_number,
                episode_title = excluded.episode_title,
                playback_position_ms = CASE WHEN
                    CASE excluded.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    >= CASE provider_episode_progress.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    THEN excluded.playback_position_ms ELSE provider_episode_progress.playback_position_ms END,
                duration_ms = COALESCE(excluded.duration_ms, provider_episode_progress.duration_ms),
                completion_ratio = CASE WHEN
                    CASE excluded.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    >= CASE provider_episode_progress.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    THEN excluded.completion_ratio ELSE provider_episode_progress.completion_ratio END,
                last_watched_at = CASE
                    WHEN provider_episode_progress.last_watched_at IS NULL THEN excluded.last_watched_at
                    WHEN excluded.last_watched_at IS NULL THEN provider_episode_progress.last_watched_at
                    WHEN excluded.last_watched_at > provider_episode_progress.last_watched_at THEN excluded.last_watched_at
                    ELSE provider_episode_progress.last_watched_at END,
                audio_locale = COALESCE(excluded.audio_locale, provider_episode_progress.audio_locale),
                subtitle_locale = COALESCE(excluded.subtitle_locale, provider_episode_progress.subtitle_locale),
                rating = COALESCE(excluded.rating, provider_episode_progress.rating),
                progress_source_surface = CASE WHEN
                    CASE excluded.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    >= CASE provider_episode_progress.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    THEN excluded.progress_source_surface ELSE provider_episode_progress.progress_source_surface END,
                progress_observation_kind = CASE WHEN
                    CASE excluded.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    >= CASE provider_episode_progress.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    THEN excluded.progress_observation_kind ELSE provider_episode_progress.progress_observation_kind END,
                completion_assertion = CASE WHEN
                    CASE excluded.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    >= CASE provider_episode_progress.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    THEN excluded.completion_assertion ELSE provider_episode_progress.completion_assertion END,
                normalization_logic_version = CASE WHEN
                    CASE excluded.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    >= CASE provider_episode_progress.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    THEN excluded.normalization_logic_version ELSE provider_episode_progress.normalization_logic_version END,
                raw_json = CASE WHEN
                    CASE excluded.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    >= CASE provider_episode_progress.progress_observation_kind
                        WHEN 'explicit_completed' THEN 4 WHEN 'position' THEN 3 WHEN 'ratio' THEN 3
                        WHEN 'inferred_later_episode' THEN 3 WHEN 'history_membership' THEN 1 ELSE 2 END
                    THEN excluded.raw_json ELSE provider_episode_progress.raw_json END,
                last_seen_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                entry.provider_episode_id,
                entry.provider_series_id,
                entry.episode_number,
                entry.episode_title,
                entry.playback_position_ms,
                entry.duration_ms,
                entry.completion_ratio,
                entry.last_watched_at,
                entry.audio_locale,
                entry.subtitle_locale,
                entry.rating,
                entry.progress_source_surface,
                entry.progress_observation_kind,
                entry.completion_assertion,
                entry.normalization_logic_version,
                _entry_json(entry),
            ),
        )


def _upsert_watchlist(
    conn, provider: str, watchlist_entries: list[Any], *,
    generation: int, account_id_hint: str | None,
) -> None:
    for entry in watchlist_entries:
        list_id = entry.list_id or "default"
        provider_item_type = entry.provider_item_type or "series"
        provider_item_id = entry.provider_item_id or entry.provider_series_id
        conn.execute(
            """
            INSERT INTO provider_watchlist (
                provider,
                provider_series_id,
                added_at,
                status,
                list_id,
                list_name,
                list_kind,
                provider_item_id,
                provider_item_type,
                position,
                raw_json,
                first_seen_at,
                last_seen_at,
                is_active,
                membership_generation,
                account_id_hint,
                deactivated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, ?, ?, NULL)
            ON CONFLICT(provider, list_id, provider_series_id, provider_item_type, provider_item_id) DO UPDATE SET
                added_at = excluded.added_at,
                status = excluded.status,
                list_name = excluded.list_name,
                list_kind = excluded.list_kind,
                position = excluded.position,
                raw_json = excluded.raw_json,
                last_seen_at = CURRENT_TIMESTAMP,
                is_active = 1,
                membership_generation = excluded.membership_generation,
                account_id_hint = CASE
                    WHEN provider_watchlist.account_id_hint IS NULL
                         OR provider_watchlist.account_id_hint = excluded.account_id_hint
                    THEN excluded.account_id_hint
                    ELSE provider_watchlist.account_id_hint
                END,
                deactivated_at = NULL
            """,
            (
                provider,
                entry.provider_series_id,
                entry.added_at,
                entry.status,
                list_id,
                entry.list_name,
                entry.list_kind,
                provider_item_id,
                provider_item_type,
                entry.position,
                _entry_json(entry),
                generation,
                account_id_hint,
            ),
        )


def _watchlist_account_conflict(conn, *, provider: str, account_id_hint: str | None) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM provider_watchlist
        WHERE provider = ? AND account_id_hint IS NOT NULL
          AND (? IS NULL OR account_id_hint <> ?)
        LIMIT 1
        """,
        (provider, account_id_hint, account_id_hint),
    ).fetchone()
    return row is not None


def _hidive_producer_evidence(
    raw: dict[str, Any], *, authenticated: bool
) -> dict[str, bool]:
    # Producer identity is an internal object-identity capability carried only
    # by an in-process concrete HIDIVE fetch result. JSON fields are corroborating
    # contract evidence, never credentials.
    return {
        "producer_authenticated": bool(
            authenticated
            and raw.get("snapshot_producer") == "mal_updater.hidive_snapshot"
            and raw.get("surface_authority_schema_version") == 1
        )
    }


def _watchlist_snapshot_is_complete(
    *,
    provider: str,
    mode: str,
    raw: dict[str, Any],
    hidive_producer_authenticated: bool = False,
) -> tuple[bool, str]:
    if provider == "crunchyroll":
        if mode != "full_refresh":
            return False, "not_full_refresh_mode"
        if raw.get("partial") is not False:
            return False, "snapshot_not_explicitly_complete"
        if raw.get("sync_boundary_refresh_kind") not in {"explicit_full_refresh", "bootstrap_full_refresh"}:
            return False, "not_full_watchlist_traversal"
        if int(raw.get("watchlist_start") or 0) != 0 or raw.get("watchlist_partial") is not False:
            return False, "watchlist_traversal_incomplete"
        if raw.get("sync_boundary_refresh_kind") == "bootstrap_full_refresh" and raw.get("sync_boundary_bootstrap_complete") is not True:
            return False, "bootstrap_unvalidated"
        if raw.get("bootstrap_generation_validated") is False:
            return False, "bootstrap_generation_unvalidated"
        return True, "complete_crunchyroll_watchlist"
    if provider == "hidive":
        # Absence is destructive evidence.  Accept it only from this producer's
        # explicit full/repair traversal contract, never from standalone booleans
        # in imported JSON or from a hot snapshot reusing an older boundary.
        raw_authority = raw.get("surface_authority")
        authority: dict[str, Any] = raw_authority if isinstance(raw_authority, dict) else {}
        evidence = _hidive_producer_evidence(raw, authenticated=hidive_producer_authenticated)
        if not evidence["producer_authenticated"]:
            return False, "producer_evidence_unproven"
        if mode != "full_refresh":
            return False, "not_full_refresh_mode"
        if raw.get("sync_boundary_refresh_kind") not in {"explicit_full_refresh", "account_repair_full_refresh"}:
            return False, "not_full_watchlist_traversal"
        if raw.get("sync_boundary_mode") != "full_refresh" or raw.get("hot_surface_only") is not False:
            return False, "hot_snapshot_not_authoritative"
        raw_supports = raw.get("supports")
        supports: dict[str, Any] = raw_supports if isinstance(raw_supports, dict) else {}
        favourite_pages = raw.get("favourite_pages_fetched")
        custom_collection_pages = raw.get("custom_watchlist_collection_pages_fetched")
        custom_detail_pages = raw.get("custom_watchlist_detail_pages_fetched")
        concrete_complete = bool(
            supports.get("watchlists") is True
            and raw.get("watchlist_complete") is True
            and authority.get("watchlist_complete") is True
            and authority.get("account_identity_proven") is True
            and raw.get("favourite_stopped_early") is False
            and raw.get("custom_watchlist_partial") is False
            and isinstance(favourite_pages, int)
            and favourite_pages >= 1
            and isinstance(custom_collection_pages, int)
            and custom_collection_pages >= 1
            and isinstance(custom_detail_pages, int)
            and custom_detail_pages >= 0
        )
        if not concrete_complete:
            return False, "watchlist_traversal_evidence_incomplete_or_contradictory"
        return True, "complete_hidive_watchlists"
    return False, "provider_has_no_complete_watchlist_contract"


def _deactivate_absent_watchlist_memberships(
    conn, *, provider: str, generation: int, account_id_hint: str | None,
    mode: str, raw: dict[str, Any], hidive_producer_authenticated: bool = False,
) -> dict[str, Any]:
    complete, reason = _watchlist_snapshot_is_complete(
        provider=provider,
        mode=mode,
        raw=raw,
        hidive_producer_authenticated=hidive_producer_authenticated,
    )
    diagnostic: dict[str, Any] = {
        "code": "watchlist_membership_generation", "surface": "watchlist",
        "severity": "info", "generation": generation, "complete": complete,
        "deactivated": 0, "reason": reason,
    }
    if not complete or not account_id_hint:
        if complete and not account_id_hint:
            diagnostic.update(complete=False, reason="account_identity_unproven", severity="warning")
        return diagnostic
    conflicting = conn.execute(
        "SELECT 1 FROM provider_watchlist WHERE provider = ? AND account_id_hint IS NOT NULL AND account_id_hint <> ? LIMIT 1",
        (provider, account_id_hint),
    ).fetchone()
    if conflicting is not None:
        diagnostic.update(complete=False, reason="account_mismatch", severity="warning")
        return diagnostic
    cursor = conn.execute(
        """
        UPDATE provider_watchlist
        SET is_active = 0, deactivated_at = CURRENT_TIMESTAMP
        WHERE provider = ? AND account_id_hint = ? AND is_active = 1
          AND (membership_generation IS NULL OR membership_generation <> ?)
        """,
        (provider, account_id_hint, generation),
    )
    diagnostic["deactivated"] = max(0, int(cursor.rowcount))
    return diagnostic
