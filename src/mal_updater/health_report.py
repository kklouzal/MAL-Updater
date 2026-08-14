from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from .auth_remediation import (
    mal_missing_auth_descriptor,
    mal_rebootstrap_auth_descriptor,
    provider_missing_state_descriptor,
    provider_rebootstrap_auth_descriptor,
)
from .config import AppConfig, ensure_directories, load_config, load_mal_secrets
from .crunchyroll_auth import load_crunchyroll_credentials, resolve_crunchyroll_state_paths
from .db import (
    bootstrap_database,
    get_latest_completed_sync_run,
    get_operational_snapshot,
    get_provider_series_title_map_by_keys,
    get_provider_stale_row_counts,
    get_public_userrecs_diagnostics,
    list_provider_stale_row_samples,
    list_review_queue_entries,
)
from .hidive_auth import load_hidive_credentials, resolve_hidive_state_paths
from . import review_queue_support as _review_queue_support
from .service_auth_state import (
    describe_auth_failure_kind,
    load_service_state,
    mal_service_auth_failure,
    provider_service_auth_failure,
)
from .sync_planner import MAPPING_REVIEW_HEURISTICS_REVISION
from . import service_systemd_status as _service_systemd_status
from .service_units import systemd_unit_path_context
from .recommendation_enrichment import build_provider_enrichment_diagnostics


def _parse_sqlite_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for parser in (
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
    ):
        try:
            parsed = parser(text)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def age_seconds_from_timestamp(value: object) -> float | None:
    parsed = _parse_sqlite_timestamp(value)
    if parsed is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def _age_seconds_from_timestamp(value: object) -> float | None:
    return age_seconds_from_timestamp(value)

def _sync_run_summary_counts(sync_run: dict[str, object] | None) -> dict[str, int]:
    if not isinstance(sync_run, dict):
        return {}
    summary = sync_run.get("summary")
    if not isinstance(summary, dict):
        return {}
    counts: dict[str, int] = {}
    for field in ("series_count", "progress_count", "watchlist_count"):
        value = summary.get(field)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            counts[field] = max(0, int(value))
    return counts

def _build_partial_sync_coverage(
    latest_sync_run: dict[str, object] | None,
    provider_counts_by_provider: dict[str, object] | None,
    stale_row_counts: dict[str, int] | None = None,
    stale_row_samples: dict[str, list[dict[str, object]]] | None = None,
) -> dict[str, object] | None:
    latest_counts = _sync_run_summary_counts(latest_sync_run)
    if not latest_counts or not isinstance(provider_counts_by_provider, dict):
        return None

    provider_name = latest_sync_run.get("provider") if isinstance(latest_sync_run, dict) else None
    if not isinstance(provider_name, str) or not provider_name:
        return None
    provider_counts = provider_counts_by_provider.get(provider_name)
    if not isinstance(provider_counts, dict):
        return None

    field_map = {
        "series_count": "series",
        "progress_count": "progress",
        "watchlist_count": "watchlist",
    }
    partial_fields: dict[str, dict[str, object]] = {}
    stale_explained_fields: dict[str, dict[str, object]] = {}
    mode = latest_sync_run.get("mode") if isinstance(latest_sync_run, dict) else None
    if mode == "hot":
        return None
    for summary_field, provider_field in field_map.items():
        latest_value = latest_counts.get(summary_field)
        provider_value = provider_counts.get(provider_field)
        if not isinstance(latest_value, int):
            continue
        if not isinstance(provider_value, int) or provider_value <= 0:
            continue
        if latest_value >= provider_value:
            continue
        missing_count = provider_value - latest_value
        field_payload = {
            "latest_sync_run_count": latest_value,
            "provider_total_count": provider_value,
            "coverage_ratio": round(latest_value / provider_value, 4),
            "missing_count": missing_count,
        }
        stale_count = stale_row_counts.get(provider_field) if isinstance(stale_row_counts, dict) else None
        if isinstance(stale_count, int) and stale_count > 0:
            field_payload["older_last_seen_row_count"] = stale_count
            stale_samples = stale_row_samples.get(provider_field) if isinstance(stale_row_samples, dict) else None
            if isinstance(stale_samples, list) and stale_samples:
                field_payload["older_last_seen_samples"] = stale_samples
            if stale_count == missing_count:
                field_payload["classification"] = (
                    "stale_or_deleted_provider_rows"
                    if mode == "full_refresh"
                    else "incremental_backfill_pending_provider_rows"
                )
                stale_explained_fields[provider_field] = field_payload
        partial_fields[provider_field] = field_payload

    if not partial_fields:
        return None

    fully_explained_by_stale_rows = bool(partial_fields) and set(stale_explained_fields) == set(partial_fields)
    result = {
        "provider": provider_name,
        "sync_run_id": latest_sync_run.get("id") if isinstance(latest_sync_run, dict) else None,
        "mode": mode,
        "fully_explained_by_stale_rows": fully_explained_by_stale_rows,
        "fields": partial_fields,
    }
    total_provider_count = sum(
        int(field.get("provider_total_count"))
        for field in partial_fields.values()
        if isinstance(field.get("provider_total_count"), int)
    )
    total_touched_count = sum(
        int(field.get("latest_sync_run_count"))
        for field in partial_fields.values()
        if isinstance(field.get("latest_sync_run_count"), int)
    )
    total_missing_count = sum(
        int(field.get("missing_count"))
        for field in partial_fields.values()
        if isinstance(field.get("missing_count"), int)
    )
    if total_provider_count > 0:
        result["backfill_progress"] = {
            "latest_sync_run_touched_count": total_touched_count,
            "provider_total_count": total_provider_count,
            "remaining_row_count": total_missing_count,
            "coverage_ratio": round(total_touched_count / total_provider_count, 4),
        }
    return result

def _classify_partial_sync_health_posture(
    partial_sync_coverage: dict[str, object],
    *,
    latest_completed_age_seconds: float | None,
    stale_hours: float,
) -> dict[str, object]:
    """Return operator severity/posture for partial coverage or stale-row residue.

    Recent incremental residue that is fully explained by older child rows linked to
    current provider series is expected during slow front-of-line backfill. Keep it
    visible, but do not make the whole health check automation-unhealthy.
    """
    fields = partial_sync_coverage.get("fields")
    if not isinstance(fields, dict) or not fields:
        return {"severity": "warning", "health_posture": "unhealthy", "automation_safe": False}

    fully_explained = partial_sync_coverage.get("fully_explained_by_stale_rows") is True
    mode = partial_sync_coverage.get("mode")
    recent = latest_completed_age_seconds is None or latest_completed_age_seconds <= stale_hours * 3600
    classifications = {
        field.get("classification")
        for field in fields.values()
        if isinstance(field, dict)
    }
    only_incremental_backfill = classifications == {"incremental_backfill_pending_provider_rows"}

    child_linkage_safe = True
    has_child_residue = False
    for family, field in fields.items():
        if not isinstance(field, dict):
            child_linkage_safe = False
            continue
        if family == "series":
            child_linkage_safe = False
            continue
        if family in {"progress", "watchlist"}:
            has_child_residue = True
            samples = field.get("older_last_seen_samples")
            if not isinstance(samples, list) or not samples:
                child_linkage_safe = False
                continue
            for sample in samples:
                if not isinstance(sample, dict) or sample.get("linked_series_posture") not in {"current", "current_series"}:
                    child_linkage_safe = False
                    break

    if fully_explained and mode != "full_refresh" and recent and only_incremental_backfill and has_child_residue and child_linkage_safe:
        return {
            "severity": "info",
            "health_posture": "operator_visible",
            "automation_safe": True,
            "rationale": "recent_incremental_backfill_child_rows_linked_to_current_series",
        }

    return {
        "severity": "warning",
        "health_posture": "unhealthy",
        "automation_safe": False,
        "rationale": "old_unlinked_series_or_partial_provider_residue_needs_operator_review",
    }

def _build_mapping_coverage_snapshot(
    provider_series_inventory: dict[str, object] | None,
    mapping_counts: dict[str, object] | None,
) -> dict[str, object] | None:
    if not isinstance(provider_series_inventory, dict) or not isinstance(mapping_counts, dict):
        return None
    persisted_series_count = provider_series_inventory.get("persisted_total")
    catalog_only_series_count = provider_series_inventory.get("catalog_only")
    eligible_series_count = provider_series_inventory.get("mapping_eligible")
    eligible_approved_mapping_count = provider_series_inventory.get("eligible_mapping_approved")
    persisted_approved_mapping_count = mapping_counts.get("approved")
    total_mapping_count = mapping_counts.get("total")
    for value in (persisted_series_count, catalog_only_series_count, eligible_series_count, eligible_approved_mapping_count):
        if not isinstance(value, int) or value < 0:
            return None
    if not isinstance(persisted_approved_mapping_count, int) or persisted_approved_mapping_count < 0:
        persisted_approved_mapping_count = eligible_approved_mapping_count
    if not isinstance(total_mapping_count, int) or total_mapping_count < 0:
        total_mapping_count = persisted_approved_mapping_count

    coverage_ratio = None
    if eligible_series_count > 0:
        coverage_ratio = round(min(1.0, eligible_approved_mapping_count / eligible_series_count), 4)

    return {
        "persisted_provider_series_count": persisted_series_count,
        "catalog_only_series_count": catalog_only_series_count,
        "mapping_eligible_series_count": eligible_series_count,
        "eligible_approved_mapping_count": eligible_approved_mapping_count,
        "eligible_unmapped_series_count": max(0, eligible_series_count - eligible_approved_mapping_count),
        "eligible_approved_coverage_ratio": coverage_ratio,
        "persisted_approved_mapping_count": persisted_approved_mapping_count,
        "total_mapping_count": total_mapping_count,
        # Compatibility aliases: provider_series_count remains persisted inventory;
        # the other aliases now explicitly describe the operationally eligible population.
        "provider_series_count": persisted_series_count,
        "approved_mapping_count": eligible_approved_mapping_count,
        "unmapped_series_count": max(0, eligible_series_count - eligible_approved_mapping_count),
        "approved_coverage_ratio": coverage_ratio,
    }

def _build_mapping_review_revision_snapshot(items: list[object]) -> dict[str, object] | None:
    if not isinstance(items, list):
        return None
    total_open = 0
    stale_items: list[dict[str, object]] = []
    for item in items:
        payload = item.payload if hasattr(item, "payload") else None
        if not isinstance(payload, dict):
            continue
        total_open += 1
        revision = payload.get("mapper_revision")
        if revision == MAPPING_REVIEW_HEURISTICS_REVISION:
            continue
        provider_series_id = item.provider_series_id if hasattr(item, "provider_series_id") else None
        stale_items.append(
            {
                "provider_series_id": provider_series_id,
                "title": payload.get("title") if isinstance(payload.get("title"), str) else None,
                "mapper_revision": revision if isinstance(revision, str) and revision else None,
            }
        )
    return {
        "current_revision": MAPPING_REVIEW_HEURISTICS_REVISION,
        "open_count": total_open,
        "stale_open_count": len(stale_items),
        "stale_examples": stale_items[:10],
        "all_current": len(stale_items) == 0,
    }


def _provider_surface_diagnostics_from_sync_run(sync_run: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(sync_run, dict):
        return None
    provider = sync_run.get("provider")
    summary = sync_run.get("summary")
    if not isinstance(provider, str) or not provider or not isinstance(summary, dict):
        return None
    if provider != "hidive":
        return None
    diagnostics = summary.get("diagnostics")
    if not isinstance(diagnostics, list):
        return None
    normalized: list[dict[str, object]] = []
    for item in diagnostics:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        if not isinstance(code, str) or not code:
            continue
        normalized.append(item)
    if not normalized:
        return None
    codes = {str(item.get("code")) for item in normalized}
    authority_items = [item for item in normalized if item.get("code") == "hidive_surface_authority"]
    authority = authority_items[-1] if authority_items else None
    blocking_codes = {
        "snapshot_partial",
        "history_pagination_non_advancing",
        "history_page_guard_hit",
        "history_page_cap_hit",
        "custom_watchlist_partial",
        "custom_watchlist_collection_page_guard_hit",
        "custom_watchlist_collection_pagination_non_advancing",
        "custom_watchlist_collection_cursor_non_advancing",
        "custom_watchlist_collection_missing_cursor",
        "custom_watchlist_detail_page_guard_hit",
        "custom_watchlist_detail_pagination_non_advancing",
        "custom_watchlist_detail_cursor_non_advancing",
        "custom_watchlist_detail_missing_cursor",
    }
    documented_limitations = {
        "history_partial_unpageable",
        "continue_watching_partial_unpageable",
        "hidive_surface_authority",
        "watchlist_membership_generation",
    }
    authority_safe = bool(
        provider == "hidive"
        and isinstance(authority, dict)
        and authority.get("sync_boundary_mode") == "hot"
        and authority.get("sync_boundary_account_status") == "account_match"
        and authority.get("producer_authenticated") is True
        and authority.get("account_identity_proven") is True
        and authority.get("history_front_boundary_complete") is True
        and authority.get("history_full_complete") is False
        and authority.get("continue_complete") is False
        and authority.get("watchlist_authority_safe") is True
    )
    expected_limitations = {"history_partial_unpageable", "continue_watching_partial_unpageable"}
    only_documented_limitations = bool(
        expected_limitations.issubset(codes)
        and codes.issubset(documented_limitations)
    )
    automation_safe = authority_safe and only_documented_limitations
    blocking = not automation_safe
    return {
        "provider": provider,
        "sync_run_id": sync_run.get("id"),
        "mode": sync_run.get("mode"),
        "diagnostics": normalized,
        "codes": sorted(codes),
        "severity": "warning" if blocking else "info",
        "health_posture": "unhealthy" if blocking else "operator_visible",
        "automation_safe": not blocking,
        "authority": authority,
        "rationale": (
            "account_bound_hot_boundary_and_watchlist_authority_with_documented_unpageable_limits"
            if automation_safe
            else "surface_authority_missing_or_unproven_or_unexpected_partial_diagnostic"
        ),
    }

def _format_systemd_usec_timestamp(value: str) -> str | None:
    return _service_systemd_status.format_systemd_usec_timestamp(value)

def _read_systemd_user_unit_runtime(unit_name: str) -> dict[str, object]:
    return _service_systemd_status.read_systemd_user_unit_runtime(unit_name)

def _build_automation_installation_status(config: AppConfig) -> dict[str, object] | None:
    return _service_systemd_status.build_automation_installation_status(
        config.project_root,
        runtime_reader=_read_systemd_user_unit_runtime,
        path_context=systemd_unit_path_context(config),
    )

def _build_health_maintenance_commands(
    *,
    crunchyroll_credentials_present: bool,
    crunchyroll_state_present: bool,
    hidive_credentials_present: bool,
    hidive_state_present: bool,
    mal_client_id_present: bool,
    mal_auth_present: bool,
    mal_auth_failure: dict[str, object] | None = None,
    latest_sync_run: dict[str, object] | None,
    latest_completed_sync_run: dict[str, object] | None,
    latest_completed_age_seconds: float | None,
    stale_hours: float,
    crunchyroll_snapshot_output_path: Path,
    hidive_snapshot_output_path: Path,
    partial_sync_coverage: dict[str, object] | None = None,
    mapping_coverage: dict[str, object] | None = None,
    mapping_coverage_threshold: float | None = None,
    maintenance_review_limit: int = 25,
    automation_installation: dict[str, object] | None = None,
    review_queue_refresh_command_args: list[str] | None = None,
    review_queue_refresh_worklist_command_args: list[str] | None = None,
    provider_auth_failures: dict[str, dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    commands: list[dict[str, object]] = []
    seen_commands: dict[str, dict[str, object]] = {}

    def add_command(
        reason_code: str,
        detail: str,
        args: list[str],
        *,
        automation_safe: bool,
        requires_auth_interaction: bool,
        auth_failure_kind: str | None = None,
        auth_remediation_kind: str | None = None,
        command_builder=None,
    ) -> None:
        command = command_builder(args) if command_builder is not None else _build_review_queue_command(args)
        existing = seen_commands.get(command)
        if isinstance(existing, dict):
            additional_reason_codes = existing.setdefault("additional_reason_codes", [])
            if isinstance(additional_reason_codes, list) and reason_code not in additional_reason_codes and reason_code != existing.get("reason_code"):
                additional_reason_codes.append(reason_code)
            if auth_failure_kind and not existing.get("auth_failure_kind"):
                existing["reason_code"] = reason_code
                existing["detail"] = detail
                existing["auth_failure_kind"] = auth_failure_kind
                if auth_remediation_kind:
                    existing["auth_remediation_kind"] = auth_remediation_kind
            return
        payload = {
            "reason_code": reason_code,
            "detail": detail,
            "command_args": args,
            "command": command,
            "automation_safe": automation_safe,
            "requires_auth_interaction": requires_auth_interaction,
        }
        if isinstance(auth_failure_kind, str) and auth_failure_kind:
            payload["auth_failure_kind"] = auth_failure_kind
        if isinstance(auth_remediation_kind, str) and auth_remediation_kind:
            payload["auth_remediation_kind"] = auth_remediation_kind
        commands.append(payload)
        seen_commands[command] = payload

    def add_auth_remediation_command(descriptor) -> None:
        add_command(
            descriptor.reason_code,
            descriptor.health_detail(),
            descriptor.maintenance_command_args(),
            automation_safe=descriptor.automation_safe,
            requires_auth_interaction=descriptor.requires_auth_interaction,
            auth_failure_kind=descriptor.auth_failure_kind,
            auth_remediation_kind=descriptor.health_auth_remediation_kind,
        )

    if crunchyroll_credentials_present and not crunchyroll_state_present:
        add_auth_remediation_command(provider_missing_state_descriptor("crunchyroll"))
    if hidive_credentials_present and not hidive_state_present:
        add_auth_remediation_command(provider_missing_state_descriptor("hidive"))

    if mal_client_id_present and isinstance(mal_auth_failure, dict):
        add_auth_remediation_command(mal_rebootstrap_auth_descriptor(mal_auth_failure))

    if mal_client_id_present and not mal_auth_present:
        add_auth_remediation_command(mal_missing_auth_descriptor())

    if isinstance(provider_auth_failures, dict):
        crunchyroll_failure = provider_auth_failures.get("crunchyroll")
        if crunchyroll_credentials_present and isinstance(crunchyroll_failure, dict):
            add_auth_remediation_command(provider_rebootstrap_auth_descriptor("crunchyroll", crunchyroll_failure))
        hidive_failure = provider_auth_failures.get("hidive")
        if hidive_credentials_present and isinstance(hidive_failure, dict):
            add_auth_remediation_command(provider_rebootstrap_auth_descriptor("hidive", hidive_failure))

    snapshot_needs_refresh = not isinstance(latest_completed_sync_run, dict)
    if latest_completed_age_seconds is not None and latest_completed_age_seconds > stale_hours * 3600:
        snapshot_needs_refresh = True
    if isinstance(latest_sync_run, dict) and latest_sync_run.get("status") == "failed":
        snapshot_needs_refresh = True

    refresh_provider = "crunchyroll"
    if isinstance(latest_completed_sync_run, dict) and isinstance(latest_completed_sync_run.get("provider"), str):
        refresh_provider = str(latest_completed_sync_run.get("provider"))
    elif hidive_credentials_present and hidive_state_present and not crunchyroll_credentials_present:
        refresh_provider = "hidive"

    provider_ready = {
        "crunchyroll": crunchyroll_credentials_present and crunchyroll_state_present,
        "hidive": hidive_credentials_present and hidive_state_present,
    }
    provider_snapshot_output = {
        "crunchyroll": crunchyroll_snapshot_output_path,
        "hidive": hidive_snapshot_output_path,
    }
    provider_refresh_args = {
        "crunchyroll": ["provider-fetch-snapshot", "--provider", "crunchyroll"],
        "hidive": ["provider-fetch-snapshot", "--provider", "hidive"],
    }
    provider_label = {
        "crunchyroll": "Crunchyroll",
        "hidive": "HIDIVE",
    }

    if provider_ready.get(refresh_provider) and snapshot_needs_refresh:
        add_command(
            "refresh_ingested_snapshot",
            f"Fetch a fresh {provider_label.get(refresh_provider, refresh_provider)} snapshot and ingest it so health state is current again",
            [*provider_refresh_args[refresh_provider], "--out", str(provider_snapshot_output[refresh_provider]), "--ingest"],
            automation_safe=True,
            requires_auth_interaction=False,
        )

    if isinstance(partial_sync_coverage, dict):
        coverage_provider = partial_sync_coverage.get("provider")
        fields = partial_sync_coverage.get("fields")
        classifications: set[object] = set()
        if isinstance(fields, dict):
            classifications = {
                field.get("classification")
                for field in fields.values()
                if isinstance(field, dict)
            }
        if (
            isinstance(coverage_provider, str)
            and coverage_provider
            and provider_ready.get(coverage_provider)
            and classifications == {"incremental_backfill_pending_provider_rows"}
        ):
            progress = partial_sync_coverage.get("backfill_progress")
            remaining = progress.get("remaining_row_count") if isinstance(progress, dict) else None
            detail = "Inspect older cached provider rows that remain queued behind bounded incremental backfill"
            if isinstance(remaining, int):
                detail = f"Inspect {remaining} older cached provider rows that remain queued behind bounded incremental backfill"
            add_command(
                "inspect_incremental_backfill_provider_rows",
                detail,
                ["provider-stale-rows", "--provider", coverage_provider, "--limit", "20"],
                automation_safe=True,
                requires_auth_interaction=False,
            )

    missing_units = automation_installation.get("missing_required_units") if isinstance(automation_installation, dict) else None
    outdated_units = automation_installation.get("outdated_required_units") if isinstance(automation_installation, dict) else None
    disabled_services = automation_installation.get("disabled_services") if isinstance(automation_installation, dict) else None
    inactive_services = automation_installation.get("inactive_services") if isinstance(automation_installation, dict) else None
    install_script_path = automation_installation.get("install_script_path") if isinstance(automation_installation, dict) else None
    if (
        isinstance(install_script_path, str)
        and install_script_path
        and (
            (isinstance(missing_units, list) and missing_units)
            or (isinstance(outdated_units, list) and outdated_units)
            or (isinstance(disabled_services, list) and disabled_services)
            or (isinstance(inactive_services, list) and inactive_services)
        )
    ):
        detail = "Install the repo-owned user systemd service so MAL-Updater can run as a persistent unattended daemon"
        if isinstance(outdated_units, list) and outdated_units:
            detail = "Reinstall/update the repo-owned user systemd service so the installed daemon matches the current repo version"
        elif isinstance(disabled_services, list) and disabled_services:
            detail = "Enable the repo-owned user systemd service so the background daemon starts automatically for this user"
        elif isinstance(inactive_services, list) and inactive_services:
            detail = "Restart the repo-owned user systemd service so the background daemon is actually active in the user runtime"
        add_command(
            "install_user_systemd_service",
            detail,
            [install_script_path],
            automation_safe=True,
            requires_auth_interaction=False,
            command_builder=_build_shell_command,
        )

    if review_queue_refresh_worklist_command_args and mal_auth_present:
        add_command(
            "refresh_mapping_review_worklist",
            "Re-evaluate the highest-signal open mapping-review slices under the latest mapper heuristics before rebuilding the whole backlog",
            review_queue_refresh_worklist_command_args,
            automation_safe=True,
            requires_auth_interaction=False,
        )
    elif review_queue_refresh_command_args and mal_auth_present:
        add_command(
            "refresh_mapping_review_queue",
            "Re-evaluate the highest-signal open mapping-review slice under the latest mapper heuristics before rebuilding the whole backlog",
            review_queue_refresh_command_args,
            automation_safe=True,
            requires_auth_interaction=False,
        )

    coverage_ratio = mapping_coverage.get("eligible_approved_coverage_ratio") if isinstance(mapping_coverage, dict) else None
    unmapped_series_count = mapping_coverage.get("eligible_unmapped_series_count") if isinstance(mapping_coverage, dict) else None
    if (
        provider_ready.get(refresh_provider)
        and not isinstance(partial_sync_coverage, dict)
        and isinstance(coverage_ratio, float)
        and isinstance(unmapped_series_count, int)
        and unmapped_series_count > 0
        and isinstance(mapping_coverage_threshold, float)
        and coverage_ratio < mapping_coverage_threshold
    ):
        threshold_percent = int(round(mapping_coverage_threshold * 100))
        requested_review_limit = max(0, int(maintenance_review_limit))
        review_command_args = ["review-mappings", "--limit", "0", "--mapping-limit", "5", "--persist-review-queue"]
        detail = f"Rebuild the full mapping review residue because approved mapping coverage is still below {threshold_percent}%"
        if requested_review_limit > 0:
            detail += (
                f"; persisted review-queue replacement requires a full scan, so "
                f"--maintenance-review-limit={requested_review_limit} is not applied to this command"
            )
        add_command(
            "refresh_mapping_review_backlog",
            detail,
            review_command_args,
            automation_safe=True,
            requires_auth_interaction=False,
        )

    return commands

def select_maintenance_command(
    recommended_commands: object,
    *,
    require_automation_safe: bool = False,
) -> dict[str, object] | None:
    if not isinstance(recommended_commands, list):
        return None
    for item in recommended_commands:
        if not isinstance(item, dict):
            continue
        command = item.get("command")
        if not isinstance(command, str) or not command:
            continue
        if require_automation_safe:
            if item.get("automation_safe") is not True:
                continue
            if item.get("requires_auth_interaction") is True:
                continue
        return item
    return None


def _select_maintenance_command(
    recommended_commands: object,
    *,
    require_automation_safe: bool = False,
) -> dict[str, object] | None:
    return select_maintenance_command(
        recommended_commands,
        require_automation_safe=require_automation_safe,
    )


def emit_recommended_command_summary(prefix: str, command_payload: object) -> None:
    if not isinstance(command_payload, dict):
        return
    command = command_payload.get("command")
    if not isinstance(command, str) or not command:
        return
    print(f"{prefix}_command={command}")
    reason_code = command_payload.get("reason_code")
    if isinstance(reason_code, str) and reason_code:
        print(f"{prefix}_reason_code={reason_code}")
    automation_safe = command_payload.get("automation_safe")
    if automation_safe is not None:
        print(f"{prefix}_automation_safe={automation_safe}")
    requires_auth_interaction = command_payload.get("requires_auth_interaction")
    if requires_auth_interaction is not None:
        print(f"{prefix}_requires_auth_interaction={requires_auth_interaction}")
    auth_failure_kind = command_payload.get("auth_failure_kind")
    if isinstance(auth_failure_kind, str) and auth_failure_kind:
        print(f"{prefix}_auth_failure_kind={auth_failure_kind}")
    auth_remediation_kind = command_payload.get("auth_remediation_kind")
    if isinstance(auth_remediation_kind, str) and auth_remediation_kind:
        print(f"{prefix}_auth_remediation_kind={auth_remediation_kind}")


def _emit_recommended_command_summary(prefix: str, command_payload: object) -> None:
    emit_recommended_command_summary(prefix, command_payload)

_review_queue_item_label = _review_queue_support._review_queue_item_label
_review_queue_title_cluster_key = _review_queue_support._review_queue_title_cluster_key
_review_queue_reason_family = _review_queue_support._review_queue_reason_family
_review_queue_apply_worklist_args = _review_queue_support._review_queue_apply_worklist_args
_filter_review_queue_items = _review_queue_support._filter_review_queue_items
_review_queue_refresh_worklist_args = _review_queue_support._review_queue_refresh_worklist_args
_summarize_review_queue = _review_queue_support._summarize_review_queue
_build_review_queue_worklist = _review_queue_support._build_review_queue_worklist
_build_shell_command = _review_queue_support._build_shell_command
_build_review_queue_command = _review_queue_support._build_review_queue_command
_select_review_queue_next_bucket = _review_queue_support._select_review_queue_next_bucket


def _emit_health_check_summary(payload: dict[str, object]) -> None:
    warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    warning_codes = [item.get("code") for item in warnings if isinstance(item, dict) and item.get("code")]
    review_queue = payload.get("review_queue") if isinstance(payload.get("review_queue"), dict) else {}
    recommended_next = review_queue.get("recommended_next") if isinstance(review_queue.get("recommended_next"), dict) else None
    recommended_worklist = review_queue.get("recommended_worklist") if isinstance(review_queue.get("recommended_worklist"), list) else []
    recommended_apply_worklist = review_queue.get("recommended_apply_worklist") if isinstance(review_queue.get("recommended_apply_worklist"), dict) else None
    recommended_refresh_worklist = review_queue.get("recommended_refresh_worklist") if isinstance(review_queue.get("recommended_refresh_worklist"), dict) else None
    maintenance = payload.get("maintenance") if isinstance(payload.get("maintenance"), dict) else {}
    recommended_commands = maintenance.get("recommended_commands") if isinstance(maintenance.get("recommended_commands"), list) else []
    automation = payload.get("automation") if isinstance(payload.get("automation"), dict) else None
    mappings = payload.get("mappings") if isinstance(payload.get("mappings"), dict) else {}
    mapping_coverage = mappings.get("coverage") if isinstance(mappings.get("coverage"), dict) else None
    mapping_review_revision = review_queue.get("mapping_review_revision") if isinstance(review_queue.get("mapping_review_revision"), dict) else None

    install_units_command = None
    if isinstance(recommended_commands, list):
        for item in recommended_commands:
            if not isinstance(item, dict):
                continue
            if item.get("reason_code") != "install_user_systemd_service":
                continue
            command = item.get("command")
            if isinstance(command, str) and command:
                install_units_command = command
                break

    print(f"healthy={bool(payload.get('healthy'))}")
    print(f"warning_count={len(warnings)}")
    niceness_policy = payload.get("niceness_policy") if isinstance(payload.get("niceness_policy"), dict) else {}
    cadences = niceness_policy.get("cadences") if isinstance(niceness_policy.get("cadences"), dict) else {}
    for name in (
        "provider_hot_incremental_seconds",
        "provider_cold_full_seconds",
        "mal_user_list_refresh_seconds",
        "recommendation_metadata_refresh_seconds",
        "recommendation_full_harvest_seconds",
        "provider_eligibility_refresh_seconds",
        "recommendation_snapshot_health_seconds",
    ):
        value = cadences.get(name)
        if isinstance(value, int):
            print(f"niceness_{name}={value}")
    if isinstance(mapping_coverage, dict):
        approved_count = mapping_coverage.get("eligible_approved_mapping_count")
        provider_series_count = mapping_coverage.get("mapping_eligible_series_count")
        coverage_ratio = mapping_coverage.get("eligible_approved_coverage_ratio")
        if isinstance(approved_count, int) and isinstance(provider_series_count, int):
            if isinstance(coverage_ratio, float):
                print(
                    "approved_mapping_coverage="
                    + f"{approved_count}/{provider_series_count} ({coverage_ratio * 100:.1f}%)"
                )
            else:
                print(f"approved_mapping_coverage={approved_count}/{provider_series_count}")
    if warning_codes:
        print("warnings=" + ", ".join(str(code) for code in warning_codes))
    if isinstance(mapping_review_revision, dict):
        stale_open_count = mapping_review_revision.get("stale_open_count")
        open_count = mapping_review_revision.get("open_count")
        current_revision = mapping_review_revision.get("current_revision")
        if isinstance(stale_open_count, int) and isinstance(open_count, int) and open_count > 0:
            line = f"mapping_review_stale_entries={stale_open_count}/{open_count}"
            if current_revision:
                line += f" revision={current_revision}"
            print(line)
    if isinstance(automation, dict):
        all_units_installed = automation.get("all_units_installed")
        if isinstance(all_units_installed, bool):
            print(f"automation_all_units_installed={all_units_installed}")
        all_units_current = automation.get("all_units_current")
        if isinstance(all_units_current, bool):
            print(f"automation_all_units_current={all_units_current}")
        all_tracked_units_installed = automation.get("all_tracked_units_installed")
        if isinstance(all_tracked_units_installed, bool):
            print(f"automation_all_tracked_units_installed={all_tracked_units_installed}")
        all_tracked_units_current = automation.get("all_tracked_units_current")
        if isinstance(all_tracked_units_current, bool):
            print(f"automation_all_tracked_units_current={all_tracked_units_current}")
        service_enabled = automation.get("service_enabled")
        if isinstance(service_enabled, bool):
            print(f"automation_service_enabled={service_enabled}")
        service_active = automation.get("service_active")
        if isinstance(service_active, bool):
            print(f"automation_service_active={service_active}")
        missing_units = automation.get("missing_units")
        if isinstance(missing_units, list) and missing_units:
            print("automation_missing_units=" + ", ".join(str(item) for item in missing_units))
        outdated_units = automation.get("outdated_units")
        if isinstance(outdated_units, list) and outdated_units:
            print("automation_outdated_units=" + ", ".join(str(item) for item in outdated_units))
        disabled_services = automation.get("disabled_services")
        if isinstance(disabled_services, list) and disabled_services:
            print("automation_disabled_services=" + ", ".join(str(item) for item in disabled_services))
        inactive_services = automation.get("inactive_services")
        if isinstance(inactive_services, list) and inactive_services:
            print("automation_inactive_services=" + ", ".join(str(item) for item in inactive_services))
        unit_info = automation.get("unit")
        if isinstance(unit_info, dict):
            runtime_state = unit_info.get("runtime_state")
            if isinstance(runtime_state, dict) and runtime_state.get("available") is True:
                parts = [str(automation.get("unit_name") or "mal-updater.service")]
                active_state = runtime_state.get("active_state")
                sub_state = runtime_state.get("sub_state")
                last_trigger_at = runtime_state.get("last_trigger_at")
                if active_state:
                    parts.append(f"active={active_state}")
                if sub_state:
                    parts.append(f"sub={sub_state}")
                if last_trigger_at:
                    parts.append(f"last={last_trigger_at}")
                print("automation_service_runtime=" + " | ".join(parts))
    if install_units_command:
        print("automation_install_command=" + install_units_command)
    top_command = _select_maintenance_command(recommended_commands)
    _emit_recommended_command_summary("maintenance_recommended", top_command)
    top_auto_command = _select_maintenance_command(recommended_commands, require_automation_safe=True)
    _emit_recommended_command_summary("maintenance_recommended_auto", top_auto_command)
    if recommended_next:
        command = recommended_next.get("drilldown_command")
        if command:
            print("recommended_next=" + str(command))
    if recommended_apply_worklist and recommended_apply_worklist.get("command"):
        print("recommended_apply_worklist=" + str(recommended_apply_worklist["command"]))
    if recommended_refresh_worklist and recommended_refresh_worklist.get("command"):
        print("recommended_refresh_worklist=" + str(recommended_refresh_worklist["command"]))
    if recommended_worklist:
        top = recommended_worklist[0]
        if isinstance(top, dict):
            action_command = top.get("action_command")
            if action_command:
                print("recommended_action=" + str(action_command))
            resolve_command = top.get("resolve_command")
            if resolve_command:
                print("recommended_resolve=" + str(resolve_command))


def build_health_report(
    config: AppConfig,
    *,
    stale_hours: float,
    review_issue_type: str | None,
    review_worklist_limit: int,
    mapping_coverage_threshold: float,
    maintenance_review_limit: int,
) -> dict[str, object]:
    from .service_runtime import effective_niceness_policy

    ensure_directories(config)
    bootstrap_database(config.db_path)
    secrets = load_mal_secrets(config)
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    crunchyroll_state = resolve_crunchyroll_state_paths(config)
    hidive_credentials = load_hidive_credentials(config)
    hidive_state = resolve_hidive_state_paths(config)
    snapshot = get_operational_snapshot(config.db_path)
    public_userrecs_diagnostics = get_public_userrecs_diagnostics(
        config.db_path,
        configured_source_titles_per_hour=config.service.execute_limit_for("recommend_full_harvest"),
        max_pages_per_source_per_run=config.service.execute_limit_for("recommend_full_harvest_pages"),
        stale_after_days=max(1, int(config.service.recommendation_full_harvest_stale_after_days)),
    )
    credentialed_provider_slugs: list[str] = []
    if crunchyroll_credentials.username and crunchyroll_credentials.password:
        credentialed_provider_slugs.append("crunchyroll")
    if hidive_credentials.username and hidive_credentials.password:
        credentialed_provider_slugs.append("hidive")
    provider_enrichment_diagnostics = build_provider_enrichment_diagnostics(
        config,
        provider_slugs=credentialed_provider_slugs,
    )

    latest_sync_run = snapshot.get("latest_sync_run")
    latest_completed_sync_run = snapshot.get("latest_completed_sync_run")
    latest_completed_age_seconds = _age_seconds_from_timestamp(
        latest_completed_sync_run.get("completed_at") if isinstance(latest_completed_sync_run, dict) else None
    )
    latest_provider_stale_row_counts: dict[str, int] | None = None
    latest_provider_stale_row_samples: dict[str, list[dict[str, object]]] | None = None
    if isinstance(latest_sync_run, dict):
        latest_provider = latest_sync_run.get("provider")
        latest_started_at = latest_sync_run.get("started_at")
        if isinstance(latest_provider, str) and latest_provider and isinstance(latest_started_at, str) and latest_started_at:
            latest_provider_stale_row_counts = get_provider_stale_row_counts(
                config.db_path,
                provider=latest_provider,
                cutoff=latest_started_at,
            )
            latest_provider_stale_row_samples = list_provider_stale_row_samples(
                config.db_path,
                provider=latest_provider,
                cutoff=latest_started_at,
                limit=5,
                series_cutoff=latest_started_at,
            )
    provider_counts_by_provider = snapshot.get("provider_counts_by_provider") if isinstance(snapshot.get("provider_counts_by_provider"), dict) else None
    partial_sync_coverage = _build_partial_sync_coverage(
        latest_sync_run if isinstance(latest_sync_run, dict) else None,
        provider_counts_by_provider,
        latest_provider_stale_row_counts,
        latest_provider_stale_row_samples,
    )
    # HIDIVE's authority matrix is provider-specific.  A newer successful run
    # from Crunchyroll (or another provider) must not inherit HIDIVE semantics,
    # and must not hide the latest HIDIVE diagnostics either.
    latest_hidive_completed_sync_run = get_latest_completed_sync_run(
        config.db_path, provider="hidive"
    )
    provider_surface_diagnostics = _provider_surface_diagnostics_from_sync_run(
        latest_hidive_completed_sync_run
    )
    if (
        isinstance(partial_sync_coverage, dict)
        and partial_sync_coverage.get("fully_explained_by_stale_rows") is not True
        and isinstance(latest_sync_run, dict)
        and latest_sync_run.get("mode") != "full_refresh"
    ):
        latest_provider = latest_sync_run.get("provider")
        latest_full_refresh = (
            get_latest_completed_sync_run(config.db_path, provider=latest_provider, mode="full_refresh")
            if isinstance(latest_provider, str) and latest_provider
            else None
        )
        latest_full_refresh_age_seconds = _age_seconds_from_timestamp(
            latest_full_refresh.get("completed_at") if isinstance(latest_full_refresh, dict) else None
        )
        latest_full_refresh_started_at = latest_full_refresh.get("started_at") if isinstance(latest_full_refresh, dict) else None
        if (
            isinstance(latest_full_refresh, dict)
            and (latest_full_refresh_age_seconds is None or latest_full_refresh_age_seconds <= stale_hours * 3600)
            and isinstance(latest_full_refresh_started_at, str)
            and latest_full_refresh_started_at
        ):
            full_refresh_stale_row_counts = get_provider_stale_row_counts(
                config.db_path,
                provider=latest_provider,
                cutoff=latest_full_refresh_started_at,
            )
            full_refresh_stale_row_samples = list_provider_stale_row_samples(
                config.db_path,
                provider=latest_provider,
                cutoff=latest_full_refresh_started_at,
                limit=5,
                series_cutoff=latest_full_refresh_started_at,
            )
            full_refresh_coverage = _build_partial_sync_coverage(
                latest_full_refresh,
                provider_counts_by_provider,
                full_refresh_stale_row_counts,
                full_refresh_stale_row_samples,
            )
            if isinstance(full_refresh_coverage, dict) and full_refresh_coverage.get("fully_explained_by_stale_rows") is True:
                full_refresh_coverage["latest_incremental_sync_run_id"] = latest_sync_run.get("id")
                partial_sync_coverage = full_refresh_coverage
    mapping_coverage = _build_mapping_coverage_snapshot(
        snapshot.get("provider_series_inventory") if isinstance(snapshot.get("provider_series_inventory"), dict) else None,
        snapshot.get("mappings") if isinstance(snapshot.get("mappings"), dict) else None,
    )
    automation_installation = _build_automation_installation_status(config)
    service_state = load_service_state(config)
    provider_auth_failures = {
        provider: payload
        for provider in ("crunchyroll", "hidive")
        if (payload := provider_service_auth_failure(service_state, provider=provider, config=config)) is not None
    }
    mal_auth_failure = mal_service_auth_failure(service_state)

    warnings: list[dict[str, object]] = []

    if not crunchyroll_credentials.username or not crunchyroll_credentials.password:
        warnings.append({"code": "missing_crunchyroll_credentials", "detail": "Crunchyroll username/password secrets are not both present"})
    if not crunchyroll_state.refresh_token_path.exists() or not crunchyroll_state.device_id_path.exists():
        warnings.append({"code": "missing_crunchyroll_state", "detail": "Crunchyroll refresh token or device id is missing"})
    if hidive_credentials.username or hidive_credentials.password:
        if not (hidive_credentials.username and hidive_credentials.password):
            warnings.append({"code": "missing_hidive_credentials", "detail": "HIDIVE username/password secrets are not both present"})
    if (hidive_credentials.username and hidive_credentials.password) and (
        not hidive_state.access_token_path.exists() or not hidive_state.refresh_token_path.exists()
    ):
        warnings.append({"code": "missing_hidive_state", "detail": "HIDIVE authorisation token or refresh token is missing"})
    if not secrets.client_id or not secrets.access_token or not secrets.refresh_token:
        warnings.append({"code": "missing_mal_auth_material", "detail": "MAL client id/access token/refresh token are not all present"})
    missing_automation_units = automation_installation.get("missing_required_units") if isinstance(automation_installation, dict) else None
    outdated_automation_units = automation_installation.get("outdated_required_units") if isinstance(automation_installation, dict) else None
    disabled_automation_services = automation_installation.get("disabled_services") if isinstance(automation_installation, dict) else None
    inactive_automation_services = automation_installation.get("inactive_services") if isinstance(automation_installation, dict) else None
    if isinstance(missing_automation_units, list) and missing_automation_units:
        warnings.append(
            {
                "code": "automation_units_missing",
                "detail": "Repo-owned MAL-Updater user systemd units are not fully installed for this user",
                "missing_units": missing_automation_units,
                "target_dir": automation_installation.get("target_dir"),
            }
        )
    if isinstance(outdated_automation_units, list) and outdated_automation_units:
        warnings.append(
            {
                "code": "automation_units_outdated",
                "detail": "Installed repo-owned MAL-Updater user systemd units do not match the current checked-in repo versions",
                "outdated_units": outdated_automation_units,
                "target_dir": automation_installation.get("target_dir"),
            }
        )
    if isinstance(disabled_automation_services, list) and disabled_automation_services:
        warnings.append(
            {
                "code": "automation_service_disabled",
                "detail": "Repo-owned MAL-Updater user systemd service is installed but not enabled for this user",
                "disabled_services": disabled_automation_services,
            }
        )
    if isinstance(inactive_automation_services, list) and inactive_automation_services:
        warnings.append(
            {
                "code": "automation_service_inactive",
                "detail": "Repo-owned MAL-Updater user systemd service is enabled on disk but is not currently active in the user systemd runtime",
                "inactive_services": inactive_automation_services,
            }
        )
    if not isinstance(latest_completed_sync_run, dict):
        warnings.append({"code": "no_completed_ingest_snapshot", "detail": "No completed sync_runs row exists yet"})
    if isinstance(latest_sync_run, dict) and latest_sync_run.get("status") == "failed":
        warnings.append({"code": "latest_sync_run_failed", "detail": "Latest sync_runs row is failed", "sync_run_id": latest_sync_run.get("id")})
    if latest_completed_age_seconds is not None and latest_completed_age_seconds > stale_hours * 3600:
        warnings.append(
            {
                "code": "completed_snapshot_stale",
                "detail": f"Latest completed ingest snapshot is older than {stale_hours:g} hours",
                "age_seconds": latest_completed_age_seconds,
            }
        )
    if isinstance(partial_sync_coverage, dict):
        fully_explained_by_stale_rows = partial_sync_coverage.get("fully_explained_by_stale_rows") is True
        detail = "Latest completed ingest touched fewer provider rows than currently exist in the local cache; freshness is only partial until incremental backfill catches up"
        if fully_explained_by_stale_rows:
            classifications = {
                field.get("classification")
                for field in (partial_sync_coverage.get("fields") or {}).values()
                if isinstance(field, dict)
            }
            if classifications == {"stale_or_deleted_provider_rows"}:
                detail = "Latest ingest touched fewer rows than the local cache, and the gap is fully explained by older last_seen rows that may be stale/deleted upstream"
            else:
                detail = "Latest incremental ingest intentionally touched front-of-line rows while older cached rows remain queued for slow incremental backfill"
        posture = _classify_partial_sync_health_posture(
            partial_sync_coverage,
            latest_completed_age_seconds=latest_completed_age_seconds,
            stale_hours=stale_hours,
        )
        warnings.append(
            {
                "code": "latest_sync_run_stale_provider_rows" if fully_explained_by_stale_rows else "latest_sync_run_partial_coverage",
                "detail": detail,
                "severity": posture["severity"],
                "health_posture": posture["health_posture"],
                "automation_safe": posture["automation_safe"],
                "rationale": posture.get("rationale"),
                "sync_run_id": partial_sync_coverage.get("sync_run_id"),
                "mode": partial_sync_coverage.get("mode"),
                "backfill_progress": partial_sync_coverage.get("backfill_progress"),
                "fields": partial_sync_coverage.get("fields"),
            }
        )
    if isinstance(provider_surface_diagnostics, dict):
        provider_name = provider_surface_diagnostics.get("provider")
        codes = provider_surface_diagnostics.get("codes")
        code_label = ", ".join(str(code) for code in codes) if isinstance(codes, list) else "provider surface diagnostics"
        warnings.append(
            {
                "code": f"{provider_name}_surface_diagnostics" if isinstance(provider_name, str) and provider_name else "provider_surface_diagnostics",
                "detail": (
                    f"Latest completed {provider_name or 'provider'} ingest reported provider-surface authority diagnostics: "
                    f"{code_label}. Info/operator-visible means the expected documented limitation is safely bounded; "
                    "warning/unhealthy means authority or completeness remains unproven."
                ),
                "severity": provider_surface_diagnostics.get("severity"),
                "health_posture": provider_surface_diagnostics.get("health_posture"),
                "automation_safe": provider_surface_diagnostics.get("automation_safe"),
                "sync_run_id": provider_surface_diagnostics.get("sync_run_id"),
                "mode": provider_surface_diagnostics.get("mode"),
                "rationale": provider_surface_diagnostics.get("rationale"),
                "authority": provider_surface_diagnostics.get("authority"),
                "diagnostics": provider_surface_diagnostics.get("diagnostics"),
            }
        )
    for provider, failure in provider_auth_failures.items():
        failure_label = describe_auth_failure_kind({
            "kind": str(failure.get("auth_failure_kind")),
            "label": str(failure.get("auth_failure_label")),
        })
        warning = {
            "code": f"{provider}_auth_failures_repeated",
            "detail": f"Repeated unattended {provider} fetch failures look auth-related ({failure_label}) and likely need auth re-bootstrap",
            **failure,
        }
        warnings.append(warning)
    if isinstance(mal_auth_failure, dict):
        failure_label = describe_auth_failure_kind({
            "kind": str(mal_auth_failure.get("auth_failure_kind")),
            "label": str(mal_auth_failure.get("auth_failure_label")),
        })
        warnings.append(
            {
                "code": "mal_auth_failures_repeated",
                "detail": f"Repeated unattended MAL token refresh failures look auth-related ({failure_label}) and likely need MAL OAuth again",
                **mal_auth_failure,
            }
        )
    coverage_ratio = mapping_coverage.get("eligible_approved_coverage_ratio") if isinstance(mapping_coverage, dict) else None
    unmapped_series_count = mapping_coverage.get("eligible_unmapped_series_count") if isinstance(mapping_coverage, dict) else None
    if (
        not isinstance(partial_sync_coverage, dict)
        and isinstance(coverage_ratio, float)
        and isinstance(unmapped_series_count, int)
        and unmapped_series_count > 0
        and coverage_ratio < mapping_coverage_threshold
    ):
        warnings.append(
            {
                "code": "approved_mapping_coverage_below_threshold",
                "detail": f"Approved mapping coverage is below the configured {mapping_coverage_threshold * 100:.1f}% threshold",
                "coverage": mapping_coverage,
                "threshold_ratio": round(mapping_coverage_threshold, 4),
            }
        )

    review_queue = snapshot.get("review_queue") if isinstance(snapshot.get("review_queue"), dict) else {}
    open_review_counts = review_queue.get("open") if isinstance(review_queue.get("open"), dict) else {}
    open_review_total = sum(value for value in open_review_counts.values() if isinstance(value, int))
    review_queue_next: dict[str, object] | None = None
    review_queue_worklist: list[dict[str, object]] = []
    review_queue_apply_worklist: dict[str, object] | None = None
    review_queue_refresh_worklist: dict[str, object] | None = None
    mapping_review_items = list_review_queue_entries(
        config.db_path,
        status="open",
        issue_type="mapping_review",
    )
    mapping_review_revision = _build_mapping_review_revision_snapshot(mapping_review_items)
    if isinstance(mapping_review_revision, dict) and mapping_review_revision.get("stale_open_count"):
        warnings.append(
            {
                "code": "mapping_review_queue_stale_heuristics",
                "detail": "Open mapping-review rows were produced by older or unknown mapper heuristics and should be refreshed before manual triage",
                "current_revision": mapping_review_revision.get("current_revision"),
                "stale_open_count": mapping_review_revision.get("stale_open_count"),
                "open_count": mapping_review_revision.get("open_count"),
                "stale_examples": mapping_review_revision.get("stale_examples"),
            }
        )
    if open_review_total > 0:
        warnings.append(
            {
                "code": "open_review_queue",
                "detail": "Open review backlog exists",
                "open_count": open_review_total,
                "by_issue_type": open_review_counts,
            }
        )
        recommendation_items = list_review_queue_entries(
            config.db_path,
            status="open",
            issue_type=review_issue_type,
        )
        provider_series_titles = get_provider_series_title_map_by_keys(
            config.db_path,
            provider_series_keys=[
                (item.provider, item.provider_series_id)
                for item in recommendation_items
                if item.provider and item.provider_series_id
            ],
        )
        review_queue_summary = _summarize_review_queue(
            recommendation_items,
            status="open",
            issue_type=review_issue_type,
            provider_series_titles=provider_series_titles,
        )
        review_queue_next = _select_review_queue_next_bucket(review_queue_summary, bucket="cluster-strategy")
        if review_queue_next is None:
            for fallback_bucket in ("fix-strategy", "title-cluster", "reason", "decision"):
                review_queue_next = _select_review_queue_next_bucket(review_queue_summary, bucket=fallback_bucket)
                if review_queue_next is not None:
                    break
        review_queue_worklist = _build_review_queue_worklist(
            review_queue_summary,
            bucket_order=["cluster-strategy", "fix-strategy", "title-cluster", "reason", "decision"],
            limit=review_worklist_limit,
        )
        if review_queue_worklist:
            apply_args = _review_queue_apply_worklist_args(
                status="open",
                issue_type=review_issue_type,
                limit=review_worklist_limit,
                per_bucket_limit=20,
            )
            review_queue_apply_worklist = {
                "status_from": "open",
                "status_to": "resolved",
                "bucket_limit": review_worklist_limit,
                "per_bucket_limit": 20,
                "command_args": apply_args,
                "command": _build_review_queue_command(apply_args),
            }
            if review_issue_type != "sync_review":
                refresh_issue_type = review_issue_type if review_issue_type == "mapping_review" else None
                refresh_args = _review_queue_refresh_worklist_args(
                    status="open",
                    issue_type=refresh_issue_type,
                    limit=review_worklist_limit,
                    per_bucket_limit=20,
                    mapping_limit=5,
                )
                review_queue_refresh_worklist = {
                    "status": "open",
                    "issue_type": refresh_issue_type or "mapping_review",
                    "bucket_limit": review_worklist_limit,
                    "per_bucket_limit": 20,
                    "mapping_limit": 5,
                    "command_args": refresh_args,
                    "command": _build_review_queue_command(refresh_args),
                }

    review_queue_refresh_command_args = None
    if isinstance(review_queue_next, dict):
        refresh_args = review_queue_next.get("refresh_args")
        if isinstance(refresh_args, list) and refresh_args:
            review_queue_refresh_command_args = [str(item) for item in refresh_args if item is not None]

    review_queue_refresh_worklist_command_args = None
    if isinstance(review_queue_refresh_worklist, dict):
        refresh_worklist_args = review_queue_refresh_worklist.get("command_args")
        refresh_worklist_provider_series_ids: set[str] = set()
        for item in review_queue_worklist:
            if not isinstance(item, dict):
                continue
            provider_series_ids = item.get("refresh_provider_series_ids")
            if not isinstance(provider_series_ids, list):
                continue
            for provider_series_id in provider_series_ids:
                if isinstance(provider_series_id, str) and provider_series_id.strip():
                    refresh_worklist_provider_series_ids.add(provider_series_id)
        if isinstance(refresh_worklist_args, list) and len(refresh_worklist_provider_series_ids) > 1:
            review_queue_refresh_worklist_command_args = [str(item) for item in refresh_worklist_args if item is not None]

    maintenance_commands = _build_health_maintenance_commands(
        crunchyroll_credentials_present=bool(crunchyroll_credentials.username and crunchyroll_credentials.password),
        crunchyroll_state_present=crunchyroll_state.refresh_token_path.exists() and crunchyroll_state.device_id_path.exists(),
        hidive_credentials_present=bool(hidive_credentials.username and hidive_credentials.password),
        hidive_state_present=hidive_state.access_token_path.exists() and hidive_state.refresh_token_path.exists(),
        mal_client_id_present=bool(secrets.client_id),
        mal_auth_present=bool(secrets.client_id and secrets.access_token and secrets.refresh_token),
        mal_auth_failure=mal_auth_failure,
        latest_sync_run=latest_sync_run if isinstance(latest_sync_run, dict) else None,
        latest_completed_sync_run=latest_completed_sync_run if isinstance(latest_completed_sync_run, dict) else None,
        latest_completed_age_seconds=latest_completed_age_seconds,
        stale_hours=stale_hours,
        crunchyroll_snapshot_output_path=Path(os.path.relpath(config.cache_dir / "live-crunchyroll-snapshot.json", config.project_root)),
        hidive_snapshot_output_path=Path(os.path.relpath(config.cache_dir / "live-hidive-snapshot.json", config.project_root)),
        partial_sync_coverage=partial_sync_coverage,
        mapping_coverage=mapping_coverage,
        mapping_coverage_threshold=mapping_coverage_threshold,
        maintenance_review_limit=maintenance_review_limit,
        automation_installation=automation_installation,
        review_queue_refresh_command_args=review_queue_refresh_command_args,
        review_queue_refresh_worklist_command_args=review_queue_refresh_worklist_command_args,
        provider_auth_failures=provider_auth_failures,
    )

    health_blocking_warnings = [
        warning
        for warning in warnings
        if not (isinstance(warning, dict) and warning.get("health_posture") == "operator_visible")
    ]

    payload = {
        "healthy": not health_blocking_warnings,
        "stale_hours_threshold": stale_hours,
        "niceness_policy": effective_niceness_policy(config),
        "warnings": warnings,
        "maintenance": {
            "recommended_commands": maintenance_commands,
            "recommended_command": _select_maintenance_command(maintenance_commands),
            "recommended_automation_command": _select_maintenance_command(
                maintenance_commands,
                require_automation_safe=True,
            ),
        },
        "paths": {
            "project_root": str(config.project_root),
            "db_path": str(config.db_path),
            "sync_boundary_path": str(crunchyroll_state.sync_boundary_path),
            "provider_state_roots": {
                "crunchyroll": str(crunchyroll_state.root),
                "hidive": str(hidive_state.root),
            },
            "provider_runtime_paths": {
                "crunchyroll": {
                    "refresh_token_path": str(crunchyroll_state.refresh_token_path),
                    "device_id_path": str(crunchyroll_state.device_id_path),
                    "session_state_path": str(crunchyroll_state.session_state_path),
                    "sync_boundary_path": str(crunchyroll_state.sync_boundary_path),
                },
                "hidive": {
                    "authorisation_token_path": str(hidive_state.access_token_path),
                    "refresh_token_path": str(hidive_state.refresh_token_path),
                    "session_state_path": str(hidive_state.session_state_path),
                },
            },
        },
        "automation": automation_installation,
        "auth": {
            "crunchyroll": {
                "username_present": bool(crunchyroll_credentials.username),
                "password_present": bool(crunchyroll_credentials.password),
                "refresh_token_present": crunchyroll_state.refresh_token_path.exists(),
                "device_id_present": crunchyroll_state.device_id_path.exists(),
                "sync_boundary_present": crunchyroll_state.sync_boundary_path.exists(),
            },
            "hidive": {
                "username_present": bool(hidive_credentials.username),
                "password_present": bool(hidive_credentials.password),
                "authorisation_token_present": hidive_state.access_token_path.exists(),
                "refresh_token_present": hidive_state.refresh_token_path.exists(),
            },
            "mal": {
                "client_id_present": bool(secrets.client_id),
                "access_token_present": bool(secrets.access_token),
                "refresh_token_present": bool(secrets.refresh_token),
            },
        },
        "coverage": {
            "public_userrecs": public_userrecs_diagnostics,
        },
        "operational": {
            "provider_enrichment": provider_enrichment_diagnostics,
        },
        "latest_sync_run": latest_sync_run,
        "latest_completed_sync_run": latest_completed_sync_run,
        "latest_completed_sync_run_age_seconds": latest_completed_age_seconds,
        "provider_counts": snapshot.get("provider_counts"),
        "provider_counts_by_provider": snapshot.get("provider_counts_by_provider"),
        "provider_freshness": snapshot.get("provider_freshness"),
        "provider_freshness_by_provider": snapshot.get("provider_freshness_by_provider"),
        "partial_sync_coverage": partial_sync_coverage,
        "provider_surface_diagnostics": provider_surface_diagnostics,
        "review_queue": {
            **review_queue,
            "mapping_review_revision": mapping_review_revision,
            "recommendation_issue_type_filter": review_issue_type,
            "recommendation_worklist_limit": review_worklist_limit,
            "recommended_next": review_queue_next,
            "recommended_worklist": review_queue_worklist,
            "recommended_apply_worklist": review_queue_apply_worklist,
            "recommended_refresh_worklist": review_queue_refresh_worklist,
        },
        "mappings": {
            **(snapshot.get("mappings") if isinstance(snapshot.get("mappings"), dict) else {}),
            "coverage": mapping_coverage,
            "coverage_threshold": round(mapping_coverage_threshold, 4),
        },
    }
    return payload


def render_health_report_summary(payload: dict[str, object]) -> str:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        _emit_health_check_summary(payload)
    return buffer.getvalue()


def render_health_report_json(payload: dict[str, object], *, trailing_newline: bool = True) -> str:
    text = json.dumps(payload, indent=2)
    return text + "\n" if trailing_newline else text


def render_health_report(payload: dict[str, object], *, output_format: str) -> str:
    if output_format == "summary":
        return render_health_report_summary(payload)
    return render_health_report_json(payload)


def health_report_exit_code(payload: dict[str, object], *, strict: bool) -> int:
    warnings = payload.get("warnings")
    if strict and isinstance(warnings, list) and warnings:
        return 2
    return 0


def run_health_report(
    project_root: Path | None,
    *,
    stale_hours: float,
    strict: bool,
    review_issue_type: str | None,
    review_worklist_limit: int,
    output_format: str,
    mapping_coverage_threshold: float,
    maintenance_review_limit: int,
) -> tuple[int, dict[str, object], str]:
    config = load_config(project_root)
    payload = build_health_report(
        config,
        stale_hours=stale_hours,
        review_issue_type=review_issue_type,
        review_worklist_limit=review_worklist_limit,
        mapping_coverage_threshold=mapping_coverage_threshold,
        maintenance_review_limit=maintenance_review_limit,
    )
    return (
        health_report_exit_code(payload, strict=strict),
        payload,
        render_health_report(payload, output_format=output_format),
    )
