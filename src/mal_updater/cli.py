from __future__ import annotations

import io
import json
import os
import sys
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import shutil
import uuid

from .auth import OAuthCallbackError, format_auth_flow_prompt, persist_token_response, wait_for_oauth_callback
from . import bootstrap_guidance as _bootstrap_guidance
from . import health_report as _health_report
from . import review_queue_support as _review_queue_support
from .cli_parser import build_parser
from .config import ConfigError, ensure_directories, load_config, load_mal_secrets, mal_callback_bind_warning
from .openclaw_delivery import OpenClawDeliveryError, deliver_recommendations_via_openclaw
from .crunchyroll_auth import (
    CrunchyrollAuthError,
    crunchyroll_login_with_credentials,
    load_crunchyroll_credentials,
    resolve_crunchyroll_state_paths,
)
from .crunchyroll_snapshot import CrunchyrollSnapshotError
from .hidive_auth import HidiveAuthError, load_hidive_credentials, resolve_hidive_state_paths
from .hidive_snapshot import HidiveSnapshotError
from . import provider_snapshot
from .provider_registry import get_provider, list_provider_slugs
from .request_tracking import begin_api_request_context, current_api_request_context, end_api_request_context
from .redaction import sanitize_text
from .runtime_retention_audit import (
    AuditCaps,
    AuditOptions,
    WarningThresholds,
    build_runtime_retention_audit_payload,
    render_runtime_retention_audit_summary,
)
from . import providers as _providers  # noqa: F401
from .db import (
    backfill_hidive_series_urls,
    bootstrap_database,
    get_mal_recommendation_harvest_coverage,
    get_latest_completed_sync_run,
    get_operational_snapshot,
    get_provider_stale_row_age_buckets,
    get_provider_stale_row_counts,
    get_provider_stale_row_last_seen_ranges,
    get_provider_stale_row_linkage,
    insert_recommendation_snapshot_rows,
    list_latest_recommendation_snapshot_rows,
    list_provider_stale_row_samples,
    get_provider_series_title_map_by_keys,
    list_review_queue_entries,
    list_series_mappings,
    refresh_review_queue_entries,
    update_review_queue_entry_statuses,
    upsert_series_mapping,
)
from .ingestion import ingest_snapshot_file, ingest_snapshot_payload
from .mal_client import MalApiError, MalClient
from .mapping import SeriesMappingInput, map_series, normalize_title
from .recommendation_dashboard import (
    DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT,
    recommendation_snapshot_availability_payload,
    recommendation_snapshot_row_base_payload,
    serve_dashboard,
    write_recommendation_dashboard,
)
from .recommendation_enrichment import enrich_discovery_provider_availability
from .recommendation_metadata import refresh_full_user_recommendation_harvest, refresh_mal_user_anime_list_cache, refresh_recommendation_metadata
from .recommendations import build_recommendations, group_recommendations, trim_grouped_recommendations
from .service_manager import doctor_service, install_service, restart_service, service_status, start_service, stop_service, uninstall_service
from .service_runtime import run_maintenance_cycle, run_pending_tasks, run_service_loop
from . import service_systemd_status as _service_systemd_status
from .service_units import SERVICE_UNIT_NAME, render_systemd_unit_template_file as _render_systemd_unit_template
from .sync_planner import (
    MAPPING_REVIEW_HEURISTICS_REVISION,
    MAPPING_REVIEW_NO_QUEUE_DECISIONS,
    build_dry_run_sync_plan,
    build_mapping_review,
    execute_approved_sync,
    load_provider_series_states,
    persist_mapping_review_queue,
    persist_sync_review_queue,
)
from .validation import SnapshotValidationError, validate_snapshot_payload

_filter_review_queue_items = _review_queue_support._filter_review_queue_items
_review_queue_item_label = _review_queue_support._review_queue_item_label
_summarize_review_queue = _review_queue_support._summarize_review_queue
_age_seconds_from_timestamp = _health_report._age_seconds_from_timestamp


def _cmd_init(project_root: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    print(f"project_root={config.project_root}")
    print(f"workspace_root={config.workspace_root}")
    print(f"runtime_root={config.runtime_root}")
    print(f"db_path={config.db_path}")
    return 0


def _cmd_status(project_root: Path | None) -> int:
    config = load_config(project_root)
    secrets = load_mal_secrets(config)
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    crunchyroll_state = resolve_crunchyroll_state_paths(config)
    hidive_credentials = load_hidive_credentials(config)
    hidive_state = resolve_hidive_state_paths(config)
    print(f"project_root={config.project_root}")
    print(f"workspace_root={config.workspace_root}")
    print(f"runtime_root={config.runtime_root}")
    print(f"settings_path={config.settings_path}")
    print(f"config_dir={config.config_dir}")
    print(f"secrets_dir={config.secrets_dir}")
    print(f"data_dir={config.data_dir}")
    print(f"state_dir={config.state_dir}")
    print(f"cache_dir={config.cache_dir}")
    print(f"db_path={config.db_path}")
    print(f"contract_version={config.contract_version}")
    print(f"request_timeout_seconds={config.request_timeout_seconds}")
    print(f"completion_threshold={config.completion_threshold}")
    print(f"credits_skip_window_seconds={config.credits_skip_window_seconds}")
    print(f"mal.base_url={config.mal.base_url}")
    print(f"mal.public_base_url={config.mal.public_base_url}")
    print(f"mal.auth_url={config.mal.auth_url}")
    print(f"mal.token_url={config.mal.token_url}")
    print(f"mal.bind_host={config.mal.bind_host}")
    print(f"mal.non_loopback_callback_ack={config.mal.non_loopback_callback_ack}")
    warning = mal_callback_bind_warning(config.mal)
    if warning:
        print(f"mal.callback_bind_warning={warning}")
    print(f"mal.redirect_uri={config.mal.redirect_uri}")
    print(f"mal.request_spacing_seconds={config.mal.request_spacing_seconds}")
    print(f"mal.request_spacing_jitter_seconds={config.mal.request_spacing_jitter_seconds}")
    print(f"mal.retry_max_attempts={config.mal.retry_max_attempts}")
    print(f"mal.retry_after_cap_seconds={config.mal.retry_after_cap_seconds}")
    print(f"crunchyroll.locale={config.crunchyroll.locale}")
    print(f"crunchyroll.request_spacing_seconds={config.crunchyroll.request_spacing_seconds}")
    print(f"crunchyroll.request_spacing_jitter_seconds={config.crunchyroll.request_spacing_jitter_seconds}")
    print(f"crunchyroll.retry_max_attempts={config.crunchyroll.retry_max_attempts}")
    print(f"crunchyroll.retry_after_cap_seconds={config.crunchyroll.retry_after_cap_seconds}")
    print(f"crunchyroll.username_present={bool(crunchyroll_credentials.username)}")
    print(f"crunchyroll.password_present={bool(crunchyroll_credentials.password)}")
    print(f"crunchyroll.username_path={crunchyroll_credentials.username_path}")
    print(f"crunchyroll.password_path={crunchyroll_credentials.password_path}")
    print(f"crunchyroll.state_root={crunchyroll_state.root}")
    print(f"crunchyroll.refresh_token_path={crunchyroll_state.refresh_token_path}")
    print(f"crunchyroll.device_id_path={crunchyroll_state.device_id_path}")
    print(f"crunchyroll.session_state_path={crunchyroll_state.session_state_path}")
    print(f"crunchyroll.sync_boundary_path={crunchyroll_state.sync_boundary_path}")
    print(f"crunchyroll.refresh_token_present={crunchyroll_state.refresh_token_path.exists()}")
    print(f"crunchyroll.device_id_present={crunchyroll_state.device_id_path.exists()}")
    print(f"crunchyroll.sync_boundary_present={crunchyroll_state.sync_boundary_path.exists()}")
    print(f"hidive.username_present={bool(hidive_credentials.username)}")
    print(f"hidive.request_spacing_seconds={config.hidive.request_spacing_seconds}")
    print(f"hidive.request_spacing_jitter_seconds={config.hidive.request_spacing_jitter_seconds}")
    print(f"hidive.retry_max_attempts={config.hidive.retry_max_attempts}")
    print(f"hidive.retry_after_cap_seconds={config.hidive.retry_after_cap_seconds}")
    print(f"hidive.password_present={bool(hidive_credentials.password)}")
    print(f"hidive.username_path={hidive_credentials.username_path}")
    print(f"hidive.password_path={hidive_credentials.password_path}")
    print(f"hidive.state_root={hidive_state.root}")
    print(f"hidive.authorisation_token_path={hidive_state.access_token_path}")
    print(f"hidive.refresh_token_path={hidive_state.refresh_token_path}")
    print(f"hidive.session_state_path={hidive_state.session_state_path}")
    print(f"hidive.authorisation_token_present={hidive_state.access_token_path.exists()}")
    print(f"hidive.refresh_token_present={hidive_state.refresh_token_path.exists()}")
    print(f"mal.client_id_present={bool(secrets.client_id)}")
    print(f"mal.client_secret_present={bool(secrets.client_secret)}")
    print(f"mal.access_token_present={bool(secrets.access_token)}")
    print(f"mal.refresh_token_present={bool(secrets.refresh_token)}")
    print(f"mal.client_id_path={secrets.client_id_path}")
    print(f"mal.client_secret_path={secrets.client_secret_path}")
    print(f"mal.access_token_path={secrets.access_token_path}")
    print(f"mal.refresh_token_path={secrets.refresh_token_path}")
    return 0


def _service_status_strict_failures(payload: dict[str, object]) -> list[str]:
    failures: list[str] = []
    if payload.get("systemctl_available") is False:
        failures.append("systemctl_unavailable")
    if payload.get("unit_exists") is not True:
        failures.append("main_unit_missing")
    if payload.get("enabled") is not True:
        failures.append("main_unit_not_enabled")
    if payload.get("active") is not True:
        failures.append("main_unit_not_active")
    env_exists = payload.get("env_exists")
    if env_exists is not True:
        failures.append("service_env_missing")
    elif payload.get("env_restrictive") is not True:
        if payload.get("env_mode_octal") is None:
            failures.append("service_env_permissions_unknown")
        else:
            failures.append("service_env_not_0600")
    for key, reason in (
        ("service_state_parse_error", "service_state_parse_error"),
        ("health_latest_parse_error", "health_latest_parse_error"),
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            failures.append(reason)
    return failures


def _cmd_service_status(project_root: Path | None, output_format: str, *, strict: bool = False) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    payload = doctor_service(config)
    strict_failures = _service_status_strict_failures(payload)
    if strict:
        payload = {**payload, "strict": {"ok": not strict_failures, "failures": strict_failures, "unit_name": SERVICE_UNIT_NAME}}
    if output_format == "summary":
        _emit_service_status_summary(payload)
    else:
        print(json.dumps(payload, indent=2))
    return 2 if strict and strict_failures else 0


def _cmd_install_service(project_root: Path | None, start_now: bool, *, install_dashboard: bool = False, enable_dashboard: bool = False) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    result = install_service(start_now=start_now, config=config, install_dashboard=install_dashboard, enable_dashboard=enable_dashboard)
    print(json.dumps(result.details or {"status": result.status, "message": result.message}, indent=2))
    return 0


def _cmd_uninstall_service(stop_now: bool) -> int:
    result = uninstall_service(stop_now=stop_now)
    print(json.dumps(result.details or {"status": result.status, "message": result.message}, indent=2))
    return 0


def _cmd_start_service() -> int:
    result = start_service()
    print(json.dumps(result.details or {"status": result.status, "message": result.message}, indent=2))
    return 0


def _cmd_stop_service() -> int:
    result = stop_service()
    print(json.dumps(result.details or {"status": result.status, "message": result.message}, indent=2))
    return 0


def _cmd_restart_service() -> int:
    result = restart_service()
    print(json.dumps(result.details or {"status": result.status, "message": result.message}, indent=2))
    return 0


def _cmd_service_run_once(project_root: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    print(json.dumps(run_pending_tasks(config), indent=2))
    return 0


def _cmd_service_run(project_root: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    return run_service_loop(config)


def _cmd_recommend_maintain(
    project_root: Path | None,
    *,
    dry_run: bool,
    metadata_limit: int,
    discovery_target_limit: int,
    recommendation_limit: int,
    mapping_limit: int,
    mal_list_max_pages: int,
    provider_max_history_pages: int | None,
    provider_max_watchlist_pages: int | None,
    skip_provider_refresh: bool,
    local_only: bool,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    result = run_maintenance_cycle(
        config,
        dry_run=dry_run,
        metadata_limit=metadata_limit,
        discovery_target_limit=discovery_target_limit,
        recommendation_limit=recommendation_limit,
        mapping_limit=mapping_limit,
        mal_list_max_pages=mal_list_max_pages,
        provider_max_history_pages=provider_max_history_pages,
        provider_max_watchlist_pages=provider_max_watchlist_pages,
        include_provider_refresh=not skip_provider_refresh,
        local_only=local_only,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("status") in {"ok", "dry_run", "skipped"} else 1


_runtime_initialization_status = _bootstrap_guidance._runtime_initialization_status
_secrets_dir_permission_status = _bootstrap_guidance._secrets_dir_permission_status
_guidance_command_fields = _bootstrap_guidance._guidance_command_fields
_provider_from_refresh_command_args = _bootstrap_guidance._provider_from_refresh_command_args
_normalized_provider_fetch_command_args = _bootstrap_guidance._normalized_provider_fetch_command_args
_provider_task_clears_health_refresh_recommendation = _bootstrap_guidance._provider_task_clears_health_refresh_recommendation
_provider_bootstrap_health_refresh_recommendation = _bootstrap_guidance._provider_bootstrap_health_refresh_recommendation
_bootstrap_health_review_recommendations = _bootstrap_guidance._bootstrap_health_review_recommendations
_provider_bootstrap_guidance_status = _bootstrap_guidance._provider_bootstrap_guidance_status
_mal_bootstrap_guidance_status = _bootstrap_guidance._mal_bootstrap_guidance_status
_bootstrap_operation_mode_status = _bootstrap_guidance._bootstrap_operation_mode_status


def _cmd_bootstrap_audit(project_root: Path | None, summary_only: bool) -> int:
    config = load_config(project_root)
    payload = _bootstrap_guidance.build_bootstrap_audit_payload(config)
    if summary_only:
        sys.stdout.write(_bootstrap_guidance.render_bootstrap_audit_summary(payload))
    else:
        sys.stdout.write(_bootstrap_guidance.render_bootstrap_audit_json(payload))
    return 0


def _non_negative_int(value: int, *, minimum: int = 0) -> int:
    return max(minimum, int(value))


def _non_negative_float(value: float | None) -> float | None:
    if value is None:
        return None
    return max(0.0, float(value))


def _cmd_runtime_retention_audit(
    project_root: Path | None,
    output_format: str,
    *,
    strict: bool,
    max_files_per_family: int,
    max_dirs_per_family: int,
    max_depth: int,
    max_scan_errors_per_family: int,
    warn_file_count: int | None,
    warn_total_bytes: int | None,
    warn_oldest_days: float | None,
) -> int:
    config = load_config(project_root)
    options = AuditOptions(
        caps=AuditCaps(
            max_files_per_family=_non_negative_int(max_files_per_family, minimum=1),
            max_dirs_per_family=_non_negative_int(max_dirs_per_family, minimum=1),
            max_depth=_non_negative_int(max_depth),
            max_scan_errors_per_family=_non_negative_int(max_scan_errors_per_family),
        ),
        warning_threshold_overrides=WarningThresholds(
            file_count=None if warn_file_count is None else _non_negative_int(warn_file_count),
            total_bytes=None if warn_total_bytes is None else _non_negative_int(warn_total_bytes),
            oldest_days=_non_negative_float(warn_oldest_days),
        ),
        strict=strict,
    )
    payload = build_runtime_retention_audit_payload(config, options)
    if output_format == "summary":
        sys.stdout.write(render_runtime_retention_audit_summary(payload))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))
    strict_status = payload.get("strict") if isinstance(payload.get("strict"), dict) else {}
    return 2 if strict and strict_status.get("would_fail") is True else 0


def _emit_service_status_summary(payload: dict[str, object]) -> None:
    print(f"unit_exists={bool(payload.get('unit_exists'))}")
    print(f"enabled={bool(payload.get('enabled'))}")
    print(f"active={bool(payload.get('active'))}")
    systemctl_status = payload.get("systemctl_status") if isinstance(payload.get("systemctl_status"), str) else None
    if systemctl_status:
        print(f"systemctl_status={systemctl_status}")
    if payload.get("systemctl_available") is not None:
        print(f"systemctl_available={bool(payload.get('systemctl_available'))}")
    systemctl_error = payload.get("systemctl_error") if isinstance(payload.get("systemctl_error"), str) else None
    if systemctl_error:
        print(f"systemctl_error={systemctl_error}")
    if payload.get("enabled_raw"):
        print(f"enabled_raw={payload['enabled_raw']}")
    if payload.get("active_raw"):
        print(f"active_raw={payload['active_raw']}")
    strict_status = payload.get("strict") if isinstance(payload.get("strict"), dict) else None
    if strict_status is not None:
        print(f"strict_ok={bool(strict_status.get('ok'))}")
        strict_failures = strict_status.get("failures") if isinstance(strict_status.get("failures"), list) else []
        if strict_failures:
            print("strict_failures=" + ", ".join(str(item) for item in strict_failures))
    print(f"env_exists={bool(payload.get('env_exists'))}")
    if payload.get("env_mode_octal") is not None:
        print(f"env_mode={payload['env_mode_octal']}")
    if payload.get("env_restrictive") is not None:
        print(f"env_restrictive={payload['env_restrictive']}")
    print(f"service_state_exists={bool(payload.get('service_state_exists'))}")
    print(f"service_log_exists={bool(payload.get('service_log_exists'))}")
    print(f"health_latest_exists={bool(payload.get('health_latest_exists'))}")

    niceness_policy = payload.get("niceness_policy")
    if isinstance(niceness_policy, dict):
        cadences = niceness_policy.get("cadences")
        if isinstance(cadences, dict):
            for name in sorted(cadences):
                value = cadences.get(name)
                if isinstance(value, (int, float, str)):
                    print(f"niceness_{name}={value}")
        cache_horizons = niceness_policy.get("cache_horizons_days")
        if isinstance(cache_horizons, dict):
            for name in sorted(cache_horizons):
                value = cache_horizons.get(name)
                if isinstance(value, (int, float)):
                    print(f"cache_{name}_days={value}")

    if isinstance(payload.get("last_loop_at"), str):
        print(f"last_loop_at={payload['last_loop_at']}")
        age_seconds = _health_report.age_seconds_from_timestamp(payload["last_loop_at"])
        if age_seconds is not None:
            print(f"last_loop_age_seconds={age_seconds:.1f}")

    service_state_parse_error = payload.get("service_state_parse_error")
    if isinstance(service_state_parse_error, str) and service_state_parse_error:
        print(f"service_state_parse_error={service_state_parse_error}")
    health_latest_parse_error = payload.get("health_latest_parse_error")
    if isinstance(health_latest_parse_error, str) and health_latest_parse_error:
        print(f"health_latest_parse_error={health_latest_parse_error}")

    health_latest = payload.get("health_latest_summary")
    if isinstance(health_latest, dict):
        if isinstance(health_latest.get("healthy"), bool):
            print(f"health_healthy={health_latest['healthy']}")
        warnings = health_latest.get("warnings") if isinstance(health_latest.get("warnings"), list) else []
        warning_count = health_latest.get("warning_count")
        if not isinstance(warning_count, int):
            warning_count = len(warnings)
        print(f"health_warning_count={warning_count}")
        warning_codes = [item.get("code") for item in warnings if isinstance(item, dict) and isinstance(item.get("code"), str)]
        if warning_codes:
            print("health_warnings=" + ", ".join(warning_codes))

        maintenance = health_latest.get("maintenance")
        if isinstance(maintenance, dict):
            _health_report.emit_recommended_command_summary("maintenance_recommended", maintenance.get("recommended_command"))
            _health_report.emit_recommended_command_summary("maintenance_recommended_auto", maintenance.get("recommended_automation_command"))

    api_usage = payload.get("api_usage")
    if isinstance(api_usage, dict):
        for provider_name in sorted(api_usage):
            provider_usage = api_usage.get(provider_name)
            if not isinstance(provider_usage, dict):
                continue
            request_count = provider_usage.get("request_count")
            if isinstance(request_count, int):
                print(f"api_{provider_name}_request_count={request_count}")
            success_count = provider_usage.get("success_count")
            if isinstance(success_count, int):
                print(f"api_{provider_name}_success_count={success_count}")
            error_count = provider_usage.get("error_count")
            if isinstance(error_count, int):
                print(f"api_{provider_name}_error_count={error_count}")
            last_event_at = provider_usage.get("last_event_at")
            if isinstance(last_event_at, str) and last_event_at:
                print(f"api_{provider_name}_last_event_at={last_event_at}")

    task_state = payload.get("task_state")
    if isinstance(task_state, dict):
        for task_name in sorted(task_state):
            task_payload = task_state.get(task_name)
            if not isinstance(task_payload, dict):
                continue
            status = task_payload.get("last_status") if isinstance(task_payload.get("last_status"), str) else None
            if status is not None:
                print(f"task_{task_name}_last_status={status}")
            last_run_at = task_payload.get("last_run_at") if isinstance(task_payload.get("last_run_at"), str) else None
            if last_run_at is not None:
                print(f"task_{task_name}_last_run_at={last_run_at}")
            last_skipped_at = task_payload.get("last_skipped_at") if isinstance(task_payload.get("last_skipped_at"), str) else None
            if last_skipped_at is not None:
                print(f"task_{task_name}_last_skipped_at={last_skipped_at}")
            last_skip_reason = task_payload.get("last_skip_reason") if isinstance(task_payload.get("last_skip_reason"), str) else None
            if last_skip_reason is not None:
                print(f"task_{task_name}_last_skip_reason={last_skip_reason}")
            last_error = task_payload.get("last_error") if isinstance(task_payload.get("last_error"), str) else None
            if last_error is not None:
                print(f"task_{task_name}_last_error={last_error}")
            last_decision_at = task_payload.get("last_decision_at") if isinstance(task_payload.get("last_decision_at"), str) else None
            if last_decision_at is not None:
                print(f"task_{task_name}_last_decision_at={last_decision_at}")
            last_started_at = task_payload.get("last_started_at") if isinstance(task_payload.get("last_started_at"), str) else None
            if last_started_at is not None:
                print(f"task_{task_name}_last_started_at={last_started_at}")
            last_finished_at = task_payload.get("last_finished_at") if isinstance(task_payload.get("last_finished_at"), str) else None
            if last_finished_at is not None:
                print(f"task_{task_name}_last_finished_at={last_finished_at}")
            last_duration_seconds = task_payload.get("last_duration_seconds") if isinstance(task_payload.get("last_duration_seconds"), (int, float)) else None
            if last_duration_seconds is not None:
                print(f"task_{task_name}_last_duration_seconds={last_duration_seconds}")
            every_seconds = task_payload.get("every_seconds") if isinstance(task_payload.get("every_seconds"), int) else None
            if every_seconds is not None:
                print(f"task_{task_name}_every_seconds={every_seconds}")
            budget_provider = task_payload.get("budget_provider") if isinstance(task_payload.get("budget_provider"), str) else None
            if budget_provider is not None:
                print(f"task_{task_name}_budget_provider={budget_provider}")
            budget_scope = task_payload.get("budget_scope") if isinstance(task_payload.get("budget_scope"), str) else None
            if budget_scope is not None:
                print(f"task_{task_name}_budget_scope={budget_scope}")
            projected_request_source = task_payload.get("projected_request_source") if isinstance(task_payload.get("projected_request_source"), str) else None
            if projected_request_source is not None:
                print(f"task_{task_name}_projected_request_source={projected_request_source}")
            projected_request_count = task_payload.get("projected_request_count") if isinstance(task_payload.get("projected_request_count"), int) else None
            if projected_request_count is not None:
                print(f"task_{task_name}_projected_request_count={projected_request_count}")
            projected_request_total = task_payload.get("projected_request_total") if isinstance(task_payload.get("projected_request_total"), int) else None
            if projected_request_total is not None:
                print(f"task_{task_name}_projected_request_total={projected_request_total}")
            projected_request_history_window = task_payload.get("projected_request_history_window") if isinstance(task_payload.get("projected_request_history_window"), int) else None
            if projected_request_history_window is not None:
                print(f"task_{task_name}_projected_request_history_window={projected_request_history_window}")
            projected_request_history_mode = task_payload.get("projected_request_history_mode") if isinstance(task_payload.get("projected_request_history_mode"), str) else None
            if projected_request_history_mode is not None:
                print(f"task_{task_name}_projected_request_history_mode={projected_request_history_mode}")
            projected_request_history_sample_count = task_payload.get("projected_request_history_sample_count") if isinstance(task_payload.get("projected_request_history_sample_count"), int) else None
            if projected_request_history_sample_count is not None:
                print(f"task_{task_name}_projected_request_history_sample_count={projected_request_history_sample_count}")
            projected_ratio = task_payload.get("projected_ratio") if isinstance(task_payload.get("projected_ratio"), (int, float)) else None
            if projected_ratio is not None:
                print(f"task_{task_name}_projected_ratio={projected_ratio}")
            projected_request_percentile = task_payload.get("projected_request_percentile") if isinstance(task_payload.get("projected_request_percentile"), (int, float)) else None
            if projected_request_percentile is not None:
                print(f"task_{task_name}_projected_request_percentile={projected_request_percentile}")
            projected_request_percentile_source = task_payload.get("projected_request_percentile_source") if isinstance(task_payload.get("projected_request_percentile_source"), str) else None
            if projected_request_percentile_source is not None:
                print(f"task_{task_name}_projected_request_percentile_source={projected_request_percentile_source}")
            last_request_delta = task_payload.get("last_request_delta") if isinstance(task_payload.get("last_request_delta"), int) else None
            if last_request_delta is not None:
                print(f"task_{task_name}_last_request_delta={last_request_delta}")
            last_fetch_mode = task_payload.get("last_fetch_mode") if isinstance(task_payload.get("last_fetch_mode"), str) else None
            if last_fetch_mode is not None:
                print(f"task_{task_name}_last_fetch_mode={last_fetch_mode}")
            last_full_refresh_reason = task_payload.get("last_full_refresh_reason") if isinstance(task_payload.get("last_full_refresh_reason"), str) else None
            if last_full_refresh_reason is not None:
                print(f"task_{task_name}_last_full_refresh_reason={last_full_refresh_reason}")
            planned_fetch_mode = task_payload.get("planned_fetch_mode") if isinstance(task_payload.get("planned_fetch_mode"), str) else None
            if planned_fetch_mode is not None:
                print(f"task_{task_name}_planned_fetch_mode={planned_fetch_mode}")
            planned_full_refresh_reason = task_payload.get("planned_full_refresh_reason") if isinstance(task_payload.get("planned_full_refresh_reason"), str) else None
            if planned_full_refresh_reason is not None:
                print(f"task_{task_name}_planned_full_refresh_reason={planned_full_refresh_reason}")
            planned_full_refresh_due_at = task_payload.get("planned_full_refresh_due_at") if isinstance(task_payload.get("planned_full_refresh_due_at"), str) else None
            if planned_full_refresh_due_at is not None:
                print(f"task_{task_name}_planned_full_refresh_due_at={planned_full_refresh_due_at}")
            planned_full_refresh_overdue_seconds = task_payload.get("planned_full_refresh_overdue_seconds") if isinstance(task_payload.get("planned_full_refresh_overdue_seconds"), int) else None
            if planned_full_refresh_overdue_seconds is not None:
                print(f"task_{task_name}_planned_full_refresh_overdue_seconds={planned_full_refresh_overdue_seconds}")
            planned_full_refresh_budget_deferred = task_payload.get("planned_full_refresh_budget_deferred") if isinstance(task_payload.get("planned_full_refresh_budget_deferred"), bool) else None
            if planned_full_refresh_budget_deferred is not None:
                print(f"task_{task_name}_planned_full_refresh_budget_deferred={planned_full_refresh_budget_deferred}")
            planned_full_refresh_deferred_reason = task_payload.get("planned_full_refresh_deferred_reason") if isinstance(task_payload.get("planned_full_refresh_deferred_reason"), str) else None
            if planned_full_refresh_deferred_reason is not None:
                print(f"task_{task_name}_planned_full_refresh_deferred_reason={planned_full_refresh_deferred_reason}")
            next_due_at = task_payload.get("next_due_at") if isinstance(task_payload.get("next_due_at"), str) else None
            if next_due_at is not None:
                print(f"task_{task_name}_next_due_at={next_due_at}")
            next_due_in_seconds = task_payload.get("next_due_in_seconds") if isinstance(task_payload.get("next_due_in_seconds"), int) else None
            if next_due_in_seconds is not None:
                print(f"task_{task_name}_next_due_in_seconds={next_due_in_seconds}")
            execution_state = task_payload.get("execution_state") if isinstance(task_payload.get("execution_state"), str) else None
            if execution_state is not None:
                print(f"task_{task_name}_execution_state={execution_state}")
            execution_state_reason = task_payload.get("execution_state_reason") if isinstance(task_payload.get("execution_state_reason"), str) else None
            if execution_state_reason is not None:
                print(f"task_{task_name}_execution_state_reason={execution_state_reason}")
            execution_state_detail = task_payload.get("execution_state_detail") if isinstance(task_payload.get("execution_state_detail"), str) else None
            if execution_state_detail is not None:
                print(f"task_{task_name}_execution_state_detail={execution_state_detail}")
            execution_state_remaining_seconds = task_payload.get("execution_state_remaining_seconds") if isinstance(task_payload.get("execution_state_remaining_seconds"), int) else None
            if execution_state_remaining_seconds is not None:
                print(f"task_{task_name}_execution_state_remaining_seconds={execution_state_remaining_seconds}")
            execution_state_elapsed_seconds = task_payload.get("execution_state_elapsed_seconds") if isinstance(task_payload.get("execution_state_elapsed_seconds"), int) else None
            if execution_state_elapsed_seconds is not None:
                print(f"task_{task_name}_execution_state_elapsed_seconds={execution_state_elapsed_seconds}")
            running_started_at = task_payload.get("running_started_at") if isinstance(task_payload.get("running_started_at"), str) else None
            if running_started_at is not None:
                print(f"task_{task_name}_running_started_at={running_started_at}")
            running_command = task_payload.get("running_command") if isinstance(task_payload.get("running_command"), str) else None
            if running_command is not None:
                print(f"task_{task_name}_running_command={running_command}")
            running_timeout_seconds = task_payload.get("running_timeout_seconds") if isinstance(task_payload.get("running_timeout_seconds"), int) else None
            if running_timeout_seconds is not None:
                print(f"task_{task_name}_running_timeout_seconds={running_timeout_seconds}")
            running_duration_seconds = task_payload.get("running_duration_seconds") if isinstance(task_payload.get("running_duration_seconds"), (int, float)) else None
            if running_duration_seconds is not None:
                print(f"task_{task_name}_running_duration_seconds={running_duration_seconds}")
            budget_backoff_level = task_payload.get("budget_backoff_level") if isinstance(task_payload.get("budget_backoff_level"), str) else None
            if budget_backoff_level is not None:
                print(f"task_{task_name}_budget_backoff_level={budget_backoff_level}")
            budget_backoff_until = task_payload.get("budget_backoff_until") if isinstance(task_payload.get("budget_backoff_until"), str) else None
            if budget_backoff_until is not None:
                print(f"task_{task_name}_budget_backoff_until={budget_backoff_until}")
            budget_backoff_remaining_seconds = task_payload.get("budget_backoff_remaining_seconds") if isinstance(task_payload.get("budget_backoff_remaining_seconds"), int) else None
            if budget_backoff_remaining_seconds is not None:
                print(f"task_{task_name}_budget_backoff_remaining_seconds={budget_backoff_remaining_seconds}")
            budget_backoff_floor_seconds = task_payload.get("budget_backoff_floor_seconds") if isinstance(task_payload.get("budget_backoff_floor_seconds"), int) else None
            if budget_backoff_floor_seconds is not None:
                print(f"task_{task_name}_budget_backoff_floor_seconds={budget_backoff_floor_seconds}")
            budget_backoff_cooldown_source = task_payload.get("budget_backoff_cooldown_source") if isinstance(task_payload.get("budget_backoff_cooldown_source"), str) else None
            if budget_backoff_cooldown_source is not None:
                print(f"task_{task_name}_budget_backoff_cooldown_source={budget_backoff_cooldown_source}")
            failure_backoff_until = task_payload.get("failure_backoff_until") if isinstance(task_payload.get("failure_backoff_until"), str) else None
            if failure_backoff_until is not None:
                print(f"task_{task_name}_failure_backoff_until={failure_backoff_until}")
            failure_backoff_remaining_seconds = task_payload.get("failure_backoff_remaining_seconds") if isinstance(task_payload.get("failure_backoff_remaining_seconds"), int) else None
            if failure_backoff_remaining_seconds is not None:
                print(f"task_{task_name}_failure_backoff_remaining_seconds={failure_backoff_remaining_seconds}")
            failure_backoff_reason = task_payload.get("failure_backoff_reason") if isinstance(task_payload.get("failure_backoff_reason"), str) else None
            if failure_backoff_reason is not None:
                print(f"task_{task_name}_failure_backoff_reason={failure_backoff_reason}")
            failure_backoff_class = task_payload.get("failure_backoff_class") if isinstance(task_payload.get("failure_backoff_class"), str) else None
            if failure_backoff_class is not None:
                print(f"task_{task_name}_failure_backoff_class={failure_backoff_class}")
            failure_backoff_floor_seconds = task_payload.get("failure_backoff_floor_seconds") if isinstance(task_payload.get("failure_backoff_floor_seconds"), int) else None
            if failure_backoff_floor_seconds is not None:
                print(f"task_{task_name}_failure_backoff_floor_seconds={failure_backoff_floor_seconds}")
            failure_backoff_consecutive_failures = task_payload.get("failure_backoff_consecutive_failures") if isinstance(task_payload.get("failure_backoff_consecutive_failures"), int) else None
            if failure_backoff_consecutive_failures is not None:
                print(f"task_{task_name}_failure_backoff_consecutive_failures={failure_backoff_consecutive_failures}")
            last_result = task_payload.get("last_result") if isinstance(task_payload.get("last_result"), dict) else None
            if last_result is not None:
                last_result_status = last_result.get("status") if isinstance(last_result.get("status"), str) else None
                if last_result_status is not None:
                    print(f"task_{task_name}_last_result_status={last_result_status}")
                last_result_label = last_result.get("label") if isinstance(last_result.get("label"), str) else None
                if last_result_label is not None:
                    print(f"task_{task_name}_last_result_label={last_result_label}")
                last_result_returncode = last_result.get("returncode") if isinstance(last_result.get("returncode"), int) else None
                if last_result_returncode is not None:
                    print(f"task_{task_name}_last_result_returncode={last_result_returncode}")
                last_result_reason = last_result.get("reason") if isinstance(last_result.get("reason"), str) else None
                if last_result_reason is not None:
                    print(f"task_{task_name}_last_result_reason={last_result_reason}")
                last_result_fetch_mode = last_result.get("fetch_mode") if isinstance(last_result.get("fetch_mode"), str) else None
                if last_result_fetch_mode is not None:
                    print(f"task_{task_name}_last_result_fetch_mode={last_result_fetch_mode}")
                last_result_full_refresh_reason = last_result.get("full_refresh_reason") if isinstance(last_result.get("full_refresh_reason"), str) else None
                if last_result_full_refresh_reason is not None:
                    print(f"task_{task_name}_last_result_full_refresh_reason={last_result_full_refresh_reason}")
                last_result_deferred_full_refresh_reason = last_result.get("deferred_full_refresh_reason") if isinstance(last_result.get("deferred_full_refresh_reason"), str) else None
                if last_result_deferred_full_refresh_reason is not None:
                    print(f"task_{task_name}_last_result_deferred_full_refresh_reason={last_result_deferred_full_refresh_reason}")
                last_result_stdout_snippet = last_result.get("stdout_snippet") if isinstance(last_result.get("stdout_snippet"), str) else None
                if last_result_stdout_snippet is not None:
                    print(f"task_{task_name}_last_result_stdout_snippet={last_result_stdout_snippet}")
                last_result_stderr_snippet = last_result.get("stderr_snippet") if isinstance(last_result.get("stderr_snippet"), str) else None
                if last_result_stderr_snippet is not None:
                    print(f"task_{task_name}_last_result_stderr_snippet={last_result_stderr_snippet}")
                last_result_delivery_status = last_result.get("delivery_status") if isinstance(last_result.get("delivery_status"), str) else None
                if last_result_delivery_status is not None:
                    print(f"task_{task_name}_last_result_delivery_status={last_result_delivery_status}")
                last_result_request_id = last_result.get("request_id") if isinstance(last_result.get("request_id"), str) else None
                if last_result_request_id is not None:
                    print(f"task_{task_name}_last_result_request_id={last_result_request_id}")
                last_result_request_url = last_result.get("request_url") if isinstance(last_result.get("request_url"), str) else None
                if last_result_request_url is not None:
                    print(f"task_{task_name}_last_result_request_url={last_result_request_url}")
                last_result_http_status = last_result.get("http_status") if isinstance(last_result.get("http_status"), int) else None
                if last_result_http_status is not None:
                    print(f"task_{task_name}_last_result_http_status={last_result_http_status}")
                for result_field in (
                    "db_size_before",
                    "db_size_after",
                    "freelist_bytes",
                    "freelist_ratio",
                    "bytes_reclaimed",
                    "backup_archive",
                    "backup_archive_sha256",
                    "required_free_bytes",
                    "available_free_bytes",
                    "initial_available_free_bytes",
                    "post_backup_available_free_bytes",
                    "error_type",
                ):
                    result_value = last_result.get(result_field)
                    if isinstance(result_value, (str, int, float)) and not isinstance(result_value, bool):
                        print(f"task_{task_name}_last_result_{result_field}={result_value}")

    service_log_tail = payload.get("service_log_tail")
    if isinstance(service_log_tail, list) and service_log_tail:
        last_log_line = service_log_tail[-1]
        if isinstance(last_log_line, str) and last_log_line:
            print(f"service_log_last_line={last_log_line}")














































def _cmd_health_check(
    project_root: Path | None,
    stale_hours: float,
    strict: bool,
    review_issue_type: str | None,
    review_worklist_limit: int,
    output_format: str,
    mapping_coverage_threshold: float,
    maintenance_review_limit: int,
) -> int:
    returncode, _, output = _health_report.run_health_report(
        project_root,
        stale_hours=stale_hours,
        strict=strict,
        review_issue_type=review_issue_type,
        review_worklist_limit=review_worklist_limit,
        output_format=output_format,
        mapping_coverage_threshold=mapping_coverage_threshold,
        maintenance_review_limit=maintenance_review_limit,
    )
    sys.stdout.write(output)
    return returncode


def _cmd_mal_auth_url(project_root: Path | None, emit_json: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    if warning := mal_callback_bind_warning(config.mal):
        print(f"WARNING: {warning}", file=sys.stderr)
    client = MalClient(config, load_mal_secrets(config))
    pkce = client.generate_pkce_pair()
    try:
        auth_url = client.build_authorization_url(code_challenge=pkce.code_challenge)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if emit_json:
        print(
            json.dumps(
                {
                    "authorization_url": auth_url,
                    "redirect_uri": config.mal.redirect_uri,
                    "code_verifier": pkce.code_verifier,
                    "code_challenge": pkce.code_challenge,
                },
                indent=2,
            )
        )
        return 0
    print("Open this URL in a browser after writing down the code verifier:")
    print(auth_url)
    print()
    print("code_verifier=")
    print(pkce.code_verifier)
    print()
    print(f"redirect_uri={config.mal.redirect_uri}")
    return 0


def _cmd_mal_auth_login(project_root: Path | None, timeout_seconds: float, verify_whoami: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    pkce = client.generate_pkce_pair()
    state = client.generate_state()
    try:
        auth_url = client.build_authorization_url(code_challenge=pkce.code_challenge, state=state)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(format_auth_flow_prompt(config, auth_url, timeout_seconds))
    try:
        callback = wait_for_oauth_callback(
            config.mal.bind_host,
            config.mal.redirect_port,
            expected_state=state,
            timeout_seconds=timeout_seconds,
        )
        token = client.exchange_code(callback.code, pkce.code_verifier)
        persisted = persist_token_response(token, secrets)
    except OSError as exc:
        print(f"Unable to start MAL callback listener on {config.mal.redirect_uri}: {exc}", file=sys.stderr)
        return 1
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OAuthCallbackError, MalApiError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print()
    print(f"Persisted access token to {persisted.access_token_path}")
    if token.refresh_token:
        print(f"Persisted refresh token to {persisted.refresh_token_path}")
    else:
        print("No refresh token returned by MAL; existing refresh token file left untouched")

    if not verify_whoami:
        return 0

    try:
        whoami = client.get_my_user(access_token=token.access_token)
    except MalApiError as exc:
        print(f"Token exchange succeeded, but /users/@me verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"Authenticated MAL user: {json.dumps(whoami, indent=2)}")
    return 0


def _cmd_mal_refresh(project_root: Path | None, verify_whoami: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    try:
        token = client.refresh_access_token()
        persisted = persist_token_response(token, secrets)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Persisted access token to {persisted.access_token_path}")
    if token.refresh_token:
        print(f"Persisted refresh token to {persisted.refresh_token_path}")

    if not verify_whoami:
        return 0

    try:
        whoami = client.get_my_user(access_token=token.access_token)
    except MalApiError as exc:
        print(f"Refresh succeeded, but /users/@me verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"Authenticated MAL user: {json.dumps(whoami, indent=2)}")
    return 0


def _cmd_mal_whoami(project_root: Path | None) -> int:
    config = load_config(project_root)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    try:
        whoami = client.get_my_user()
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(whoami, indent=2))
    return 0


def _cmd_provider_auth_login(project_root: Path | None, provider_slug: str, profile: str, no_verify: bool) -> int:
    if provider_slug == "crunchyroll":
        return _cmd_crunchyroll_auth_login(project_root, profile, no_verify)
    if provider_slug == "hidive":
        from .hidive_auth import HidiveAuthError, hidive_login_with_credentials

        config = load_config(project_root)
        ensure_directories(config)
        try:
            result = hidive_login_with_credentials(
                config,
                profile=profile,
                verify_account=not no_verify,
            )
        except HidiveAuthError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        print(f"Staged HIDIVE authorisation token to {result.access_token_path}")
        print(f"Staged HIDIVE refresh token to {result.refresh_token_path}")
        print(f"Updated session state at {result.session_state_path}")
        if result.account_id:
            print(f"HIDIVE account_id={result.account_id}")
        if result.account_name:
            print(f"HIDIVE account_name={result.account_name}")
        print(f"profile={result.profile}")
        return 0
    print(f"provider-auth-login is not implemented yet for provider '{provider_slug}'", file=sys.stderr)
    return 2


def _cmd_crunchyroll_auth_login(project_root: Path | None, profile: str, no_verify: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    try:
        result = crunchyroll_login_with_credentials(
            config,
            profile=profile,
            verify_account=not no_verify,
        )
    except CrunchyrollAuthError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Staged Crunchyroll refresh token to {result.refresh_token_path}")
    print(f"Staged Crunchyroll device id to {result.device_id_path}")
    print(f"Updated session state at {result.session_state_path}")
    if result.account_id:
        print(f"Crunchyroll account_id={result.account_id}")
    if result.account_email:
        print(f"Crunchyroll account_email={result.account_email}")
    print(f"profile={result.profile}")
    print(f"locale={result.locale}")
    print(f"device_type={result.device_type}")
    return 0


def _cmd_validate_snapshot(project_root: Path | None, snapshot_path: Path | None) -> int:
    load_config(project_root)
    if snapshot_path is None:
        payload = json.load(sys.stdin)
        source = "stdin"
    else:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        source = str(snapshot_path)
    try:
        validate_snapshot_payload(payload)
    except SnapshotValidationError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1
    print(f"VALID: {source}")
    return 0


def _cmd_provider_fetch_snapshot(
    project_root: Path | None,
    provider_slug: str,
    profile: str,
    out_path: Path | None,
    ingest: bool,
    full_refresh: bool,
    max_history_pages: int | None = None,
    max_watchlist_pages: int | None = None,
    history_start_page: int = 1,
    watchlist_start: int = 0,
) -> int:
    for name, value in (("max-history-pages", max_history_pages), ("max-watchlist-pages", max_watchlist_pages)):
        if value is not None and value < 1:
            print(f"{name} must be >= 1", file=sys.stderr)
            return 1
    if history_start_page < 1:
        print("history-start-page must be >= 1", file=sys.stderr)
        return 1
    if watchlist_start < 0:
        print("watchlist-start must be >= 0", file=sys.stderr)
        return 1
    config = load_config(project_root)
    ensure_directories(config)
    provider = get_provider(provider_slug)
    if any(value is not None for value in (max_history_pages, max_watchlist_pages)) or history_start_page != 1 or watchlist_start != 0:
        if provider_slug != "crunchyroll":
            print("Page chunk controls are currently supported only for Crunchyroll", file=sys.stderr)
            return 1
    fetch_kwargs = {
        "profile": profile,
        "full_refresh": full_refresh,
        "max_history_pages": max_history_pages,
        "max_watchlist_pages": max_watchlist_pages,
        "history_start_page": history_start_page,
        "watchlist_start": watchlist_start,
    }
    try:
        result = provider.fetch_snapshot(
            config,
            **fetch_kwargs,
        )
    except (CrunchyrollAuthError, CrunchyrollSnapshotError, HidiveAuthError, HidiveSnapshotError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    payload = provider_snapshot.snapshot_to_dict(result.snapshot)
    target_path = out_path
    if target_path is not None:
        provider.write_snapshot_file(target_path, result.snapshot)
        print(f"Wrote {provider.display_name} snapshot to {target_path}")

    if ingest:
        raw = payload.get("raw", {})
        raw = raw if isinstance(raw, dict) else {}
        is_partial = bool(raw.get("partial"))
        bootstrap_full_refresh = provider_slug == "crunchyroll" and bool(
            raw.get("bootstrap_full_refresh")
            or raw.get("sync_boundary_bootstrap")
            or raw.get("sync_boundary_refresh_kind") == "bootstrap_full_refresh"
        )
        completed_bootstrap_full_refresh = bootstrap_full_refresh and not is_partial
        ingest_mode = "full_refresh" if (full_refresh or completed_bootstrap_full_refresh) and not is_partial else "hot"
        if (full_refresh or bootstrap_full_refresh) and is_partial:
            print(
                f"Partial {provider.display_name} snapshot detected; ingesting as hot instead of marking a completed full refresh",
                file=sys.stderr,
            )
        summary = ingest_snapshot_payload(payload, config, mode=ingest_mode)
        print(json.dumps(summary.as_dict(), indent=2))
        return 0

    print(json.dumps(payload, indent=2))
    return 0


def _cmd_crunchyroll_fetch_snapshot(
    project_root: Path | None,
    profile: str,
    out_path: Path | None,
    ingest: bool,
    full_refresh: bool,
    max_history_pages: int | None = None,
    max_watchlist_pages: int | None = None,
    history_start_page: int = 1,
    watchlist_start: int = 0,
) -> int:
    return _cmd_provider_fetch_snapshot(
        project_root,
        "crunchyroll",
        profile,
        out_path,
        ingest,
        full_refresh,
        max_history_pages=max_history_pages,
        max_watchlist_pages=max_watchlist_pages,
        history_start_page=history_start_page,
        watchlist_start=watchlist_start,
    )


def _cmd_ingest_snapshot(project_root: Path | None, snapshot_path: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    if snapshot_path is None:
        payload = json.load(sys.stdin)
        summary = ingest_snapshot_payload(payload, config)
    else:
        summary = ingest_snapshot_file(snapshot_path, config)
    print(json.dumps(summary.as_dict(), indent=2))
    return 0


def _cmd_backfill_hidive_series_urls(project_root: Path | None, *, apply: bool, output_format: str) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    payload = backfill_hidive_series_urls(config.db_path, apply=apply)
    if output_format == "summary":
        print(f"provider={payload['provider']}")
        print(f"dry_run={payload['dry_run']}")
        print(f"canonical_route={payload['canonical_route']}")
        provider_series = payload.get("provider_series") if isinstance(payload.get("provider_series"), dict) else {}
        eligibility = payload.get("eligibility") if isinstance(payload.get("eligibility"), dict) else {}
        cache = payload.get("provider_title_search_cache") if isinstance(payload.get("provider_title_search_cache"), dict) else {}
        snapshots = payload.get("recommendation_score_snapshots") if isinstance(payload.get("recommendation_score_snapshots"), dict) else {}
        print(f"provider_series_matched={provider_series.get('matched', 0)}")
        print(f"provider_series_updated={provider_series.get('updated', 0)}")
        print(f"provider_series_sample_count={provider_series.get('sample_count', 0)}")
        print(f"eligibility_matched={eligibility.get('matched', 0)}")
        print(f"eligibility_updated={eligibility.get('updated', 0)}")
        print(f"eligibility_sample_count={eligibility.get('sample_count', 0)}")
        print(f"provider_title_search_cache_matched={cache.get('matched', 0)}")
        print(f"provider_title_search_cache_updated={cache.get('updated', 0)}")
        print(f"provider_title_search_cache_sample_count={cache.get('sample_count', 0)}")
        print(f"recommendation_score_snapshots_matched={snapshots.get('matched', 0)}")
        print(f"recommendation_score_snapshots_updated={snapshots.get('updated', 0)}")
        print(f"recommendation_score_snapshots_sample_count={snapshots.get('sample_count', 0)}")
    else:
        print(json.dumps(payload, indent=2))
    return 0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _format_sqlite_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def _stale_row_age_days(reference_now: datetime, timestamp: object) -> float | None:
    parsed = _parse_sqlite_timestamp(timestamp)
    if parsed is None:
        return None
    age_seconds = max(0.0, (reference_now.astimezone(timezone.utc) - parsed).total_seconds())
    return round(age_seconds / 86400, 3)


def _provider_stale_row_age_ranges(
    last_seen_ranges: dict[str, dict[str, object]],
    *,
    reference_now: datetime,
) -> dict[str, dict[str, object]]:
    age_ranges: dict[str, dict[str, object]] = {}
    for family in ("series", "progress", "watchlist"):
        family_range = last_seen_ranges.get(family) if isinstance(last_seen_ranges, dict) else None
        if not isinstance(family_range, dict):
            age_ranges[family] = {"count": 0, "oldest_age_days": None, "newest_age_days": None}
            continue
        count = family_range.get("count")
        safe_count = int(count) if isinstance(count, int) and not isinstance(count, bool) else 0
        oldest_age_days = _stale_row_age_days(reference_now, family_range.get("oldest_last_seen_at")) if safe_count else None
        newest_age_days = _stale_row_age_days(reference_now, family_range.get("newest_last_seen_at")) if safe_count else None
        age_ranges[family] = {
            "count": safe_count,
            "oldest_age_days": oldest_age_days,
            "newest_age_days": newest_age_days,
        }
    return age_ranges


def _provider_stale_row_samples_with_ages(
    samples: dict[str, list[dict[str, object]]],
    *,
    reference_now: datetime,
) -> dict[str, list[dict[str, object]]]:
    """Attach exact age-in-days diagnostics to stale row samples.

    The sample payload is operator evidence only. Adding age to the sampled rows
    keeps retention-policy review grounded in the same timestamp math as the
    aggregate ranges without making any archive/prune decision.
    """
    annotated: dict[str, list[dict[str, object]]] = {}
    for family in ("series", "progress", "watchlist"):
        family_samples = samples.get(family) if isinstance(samples, dict) else None
        annotated_rows: list[dict[str, object]] = []
        if isinstance(family_samples, list):
            for row in family_samples:
                if not isinstance(row, dict):
                    continue
                annotated_row = dict(row)
                annotated_row["age_days"] = _stale_row_age_days(reference_now, annotated_row.get("last_seen_at"))
                annotated_rows.append(annotated_row)
        annotated[family] = annotated_rows
    return annotated


def _provider_stale_row_retention_review(
    counts: dict[str, object],
    age_buckets: dict[str, dict[str, object]],
    linkage: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Build a policy-neutral retention-review hint for stale provider rows.

    This deliberately does not recommend deletion. It condenses the age/linkage
    evidence into a stable operator field so callers can tell whether residue is
    merely fresh post-refresh noise or old enough to deserve a manual
    archive/prune/retain policy discussion.
    """
    total_count = sum(value for value in counts.values() if isinstance(value, int) and not isinstance(value, bool))
    old_31_plus_count = 0
    recent_0_7_count = 0
    older_8_30_count = 0
    for family_buckets in age_buckets.values():
        if not isinstance(family_buckets, dict):
            continue
        recent = family_buckets.get("recent_0_7_days")
        middle = family_buckets.get("older_8_30_days")
        old = family_buckets.get("older_31_plus_days")
        if isinstance(recent, int) and not isinstance(recent, bool):
            recent_0_7_count += recent
        if isinstance(middle, int) and not isinstance(middle, bool):
            older_8_30_count += middle
        if isinstance(old, int) and not isinstance(old, bool):
            old_31_plus_count += old

    current_linked_child_count = 0
    missing_series_child_count = 0
    stale_linked_child_count = 0
    for family_linkage in linkage.values():
        if not isinstance(family_linkage, dict):
            continue
        current = family_linkage.get("with_current_series")
        missing = family_linkage.get("with_missing_series")
        stale = family_linkage.get("with_stale_series")
        if isinstance(current, int) and not isinstance(current, bool):
            current_linked_child_count += current
        if isinstance(missing, int) and not isinstance(missing, bool):
            missing_series_child_count += missing
        if isinstance(stale, int) and not isinstance(stale, bool):
            stale_linked_child_count += stale

    if total_count <= 0:
        posture = "empty"
        next_step = "no_retention_review_needed"
        review_candidate = False
    elif current_linked_child_count > 0:
        posture = "current_series_child_residue"
        next_step = "review_current-linked_child_rows_before_any_archive_or_prune_policy"
        review_candidate = True
    elif old_31_plus_count > 0 or missing_series_child_count > 0:
        posture = "manual_retention_policy_candidate"
        next_step = "compare_samples_against_provider_behavior_before_choosing_retain_archive_or_prune"
        review_candidate = True
    elif older_8_30_count > 0:
        posture = "aging_residue_observe"
        next_step = "recheck_after_next_full_refresh_or_escalate_if_age_bucket_grows"
        review_candidate = False
    else:
        posture = "recent_residue_observe"
        next_step = "leave_classified_as_recent_residue_and_recheck_after_future_full_refresh"
        review_candidate = False

    return {
        "posture": posture,
        "review_candidate": review_candidate,
        "total_count": total_count,
        "recent_0_7_days_count": recent_0_7_count,
        "older_8_30_days_count": older_8_30_count,
        "older_31_plus_days_count": old_31_plus_count,
        "current_linked_child_count": current_linked_child_count,
        "stale_linked_child_count": stale_linked_child_count,
        "missing_series_child_count": missing_series_child_count,
        "next_step": next_step,
        "policy": "diagnostic_only_no_archive_or_prune",
    }


def _build_provider_stale_rows_payload(
    *,
    db_path: Path,
    provider: str,
    cutoff: str | None,
    safe_limit: int,
    older_than_days: float | None = None,
    reference_now: datetime | None = None,
) -> dict[str, object]:
    reference_now = reference_now or _utcnow()
    cutoff_source = "explicit"
    sync_run: dict[str, object] | None = None
    base_cutoff = cutoff.strip() if isinstance(cutoff, str) and cutoff.strip() else None
    effective_cutoff = base_cutoff
    age_cutoff: str | None = None
    safe_older_than_days = older_than_days
    if older_than_days is not None:
        safe_older_than_days = max(0.0, float(older_than_days))
        age_cutoff = _format_sqlite_timestamp(reference_now - timedelta(days=safe_older_than_days))
    if effective_cutoff is None:
        sync_run = get_latest_completed_sync_run(db_path, provider=provider, mode="full_refresh")
        if isinstance(sync_run, dict):
            started_at = sync_run.get("started_at")
            if isinstance(started_at, str) and started_at.strip():
                base_cutoff = started_at.strip()
                effective_cutoff = base_cutoff
                cutoff_source = "latest_completed_full_refresh_started_at"

    if effective_cutoff is not None and age_cutoff is not None:
        effective_cutoff = min(effective_cutoff, age_cutoff)
        cutoff_source = f"{cutoff_source}_and_older_than_days"

    if effective_cutoff is None:
        return {
            "provider": provider,
            "cutoff": None,
            "base_cutoff": None,
            "age_cutoff": age_cutoff,
            "older_than_days": safe_older_than_days,
            "cutoff_source": None,
            "latest_completed_full_refresh": None,
            "counts": {"series": 0, "progress": 0, "watchlist": 0},
            "samples": {"series": [], "progress": [], "watchlist": []},
            "linkage": {
                "progress": {"with_stale_series": 0, "with_current_series": 0, "with_missing_series": 0},
                "watchlist": {"with_stale_series": 0, "with_current_series": 0, "with_missing_series": 0},
            },
            "retention_review": _provider_stale_row_retention_review(
                {"series": 0, "progress": 0, "watchlist": 0},
                {
                    "series": {"recent_0_7_days": 0, "older_8_30_days": 0, "older_31_plus_days": 0},
                    "progress": {"recent_0_7_days": 0, "older_8_30_days": 0, "older_31_plus_days": 0},
                    "watchlist": {"recent_0_7_days": 0, "older_8_30_days": 0, "older_31_plus_days": 0},
                },
                {
                    "progress": {"with_stale_series": 0, "with_current_series": 0, "with_missing_series": 0},
                    "watchlist": {"with_stale_series": 0, "with_current_series": 0, "with_missing_series": 0},
                },
            ),
            "ready": False,
            "detail": "No cutoff was provided and no completed full-refresh sync run exists for this provider.",
        }

    counts = get_provider_stale_row_counts(db_path, provider=provider, cutoff=effective_cutoff)
    last_seen_ranges = get_provider_stale_row_last_seen_ranges(db_path, provider=provider, cutoff=effective_cutoff)
    age_bucket_cutoffs = {
        "seven_day_cutoff": _format_sqlite_timestamp(reference_now - timedelta(days=7)),
        "thirty_day_cutoff": _format_sqlite_timestamp(reference_now - timedelta(days=30)),
    }
    age_buckets = get_provider_stale_row_age_buckets(
        db_path,
        provider=provider,
        cutoff=effective_cutoff,
        seven_day_cutoff=age_bucket_cutoffs["seven_day_cutoff"],
        thirty_day_cutoff=age_bucket_cutoffs["thirty_day_cutoff"],
    )
    samples = _provider_stale_row_samples_with_ages(
        list_provider_stale_row_samples(
            db_path,
            provider=provider,
            cutoff=effective_cutoff,
            limit=safe_limit,
            series_cutoff=base_cutoff,
        ),
        reference_now=reference_now,
    )
    linkage = get_provider_stale_row_linkage(
        db_path, provider=provider, cutoff=effective_cutoff, series_cutoff=base_cutoff
    )
    retention_review = _provider_stale_row_retention_review(counts, age_buckets, linkage)
    return {
        "provider": provider,
        "cutoff": effective_cutoff,
        "base_cutoff": base_cutoff,
        "age_cutoff": age_cutoff,
        "older_than_days": safe_older_than_days,
        "cutoff_source": cutoff_source,
        "latest_completed_full_refresh": sync_run,
        "counts": counts,
        "total_count": sum(value for value in counts.values() if isinstance(value, int)),
        "last_seen_ranges": last_seen_ranges,
        "age_ranges_days": _provider_stale_row_age_ranges(last_seen_ranges, reference_now=reference_now),
        "age_bucket_cutoffs": age_bucket_cutoffs,
        "age_buckets": age_buckets,
        "sample_limit": safe_limit,
        "samples": samples,
        "linkage": linkage,
        "retention_review": retention_review,
        "ready": True,
        "read_only": True,
        "policy": "diagnostic_only_no_archive_or_prune",
    }


def _aggregate_provider_stale_row_last_seen_ranges(provider_payloads: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
    aggregated: dict[str, dict[str, object]] = {}
    for family in ("series", "progress", "watchlist"):
        count = 0
        oldest_values: list[str] = []
        newest_values: list[str] = []
        for payload in provider_payloads.values():
            ranges = payload.get("last_seen_ranges") if isinstance(payload, dict) else None
            family_range = ranges.get(family) if isinstance(ranges, dict) else None
            if not isinstance(family_range, dict):
                continue
            family_count = family_range.get("count")
            if isinstance(family_count, int) and not isinstance(family_count, bool):
                count += family_count
            oldest = family_range.get("oldest_last_seen_at")
            if isinstance(oldest, str) and oldest:
                oldest_values.append(oldest)
            newest = family_range.get("newest_last_seen_at")
            if isinstance(newest, str) and newest:
                newest_values.append(newest)
        aggregated[family] = {
            "count": count,
            "oldest_last_seen_at": min(oldest_values) if oldest_values else None,
            "newest_last_seen_at": max(newest_values) if newest_values else None,
        }
    return aggregated


def _aggregate_provider_stale_row_age_ranges(
    provider_payloads: dict[str, dict[str, object]],
    *,
    reference_now: datetime,
) -> dict[str, dict[str, object]]:
    return _provider_stale_row_age_ranges(
        _aggregate_provider_stale_row_last_seen_ranges(provider_payloads),
        reference_now=reference_now,
    )


def _aggregate_provider_stale_row_linkage(provider_payloads: dict[str, dict[str, object]]) -> dict[str, dict[str, int]]:
    aggregated: dict[str, dict[str, int]] = {}
    for family in ("progress", "watchlist"):
        family_totals = {"with_stale_series": 0, "with_current_series": 0, "with_missing_series": 0}
        for payload in provider_payloads.values():
            linkage = payload.get("linkage") if isinstance(payload, dict) else None
            family_linkage = linkage.get(family) if isinstance(linkage, dict) else None
            if not isinstance(family_linkage, dict):
                continue
            for key in family_totals:
                value = family_linkage.get(key)
                if isinstance(value, int) and not isinstance(value, bool):
                    family_totals[key] += value
        aggregated[family] = family_totals
    return aggregated


def _aggregate_provider_stale_row_age_buckets(provider_payloads: dict[str, dict[str, object]]) -> dict[str, dict[str, int]]:
    aggregated: dict[str, dict[str, int]] = {}
    for family in ("series", "progress", "watchlist"):
        family_totals = {"recent_0_7_days": 0, "older_8_30_days": 0, "older_31_plus_days": 0}
        for payload in provider_payloads.values():
            age_buckets = payload.get("age_buckets") if isinstance(payload, dict) else None
            family_buckets = age_buckets.get(family) if isinstance(age_buckets, dict) else None
            if not isinstance(family_buckets, dict):
                continue
            for bucket in family_totals:
                value = family_buckets.get(bucket)
                if isinstance(value, int) and not isinstance(value, bool):
                    family_totals[bucket] += value
        aggregated[family] = family_totals
    return aggregated


def _cmd_provider_stale_rows(
    project_root: Path | None,
    provider: str,
    cutoff: str | None,
    limit: int,
    output_format: str = "json",
    older_than_days: float | None = None,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    safe_limit = max(1, min(25, int(limit)))
    reference_now = _utcnow()

    if provider == "all":
        provider_payloads = {
            slug: _build_provider_stale_rows_payload(
                db_path=config.db_path,
                provider=slug,
                cutoff=cutoff,
                safe_limit=safe_limit,
                older_than_days=older_than_days,
                reference_now=reference_now,
            )
            for slug in list_provider_slugs()
        }
        counts = {
            family: sum(
                int(payload.get("counts", {}).get(family, 0))
                for payload in provider_payloads.values()
                if isinstance(payload.get("counts"), dict)
            )
            for family in ("series", "progress", "watchlist")
        }
        ready_count = sum(1 for payload in provider_payloads.values() if payload.get("ready") is True)
        age_cutoffs = [
            payload.get("age_cutoff")
            for payload in provider_payloads.values()
            if isinstance(payload.get("age_cutoff"), str)
        ]
        aggregated_age_buckets = _aggregate_provider_stale_row_age_buckets(provider_payloads)
        aggregated_linkage = _aggregate_provider_stale_row_linkage(provider_payloads)
        payload = {
            "provider": "all",
            "providers": provider_payloads,
            "provider_count": len(provider_payloads),
            "providers_ready_count": ready_count,
            "ready": ready_count > 0,
            "all_ready": ready_count == len(provider_payloads),
            "counts": counts,
            "total_count": sum(counts.values()),
            "last_seen_ranges": _aggregate_provider_stale_row_last_seen_ranges(provider_payloads),
            "age_ranges_days": _aggregate_provider_stale_row_age_ranges(provider_payloads, reference_now=reference_now),
            "age_bucket_cutoffs": next(
                (payload.get("age_bucket_cutoffs") for payload in provider_payloads.values() if isinstance(payload.get("age_bucket_cutoffs"), dict)),
                None,
            ),
            "age_buckets": aggregated_age_buckets,
            "linkage": aggregated_linkage,
            "retention_review": _provider_stale_row_retention_review(counts, aggregated_age_buckets, aggregated_linkage),
            "sample_limit": safe_limit,
            "age_cutoff": min(age_cutoffs) if age_cutoffs else None,
            "older_than_days": next(
                (payload.get("older_than_days") for payload in provider_payloads.values() if isinstance(payload.get("older_than_days"), (int, float)) and not isinstance(payload.get("older_than_days"), bool)),
                None,
            ),
            "read_only": True,
            "policy": "diagnostic_only_no_archive_or_prune",
        }
        if output_format == "summary":
            _print_provider_stale_rows_summary(payload)
        else:
            print(json.dumps(payload, indent=2))
        return 0 if ready_count > 0 else 1

    payload = _build_provider_stale_rows_payload(
        db_path=config.db_path,
        provider=provider,
        cutoff=cutoff,
        safe_limit=safe_limit,
        older_than_days=older_than_days,
        reference_now=reference_now,
    )
    if output_format == "summary":
        _print_provider_stale_rows_summary(payload)
    else:
        print(json.dumps(payload, indent=2))
    return 0 if payload.get("ready") is True else 1


def _print_provider_stale_rows_summary(payload: dict[str, object]) -> None:
    """Emit a stable terse operator summary for stale provider-row diagnostics."""
    provider = payload.get("provider") if isinstance(payload.get("provider"), str) else "unknown"
    ready = bool(payload.get("ready"))
    print(f"provider={provider}")
    print(f"ready={ready}")
    all_ready = payload.get("all_ready")
    if isinstance(all_ready, bool):
        print(f"all_ready={all_ready}")
    provider_count = payload.get("provider_count")
    if isinstance(provider_count, int) and not isinstance(provider_count, bool):
        print(f"provider_count={provider_count}")
    providers_ready_count = payload.get("providers_ready_count")
    if isinstance(providers_ready_count, int) and not isinstance(providers_ready_count, bool):
        print(f"providers_ready_count={providers_ready_count}")
    cutoff = payload.get("cutoff") if isinstance(payload.get("cutoff"), str) else None
    if cutoff is not None:
        print(f"cutoff={cutoff}")
    base_cutoff = payload.get("base_cutoff") if isinstance(payload.get("base_cutoff"), str) else None
    if base_cutoff is not None and base_cutoff != cutoff:
        print(f"base_cutoff={base_cutoff}")
    age_cutoff = payload.get("age_cutoff") if isinstance(payload.get("age_cutoff"), str) else None
    if age_cutoff is not None:
        print(f"age_cutoff={age_cutoff}")
    older_than_days = payload.get("older_than_days")
    if isinstance(older_than_days, (int, float)) and not isinstance(older_than_days, bool):
        print(f"older_than_days={older_than_days:g}")
    cutoff_source = payload.get("cutoff_source") if isinstance(payload.get("cutoff_source"), str) else None
    if cutoff_source is not None:
        print(f"cutoff_source={cutoff_source}")
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    for family in ("series", "progress", "watchlist"):
        value = counts.get(family) if isinstance(counts, dict) else None
        count = int(value) if isinstance(value, int) and not isinstance(value, bool) else 0
        print(f"{family}_stale_count={count}")
    last_seen_ranges = payload.get("last_seen_ranges") if isinstance(payload.get("last_seen_ranges"), dict) else {}
    for family in ("series", "progress", "watchlist"):
        family_range = last_seen_ranges.get(family) if isinstance(last_seen_ranges, dict) else None
        if not isinstance(family_range, dict):
            continue
        oldest = family_range.get("oldest_last_seen_at")
        newest = family_range.get("newest_last_seen_at")
        if isinstance(oldest, str) and oldest:
            print(f"{family}_oldest_last_seen_at={oldest}")
        if isinstance(newest, str) and newest:
            print(f"{family}_newest_last_seen_at={newest}")
    age_ranges_days = payload.get("age_ranges_days") if isinstance(payload.get("age_ranges_days"), dict) else {}
    for family in ("series", "progress", "watchlist"):
        family_age_range = age_ranges_days.get(family) if isinstance(age_ranges_days, dict) else None
        if not isinstance(family_age_range, dict):
            continue
        oldest_age_days = family_age_range.get("oldest_age_days")
        newest_age_days = family_age_range.get("newest_age_days")
        if isinstance(oldest_age_days, (int, float)) and not isinstance(oldest_age_days, bool):
            print(f"{family}_oldest_age_days={oldest_age_days:g}")
        if isinstance(newest_age_days, (int, float)) and not isinstance(newest_age_days, bool):
            print(f"{family}_newest_age_days={newest_age_days:g}")
    age_buckets = payload.get("age_buckets") if isinstance(payload.get("age_buckets"), dict) else {}
    for family in ("series", "progress", "watchlist"):
        family_buckets = age_buckets.get(family) if isinstance(age_buckets, dict) else None
        if not isinstance(family_buckets, dict):
            continue
        for bucket in ("recent_0_7_days", "older_8_30_days", "older_31_plus_days"):
            value = family_buckets.get(bucket)
            count = int(value) if isinstance(value, int) and not isinstance(value, bool) else 0
            print(f"{family}_{bucket}_count={count}")
    linkage = payload.get("linkage") if isinstance(payload.get("linkage"), dict) else {}
    for family in ("progress", "watchlist"):
        family_linkage = linkage.get(family) if isinstance(linkage, dict) else None
        if not isinstance(family_linkage, dict):
            continue
        for key in ("with_stale_series", "with_current_series", "with_missing_series"):
            value = family_linkage.get(key)
            count = int(value) if isinstance(value, int) and not isinstance(value, bool) else 0
            print(f"{family}_{key}_count={count}")
    retention_review = payload.get("retention_review") if isinstance(payload.get("retention_review"), dict) else {}
    retention_posture = retention_review.get("posture") if isinstance(retention_review, dict) else None
    if isinstance(retention_posture, str) and retention_posture:
        print(f"retention_posture={retention_posture}")
    retention_review_candidate = retention_review.get("review_candidate") if isinstance(retention_review, dict) else None
    if isinstance(retention_review_candidate, bool):
        print(f"retention_review_candidate={retention_review_candidate}")
    for key in (
        "recent_0_7_days_count",
        "older_8_30_days_count",
        "older_31_plus_days_count",
        "current_linked_child_count",
        "missing_series_child_count",
    ):
        value = retention_review.get(key) if isinstance(retention_review, dict) else None
        if isinstance(value, int) and not isinstance(value, bool):
            print(f"retention_{key}={value}")
    retention_next_step = retention_review.get("next_step") if isinstance(retention_review, dict) else None
    if isinstance(retention_next_step, str) and retention_next_step:
        print(f"retention_next_step={retention_next_step}")
    total_count = payload.get("total_count")
    if isinstance(total_count, int) and not isinstance(total_count, bool):
        print(f"total_stale_count={total_count}")
    policy = payload.get("policy") if isinstance(payload.get("policy"), str) else None
    if policy is not None:
        print(f"policy={policy}")
    read_only = payload.get("read_only")
    if isinstance(read_only, bool):
        print(f"read_only={read_only}")
    detail = payload.get("detail") if isinstance(payload.get("detail"), str) else None
    if detail is not None:
        print(f"detail={detail}")
    providers = payload.get("providers") if isinstance(payload.get("providers"), dict) else {}
    for slug in sorted(providers):
        provider_payload = providers.get(slug)
        if not isinstance(provider_payload, dict):
            continue
        prefix = f"provider.{slug}"
        print(f"{prefix}.ready={provider_payload.get('ready') is True}")
        provider_cutoff = provider_payload.get("cutoff") if isinstance(provider_payload.get("cutoff"), str) else None
        if provider_cutoff is not None:
            print(f"{prefix}.cutoff={provider_cutoff}")
        provider_age_cutoff = provider_payload.get("age_cutoff") if isinstance(provider_payload.get("age_cutoff"), str) else None
        if provider_age_cutoff is not None:
            print(f"{prefix}.age_cutoff={provider_age_cutoff}")
        provider_older_than_days = provider_payload.get("older_than_days")
        if isinstance(provider_older_than_days, (int, float)) and not isinstance(provider_older_than_days, bool):
            print(f"{prefix}.older_than_days={provider_older_than_days:g}")
        provider_counts = provider_payload.get("counts") if isinstance(provider_payload.get("counts"), dict) else {}
        for family in ("series", "progress", "watchlist"):
            value = provider_counts.get(family) if isinstance(provider_counts, dict) else None
            count = int(value) if isinstance(value, int) and not isinstance(value, bool) else 0
            print(f"{prefix}.{family}_stale_count={count}")
        provider_ranges = provider_payload.get("last_seen_ranges") if isinstance(provider_payload.get("last_seen_ranges"), dict) else {}
        for family in ("series", "progress", "watchlist"):
            family_range = provider_ranges.get(family) if isinstance(provider_ranges, dict) else None
            if not isinstance(family_range, dict):
                continue
            oldest = family_range.get("oldest_last_seen_at")
            newest = family_range.get("newest_last_seen_at")
            if isinstance(oldest, str) and oldest:
                print(f"{prefix}.{family}_oldest_last_seen_at={oldest}")
            if isinstance(newest, str) and newest:
                print(f"{prefix}.{family}_newest_last_seen_at={newest}")
        provider_age_ranges_days = provider_payload.get("age_ranges_days") if isinstance(provider_payload.get("age_ranges_days"), dict) else {}
        for family in ("series", "progress", "watchlist"):
            family_age_range = provider_age_ranges_days.get(family) if isinstance(provider_age_ranges_days, dict) else None
            if not isinstance(family_age_range, dict):
                continue
            oldest_age_days = family_age_range.get("oldest_age_days")
            newest_age_days = family_age_range.get("newest_age_days")
            if isinstance(oldest_age_days, (int, float)) and not isinstance(oldest_age_days, bool):
                print(f"{prefix}.{family}_oldest_age_days={oldest_age_days:g}")
            if isinstance(newest_age_days, (int, float)) and not isinstance(newest_age_days, bool):
                print(f"{prefix}.{family}_newest_age_days={newest_age_days:g}")
        provider_age_buckets = provider_payload.get("age_buckets") if isinstance(provider_payload.get("age_buckets"), dict) else {}
        for family in ("series", "progress", "watchlist"):
            family_buckets = provider_age_buckets.get(family) if isinstance(provider_age_buckets, dict) else None
            if not isinstance(family_buckets, dict):
                continue
            for bucket in ("recent_0_7_days", "older_8_30_days", "older_31_plus_days"):
                value = family_buckets.get(bucket)
                count = int(value) if isinstance(value, int) and not isinstance(value, bool) else 0
                print(f"{prefix}.{family}_{bucket}_count={count}")
        provider_linkage = provider_payload.get("linkage") if isinstance(provider_payload.get("linkage"), dict) else {}
        for family in ("progress", "watchlist"):
            family_linkage = provider_linkage.get(family) if isinstance(provider_linkage, dict) else None
            if not isinstance(family_linkage, dict):
                continue
            for key in ("with_stale_series", "with_current_series", "with_missing_series"):
                value = family_linkage.get(key)
                count = int(value) if isinstance(value, int) and not isinstance(value, bool) else 0
                print(f"{prefix}.{family}_{key}_count={count}")
        provider_retention_review = provider_payload.get("retention_review") if isinstance(provider_payload.get("retention_review"), dict) else {}
        provider_retention_posture = provider_retention_review.get("posture") if isinstance(provider_retention_review, dict) else None
        if isinstance(provider_retention_posture, str) and provider_retention_posture:
            print(f"{prefix}.retention_posture={provider_retention_posture}")
        provider_retention_review_candidate = provider_retention_review.get("review_candidate") if isinstance(provider_retention_review, dict) else None
        if isinstance(provider_retention_review_candidate, bool):
            print(f"{prefix}.retention_review_candidate={provider_retention_review_candidate}")
        provider_total = provider_payload.get("total_count")
        if isinstance(provider_total, int) and not isinstance(provider_total, bool):
            print(f"{prefix}.total_stale_count={provider_total}")
        provider_detail = provider_payload.get("detail") if isinstance(provider_payload.get("detail"), str) else None
        if provider_detail is not None:
            print(f"{prefix}.detail={provider_detail}")


def _cmd_map_series(project_root: Path | None, limit: int, mapping_limit: int) -> int:
    config = load_config(project_root)
    states = load_provider_series_states(config, limit=limit)
    client = MalClient(config, load_mal_secrets(config))
    results = []
    for state in states:
        try:
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
                    max_completed_episode_number=state.max_completed_episode_number,
                    verified_mal_anime_id=state.verified_mal_anime_id,
                    verified_identity_kind=state.verified_identity_kind,
                    provider_episode_count=state.provider_episode_count,
                    provider_season_count=state.provider_season_count,
                    provider_start_year=state.provider_start_year,
                    provider_start_year_is_trustworthy=state.provider_start_year_is_trustworthy,
                    verified_identity_evidence=state.verified_identity_evidence,
                ),
                limit=mapping_limit,
            )
        except MalApiError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        results.append(
            {
                "provider_series_id": state.provider_series_id,
                "title": state.title,
                "season_title": state.season_title,
                "mapping_status": mapping.status,
                "confidence": mapping.confidence,
                "rationale": mapping.rationale,
                "chosen_candidate": None
                if not mapping.chosen_candidate
                else {
                    "mal_anime_id": mapping.chosen_candidate.mal_anime_id,
                    "title": mapping.chosen_candidate.title,
                    "score": mapping.chosen_candidate.score,
                    "matched_query": mapping.chosen_candidate.matched_query,
                    "match_reasons": mapping.chosen_candidate.match_reasons,
                },
                "bundle_companion_candidate": None
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
                "bundle_companion_candidates": [
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
                "candidates": [
                    {
                        "mal_anime_id": candidate.mal_anime_id,
                        "title": candidate.title,
                        "score": candidate.score,
                        "matched_query": candidate.matched_query,
                        "media_type": candidate.media_type,
                    }
                    for candidate in mapping.candidates
                ],
            }
        )
    print(json.dumps(results, indent=2))
    return 0


def _normalize_limit(limit: int) -> int | None:
    return None if limit <= 0 else limit


def _cmd_review_mappings(project_root: Path | None, limit: int, mapping_limit: int, persist_queue: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    normalized_limit = _normalize_limit(limit)
    if persist_queue and normalized_limit is not None:
        print("--persist-review-queue requires a full scan; rerun with --limit 0", file=sys.stderr)
        return 2
    try:
        items = build_mapping_review(config, limit=normalized_limit, mapping_limit=mapping_limit)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload: dict[str, object] = {"items": [item.as_dict() for item in items]}
    if persist_queue:
        payload["review_queue"] = persist_mapping_review_queue(config, items)
    print(json.dumps(payload, indent=2))
    return 0


def _refresh_mapping_review_queue_for_provider_series_ids(
    config: object,
    provider_series_ids: list[str],
    mapping_limit: int,
) -> dict[str, object]:
    normalized_provider_series_ids = sorted({value.strip() for value in provider_series_ids if isinstance(value, str) and value.strip()})
    items = build_mapping_review(
        config,
        limit=None,
        mapping_limit=mapping_limit,
        provider_series_ids=normalized_provider_series_ids,
    )
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
    review_queue_result = refresh_review_queue_entries(
        config.db_path,
        issue_type="mapping_review",
        provider_series_ids=normalized_provider_series_ids,
        entries=queue_entries,
    )
    return {
        "provider_series_ids": normalized_provider_series_ids,
        "items": [item.as_dict() for item in items],
        "review_queue": review_queue_result,
    }


def _resolve_refresh_mapping_review_queue_provider_series_ids(
    config: object,
    provider_series_ids: list[str],
    *,
    include_all_open: bool,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
) -> list[str]:
    normalized_provider_series_ids = {
        value.strip() for value in provider_series_ids if isinstance(value, str) and value.strip()
    }
    if include_all_open or any(
        value is not None
        for value in (
            title_cluster,
            fix_strategy,
            cluster_strategy,
            decision,
            reason,
            reason_family,
            fix_strategy_family,
            cluster_strategy_family,
        )
    ):
        open_rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")
        provider_series_titles = get_provider_series_title_map_by_keys(
            config.db_path,
            provider_series_keys=[
                (item.provider, item.provider_series_id)
                for item in open_rows
                if item.provider and item.provider_series_id
            ],
        )
        filtered_open_rows = _filter_review_queue_items(
            open_rows,
            provider_series_titles=provider_series_titles,
            title_cluster=title_cluster,
            fix_strategy=fix_strategy,
            cluster_strategy=cluster_strategy,
            decision=decision,
            reason=reason,
            reason_family=reason_family,
            fix_strategy_family=fix_strategy_family,
            cluster_strategy_family=cluster_strategy_family,
        )
        normalized_provider_series_ids.update(
            str(item.provider_series_id).strip()
            for item in filtered_open_rows
            if isinstance(item.provider_series_id, str) and item.provider_series_id.strip()
        )
    return sorted(normalized_provider_series_ids)


def _cmd_refresh_mapping_review_queue(
    project_root: Path | None,
    provider_series_ids: list[str],
    mapping_limit: int,
    include_all_open: bool = False,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    normalized_provider_series_ids = _resolve_refresh_mapping_review_queue_provider_series_ids(
        config,
        provider_series_ids,
        include_all_open=include_all_open,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    if not normalized_provider_series_ids:
        print(
            "--provider-series-id is required at least once (or use --all-open or a queue-slice filter)",
            file=sys.stderr,
        )
        return 2
    try:
        payload = _refresh_mapping_review_queue_for_provider_series_ids(
            config,
            normalized_provider_series_ids,
            mapping_limit,
        )
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_list_mappings(project_root: Path | None, approved_only: bool, provider: str | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    normalized_provider = None if provider in {None, "all"} else provider
    items = list_series_mappings(config.db_path, provider=normalized_provider, approved_only=approved_only)
    print(
        json.dumps(
            [
                {
                    "provider": item.provider,
                    "provider_series_id": item.provider_series_id,
                    "mal_anime_id": item.mal_anime_id,
                    "confidence": item.confidence,
                    "mapping_source": item.mapping_source,
                    "approved_by_user": item.approved_by_user,
                    "notes": item.notes,
                    "created_at": item.created_at,
                    "updated_at": item.updated_at,
                }
                for item in items
            ],
            indent=2,
        )
    )
    return 0


def _cmd_approve_mapping(
    project_root: Path | None,
    provider_series_id: str,
    mal_anime_id: int,
    confidence: float | None,
    notes: str | None,
    exact: bool,
    provider: str = "crunchyroll",
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    mapping = upsert_series_mapping(
        config.db_path,
        provider=provider,
        provider_series_id=provider_series_id,
        mal_anime_id=mal_anime_id,
        confidence=confidence,
        mapping_source="user_exact" if exact else "user_approved",
        approved_by_user=True,
        notes=notes,
    )
    print(
        json.dumps(
            {
                "provider": mapping.provider,
                "provider_series_id": mapping.provider_series_id,
                "mal_anime_id": mapping.mal_anime_id,
                "confidence": mapping.confidence,
                "mapping_source": mapping.mapping_source,
                "approved_by_user": mapping.approved_by_user,
                "notes": mapping.notes,
                "created_at": mapping.created_at,
                "updated_at": mapping.updated_at,
            },
            indent=2,
        )
    )
    return 0


def _cmd_dry_run_sync(
    project_root: Path | None,
    provider: str = "crunchyroll",
    limit: int = 20,
    mapping_limit: int = 5,
    approved_mappings_only: bool = False,
    exact_approved_only: bool = False,
    persist_queue: bool = False,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    normalized_limit = _normalize_limit(limit)
    if persist_queue and normalized_limit is not None:
        print("--persist-review-queue requires a full scan; rerun with --limit 0", file=sys.stderr)
        return 2
    try:
        proposals = build_dry_run_sync_plan(
            config,
            limit=normalized_limit,
            mapping_limit=mapping_limit,
            approved_mappings_only=approved_mappings_only,
            exact_approved_only=exact_approved_only,
            provider=None if provider == "all" else provider,
        )
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload: dict[str, object] = {"proposals": [proposal.as_dict() for proposal in proposals]}
    if persist_queue:
        payload["review_queue"] = persist_sync_review_queue(config, proposals)
    print(json.dumps(payload, indent=2))
    return 0

















































def _cmd_review_queue_next(
    project_root: Path | None,
    status: str,
    issue_type: str | None,
    bucket: str,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    items = list_review_queue_entries(config.db_path, status=status, issue_type=issue_type)
    provider_series_titles = get_provider_series_title_map_by_keys(
        config.db_path,
        provider_series_keys=[
            (item.provider, item.provider_series_id)
            for item in items
            if item.provider and item.provider_series_id
        ],
    )
    filtered_items = _filter_review_queue_items(
        items,
        provider_series_titles=provider_series_titles,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    summary = _summarize_review_queue(
        filtered_items,
        status=status,
        issue_type=issue_type,
        provider_series_titles=provider_series_titles,
        title_cluster_filter=title_cluster,
        fix_strategy_filter=fix_strategy,
        cluster_strategy_filter=cluster_strategy,
        decision_filter=decision,
        reason_filter=reason,
        reason_family_filter=reason_family,
        fix_strategy_family_filter=fix_strategy_family,
        cluster_strategy_family_filter=cluster_strategy_family,
    )
    bucket_order = [bucket] if bucket != "auto" else list(_review_queue_support._REVIEW_QUEUE_AUTO_BUCKET_ORDER)
    chosen_bucket = None
    for candidate_bucket in bucket_order:
        chosen_bucket = _review_queue_support._select_review_queue_next_bucket(summary, bucket=candidate_bucket)
        if chosen_bucket is not None:
            break
    payload = {
        "status": status,
        "issue_type_filter": issue_type,
        "title_cluster_filter": _review_queue_support._review_queue_title_cluster_key(title_cluster) if title_cluster else None,
        "fix_strategy_filter": fix_strategy.strip() if isinstance(fix_strategy, str) and fix_strategy.strip() else None,
        "cluster_strategy_filter": cluster_strategy.strip() if isinstance(cluster_strategy, str) and cluster_strategy.strip() else None,
        "decision_filter": decision.strip() if isinstance(decision, str) and decision.strip() else None,
        "reason_filter": reason.strip() if isinstance(reason, str) and reason.strip() else None,
        "reason_family_filter": _review_queue_support._review_queue_reason_family(reason_family) if isinstance(reason_family, str) and reason_family.strip() else None,
        "fix_strategy_family_filter": fix_strategy_family.strip() if isinstance(fix_strategy_family, str) and fix_strategy_family.strip() else None,
        "cluster_strategy_family_filter": cluster_strategy_family.strip() if isinstance(cluster_strategy_family, str) and cluster_strategy_family.strip() else None,
        "count": summary["count"],
        "bucket_preference": bucket,
        "selected": chosen_bucket,
    }
    print(json.dumps(payload, indent=2))
    return 0


def _load_filtered_review_queue_context(
    project_root: Path | None,
    *,
    status: str,
    issue_type: str | None,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> tuple[object, dict[tuple[str, str], dict[str, str | None]], list[object], dict[str, object]]:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    items = list_review_queue_entries(config.db_path, status=status, issue_type=issue_type)
    provider_series_titles = get_provider_series_title_map_by_keys(
        config.db_path,
        provider_series_keys=[
            (item.provider, item.provider_series_id)
            for item in items
            if item.provider and item.provider_series_id
        ],
    )
    filtered_items = _filter_review_queue_items(
        items,
        provider_series_titles=provider_series_titles,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    summary = _summarize_review_queue(
        filtered_items,
        status=status,
        issue_type=issue_type,
        provider_series_titles=provider_series_titles,
        title_cluster_filter=title_cluster,
        fix_strategy_filter=fix_strategy,
        cluster_strategy_filter=cluster_strategy,
        decision_filter=decision,
        reason_filter=reason,
        reason_family_filter=reason_family,
        fix_strategy_family_filter=fix_strategy_family,
        cluster_strategy_family_filter=cluster_strategy_family,
    )
    return config, provider_series_titles, filtered_items, summary


def _cmd_review_queue_worklist(
    project_root: Path | None,
    status: str,
    issue_type: str | None,
    limit: int,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> int:
    _, _, _, summary = _load_filtered_review_queue_context(
        project_root,
        status=status,
        issue_type=issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    selected = _review_queue_support._build_review_queue_worklist(
        summary,
        bucket_order=list(_review_queue_support._REVIEW_QUEUE_AUTO_BUCKET_ORDER),
        limit=limit,
    )
    payload = {
        "status": status,
        "issue_type_filter": issue_type,
        "title_cluster_filter": _review_queue_support._review_queue_title_cluster_key(title_cluster) if title_cluster else None,
        "fix_strategy_filter": fix_strategy.strip() if isinstance(fix_strategy, str) and fix_strategy.strip() else None,
        "cluster_strategy_filter": cluster_strategy.strip() if isinstance(cluster_strategy, str) and cluster_strategy.strip() else None,
        "decision_filter": decision.strip() if isinstance(decision, str) and decision.strip() else None,
        "reason_filter": reason.strip() if isinstance(reason, str) and reason.strip() else None,
        "reason_family_filter": _review_queue_support._review_queue_reason_family(reason_family) if isinstance(reason_family, str) and reason_family.strip() else None,
        "fix_strategy_family_filter": fix_strategy_family.strip() if isinstance(fix_strategy_family, str) and fix_strategy_family.strip() else None,
        "cluster_strategy_family_filter": cluster_strategy_family.strip() if isinstance(cluster_strategy_family, str) and cluster_strategy_family.strip() else None,
        "count": summary["count"],
        "limit": limit,
        "selected": selected,
    }
    print(json.dumps(payload, indent=2))
    return 0




def _cmd_review_queue_apply_worklist(
    project_root: Path | None,
    status: str,
    issue_type: str | None,
    limit: int,
    per_bucket_limit: int,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> int:
    config, provider_series_titles, filtered_items, summary = _load_filtered_review_queue_context(
        project_root,
        status=status,
        issue_type=issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    selected_buckets = _review_queue_support._build_review_queue_worklist(
        summary,
        bucket_order=list(_review_queue_support._REVIEW_QUEUE_AUTO_BUCKET_ORDER),
        limit=limit,
    )
    selected_ids: list[int] = []
    seen_ids: set[int] = set()
    bucket_updates: list[dict[str, object]] = []
    for candidate in selected_buckets:
        bucket_filters = _review_queue_support._review_queue_bucket_filter_kwargs(candidate)
        bucket_items = _filter_review_queue_items(
            filtered_items,
            provider_series_titles=provider_series_titles,
            **bucket_filters,
        )
        chosen_items = bucket_items if per_bucket_limit == 0 else bucket_items[:per_bucket_limit]
        new_items = [item for item in chosen_items if item.id not in seen_ids]
        if not new_items:
            continue
        for item in new_items:
            seen_ids.add(item.id)
            selected_ids.append(item.id)
        bucket_updates.append(
            {
                **candidate,
                "matched_rows": len(bucket_items),
                "selected_rows": len(chosen_items),
                "new_rows": len(new_items),
                "selected_entry_ids": [item.id for item in new_items],
            }
        )
    status_to = "open" if status == "resolved" else "resolved"
    updated_count = update_review_queue_entry_statuses(
        config.db_path,
        entry_ids=selected_ids,
        status=status_to,
    )
    payload = {
        "status_from": status,
        "status_to": status_to,
        "issue_type_filter": issue_type,
        "title_cluster_filter": _review_queue_support._review_queue_title_cluster_key(title_cluster) if title_cluster else None,
        "fix_strategy_filter": fix_strategy.strip() if isinstance(fix_strategy, str) and fix_strategy.strip() else None,
        "cluster_strategy_filter": cluster_strategy.strip() if isinstance(cluster_strategy, str) and cluster_strategy.strip() else None,
        "decision_filter": decision.strip() if isinstance(decision, str) and decision.strip() else None,
        "reason_filter": reason.strip() if isinstance(reason, str) and reason.strip() else None,
        "reason_family_filter": _review_queue_support._review_queue_reason_family(reason_family) if isinstance(reason_family, str) and reason_family.strip() else None,
        "fix_strategy_family_filter": fix_strategy_family.strip() if isinstance(fix_strategy_family, str) and fix_strategy_family.strip() else None,
        "cluster_strategy_family_filter": cluster_strategy_family.strip() if isinstance(cluster_strategy_family, str) and cluster_strategy_family.strip() else None,
        "count": summary["count"],
        "worklist_limit": limit,
        "per_bucket_limit": per_bucket_limit,
        "selected_bucket_count": len(selected_buckets),
        "updated": updated_count,
        "selected_entry_ids": selected_ids,
        "selected_buckets": bucket_updates,
    }
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_review_queue_refresh_worklist(
    project_root: Path | None,
    status: str,
    issue_type: str | None,
    limit: int,
    per_bucket_limit: int,
    mapping_limit: int,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
    output_format: str = "json",
) -> int:
    effective_issue_type = issue_type or "mapping_review"
    if effective_issue_type != "mapping_review":
        print("review-queue-refresh-worklist currently supports only mapping_review", file=sys.stderr)
        return 2
    config, provider_series_titles, filtered_items, summary = _load_filtered_review_queue_context(
        project_root,
        status=status,
        issue_type=effective_issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    selected_buckets = _review_queue_support._build_review_queue_worklist(
        summary,
        bucket_order=list(_review_queue_support._REVIEW_QUEUE_AUTO_BUCKET_ORDER),
        limit=limit,
    )
    selected_provider_series_ids: list[str] = []
    seen_provider_series_ids: set[str] = set()
    bucket_updates: list[dict[str, object]] = []
    for candidate in selected_buckets:
        bucket_filters = _review_queue_support._review_queue_bucket_filter_kwargs(candidate)
        bucket_items = _filter_review_queue_items(
            filtered_items,
            provider_series_titles=provider_series_titles,
            **bucket_filters,
        )
        chosen_items = bucket_items if per_bucket_limit == 0 else bucket_items[:per_bucket_limit]
        chosen_provider_series_ids = [
            item.provider_series_id
            for item in chosen_items
            if isinstance(item.provider_series_id, str) and item.provider_series_id.strip()
        ]
        new_provider_series_ids = [
            provider_series_id
            for provider_series_id in chosen_provider_series_ids
            if provider_series_id not in seen_provider_series_ids
        ]
        if not new_provider_series_ids:
            continue
        for provider_series_id in new_provider_series_ids:
            seen_provider_series_ids.add(provider_series_id)
            selected_provider_series_ids.append(provider_series_id)
        bucket_updates.append(
            {
                **candidate,
                "matched_rows": len(bucket_items),
                "selected_rows": len(chosen_items),
                "new_rows": len(new_provider_series_ids),
                "selected_provider_series_ids": new_provider_series_ids,
            }
        )
    try:
        refresh_result = _refresh_mapping_review_queue_for_provider_series_ids(
            config,
            selected_provider_series_ids,
            mapping_limit,
        ) if selected_provider_series_ids else {
            "provider_series_ids": [],
            "items": [],
            "review_queue": {"resolved": 0, "inserted": 0},
        }
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload = {
        "status": status,
        "issue_type_filter": effective_issue_type,
        "title_cluster_filter": _review_queue_support._review_queue_title_cluster_key(title_cluster) if title_cluster else None,
        "fix_strategy_filter": fix_strategy.strip() if isinstance(fix_strategy, str) and fix_strategy.strip() else None,
        "cluster_strategy_filter": cluster_strategy.strip() if isinstance(cluster_strategy, str) and cluster_strategy.strip() else None,
        "decision_filter": decision.strip() if isinstance(decision, str) and decision.strip() else None,
        "reason_filter": reason.strip() if isinstance(reason, str) and reason.strip() else None,
        "reason_family_filter": _review_queue_support._review_queue_reason_family(reason_family) if isinstance(reason_family, str) and reason_family.strip() else None,
        "fix_strategy_family_filter": fix_strategy_family.strip() if isinstance(fix_strategy_family, str) and fix_strategy_family.strip() else None,
        "cluster_strategy_family_filter": cluster_strategy_family.strip() if isinstance(cluster_strategy_family, str) and cluster_strategy_family.strip() else None,
        "count": summary["count"],
        "worklist_limit": limit,
        "per_bucket_limit": per_bucket_limit,
        "mapping_limit": mapping_limit,
        "selected_bucket_count": len(selected_buckets),
        "selected_provider_series_ids": selected_provider_series_ids,
        "selected_buckets": bucket_updates,
        "refresh": refresh_result,
    }
    if output_format == "summary":
        print(f"issue_type_filter={payload['issue_type_filter']}")
        print(f"selected_bucket_count={payload['selected_bucket_count']}")
        print(f"selected_provider_series_ids={len(payload['selected_provider_series_ids'])}")
        review_queue = refresh_result.get("review_queue") if isinstance(refresh_result, dict) else None
        if isinstance(review_queue, dict):
            print(f"review_queue_resolved={review_queue.get('resolved', 0)}")
            print(f"review_queue_inserted={review_queue.get('inserted', 0)}")
    else:
        print(json.dumps(payload, indent=2))
    return 0


def _cmd_list_review_queue(
    project_root: Path | None,
    status: str,
    issue_type: str | None,
    summary: bool,
    limit: int,
    provider_series_id: str | None,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    items = list_review_queue_entries(
        config.db_path,
        status=status,
        issue_type=issue_type,
        provider_series_id=provider_series_id,
    )
    provider_series_titles = get_provider_series_title_map_by_keys(
        config.db_path,
        provider_series_keys=[
            (item.provider, item.provider_series_id)
            for item in items
            if item.provider and item.provider_series_id
        ],
    )
    items = _filter_review_queue_items(
        items,
        provider_series_titles=provider_series_titles,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    if summary:
        print(
            json.dumps(
                _summarize_review_queue(
                    items,
                    status=status,
                    issue_type=issue_type,
                    provider_series_titles=provider_series_titles,
                    title_cluster_filter=title_cluster,
                    fix_strategy_filter=fix_strategy,
                    cluster_strategy_filter=cluster_strategy,
                    decision_filter=decision,
                    reason_filter=reason,
                ),
                indent=2,
            )
        )
        return 0
    if limit > 0:
        items = items[:limit]
    print(
        json.dumps(
            [
                {
                    "id": item.id,
                    "provider": item.provider,
                    "provider_series_id": item.provider_series_id,
                    "provider_episode_id": item.provider_episode_id,
                    "issue_type": item.issue_type,
                    "severity": item.severity,
                    "status": item.status,
                    "created_at": item.created_at,
                    "resolved_at": item.resolved_at,
                    "payload": item.payload,
                }
                for item in items
            ],
            indent=2,
        )
    )
    return 0


def _cmd_update_review_queue_status(
    project_root: Path | None,
    *,
    status_from: str,
    status_to: str,
    issue_type: str | None,
    limit: int,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    items = list_review_queue_entries(config.db_path, status=status_from, issue_type=issue_type)
    provider_series_titles = get_provider_series_title_map_by_keys(
        config.db_path,
        provider_series_keys=[
            (item.provider, item.provider_series_id)
            for item in items
            if item.provider and item.provider_series_id
        ],
    )
    filtered_items = _filter_review_queue_items(
        items,
        provider_series_titles=provider_series_titles,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    selected_items = filtered_items if limit == 0 else filtered_items[:limit]
    updated_count = update_review_queue_entry_statuses(
        config.db_path,
        entry_ids=[item.id for item in selected_items],
        status=status_to,
    )
    payload = {
        "status_from": status_from,
        "status_to": status_to,
        "issue_type_filter": issue_type,
        "title_cluster_filter": _review_queue_support._review_queue_title_cluster_key(title_cluster) if title_cluster else None,
        "fix_strategy_filter": fix_strategy.strip() if isinstance(fix_strategy, str) and fix_strategy.strip() else None,
        "cluster_strategy_filter": cluster_strategy.strip() if isinstance(cluster_strategy, str) and cluster_strategy.strip() else None,
        "decision_filter": decision.strip() if isinstance(decision, str) and decision.strip() else None,
        "reason_filter": reason.strip() if isinstance(reason, str) and reason.strip() else None,
        "reason_family_filter": _review_queue_support._review_queue_reason_family(reason_family) if isinstance(reason_family, str) and reason_family.strip() else None,
        "fix_strategy_family_filter": fix_strategy_family.strip() if isinstance(fix_strategy_family, str) and fix_strategy_family.strip() else None,
        "cluster_strategy_family_filter": cluster_strategy_family.strip() if isinstance(cluster_strategy_family, str) and cluster_strategy_family.strip() else None,
        "limit": limit,
        "matched": len(filtered_items),
        "updated": updated_count,
        "selected": [
            {
                "id": item.id,
                "provider_series_id": item.provider_series_id,
                "issue_type": item.issue_type,
                "severity": item.severity,
                "decision": item.payload.get("decision") if isinstance(item.payload, dict) else None,
                "reasons": item.payload.get("reasons") if isinstance(item.payload.get("reasons"), list) else [],
                "title": _review_queue_item_label(item, provider_series_titles=provider_series_titles).get("title"),
            }
            for item in selected_items
        ],
    }
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_resolve_review_queue(
    project_root: Path | None,
    issue_type: str | None,
    limit: int,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> int:
    return _cmd_update_review_queue_status(
        project_root,
        status_from="open",
        status_to="resolved",
        issue_type=issue_type,
        limit=limit,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )


def _cmd_reopen_review_queue(
    project_root: Path | None,
    issue_type: str | None,
    limit: int,
    title_cluster: str | None,
    fix_strategy: str | None,
    cluster_strategy: str | None,
    decision: str | None,
    reason: str | None,
    reason_family: str | None,
    fix_strategy_family: str | None,
    cluster_strategy_family: str | None,
) -> int:
    return _cmd_update_review_queue_status(
        project_root,
        status_from="resolved",
        status_to="open",
        issue_type=issue_type,
        limit=limit,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )


def _run_apply_sync(config, *, limit: int, mapping_limit: int, exact_approved_only: bool, execute: bool):
    ensure_directories(config)
    bootstrap_database(config.db_path)
    return execute_approved_sync(
        config,
        limit=_normalize_limit(limit),
        mapping_limit=mapping_limit,
        exact_approved_only=exact_approved_only,
        dry_run=not execute,
    )


def _cmd_apply_sync(project_root: Path | None, limit: int, mapping_limit: int, exact_approved_only: bool, execute: bool) -> int:
    config = load_config(project_root)
    try:
        results = _run_apply_sync(
            config,
            limit=limit,
            mapping_limit=mapping_limit,
            exact_approved_only=exact_approved_only,
            execute=execute,
        )
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps([item.as_dict() for item in results], indent=2))
    return 0


def _iter_exact_approved_sync_cycle_providers(config) -> list[tuple[str, Path]]:
    providers: list[tuple[str, Path]] = []
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    if crunchyroll_credentials.username and crunchyroll_credentials.password:
        providers.append(("crunchyroll", config.cache_dir / "live-crunchyroll-snapshot.json"))
    hidive_credentials = load_hidive_credentials(config)
    if hidive_credentials.username and hidive_credentials.password:
        providers.append(("hidive", config.cache_dir / "live-hidive-snapshot.json"))
    return providers


def _cmd_exact_approved_sync_cycle(project_root: Path | None, full_refresh: bool, allow_stale_provider_apply: bool = False) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)

    provider_targets = _iter_exact_approved_sync_cycle_providers(config)
    fetches: list[dict[str, object]] = []
    for provider_slug, snapshot_path in provider_targets:
        fetch_stdout = io.StringIO()
        fetch_stderr = io.StringIO()
        with redirect_stdout(fetch_stdout), redirect_stderr(fetch_stderr):
            exit_code = _cmd_provider_fetch_snapshot(
                config.project_root,
                provider_slug,
                "default",
                snapshot_path,
                True,
                full_refresh,
            )
        fetch_payload: dict[str, object] = {
            "provider": provider_slug,
            "snapshot_path": str(snapshot_path),
            "full_refresh": full_refresh,
            "exit_code": exit_code,
            "status": "ok" if exit_code == 0 else "failed",
            "failed": exit_code != 0,
        }
        fetches.append(fetch_payload)

    failed_fetches = [item for item in fetches if item["exit_code"] != 0]
    provider_refresh_reason: str | None = None
    if not provider_targets:
        provider_refresh_status = "not_configured"
        provider_refresh_reason = "no_provider_targets"
    elif failed_fetches:
        provider_refresh_status = "failed"
        provider_refresh_reason = "provider_refresh_failed"
    else:
        provider_refresh_status = "ok"

    stale_provider_apply_authorized = bool(provider_refresh_reason and allow_stale_provider_apply)
    warnings: list[dict[str, object]] = []
    if provider_refresh_reason == "no_provider_targets":
        warnings.append(
            {
                "code": "no_provider_targets",
                "message": "No credentialed provider targets were configured; applying would use stale local DB state.",
            }
        )
    if provider_refresh_reason == "provider_refresh_failed":
        warnings.append(
            {
                "code": "provider_refresh_failed",
                "message": "One or more configured provider refreshes failed; applying would use stale local DB state.",
                "providers": [str(item["provider"]) for item in failed_fetches],
            }
        )
    if stale_provider_apply_authorized:
        warnings.append(
            {
                "code": "stale_provider_apply_authorized",
                "message": "Operator supplied --allow-stale-provider-apply, so exact-approved apply may proceed using existing local DB state.",
                "reason": provider_refresh_reason,
            }
        )

    if provider_refresh_reason and not allow_stale_provider_apply:
        apply_payload = {
            "exact_approved_only": True,
            "limit": 0,
            "mapping_limit": 5,
            "execute": True,
            "status": "skipped",
            "skipped": True,
            "skip_reason": provider_refresh_reason,
            "reason": provider_refresh_reason,
        }
        summary = {
            "status": "aborted",
            "reason": provider_refresh_reason,
            "allow_stale_provider_apply": allow_stale_provider_apply,
            "stale_provider_apply_authorized": False,
            "stale_provider_apply_reason": provider_refresh_reason,
            "provider_refresh": {
                "status": provider_refresh_status,
                "reason": provider_refresh_reason,
                "target_count": len(provider_targets),
                "attempted_count": len(fetches),
                "succeeded_count": len([item for item in fetches if item["exit_code"] == 0]),
                "failed_count": len(failed_fetches),
                "failed_providers": [str(item["provider"]) for item in failed_fetches],
            },
            "providers_considered": [provider for provider, _ in provider_targets],
            "providers_fetch_attempted": [str(item["provider"]) for item in fetches],
            "providers_fetched": [str(item["provider"]) for item in fetches if item["exit_code"] == 0],
            "providers_failed": [str(item["provider"]) for item in failed_fetches],
            "fetches": fetches,
            "apply_skipped": True,
            "apply_skip_reason": provider_refresh_reason,
            "warnings": warnings,
            "apply": apply_payload,
        }
        print(json.dumps(summary, indent=2))
        return 1

    try:
        apply_results = _run_apply_sync(
            config,
            limit=0,
            mapping_limit=5,
            exact_approved_only=True,
            execute=True,
        )
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        apply_exit_code = 1
        apply_payload = {
            "exact_approved_only": True,
            "limit": 0,
            "mapping_limit": 5,
            "execute": True,
            "status": "error",
            "skipped": False,
            "error": str(exc),
        }
    else:
        apply_exit_code = 0
        apply_payload = {
            "exact_approved_only": True,
            "limit": 0,
            "mapping_limit": 5,
            "execute": True,
            "status": "ok",
            "skipped": False,
            "results": [item.as_dict() for item in apply_results],
        }

    summary = {
        "status": "error" if apply_exit_code != 0 else "ok_with_warnings" if warnings else "ok",
        "reason": "apply_failed" if apply_exit_code != 0 else "stale_provider_apply_authorized" if stale_provider_apply_authorized else None,
        "allow_stale_provider_apply": allow_stale_provider_apply,
        "stale_provider_apply_authorized": stale_provider_apply_authorized,
        "stale_provider_apply_reason": provider_refresh_reason,
        "provider_refresh": {
            "status": provider_refresh_status,
            "reason": provider_refresh_reason,
            "target_count": len(provider_targets),
            "attempted_count": len(fetches),
            "succeeded_count": len([item for item in fetches if item["exit_code"] == 0]),
            "failed_count": len(failed_fetches),
            "failed_providers": [str(item["provider"]) for item in failed_fetches],
        },
        "providers_considered": [provider for provider, _ in provider_targets],
        "providers_fetch_attempted": [str(item["provider"]) for item in fetches],
        "providers_fetched": [str(item["provider"]) for item in fetches if item["exit_code"] == 0],
        "providers_failed": [str(item["provider"]) for item in failed_fetches],
        "fetches": fetches,
        "apply_skipped": False,
        "apply_skip_reason": None,
        "warnings": warnings,
        "apply": apply_payload,
    }
    print(json.dumps(summary, indent=2))
    return 0 if apply_exit_code == 0 else 1


def _snapshot_recommendation_rows(results: list[object], limit: int | None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in results:
        if hasattr(item, "as_dict"):
            row = item.as_dict()
        elif isinstance(item, dict):
            row = item
        else:
            continue
        if isinstance(row, dict):
            rows.append(row)
        if limit is not None and limit > 0 and len(rows) >= limit:
            break
    return rows


def _cmd_recommend(project_root: Path | None, limit: int, flat: bool, include_dormant: bool, persist_snapshot: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    normalized_limit = _normalize_limit(limit)
    results = build_recommendations(
        config,
        limit=normalized_limit if flat else 0,
        require_provider_availability=not include_dormant,
        include_discovery_candidates_without_actionable_provider_evidence=include_dormant,
    )
    payload: object
    if flat:
        payload = [item.as_dict() for item in results]
    else:
        payload = trim_grouped_recommendations(group_recommendations(results), normalized_limit)
    if persist_snapshot:
        run_id = f"recommend-{uuid.uuid4()}"
        generated_at = datetime.now(timezone.utc).isoformat()
        insert_recommendation_snapshot_rows(
            config.db_path,
            _snapshot_recommendation_rows(results, normalized_limit),
            run_id=run_id,
            generated_at=generated_at,
        )
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_recommend_snapshots(project_root: Path | None, limit: int, output_format: str = "json") -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    rows = list_latest_recommendation_snapshot_rows(config.db_path, limit=_normalize_limit(limit) or 100)
    payload = []
    for row in rows:
        item = recommendation_snapshot_row_base_payload(row)
        item["providers"] = item.pop("availability_providers")
        item["availability"] = recommendation_snapshot_availability_payload(row)
        payload.append(item)
    if output_format == "summary":
        run_id = payload[0].get("run_id") if payload else None
        generated_at = payload[0].get("generated_at") if payload else None
        print(f"recommendation_snapshot_rows={len(payload)}")
        if run_id is not None:
            print(f"run_id={run_id}")
        if generated_at is not None:
            print(f"generated_at={generated_at}")
    else:
        print(json.dumps(payload, indent=2))
    return 0


def _cmd_recommend_dashboard(project_root: Path | None, output: Path, limit: int, include_dormant: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    display_limit = _normalize_limit(limit)
    results = build_recommendations(
        config,
        limit=None,
        require_provider_availability=not include_dormant,
        include_discovery_candidates_without_actionable_provider_evidence=include_dormant,
    )
    written = write_recommendation_dashboard(output, results, limit=display_limit, diagnostic_mode=include_dormant)
    print(json.dumps({"status": "ok", "output": str(written), "recommendation_count": len(results), "display_limit_per_section": display_limit}, indent=2))
    return 0


def _cmd_dashboard_serve(project_root: Path | None, host: str, port: int, limit: int) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    serve_dashboard(config.db_path, host=host, port=port, limit=_normalize_limit(limit) or DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT)
    return 0


def _cmd_mal_list_refresh(
    project_root: Path | None,
    statuses: list[str] | None,
    page_size: int,
    max_pages: int,
    complete: bool,
    output_format: str,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    summary = refresh_mal_user_anime_list_cache(
        config,
        statuses=statuses or ["all"],
        page_size=page_size,
        max_pages=max(0, int(max_pages)),
        prune_on_complete=complete,
    )
    if output_format == "summary":
        payload = summary.as_dict()
        print(
            " ".join(
                [
                    f"status={payload['status']}",
                    f"partial={str(payload['partial']).lower()}",
                    f"pages={payload['pages']}",
                    f"items={payload['items']}",
                    f"upserted={payload['upserted']}",
                    f"pruned={payload['pruned']}",
                    f"preserved_absent={payload['preserved_absent']}",
                    f"scored={payload['scored']}",
                    f"unscored={payload['unscored']}",
                    f"preference_counts={json.dumps(payload.get('preference_counts', {}), sort_keys=True)}",
                    f"by_status={json.dumps(payload['by_status'], sort_keys=True)}",
                ]
            )
        )
        if payload.get("error"):
            print(f"detail={payload['error']}")
    else:
        print(json.dumps(summary.as_dict(), indent=2))
    return 0 if summary.status in {"ok", "partial"} else 1


def _cmd_recommend_refresh_metadata(
    project_root: Path | None,
    limit: int,
    include_discovery_targets: bool,
    discovery_target_limit: int,
    force_refresh: bool = False,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    summary = refresh_recommendation_metadata(
        config,
        limit=_normalize_limit(limit),
        include_discovery_targets=include_discovery_targets,
        discovery_target_limit=_normalize_limit(discovery_target_limit),
        force_refresh=force_refresh,
    )
    print(json.dumps(summary.as_dict(), indent=2))
    return 0


def _cmd_recommend_refresh_full_userrecs(
    project_root: Path | None,
    limit: int,
    force_refresh: bool,
    stale_after_days: int,
    max_pages: int,
    max_body_mb: float,
    output_format: str,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    max_body_bytes = max(1024, int(float(max_body_mb) * 1024 * 1024))
    summary = refresh_full_user_recommendation_harvest(
        config,
        limit=_normalize_limit(limit),
        force_refresh=force_refresh,
        stale_after_days=max(1, int(stale_after_days)),
        max_pages=max(1, int(max_pages)),
        max_body_bytes=max_body_bytes,
    )
    payload = summary.as_dict()
    if output_format == "summary":
        print(
            " ".join(
                [
                    f"status={payload['status']}",
                    f"seed_count={payload['seed_count']}",
                    f"considered={payload['considered']}",
                    f"harvested={payload['harvested']}",
                    f"failed={payload['failed']}",
                    f"paused={payload.get('paused', 0)}",
                    f"skipped_fresh={payload['skipped_fresh']}",
                    f"total_edges={payload['total_edges']}",
                    f"max_pages={payload['max_pages']}",
                    "max_pages_per_source_per_run=true",
                    "partial_preserves_existing_edges=true",
                ]
            )
        )
        for failure in payload.get("failures", []):
            if isinstance(failure, dict):
                print(
                    "failure="
                    + json.dumps(
                        {
                            "mal_anime_id": failure.get("mal_anime_id"),
                            "pages_fetched": failure.get("pages_fetched"),
                            "error": failure.get("error"),
                        },
                        sort_keys=True,
                    )
                )
    else:
        print(json.dumps(payload, indent=2))
    return 0 if summary.status in {"ok", "partial"} else 1


def _cmd_recommend_enrich_provider_availability(
    project_root: Path | None,
    limit: int,
    provider: str | None,
    search_limit: int,
    queries_per_candidate: int,
    dry_run: bool,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    provider_slugs = [provider] if provider else list(list_provider_slugs())
    providers = [get_provider(slug) for slug in provider_slugs]
    summary = enrich_discovery_provider_availability(
        config,
        providers=providers,
        candidate_limit=_normalize_limit(limit) or 2,
        search_limit=_normalize_limit(search_limit) or 5,
        queries_per_candidate=max(0, int(queries_per_candidate)),
        persist_review_queue=not dry_run,
    )
    print(json.dumps(summary.as_dict(), indent=2))
    return 0


def _cmd_recommend_coverage(project_root: Path | None, stale_after_days: int) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    payload = get_mal_recommendation_harvest_coverage(config.db_path, stale_after_days=stale_after_days)
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_push_recommendations_webhook(
    project_root: Path | None,
    limit: int,
    include_dormant: bool,
    delivery_mode: str | None,
    dry_run: bool,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    try:
        result = deliver_recommendations_via_openclaw(
            config,
            limit=_normalize_limit(limit),
            include_dormant=include_dormant,
            delivery_mode=delivery_mode,
            dry_run=dry_run,
        )
    except OpenClawDeliveryError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result.as_dict(), indent=2))
    return 0 if result.status in {"dry_run", "delivered", "no_recommendations"} else 1


def _dispatch(parser, args) -> int:
    if args.command == "init":
        return _cmd_init(args.project_root)
    if args.command == "status":
        return _cmd_status(args.project_root)
    if args.command == "install-service":
        return _cmd_install_service(
            args.project_root,
            start_now=not args.no_start,
            install_dashboard=args.install_dashboard,
            enable_dashboard=args.enable_dashboard,
        )
    if args.command == "uninstall-service":
        return _cmd_uninstall_service(stop_now=not args.no_stop)
    if args.command == "start-service":
        return _cmd_start_service()
    if args.command == "stop-service":
        return _cmd_stop_service()
    if args.command == "restart-service":
        return _cmd_restart_service()
    if args.command == "service-status":
        return _cmd_service_status(args.project_root, args.format, strict=args.strict)
    if args.command == "service-run":
        return _cmd_service_run(args.project_root)
    if args.command == "service-run-once":
        return _cmd_service_run_once(args.project_root)
    if args.command == "exact-approved-sync-cycle":
        return _cmd_exact_approved_sync_cycle(args.project_root, args.full_refresh, args.allow_stale_provider_apply)
    if args.command == "bootstrap-audit":
        return _cmd_bootstrap_audit(args.project_root, args.summary)
    if args.command == "runtime-retention-audit":
        return _cmd_runtime_retention_audit(
            args.project_root,
            args.format,
            strict=args.strict,
            max_files_per_family=args.max_files_per_family,
            max_dirs_per_family=args.max_dirs_per_family,
            max_depth=args.max_depth,
            max_scan_errors_per_family=args.max_scan_errors_per_family,
            warn_file_count=args.warn_file_count,
            warn_total_bytes=args.warn_total_bytes,
            warn_oldest_days=args.warn_oldest_days,
        )
    if args.command == "health-check":
        return _cmd_health_check(
            args.project_root,
            args.stale_hours,
            args.strict,
            args.review_issue_type,
            args.review_worklist_limit,
            args.format,
            args.mapping_coverage_threshold,
            args.maintenance_review_limit,
        )
    if args.command == "health-check-cycle":
        from mal_updater.health_cycle import run_health_check_cycle

        allow_reason_codes = {
            item.strip()
            for item in args.auto_run_reason_codes.split(",")
            if item.strip()
        }
        return run_health_check_cycle(
            load_config(args.project_root),
            stale_hours=args.stale_hours,
            strict=args.strict,
            auto_run_recommended=args.auto_run_recommended,
            auto_run_reason_codes=allow_reason_codes,
            review_issue_type=args.review_issue_type,
            review_worklist_limit=args.review_worklist_limit,
            mapping_coverage_threshold=args.mapping_coverage_threshold,
            maintenance_review_limit=args.maintenance_review_limit,
        )
    if args.command == "mal-auth-url":
        return _cmd_mal_auth_url(args.project_root, args.json)
    if args.command == "mal-auth-login":
        return _cmd_mal_auth_login(args.project_root, args.timeout_seconds, verify_whoami=not args.no_verify)
    if args.command == "mal-refresh":
        return _cmd_mal_refresh(args.project_root, verify_whoami=not args.no_verify)
    if args.command == "mal-whoami":
        return _cmd_mal_whoami(args.project_root)
    if args.command == "provider-auth-login":
        return _cmd_provider_auth_login(args.project_root, args.provider, args.profile, args.no_verify)
    if args.command == "provider-fetch-snapshot":
        return _cmd_provider_fetch_snapshot(
            args.project_root,
            args.provider,
            args.profile,
            args.out,
            args.ingest,
            args.full_refresh,
            max_history_pages=args.max_history_pages,
            max_watchlist_pages=args.max_watchlist_pages,
            history_start_page=args.history_start_page,
            watchlist_start=args.watchlist_start,
        )
    if args.command == "crunchyroll-auth-login":
        return _cmd_crunchyroll_auth_login(args.project_root, args.profile, args.no_verify)
    if args.command == "crunchyroll-fetch-snapshot":
        return _cmd_crunchyroll_fetch_snapshot(
            args.project_root,
            args.profile,
            args.out,
            args.ingest,
            args.full_refresh,
            max_history_pages=args.max_history_pages,
            max_watchlist_pages=args.max_watchlist_pages,
            history_start_page=args.history_start_page,
            watchlist_start=args.watchlist_start,
        )
    if args.command == "validate-snapshot":
        return _cmd_validate_snapshot(args.project_root, args.snapshot)
    if args.command == "ingest-snapshot":
        return _cmd_ingest_snapshot(args.project_root, args.snapshot)
    if args.command == "backfill-hidive-series-urls":
        return _cmd_backfill_hidive_series_urls(args.project_root, apply=args.apply, output_format=args.format)
    if args.command == "provider-stale-rows":
        return _cmd_provider_stale_rows(args.project_root, args.provider, args.cutoff, args.limit, args.format, args.older_than_days)
    if args.command == "map-series":
        return _cmd_map_series(args.project_root, args.limit, args.mapping_limit)
    if args.command == "review-mappings":
        return _cmd_review_mappings(args.project_root, args.limit, args.mapping_limit, args.persist_review_queue)
    if args.command == "refresh-mapping-review-queue":
        return _cmd_refresh_mapping_review_queue(
            args.project_root,
            args.provider_series_id,
            args.mapping_limit,
            include_all_open=args.all_open,
            title_cluster=args.title_cluster,
            fix_strategy=args.fix_strategy,
            cluster_strategy=args.cluster_strategy,
            decision=args.decision,
            reason=args.reason,
            reason_family=args.reason_family,
            fix_strategy_family=args.fix_strategy_family,
            cluster_strategy_family=args.cluster_strategy_family,
        )
    if args.command == "list-mappings":
        return _cmd_list_mappings(args.project_root, args.approved_only, args.provider)
    if args.command == "approve-mapping":
        return _cmd_approve_mapping(
            args.project_root,
            args.provider_series_id,
            args.mal_anime_id,
            args.confidence,
            args.notes,
            args.exact,
            provider=args.provider,
        )
    if args.command == "dry-run-sync":
        return _cmd_dry_run_sync(
            args.project_root,
            args.provider,
            args.limit,
            args.mapping_limit,
            args.approved_mappings_only,
            args.exact_approved_only,
            args.persist_review_queue,
        )
    if args.command == "list-review-queue":
        return _cmd_list_review_queue(
            args.project_root,
            args.status,
            args.issue_type,
            args.summary or args.output_format == "summary",
            args.limit,
            args.provider_series_id,
            args.title_cluster,
            args.fix_strategy,
            args.cluster_strategy,
            args.decision,
            args.reason,
            args.reason_family,
            args.fix_strategy_family,
            args.cluster_strategy_family,
        )
    if args.command == "review-queue-next":
        return _cmd_review_queue_next(
            args.project_root,
            args.status,
            args.issue_type,
            args.bucket,
            args.title_cluster,
            args.fix_strategy,
            args.cluster_strategy,
            args.decision,
            args.reason,
            args.reason_family,
            args.fix_strategy_family,
            args.cluster_strategy_family,
        )
    if args.command == "review-queue-worklist":
        return _cmd_review_queue_worklist(
            args.project_root,
            args.status,
            args.issue_type,
            args.limit,
            args.title_cluster,
            args.fix_strategy,
            args.cluster_strategy,
            args.decision,
            args.reason,
            args.reason_family,
            args.fix_strategy_family,
            args.cluster_strategy_family,
        )
    if args.command == "review-queue-apply-worklist":
        return _cmd_review_queue_apply_worklist(
            args.project_root,
            args.status,
            args.issue_type,
            args.limit,
            args.per_bucket_limit,
            args.title_cluster,
            args.fix_strategy,
            args.cluster_strategy,
            args.decision,
            args.reason,
            args.reason_family,
            args.fix_strategy_family,
            args.cluster_strategy_family,
        )
    if args.command == "review-queue-refresh-worklist":
        return _cmd_review_queue_refresh_worklist(
            args.project_root,
            args.status,
            args.issue_type,
            args.limit,
            args.per_bucket_limit,
            args.mapping_limit,
            args.title_cluster,
            args.fix_strategy,
            args.cluster_strategy,
            args.decision,
            args.reason,
            args.reason_family,
            args.fix_strategy_family,
            args.cluster_strategy_family,
            args.output_format,
        )
    if args.command == "resolve-review-queue":
        return _cmd_resolve_review_queue(
            args.project_root,
            args.issue_type,
            args.limit,
            args.title_cluster,
            args.fix_strategy,
            args.cluster_strategy,
            args.decision,
            args.reason,
            args.reason_family,
            args.fix_strategy_family,
            args.cluster_strategy_family,
        )
    if args.command == "reopen-review-queue":
        return _cmd_reopen_review_queue(
            args.project_root,
            args.issue_type,
            args.limit,
            args.title_cluster,
            args.fix_strategy,
            args.cluster_strategy,
            args.decision,
            args.reason,
            args.reason_family,
            args.fix_strategy_family,
            args.cluster_strategy_family,
        )
    if args.command == "apply-sync":
        return _cmd_apply_sync(args.project_root, args.limit, args.mapping_limit, args.exact_approved_only, args.execute)
    if args.command == "recommend":
        return _cmd_recommend(args.project_root, args.limit, args.flat, args.include_dormant, args.persist_snapshot)
    if args.command == "recommend-snapshots":
        return _cmd_recommend_snapshots(args.project_root, args.limit, args.output_format)
    if args.command == "recommend-maintain":
        return _cmd_recommend_maintain(
            args.project_root,
            dry_run=args.dry_run,
            metadata_limit=args.metadata_limit,
            discovery_target_limit=args.discovery_target_limit,
            recommendation_limit=args.recommendation_limit,
            mapping_limit=args.mapping_limit,
            mal_list_max_pages=args.mal_list_max_pages,
            provider_max_history_pages=args.provider_max_history_pages,
            provider_max_watchlist_pages=args.provider_max_watchlist_pages,
            skip_provider_refresh=args.skip_provider_refresh,
            local_only=args.local_only,
        )
    if args.command == "recommend-dashboard":
        return _cmd_recommend_dashboard(args.project_root, args.output, args.limit, args.include_dormant)
    if args.command == "dashboard-serve":
        return _cmd_dashboard_serve(args.project_root, args.host, args.port, args.limit)
    if args.command == "mal-list-refresh":
        return _cmd_mal_list_refresh(args.project_root, args.status, args.page_size, args.max_pages, args.complete, args.format)
    if args.command == "recommend-refresh-metadata":
        return _cmd_recommend_refresh_metadata(
            args.project_root,
            args.limit,
            args.include_discovery_targets,
            args.discovery_target_limit,
            args.force_refresh,
        )
    if args.command == "recommend-refresh-full-userrecs":
        return _cmd_recommend_refresh_full_userrecs(
            args.project_root,
            args.limit,
            args.force_refresh,
            args.stale_after_days,
            args.max_pages,
            args.max_body_mb,
            args.format,
        )
    if args.command == "recommend-enrich-provider-availability":
        return _cmd_recommend_enrich_provider_availability(
            args.project_root,
            args.limit,
            args.provider,
            args.search_limit,
            args.queries_per_candidate,
            args.dry_run,
        )
    if args.command == "recommend-coverage":
        return _cmd_recommend_coverage(args.project_root, args.stale_after_days)
    if args.command == "push-recommendations-webhook":
        return _cmd_push_recommendations_webhook(
            args.project_root,
            args.limit,
            args.include_dormant,
            args.delivery_mode,
            args.dry_run,
        )
    parser.error(f"Unknown command: {args.command}")
    return 2


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    inherited = current_api_request_context()
    token = None
    if inherited.run_id is None:
        token = begin_api_request_context(task=f"cli:{args.command}", run_id=str(uuid.uuid4()))
    try:
        return _dispatch(parser, args)
    except ConfigError as exc:
        print(f"configuration error: {sanitize_text(exc.safe_message, max_length=1_000)}", file=sys.stderr)
        return 2
    finally:
        if token is not None:
            end_api_request_context(token)

if __name__ == "__main__":
    raise SystemExit(main())
