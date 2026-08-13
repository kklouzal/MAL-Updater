from __future__ import annotations

import importlib
import ipaddress
import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


def _load_toml_parser(import_module: Callable[[str], Any] = importlib.import_module) -> Any:
    try:
        return import_module("tomllib")
    except ModuleNotFoundError as exc:  # pragma: no cover - Python < 3.11
        if exc.name not in {None, "tomllib"}:
            raise
        return import_module("tomli")


_toml_parser = _load_toml_parser()


class ConfigError(ValueError):
    """Sanitized, operator-facing configuration error."""

    def __init__(self, safe_message: str) -> None:
        super().__init__(safe_message)
        self.safe_message = safe_message


DEFAULT_COMPLETION_THRESHOLD = 0.95
DEFAULT_CREDITS_SKIP_WINDOW_SECONDS = 120
DEFAULT_CONTRACT_VERSION = "1.0"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 20.0
DEFAULT_MAL_BASE_URL = "https://api.myanimelist.net/v2"
DEFAULT_MAL_PUBLIC_BASE_URL = "https://myanimelist.net"
DEFAULT_MAL_AUTH_URL = "https://myanimelist.net/v1/oauth2/authorize"
DEFAULT_MAL_TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"
DEFAULT_MAL_BIND_HOST = "127.0.0.1"
DEFAULT_MAL_REDIRECT_HOST = "127.0.0.1"
DEFAULT_MAL_REDIRECT_PORT = 8765
DEFAULT_MAL_NON_LOOPBACK_CALLBACK_ACK = False
DEFAULT_MAL_REQUEST_SPACING_SECONDS = 1.0
DEFAULT_MAL_REQUEST_SPACING_JITTER_SECONDS = 0.2
DEFAULT_MAL_SEARCH_CACHE_TTL_DAYS = 120
DEFAULT_MAL_SEARCH_NEGATIVE_CACHE_TTL_DAYS = 3
DEFAULT_MAL_DETAIL_CACHE_TTL_DAYS = 120
DEFAULT_PROVIDER_DETAIL_CACHE_TTL_DAYS = 120
DEFAULT_CRUNCHYROLL_LOCALE = "en-US"
DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS = 22.5
DEFAULT_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS = 7.5
DEFAULT_HIDIVE_REQUEST_SPACING_SECONDS = 5.0
DEFAULT_HIDIVE_REQUEST_SPACING_JITTER_SECONDS = 1.0
DEFAULT_PROVIDER_RETRY_MAX_ATTEMPTS = 2
DEFAULT_PROVIDER_RETRY_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_PROVIDER_RETRY_BACKOFF_JITTER_SECONDS = 0.25
DEFAULT_PROVIDER_RETRY_AFTER_CAP_SECONDS = 60.0
DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_TIMEOUT_SECONDS = 20.0
DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_DELIVERY_MODE = "fresh"
DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_SECTION_LIMITS = {
    "continue_next": 5,
    "fresh_dubbed_episodes": 5,
    "discovery_candidates": 3,
    "resume_backlog": 2,
    "other": 2,
}
DEFAULT_MAL_CLIENT_ID_FILE = "mal_client_id.txt"
DEFAULT_MAL_CLIENT_SECRET_FILE = "mal_client_secret.txt"
DEFAULT_MAL_ACCESS_TOKEN_FILE = "mal_access_token.txt"
DEFAULT_MAL_REFRESH_TOKEN_FILE = "mal_refresh_token.txt"
DEFAULT_DB_FILE = "mal_updater.sqlite3"
DEFAULT_RUNTIME_DIR_NAME = ".MAL-Updater"
DEFAULT_SERVICE_SYNC_EVERY_SECONDS = 60 * 60
DEFAULT_SERVICE_FULL_REFRESH_EVERY_SECONDS = 7 * 24 * 60 * 60
DEFAULT_SERVICE_HEALTH_EVERY_SECONDS = 60 * 60
DEFAULT_SERVICE_MAL_REFRESH_EVERY_SECONDS = 60 * 60
DEFAULT_SERVICE_MAL_LIST_REFRESH_EVERY_SECONDS = 8 * 60 * 60
DEFAULT_SERVICE_RECOMMENDATION_METADATA_REFRESH_EVERY_SECONDS = 12 * 60 * 60
DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_EVERY_SECONDS = 60 * 60
DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS = 120
DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_EVERY_SECONDS = 60 * 60
DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_TARGET_DAYS = 120
DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_JITTER_DAYS = 15
DEFAULT_SERVICE_RECOMMEND_MAINTAIN_EVERY_SECONDS = 60 * 60
DEFAULT_SERVICE_RECOMMENDATIONS_WEBHOOK_PUSH_EVERY_SECONDS = 0
DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_RETENTION_DAYS = 14
DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_MIN_RUNS_PER_KIND = 30
DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_PRUNE_BATCH_SIZE = 10_000
DEFAULT_SERVICE_DB_COMPACTION_EVERY_SECONDS = 7 * 24 * 60 * 60
DEFAULT_SERVICE_DB_COMPACTION_MIN_INTERVAL_SECONDS = 30 * 24 * 60 * 60
DEFAULT_SERVICE_DB_COMPACTION_MIN_FREELIST_BYTES = 128 * 1024 * 1024
DEFAULT_SERVICE_DB_COMPACTION_MIN_FREELIST_RATIO = 0.10
DEFAULT_SERVICE_DB_COMPACTION_FREE_SPACE_MARGIN_BYTES = 64 * 1024 * 1024
DEFAULT_SERVICE_HEALTH_HISTORY_RETENTION_EVERY_SECONDS = 24 * 60 * 60
DEFAULT_SERVICE_HEALTH_HISTORY_RETENTION_DAYS = 90
DEFAULT_SERVICE_HEALTH_HISTORY_MIN_COUNT = 168
DEFAULT_SERVICE_HEALTH_HISTORY_PRUNE_BATCH_SIZE = 100
DEFAULT_SERVICE_LOG_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_SERVICE_LOG_RETAINED_GENERATIONS = 5
DEFAULT_SERVICE_RUNTIME_RETENTION_AUDIT_EVERY_SECONDS = 7 * 24 * 60 * 60
DEFAULT_SERVICE_LOOP_SLEEP_SECONDS = 30
DEFAULT_SERVICE_STARTUP_GRACE_SECONDS = 30
DEFAULT_SERVICE_CRUNCHYROLL_HOURLY_LIMIT = 180
DEFAULT_SERVICE_SOURCE_PROVIDER_HOURLY_LIMIT = DEFAULT_SERVICE_CRUNCHYROLL_HOURLY_LIMIT
DEFAULT_SERVICE_MAL_HOURLY_LIMIT = 120
DEFAULT_SERVICE_SOURCE_PROVIDER_WARN_BACKOFF_FLOOR_SECONDS = 0
DEFAULT_SERVICE_SOURCE_PROVIDER_CRITICAL_BACKOFF_FLOOR_SECONDS = 0
DEFAULT_SERVICE_SOURCE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOOR_SECONDS = 0
DEFAULT_SERVICE_CRUNCHYROLL_PROVIDER_MAX_HISTORY_PAGES = 10
DEFAULT_SERVICE_CRUNCHYROLL_PROVIDER_MAX_WATCHLIST_PAGES = 2
DEFAULT_SERVICE_TASK_TIMEOUT_SECONDS = 15 * 60
DEFAULT_SERVICE_LEASE_STALE_AFTER_SECONDS = 30 * 60
DEFAULT_SERVICE_WARN_RATIO = 0.8
DEFAULT_SERVICE_CRITICAL_RATIO = 0.95
DEFAULT_SERVICE_PROJECTED_REQUEST_HISTORY_WINDOW = 5
MAX_SERVICE_PROJECTED_REQUEST_HISTORY_WINDOW = 20
DEFAULT_SERVICE_PROVIDER_HOURLY_LIMITS = {
    "hidive": 72,
}
DEFAULT_SERVICE_TASK_HOURLY_LIMITS = {
    "mal_list_refresh": 6,
    "sync_apply": 48,
    "recommend_metadata_refresh": 12,
    "recommend_full_harvest": 16,
    "recommend_provider_eligibility_crunchyroll": 72,
    "recommend_provider_eligibility_hidive": 32,
}
DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS = {
    "mal_refresh": 1,
    "mal_list_refresh": 3,
    "sync_apply": 8,
    "recommend_metadata_refresh": 8,
    "recommend_full_harvest": 12,
    "recommend_provider_eligibility_crunchyroll": 28,
    "recommend_provider_eligibility_hidive": 8,
}
DEFAULT_SERVICE_TASK_EXECUTE_LIMITS = {
    "mal_list_refresh_pages": 3,
    "sync_apply": 8,
    "recommend_metadata_refresh": 3,
    "recommend_metadata_discovery_targets": 5,
    "recommend_full_harvest": 2,
    "recommend_full_harvest_pages": 3,
    "recommend_provider_eligibility_candidates": 2,
    "recommend_provider_eligibility_search_results": 5,
    "recommend_provider_eligibility_queries_per_candidate": 1,
    "recommendation_snapshot": 100,
}
DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS_BY_MODE = {
    "sync_fetch_crunchyroll": {
        "hot": 4,
        "incremental": 4,
        "full_refresh": 55,
    },
    "sync_fetch_hidive": {
        "hot": 4,
        "incremental": 4,
        "full_refresh": 71,
    },
}
DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_HISTORY_WINDOWS = {
    "crunchyroll": 7,
    "hidive": 9,
}
DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_HISTORY_WINDOWS = {
    "mal_refresh": 3,
    "mal_list_refresh": 5,
    "sync_apply": 3,
    "recommend_metadata_refresh": 5,
    "recommend_full_harvest": 5,
    "recommend_provider_eligibility_crunchyroll": 7,
    "recommend_provider_eligibility_hidive": 7,
}
DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_PERCENTILES = {
    "mal_list_refresh": 0.9,
    "sync_apply": 0.9,
    "recommend_metadata_refresh": 0.9,
    "recommend_full_harvest": 0.9,
    "recommend_provider_eligibility_crunchyroll": 0.9,
    "recommend_provider_eligibility_hidive": 0.9,
}
DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_PERCENTILES = {
    "crunchyroll": 0.9,
    "hidive": 0.9,
}
DEFAULT_SERVICE_PROVIDER_WARN_BACKOFF_FLOORS = {
    "crunchyroll": 900,
    "hidive": 300,
}
DEFAULT_SERVICE_TASK_WARN_BACKOFF_FLOORS = {
    "mal_list_refresh": 1800,
    "sync_apply": 900,
    "recommend_metadata_refresh": 1800,
    "recommend_full_harvest": 1800,
    "recommend_provider_eligibility_crunchyroll": 3600,
    "recommend_provider_eligibility_hidive": 3600,
}
DEFAULT_SERVICE_PROVIDER_CRITICAL_BACKOFF_FLOORS = {
    "crunchyroll": 1800,
    "hidive": 1200,
}
DEFAULT_SERVICE_TASK_CRITICAL_BACKOFF_FLOORS = {
    "mal_list_refresh": 3600,
    "sync_apply": 1800,
    "recommend_metadata_refresh": 3600,
    "recommend_full_harvest": 3600,
    "recommend_provider_eligibility_crunchyroll": 7200,
    "recommend_provider_eligibility_hidive": 7200,
}
DEFAULT_SERVICE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOORS = {
    "crunchyroll": 7200,
    "hidive": 3600,
}
DEFAULT_SERVICE_TASK_AUTH_FAILURE_BACKOFF_FLOORS = {
    "mal_list_refresh": 6 * 60 * 60,
    "sync_apply": 2400,
    "recommend_metadata_refresh": 6 * 60 * 60,
    "recommend_full_harvest": 6 * 60 * 60,
    "recommend_provider_eligibility_crunchyroll": 12 * 60 * 60,
    "recommend_provider_eligibility_hidive": 12 * 60 * 60,
}
WORKSPACE_MARKER_FILES = ("AGENTS.md", "SOUL.md", "USER.md")


@dataclass(slots=True)
class MalSettings:
    base_url: str = DEFAULT_MAL_BASE_URL
    public_base_url: str = DEFAULT_MAL_PUBLIC_BASE_URL
    auth_url: str = DEFAULT_MAL_AUTH_URL
    token_url: str = DEFAULT_MAL_TOKEN_URL
    bind_host: str = DEFAULT_MAL_BIND_HOST
    non_loopback_callback_ack: bool = DEFAULT_MAL_NON_LOOPBACK_CALLBACK_ACK
    redirect_host: str = DEFAULT_MAL_REDIRECT_HOST
    redirect_port: int = DEFAULT_MAL_REDIRECT_PORT
    request_spacing_seconds: float = DEFAULT_MAL_REQUEST_SPACING_SECONDS
    request_spacing_jitter_seconds: float = DEFAULT_MAL_REQUEST_SPACING_JITTER_SECONDS
    search_cache_ttl_days: int = DEFAULT_MAL_SEARCH_CACHE_TTL_DAYS
    search_negative_cache_ttl_days: int = DEFAULT_MAL_SEARCH_NEGATIVE_CACHE_TTL_DAYS
    detail_cache_ttl_days: int = DEFAULT_MAL_DETAIL_CACHE_TTL_DAYS
    provider_detail_cache_ttl_days: int = DEFAULT_PROVIDER_DETAIL_CACHE_TTL_DAYS
    retry_max_attempts: int = DEFAULT_PROVIDER_RETRY_MAX_ATTEMPTS
    retry_backoff_base_seconds: float = DEFAULT_PROVIDER_RETRY_BACKOFF_BASE_SECONDS
    retry_backoff_jitter_seconds: float = DEFAULT_PROVIDER_RETRY_BACKOFF_JITTER_SECONDS
    retry_after_cap_seconds: float = DEFAULT_PROVIDER_RETRY_AFTER_CAP_SECONDS

    @property
    def redirect_uri(self) -> str:
        return f"http://{self.redirect_host}:{self.redirect_port}/callback"


@dataclass(slots=True)
class CrunchyrollSettings:
    locale: str = DEFAULT_CRUNCHYROLL_LOCALE
    request_spacing_seconds: float = DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS
    request_spacing_jitter_seconds: float = DEFAULT_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS
    retry_max_attempts: int = DEFAULT_PROVIDER_RETRY_MAX_ATTEMPTS
    retry_backoff_base_seconds: float = DEFAULT_PROVIDER_RETRY_BACKOFF_BASE_SECONDS
    retry_backoff_jitter_seconds: float = DEFAULT_PROVIDER_RETRY_BACKOFF_JITTER_SECONDS
    retry_after_cap_seconds: float = DEFAULT_PROVIDER_RETRY_AFTER_CAP_SECONDS


@dataclass(slots=True)
class HidiveSettings:
    request_spacing_seconds: float = DEFAULT_HIDIVE_REQUEST_SPACING_SECONDS
    request_spacing_jitter_seconds: float = DEFAULT_HIDIVE_REQUEST_SPACING_JITTER_SECONDS
    retry_max_attempts: int = DEFAULT_PROVIDER_RETRY_MAX_ATTEMPTS
    retry_backoff_base_seconds: float = DEFAULT_PROVIDER_RETRY_BACKOFF_BASE_SECONDS
    retry_backoff_jitter_seconds: float = DEFAULT_PROVIDER_RETRY_BACKOFF_JITTER_SECONDS
    retry_after_cap_seconds: float = DEFAULT_PROVIDER_RETRY_AFTER_CAP_SECONDS


@dataclass(slots=True)
class OpenClawSettings:
    recommendations_webhook_enabled: bool = False
    recommendations_webhook_url: str = ""
    recommendations_webhook_timeout_seconds: float = DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_TIMEOUT_SECONDS
    recommendations_webhook_channel: str = "discord"
    recommendations_webhook_to: str = ""
    recommendations_webhook_delivery_mode: str = DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_DELIVERY_MODE
    recommendations_webhook_section_limits: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_SECTION_LIMITS))


@dataclass(slots=True)
class ServiceSettings:
    sync_every_seconds: int = DEFAULT_SERVICE_SYNC_EVERY_SECONDS
    full_refresh_every_seconds: int = DEFAULT_SERVICE_FULL_REFRESH_EVERY_SECONDS
    health_every_seconds: int = DEFAULT_SERVICE_HEALTH_EVERY_SECONDS
    mal_refresh_every_seconds: int = DEFAULT_SERVICE_MAL_REFRESH_EVERY_SECONDS
    mal_list_refresh_every_seconds: int = DEFAULT_SERVICE_MAL_LIST_REFRESH_EVERY_SECONDS
    recommendation_metadata_refresh_every_seconds: int = DEFAULT_SERVICE_RECOMMENDATION_METADATA_REFRESH_EVERY_SECONDS
    recommendation_full_harvest_every_seconds: int = DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_EVERY_SECONDS
    recommendation_full_harvest_stale_after_days: int = DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS
    provider_eligibility_refresh_every_seconds: int = DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_EVERY_SECONDS
    provider_eligibility_refresh_target_days: int = DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_TARGET_DAYS
    provider_eligibility_refresh_jitter_days: int = DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_JITTER_DAYS
    recommend_maintain_every_seconds: int = DEFAULT_SERVICE_RECOMMEND_MAINTAIN_EVERY_SECONDS
    recommendations_webhook_push_every_seconds: int = DEFAULT_SERVICE_RECOMMENDATIONS_WEBHOOK_PUSH_EVERY_SECONDS
    recommendation_snapshot_retention_days: int = DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_RETENTION_DAYS
    recommendation_snapshot_min_runs_per_kind: int = DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_MIN_RUNS_PER_KIND
    recommendation_snapshot_prune_batch_size: int = DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_PRUNE_BATCH_SIZE
    db_compaction_every_seconds: int = DEFAULT_SERVICE_DB_COMPACTION_EVERY_SECONDS
    db_compaction_min_interval_seconds: int = DEFAULT_SERVICE_DB_COMPACTION_MIN_INTERVAL_SECONDS
    db_compaction_min_freelist_bytes: int = DEFAULT_SERVICE_DB_COMPACTION_MIN_FREELIST_BYTES
    db_compaction_min_freelist_ratio: float = DEFAULT_SERVICE_DB_COMPACTION_MIN_FREELIST_RATIO
    db_compaction_free_space_margin_bytes: int = DEFAULT_SERVICE_DB_COMPACTION_FREE_SPACE_MARGIN_BYTES
    health_history_retention_every_seconds: int = DEFAULT_SERVICE_HEALTH_HISTORY_RETENTION_EVERY_SECONDS
    health_history_retention_days: int = DEFAULT_SERVICE_HEALTH_HISTORY_RETENTION_DAYS
    health_history_min_count: int = DEFAULT_SERVICE_HEALTH_HISTORY_MIN_COUNT
    health_history_prune_batch_size: int = DEFAULT_SERVICE_HEALTH_HISTORY_PRUNE_BATCH_SIZE
    service_log_max_bytes: int = DEFAULT_SERVICE_LOG_MAX_BYTES
    service_log_retained_generations: int = DEFAULT_SERVICE_LOG_RETAINED_GENERATIONS
    runtime_retention_audit_every_seconds: int = DEFAULT_SERVICE_RUNTIME_RETENTION_AUDIT_EVERY_SECONDS
    loop_sleep_seconds: int = DEFAULT_SERVICE_LOOP_SLEEP_SECONDS
    startup_grace_seconds: int = DEFAULT_SERVICE_STARTUP_GRACE_SECONDS
    task_timeout_seconds: int = DEFAULT_SERVICE_TASK_TIMEOUT_SECONDS
    lease_stale_after_seconds: int = DEFAULT_SERVICE_LEASE_STALE_AFTER_SECONDS
    crunchyroll_hourly_limit: int = DEFAULT_SERVICE_CRUNCHYROLL_HOURLY_LIMIT
    source_provider_hourly_limit: int = DEFAULT_SERVICE_SOURCE_PROVIDER_HOURLY_LIMIT
    mal_hourly_limit: int = DEFAULT_SERVICE_MAL_HOURLY_LIMIT
    provider_hourly_limits: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_PROVIDER_HOURLY_LIMITS))
    task_hourly_limits: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_HOURLY_LIMITS))
    task_projected_request_counts: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS))
    task_execute_limits: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_EXECUTE_LIMITS))
    task_projected_request_counts_by_mode: dict[str, dict[str, int]] = field(
        default_factory=lambda: {task_name: dict(mode_map) for task_name, mode_map in DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS_BY_MODE.items()}
    )
    provider_projected_request_history_windows: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_HISTORY_WINDOWS))
    task_projected_request_history_windows: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_HISTORY_WINDOWS))
    provider_projected_request_percentiles: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_PERCENTILES))
    task_projected_request_percentiles: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_PERCENTILES))
    source_provider_warn_backoff_floor_seconds: int = DEFAULT_SERVICE_SOURCE_PROVIDER_WARN_BACKOFF_FLOOR_SECONDS
    source_provider_critical_backoff_floor_seconds: int = DEFAULT_SERVICE_SOURCE_PROVIDER_CRITICAL_BACKOFF_FLOOR_SECONDS
    provider_warn_backoff_floor_seconds: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_PROVIDER_WARN_BACKOFF_FLOORS))
    provider_critical_backoff_floor_seconds: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_PROVIDER_CRITICAL_BACKOFF_FLOORS))
    task_warn_backoff_floor_seconds: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_WARN_BACKOFF_FLOORS))
    task_critical_backoff_floor_seconds: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_CRITICAL_BACKOFF_FLOORS))
    source_provider_auth_failure_backoff_floor_seconds: int = DEFAULT_SERVICE_SOURCE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOOR_SECONDS
    crunchyroll_provider_max_history_pages: int = DEFAULT_SERVICE_CRUNCHYROLL_PROVIDER_MAX_HISTORY_PAGES
    crunchyroll_provider_max_watchlist_pages: int = DEFAULT_SERVICE_CRUNCHYROLL_PROVIDER_MAX_WATCHLIST_PAGES
    provider_auth_failure_backoff_floor_seconds: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOORS))
    task_auth_failure_backoff_floor_seconds: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_SERVICE_TASK_AUTH_FAILURE_BACKOFF_FLOORS))
    warn_ratio: float = DEFAULT_SERVICE_WARN_RATIO
    critical_ratio: float = DEFAULT_SERVICE_CRITICAL_RATIO

    def budget_scope_for(self, provider: str | None, *, task_name: str | None = None) -> str:
        if task_name and (
            task_name in self.task_hourly_limits
            or task_name in self.task_warn_backoff_floor_seconds
            or task_name in self.task_critical_backoff_floor_seconds
            or task_name in self.task_auth_failure_backoff_floor_seconds
        ):
            return "task"
        if provider:
            return "provider"
        return "none"

    def projected_request_count_for(self, task_name: str, *, fetch_mode: str | None = None) -> tuple[int | None, str | None]:
        if fetch_mode:
            mode_map = self.task_projected_request_counts_by_mode.get(task_name)
            if isinstance(mode_map, dict):
                value = mode_map.get(fetch_mode)
                if value is None and fetch_mode == "hot":
                    value = mode_map.get("incremental")
                if isinstance(value, int):
                    return max(0, int(value)), f"configured_{fetch_mode}"
        value = self.task_projected_request_counts.get(task_name)
        if isinstance(value, int):
            return max(0, int(value)), "configured"
        return None, None

    def execute_limit_for(self, task_name: str) -> int | None:
        value = self.task_execute_limits.get(task_name)
        if isinstance(value, int):
            return max(0, int(value))
        return None

    def projected_request_history_window_for(self, task_name: str | None = None, *, provider: str | None = None) -> int:
        if task_name:
            value = self.task_projected_request_history_windows.get(task_name)
            if isinstance(value, int):
                return max(1, min(MAX_SERVICE_PROJECTED_REQUEST_HISTORY_WINDOW, int(value)))
        if provider:
            value = self.provider_projected_request_history_windows.get(provider)
            if isinstance(value, int):
                return max(1, min(MAX_SERVICE_PROJECTED_REQUEST_HISTORY_WINDOW, int(value)))
        return DEFAULT_SERVICE_PROJECTED_REQUEST_HISTORY_WINDOW

    def projected_request_percentile_for(self, task_name: str | None = None, *, provider: str | None = None) -> float | None:
        if task_name:
            value = self.task_projected_request_percentiles.get(task_name)
            if isinstance(value, (int, float)):
                normalized = float(value)
                if 0.0 < normalized <= 1.0:
                    return normalized
        if provider:
            value = self.provider_projected_request_percentiles.get(provider)
            if isinstance(value, (int, float)):
                normalized = float(value)
                if 0.0 < normalized <= 1.0:
                    return normalized
        return None

    def hourly_limit_for(self, provider: str, *, task_name: str | None = None) -> int:
        if task_name:
            value = self.task_hourly_limits.get(task_name)
            if isinstance(value, int):
                return max(0, int(value))
        if provider == "mal":
            return self.mal_hourly_limit
        value = self.provider_hourly_limits.get(provider)
        if isinstance(value, int):
            return max(0, int(value))
        if provider == "crunchyroll":
            return max(0, int(self.crunchyroll_hourly_limit))
        return max(0, int(self.source_provider_hourly_limit))

    def backoff_floor_seconds_for(self, provider: str, *, level: str, task_name: str | None = None) -> int:
        if task_name:
            task_floors = self.task_warn_backoff_floor_seconds if level == "warn" else self.task_critical_backoff_floor_seconds
            task_value = task_floors.get(task_name)
            if isinstance(task_value, int):
                return max(0, int(task_value))
        floors = self.provider_warn_backoff_floor_seconds if level == "warn" else self.provider_critical_backoff_floor_seconds
        value = floors.get(provider)
        if isinstance(value, int):
            return max(0, int(value))
        if provider != "mal":
            if level == "warn":
                return max(0, int(self.source_provider_warn_backoff_floor_seconds))
            return max(0, int(self.source_provider_critical_backoff_floor_seconds))
        return 0

    def auth_failure_backoff_floor_seconds_for(self, provider: str, *, task_name: str | None = None) -> int:
        if task_name:
            value = self.task_auth_failure_backoff_floor_seconds.get(task_name)
            if isinstance(value, int):
                return max(0, int(value))
        value = self.provider_auth_failure_backoff_floor_seconds.get(provider)
        if isinstance(value, int):
            return max(0, int(value))
        if provider != "mal":
            return max(0, int(self.source_provider_auth_failure_backoff_floor_seconds))
        return 0


@dataclass(slots=True)
class MalSecrets:
    client_id: str | None
    client_secret: str | None
    access_token: str | None
    refresh_token: str | None
    client_id_path: Path
    client_secret_path: Path
    access_token_path: Path
    refresh_token_path: Path


@dataclass(slots=True)
class AppConfig:
    project_root: Path
    workspace_root: Path
    runtime_root: Path
    settings_path: Path
    config_dir: Path
    secrets_dir: Path
    data_dir: Path
    state_dir: Path
    cache_dir: Path
    db_path: Path
    secret_files: dict[str, Any] = field(default_factory=dict)
    completion_threshold: float = DEFAULT_COMPLETION_THRESHOLD
    credits_skip_window_seconds: int = DEFAULT_CREDITS_SKIP_WINDOW_SECONDS
    contract_version: str = DEFAULT_CONTRACT_VERSION
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    mal: MalSettings = field(default_factory=MalSettings)
    crunchyroll: CrunchyrollSettings = field(default_factory=CrunchyrollSettings)
    hidive: HidiveSettings = field(default_factory=HidiveSettings)
    openclaw: OpenClawSettings = field(default_factory=OpenClawSettings)
    service: ServiceSettings = field(default_factory=ServiceSettings)

    @property
    def service_log_path(self) -> Path:
        return self.state_dir / "logs" / "service.log"

    @property
    def service_state_path(self) -> Path:
        return self.state_dir / "service-state.json"

    @property
    def service_leases_dir(self) -> Path:
        return self.state_dir / "leases"

    @property
    def api_request_events_path(self) -> Path:
        return self.state_dir / "api-request-events.jsonl"

    @property
    def health_latest_json_path(self) -> Path:
        return self.state_dir / "health" / "latest-health-check.json"


def _resolve_from(base_dir: Path, raw_value: str | Path) -> Path:
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _discover_workspace_root(project_root: Path) -> Path:
    env_override = os.getenv("MAL_UPDATER_WORKSPACE_DIR") or os.getenv("OPENCLAW_WORKSPACE_DIR")
    if env_override:
        return _resolve_from(Path.cwd(), env_override)

    for candidate in (project_root, *project_root.parents):
        for marker in WORKSPACE_MARKER_FILES:
            if (candidate / marker).exists():
                return candidate.resolve()

    if project_root.parent.name == "skills" and project_root.parent.parent.exists():
        return project_root.parent.parent.resolve()

    return project_root.resolve()


def _first_env_value(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _default_runtime_root(workspace_root: Path) -> Path:
    env_override = _first_env_value("MAL_UPDATER_RUNTIME_ROOT", "MAL_UPDATER_RUNTIME_DIR")
    if env_override:
        return _resolve_from(Path.cwd(), env_override)
    return (workspace_root / DEFAULT_RUNTIME_DIR_NAME).resolve()


def _get_table(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name)
    return value if isinstance(value, dict) else {}


def _get_nested_table(data: dict[str, Any], parent: str, child: str) -> dict[str, Any]:
    nested_parent = _get_table(data, parent)
    nested_child = nested_parent.get(child) if isinstance(nested_parent, dict) else None
    if isinstance(nested_child, dict):
        return nested_child
    fallback = data.get(f"{parent}.{child}")
    return fallback if isinstance(fallback, dict) else {}


def _get_dotted_nested_tables(data: dict[str, Any], *parts: str) -> dict[str, dict[str, Any]]:
    tables: dict[str, dict[str, Any]] = {}

    current: Any = data
    for part in parts:
        if not isinstance(current, dict):
            current = None
            break
        current = current.get(part)
    if isinstance(current, dict):
        for key, value in current.items():
            if isinstance(key, str) and isinstance(value, dict):
                tables[key] = dict(value)

    prefix = ".".join(parts) + "."
    for key, value in data.items():
        if not isinstance(key, str) or not key.startswith(prefix) or not isinstance(value, dict):
            continue
        suffix = key[len(prefix):]
        if not suffix:
            continue
        tables[suffix] = {**tables.get(suffix, {}), **dict(value)}
    return tables


def _get_str(data: dict[str, Any], key: str, default: str) -> str:
    value = data.get(key, default)
    return str(value)


def _setting_label(section: str | None, key: str) -> str:
    return f"{section}.{key}" if section else key


def _coerce_float(value: object, *, label: str) -> float:
    if isinstance(value, bool):
        raise ConfigError(f"Invalid numeric value for {label}") from None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ConfigError(f"Invalid numeric value for {label}") from None
    if not math.isfinite(number):
        raise ConfigError(f"Invalid numeric value for {label}") from None
    return number


def _coerce_int(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise ConfigError(f"Invalid integer value for {label}") from None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        raise ConfigError(f"Invalid integer value for {label}") from None


def _get_float(data: dict[str, Any], key: str, default: float, *, section: str | None = None) -> float:
    return _coerce_float(data.get(key, default), label=_setting_label(section, key))


def _get_int(data: dict[str, Any], key: str, default: int, *, section: str | None = None) -> int:
    return _coerce_int(data.get(key, default), label=_setting_label(section, key))


def _coerce_bool(value: object, *, label: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"Invalid boolean value for {label}") from None


def _get_bool(data: dict[str, Any], key: str, default: bool, *, section: str | None = None) -> bool:
    return _coerce_bool(data.get(key, default), label=_setting_label(section, key))


def _is_loopback_callback_host(host: str) -> bool:
    normalized = str(host).strip().lower()
    if normalized.startswith("[") and normalized.endswith("]"):
        normalized = normalized[1:-1]
    if normalized in {"localhost", "ip6-localhost", "ip6-loopback"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def mal_callback_bind_warning(mal: MalSettings) -> str | None:
    if _is_loopback_callback_host(mal.bind_host):
        return None
    return (
        "MAL OAuth callback listener is configured on a non-loopback bind_host; "
        "only use this on a trusted network and only when another host must reach the callback."
    )


def _validate_mal_callback_config(mal: MalSettings) -> None:
    if not str(mal.bind_host).strip():
        raise ConfigError("Invalid string value for mal.bind_host") from None
    if not 1 <= int(mal.redirect_port) <= 65535:
        raise ConfigError("Invalid integer value for mal.redirect_port: expected 1..65535") from None
    if mal_callback_bind_warning(mal) and not mal.non_loopback_callback_ack:
        raise ConfigError(
            "Refusing non-loopback MAL OAuth callback bind_host without explicit acknowledgement; "
            "set mal.non_loopback_callback_ack = true or MAL_UPDATER_MAL_NON_LOOPBACK_CALLBACK_ACK=true "
            "only when the callback listener must be reachable from another host."
        ) from None


def _validate_finite_float(label: str, value: float) -> None:
    if not math.isfinite(value):
        raise ConfigError(f"Invalid numeric value for {label}") from None


def _validate_finite_numeric_config(config: AppConfig) -> None:
    for label, value in (
        ("completion_threshold", config.completion_threshold),
        ("request_timeout_seconds", config.request_timeout_seconds),
        ("mal.request_spacing_seconds", config.mal.request_spacing_seconds),
        ("mal.request_spacing_jitter_seconds", config.mal.request_spacing_jitter_seconds),
        ("mal.retry_backoff_base_seconds", config.mal.retry_backoff_base_seconds),
        ("mal.retry_backoff_jitter_seconds", config.mal.retry_backoff_jitter_seconds),
        ("mal.retry_after_cap_seconds", config.mal.retry_after_cap_seconds),
        ("crunchyroll.request_spacing_seconds", config.crunchyroll.request_spacing_seconds),
        ("crunchyroll.request_spacing_jitter_seconds", config.crunchyroll.request_spacing_jitter_seconds),
        ("crunchyroll.retry_backoff_base_seconds", config.crunchyroll.retry_backoff_base_seconds),
        ("crunchyroll.retry_backoff_jitter_seconds", config.crunchyroll.retry_backoff_jitter_seconds),
        ("crunchyroll.retry_after_cap_seconds", config.crunchyroll.retry_after_cap_seconds),
        ("hidive.request_spacing_seconds", config.hidive.request_spacing_seconds),
        ("hidive.request_spacing_jitter_seconds", config.hidive.request_spacing_jitter_seconds),
        ("hidive.retry_backoff_base_seconds", config.hidive.retry_backoff_base_seconds),
        ("hidive.retry_backoff_jitter_seconds", config.hidive.retry_backoff_jitter_seconds),
        ("hidive.retry_after_cap_seconds", config.hidive.retry_after_cap_seconds),
        ("openclaw.recommendations_webhook_timeout_seconds", config.openclaw.recommendations_webhook_timeout_seconds),
        ("service.warn_ratio", config.service.warn_ratio),
        ("service.critical_ratio", config.service.critical_ratio),
    ):
        _validate_finite_float(label, value)


def ensure_secret_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def _resolve_path_setting(
    env_name: str,
    settings: dict[str, Any],
    key: str,
    *,
    base_dir: Path,
    default: Path,
) -> Path:
    env_value = os.getenv(env_name)
    if env_value:
        return _resolve_from(Path.cwd(), env_value)
    raw_value = settings.get(key)
    if raw_value is None:
        return _resolve_from(base_dir, default)
    return _resolve_from(base_dir, str(raw_value))


def _resolve_secret_path(
    env_name: str,
    settings: dict[str, Any],
    key: str,
    *,
    secrets_dir: Path,
    default_file: str,
) -> Path:
    env_value = os.getenv(env_name)
    if env_value:
        return _resolve_from(Path.cwd(), env_value)
    raw_value = settings.get(key)
    if raw_value is None:
        return (secrets_dir / default_file).resolve()
    return _resolve_from(secrets_dir, str(raw_value))


def _read_toml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as fh:
            data = _toml_parser.load(fh)
    except ValueError as exc:
        line = getattr(exc, "lineno", None)
        column = getattr(exc, "colno", None)
        if line is None or column is None:
            match = re.search(r"\(at line (\d+), column (\d+)\)", str(exc))
            if match:
                line = int(match.group(1))
                column = int(match.group(2))
        location = f" at line {line}, column {column}" if line is not None and column is not None else ""
        raise ConfigError(f"Invalid TOML in {path.name}: {type(exc).__name__}{location}") from None
    if not isinstance(data, dict):
        raise ConfigError(f"Invalid TOML in {path.name}: expected top-level table") from None
    return data


def _read_secret_file(path: Path) -> str | None:
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def _load_config_unchecked(project_root: Path | None = None) -> AppConfig:
    root = (project_root or Path(__file__).resolve().parents[2]).resolve()
    workspace_root = _discover_workspace_root(root)
    runtime_root = _default_runtime_root(workspace_root)
    default_config_dir = (runtime_root / "config").resolve()
    settings_path = _resolve_from(
        Path.cwd(),
        _first_env_value("MAL_UPDATER_SETTINGS_PATH", "MAL_UPDATER_CONFIG") or str(default_config_dir / "settings.toml"),
    )
    settings = _read_toml_file(settings_path)

    paths_section = _get_table(settings, "paths")
    mal_section = _get_table(settings, "mal")
    crunchyroll_section = _get_table(settings, "crunchyroll")
    hidive_section = _get_table(settings, "hidive")
    openclaw_section = _get_table(settings, "openclaw")
    openclaw_section_limits_section = _get_nested_table(settings, "openclaw", "recommendations_webhook_section_limits")
    service_section = _get_table(settings, "service")
    service_provider_limits_section = _get_nested_table(settings, "service", "provider_hourly_limits")
    service_task_limits_section = _get_nested_table(settings, "service", "task_hourly_limits")
    service_task_projected_request_counts_section = _get_nested_table(settings, "service", "task_projected_request_counts")
    service_task_execute_limits_section = _get_nested_table(settings, "service", "task_execute_limits")
    service_task_projected_request_counts_by_mode_section = _get_dotted_nested_tables(settings, "service", "task_projected_request_counts_by_mode")
    service_provider_projected_request_history_windows_section = _get_nested_table(settings, "service", "provider_projected_request_history_windows")
    service_task_projected_request_history_windows_section = _get_nested_table(settings, "service", "task_projected_request_history_windows")
    service_provider_projected_request_percentiles_section = _get_nested_table(settings, "service", "provider_projected_request_percentiles")
    service_task_projected_request_percentiles_section = _get_nested_table(settings, "service", "task_projected_request_percentiles")
    service_warn_backoff_floors_section = _get_nested_table(settings, "service", "provider_warn_backoff_floor_seconds")
    service_critical_backoff_floors_section = _get_nested_table(settings, "service", "provider_critical_backoff_floor_seconds")
    service_task_warn_backoff_floors_section = _get_nested_table(settings, "service", "task_warn_backoff_floor_seconds")
    service_task_critical_backoff_floors_section = _get_nested_table(settings, "service", "task_critical_backoff_floor_seconds")
    service_auth_failure_backoff_floors_section = _get_nested_table(settings, "service", "provider_auth_failure_backoff_floor_seconds")
    service_task_auth_failure_backoff_floors_section = _get_nested_table(settings, "service", "task_auth_failure_backoff_floor_seconds")
    secret_files_section = _get_table(settings, "secret_files")
    settings_dir = settings_path.parent

    config_dir = _resolve_path_setting(
        "MAL_UPDATER_CONFIG_DIR",
        paths_section,
        "config_dir",
        base_dir=settings_dir,
        default=runtime_root / "config",
    )
    secrets_dir = _resolve_path_setting(
        "MAL_UPDATER_SECRETS_DIR",
        paths_section,
        "secrets_dir",
        base_dir=settings_dir,
        default=runtime_root / "secrets",
    )
    data_dir = _resolve_path_setting(
        "MAL_UPDATER_DATA_DIR",
        paths_section,
        "data_dir",
        base_dir=settings_dir,
        default=runtime_root / "data",
    )
    state_dir = _resolve_path_setting(
        "MAL_UPDATER_STATE_DIR",
        paths_section,
        "state_dir",
        base_dir=settings_dir,
        default=runtime_root / "state",
    )
    cache_dir = _resolve_path_setting(
        "MAL_UPDATER_CACHE_DIR",
        paths_section,
        "cache_dir",
        base_dir=settings_dir,
        default=runtime_root / "cache",
    )
    db_path = _resolve_path_setting(
        "MAL_UPDATER_DB_PATH",
        paths_section,
        "db_path",
        base_dir=settings_dir,
        default=data_dir / DEFAULT_DB_FILE,
    )
    app_config = AppConfig(
        project_root=root,
        workspace_root=workspace_root,
        runtime_root=runtime_root,
        settings_path=settings_path,
        config_dir=config_dir,
        secrets_dir=secrets_dir,
        data_dir=data_dir,
        state_dir=state_dir,
        cache_dir=cache_dir,
        db_path=db_path,
        secret_files=secret_files_section,
        completion_threshold=_coerce_float(
            os.getenv("MAL_UPDATER_COMPLETION_THRESHOLD", _get_float(settings, "completion_threshold", DEFAULT_COMPLETION_THRESHOLD)),
            label="environment variable MAL_UPDATER_COMPLETION_THRESHOLD",
        ),
        credits_skip_window_seconds=_coerce_int(
            os.getenv(
                "MAL_UPDATER_CREDITS_SKIP_WINDOW_SECONDS",
                _get_int(settings, "credits_skip_window_seconds", DEFAULT_CREDITS_SKIP_WINDOW_SECONDS),
            ),
            label="environment variable MAL_UPDATER_CREDITS_SKIP_WINDOW_SECONDS",
        ),
        contract_version=os.getenv("MAL_UPDATER_CONTRACT_VERSION", _get_str(settings, "contract_version", DEFAULT_CONTRACT_VERSION)),
        request_timeout_seconds=_coerce_float(
            os.getenv("MAL_UPDATER_REQUEST_TIMEOUT_SECONDS", _get_float(settings, "request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS)),
            label="environment variable MAL_UPDATER_REQUEST_TIMEOUT_SECONDS",
        ),
        mal=MalSettings(
            base_url=os.getenv("MAL_UPDATER_MAL_BASE_URL", _get_str(mal_section, "base_url", DEFAULT_MAL_BASE_URL)),
            public_base_url=os.getenv("MAL_UPDATER_MAL_PUBLIC_BASE_URL", _get_str(mal_section, "public_base_url", DEFAULT_MAL_PUBLIC_BASE_URL)),
            auth_url=os.getenv("MAL_UPDATER_MAL_AUTH_URL", _get_str(mal_section, "auth_url", DEFAULT_MAL_AUTH_URL)),
            token_url=os.getenv("MAL_UPDATER_MAL_TOKEN_URL", _get_str(mal_section, "token_url", DEFAULT_MAL_TOKEN_URL)),
            bind_host=os.getenv("MAL_UPDATER_MAL_BIND_HOST", _get_str(mal_section, "bind_host", DEFAULT_MAL_BIND_HOST)),
            non_loopback_callback_ack=_coerce_bool(
                os.getenv(
                    "MAL_UPDATER_MAL_NON_LOOPBACK_CALLBACK_ACK",
                    str(_get_bool(mal_section, "non_loopback_callback_ack", DEFAULT_MAL_NON_LOOPBACK_CALLBACK_ACK, section="mal")),
                ),
                label="environment variable MAL_UPDATER_MAL_NON_LOOPBACK_CALLBACK_ACK",
            ),
            redirect_host=os.getenv("MAL_UPDATER_MAL_REDIRECT_HOST", _get_str(mal_section, "redirect_host", DEFAULT_MAL_REDIRECT_HOST)),
            redirect_port=_coerce_int(
                os.getenv("MAL_UPDATER_MAL_REDIRECT_PORT", _get_int(mal_section, "redirect_port", DEFAULT_MAL_REDIRECT_PORT, section="mal")),
                label="environment variable MAL_UPDATER_MAL_REDIRECT_PORT",
            ),
            request_spacing_seconds=_coerce_float(
                os.getenv(
                    "MAL_UPDATER_MAL_REQUEST_SPACING_SECONDS",
                    _get_float(mal_section, "request_spacing_seconds", DEFAULT_MAL_REQUEST_SPACING_SECONDS),
                ),
                label="environment variable MAL_UPDATER_MAL_REQUEST_SPACING_SECONDS",
            ),
            request_spacing_jitter_seconds=_coerce_float(
                os.getenv(
                    "MAL_UPDATER_MAL_REQUEST_SPACING_JITTER_SECONDS",
                    _get_float(mal_section, "request_spacing_jitter_seconds", DEFAULT_MAL_REQUEST_SPACING_JITTER_SECONDS),
                ),
                label="environment variable MAL_UPDATER_MAL_REQUEST_SPACING_JITTER_SECONDS",
            ),
            search_cache_ttl_days=max(0, int(os.getenv("MAL_UPDATER_MAL_SEARCH_CACHE_TTL_DAYS", _get_int(mal_section, "search_cache_ttl_days", DEFAULT_MAL_SEARCH_CACHE_TTL_DAYS)))),
            search_negative_cache_ttl_days=max(0, int(os.getenv("MAL_UPDATER_MAL_SEARCH_NEGATIVE_CACHE_TTL_DAYS", _get_int(mal_section, "search_negative_cache_ttl_days", DEFAULT_MAL_SEARCH_NEGATIVE_CACHE_TTL_DAYS)))),
            detail_cache_ttl_days=max(0, int(os.getenv("MAL_UPDATER_MAL_DETAIL_CACHE_TTL_DAYS", _get_int(mal_section, "detail_cache_ttl_days", DEFAULT_MAL_DETAIL_CACHE_TTL_DAYS)))),
            provider_detail_cache_ttl_days=max(0, int(os.getenv("MAL_UPDATER_PROVIDER_DETAIL_CACHE_TTL_DAYS", _get_int(mal_section, "provider_detail_cache_ttl_days", DEFAULT_PROVIDER_DETAIL_CACHE_TTL_DAYS)))),
            retry_max_attempts=max(1, int(os.getenv("MAL_UPDATER_MAL_RETRY_MAX_ATTEMPTS", _get_int(mal_section, "retry_max_attempts", DEFAULT_PROVIDER_RETRY_MAX_ATTEMPTS)))),
            retry_backoff_base_seconds=max(0.0, float(os.getenv("MAL_UPDATER_MAL_RETRY_BACKOFF_BASE_SECONDS", _get_float(mal_section, "retry_backoff_base_seconds", DEFAULT_PROVIDER_RETRY_BACKOFF_BASE_SECONDS)))),
            retry_backoff_jitter_seconds=max(0.0, float(os.getenv("MAL_UPDATER_MAL_RETRY_BACKOFF_JITTER_SECONDS", _get_float(mal_section, "retry_backoff_jitter_seconds", DEFAULT_PROVIDER_RETRY_BACKOFF_JITTER_SECONDS)))),
            retry_after_cap_seconds=max(0.0, float(os.getenv("MAL_UPDATER_MAL_RETRY_AFTER_CAP_SECONDS", _get_float(mal_section, "retry_after_cap_seconds", DEFAULT_PROVIDER_RETRY_AFTER_CAP_SECONDS)))),
        ),
        crunchyroll=CrunchyrollSettings(
            locale=os.getenv("MAL_UPDATER_CRUNCHYROLL_LOCALE", _get_str(crunchyroll_section, "locale", DEFAULT_CRUNCHYROLL_LOCALE)),
            request_spacing_seconds=float(
                os.getenv(
                    "MAL_UPDATER_CRUNCHYROLL_REQUEST_SPACING_SECONDS",
                    _get_float(crunchyroll_section, "request_spacing_seconds", DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS),
                )
            ),
            request_spacing_jitter_seconds=float(
                os.getenv(
                    "MAL_UPDATER_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS",
                    _get_float(
                        crunchyroll_section,
                        "request_spacing_jitter_seconds",
                        DEFAULT_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS,
                    ),
                )
            ),
            retry_max_attempts=max(1, int(os.getenv("MAL_UPDATER_CRUNCHYROLL_RETRY_MAX_ATTEMPTS", _get_int(crunchyroll_section, "retry_max_attempts", DEFAULT_PROVIDER_RETRY_MAX_ATTEMPTS)))),
            retry_backoff_base_seconds=max(0.0, float(os.getenv("MAL_UPDATER_CRUNCHYROLL_RETRY_BACKOFF_BASE_SECONDS", _get_float(crunchyroll_section, "retry_backoff_base_seconds", DEFAULT_PROVIDER_RETRY_BACKOFF_BASE_SECONDS)))),
            retry_backoff_jitter_seconds=max(0.0, float(os.getenv("MAL_UPDATER_CRUNCHYROLL_RETRY_BACKOFF_JITTER_SECONDS", _get_float(crunchyroll_section, "retry_backoff_jitter_seconds", DEFAULT_PROVIDER_RETRY_BACKOFF_JITTER_SECONDS)))),
            retry_after_cap_seconds=max(0.0, float(os.getenv("MAL_UPDATER_CRUNCHYROLL_RETRY_AFTER_CAP_SECONDS", _get_float(crunchyroll_section, "retry_after_cap_seconds", DEFAULT_PROVIDER_RETRY_AFTER_CAP_SECONDS)))),
        ),
        hidive=HidiveSettings(
            request_spacing_seconds=max(0.0, float(os.getenv("MAL_UPDATER_HIDIVE_REQUEST_SPACING_SECONDS", _get_float(hidive_section, "request_spacing_seconds", DEFAULT_HIDIVE_REQUEST_SPACING_SECONDS)))),
            request_spacing_jitter_seconds=max(0.0, float(os.getenv("MAL_UPDATER_HIDIVE_REQUEST_SPACING_JITTER_SECONDS", _get_float(hidive_section, "request_spacing_jitter_seconds", DEFAULT_HIDIVE_REQUEST_SPACING_JITTER_SECONDS)))),
            retry_max_attempts=max(1, int(os.getenv("MAL_UPDATER_HIDIVE_RETRY_MAX_ATTEMPTS", _get_int(hidive_section, "retry_max_attempts", DEFAULT_PROVIDER_RETRY_MAX_ATTEMPTS)))),
            retry_backoff_base_seconds=max(0.0, float(os.getenv("MAL_UPDATER_HIDIVE_RETRY_BACKOFF_BASE_SECONDS", _get_float(hidive_section, "retry_backoff_base_seconds", DEFAULT_PROVIDER_RETRY_BACKOFF_BASE_SECONDS)))),
            retry_backoff_jitter_seconds=max(0.0, float(os.getenv("MAL_UPDATER_HIDIVE_RETRY_BACKOFF_JITTER_SECONDS", _get_float(hidive_section, "retry_backoff_jitter_seconds", DEFAULT_PROVIDER_RETRY_BACKOFF_JITTER_SECONDS)))),
            retry_after_cap_seconds=max(0.0, float(os.getenv("MAL_UPDATER_HIDIVE_RETRY_AFTER_CAP_SECONDS", _get_float(hidive_section, "retry_after_cap_seconds", DEFAULT_PROVIDER_RETRY_AFTER_CAP_SECONDS)))),
        ),
        openclaw=OpenClawSettings(
            recommendations_webhook_enabled=(
                str(os.getenv("MAL_UPDATER_OPENCLAW_RECOMMENDATIONS_WEBHOOK_ENABLED", openclaw_section.get("recommendations_webhook_enabled", False))).strip().lower()
                in {"1", "true", "yes", "on"}
            ),
            recommendations_webhook_url=os.getenv(
                "MAL_UPDATER_OPENCLAW_RECOMMENDATIONS_WEBHOOK_URL",
                _get_str(openclaw_section, "recommendations_webhook_url", ""),
            ),
            recommendations_webhook_timeout_seconds=float(
                os.getenv(
                    "MAL_UPDATER_OPENCLAW_RECOMMENDATIONS_WEBHOOK_TIMEOUT_SECONDS",
                    _get_float(
                        openclaw_section,
                        "recommendations_webhook_timeout_seconds",
                        DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_TIMEOUT_SECONDS,
                    ),
                )
            ),
            recommendations_webhook_channel=os.getenv(
                "MAL_UPDATER_OPENCLAW_RECOMMENDATIONS_WEBHOOK_CHANNEL",
                _get_str(openclaw_section, "recommendations_webhook_channel", "discord"),
            ),
            recommendations_webhook_to=os.getenv(
                "MAL_UPDATER_OPENCLAW_RECOMMENDATIONS_WEBHOOK_TO",
                _get_str(openclaw_section, "recommendations_webhook_to", ""),
            ),
            recommendations_webhook_delivery_mode=str(
                os.getenv(
                    "MAL_UPDATER_OPENCLAW_RECOMMENDATIONS_WEBHOOK_DELIVERY_MODE",
                    _get_str(
                        openclaw_section,
                        "recommendations_webhook_delivery_mode",
                        DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_DELIVERY_MODE,
                    ),
                )
            ).strip().lower()
            or DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_DELIVERY_MODE,
            recommendations_webhook_section_limits={
                **DEFAULT_OPENCLAW_RECOMMENDATIONS_WEBHOOK_SECTION_LIMITS,
                **{
                    str(key): max(0, int(value))
                    for key, value in openclaw_section_limits_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
        ),
        service=ServiceSettings(
            sync_every_seconds=int(os.getenv("MAL_UPDATER_SERVICE_SYNC_EVERY_SECONDS", _get_int(service_section, "sync_every_seconds", DEFAULT_SERVICE_SYNC_EVERY_SECONDS))),
            full_refresh_every_seconds=int(os.getenv("MAL_UPDATER_SERVICE_FULL_REFRESH_EVERY_SECONDS", _get_int(service_section, "full_refresh_every_seconds", DEFAULT_SERVICE_FULL_REFRESH_EVERY_SECONDS))),
            health_every_seconds=int(os.getenv("MAL_UPDATER_SERVICE_HEALTH_EVERY_SECONDS", _get_int(service_section, "health_every_seconds", DEFAULT_SERVICE_HEALTH_EVERY_SECONDS))),
            mal_refresh_every_seconds=int(os.getenv("MAL_UPDATER_SERVICE_MAL_REFRESH_EVERY_SECONDS", _get_int(service_section, "mal_refresh_every_seconds", DEFAULT_SERVICE_MAL_REFRESH_EVERY_SECONDS))),
            mal_list_refresh_every_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_MAL_LIST_REFRESH_EVERY_SECONDS",
                    _get_int(service_section, "mal_list_refresh_every_seconds", DEFAULT_SERVICE_MAL_LIST_REFRESH_EVERY_SECONDS),
                )
            ),
            recommendation_metadata_refresh_every_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_RECOMMENDATION_METADATA_REFRESH_EVERY_SECONDS",
                    _get_int(
                        service_section,
                        "recommendation_metadata_refresh_every_seconds",
                        DEFAULT_SERVICE_RECOMMENDATION_METADATA_REFRESH_EVERY_SECONDS,
                    ),
                )
            ),
            recommendation_full_harvest_every_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_RECOMMENDATION_FULL_HARVEST_EVERY_SECONDS",
                    _get_int(
                        service_section,
                        "recommendation_full_harvest_every_seconds",
                        DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_EVERY_SECONDS,
                    ),
                )
            ),
            recommendation_full_harvest_stale_after_days=max(
                1,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS",
                        _get_int(
                            service_section,
                            "recommendation_full_harvest_stale_after_days",
                            DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS,
                        ),
                    )
                ),
            ),
            provider_eligibility_refresh_every_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_EVERY_SECONDS",
                    _get_int(
                        service_section,
                        "provider_eligibility_refresh_every_seconds",
                        DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_EVERY_SECONDS,
                    ),
                )
            ),
            provider_eligibility_refresh_target_days=max(
                0,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_TARGET_DAYS",
                        _get_int(
                            service_section,
                            "provider_eligibility_refresh_target_days",
                            DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_TARGET_DAYS,
                        ),
                    )
                ),
            ),
            provider_eligibility_refresh_jitter_days=max(
                0,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_JITTER_DAYS",
                        _get_int(
                            service_section,
                            "provider_eligibility_refresh_jitter_days",
                            DEFAULT_SERVICE_PROVIDER_ELIGIBILITY_REFRESH_JITTER_DAYS,
                        ),
                    )
                ),
            ),
            recommend_maintain_every_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_RECOMMEND_MAINTAIN_EVERY_SECONDS",
                    _get_int(
                        service_section,
                        "recommend_maintain_every_seconds",
                        DEFAULT_SERVICE_RECOMMEND_MAINTAIN_EVERY_SECONDS,
                    ),
                )
            ),
            recommendations_webhook_push_every_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_RECOMMENDATIONS_WEBHOOK_PUSH_EVERY_SECONDS",
                    _get_int(
                        service_section,
                        "recommendations_webhook_push_every_seconds",
                        DEFAULT_SERVICE_RECOMMENDATIONS_WEBHOOK_PUSH_EVERY_SECONDS,
                    ),
                )
            ),
            recommendation_snapshot_retention_days=max(
                1,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_RECOMMENDATION_SNAPSHOT_RETENTION_DAYS",
                        _get_int(
                            service_section,
                            "recommendation_snapshot_retention_days",
                            DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_RETENTION_DAYS,
                        ),
                    )
                ),
            ),
            recommendation_snapshot_min_runs_per_kind=max(
                1,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_RECOMMENDATION_SNAPSHOT_MIN_RUNS_PER_KIND",
                        _get_int(
                            service_section,
                            "recommendation_snapshot_min_runs_per_kind",
                            DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_MIN_RUNS_PER_KIND,
                        ),
                    )
                ),
            ),
            recommendation_snapshot_prune_batch_size=max(
                1,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_RECOMMENDATION_SNAPSHOT_PRUNE_BATCH_SIZE",
                        _get_int(
                            service_section,
                            "recommendation_snapshot_prune_batch_size",
                            DEFAULT_SERVICE_RECOMMENDATION_SNAPSHOT_PRUNE_BATCH_SIZE,
                        ),
                    )
                ),
            ),
            db_compaction_every_seconds=max(
                0,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_DB_COMPACTION_EVERY_SECONDS",
                        _get_int(service_section, "db_compaction_every_seconds", DEFAULT_SERVICE_DB_COMPACTION_EVERY_SECONDS),
                    )
                ),
            ),
            db_compaction_min_interval_seconds=max(
                0,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_DB_COMPACTION_MIN_INTERVAL_SECONDS",
                        _get_int(service_section, "db_compaction_min_interval_seconds", DEFAULT_SERVICE_DB_COMPACTION_MIN_INTERVAL_SECONDS),
                    )
                ),
            ),
            db_compaction_min_freelist_bytes=max(
                0,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_DB_COMPACTION_MIN_FREELIST_BYTES",
                        _get_int(service_section, "db_compaction_min_freelist_bytes", DEFAULT_SERVICE_DB_COMPACTION_MIN_FREELIST_BYTES),
                    )
                ),
            ),
            db_compaction_min_freelist_ratio=max(
                0.0,
                float(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_DB_COMPACTION_MIN_FREELIST_RATIO",
                        _get_float(service_section, "db_compaction_min_freelist_ratio", DEFAULT_SERVICE_DB_COMPACTION_MIN_FREELIST_RATIO),
                    )
                ),
            ),
            db_compaction_free_space_margin_bytes=max(
                0,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_DB_COMPACTION_FREE_SPACE_MARGIN_BYTES",
                        _get_int(service_section, "db_compaction_free_space_margin_bytes", DEFAULT_SERVICE_DB_COMPACTION_FREE_SPACE_MARGIN_BYTES),
                    )
                ),
            ),
            health_history_retention_every_seconds=max(
                0,
                int(os.getenv("MAL_UPDATER_SERVICE_HEALTH_HISTORY_RETENTION_EVERY_SECONDS", _get_int(service_section, "health_history_retention_every_seconds", DEFAULT_SERVICE_HEALTH_HISTORY_RETENTION_EVERY_SECONDS))),
            ),
            health_history_retention_days=max(
                1,
                int(os.getenv("MAL_UPDATER_SERVICE_HEALTH_HISTORY_RETENTION_DAYS", _get_int(service_section, "health_history_retention_days", DEFAULT_SERVICE_HEALTH_HISTORY_RETENTION_DAYS))),
            ),
            health_history_min_count=max(
                1,
                int(os.getenv("MAL_UPDATER_SERVICE_HEALTH_HISTORY_MIN_COUNT", _get_int(service_section, "health_history_min_count", DEFAULT_SERVICE_HEALTH_HISTORY_MIN_COUNT))),
            ),
            health_history_prune_batch_size=max(
                1,
                int(os.getenv("MAL_UPDATER_SERVICE_HEALTH_HISTORY_PRUNE_BATCH_SIZE", _get_int(service_section, "health_history_prune_batch_size", DEFAULT_SERVICE_HEALTH_HISTORY_PRUNE_BATCH_SIZE))),
            ),
            service_log_max_bytes=max(
                1,
                int(os.getenv("MAL_UPDATER_SERVICE_LOG_MAX_BYTES", _get_int(service_section, "service_log_max_bytes", DEFAULT_SERVICE_LOG_MAX_BYTES))),
            ),
            service_log_retained_generations=max(
                1,
                int(os.getenv("MAL_UPDATER_SERVICE_LOG_RETAINED_GENERATIONS", _get_int(service_section, "service_log_retained_generations", DEFAULT_SERVICE_LOG_RETAINED_GENERATIONS))),
            ),
            runtime_retention_audit_every_seconds=max(
                0,
                int(os.getenv("MAL_UPDATER_SERVICE_RUNTIME_RETENTION_AUDIT_EVERY_SECONDS", _get_int(service_section, "runtime_retention_audit_every_seconds", DEFAULT_SERVICE_RUNTIME_RETENTION_AUDIT_EVERY_SECONDS))),
            ),
            loop_sleep_seconds=int(os.getenv("MAL_UPDATER_SERVICE_LOOP_SLEEP_SECONDS", _get_int(service_section, "loop_sleep_seconds", DEFAULT_SERVICE_LOOP_SLEEP_SECONDS))),
            startup_grace_seconds=max(
                0,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_STARTUP_GRACE_SECONDS",
                        _get_int(service_section, "startup_grace_seconds", DEFAULT_SERVICE_STARTUP_GRACE_SECONDS),
                    )
                ),
            ),
            task_timeout_seconds=max(1, int(os.getenv("MAL_UPDATER_SERVICE_TASK_TIMEOUT_SECONDS", _get_int(service_section, "task_timeout_seconds", DEFAULT_SERVICE_TASK_TIMEOUT_SECONDS)))),
            lease_stale_after_seconds=max(
                1,
                int(
                    os.getenv(
                        "MAL_UPDATER_SERVICE_LEASE_STALE_AFTER_SECONDS",
                        _get_int(service_section, "lease_stale_after_seconds", DEFAULT_SERVICE_LEASE_STALE_AFTER_SECONDS),
                    )
                ),
            ),
            crunchyroll_hourly_limit=int(os.getenv("MAL_UPDATER_SERVICE_CRUNCHYROLL_HOURLY_LIMIT", _get_int(service_section, "crunchyroll_hourly_limit", DEFAULT_SERVICE_CRUNCHYROLL_HOURLY_LIMIT))),
            source_provider_hourly_limit=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_SOURCE_PROVIDER_HOURLY_LIMIT",
                    _get_int(service_section, "source_provider_hourly_limit", DEFAULT_SERVICE_SOURCE_PROVIDER_HOURLY_LIMIT),
                )
            ),
            mal_hourly_limit=int(os.getenv("MAL_UPDATER_SERVICE_MAL_HOURLY_LIMIT", _get_int(service_section, "mal_hourly_limit", DEFAULT_SERVICE_MAL_HOURLY_LIMIT))),
            provider_hourly_limits={
                **DEFAULT_SERVICE_PROVIDER_HOURLY_LIMITS,
                **{
                    str(key): int(value)
                    for key, value in service_provider_limits_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            task_hourly_limits={
                **DEFAULT_SERVICE_TASK_HOURLY_LIMITS,
                **{
                    str(key): int(value)
                    for key, value in service_task_limits_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            task_projected_request_counts={
                **DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS,
                **{
                    str(key): int(value)
                    for key, value in service_task_projected_request_counts_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            task_execute_limits={
                **DEFAULT_SERVICE_TASK_EXECUTE_LIMITS,
                **{
                    str(key): int(value)
                    for key, value in service_task_execute_limits_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            crunchyroll_provider_max_history_pages=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_CRUNCHYROLL_PROVIDER_MAX_HISTORY_PAGES",
                    _get_int(
                        service_section,
                        "crunchyroll_provider_max_history_pages",
                        DEFAULT_SERVICE_CRUNCHYROLL_PROVIDER_MAX_HISTORY_PAGES,
                    ),
                )
            ),
            crunchyroll_provider_max_watchlist_pages=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_CRUNCHYROLL_PROVIDER_MAX_WATCHLIST_PAGES",
                    _get_int(
                        service_section,
                        "crunchyroll_provider_max_watchlist_pages",
                        DEFAULT_SERVICE_CRUNCHYROLL_PROVIDER_MAX_WATCHLIST_PAGES,
                    ),
                )
            ),
            task_projected_request_counts_by_mode={
                **{task_name: dict(mode_map) for task_name, mode_map in DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS_BY_MODE.items()},
                **{
                    str(task_name): {
                        str(mode): int(value)
                        for mode, value in mode_map.items()
                        if isinstance(mode, str) and isinstance(value, (int, float))
                    }
                    for task_name, mode_map in service_task_projected_request_counts_by_mode_section.items()
                    if isinstance(task_name, str) and isinstance(mode_map, dict)
                },
            },
            provider_projected_request_history_windows={
                **DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_HISTORY_WINDOWS,
                **{
                    str(key): max(1, min(MAX_SERVICE_PROJECTED_REQUEST_HISTORY_WINDOW, int(value)))
                    for key, value in service_provider_projected_request_history_windows_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            task_projected_request_history_windows={
                **DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_HISTORY_WINDOWS,
                **{
                    str(key): max(1, min(MAX_SERVICE_PROJECTED_REQUEST_HISTORY_WINDOW, int(value)))
                    for key, value in service_task_projected_request_history_windows_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            provider_projected_request_percentiles={
                **DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_PERCENTILES,
                **{
                    str(key): float(value)
                    for key, value in service_provider_projected_request_percentiles_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float)) and 0.0 < float(value) <= 1.0
                },
            },
            task_projected_request_percentiles={
                **DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_PERCENTILES,
                **{
                    str(key): float(value)
                    for key, value in service_task_projected_request_percentiles_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float)) and 0.0 < float(value) <= 1.0
                },
            },
            source_provider_warn_backoff_floor_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_SOURCE_PROVIDER_WARN_BACKOFF_FLOOR_SECONDS",
                    _get_int(
                        service_section,
                        "source_provider_warn_backoff_floor_seconds",
                        DEFAULT_SERVICE_SOURCE_PROVIDER_WARN_BACKOFF_FLOOR_SECONDS,
                    ),
                )
            ),
            source_provider_critical_backoff_floor_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_SOURCE_PROVIDER_CRITICAL_BACKOFF_FLOOR_SECONDS",
                    _get_int(
                        service_section,
                        "source_provider_critical_backoff_floor_seconds",
                        DEFAULT_SERVICE_SOURCE_PROVIDER_CRITICAL_BACKOFF_FLOOR_SECONDS,
                    ),
                )
            ),
            provider_warn_backoff_floor_seconds={
                **DEFAULT_SERVICE_PROVIDER_WARN_BACKOFF_FLOORS,
                **{
                    str(key): int(value)
                    for key, value in service_warn_backoff_floors_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            provider_critical_backoff_floor_seconds={
                **DEFAULT_SERVICE_PROVIDER_CRITICAL_BACKOFF_FLOORS,
                **{
                    str(key): int(value)
                    for key, value in service_critical_backoff_floors_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            task_warn_backoff_floor_seconds={
                **DEFAULT_SERVICE_TASK_WARN_BACKOFF_FLOORS,
                **{
                    str(key): int(value)
                    for key, value in service_task_warn_backoff_floors_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            task_critical_backoff_floor_seconds={
                **DEFAULT_SERVICE_TASK_CRITICAL_BACKOFF_FLOORS,
                **{
                    str(key): int(value)
                    for key, value in service_task_critical_backoff_floors_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            source_provider_auth_failure_backoff_floor_seconds=int(
                os.getenv(
                    "MAL_UPDATER_SERVICE_SOURCE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOOR_SECONDS",
                    _get_int(
                        service_section,
                        "source_provider_auth_failure_backoff_floor_seconds",
                        DEFAULT_SERVICE_SOURCE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOOR_SECONDS,
                    ),
                )
            ),
            provider_auth_failure_backoff_floor_seconds={
                **DEFAULT_SERVICE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOORS,
                **{
                    str(key): int(value)
                    for key, value in service_auth_failure_backoff_floors_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            task_auth_failure_backoff_floor_seconds={
                **DEFAULT_SERVICE_TASK_AUTH_FAILURE_BACKOFF_FLOORS,
                **{
                    str(key): int(value)
                    for key, value in service_task_auth_failure_backoff_floors_section.items()
                    if isinstance(key, str) and isinstance(value, (int, float))
                },
            },
            warn_ratio=float(os.getenv("MAL_UPDATER_SERVICE_WARN_RATIO", _get_float(service_section, "warn_ratio", DEFAULT_SERVICE_WARN_RATIO))),
            critical_ratio=float(os.getenv("MAL_UPDATER_SERVICE_CRITICAL_RATIO", _get_float(service_section, "critical_ratio", DEFAULT_SERVICE_CRITICAL_RATIO))),
        ),
    )
    _validate_mal_callback_config(app_config.mal)
    _validate_finite_numeric_config(app_config)
    if (
        app_config.service.provider_eligibility_refresh_target_days > 0
        and app_config.service.provider_eligibility_refresh_jitter_days
        > app_config.service.provider_eligibility_refresh_target_days
    ):
        raise ConfigError(
            "service.provider_eligibility_refresh_jitter_days must not exceed "
            "service.provider_eligibility_refresh_target_days"
        ) from None
    return app_config


def load_config(project_root: Path | None = None) -> AppConfig:
    try:
        return _load_config_unchecked(project_root)
    except ConfigError:
        raise
    except (TypeError, ValueError, OverflowError) as exc:
        raise ConfigError(
            f"Invalid configuration value ({type(exc).__name__}); check MAL_UPDATER_* environment variables and settings.toml."
        ) from None


def load_mal_secrets(config: AppConfig) -> MalSecrets:
    secret_files_section = config.secret_files
    client_id_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_CLIENT_ID_FILE",
        secret_files_section,
        "mal_client_id",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_CLIENT_ID_FILE,
    )
    client_secret_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_CLIENT_SECRET_FILE",
        secret_files_section,
        "mal_client_secret",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_CLIENT_SECRET_FILE,
    )
    access_token_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_ACCESS_TOKEN_FILE",
        secret_files_section,
        "mal_access_token",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_ACCESS_TOKEN_FILE,
    )
    refresh_token_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_REFRESH_TOKEN_FILE",
        secret_files_section,
        "mal_refresh_token",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_REFRESH_TOKEN_FILE,
    )

    return MalSecrets(
        client_id=os.getenv("MAL_UPDATER_MAL_CLIENT_ID") or _read_secret_file(client_id_path),
        client_secret=os.getenv("MAL_UPDATER_MAL_CLIENT_SECRET") or _read_secret_file(client_secret_path),
        access_token=os.getenv("MAL_UPDATER_MAL_ACCESS_TOKEN") or _read_secret_file(access_token_path),
        refresh_token=os.getenv("MAL_UPDATER_MAL_REFRESH_TOKEN") or _read_secret_file(refresh_token_path),
        client_id_path=client_id_path,
        client_secret_path=client_secret_path,
        access_token_path=access_token_path,
        refresh_token_path=refresh_token_path,
    )


def load_openclaw_recommendations_hook_token(config: AppConfig) -> tuple[str | None, Path]:
    secret_files_section = config.secret_files if isinstance(config.secret_files, dict) else {}
    token_path = _resolve_secret_path(
        "MAL_UPDATER_OPENCLAW_HOOK_TOKEN_FILE",
        secret_files_section,
        "openclaw_hook_token",
        secrets_dir=config.secrets_dir,
        default_file="openclaw_hook_token.txt",
    )
    token = os.getenv("MAL_UPDATER_OPENCLAW_HOOK_TOKEN") or _read_secret_file(token_path)
    return token, token_path


def ensure_directories(config: AppConfig) -> None:
    for path in (
        config.runtime_root,
        config.config_dir,
        config.data_dir,
        config.state_dir,
        config.cache_dir,
        config.service_log_path.parent,
        config.service_leases_dir,
        config.health_latest_json_path.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)
    ensure_secret_directory(config.secrets_dir)
