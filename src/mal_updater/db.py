from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
import sqlite3
import uuid
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

from .hidive_urls import canonical_hidive_series_url

BROADCAST_COMPATIBILITY_MIGRATION = "013_mal_anime_metadata_broadcast_compatibility.sql"
PROVIDER_ENRICHMENT_CURSOR_MIGRATION = "014_recommendation_provider_enrichment_cursor.sql"
PUBLIC_USERRECS_STAGING_MIGRATION = "015_public_userrecs_resumable_staging.sql"
PROVIDER_WATCHLIST_MEMBERSHIP_MIGRATION = "016_provider_watchlist_membership_keys.sql"
PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION = "017_provider_series_observation_provenance.sql"
PROVIDER_TITLE_SEARCH_CACHE_FULL_KEY_MIGRATION = "018_provider_title_search_cache_full_key.sql"
MAL_USER_ANIME_LIST_REFRESH_GENERATIONS_MIGRATION = "019_mal_user_anime_list_refresh_generations.sql"
PROVIDER_ELIGIBILITY_REFRESH_LIFECYCLE_MIGRATION = "020_provider_eligibility_refresh_lifecycle.sql"
PUBLIC_USERRECS_DURABLE_QUEUE_MIGRATION = "021_public_userrecs_durable_queue_and_snapshot_guards.sql"
MAL_USER_LIST_DURABLE_PAGINATION_MIGRATION = "022_mal_user_list_durable_pagination.sql"
PUBLIC_USERRECS_INCREMENTAL_VALIDATION_MIGRATION = "023_public_userrecs_incremental_validation.sql"
PROVIDER_WATCHLIST_CURRENT_MEMBERSHIP_MIGRATION = "025_provider_watchlist_current_membership.sql"
PROVIDER_EPISODE_PROGRESS_PROVENANCE_MIGRATION = "026_provider_episode_progress_provenance.sql"
EVALUATION_EVENTS_MIGRATION = "027_evaluation_events.sql"
PROVIDER_PROGRESS_OBSERVATIONS_MIGRATION = "028_provider_progress_observations.sql"
PROVIDER_FETCH_PROVENANCE_MIGRATION = "029_provider_fetch_provenance.sql"
RECOMMENDATION_DECISION_LEDGER_MIGRATION = "030_recommendation_decision_ledger.sql"

MIGRATION_FILENAMES: tuple[str, ...] = (
    "001_initial.sql",
    "002_mal_metadata_cache.sql",
    "003_mal_recommendation_edges.sql",
    "004_provider_search_cache.sql",
    "004_mal_recommendation_harvest_status.sql",
    "005_recommendation_score_snapshots.sql",
    "006_recommendation_eligibility_evidence.sql",
    "007_mal_user_anime_list_cache.sql",
    "008_niceness_caches.sql",
    "009_recommendation_full_harvest_provenance.sql",
    "010_mal_anime_metadata_official_detail_fields.sql",
    "011_mal_user_anime_list_preference_fields.sql",
    "012_watch_confirmation_provenance.sql",
    BROADCAST_COMPATIBILITY_MIGRATION,
    PROVIDER_ENRICHMENT_CURSOR_MIGRATION,
    PUBLIC_USERRECS_STAGING_MIGRATION,
    PROVIDER_WATCHLIST_MEMBERSHIP_MIGRATION,
    PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION,
    PROVIDER_TITLE_SEARCH_CACHE_FULL_KEY_MIGRATION,
    MAL_USER_ANIME_LIST_REFRESH_GENERATIONS_MIGRATION,
    PROVIDER_ELIGIBILITY_REFRESH_LIFECYCLE_MIGRATION,
    PUBLIC_USERRECS_DURABLE_QUEUE_MIGRATION,
    MAL_USER_LIST_DURABLE_PAGINATION_MIGRATION,
    PUBLIC_USERRECS_INCREMENTAL_VALIDATION_MIGRATION,
    "024_public_userrecs_final_anchor_validation.sql",
    PROVIDER_WATCHLIST_CURRENT_MEMBERSHIP_MIGRATION,
    PROVIDER_EPISODE_PROGRESS_PROVENANCE_MIGRATION,
    EVALUATION_EVENTS_MIGRATION,
    PROVIDER_PROGRESS_OBSERVATIONS_MIGRATION,
    PROVIDER_FETCH_PROVENANCE_MIGRATION,
    RECOMMENDATION_DECISION_LEDGER_MIGRATION,
)

_MIGRATIONS_PACKAGE = "mal_updater.migrations"

# Historical ordering is intentionally explicit. The two 004 migrations already
# exist in deployed databases under these exact filenames, so their duplicate
# numeric prefix is allowed but must not be generalized to future migrations.
ALLOWED_DUPLICATE_MIGRATION_PREFIXES: dict[str, tuple[str, ...]] = {
    "004": (
        "004_provider_search_cache.sql",
        "004_mal_recommendation_harvest_status.sql",
    ),
}

# Backwards-compatible iterable for callers that inspect migration resources:
# Traversable entries expose `.name` and `.read_text()`, just like the Path
# objects previously used here, but remain safe after wheel installation.
def iter_migrations(filenames: tuple[str, ...] = MIGRATION_FILENAMES):
    """Return migration resources in the exact order recorded in SQLite."""
    return tuple(files(_MIGRATIONS_PACKAGE).joinpath(filename) for filename in filenames)


MIGRATIONS = iter_migrations()


def validate_migration_catalog(
    filenames: tuple[str, ...] = MIGRATION_FILENAMES,
    *,
    packaged_filenames: tuple[str, ...] | None = None,
    allowed_duplicate_prefixes: dict[str, tuple[str, ...]] | None = None,
) -> None:
    """Guard the explicit migration order and historical schema version names."""
    if allowed_duplicate_prefixes is None:
        allowed_duplicate_prefixes = ALLOWED_DUPLICATE_MIGRATION_PREFIXES

    if len(filenames) != len(set(filenames)):
        raise RuntimeError("migration catalog contains duplicate filenames")

    if packaged_filenames is None:
        packaged = tuple(
            sorted(
                child.name
                for child in files(_MIGRATIONS_PACKAGE).iterdir()
                if child.name.endswith(".sql")
            )
        )
    else:
        packaged = tuple(sorted(packaged_filenames))
    cataloged = tuple(sorted(filenames))
    if packaged != cataloged:
        raise RuntimeError(
            "packaged migration files do not match MIGRATION_FILENAMES: "
            f"packaged={packaged!r}, cataloged={cataloged!r}"
        )

    allowed_duplicates = {
        filename: prefix
        for prefix, allowed_filenames in allowed_duplicate_prefixes.items()
        for filename in allowed_filenames
    }
    seen_prefixes: dict[str, str] = {}
    last_prefix_number = -1
    for filename in filenames:
        prefix = filename.split("_", 1)[0]
        try:
            prefix_number = int(prefix)
        except ValueError as exc:
            raise RuntimeError(
                "migration filename does not start with a numeric prefix: "
                f"{filename!r}"
            ) from exc
        if prefix_number < last_prefix_number:
            raise RuntimeError(
                "migration catalog order drifted: "
                f"{filename!r} appears after a later prefix"
            )
        last_prefix_number = prefix_number
        previous = seen_prefixes.get(prefix)
        if previous is None:
            seen_prefixes[prefix] = filename
            continue
        if (
            allowed_duplicates.get(previous) == prefix
            and allowed_duplicates.get(filename) == prefix
        ):
            continue
        raise RuntimeError(
            "migration catalog contains duplicate numeric prefix "
            f"{prefix!r}: {previous!r}, {filename!r}"
        )

    for prefix, allowed_filenames in allowed_duplicate_prefixes.items():
        actual = tuple(
            filename for filename in filenames if filename.split("_", 1)[0] == prefix
        )
        if actual != allowed_filenames:
            raise RuntimeError(
                "historical duplicate migration order changed for prefix "
                f"{prefix!r}: expected {allowed_filenames!r}, got {actual!r}"
            )


validate_migration_catalog()


@dataclass(slots=True)
class PersistedSeriesMapping:
    provider: str
    provider_series_id: str
    mal_anime_id: int
    confidence: float | None
    mapping_source: str
    approved_by_user: bool
    notes: str | None
    created_at: str
    updated_at: str


@dataclass(slots=True)
class ReviewQueueEntry:
    id: int
    provider: str
    provider_series_id: str | None
    provider_episode_id: str | None
    issue_type: str
    severity: str
    payload: dict[str, Any]
    status: str
    created_at: str
    resolved_at: str | None


@dataclass(slots=True)
class MalAnimeMetadata:
    mal_anime_id: int
    title: str
    title_english: str | None
    title_japanese: str | None
    alternative_titles: list[str]
    media_type: str | None
    status: str | None
    num_episodes: int | None
    mean: float | None
    popularity: int | None
    start_season: dict[str, Any] | None
    raw: dict[str, Any]
    fetched_at: str
    updated_at: str
    rank: int | None = None
    num_list_users: int | None = None
    num_scoring_users: int | None = None
    rating: str | None = None
    average_episode_duration: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    broadcast_day: str | None = None
    broadcast_time: str | None = None
    broadcast_timezone: str | None = None
    nsfw: str | None = None


@dataclass(slots=True)
class MalUserAnimeListCacheEntry:
    mal_anime_id: int
    title: str
    list_status: str | None
    user_score: int | None
    num_episodes_watched: int | None
    start_date: str | None
    finish_date: str | None
    list_updated_at: str | None
    priority: int | None
    is_rewatching: bool | None
    num_times_rewatched: int | None
    rewatch_value: int | None
    tag_count: int
    has_comments: bool
    node: dict[str, Any]
    list_status_raw: dict[str, Any]
    raw: dict[str, Any]
    refresh_run_id: str
    refresh_generation: int
    fetched_at: str
    last_seen_at: str
    created_at: str
    updated_at: str


@dataclass(slots=True)
class MalUserAnimeListRefreshSummary:
    status: str
    refresh_run_id: str
    generation: int
    pages: int = 0
    items: int = 0
    upserted: int = 0
    pruned: int = 0
    preserved_absent: int = 0
    scored: int = 0
    unscored: int = 0
    preference_counts: dict[str, int] = field(default_factory=dict)
    metadata_rows_with_my_list_status: int = 0
    by_status: dict[str, int] | None = None
    partial: bool = False
    error: str | None = None
    traversal: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "refresh_run_id": self.refresh_run_id,
            "generation": self.generation,
            "pages": self.pages,
            "items": self.items,
            "upserted": self.upserted,
            "pruned": self.pruned,
            "preserved_absent": self.preserved_absent,
            "scored": self.scored,
            "unscored": self.unscored,
            "preference_counts": dict(self.preference_counts or {}),
            "metadata_rows_with_my_list_status": self.metadata_rows_with_my_list_status,
            "by_status": dict(self.by_status or {}),
            "partial": self.partial,
            "error": self.error,
            "traversal": dict(self.traversal or {}),
        }



@dataclass(slots=True)
class ProviderTitleSearchCacheEntry:
    provider: str
    normalized_query: str
    query: str
    candidate_mal_anime_id: int | None
    candidate_title: str | None
    matches: list[dict[str, Any]]
    status: str
    fetched_at: str
    expires_at: str
    logic_version: str = "legacy-v1"
    search_limit: int = 10
    identity_key: str = ""


@dataclass(slots=True)
class JsonResponseCacheEntry:
    status: str
    response: dict[str, Any]
    fetched_at: str
    expires_at: str
    failure_count: int = 0
    next_retry_at: str | None = None


@dataclass(slots=True)
class MalAnimeRelation:
    mal_anime_id: int
    related_mal_anime_id: int
    relation_type: str
    relation_type_formatted: str | None
    related_title: str | None
    raw: dict[str, Any]
    fetched_at: str


@dataclass(slots=True)
class MalRecommendationEdge:
    source_mal_anime_id: int
    target_mal_anime_id: int
    target_title: str | None
    num_recommendations: int | None
    hop_distance: int
    source_kind: str
    raw: dict[str, Any]
    fetched_at: str


@dataclass(slots=True)
class MalPublicUserRecsCrawlGeneration:
    generation_id: int
    source_mal_anime_id: int
    source_title: str | None
    source_url: str | None
    status: str
    cursor_url: str | None
    pages_fetched: int
    staged_edge_count: int
    last_page_url: str | None
    last_page_fingerprint: str | None
    last_error: str | None
    started_at: str
    completed_at: str | None
    published_at: str | None
    discarded_at: str | None
    created_at: str
    updated_at: str
    logic_version: str = "public-userrecs-snapshot-v2"
    generation_key: str | None = None
    attempt_count: int = 0
    restart_count: int = 0
    drift_count: int = 0
    next_retry_at: str | None = None
    retry_class: str | None = None
    first_page_revalidated_at: str | None = None
    boundary_revalidated_at: str | None = None
    terminal_evidence_json: str = "{}"
    quarantined_at: str | None = None
    quarantine_reason: str | None = None
    claim_token: str | None = None
    claim_expires_at: str | None = None
    generation_revision: int = 0
    staged_revision: int = 0
    validated_staged_revision: int | None = None
    validation_fingerprint: str | None = None
    validation_page_number: int = 0
    validation_revision: int | None = None
    final_anchor_step: int = 0
    final_anchor_revision: int | None = None

    @property
    def terminal_evidence(self) -> dict[str, Any]:
        value = _load_json_value(self.terminal_evidence_json, {})
        return value if isinstance(value, dict) else {}


@dataclass(slots=True)
class MalPublicUserRecsSourceQueueRow:
    source_mal_anime_id: int
    queue_class: str
    eligible: bool
    enqueued_at: str
    class_entered_at: str
    last_selected_at: str | None
    selection_count: int
    selection_sequence: int
    next_retry_at: str | None
    claim_token: str | None
    claim_expires_at: str | None
    last_generation_id: int | None
    last_outcome: str | None
    last_error_code: str | None
    created_at: str
    updated_at: str


@dataclass(slots=True)
class MalPublicUserRecsStagedPage:
    generation_id: int
    source_mal_anime_id: int
    page_number: int
    page_url: str
    page_fingerprint: str
    anchor_json: str
    next_url: str | None
    edge_count: int
    fetched_at: str
    created_at: str
    updated_at: str

    @property
    def anchor(self) -> dict[str, Any]:
        decoded = _load_json_value(self.anchor_json, {})
        return decoded if isinstance(decoded, dict) else {}


@dataclass(frozen=True, slots=True)
class MalPublicUserRecsPublicationResult:
    generation_id: int
    source_mal_anime_id: int
    published_edge_count: int
    pages_fetched: int


@dataclass(slots=True)
class RecommendationSnapshotRow:
    id: int
    run_id: str
    generated_at: str
    kind: str
    provider: str | None
    title: str
    provider_series_id: str | None
    mal_anime_id: int | None
    score: float | None
    priority: int | None
    reasons: list[Any]
    scorecard: dict[str, Any] | None
    context: dict[str, Any] | None
    availability_providers: list[str]
    dub_signal: str | None
    availability_confidence: float | None
    availability_confidence_label: str | None


@dataclass(slots=True)
class RecommendationProviderEligibilityEvidence:
    mal_anime_id: int
    provider: str
    provider_series_id: str
    provider_title: str | None
    provider_url: str | None
    identity_match_kind: str
    match_confidence: float | None
    review_status: str
    catalog_status: str
    english_dub_status: str
    explicit_dub_evidence_source: str | None
    audio_locales: list[Any]
    source_evidence: dict[str, Any]
    fetched_at: str
    expires_at: str
    last_verified_at: str | None
    refresh_status: str
    failure_count: int
    next_retry_at: str | None
    logic_version: str
    verification_outcome: str
    refresh_due_at: str | None
    refresh_schedule_version: str
    refresh_schedule_key: str | None
    last_successful_positive_at: str | None
    invalidated_at: str | None
    invalidation_reason: str | None
    created_at: str
    updated_at: str


@dataclass(slots=True)
class RecommendationProviderEnrichmentCursor:
    provider: str
    cursor_mal_anime_id: int | None
    cursor_rank_key_json: str | None
    cursor_generation: int
    wrapped_at: str | None
    last_attempted_mal_anime_id: int | None
    last_attempted_rank_key_json: str | None
    last_attempted_at: str | None
    last_selection_class: str | None
    last_outcome: str | None
    created_at: str
    updated_at: str

    @property
    def cursor_rank_key(self) -> dict[str, Any] | None:
        return _decode_json_object_or_none(self.cursor_rank_key_json)

    @property
    def last_attempted_rank_key(self) -> dict[str, Any] | None:
        return _decode_json_object_or_none(self.last_attempted_rank_key_json)


@dataclass(slots=True)
class RecommendationProviderEnrichmentAttempt:
    provider: str
    mal_anime_id: int
    rank_key_json: str
    selection_class: str
    attempted_at: str
    attempt_count: int
    last_outcome: str | None
    created_at: str
    updated_at: str

    @property
    def rank_key(self) -> dict[str, Any]:
        return _decode_json_object_or_none(self.rank_key_json) or {}


@dataclass(slots=True)
class RecommendationProviderEnrichmentProgress:
    provider: str
    cursor: RecommendationProviderEnrichmentCursor | None
    attempts_by_mal_anime_id: dict[int, RecommendationProviderEnrichmentAttempt]


@dataclass(slots=True)
class WatchConfirmationProvenance:
    provider: str
    provider_series_id: str
    identity_key: str
    mal_anime_id: int | None
    source_title: str
    season_title: str | None
    mapped_mal_title: str | None
    progress_rows: int
    completed_episode_count: int
    max_episode_number: int | None
    max_completed_episode_number: int | None
    provider_watched_episodes: int
    mal_num_episodes: int | None
    confirmed_complete: bool
    completion_decision: str
    completion_status: str
    completion_threshold: float | None
    credits_skip_window_seconds: int | None
    last_watched_at: str | None
    last_progress_seen_at: str | None
    last_series_seen_at: str | None
    last_evidence_at: str | None
    mapping_source: str | None
    mapping_confidence: float | None
    mapping_approved: bool
    verified_identity_kind: str | None
    verified_identity: dict[str, Any] | None
    completed_by: dict[str, Any]
    completed_examples: dict[str, Any]
    incomplete_examples: list[Any]
    thresholds: dict[str, Any]
    progress_audit: dict[str, Any]
    mapping_audit: dict[str, Any]
    decision_audit: dict[str, Any]
    generated_at: str
    created_at: str
    updated_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "provider_series_id": self.provider_series_id,
            "identity_key": self.identity_key,
            "mal_anime_id": self.mal_anime_id,
            "source_title": self.source_title,
            "season_title": self.season_title,
            "mapped_mal_title": self.mapped_mal_title,
            "progress_rows": self.progress_rows,
            "completed_episode_count": self.completed_episode_count,
            "max_episode_number": self.max_episode_number,
            "max_completed_episode_number": self.max_completed_episode_number,
            "provider_watched_episodes": self.provider_watched_episodes,
            "mal_num_episodes": self.mal_num_episodes,
            "confirmed_complete": self.confirmed_complete,
            "completion_decision": self.completion_decision,
            "completion_status": self.completion_status,
            "completion_threshold": self.completion_threshold,
            "credits_skip_window_seconds": self.credits_skip_window_seconds,
            "last_watched_at": self.last_watched_at,
            "last_progress_seen_at": self.last_progress_seen_at,
            "last_series_seen_at": self.last_series_seen_at,
            "last_evidence_at": self.last_evidence_at,
            "mapping_source": self.mapping_source,
            "mapping_confidence": self.mapping_confidence,
            "mapping_approved": self.mapping_approved,
            "verified_identity_kind": self.verified_identity_kind,
            "verified_identity": self.verified_identity,
            "completed_by": self.completed_by,
            "completed_examples": self.completed_examples,
            "incomplete_examples": self.incomplete_examples,
            "thresholds": self.thresholds,
            "progress_audit": self.progress_audit,
            "mapping_audit": self.mapping_audit,
            "decision_audit": self.decision_audit,
            "generated_at": self.generated_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class ManagedConnection(sqlite3.Connection):
    """SQLite connection that preserves transaction context semantics and closes on exit."""

    _mal_lock_handle: Any = None

    def close(self) -> None:
        lock_handle = getattr(self, "_mal_lock_handle", None)
        try:
            super().close()
        finally:
            if lock_handle is not None:
                from .database_maintenance import release_database_lock

                self._mal_lock_handle = None
                release_database_lock(lock_handle)

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc_value, traceback))
        finally:
            self.close()


def connect(db_path: Path) -> sqlite3.Connection:
    from .database_maintenance import acquire_database_lock

    lock_handle = acquire_database_lock(db_path, exclusive=False, blocking=True)
    try:
        conn = sqlite3.connect(db_path, factory=ManagedConnection)
    except BaseException:
        from .database_maintenance import release_database_lock

        release_database_lock(lock_handle)
        raise
    conn._mal_lock_handle = lock_handle
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _migration_statements(sql: str) -> list[str]:
    """Split a simple migration script without executescript transaction side effects."""
    statements: list[str] = []
    pending = ""
    for line in sql.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip()
            if statement:
                statements.append(statement)
            pending = ""
    if pending.strip():
        raise RuntimeError("migration contains an incomplete SQL statement")
    return statements


def _execute_migration_statement(conn: sqlite3.Connection, statement: str) -> None:
    """Narrow injection seam used to prove rollback/retry behavior in tests."""
    conn.execute(statement)


def _repair_mal_anime_metadata_broadcast_columns(conn: sqlite3.Connection) -> None:
    """Repair historical dirty 010 schemas that used legacy broadcast aliases."""
    table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'mal_anime_metadata'
        """
    ).fetchone()
    if table_exists is None:
        return

    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(mal_anime_metadata)")}
    if "broadcast_day" not in columns:
        conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_day TEXT")
        columns.add("broadcast_day")
    if "broadcast_time" not in columns:
        conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_time TEXT")
        columns.add("broadcast_time")

    if "broadcast_day_of_the_week" in columns:
        conn.execute(
            """
            UPDATE mal_anime_metadata
            SET broadcast_day = LOWER(TRIM(CAST(broadcast_day_of_the_week AS TEXT)))
            WHERE (broadcast_day IS NULL OR TRIM(CAST(broadcast_day AS TEXT)) = '')
              AND broadcast_day_of_the_week IS NOT NULL
              AND TRIM(CAST(broadcast_day_of_the_week AS TEXT)) <> ''
            """
        )
    if "broadcast_start_time" in columns:
        conn.execute(
            """
            UPDATE mal_anime_metadata
            SET broadcast_time = TRIM(CAST(broadcast_start_time AS TEXT))
            WHERE (broadcast_time IS NULL OR TRIM(CAST(broadcast_time AS TEXT)) = '')
              AND broadcast_start_time IS NOT NULL
              AND TRIM(CAST(broadcast_start_time AS TEXT)) <> ''
            """
        )


def _repair_recorded_broadcast_compatibility_migration(conn: sqlite3.Connection) -> None:
    if conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (BROADCAST_COMPATIBILITY_MIGRATION,),
    ).fetchone() is None:
        return
    try:
        conn.execute("BEGIN IMMEDIATE")
        _repair_mal_anime_metadata_broadcast_columns(conn)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _backfill_provider_eligibility_refresh_lifecycle(conn: sqlite3.Connection) -> None:
    """Populate deterministic schedules after the additive lifecycle columns exist."""
    from .provider_eligibility_lifecycle import (
        PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
        provider_eligibility_refresh_due_at,
        provider_eligibility_refresh_schedule_key,
    )

    rows = conn.execute(
        """
        SELECT mal_anime_id, provider, provider_series_id, last_verified_at, verification_outcome
        FROM recommendation_provider_eligibility_evidence
        """
    ).fetchall()
    for row in rows:
        semantic = {
            "mal_anime_id": int(row["mal_anime_id"]),
            "provider": str(row["provider"]),
            "provider_series_id": str(row["provider_series_id"]),
        }
        schedule_key = provider_eligibility_refresh_schedule_key(**semantic)
        refresh_due_at = None
        if row["last_verified_at"] is not None and str(row["verification_outcome"]) in {"positive", "negative"}:
            refresh_due_at = provider_eligibility_refresh_due_at(
                successful_verified_at=str(row["last_verified_at"]),
                **semantic,
            )
        conn.execute(
            """
            UPDATE recommendation_provider_eligibility_evidence
            SET refresh_due_at = ?, refresh_schedule_version = ?, refresh_schedule_key = ?
            WHERE mal_anime_id = ? AND provider = ? AND provider_series_id = ?
            """,
            (
                refresh_due_at,
                PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
                schedule_key,
                semantic["mal_anime_id"],
                semantic["provider"],
                semantic["provider_series_id"],
            ),
        )


def apply_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # Persist the catalog table before starting per-migration transactions.
    conn.commit()
    for migration in MIGRATIONS:
        version = migration.name
        already_applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?", (version,)
        ).fetchone()
        if already_applied:
            continue
        statements = _migration_statements(migration.read_text(encoding="utf-8"))
        try:
            conn.execute("BEGIN IMMEDIATE")
            for statement in statements:
                _execute_migration_statement(conn, statement)
            if version == BROADCAST_COMPATIBILITY_MIGRATION:
                _repair_mal_anime_metadata_broadcast_columns(conn)
            if version == PROVIDER_ELIGIBILITY_REFRESH_LIFECYCLE_MIGRATION:
                _backfill_provider_eligibility_refresh_lifecycle(conn)
            conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    _repair_recorded_broadcast_compatibility_migration(conn)


def bootstrap_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        apply_migrations(conn)


def insert_recommendation_snapshot_rows(
    db_path: Path,
    rows: Iterable[dict[str, Any]],
    *,
    run_id: str,
    generated_at: str,
) -> int:
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        context = row.get("context") if isinstance(row.get("context"), dict) else None
        scorecard = row.get("scorecard") if isinstance(row.get("scorecard"), dict) else None
        if scorecard is None and isinstance(context, dict) and isinstance(context.get("scorecard"), dict):
            scorecard = context.get("scorecard")
        providers = row.get("available_via_providers")
        if not isinstance(providers, list) and isinstance(context, dict):
            providers = context.get("available_via_providers")
        if not isinstance(providers, list):
            providers = row.get("providers")
        if not isinstance(providers, list):
            providers = []
        providers = [p for p in providers if isinstance(p, str) and p.lower() != "mal"]
        dub_signal = row.get("dub_signal")
        if not dub_signal and isinstance(context, dict):
            dub_signal = context.get("dub_signal") or context.get("english_dub_signal")
        availability_confidence_raw = row.get("availability_confidence")
        if availability_confidence_raw is None and isinstance(context, dict):
            availability_confidence_raw = context.get("availability_confidence")
        availability_confidence_label = row.get("availability_confidence_label")
        if availability_confidence_label is None and isinstance(context, dict):
            availability_confidence_label = context.get("availability_confidence_label") or context.get("availability_evidence_label")
        if availability_confidence_label is None:
            availability_confidence_label = _non_numeric_label(availability_confidence_raw)
        reasons = row.get("reasons") if isinstance(row.get("reasons"), list) else []
        prepared.append(
            (
                run_id,
                generated_at,
                row.get("kind"),
                row.get("provider"),
                row.get("title"),
                row.get("provider_series_id"),
                _coerce_int(row.get("mal_anime_id") or (context or {}).get("mal_anime_id")),
                _coerce_float(row.get("scorecard_total") or row.get("score") or (scorecard or {}).get("total")),
                _coerce_int(row.get("priority")),
                json.dumps(reasons, sort_keys=True),
                json.dumps(scorecard, sort_keys=True) if scorecard is not None else None,
                json.dumps(context, sort_keys=True) if context is not None else None,
                json.dumps(providers, sort_keys=True),
                dub_signal,
                _coerce_float(availability_confidence_raw),
                None if availability_confidence_label is None else str(availability_confidence_label),
            )
        )
    if not prepared:
        return 0
    with connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO recommendation_score_snapshots (
                run_id, generated_at, kind, provider, title, provider_series_id, mal_anime_id,
                score, priority, reasons_json, scorecard_json, context_json,
                availability_providers_json, dub_signal, availability_confidence, availability_confidence_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prepared,
        )
        conn.commit()
    return len(prepared)


def list_latest_recommendation_snapshot_rows(db_path: Path, *, limit: int | None = 100, kind: str | None = None) -> list[RecommendationSnapshotRow]:
    with connect(db_path) as conn:
        if kind is None:
            run = conn.execute(
                "SELECT run_id FROM recommendation_score_snapshots ORDER BY generated_at DESC, id DESC LIMIT 1"
            ).fetchone()
        else:
            run = conn.execute(
                "SELECT run_id FROM recommendation_score_snapshots WHERE kind = ? ORDER BY generated_at DESC, id DESC LIMIT 1",
                (kind,),
            ).fetchone()
        if run is None:
            return []
        sql = """
            SELECT * FROM recommendation_score_snapshots
            WHERE run_id = ?
            ORDER BY priority DESC, score DESC, title COLLATE NOCASE ASC, id ASC
            """
        params: tuple[Any, ...]
        if kind is not None:
            sql = sql.replace("WHERE run_id = ?", "WHERE run_id = ? AND kind = ?")
        if limit is None:
            params = (run["run_id"],) if kind is None else (run["run_id"], kind)
        else:
            sql += " LIMIT ?"
            params = (run["run_id"], max(1, int(limit))) if kind is None else (run["run_id"], kind, max(1, int(limit)))
        rows = conn.execute(sql, params).fetchall()
    return [_recommendation_snapshot_row_from_db(row) for row in rows]


def _coerce_int(value: Any) -> int | None:
    try:
        return None if value is None or value == "" else int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def _non_numeric_label(value: Any) -> str | None:
    if isinstance(value, str) and value.strip() and _coerce_float(value) is None:
        return value.strip()
    return None


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    coerced = _coerce_int(value)
    if coerced is None or coerced <= 0:
        return None
    return coerced


def _coerce_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    coerced = _coerce_int(value)
    if coerced is None or coerced < 0:
        return None
    return coerced


def _coerce_non_empty_text(value: Any, *, lowercase: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    return text.lower() if lowercase else text


def _coerce_optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _row_get(row: sqlite3.Row, name: str, default: Any = None) -> Any:
    return row[name] if name in row.keys() else default


def _privacy_safe_tag_count(value: Any) -> int:
    if isinstance(value, list):
        return sum(1 for item in value if isinstance(item, str) and item.strip())
    if isinstance(value, str) and value.strip():
        return 1
    return 0


def _privacy_safe_has_comments(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _raw_payload(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _broadcast_payload(raw: dict[str, Any]) -> dict[str, Any]:
    broadcast = raw.get("broadcast")
    return broadcast if isinstance(broadcast, dict) else {}


def _load_json_value(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _recommendation_snapshot_row_from_db(row: sqlite3.Row) -> RecommendationSnapshotRow:
    return RecommendationSnapshotRow(
        id=int(row["id"]),
        run_id=str(row["run_id"]),
        generated_at=str(row["generated_at"]),
        kind=str(row["kind"]),
        provider=row["provider"],
        title=str(row["title"]),
        provider_series_id=row["provider_series_id"],
        mal_anime_id=row["mal_anime_id"],
        score=row["score"],
        priority=row["priority"],
        reasons=_load_json_value(row["reasons_json"], []),
        scorecard=_load_json_value(row["scorecard_json"], None),
        context=_load_json_value(row["context_json"], None),
        availability_providers=_load_json_value(row["availability_providers_json"], []),
        dub_signal=row["dub_signal"],
        availability_confidence=row["availability_confidence"],
        availability_confidence_label=row["availability_confidence_label"],
    )


def get_series_mapping(db_path: Path, provider: str, provider_series_id: str) -> PersistedSeriesMapping | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                provider,
                provider_series_id,
                mal_anime_id,
                confidence,
                mapping_source,
                approved_by_user,
                notes,
                created_at,
                updated_at
            FROM mal_series_mapping
            WHERE provider = ? AND provider_series_id = ?
            """,
            (provider, provider_series_id),
        ).fetchone()
    if row is None:
        return None
    return PersistedSeriesMapping(
        provider=row["provider"],
        provider_series_id=row["provider_series_id"],
        mal_anime_id=int(row["mal_anime_id"]),
        confidence=None if row["confidence"] is None else float(row["confidence"]),
        mapping_source=str(row["mapping_source"]),
        approved_by_user=bool(row["approved_by_user"]),
        notes=row["notes"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def list_series_mappings(db_path: Path, provider: str | None = None, approved_only: bool = False) -> list[PersistedSeriesMapping]:
    query = """
        SELECT
            provider,
            provider_series_id,
            mal_anime_id,
            confidence,
            mapping_source,
            approved_by_user,
            notes,
            created_at,
            updated_at
        FROM mal_series_mapping
    """
    conditions: list[str] = []
    params: list[object] = []
    if provider is not None:
        conditions.append("provider = ?")
        params.append(provider)
    if approved_only:
        conditions.append("approved_by_user = 1")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY approved_by_user DESC, updated_at DESC, provider_series_id ASC"

    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        PersistedSeriesMapping(
            provider=row["provider"],
            provider_series_id=row["provider_series_id"],
            mal_anime_id=int(row["mal_anime_id"]),
            confidence=None if row["confidence"] is None else float(row["confidence"]),
            mapping_source=str(row["mapping_source"]),
            approved_by_user=bool(row["approved_by_user"]),
            notes=row["notes"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )
        for row in rows
    ]


def upsert_series_mapping(
    db_path: Path,
    *,
    provider: str,
    provider_series_id: str,
    mal_anime_id: int,
    confidence: float | None,
    mapping_source: str,
    approved_by_user: bool,
    notes: str | None,
) -> PersistedSeriesMapping:
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO mal_series_mapping (
                provider,
                provider_series_id,
                mal_anime_id,
                confidence,
                mapping_source,
                approved_by_user,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_series_id) DO UPDATE SET
                mal_anime_id = excluded.mal_anime_id,
                confidence = excluded.confidence,
                mapping_source = excluded.mapping_source,
                approved_by_user = excluded.approved_by_user,
                notes = excluded.notes,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                provider_series_id,
                int(mal_anime_id),
                confidence,
                mapping_source,
                1 if approved_by_user else 0,
                notes,
            ),
        )
        conn.commit()
    mapping = get_series_mapping(db_path, provider, provider_series_id)
    if mapping is None:
        raise RuntimeError("Persisted mapping disappeared after upsert")
    return mapping


def replace_review_queue_entries(
    db_path: Path,
    *,
    issue_type: str,
    entries: list[dict[str, Any]],
) -> dict[str, int]:
    with connect(db_path) as conn:
        cursor = conn.execute(
            "UPDATE review_queue SET status = 'resolved', resolved_at = CURRENT_TIMESTAMP WHERE issue_type = ? AND status = 'open'",
            (issue_type,),
        )
        resolved = int(cursor.rowcount or 0)
        inserted = 0
        for entry in entries:
            conn.execute(
                """
                INSERT INTO review_queue (
                    provider,
                    provider_series_id,
                    provider_episode_id,
                    issue_type,
                    severity,
                    payload_json,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, 'open')
                """,
                (
                    entry["provider"],
                    entry.get("provider_series_id"),
                    entry.get("provider_episode_id"),
                    issue_type,
                    entry.get("severity", "warning"),
                    json.dumps(entry["payload"], sort_keys=True),
                ),
            )
            inserted += 1
        conn.commit()
    return {"resolved": resolved, "inserted": inserted}



def refresh_review_queue_entries(
    db_path: Path,
    *,
    issue_type: str,
    provider_series_ids: Iterable[str],
    entries: list[dict[str, Any]],
) -> dict[str, int]:
    normalized_ids = sorted({value for value in provider_series_ids if isinstance(value, str) and value})
    if not normalized_ids:
        return {"resolved": 0, "inserted": 0}
    placeholders = ", ".join("?" for _ in normalized_ids)
    with connect(db_path) as conn:
        cursor = conn.execute(
            f"UPDATE review_queue SET status = 'resolved', resolved_at = CURRENT_TIMESTAMP WHERE issue_type = ? AND status = 'open' AND provider_series_id IN ({placeholders})",
            [issue_type, *normalized_ids],
        )
        resolved = int(cursor.rowcount or 0)
        inserted = 0
        for entry in entries:
            provider_series_id = entry.get("provider_series_id")
            if provider_series_id not in normalized_ids:
                continue
            conn.execute(
                """
                INSERT INTO review_queue (
                    provider,
                    provider_series_id,
                    provider_episode_id,
                    issue_type,
                    severity,
                    payload_json,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, 'open')
                """,
                (
                    entry["provider"],
                    provider_series_id,
                    entry.get("provider_episode_id"),
                    issue_type,
                    entry.get("severity", "warning"),
                    json.dumps(entry["payload"], sort_keys=True),
                ),
            )
            inserted += 1
        conn.commit()
    return {"resolved": resolved, "inserted": inserted}


def upsert_mal_anime_metadata(
    db_path: Path,
    *,
    mal_anime_id: int,
    title: str,
    title_english: str | None,
    title_japanese: str | None,
    alternative_titles: list[str],
    media_type: str | None,
    status: str | None,
    num_episodes: int | None,
    mean: float | None,
    popularity: int | None,
    start_season: dict[str, Any] | None,
    raw: dict[str, Any],
    rank: int | None = None,
    num_list_users: int | None = None,
    num_scoring_users: int | None = None,
    rating: str | None = None,
    average_episode_duration: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    broadcast_day: str | None = None,
    broadcast_time: str | None = None,
    broadcast_timezone: str | None = None,
    nsfw: str | None = None,
) -> None:
    raw_payload = _raw_payload(raw)
    broadcast_payload = _broadcast_payload(raw_payload)
    rank_value = _coerce_positive_int(rank if rank is not None else raw_payload.get("rank"))
    num_list_users_value = _coerce_nonnegative_int(num_list_users if num_list_users is not None else raw_payload.get("num_list_users"))
    num_scoring_users_value = _coerce_nonnegative_int(num_scoring_users if num_scoring_users is not None else raw_payload.get("num_scoring_users"))
    rating_value = _coerce_non_empty_text(rating if rating is not None else raw_payload.get("rating"), lowercase=True)
    average_episode_duration_value = _coerce_positive_int(
        average_episode_duration if average_episode_duration is not None else raw_payload.get("average_episode_duration")
    )
    start_date_value = _coerce_non_empty_text(start_date if start_date is not None else raw_payload.get("start_date"))
    end_date_value = _coerce_non_empty_text(end_date if end_date is not None else raw_payload.get("end_date"))
    broadcast_day_value = _coerce_non_empty_text(
        broadcast_day if broadcast_day is not None else broadcast_payload.get("day_of_the_week"),
        lowercase=True,
    )
    broadcast_time_value = _coerce_non_empty_text(
        broadcast_time if broadcast_time is not None else broadcast_payload.get("start_time")
    )
    broadcast_timezone_value = _coerce_non_empty_text(
        broadcast_timezone if broadcast_timezone is not None else broadcast_payload.get("timezone")
    )
    nsfw_value = _coerce_non_empty_text(nsfw if nsfw is not None else raw_payload.get("nsfw"), lowercase=True)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO mal_anime_metadata (
                mal_anime_id,
                title,
                title_english,
                title_japanese,
                alternative_titles_json,
                media_type,
                status,
                num_episodes,
                mean,
                popularity,
                start_season_json,
                rank,
                num_list_users,
                num_scoring_users,
                rating,
                average_episode_duration,
                start_date,
                end_date,
                broadcast_day,
                broadcast_time,
                broadcast_timezone,
                nsfw,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mal_anime_id) DO UPDATE SET
                title = excluded.title,
                title_english = excluded.title_english,
                title_japanese = excluded.title_japanese,
                alternative_titles_json = excluded.alternative_titles_json,
                media_type = excluded.media_type,
                status = excluded.status,
                num_episodes = excluded.num_episodes,
                mean = excluded.mean,
                popularity = excluded.popularity,
                start_season_json = excluded.start_season_json,
                rank = excluded.rank,
                num_list_users = excluded.num_list_users,
                num_scoring_users = excluded.num_scoring_users,
                rating = excluded.rating,
                average_episode_duration = excluded.average_episode_duration,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                broadcast_day = excluded.broadcast_day,
                broadcast_time = excluded.broadcast_time,
                broadcast_timezone = excluded.broadcast_timezone,
                nsfw = excluded.nsfw,
                raw_json = excluded.raw_json,
                fetched_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(mal_anime_id),
                title,
                title_english,
                title_japanese,
                json.dumps(alternative_titles, ensure_ascii=False, sort_keys=True),
                media_type,
                status,
                num_episodes,
                mean,
                popularity,
                json.dumps(start_season, ensure_ascii=False, sort_keys=True) if start_season is not None else None,
                rank_value,
                num_list_users_value,
                num_scoring_users_value,
                rating_value,
                average_episode_duration_value,
                start_date_value,
                end_date_value,
                broadcast_day_value,
                broadcast_time_value,
                broadcast_timezone_value,
                nsfw_value,
                json.dumps(raw, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()


def _json_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _json_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _watch_confirmation_provenance_from_row(row: sqlite3.Row) -> WatchConfirmationProvenance:
    return WatchConfirmationProvenance(
        provider=str(row["provider"]),
        provider_series_id=str(row["provider_series_id"]),
        identity_key=str(row["identity_key"] or ""),
        mal_anime_id=None if row["mal_anime_id"] is None else int(row["mal_anime_id"]),
        source_title=str(row["source_title"]),
        season_title=row["season_title"],
        mapped_mal_title=row["mapped_mal_title"],
        progress_rows=int(row["progress_rows"] or 0),
        completed_episode_count=int(row["completed_episode_count"] or 0),
        max_episode_number=None if row["max_episode_number"] is None else int(row["max_episode_number"]),
        max_completed_episode_number=None if row["max_completed_episode_number"] is None else int(row["max_completed_episode_number"]),
        provider_watched_episodes=int(row["provider_watched_episodes"] or 0),
        mal_num_episodes=None if row["mal_num_episodes"] is None else int(row["mal_num_episodes"]),
        confirmed_complete=bool(row["confirmed_complete"]),
        completion_decision=str(row["completion_decision"]),
        completion_status=str(row["completion_status"]),
        completion_threshold=None if row["completion_threshold"] is None else float(row["completion_threshold"]),
        credits_skip_window_seconds=None if row["credits_skip_window_seconds"] is None else int(row["credits_skip_window_seconds"]),
        last_watched_at=row["last_watched_at"],
        last_progress_seen_at=row["last_progress_seen_at"],
        last_series_seen_at=row["last_series_seen_at"],
        last_evidence_at=row["last_evidence_at"],
        mapping_source=row["mapping_source"],
        mapping_confidence=None if row["mapping_confidence"] is None else float(row["mapping_confidence"]),
        mapping_approved=bool(row["mapping_approved"]),
        verified_identity_kind=row["verified_identity_kind"],
        verified_identity=_json_dict(_load_json_value(row["verified_identity_json"], {})) if row["verified_identity_json"] else None,
        completed_by=_json_dict(_load_json_value(row["completed_by_json"], {})),
        completed_examples=_json_dict(_load_json_value(row["completed_examples_json"], {})),
        incomplete_examples=_json_list(_load_json_value(row["incomplete_examples_json"], [])),
        thresholds=_json_dict(_load_json_value(row["thresholds_json"], {})),
        progress_audit=_json_dict(_load_json_value(row["progress_audit_json"], {})),
        mapping_audit=_json_dict(_load_json_value(row["mapping_audit_json"], {})),
        decision_audit=_json_dict(_load_json_value(row["decision_audit_json"], {})),
        generated_at=str(row["generated_at"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def get_watch_confirmation_provenance(
    db_path: Path,
    *,
    provider: str,
    provider_series_id: str,
) -> WatchConfirmationProvenance | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT * FROM watch_confirmation_provenance
            WHERE provider = ? AND provider_series_id = ?
            """,
            (provider, provider_series_id),
        ).fetchone()
    return None if row is None else _watch_confirmation_provenance_from_row(row)


def list_watch_confirmation_provenance(
    db_path: Path,
    *,
    provider: str | None = None,
    mal_anime_id: int | None = None,
    identity_key: str | None = None,
    confirmed_complete: bool | None = None,
    limit: int | None = None,
) -> list[WatchConfirmationProvenance]:
    conditions: list[str] = []
    params: list[Any] = []
    if provider is not None:
        conditions.append("provider = ?")
        params.append(provider)
    if mal_anime_id is not None:
        conditions.append("mal_anime_id = ?")
        params.append(int(mal_anime_id))
    if identity_key is not None:
        conditions.append("identity_key = ?")
        params.append(identity_key)
    if confirmed_complete is not None:
        conditions.append("confirmed_complete = ?")
        params.append(1 if confirmed_complete else 0)
    query = "SELECT * FROM watch_confirmation_provenance"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY updated_at DESC, provider ASC, provider_series_id ASC"
    if limit is not None and limit > 0:
        query += " LIMIT ?"
        params.append(int(limit))
    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [_watch_confirmation_provenance_from_row(row) for row in rows]


def get_watch_confirmation_coverage(db_path: Path, *, provider: str = "hidive") -> dict[str, Any]:
    """Return a privacy-safe progress-series confirmation coverage invariant."""
    with connect(db_path) as conn:
        progress_rows = conn.execute(
            """
            SELECT DISTINCT provider_series_id
            FROM provider_episode_progress
            WHERE provider = ?
            ORDER BY provider_series_id
            """,
            (provider,),
        ).fetchall()
        confirmation_rows = conn.execute(
            """
            SELECT DISTINCT provider_series_id
            FROM watch_confirmation_provenance
            WHERE provider = ?
            ORDER BY provider_series_id
            """,
            (provider,),
        ).fetchall()
    progress_series = {str(row["provider_series_id"]) for row in progress_rows}
    confirmation_series = {str(row["provider_series_id"]) for row in confirmation_rows}
    missing = sorted(progress_series - confirmation_series)
    return {
        "provider": provider,
        "progress_series_count": len(progress_series),
        "confirmation_series_count": len(confirmation_series),
        "missing_confirmation_series_count": len(missing),
        "missing_confirmation_series_hashes": [
            hashlib.sha256(f"{provider}:{provider_series_id}".encode("utf-8")).hexdigest()
            for provider_series_id in missing
        ],
        "invariant_satisfied": not missing,
    }


def upsert_watch_confirmation_provenance(
    db_path: Path,
    *,
    provider: str,
    provider_series_id: str,
    identity_key: str | None = None,
    mal_anime_id: int | None = None,
    source_title: str,
    season_title: str | None = None,
    mapped_mal_title: str | None = None,
    progress_rows: int = 0,
    completed_episode_count: int = 0,
    max_episode_number: int | None = None,
    max_completed_episode_number: int | None = None,
    provider_watched_episodes: int = 0,
    mal_num_episodes: int | None = None,
    confirmed_complete: bool = False,
    completion_decision: str = "unknown",
    completion_status: str = "unknown",
    completion_threshold: float | None = None,
    credits_skip_window_seconds: int | None = None,
    last_watched_at: str | None = None,
    last_progress_seen_at: str | None = None,
    last_series_seen_at: str | None = None,
    last_evidence_at: str | None = None,
    mapping_source: str | None = None,
    mapping_confidence: float | None = None,
    mapping_approved: bool = False,
    verified_identity_kind: str | None = None,
    verified_identity: dict[str, Any] | None = None,
    completed_by: dict[str, Any] | None = None,
    completed_examples: dict[str, Any] | None = None,
    incomplete_examples: list[Any] | None = None,
    thresholds: dict[str, Any] | None = None,
    progress_audit: dict[str, Any] | None = None,
    mapping_audit: dict[str, Any] | None = None,
    decision_audit: dict[str, Any] | None = None,
    generated_at: str,
) -> WatchConfirmationProvenance:
    normalized_identity_key = str(identity_key or f"{provider}:{provider_series_id}")
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO watch_confirmation_provenance (
                provider,
                provider_series_id,
                identity_key,
                mal_anime_id,
                source_title,
                season_title,
                mapped_mal_title,
                progress_rows,
                completed_episode_count,
                max_episode_number,
                max_completed_episode_number,
                provider_watched_episodes,
                mal_num_episodes,
                confirmed_complete,
                completion_decision,
                completion_status,
                completion_threshold,
                credits_skip_window_seconds,
                last_watched_at,
                last_progress_seen_at,
                last_series_seen_at,
                last_evidence_at,
                mapping_source,
                mapping_confidence,
                mapping_approved,
                verified_identity_kind,
                verified_identity_json,
                completed_by_json,
                completed_examples_json,
                incomplete_examples_json,
                thresholds_json,
                progress_audit_json,
                mapping_audit_json,
                decision_audit_json,
                generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_series_id) DO UPDATE SET
                identity_key = excluded.identity_key,
                mal_anime_id = excluded.mal_anime_id,
                source_title = excluded.source_title,
                season_title = excluded.season_title,
                mapped_mal_title = excluded.mapped_mal_title,
                progress_rows = excluded.progress_rows,
                completed_episode_count = excluded.completed_episode_count,
                max_episode_number = excluded.max_episode_number,
                max_completed_episode_number = excluded.max_completed_episode_number,
                provider_watched_episodes = excluded.provider_watched_episodes,
                mal_num_episodes = excluded.mal_num_episodes,
                confirmed_complete = excluded.confirmed_complete,
                completion_decision = excluded.completion_decision,
                completion_status = excluded.completion_status,
                completion_threshold = excluded.completion_threshold,
                credits_skip_window_seconds = excluded.credits_skip_window_seconds,
                last_watched_at = excluded.last_watched_at,
                last_progress_seen_at = excluded.last_progress_seen_at,
                last_series_seen_at = excluded.last_series_seen_at,
                last_evidence_at = excluded.last_evidence_at,
                mapping_source = excluded.mapping_source,
                mapping_confidence = excluded.mapping_confidence,
                mapping_approved = excluded.mapping_approved,
                verified_identity_kind = excluded.verified_identity_kind,
                verified_identity_json = excluded.verified_identity_json,
                completed_by_json = excluded.completed_by_json,
                completed_examples_json = excluded.completed_examples_json,
                incomplete_examples_json = excluded.incomplete_examples_json,
                thresholds_json = excluded.thresholds_json,
                progress_audit_json = excluded.progress_audit_json,
                mapping_audit_json = excluded.mapping_audit_json,
                decision_audit_json = excluded.decision_audit_json,
                generated_at = excluded.generated_at,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                provider_series_id,
                normalized_identity_key,
                None if mal_anime_id is None else int(mal_anime_id),
                source_title,
                season_title,
                mapped_mal_title,
                max(0, int(progress_rows)),
                max(0, int(completed_episode_count)),
                None if max_episode_number is None else int(max_episode_number),
                None if max_completed_episode_number is None else int(max_completed_episode_number),
                max(0, int(provider_watched_episodes)),
                None if mal_num_episodes is None else int(mal_num_episodes),
                1 if confirmed_complete else 0,
                str(completion_decision or "unknown"),
                str(completion_status or "unknown"),
                None if completion_threshold is None else float(completion_threshold),
                None if credits_skip_window_seconds is None else int(credits_skip_window_seconds),
                last_watched_at,
                last_progress_seen_at,
                last_series_seen_at,
                last_evidence_at,
                mapping_source,
                None if mapping_confidence is None else float(mapping_confidence),
                1 if mapping_approved else 0,
                verified_identity_kind,
                _json_dumps(verified_identity) if verified_identity is not None else None,
                _json_dumps(completed_by or {}),
                _json_dumps(completed_examples or {}),
                _json_dumps(incomplete_examples or []),
                _json_dumps(thresholds or {}),
                _json_dumps(progress_audit or {}),
                _json_dumps(mapping_audit or {}),
                _json_dumps(decision_audit or {}),
                str(generated_at),
            ),
        )
        conn.commit()
    row = get_watch_confirmation_provenance(db_path, provider=provider, provider_series_id=provider_series_id)
    if row is None:
        raise RuntimeError("watch confirmation provenance disappeared after upsert")
    return row


_ALLOWED_MAL_USER_LIST_STATUSES = {"completed", "watching", "on_hold", "dropped", "plan_to_watch"}
_MAL_USER_LIST_REFRESH_ACTIVE_STATUS = "active"
_MAL_USER_LIST_REFRESH_TERMINAL_STATUSES = {"completed", "partial", "failed"}
_MAL_USER_LIST_REFRESH_ERROR_MAX_LENGTH = 2000
_MAL_USER_LIST_REFRESH_SUPERSEDED_ERROR = "superseded by a newer MAL user anime list refresh"


@dataclass(frozen=True, slots=True)
class MalUserAnimeListRefreshGeneration:
    refresh_run_id: str
    generation: int
    fetched_at: str


class MalUserAnimeListRefreshConflictError(RuntimeError):
    """A cache refresh lost ownership of the current active generation."""


MAL_USER_LIST_PAGINATION_LOGIC_VERSION = "mal-user-list-pagination-v2"
MAL_USER_LIST_CLAIM_SECONDS = 15 * 60
MAL_USER_LIST_MAX_DRIFT_RESTARTS = 2


@dataclass(frozen=True, slots=True)
class MalUserAnimeListTraversalGeneration:
    refresh_run_id: str
    generation: int
    fetched_at: str
    account_key: str
    account_id: int
    account_name: str
    query_identity: str
    query: dict[str, Any]
    claim_token: str | None
    claim_expires_at: str | None
    revision: int
    requests_attempted: int
    requests_succeeded: int
    requests_failed: int
    restart_count: int
    drift_count: int
    quarantined_at: str | None
    publication_epoch: int
    identity_assertion_nonce: str | None


@dataclass(frozen=True, slots=True)
class MalUserAnimeListTraversalPartition:
    generation: int
    partition_key: str
    requested_status: str | None
    ordinal: int
    initial_url: str
    next_url: str | None
    page_sequence: int
    item_count: int
    terminal: bool
    terminal_explicit: bool
    empty_proven: bool
    first_page_fingerprint: str | None
    final_page_url: str | None
    final_page_fingerprint: str | None
    page1_validated_at: str | None
    boundary_validated_at: str | None
    attempt_count: int
    retry_count: int
    requests_succeeded: int
    requests_failed: int
    next_retry_at: str | None
    retry_class: str | None
    fairness_sequence: int
    first_started_at: str | None
    terminal_at: str | None


def _normalize_mal_user_list_status(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if normalized in _ALLOWED_MAL_USER_LIST_STATUSES else None


def _coerce_optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _coerce_mal_anime_id(value: Any) -> int | None:
    mal_anime_id = _coerce_optional_int(value)
    if mal_anime_id is None or mal_anime_id <= 0:
        return None
    return mal_anime_id


def _clamp_optional_int(value: Any, *, minimum: int, maximum: int | None = None) -> int | None:
    coerced = _coerce_optional_int(value)
    if coerced is None:
        return None
    coerced = max(coerced, minimum)
    if maximum is not None:
        coerced = min(coerced, maximum)
    return coerced


def _bounded_mal_user_list_refresh_error(error: str | None) -> str | None:
    if error is None:
        return None
    text = str(error)
    if len(text) <= _MAL_USER_LIST_REFRESH_ERROR_MAX_LENGTH:
        return text
    return text[: _MAL_USER_LIST_REFRESH_ERROR_MAX_LENGTH - 1] + "…"


def _coerce_mal_user_list_refresh_run_id(refresh_run_id: str) -> str:
    run_id = str(refresh_run_id).strip()
    if not run_id:
        raise ValueError("refresh_run_id is required")
    return run_id


def _coerce_mal_user_list_refresh_generation(generation: int) -> int:
    try:
        value = int(generation)
    except (TypeError, ValueError) as exc:
        raise ValueError("refresh generation must be a positive integer") from exc
    if value <= 0:
        raise ValueError("refresh generation must be a positive integer")
    return value


def _fetch_mal_user_list_refresh_generation(
    conn: sqlite3.Connection,
    *,
    refresh_run_id: str,
    generation: int,
) -> sqlite3.Row:
    run_id = _coerce_mal_user_list_refresh_run_id(refresh_run_id)
    generation_id = _coerce_mal_user_list_refresh_generation(generation)
    row = conn.execute(
        """
        SELECT *
        FROM mal_user_anime_list_refresh_generations
        WHERE generation = ?
        """,
        (generation_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"unknown MAL user anime list refresh generation {generation_id}")
    if str(row["refresh_run_id"]) != run_id:
        raise ValueError(
            "MAL user anime list refresh generation/run mismatch: "
            f"generation {generation_id} belongs to a different refresh_run_id"
        )
    return row


def _require_active_mal_user_list_refresh_generation(
    conn: sqlite3.Connection,
    *,
    refresh_run_id: str,
    generation: int,
    operation: str,
) -> sqlite3.Row:
    row = _fetch_mal_user_list_refresh_generation(
        conn,
        refresh_run_id=refresh_run_id,
        generation=generation,
    )
    status = str(row["status"])
    if status != _MAL_USER_LIST_REFRESH_ACTIVE_STATUS:
        raise MalUserAnimeListRefreshConflictError(
            "MAL user anime list refresh generation "
            f"{int(row['generation'])} for refresh_run_id={row['refresh_run_id']!r} "
            f"is terminal ({status}); cannot {operation}"
        )
    return row


def _latest_mal_user_list_refresh_generation(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(generation), 0) AS generation FROM mal_user_anime_list_refresh_generations"
    ).fetchone()
    return int(row["generation"] or 0)


def _require_current_mal_user_list_refresh_generation(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    operation: str,
) -> None:
    generation = int(row["generation"])
    # Legacy lifecycle helpers own only the legacy/accountless namespace. A
    # durable account/query traversal may coexist and must not supersede or be
    # pruned by these compatibility APIs.
    latest = conn.execute(
        "SELECT COALESCE(MAX(generation), 0) AS generation FROM mal_user_anime_list_refresh_generations WHERE account_id IS NULL"
    ).fetchone()
    latest_generation = int(latest["generation"] or 0)
    if latest_generation != generation:
        raise MalUserAnimeListRefreshConflictError(
            "stale MAL user anime list refresh generation "
            f"{generation} cannot {operation}; latest generation is {latest_generation}"
        )


def _active_mal_user_list_refresh_generation_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM mal_user_anime_list_refresh_generations WHERE status = 'active'"
    ).fetchone()
    return int(row["n"] or 0)


def _require_single_active_mal_user_list_refresh_generation(conn: sqlite3.Connection, row: sqlite3.Row) -> None:
    active = conn.execute(
        "SELECT COUNT(*) AS n FROM mal_user_anime_list_refresh_generations WHERE status='active' AND account_id IS NULL"
    ).fetchone()
    active_count = int(active["n"] or 0)
    if active_count != 1:
        raise MalUserAnimeListRefreshConflictError(
            "expected exactly one active MAL user anime list refresh generation; "
            f"found {active_count}"
        )
    if str(row["status"]) != _MAL_USER_LIST_REFRESH_ACTIVE_STATUS:
        raise MalUserAnimeListRefreshConflictError(
            "MAL user anime list refresh generation "
            f"{int(row['generation'])} is not active after begin"
        )


def _summarize_mal_user_list_refresh_rows(rows: Iterable[sqlite3.Row]) -> tuple[dict[str, int], int, int, dict[str, int]]:
    by_status: dict[str, int] = {}
    scored = 0
    unscored = 0
    preference_counts = _empty_preference_counts()
    for row in rows:
        status = row["list_status"]
        score = row["user_score"]
        if status:
            by_status[str(status)] = by_status.get(str(status), 0) + 1
        if score is not None and int(score) > 0:
            scored += 1
        else:
            unscored += 1
        if row["priority"] is not None:
            preference_counts["with_priority"] += 1
        if row["is_rewatching"] is not None:
            preference_counts["with_rewatching"] += 1
        if row["num_times_rewatched"] is not None:
            preference_counts["with_num_times_rewatched"] += 1
        if row["rewatch_value"] is not None:
            preference_counts["with_rewatch_value"] += 1
        if int(row["tag_count"] or 0) > 0:
            preference_counts["with_tags"] += 1
        if int(row["has_comments"] or 0) > 0:
            preference_counts["with_comments"] += 1
    return by_status, scored, unscored, preference_counts


def begin_mal_user_anime_list_cache_refresh(
    db_path: Path,
    *,
    refresh_run_id: str,
    fetched_at: str,
) -> MalUserAnimeListRefreshGeneration:
    """Allocate the sole active cache refresh generation without pruning any rows."""
    run_id = _coerce_mal_user_list_refresh_run_id(refresh_run_id)
    fetched = str(fetched_at).strip()
    if not fetched:
        raise ValueError("fetched_at is required")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            """
            SELECT generation, refresh_run_id, status, fetched_at
            FROM mal_user_anime_list_refresh_generations
            WHERE refresh_run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if existing is not None:
            status = str(existing["status"])
            if status == _MAL_USER_LIST_REFRESH_ACTIVE_STATUS:
                latest_generation = _latest_mal_user_list_refresh_generation(conn)
                if int(existing["generation"]) == latest_generation:
                    conn.commit()
                    return MalUserAnimeListRefreshGeneration(
                        refresh_run_id=str(existing["refresh_run_id"]),
                        generation=int(existing["generation"]),
                        fetched_at=str(existing["fetched_at"]),
                    )
                conn.execute(
                    """
                    UPDATE mal_user_anime_list_refresh_generations
                    SET status = 'failed',
                        completed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP,
                        error = ?
                    WHERE generation = ? AND refresh_run_id = ? AND status = 'active'
                    """,
                    (_MAL_USER_LIST_REFRESH_SUPERSEDED_ERROR, int(existing["generation"]), run_id),
                )
                raise MalUserAnimeListRefreshConflictError(
                    "refresh_run_id belongs to stale active MAL user anime list "
                    f"refresh generation {int(existing['generation'])}; "
                    f"latest generation is {latest_generation}"
                )
            if status in _MAL_USER_LIST_REFRESH_TERMINAL_STATUSES:
                raise MalUserAnimeListRefreshConflictError(
                    "refresh_run_id already belongs to terminal MAL user anime list "
                    f"refresh generation {int(existing['generation'])} ({status})"
                )
            raise RuntimeError(
                "refresh_run_id belongs to MAL user anime list refresh generation "
                f"{int(existing['generation'])} with unsupported status {status!r}"
            )
        conn.execute(
            """
            UPDATE mal_user_anime_list_refresh_generations
            SET status = 'failed',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                error = ?
            WHERE status = 'active' AND account_id IS NULL
            """,
            (_MAL_USER_LIST_REFRESH_SUPERSEDED_ERROR,),
        )
        cursor = conn.execute(
            """
            INSERT INTO mal_user_anime_list_refresh_generations (refresh_run_id, status, fetched_at)
            VALUES (?, 'active', ?)
            """,
            (run_id, fetched),
        )
        generation = int(cursor.lastrowid)
        inserted = _fetch_mal_user_list_refresh_generation(
            conn,
            refresh_run_id=run_id,
            generation=generation,
        )
        _require_single_active_mal_user_list_refresh_generation(conn, inserted)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return MalUserAnimeListRefreshGeneration(
        refresh_run_id=run_id,
        generation=generation,
        fetched_at=fetched,
    )


def _mal_user_list_entry_from_row(row: sqlite3.Row) -> MalUserAnimeListCacheEntry:
    tag_count = _row_get(row, "tag_count", 0)
    has_comments = _row_get(row, "has_comments", 0)
    return MalUserAnimeListCacheEntry(
        mal_anime_id=int(row["mal_anime_id"]),
        title=str(row["title"]),
        list_status=row["list_status"],
        user_score=None if row["user_score"] is None else int(row["user_score"]),
        num_episodes_watched=None if row["num_episodes_watched"] is None else int(row["num_episodes_watched"]),
        start_date=row["start_date"],
        finish_date=row["finish_date"],
        list_updated_at=row["list_updated_at"],
        priority=None if _row_get(row, "priority") is None else int(_row_get(row, "priority")),
        is_rewatching=_coerce_optional_bool(_row_get(row, "is_rewatching")),
        num_times_rewatched=None if _row_get(row, "num_times_rewatched") is None else int(_row_get(row, "num_times_rewatched")),
        rewatch_value=None if _row_get(row, "rewatch_value") is None else int(_row_get(row, "rewatch_value")),
        tag_count=max(int(tag_count or 0), 0),
        has_comments=bool(has_comments),
        node=json.loads(row["node_json"] or "{}"),
        list_status_raw=json.loads(row["list_status_json"] or "{}"),
        raw=json.loads(row["raw_json"]),
        refresh_run_id=str(row["refresh_run_id"]),
        refresh_generation=int(row["refresh_generation"]),
        fetched_at=str(row["fetched_at"]),
        last_seen_at=str(row["last_seen_at"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _prepare_mal_user_list_cache_item(item: dict[str, Any], *, refresh_run_id: str, generation: int, fetched_at: str) -> tuple[Any, ...] | None:
    node = item.get("node") if isinstance(item.get("node"), dict) else {}
    mal_anime_id = _coerce_mal_anime_id(node.get("id"))
    if mal_anime_id is None:
        return None
    title_raw = node.get("title")
    title = title_raw.strip() if isinstance(title_raw, str) and title_raw.strip() else f"MAL anime {mal_anime_id}"
    list_status_raw = item.get("list_status") if isinstance(item.get("list_status"), dict) else {}
    status = _normalize_mal_user_list_status(list_status_raw.get("status"))
    score = _clamp_optional_int(list_status_raw.get("score"), minimum=0, maximum=10)
    watched = _clamp_optional_int(list_status_raw.get("num_episodes_watched"), minimum=0)
    start_date = list_status_raw.get("start_date") if isinstance(list_status_raw.get("start_date"), str) else None
    finish_date = list_status_raw.get("finish_date") if isinstance(list_status_raw.get("finish_date"), str) else None
    list_updated_at = list_status_raw.get("updated_at") if isinstance(list_status_raw.get("updated_at"), str) else None
    priority = _clamp_optional_int(list_status_raw.get("priority"), minimum=0, maximum=2)
    is_rewatching = _coerce_optional_bool(list_status_raw.get("is_rewatching"))
    num_times_rewatched = _clamp_optional_int(list_status_raw.get("num_times_rewatched"), minimum=0)
    rewatch_value = _clamp_optional_int(list_status_raw.get("rewatch_value"), minimum=0, maximum=5)
    tag_count = _privacy_safe_tag_count(list_status_raw.get("tags"))
    has_comments = _privacy_safe_has_comments(list_status_raw.get("comments"))
    return (
        mal_anime_id,
        title,
        status,
        score,
        watched,
        start_date,
        finish_date,
        list_updated_at,
        priority,
        None if is_rewatching is None else int(is_rewatching),
        num_times_rewatched,
        rewatch_value,
        tag_count,
        int(has_comments),
        json.dumps(node, ensure_ascii=False, sort_keys=True),
        json.dumps(list_status_raw, ensure_ascii=False, sort_keys=True),
        json.dumps(item, ensure_ascii=False, sort_keys=True),
        str(refresh_run_id),
        int(generation),
        str(fetched_at),
        str(fetched_at),
    )


def _empty_preference_counts() -> dict[str, int]:
    return {
        "with_priority": 0,
        "with_rewatching": 0,
        "with_num_times_rewatched": 0,
        "with_rewatch_value": 0,
        "with_tags": 0,
        "with_comments": 0,
    }


def _summarize_prepared_mal_user_list_rows(prepared: list[tuple[Any, ...]]) -> tuple[dict[str, int], int, int, dict[str, int]]:
    by_status: dict[str, int] = {}
    scored = 0
    unscored = 0
    preference_counts = _empty_preference_counts()
    for row in prepared:
        status = row[2]
        score = row[3]
        if status:
            by_status[str(status)] = by_status.get(str(status), 0) + 1
        if score is not None and int(score) > 0:
            scored += 1
        else:
            unscored += 1
        if row[8] is not None:
            preference_counts["with_priority"] += 1
        if row[9] is not None:
            preference_counts["with_rewatching"] += 1
        if row[10] is not None:
            preference_counts["with_num_times_rewatched"] += 1
        if row[11] is not None:
            preference_counts["with_rewatch_value"] += 1
        if int(row[12] or 0) > 0:
            preference_counts["with_tags"] += 1
        if int(row[13] or 0) > 0:
            preference_counts["with_comments"] += 1
    return by_status, scored, unscored, preference_counts


def upsert_mal_user_anime_list_cache_generation(
    db_path: Path,
    *,
    items: Iterable[dict[str, Any]],
    refresh_run_id: str,
    generation: int,
    fetched_at: str,
) -> MalUserAnimeListRefreshSummary:
    """Upsert rows for a refresh generation without deleting absent prior rows."""
    run_id = _coerce_mal_user_list_refresh_run_id(refresh_run_id)
    generation_id = _coerce_mal_user_list_refresh_generation(generation)
    fetched = str(fetched_at).strip()
    if not fetched:
        raise ValueError("fetched_at is required")
    prepared: list[tuple[Any, ...]] = []
    seen_ids: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        row = _prepare_mal_user_list_cache_item(
            item,
            refresh_run_id=run_id,
            generation=generation_id,
            fetched_at=fetched,
        )
        if row is None:
            continue
        mal_anime_id = int(row[0])
        if mal_anime_id in seen_ids:
            continue
        seen_ids.add(mal_anime_id)
        prepared.append(row)
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        lifecycle = _require_active_mal_user_list_refresh_generation(
            conn,
            refresh_run_id=run_id,
            generation=generation_id,
            operation="upsert cache rows",
        )
        _require_current_mal_user_list_refresh_generation(
            conn,
            lifecycle,
            operation="upsert cache rows",
        )
        changes_before = conn.total_changes
        if prepared:
            conn.executemany(
                """
                INSERT INTO mal_user_anime_list_cache (
                    mal_anime_id, title, list_status, user_score, num_episodes_watched,
                    start_date, finish_date, list_updated_at, priority, is_rewatching,
                    num_times_rewatched, rewatch_value, tag_count, has_comments,
                    node_json, list_status_json, raw_json, refresh_run_id,
                    refresh_generation, fetched_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mal_anime_id) DO UPDATE SET
                    title = excluded.title,
                    list_status = excluded.list_status,
                    user_score = excluded.user_score,
                    num_episodes_watched = excluded.num_episodes_watched,
                    start_date = excluded.start_date,
                    finish_date = excluded.finish_date,
                    list_updated_at = excluded.list_updated_at,
                    priority = excluded.priority,
                    is_rewatching = excluded.is_rewatching,
                    num_times_rewatched = excluded.num_times_rewatched,
                    rewatch_value = excluded.rewatch_value,
                    tag_count = excluded.tag_count,
                    has_comments = excluded.has_comments,
                    node_json = excluded.node_json,
                    list_status_json = excluded.list_status_json,
                    raw_json = excluded.raw_json,
                    refresh_run_id = excluded.refresh_run_id,
                    refresh_generation = excluded.refresh_generation,
                    fetched_at = excluded.fetched_at,
                    last_seen_at = excluded.last_seen_at,
                    updated_at = CURRENT_TIMESTAMP
                WHERE mal_user_anime_list_cache.refresh_generation <= excluded.refresh_generation
                """,
                prepared,
            )
        changed_rows = conn.total_changes - changes_before
        preserved_absent = conn.execute(
            "SELECT COUNT(*) AS n FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
            (generation_id,),
        ).fetchone()["n"]
        conn.execute(
            """
            UPDATE mal_user_anime_list_refresh_generations
            SET items = items + ?,
                upserted = upserted + ?,
                preserved_absent = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE generation = ? AND refresh_run_id = ? AND status = 'active'
            """,
            (len(prepared), int(changed_rows), int(preserved_absent or 0), generation_id, run_id),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    by_status, scored, unscored, preference_counts = _summarize_prepared_mal_user_list_rows(prepared)
    return MalUserAnimeListRefreshSummary(
        status="upserted",
        refresh_run_id=run_id,
        generation=generation_id,
        items=len(prepared),
        upserted=int(changed_rows),
        preserved_absent=int(preserved_absent or 0),
        scored=scored,
        unscored=unscored,
        preference_counts=preference_counts,
        by_status=by_status,
        partial=True,
    )


def finish_mal_user_anime_list_cache_refresh(
    db_path: Path,
    *,
    items: Iterable[dict[str, Any]],
    refresh_run_id: str,
    generation: int,
    fetched_at: str,
    proven_complete: bool,
    delete_absent: bool = False,
) -> MalUserAnimeListRefreshSummary:
    """Atomically upsert rows and terminalize a refresh generation."""
    run_id = _coerce_mal_user_list_refresh_run_id(refresh_run_id)
    generation_id = _coerce_mal_user_list_refresh_generation(generation)
    fetched = str(fetched_at).strip()
    if not fetched:
        raise ValueError("fetched_at is required")
    if delete_absent and not proven_complete:
        raise ValueError("delete_absent requires proven_complete=True")
    prepared: list[tuple[Any, ...]] = []
    seen_ids: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        row = _prepare_mal_user_list_cache_item(
            item,
            refresh_run_id=run_id,
            generation=generation_id,
            fetched_at=fetched,
        )
        if row is None:
            continue
        mal_anime_id = int(row[0])
        if mal_anime_id in seen_ids:
            continue
        seen_ids.add(mal_anime_id)
        prepared.append(row)
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        lifecycle = _require_active_mal_user_list_refresh_generation(
            conn,
            refresh_run_id=run_id,
            generation=generation_id,
            operation="finish refresh",
        )
        _require_current_mal_user_list_refresh_generation(
            conn,
            lifecycle,
            operation="finish refresh",
        )
        changes_before = conn.total_changes
        if prepared:
            conn.executemany(
                """
                INSERT INTO mal_user_anime_list_cache (
                    mal_anime_id, title, list_status, user_score, num_episodes_watched,
                    start_date, finish_date, list_updated_at, priority, is_rewatching,
                    num_times_rewatched, rewatch_value, tag_count, has_comments,
                    node_json, list_status_json, raw_json, refresh_run_id,
                    refresh_generation, fetched_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mal_anime_id) DO UPDATE SET
                    title = excluded.title,
                    list_status = excluded.list_status,
                    user_score = excluded.user_score,
                    num_episodes_watched = excluded.num_episodes_watched,
                    start_date = excluded.start_date,
                    finish_date = excluded.finish_date,
                    list_updated_at = excluded.list_updated_at,
                    priority = excluded.priority,
                    is_rewatching = excluded.is_rewatching,
                    num_times_rewatched = excluded.num_times_rewatched,
                    rewatch_value = excluded.rewatch_value,
                    tag_count = excluded.tag_count,
                    has_comments = excluded.has_comments,
                    node_json = excluded.node_json,
                    list_status_json = excluded.list_status_json,
                    raw_json = excluded.raw_json,
                    refresh_run_id = excluded.refresh_run_id,
                    refresh_generation = excluded.refresh_generation,
                    fetched_at = excluded.fetched_at,
                    last_seen_at = excluded.last_seen_at,
                    updated_at = CURRENT_TIMESTAMP
                WHERE mal_user_anime_list_cache.refresh_generation <= excluded.refresh_generation
                """,
                prepared,
            )
        changed_rows = conn.total_changes - changes_before
        current = conn.execute(
            """
            SELECT
                list_status,
                user_score,
                priority,
                is_rewatching,
                num_times_rewatched,
                rewatch_value,
                tag_count,
                has_comments
            FROM mal_user_anime_list_cache
            WHERE refresh_generation = ? AND refresh_run_id = ?
            """,
            (generation_id, run_id),
        ).fetchall()
        pruned = 0
        if delete_absent:
            pruned = conn.execute(
                "DELETE FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
                (generation_id,),
            ).rowcount
        preserved_absent = conn.execute(
            "SELECT COUNT(*) AS n FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
            (generation_id,),
        ).fetchone()["n"]
        lifecycle_status = "completed" if proven_complete else "partial"
        conn.execute(
            """
            UPDATE mal_user_anime_list_refresh_generations
            SET status = ?,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                items = items + ?,
                upserted = upserted + ?,
                pruned = ?,
                preserved_absent = ?
            WHERE generation = ? AND refresh_run_id = ? AND status = 'active'
            """,
            (
                lifecycle_status,
                len(prepared),
                int(changed_rows),
                int(pruned or 0),
                int(preserved_absent or 0),
                generation_id,
                run_id,
            ),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    by_status, scored, unscored, preference_counts = _summarize_mal_user_list_refresh_rows(current)
    return MalUserAnimeListRefreshSummary(
        status="ok" if proven_complete else "partial",
        refresh_run_id=run_id,
        generation=generation_id,
        items=len(prepared),
        upserted=int(changed_rows),
        pruned=int(pruned or 0),
        preserved_absent=int(preserved_absent or 0),
        scored=scored,
        unscored=unscored,
        preference_counts=preference_counts,
        by_status=by_status,
        partial=not proven_complete,
    )


def finalize_mal_user_anime_list_cache_refresh(
    db_path: Path,
    *,
    refresh_run_id: str,
    generation: int,
    proven_complete: bool,
    delete_absent: bool = False,
) -> MalUserAnimeListRefreshSummary:
    """Finalize a refresh generation; absent rows are deleted only with explicit proof."""
    run_id = _coerce_mal_user_list_refresh_run_id(refresh_run_id)
    generation_id = _coerce_mal_user_list_refresh_generation(generation)
    if delete_absent and not proven_complete:
        raise ValueError("delete_absent requires proven_complete=True")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        lifecycle = _require_active_mal_user_list_refresh_generation(
            conn,
            refresh_run_id=run_id,
            generation=generation_id,
            operation="finalize refresh",
        )
        _require_current_mal_user_list_refresh_generation(
            conn,
            lifecycle,
            operation="finalize refresh",
        )
        current = conn.execute(
            """
            SELECT
                list_status,
                user_score,
                priority,
                is_rewatching,
                num_times_rewatched,
                rewatch_value,
                tag_count,
                has_comments
            FROM mal_user_anime_list_cache
            WHERE refresh_generation = ? AND refresh_run_id = ?
            """,
            (generation_id, run_id),
        ).fetchall()
        pruned = 0
        if delete_absent:
            pruned = conn.execute(
                "DELETE FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
                (generation_id,),
            ).rowcount
        preserved_absent = conn.execute(
            "SELECT COUNT(*) AS n FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
            (generation_id,),
        ).fetchone()["n"]
        lifecycle_status = "completed" if proven_complete else "partial"
        conn.execute(
            """
            UPDATE mal_user_anime_list_refresh_generations
            SET status = ?,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                pruned = ?,
                preserved_absent = ?
            WHERE generation = ? AND refresh_run_id = ? AND status = 'active'
            """,
            (lifecycle_status, int(pruned or 0), int(preserved_absent or 0), generation_id, run_id),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    by_status, scored, unscored, preference_counts = _summarize_mal_user_list_refresh_rows(current)
    return MalUserAnimeListRefreshSummary(
        status="ok" if proven_complete else "partial",
        refresh_run_id=run_id,
        generation=generation_id,
        items=len(current),
        upserted=len(current),
        pruned=int(pruned or 0),
        preserved_absent=int(preserved_absent or 0),
        scored=scored,
        unscored=unscored,
        preference_counts=preference_counts,
        by_status=by_status,
        partial=not proven_complete,
    )


def abort_mal_user_anime_list_cache_refresh(
    db_path: Path,
    *,
    refresh_run_id: str,
    generation: int,
    error: str | None = None,
) -> MalUserAnimeListRefreshSummary:
    run_id = _coerce_mal_user_list_refresh_run_id(refresh_run_id)
    generation_id = _coerce_mal_user_list_refresh_generation(generation)
    bounded_error = _bounded_mal_user_list_refresh_error(error)
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        lifecycle = _require_active_mal_user_list_refresh_generation(
            conn,
            refresh_run_id=run_id,
            generation=generation_id,
            operation="abort refresh",
        )
        _require_current_mal_user_list_refresh_generation(
            conn,
            lifecycle,
            operation="abort refresh",
        )
        current = conn.execute(
            """
            SELECT
                list_status,
                user_score,
                priority,
                is_rewatching,
                num_times_rewatched,
                rewatch_value,
                tag_count,
                has_comments
            FROM mal_user_anime_list_cache
            WHERE refresh_generation = ? AND refresh_run_id = ?
            """,
            (generation_id, run_id),
        ).fetchall()
        preserved_absent = conn.execute(
            "SELECT COUNT(*) AS n FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
            (generation_id,),
        ).fetchone()["n"]
        conn.execute(
            """
            UPDATE mal_user_anime_list_refresh_generations
            SET status = 'failed',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                error = ?,
                preserved_absent = ?
            WHERE generation = ? AND refresh_run_id = ? AND status = 'active'
            """,
            (bounded_error, int(preserved_absent or 0), generation_id, run_id),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    by_status, scored, unscored, preference_counts = _summarize_mal_user_list_refresh_rows(current)
    return MalUserAnimeListRefreshSummary(
        status="aborted",
        refresh_run_id=run_id,
        generation=generation_id,
        items=len(current),
        upserted=len(current),
        preserved_absent=int(preserved_absent or 0),
        scored=scored,
        unscored=unscored,
        preference_counts=preference_counts,
        by_status=by_status,
        partial=True,
        error=bounded_error,
    )


def replace_mal_user_anime_list_cache_generation(
    db_path: Path,
    *,
    items: Iterable[dict[str, Any]],
    refresh_run_id: str,
    fetched_at: str,
    prune_absent: bool = False,
) -> MalUserAnimeListRefreshSummary:
    """
    Compatibility helper for callers that have already collected a refresh.

    The default is intentionally non-pruning.  Passing prune_absent=True performs
    the explicit proven-complete finalize step required before absent rows can be
    deleted.
    """
    refresh = begin_mal_user_anime_list_cache_refresh(
        db_path,
        refresh_run_id=refresh_run_id,
        fetched_at=fetched_at,
    )
    upsert = upsert_mal_user_anime_list_cache_generation(
        db_path,
        items=items,
        refresh_run_id=refresh.refresh_run_id,
        generation=refresh.generation,
        fetched_at=refresh.fetched_at,
    )
    finalized = finalize_mal_user_anime_list_cache_refresh(
        db_path,
        refresh_run_id=refresh.refresh_run_id,
        generation=refresh.generation,
        proven_complete=True,
        delete_absent=bool(prune_absent),
    )
    finalized.pages = upsert.pages
    finalized.items = upsert.items
    finalized.upserted = upsert.upserted
    return finalized


def get_mal_user_anime_list_cache(db_path: Path, mal_anime_id: int) -> MalUserAnimeListCacheEntry | None:
    conn = connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM mal_user_anime_list_cache WHERE mal_anime_id = ?",
            (int(mal_anime_id),),
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else _mal_user_list_entry_from_row(row)


def reconcile_mal_user_state_after_write(
    db_path: Path,
    *,
    mal_anime_id: int,
    list_status: dict[str, Any],
) -> None:
    """Remove stale detail state and reconcile an existing @me-list cache row.

    MAL's update response is the committed user-list status.  Detail cache rows
    mix long-lived catalog metadata with that mutable status, so invalidation is
    safer than extending their lifetime with a partial update response.
    """
    anime_id = int(mal_anime_id)
    committed = dict(list_status)
    with connect(db_path) as conn:
        conn.execute(
            """
            DELETE FROM mal_anime_detail_cache
            WHERE mal_anime_id = ?
              AND (',' || fields_key || ',') LIKE '%,my_list_status,%'
            """,
            (anime_id,),
        )
        row = conn.execute(
            "SELECT * FROM mal_user_anime_list_cache WHERE mal_anime_id = ?",
            (anime_id,),
        ).fetchone()
        if row is not None:
            prior_status = _load_json_value(row["list_status_json"], {})
            if not isinstance(prior_status, dict):
                prior_status = {}
            merged_status = {**prior_status, **committed}
            raw = _load_json_value(row["raw_json"], {})
            if not isinstance(raw, dict):
                raw = {}
            raw["list_status"] = merged_status
            conn.execute(
                """
                UPDATE mal_user_anime_list_cache
                SET list_status = ?, user_score = ?, num_episodes_watched = ?,
                    start_date = ?, finish_date = ?, list_updated_at = ?,
                    list_status_json = ?, raw_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE mal_anime_id = ?
                """,
                (
                    _normalize_mal_user_list_status(merged_status.get("status")),
                    _clamp_optional_int(merged_status.get("score"), minimum=0, maximum=10),
                    _clamp_optional_int(merged_status.get("num_episodes_watched"), minimum=0),
                    merged_status.get("start_date") if isinstance(merged_status.get("start_date"), str) else None,
                    merged_status.get("finish_date") if isinstance(merged_status.get("finish_date"), str) else None,
                    merged_status.get("updated_at") if isinstance(merged_status.get("updated_at"), str) else row["list_updated_at"],
                    json.dumps(merged_status, ensure_ascii=False, sort_keys=True),
                    json.dumps(raw, ensure_ascii=False, sort_keys=True),
                    anime_id,
                ),
            )
        conn.commit()


def list_mal_user_anime_list_cache(db_path: Path, *, statuses: Iterable[str] | None = None) -> list[MalUserAnimeListCacheEntry]:
    params: list[Any] = []
    where = ""
    if statuses is not None:
        normalized = sorted({status for status in (_normalize_mal_user_list_status(item) for item in statuses) if status})
        if normalized:
            where = f"WHERE list_status IN ({', '.join('?' for _ in normalized)})"
            params.extend(normalized)
        else:
            where = "WHERE 0"
    conn = connect(db_path)
    try:
        rows = conn.execute(
            f"""
            SELECT * FROM mal_user_anime_list_cache
            {where}
            ORDER BY mal_anime_id ASC
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    return [_mal_user_list_entry_from_row(row) for row in rows]


def count_mal_user_anime_list_cache(db_path: Path, *, statuses: Iterable[str] | None = None) -> int:
    params: list[Any] = []
    where = ""
    if statuses is not None:
        normalized = sorted({status for status in (_normalize_mal_user_list_status(item) for item in statuses) if status})
        if normalized:
            where = f"WHERE list_status IN ({', '.join('?' for _ in normalized)})"
            params.extend(normalized)
        else:
            return 0
    conn = connect(db_path)
    try:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM mal_user_anime_list_cache {where}", params).fetchone()
    finally:
        conn.close()
    return int(row["n"] or 0)


def get_mal_user_anime_list_cache_map(db_path: Path) -> dict[int, MalUserAnimeListCacheEntry]:
    return {entry.mal_anime_id: entry for entry in list_mal_user_anime_list_cache(db_path)}


def summarize_mal_user_anime_list_cache(db_path: Path) -> dict[str, Any]:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT list_status, COUNT(*) AS n, SUM(CASE WHEN user_score IS NOT NULL AND user_score > 0 THEN 1 ELSE 0 END) AS scored
            FROM mal_user_anime_list_cache
            GROUP BY list_status
            ORDER BY list_status ASC
            """
        ).fetchall()
        freshness = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                MAX(refresh_generation) AS generation,
                MAX(last_seen_at) AS newest_seen_at,
                MIN(last_seen_at) AS oldest_seen_at,
                SUM(CASE WHEN priority IS NOT NULL THEN 1 ELSE 0 END) AS with_priority,
                SUM(CASE WHEN is_rewatching IS NOT NULL THEN 1 ELSE 0 END) AS with_rewatching,
                SUM(CASE WHEN num_times_rewatched IS NOT NULL THEN 1 ELSE 0 END) AS with_num_times_rewatched,
                SUM(CASE WHEN rewatch_value IS NOT NULL THEN 1 ELSE 0 END) AS with_rewatch_value,
                SUM(CASE WHEN tag_count > 0 THEN 1 ELSE 0 END) AS with_tags,
                SUM(CASE WHEN has_comments > 0 THEN 1 ELSE 0 END) AS with_comments
            FROM mal_user_anime_list_cache
            """
        ).fetchone()
    finally:
        conn.close()
    by_status = {str(row["list_status"] or "unknown"): int(row["n"] or 0) for row in rows}
    scored = sum(int(row["scored"] or 0) for row in rows)
    total = int(freshness["total"] or 0)
    return {
        "total": total,
        "by_status": by_status,
        "scored": scored,
        "unscored": max(total - scored, 0),
        "generation": None if freshness["generation"] is None else int(freshness["generation"]),
        "newest_seen_at": freshness["newest_seen_at"],
        "oldest_seen_at": freshness["oldest_seen_at"],
        "preference_counts": {
            "with_priority": int(freshness["with_priority"] or 0),
            "with_rewatching": int(freshness["with_rewatching"] or 0),
            "with_num_times_rewatched": int(freshness["with_num_times_rewatched"] or 0),
            "with_rewatch_value": int(freshness["with_rewatch_value"] or 0),
            "with_tags": int(freshness["with_tags"] or 0),
            "with_comments": int(freshness["with_comments"] or 0),
        },
    }


def _my_list_status_from_cache_entry(entry: MalUserAnimeListCacheEntry) -> dict[str, Any]:
    payload = {
        key: value
        for key, value in entry.list_status_raw.items()
        if key not in {"tags", "comments"}
    }
    if entry.list_status:
        payload["status"] = entry.list_status
    if entry.user_score is not None:
        payload["score"] = entry.user_score
    if entry.num_episodes_watched is not None:
        payload["num_episodes_watched"] = entry.num_episodes_watched
    if entry.start_date:
        payload["start_date"] = entry.start_date
    if entry.finish_date:
        payload["finish_date"] = entry.finish_date
    if entry.list_updated_at:
        payload["updated_at"] = entry.list_updated_at
    if entry.priority is not None:
        payload["priority"] = entry.priority
    if entry.is_rewatching is not None:
        payload["is_rewatching"] = entry.is_rewatching
    if entry.num_times_rewatched is not None:
        payload["num_times_rewatched"] = entry.num_times_rewatched
    if entry.rewatch_value is not None:
        payload["rewatch_value"] = entry.rewatch_value
    if entry.tag_count > 0:
        payload["tag_count"] = entry.tag_count
    if entry.has_comments:
        payload["has_comments"] = True
    return payload


def merge_mal_user_anime_list_cache_into_metadata(db_path: Path, *, metadata_fetched_at_for_new_rows: str = "1970-01-01 00:00:00") -> int:
    entries = list_mal_user_anime_list_cache(db_path)
    if not entries:
        return 0
    changed = 0
    conn = connect(db_path)
    try:
        with conn:
            for entry in entries:
                my_list_status = _my_list_status_from_cache_entry(entry)
                row = conn.execute(
                    "SELECT raw_json FROM mal_anime_metadata WHERE mal_anime_id = ?",
                    (entry.mal_anime_id,),
                ).fetchone()
                if row is None:
                    raw = {
                        "id": entry.mal_anime_id,
                        "title": entry.title,
                        "my_list_status": my_list_status,
                        "mal_user_anime_list_cache": {"refresh_run_id": entry.refresh_run_id, "refresh_generation": entry.refresh_generation},
                    }
                    conn.execute(
                        """
                        INSERT INTO mal_anime_metadata (
                            mal_anime_id, title, title_english, title_japanese,
                            alternative_titles_json, media_type, status, num_episodes,
                            mean, popularity, start_season_json, raw_json, fetched_at, updated_at
                        ) VALUES (?, ?, NULL, NULL, '[]', NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            entry.mal_anime_id,
                            entry.title,
                            json.dumps(raw, ensure_ascii=False, sort_keys=True),
                            metadata_fetched_at_for_new_rows,
                        ),
                    )
                    changed += 1
                    continue
                raw = json.loads(row["raw_json"] or "{}")
                if not isinstance(raw, dict):
                    raw = {}
                raw["my_list_status"] = my_list_status
                raw["mal_user_anime_list_cache"] = {"refresh_run_id": entry.refresh_run_id, "refresh_generation": entry.refresh_generation}
                conn.execute(
                    """
                    UPDATE mal_anime_metadata
                    SET raw_json = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE mal_anime_id = ?
                    """,
                    (json.dumps(raw, ensure_ascii=False, sort_keys=True), entry.mal_anime_id),
                )
                changed += 1
    finally:
        conn.close()
    return changed

def replace_mal_anime_relations(db_path: Path, *, mal_anime_id: int, relations: list[dict[str, Any]]) -> None:
    with connect(db_path) as conn:
        conn.execute("DELETE FROM mal_anime_relations WHERE mal_anime_id = ?", (int(mal_anime_id),))
        for relation in relations:
            conn.execute(
                """
                INSERT INTO mal_anime_relations (
                    mal_anime_id,
                    related_mal_anime_id,
                    relation_type,
                    relation_type_formatted,
                    related_title,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(mal_anime_id),
                    int(relation["related_mal_anime_id"]),
                    relation["relation_type"],
                    relation.get("relation_type_formatted"),
                    relation.get("related_title"),
                    json.dumps(relation["raw"], ensure_ascii=False, sort_keys=True),
                ),
            )
        conn.commit()


def get_mal_anime_metadata_map(db_path: Path) -> dict[int, MalAnimeMetadata]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                mal_anime_id,
                title,
                title_english,
                title_japanese,
                alternative_titles_json,
                media_type,
                status,
                num_episodes,
                mean,
                popularity,
                start_season_json,
                rank,
                num_list_users,
                num_scoring_users,
                rating,
                average_episode_duration,
                start_date,
                end_date,
                broadcast_day,
                broadcast_time,
                broadcast_timezone,
                nsfw,
                raw_json,
                fetched_at,
                updated_at
            FROM mal_anime_metadata
            """
        ).fetchall()
    result: dict[int, MalAnimeMetadata] = {}
    for row in rows:
        raw = _load_json_value(row["raw_json"], {})
        raw = raw if isinstance(raw, dict) else {}
        broadcast = _broadcast_payload(raw)
        alternative_titles = _load_json_value(row["alternative_titles_json"], [])
        if not isinstance(alternative_titles, list):
            alternative_titles = []
        result[int(row["mal_anime_id"])] = MalAnimeMetadata(
            mal_anime_id=int(row["mal_anime_id"]),
            title=str(row["title"]),
            title_english=row["title_english"],
            title_japanese=row["title_japanese"],
            alternative_titles=alternative_titles,
            media_type=row["media_type"],
            status=row["status"],
            num_episodes=None if row["num_episodes"] is None else int(row["num_episodes"]),
            mean=None if row["mean"] is None else float(row["mean"]),
            popularity=None if row["popularity"] is None else int(row["popularity"]),
            start_season=_load_json_value(row["start_season_json"], None) if row["start_season_json"] else None,
            raw=raw,
            fetched_at=str(row["fetched_at"]),
            updated_at=str(row["updated_at"]),
            rank=_coerce_positive_int(row["rank"]) or _coerce_positive_int(raw.get("rank")),
            num_list_users=(
                _coerce_nonnegative_int(row["num_list_users"])
                if row["num_list_users"] is not None
                else _coerce_nonnegative_int(raw.get("num_list_users"))
            ),
            num_scoring_users=(
                _coerce_nonnegative_int(row["num_scoring_users"])
                if row["num_scoring_users"] is not None
                else _coerce_nonnegative_int(raw.get("num_scoring_users"))
            ),
            rating=_coerce_non_empty_text(row["rating"], lowercase=True) or _coerce_non_empty_text(raw.get("rating"), lowercase=True),
            average_episode_duration=(
                _coerce_positive_int(row["average_episode_duration"])
                if row["average_episode_duration"] is not None
                else _coerce_positive_int(raw.get("average_episode_duration"))
            ),
            start_date=_coerce_non_empty_text(row["start_date"]) or _coerce_non_empty_text(raw.get("start_date")),
            end_date=_coerce_non_empty_text(row["end_date"]) or _coerce_non_empty_text(raw.get("end_date")),
            broadcast_day=(
                _coerce_non_empty_text(row["broadcast_day"], lowercase=True)
                or _coerce_non_empty_text(broadcast.get("day_of_the_week"), lowercase=True)
            ),
            broadcast_time=(
                _coerce_non_empty_text(row["broadcast_time"])
                or _coerce_non_empty_text(broadcast.get("start_time"))
            ),
            broadcast_timezone=(
                _coerce_non_empty_text(row["broadcast_timezone"])
                or _coerce_non_empty_text(broadcast.get("timezone"))
            ),
            nsfw=_coerce_non_empty_text(row["nsfw"], lowercase=True) or _coerce_non_empty_text(raw.get("nsfw"), lowercase=True),
        )
    return result


def get_mal_anime_relations_map(db_path: Path) -> dict[int, list[MalAnimeRelation]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                mal_anime_id,
                related_mal_anime_id,
                relation_type,
                relation_type_formatted,
                related_title,
                raw_json,
                fetched_at
            FROM mal_anime_relations
            ORDER BY mal_anime_id ASC, related_mal_anime_id ASC
            """
        ).fetchall()
    result: dict[int, list[MalAnimeRelation]] = {}
    for row in rows:
        result.setdefault(int(row["mal_anime_id"]), []).append(
            MalAnimeRelation(
                mal_anime_id=int(row["mal_anime_id"]),
                related_mal_anime_id=int(row["related_mal_anime_id"]),
                relation_type=str(row["relation_type"]),
                relation_type_formatted=row["relation_type_formatted"],
                related_title=row["related_title"],
                raw=json.loads(row["raw_json"]),
                fetched_at=str(row["fetched_at"]),
            )
        )
    return result


MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL = "official_detail"
MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS = "public_userrecs"


def replace_mal_recommendation_edges(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    hop_distance: int,
    edges: list[dict[str, Any]],
    source_type: str = MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
    complete: bool = False,
    pages_fetched: int | None = None,
    source_url: str | None = None,
    allow_complete_downgrade: bool = False,
) -> bool:
    normalized_source_type = str(source_type or MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL).strip() or MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL
    normalized_complete = bool(complete)
    with connect(db_path) as conn:
        existing_status = conn.execute(
            """
            SELECT source_type, is_complete, status
            FROM mal_recommendation_harvest_status
            WHERE source_mal_anime_id = ?
            """,
            (int(source_mal_anime_id),),
        ).fetchone()
        if (
            not normalized_complete
            and not allow_complete_downgrade
            and existing_status is not None
            and int(existing_status["is_complete"] or 0) == 1
            and str(existing_status["source_type"] or "") == MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS
        ):
            return False
        conn.execute(
            "DELETE FROM mal_anime_recommendations WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'",
            (int(source_mal_anime_id),),
        )
        for edge in edges:
            raw = edge.get("raw") if isinstance(edge.get("raw"), dict) else {}
            provenance = edge.get("provenance") if isinstance(edge.get("provenance"), dict) else {}
            conn.execute(
                """
                INSERT INTO mal_anime_recommendations (
                    source_mal_anime_id,
                    target_mal_anime_id,
                    target_title,
                    num_recommendations,
                    hop_distance,
                    source_kind,
                    raw_json,
                    harvest_source,
                    complete_harvest,
                    provenance_json
                ) VALUES (?, ?, ?, ?, ?, 'mal_recommendation', ?, ?, ?, ?)
                """,
                (
                    int(source_mal_anime_id),
                    int(edge["target_mal_anime_id"]),
                    edge.get("target_title"),
                    edge.get("num_recommendations"),
                    int(hop_distance),
                    json.dumps(raw, ensure_ascii=False, sort_keys=True),
                    normalized_source_type,
                    1 if normalized_complete else 0,
                    json.dumps(provenance, ensure_ascii=False, sort_keys=True),
                ),
            )
        conn.execute(
            """
            INSERT INTO mal_recommendation_harvest_status (
                source_mal_anime_id,
                status,
                num_edges,
                fetched_at,
                source_type,
                is_complete,
                pages_fetched,
                source_url,
                last_attempted_at,
                last_error,
                failure_count,
                updated_at
            )
            VALUES (?, 'fetched', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(source_mal_anime_id) DO UPDATE SET
                status = excluded.status,
                num_edges = excluded.num_edges,
                fetched_at = excluded.fetched_at,
                source_type = excluded.source_type,
                is_complete = excluded.is_complete,
                pages_fetched = excluded.pages_fetched,
                source_url = excluded.source_url,
                last_attempted_at = excluded.last_attempted_at,
                last_error = NULL,
                failure_count = 0,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(source_mal_anime_id),
                len(edges),
                normalized_source_type,
                1 if normalized_complete else 0,
                max(0, int(pages_fetched or 0)),
                source_url,
            ),
        )
        conn.commit()
    return True


def record_mal_recommendation_harvest_failure(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    source_type: str = MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
    error: str | None = None,
    pages_fetched: int | None = None,
    source_url: str | None = None,
) -> None:
    normalized_source_type = str(source_type or MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL).strip() or MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL
    normalized_error = None
    if error is not None:
        normalized_error = str(error).strip()[:1000] or None
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO mal_recommendation_harvest_status (
                source_mal_anime_id,
                status,
                num_edges,
                fetched_at,
                source_type,
                is_complete,
                pages_fetched,
                source_url,
                last_attempted_at,
                last_error,
                failure_count,
                updated_at
            )
            VALUES (
                ?,
                'failed',
                COALESCE((SELECT COUNT(*) FROM mal_anime_recommendations WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'), 0),
                CURRENT_TIMESTAMP,
                ?,
                0,
                ?,
                ?,
                CURRENT_TIMESTAMP,
                ?,
                1,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT(source_mal_anime_id) DO UPDATE SET
                status = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.status
                    ELSE 'failed'
                END,
                num_edges = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.num_edges
                    ELSE COALESCE((SELECT COUNT(*) FROM mal_anime_recommendations WHERE source_mal_anime_id = excluded.source_mal_anime_id AND source_kind = 'mal_recommendation'), mal_recommendation_harvest_status.num_edges)
                END,
                source_type = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.source_type
                    ELSE excluded.source_type
                END,
                is_complete = mal_recommendation_harvest_status.is_complete,
                pages_fetched = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.pages_fetched
                    ELSE excluded.pages_fetched
                END,
                source_url = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.source_url
                    ELSE COALESCE(excluded.source_url, mal_recommendation_harvest_status.source_url)
                END,
                last_attempted_at = CURRENT_TIMESTAMP,
                last_error = excluded.last_error,
                failure_count = mal_recommendation_harvest_status.failure_count + 1,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(source_mal_anime_id),
                int(source_mal_anime_id),
                normalized_source_type,
                max(0, int(pages_fetched or 0)),
                source_url,
                normalized_error,
            ),
        )
        conn.commit()


def record_mal_recommendation_harvest_attempt_error(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    source_type: str = MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
    error: str | None = None,
    pages_fetched: int | None = None,
    source_url: str | None = None,
) -> None:
    """Record a transient harvest attempt error without demoting fetched rows.

    Resumable public-userrecs fetch/parse failures keep an open staged
    generation and should not increment the authoritative harvest failure_count
    for a previously published complete graph. They still update last_attempted
    metadata so operators can see the latest transient blocker.
    """
    normalized_source_type = str(source_type or MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL).strip() or MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL
    normalized_error = None if error is None else str(error).strip()[:1000] or None
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO mal_recommendation_harvest_status (
                source_mal_anime_id,
                status,
                num_edges,
                fetched_at,
                source_type,
                is_complete,
                pages_fetched,
                source_url,
                last_attempted_at,
                last_error,
                failure_count,
                updated_at
            )
            VALUES (
                ?,
                'failed',
                COALESCE((SELECT COUNT(*) FROM mal_anime_recommendations WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'), 0),
                CURRENT_TIMESTAMP,
                ?,
                0,
                ?,
                ?,
                CURRENT_TIMESTAMP,
                ?,
                0,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT(source_mal_anime_id) DO UPDATE SET
                status = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.status
                    ELSE 'failed'
                END,
                num_edges = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.num_edges
                    ELSE COALESCE((SELECT COUNT(*) FROM mal_anime_recommendations WHERE source_mal_anime_id = excluded.source_mal_anime_id AND source_kind = 'mal_recommendation'), mal_recommendation_harvest_status.num_edges)
                END,
                source_type = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.source_type
                    ELSE excluded.source_type
                END,
                is_complete = mal_recommendation_harvest_status.is_complete,
                pages_fetched = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.pages_fetched
                    ELSE excluded.pages_fetched
                END,
                source_url = CASE
                    WHEN mal_recommendation_harvest_status.is_complete = 1 THEN mal_recommendation_harvest_status.source_url
                    ELSE COALESCE(excluded.source_url, mal_recommendation_harvest_status.source_url)
                END,
                last_attempted_at = CURRENT_TIMESTAMP,
                last_error = excluded.last_error,
                failure_count = mal_recommendation_harvest_status.failure_count,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(source_mal_anime_id),
                int(source_mal_anime_id),
                normalized_source_type,
                max(0, int(pages_fetched or 0)),
                source_url,
                normalized_error,
            ),
        )
        conn.commit()


def replace_mal_public_userrecs_recommendation_edges(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    edges: list[dict[str, Any]],
    pages_fetched: int,
    source_url: str | None = None,
) -> bool:
    return replace_mal_recommendation_edges(
        db_path,
        source_mal_anime_id=source_mal_anime_id,
        hop_distance=1,
        edges=edges,
        source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
        complete=True,
        pages_fetched=pages_fetched,
        source_url=source_url,
    )


_PUBLIC_USERRECS_OPEN_GENERATION_STATUSES = frozenset({"active", "paused", "ready"})
_PUBLIC_USERRECS_MUTABLE_GENERATION_STATUSES = frozenset({"active", "paused"})
_PUBLIC_USERRECS_EVENT_LIMIT_PER_SOURCE = 200
_PUBLIC_USERRECS_RETAINED_FIELDS = ["target_mal_anime_id", "target_title", "num_recommendations"]
PUBLIC_USERRECS_LOGIC_VERSION = "public-userrecs-snapshot-v2"
PUBLIC_USERRECS_MAX_DRIFT_RESTARTS = 3
PUBLIC_USERRECS_CLAIM_SECONDS = 15 * 60
_PUBLIC_USERRECS_QUEUE_CLASS_PRIORITY = {
    "never_started": 0,
    "resumable": 1,
    "retry_due": 2,
    "refresh_due": 3,
    "fresh": 4,
    "quarantined": 5,
}
_UNSET = object()


def _public_userrecs_safe_text(value: Any, *, max_length: int = 1000) -> str | None:
    text = _coerce_non_empty_text(value)
    return None if text is None else text[:max(1, int(max_length))]


def _public_userrecs_sanitized_anchor(anchor: dict[str, Any] | None) -> dict[str, Any]:
    """Persist only resumability/coherence anchor fields, never raw public prose."""
    if not isinstance(anchor, dict):
        return {}
    sanitized: dict[str, Any] = {}
    for key in ("first_target_mal_anime_id", "last_target_mal_anime_id"):
        target_id = _coerce_mal_anime_id(anchor.get(key))
        if target_id is not None:
            sanitized[key] = target_id
    target_ids = anchor.get("target_mal_anime_ids")
    if isinstance(target_ids, list):
        sanitized_ids = [target_id for value in target_ids if (target_id := _coerce_mal_anime_id(value)) is not None]
        if sanitized_ids:
            sanitized["target_mal_anime_ids"] = sanitized_ids[:100]
    for key in ("first_target_title", "last_target_title"):
        text = _public_userrecs_safe_text(anchor.get(key), max_length=500)
        if text is not None:
            sanitized[key] = text
    return sanitized


def _public_userrecs_generation_from_row(row: sqlite3.Row) -> MalPublicUserRecsCrawlGeneration:
    keys = set(row.keys())
    return MalPublicUserRecsCrawlGeneration(
        generation_id=int(row["generation_id"]),
        source_mal_anime_id=int(row["source_mal_anime_id"]),
        source_title=row["source_title"],
        source_url=row["source_url"],
        status=str(row["status"]),
        cursor_url=row["cursor_url"],
        pages_fetched=int(row["pages_fetched"] or 0),
        staged_edge_count=int(row["staged_edge_count"] or 0),
        last_page_url=row["last_page_url"],
        last_page_fingerprint=row["last_page_fingerprint"],
        last_error=row["last_error"],
        started_at=str(row["started_at"]),
        completed_at=row["completed_at"],
        published_at=row["published_at"],
        discarded_at=row["discarded_at"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        logic_version=str(row["logic_version"]) if "logic_version" in keys else "public-userrecs-snapshot-v2",
        generation_key=row["generation_key"] if "generation_key" in keys else None,
        attempt_count=int(row["attempt_count"] or 0) if "attempt_count" in keys else 0,
        restart_count=int(row["restart_count"] or 0) if "restart_count" in keys else 0,
        drift_count=int(row["drift_count"] or 0) if "drift_count" in keys else 0,
        next_retry_at=row["next_retry_at"] if "next_retry_at" in keys else None,
        retry_class=row["retry_class"] if "retry_class" in keys else None,
        first_page_revalidated_at=row["first_page_revalidated_at"] if "first_page_revalidated_at" in keys else None,
        boundary_revalidated_at=row["boundary_revalidated_at"] if "boundary_revalidated_at" in keys else None,
        terminal_evidence_json=str(row["terminal_evidence_json"] or "{}") if "terminal_evidence_json" in keys else "{}",
        quarantined_at=row["quarantined_at"] if "quarantined_at" in keys else None,
        quarantine_reason=row["quarantine_reason"] if "quarantine_reason" in keys else None,
        claim_token=row["claim_token"] if "claim_token" in keys else None,
        claim_expires_at=row["claim_expires_at"] if "claim_expires_at" in keys else None,
        generation_revision=int(row["generation_revision"] or 0) if "generation_revision" in keys else 0,
        staged_revision=int(row["staged_revision"] or 0) if "staged_revision" in keys else 0,
        validated_staged_revision=(
            None if "validated_staged_revision" not in keys or row["validated_staged_revision"] is None
            else int(row["validated_staged_revision"])
        ),
        validation_fingerprint=row["validation_fingerprint"] if "validation_fingerprint" in keys else None,
        validation_page_number=int(row["validation_page_number"] or 0) if "validation_page_number" in keys else 0,
        validation_revision=(None if "validation_revision" not in keys or row["validation_revision"] is None else int(row["validation_revision"])),
        final_anchor_step=int(row["final_anchor_step"] or 0) if "final_anchor_step" in keys else 0,
        final_anchor_revision=(None if "final_anchor_revision" not in keys or row["final_anchor_revision"] is None else int(row["final_anchor_revision"])),
    )


def _public_userrecs_queue_from_row(row: sqlite3.Row) -> MalPublicUserRecsSourceQueueRow:
    return MalPublicUserRecsSourceQueueRow(
        source_mal_anime_id=int(row["source_mal_anime_id"]),
        queue_class=str(row["queue_class"]),
        eligible=bool(row["eligible"]),
        enqueued_at=str(row["enqueued_at"]),
        class_entered_at=str(row["class_entered_at"]),
        last_selected_at=row["last_selected_at"],
        selection_count=int(row["selection_count"] or 0),
        selection_sequence=int(row["selection_sequence"] or 0),
        next_retry_at=row["next_retry_at"],
        claim_token=row["claim_token"],
        claim_expires_at=row["claim_expires_at"],
        last_generation_id=None if row["last_generation_id"] is None else int(row["last_generation_id"]),
        last_outcome=row["last_outcome"],
        last_error_code=row["last_error_code"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _public_userrecs_staged_page_from_row(row: sqlite3.Row) -> MalPublicUserRecsStagedPage:
    return MalPublicUserRecsStagedPage(
        generation_id=int(row["generation_id"]),
        source_mal_anime_id=int(row["source_mal_anime_id"]),
        page_number=int(row["page_number"]),
        page_url=str(row["page_url"]),
        page_fingerprint=str(row["page_fingerprint"]),
        anchor_json=str(row["anchor_json"]),
        next_url=row["next_url"],
        edge_count=int(row["edge_count"] or 0),
        fetched_at=str(row["fetched_at"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _get_public_userrecs_generation_row(conn: sqlite3.Connection, generation_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM mal_public_userrecs_crawl_generations WHERE generation_id = ?",
        (int(generation_id),),
    ).fetchone()
    if row is None:
        raise ValueError(f"public userrecs crawl generation not found: {generation_id}")
    return row


def _record_public_userrecs_event(
    conn: sqlite3.Connection,
    *,
    generation_id: int | None,
    source_mal_anime_id: int,
    event_type: str,
    page_number: int | None = None,
    page_url: str | None = None,
    error: str | None = None,
    event_limit: int = _PUBLIC_USERRECS_EVENT_LIMIT_PER_SOURCE,
) -> None:
    normalized_error = None if error is None else str(error).strip()[:1000] or None
    conn.execute(
        """
        INSERT INTO mal_public_userrecs_crawl_events (
            generation_id, source_mal_anime_id, event_type, page_number, page_url, error
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            None if generation_id is None else int(generation_id),
            int(source_mal_anime_id),
            str(event_type),
            None if page_number is None else int(page_number),
            page_url,
            normalized_error,
        ),
    )
    limit = max(1, int(event_limit))
    conn.execute(
        """
        DELETE FROM mal_public_userrecs_crawl_events
        WHERE source_mal_anime_id = ?
          AND id NOT IN (
              SELECT id
              FROM mal_public_userrecs_crawl_events
              WHERE source_mal_anime_id = ?
              ORDER BY id DESC
              LIMIT ?
          )
        """,
        (int(source_mal_anime_id), int(source_mal_anime_id), limit),
    )


def _require_public_userrecs_generation_status(row: sqlite3.Row, allowed: frozenset[str], *, action: str) -> None:
    status = str(row["status"])
    if status not in allowed:
        raise ValueError(
            f"cannot {action} public userrecs generation {row['generation_id']} "
            f"while status is {status!r}"
        )


def _require_public_userrecs_claim(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    claim_token: str | None,
    expected_revision: int | None,
    action: str,
) -> None:
    """Fence orchestrated mutations to one live source claim and revision.

    Legacy/direct DB callers may omit a token only while neither the source
    queue nor generation is claimed. Once claimed, omission is fail-closed.
    """
    source_id = int(row["source_mal_anime_id"])
    queue = conn.execute(
        "SELECT claim_token, claim_expires_at, last_generation_id FROM mal_public_userrecs_source_queue WHERE source_mal_anime_id = ?",
        (source_id,),
    ).fetchone()
    row_token = row["claim_token"] if "claim_token" in row.keys() else None
    supplied = None if claim_token is None else str(claim_token).strip() or None
    if supplied is None:
        if row_token is not None or (queue is not None and queue["claim_token"] is not None):
            raise RuntimeError(f"public userrecs claim fence rejected {action}: claim token required")
        return
    if row_token != supplied or queue is None or queue["claim_token"] != supplied:
        raise RuntimeError(f"public userrecs claim fence rejected {action}: stale claim token")
    if queue["last_generation_id"] is not None and int(queue["last_generation_id"]) != int(row["generation_id"]):
        raise RuntimeError(f"public userrecs claim fence rejected {action}: generation identity changed")
    expires = row["claim_expires_at"]
    queue_expires = queue["claim_expires_at"]
    live = conn.execute(
        "SELECT datetime(?) > datetime('now') AND datetime(?) > datetime('now') AS live",
        (expires, queue_expires),
    ).fetchone()
    if live is None or not bool(live["live"]):
        raise RuntimeError(f"public userrecs claim fence rejected {action}: claim expired")
    if expected_revision is not None and int(row["generation_revision"] or 0) != int(expected_revision):
        raise RuntimeError(f"public userrecs claim fence rejected {action}: generation revision changed")


def _bump_public_userrecs_generation_revision(conn: sqlite3.Connection, generation_id: int) -> None:
    conn.execute(
        "UPDATE mal_public_userrecs_crawl_generations SET generation_revision = generation_revision + 1 WHERE generation_id = ?",
        (int(generation_id),),
    )


def get_mal_public_userrecs_generation(
    db_path: Path,
    *,
    generation_id: int,
) -> MalPublicUserRecsCrawlGeneration | None:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM mal_public_userrecs_crawl_generations WHERE generation_id = ?",
            (int(generation_id),),
        ).fetchone()
    return None if row is None else _public_userrecs_generation_from_row(row)


def get_active_mal_public_userrecs_generation(
    db_path: Path,
    *,
    source_mal_anime_id: int,
) -> MalPublicUserRecsCrawlGeneration | None:
    source_id = _coerce_mal_anime_id(source_mal_anime_id)
    if source_id is None:
        raise ValueError("source_mal_anime_id must be a positive integer")
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT *
            FROM mal_public_userrecs_crawl_generations
            WHERE source_mal_anime_id = ?
              AND status IN ('active', 'paused', 'ready')
            ORDER BY generation_id DESC
            LIMIT 1
            """,
            (source_id,),
        ).fetchone()
    return None if row is None else _public_userrecs_generation_from_row(row)


def list_active_mal_public_userrecs_generations(
    db_path: Path,
    *,
    source_mal_anime_ids: Iterable[int] | None = None,
) -> list[MalPublicUserRecsCrawlGeneration]:
    """Return open public-userrecs generations, oldest-updated first.

    Orchestration uses this to resume active/paused/ready generations before
    starting new stale-source generations so interrupted long crawls do not
    starve behind fresh candidate ranking.
    """
    source_ids: list[int] | None = None
    if source_mal_anime_ids is not None:
        source_ids = []
        for value in source_mal_anime_ids:
            source_id = _coerce_mal_anime_id(value)
            if source_id is not None and source_id not in source_ids:
                source_ids.append(source_id)
        if not source_ids:
            return []
    clauses = ["status IN ('active', 'paused', 'ready')"]
    params: list[Any] = []
    if source_ids is not None:
        placeholders = ", ".join("?" for _ in source_ids)
        clauses.append(f"source_mal_anime_id IN ({placeholders})")
        params.extend(source_ids)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM mal_public_userrecs_crawl_generations
            WHERE {' AND '.join(clauses)}
            ORDER BY updated_at ASC, generation_id ASC
            """,
            params,
        ).fetchall()
    return [_public_userrecs_generation_from_row(row) for row in rows]


def list_mal_public_userrecs_staged_pages(
    db_path: Path,
    *,
    generation_id: int,
) -> list[MalPublicUserRecsStagedPage]:
    """Return staged pages for one public-userrecs generation in page order."""
    with connect(db_path) as conn:
        rows = _public_userrecs_staged_pages(conn, int(generation_id))
    return [_public_userrecs_staged_page_from_row(row) for row in rows]


def sync_mal_public_userrecs_source_queue(
    db_path: Path,
    *,
    source_mal_anime_ids: Iterable[int],
    due_classes: dict[int, str] | None = None,
) -> list[MalPublicUserRecsSourceQueueRow]:
    """Add positive seeds to the durable queue without resetting traversal age.

    Ranking churn can update membership but cannot rewrite ``enqueued_at``,
    ``class_entered_at``, or ``last_selected_at`` for an existing source.
    """
    source_ids = sorted({value for item in source_mal_anime_ids if (value := _coerce_mal_anime_id(item)) is not None})
    if not source_ids:
        return []
    classes = due_classes or {}
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for source_id in source_ids:
            requested = str(classes.get(source_id) or "never_started")
            if requested not in _PUBLIC_USERRECS_QUEUE_CLASS_PRIORITY:
                requested = "never_started"
            existing = conn.execute(
                "SELECT queue_class FROM mal_public_userrecs_source_queue WHERE source_mal_anime_id = ?",
                (source_id,),
            ).fetchone()
            latest = conn.execute(
                """SELECT quarantined_at, quarantine_reason
                   FROM mal_public_userrecs_crawl_generations
                   WHERE source_mal_anime_id = ?
                   ORDER BY generation_id DESC LIMIT 1""",
                (source_id,),
            ).fetchone()
            if latest is not None and (latest["quarantined_at"] is not None or latest["quarantine_reason"] is not None):
                requested = "quarantined"
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO mal_public_userrecs_source_queue (source_mal_anime_id, queue_class)
                    VALUES (?, ?)
                    """,
                    (source_id, requested),
                )
            else:
                old_class = str(existing["queue_class"])
                # Never demote a never-started source merely because current
                # ranking/status reconstruction is incomplete. Other classes
                # move only when their semantic state changes.
                effective = old_class if old_class in {"never_started", "quarantined"} and requested != "quarantined" else requested
                conn.execute(
                    """
                    UPDATE mal_public_userrecs_source_queue
                    SET eligible = 1,
                        queue_class = ?,
                        class_entered_at = CASE WHEN queue_class <> ? THEN CURRENT_TIMESTAMP ELSE class_entered_at END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE source_mal_anime_id = ?
                    """,
                    (effective, effective, source_id),
                )
        placeholders = ", ".join("?" for _ in source_ids)
        conn.execute(
            f"UPDATE mal_public_userrecs_source_queue SET eligible = 0, updated_at = CURRENT_TIMESTAMP WHERE source_mal_anime_id NOT IN ({placeholders})",
            source_ids,
        )
        rows = conn.execute(
            f"SELECT * FROM mal_public_userrecs_source_queue WHERE source_mal_anime_id IN ({placeholders})",
            source_ids,
        ).fetchall()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return [_public_userrecs_queue_from_row(row) for row in rows]


def claim_mal_public_userrecs_sources(
    db_path: Path,
    *,
    limit: int,
    claim_token: str,
    claim_seconds: int = PUBLIC_USERRECS_CLAIM_SECONDS,
) -> list[MalPublicUserRecsSourceQueueRow]:
    """CAS-claim sources in strict class order with durable oldest-first fairness.

    If never-started work exists it consumes the full source capacity. Open
    generations therefore cannot monopolize slots. Inside a class, never
    selected rows precede least-recently selected rows, then oldest class age.
    """
    capacity = max(0, int(limit))
    if capacity == 0:
        return []
    token = str(claim_token).strip()
    if not token:
        raise ValueError("claim_token is required")
    seconds = max(1, int(claim_seconds))
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT *
            FROM mal_public_userrecs_source_queue
            WHERE eligible = 1
              AND queue_class <> 'fresh'
              AND queue_class <> 'quarantined'
              AND (next_retry_at IS NULL OR datetime(next_retry_at) <= datetime('now'))
              AND (claim_token IS NULL OR claim_expires_at IS NULL OR datetime(claim_expires_at) <= datetime('now'))
            ORDER BY
                CASE queue_class
                    WHEN 'never_started' THEN 0
                    WHEN 'resumable' THEN 1
                    WHEN 'retry_due' THEN 2
                    WHEN 'refresh_due' THEN 3
                    ELSE 9
                END,
                selection_sequence ASC,
                datetime(class_entered_at) ASC,
                source_mal_anime_id ASC
            LIMIT ?
            """,
            (capacity,),
        ).fetchall()
        selected_ids = [int(row["source_mal_anime_id"]) for row in rows]
        if selected_ids:
            max_sequence_row = conn.execute(
                "SELECT COALESCE(MAX(selection_sequence), 0) AS value FROM mal_public_userrecs_source_queue"
            ).fetchone()
            next_sequence = int(max_sequence_row["value"] or 0)
            placeholders = ", ".join("?" for _ in selected_ids)
            for source_id in selected_ids:
                next_sequence += 1
                conn.execute(
                    """
                    UPDATE mal_public_userrecs_source_queue
                    SET claim_token = ?, claim_expires_at = datetime('now', ?),
                        last_selected_at = CURRENT_TIMESTAMP, selection_count = selection_count + 1,
                        selection_sequence = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE source_mal_anime_id = ?
                      AND (claim_token IS NULL OR claim_expires_at IS NULL OR datetime(claim_expires_at) <= datetime('now'))
                    """,
                    (token, f"+{seconds} seconds", next_sequence, source_id),
                )
            claimed = conn.execute(
                f"SELECT * FROM mal_public_userrecs_source_queue WHERE source_mal_anime_id IN ({placeholders}) AND claim_token = ?",
                (*selected_ids, token),
            ).fetchall()
        else:
            claimed = []
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    by_id = {int(row["source_mal_anime_id"]): row for row in claimed}
    return [_public_userrecs_queue_from_row(by_id[source_id]) for source_id in selected_ids if source_id in by_id]


def renew_mal_public_userrecs_source_claim(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    generation_id: int,
    claim_token: str,
    expected_revision: int,
    claim_seconds: int = PUBLIC_USERRECS_CLAIM_SECONDS,
) -> MalPublicUserRecsCrawlGeneration:
    """Atomically renew a still-live claim at a crawl boundary."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        if int(row["source_mal_anime_id"]) != int(source_mal_anime_id):
            raise RuntimeError("public userrecs claim fence rejected renewal: source identity changed")
        _require_public_userrecs_claim(
            conn, row, claim_token=claim_token, expected_revision=expected_revision, action="renewal"
        )
        modifier = f"+{max(1, int(claim_seconds))} seconds"
        conn.execute(
            "UPDATE mal_public_userrecs_source_queue SET claim_expires_at = datetime('now', ?), updated_at = CURRENT_TIMESTAMP WHERE source_mal_anime_id = ? AND claim_token = ?",
            (modifier, int(source_mal_anime_id), str(claim_token)),
        )
        conn.execute(
            "UPDATE mal_public_userrecs_crawl_generations SET claim_expires_at = datetime('now', ?), updated_at = CURRENT_TIMESTAMP WHERE generation_id = ? AND claim_token = ?",
            (modifier, int(generation_id), str(claim_token)),
        )
        result = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(result)


def release_mal_public_userrecs_source_claim(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    claim_token: str,
    queue_class: str,
    outcome: str,
    generation_id: int | None = None,
    next_retry_at: str | None = None,
    error_code: str | None = None,
) -> bool:
    if queue_class not in _PUBLIC_USERRECS_QUEUE_CLASS_PRIORITY:
        raise ValueError("invalid public userrecs queue_class")
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        if generation_id is not None:
            generation = _get_public_userrecs_generation_row(conn, int(generation_id))
            _require_public_userrecs_claim(
                conn, generation, claim_token=claim_token, expected_revision=None, action="claim release"
            )
        cursor = conn.execute(
            """
            UPDATE mal_public_userrecs_source_queue
            SET queue_class = ?,
                class_entered_at = CASE WHEN queue_class <> ? THEN CURRENT_TIMESTAMP ELSE class_entered_at END,
                claim_token = NULL,
                claim_expires_at = NULL,
                next_retry_at = ?,
                last_generation_id = COALESCE(?, last_generation_id),
                last_outcome = ?,
                last_error_code = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE source_mal_anime_id = ? AND claim_token = ?
              AND datetime(claim_expires_at) > datetime('now')
            """,
            (
                queue_class,
                queue_class,
                next_retry_at,
                generation_id,
                str(outcome)[:100],
                None if error_code is None else str(error_code)[:80],
                int(source_mal_anime_id),
                str(claim_token),
            ),
        )
        if cursor.rowcount == 1 and generation_id is not None:
            conn.execute(
                "UPDATE mal_public_userrecs_crawl_generations SET claim_token = NULL, claim_expires_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE generation_id = ? AND claim_token = ?",
                (int(generation_id), str(claim_token)),
            )
        conn.commit()
        return cursor.rowcount == 1


def create_or_get_active_mal_public_userrecs_generation(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    source_title: str | None = None,
    source_url: str | None = None,
    cursor_url: str | None = None,
    claim_token: str | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    """Return the one open public-userrecs crawl generation for a source, creating it if absent."""
    source_id = _coerce_mal_anime_id(source_mal_anime_id)
    if source_id is None:
        raise ValueError("source_mal_anime_id must be a positive integer")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        initial_cursor_url = _coerce_non_empty_text(cursor_url) or _coerce_non_empty_text(source_url)
        row = conn.execute(
            """
            SELECT *
            FROM mal_public_userrecs_crawl_generations
            WHERE source_mal_anime_id = ?
              AND status IN ('active', 'paused', 'ready')
            ORDER BY generation_id DESC
            LIMIT 1
            """,
            (source_id,),
        ).fetchone()
        if row is None:
            quarantined = conn.execute(
                """SELECT generation_id FROM mal_public_userrecs_crawl_generations
                   WHERE source_mal_anime_id = ?
                     AND (quarantined_at IS NOT NULL OR quarantine_reason IS NOT NULL)
                   ORDER BY generation_id DESC LIMIT 1""",
                (source_id,),
            ).fetchone()
            if quarantined is not None:
                raise RuntimeError(
                    "quarantined public userrecs source is terminal; explicit audited reinitialization is required"
                )
            cursor = conn.execute(
                """
                INSERT INTO mal_public_userrecs_crawl_generations (
                    source_mal_anime_id, source_title, source_url, cursor_url, generation_key, logic_version, attempt_count
                ) VALUES (?, ?, ?, ?, lower(hex(randomblob(16))), ?, 1)
                """,
                (source_id, source_title, source_url, initial_cursor_url, PUBLIC_USERRECS_LOGIC_VERSION),
            )
            generation_id = int(cursor.lastrowid)
            _record_public_userrecs_event(
                conn,
                generation_id=generation_id,
                source_mal_anime_id=source_id,
                event_type="begin",
                page_url=initial_cursor_url,
            )
            row = _get_public_userrecs_generation_row(conn, generation_id)
        if claim_token is not None:
            normalized_claim_token = str(claim_token).strip()
            if not normalized_claim_token:
                raise ValueError("claim_token is required when binding a public userrecs generation")
            queue = conn.execute(
                "SELECT claim_token, claim_expires_at, last_generation_id FROM mal_public_userrecs_source_queue WHERE source_mal_anime_id = ?",
                (source_id,),
            ).fetchone()
            live = None if queue is None else conn.execute(
                "SELECT datetime(?) > datetime('now') AS live", (queue["claim_expires_at"],)
            ).fetchone()
            if queue is None or queue["claim_token"] != normalized_claim_token or live is None or not bool(live["live"]):
                raise RuntimeError("public userrecs claim fence rejected generation bind")
            generation_id = int(row["generation_id"])
            previous_token = row["claim_token"]
            if previous_token not in (None, normalized_claim_token):
                generation_expired = conn.execute(
                    "SELECT ? IS NOT NULL AND datetime(?) <= datetime('now') AS expired",
                    (row["claim_expires_at"], row["claim_expires_at"]),
                ).fetchone()
                if generation_expired is None or not bool(generation_expired["expired"]):
                    raise RuntimeError("public userrecs claim fence rejected generation bind: prior generation lease is still live")
            expected_revision = int(row["generation_revision"] or 0)
            rebound = conn.execute(
                """
                UPDATE mal_public_userrecs_crawl_generations
                SET claim_token = ?, claim_expires_at = ?, generation_revision = generation_revision + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE generation_id = ? AND source_mal_anime_id = ?
                  AND status IN ('active', 'paused', 'ready') AND generation_revision = ?
                """,
                (normalized_claim_token, queue["claim_expires_at"], generation_id, source_id, expected_revision),
            )
            if rebound.rowcount != 1:
                raise RuntimeError("public userrecs claim fence rejected generation bind: generation revision changed")
            queue_bound = conn.execute(
                """
                UPDATE mal_public_userrecs_source_queue
                SET last_generation_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE source_mal_anime_id = ? AND claim_token = ?
                  AND datetime(claim_expires_at) > datetime('now')
                  AND (last_generation_id IS NULL OR last_generation_id = ?)
                """,
                (generation_id, source_id, normalized_claim_token, generation_id),
            )
            if queue_bound.rowcount != 1:
                raise RuntimeError("public userrecs claim fence rejected generation bind: queue generation identity changed")
            conn.execute(
                """
                INSERT INTO mal_public_userrecs_claim_events (
                    generation_id, source_mal_anime_id, event_type, previous_claim_token,
                    claim_token, generation_revision
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    generation_id,
                    source_id,
                    "rebind" if previous_token not in (None, normalized_claim_token) else "bind",
                    previous_token,
                    normalized_claim_token,
                    expected_revision + 1,
                ),
            )
            row = _get_public_userrecs_generation_row(conn, generation_id)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(row)


def reinitialize_mal_public_userrecs_source(
    db_path: Path, *, source_mal_anime_id: int, source_url: str, operator_reason: str,
) -> MalPublicUserRecsCrawlGeneration:
    """Explicitly clear a source quarantine while retaining all history and published LKG."""
    source_id = _coerce_mal_anime_id(source_mal_anime_id)
    reason = str(operator_reason).strip()
    if source_id is None or not reason:
        raise ValueError("source_mal_anime_id and operator_reason are required")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        old = conn.execute(
            """SELECT * FROM mal_public_userrecs_crawl_generations
               WHERE source_mal_anime_id=? ORDER BY generation_id DESC LIMIT 1""", (source_id,),
        ).fetchone()
        if old is None or (old["quarantined_at"] is None and old["quarantine_reason"] is None):
            raise RuntimeError("latest public userrecs generation is not quarantined")
        if old["claim_token"] is not None:
            live = conn.execute("SELECT datetime(?) > datetime('now') AS live", (old["claim_expires_at"],)).fetchone()
            if live is not None and bool(live["live"]):
                raise RuntimeError("quarantined public userrecs generation still has a live claim")
        cursor = conn.execute(
            """INSERT INTO mal_public_userrecs_crawl_generations
               (source_mal_anime_id,source_title,source_url,cursor_url,generation_key,logic_version,attempt_count)
               VALUES (?,?,?,?,lower(hex(randomblob(16))),?,1)""",
            (source_id, old["source_title"], str(source_url), str(source_url), PUBLIC_USERRECS_LOGIC_VERSION),
        )
        generation_id = int(cursor.lastrowid)
        conn.execute(
            """INSERT INTO mal_public_userrecs_source_queue(source_mal_anime_id,queue_class,last_generation_id,last_outcome,last_error_code)
               VALUES (?,'resumable',?,'explicit_reinitialize',NULL)
               ON CONFLICT(source_mal_anime_id) DO UPDATE SET eligible=1,queue_class='resumable',
                 class_entered_at=CURRENT_TIMESTAMP,claim_token=NULL,claim_expires_at=NULL,next_retry_at=NULL,
                 last_generation_id=excluded.last_generation_id,last_outcome='explicit_reinitialize',last_error_code=NULL,
                 updated_at=CURRENT_TIMESTAMP""", (source_id, generation_id),
        )
        _record_public_userrecs_event(conn, generation_id=generation_id, source_mal_anime_id=source_id,
                                      event_type="begin", page_url=str(source_url), error=f"operator reinitialize: {reason[:900]}")
        row = _get_public_userrecs_generation_row(conn, generation_id)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(row)


def _prepared_public_userrecs_staged_edges(
    edges: Iterable[dict[str, Any]],
    *,
    generation_id: int,
    source_mal_anime_id: int,
    page_number: int,
    page_url: str,
    fetched_at: str,
) -> list[tuple[Any, ...]]:
    by_target: dict[int, tuple[Any, ...]] = {}
    sort_keys: dict[int, tuple[int, int]] = {}
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            continue
        target_id = _coerce_mal_anime_id(edge.get("target_mal_anime_id"))
        if target_id is None:
            raise ValueError("staged public userrecs edge target_mal_anime_id must be a positive integer")
        count = _coerce_nonnegative_int(edge.get("num_recommendations"))
        target_title = _public_userrecs_safe_text(edge.get("target_title"), max_length=500)
        raw_input = edge.get("raw") if isinstance(edge.get("raw"), dict) else {}
        provenance_input = edge.get("provenance") if isinstance(edge.get("provenance"), dict) else {}
        raw_page_url = _public_userrecs_safe_text(raw_input.get("page_url"), max_length=2000) or page_url
        provenance_page_url = _public_userrecs_safe_text(provenance_input.get("page_url"), max_length=2000) or page_url
        provenance_source_url = _public_userrecs_safe_text(provenance_input.get("source_url"), max_length=2000)
        provenance_page_count = _coerce_nonnegative_int(provenance_input.get("page_count"))
        raw = {
            "source": "public_mal_userrecs",
            "page_url": raw_page_url,
            "target_mal_anime_id": target_id,
            "target_title": target_title,
            "num_recommendations": count,
        }
        provenance = {
            "source": "public_mal_userrecs",
            "page_url": provenance_page_url,
            "retained_fields": list(_PUBLIC_USERRECS_RETAINED_FIELDS),
            "privacy": "recommendation prose and usernames are not persisted",
        }
        if provenance_source_url is not None:
            provenance["source_url"] = provenance_source_url
        if provenance_page_count is not None:
            provenance["page_count"] = provenance_page_count
        prepared = (
            int(generation_id),
            int(source_mal_anime_id),
            int(page_number),
            target_id,
            target_title,
            count,
            json.dumps(raw, ensure_ascii=False, sort_keys=True),
            json.dumps(provenance, ensure_ascii=False, sort_keys=True),
            fetched_at,
        )
        score = (count if count is not None else -1, -index)
        existing_score = sort_keys.get(target_id)
        if existing_score is None or score > existing_score:
            by_target[target_id] = prepared
            sort_keys[target_id] = score
    return list(by_target.values())


def replace_mal_public_userrecs_staged_page(
    db_path: Path,
    *,
    generation_id: int,
    page_number: int,
    page_url: str,
    page_fingerprint: str,
    anchor: dict[str, Any] | None = None,
    next_url: str | None = None,
    edges: Iterable[dict[str, Any]] = (),
    fetched_at: str | None = None,
    terminal_evidence: dict[str, Any] | None = None,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsStagedPage:
    """Replace one staged page and its edges without touching published recommendations."""
    if int(page_number) < 1:
        raise ValueError("page_number must be >= 1")
    normalized_page_url = str(page_url).strip()
    if not normalized_page_url:
        raise ValueError("page_url is required")
    normalized_fingerprint = str(page_fingerprint).strip()
    if not normalized_fingerprint:
        raise ValueError("page_fingerprint is required")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_generation_status(
            generation,
            _PUBLIC_USERRECS_MUTABLE_GENERATION_STATUSES,
            action="stage page for",
        )
        _require_public_userrecs_claim(
            conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="stage page"
        )
        source_id = int(generation["source_mal_anime_id"])
        timestamp = fetched_at or str(conn.execute("SELECT CURRENT_TIMESTAMP AS now").fetchone()["now"])
        prepared_edges = _prepared_public_userrecs_staged_edges(
            edges,
            generation_id=int(generation_id),
            source_mal_anime_id=source_id,
            page_number=int(page_number),
            page_url=normalized_page_url,
            fetched_at=timestamp,
        )
        prepared_target_ids = {int(edge[3]) for edge in prepared_edges}
        if prepared_target_ids:
            placeholders = ", ".join("?" for _ in prepared_target_ids)
            overlap = conn.execute(
                f"""
                SELECT target_mal_anime_id
                FROM mal_public_userrecs_staged_edges
                WHERE generation_id = ? AND page_number <> ?
                  AND target_mal_anime_id IN ({placeholders})
                ORDER BY target_mal_anime_id
                """,
                (int(generation_id), int(page_number), *sorted(prepared_target_ids)),
            ).fetchall()
            if overlap:
                ids = [int(row["target_mal_anime_id"]) for row in overlap]
                raise ValueError(f"staged public userrecs page overlaps prior generation pages: {ids[:10]!r}")
        conn.execute(
            """
            INSERT INTO mal_public_userrecs_staged_pages (
                generation_id,
                source_mal_anime_id,
                page_number,
                page_url,
                page_fingerprint,
                anchor_json,
                next_url,
                edge_count,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(generation_id, page_number) DO UPDATE SET
                source_mal_anime_id = excluded.source_mal_anime_id,
                page_url = excluded.page_url,
                page_fingerprint = excluded.page_fingerprint,
                anchor_json = excluded.anchor_json,
                next_url = excluded.next_url,
                edge_count = excluded.edge_count,
                fetched_at = excluded.fetched_at,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(generation_id),
                source_id,
                int(page_number),
                normalized_page_url,
                normalized_fingerprint,
                json.dumps(_public_userrecs_sanitized_anchor(anchor), ensure_ascii=False, sort_keys=True),
                next_url,
                len(prepared_edges),
                timestamp,
            ),
        )
        conn.execute(
            "DELETE FROM mal_public_userrecs_staged_edges WHERE generation_id = ? AND page_number = ?",
            (int(generation_id), int(page_number)),
        )
        conn.executemany(
            """
            INSERT INTO mal_public_userrecs_staged_edges (
                generation_id,
                source_mal_anime_id,
                page_number,
                target_mal_anime_id,
                target_title,
                num_recommendations,
                raw_json,
                provenance_json,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prepared_edges,
        )
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM mal_public_userrecs_staged_pages WHERE generation_id = ?) AS pages,
                (SELECT COUNT(*) FROM mal_public_userrecs_staged_edges WHERE generation_id = ?) AS edges
            """,
            (int(generation_id), int(generation_id)),
        ).fetchone()
        final_page = conn.execute(
            """
            SELECT page_url, page_fingerprint, next_url
            FROM mal_public_userrecs_staged_pages
            WHERE generation_id = ?
            ORDER BY page_number DESC
            LIMIT 1
            """,
            (int(generation_id),),
        ).fetchone()
        if final_page is None:  # pragma: no cover - defensive; the upsert above guarantees a page row.
            raise ValueError("public userrecs staged page replacement produced no staged pages")
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET cursor_url = ?,
                pages_fetched = ?,
                staged_edge_count = ?,
                last_page_url = ?,
                last_page_fingerprint = ?,
                terminal_evidence_json = ?,
                last_error = NULL,
                staged_revision = staged_revision + 1,
                validated_staged_revision = NULL,
                validation_fingerprint = NULL,
                validation_page_number = 0,
                validation_revision = NULL,
                final_anchor_step = 0,
                final_anchor_revision = NULL,
                first_page_revalidated_at = NULL,
                boundary_revalidated_at = NULL,
                generation_revision = generation_revision + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ?
            """,
            (
                final_page["next_url"],
                int(counts["pages"] or 0),
                int(counts["edges"] or 0),
                final_page["page_url"],
                final_page["page_fingerprint"],
                json.dumps(terminal_evidence if isinstance(terminal_evidence, dict) else {}, sort_keys=True),
                int(generation_id),
            ),
        )
        _record_public_userrecs_event(
            conn,
            generation_id=int(generation_id),
            source_mal_anime_id=source_id,
            event_type="page_upsert",
            page_number=int(page_number),
            page_url=normalized_page_url,
        )
        row = conn.execute(
            """
            SELECT *
            FROM mal_public_userrecs_staged_pages
            WHERE generation_id = ? AND page_number = ?
            """,
            (int(generation_id), int(page_number)),
        ).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_staged_page_from_row(row)


def pause_mal_public_userrecs_generation(
    db_path: Path,
    *,
    generation_id: int,
    cursor_url: str | None,
    error: str | None = None,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_generation_status(
            generation,
            _PUBLIC_USERRECS_MUTABLE_GENERATION_STATUSES,
            action="pause",
        )
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="pause")
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'paused', cursor_url = ?, last_error = ?, generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ?
            """,
            (cursor_url, None if error is None else str(error).strip()[:1000] or None, int(generation_id)),
        )
        _record_public_userrecs_event(
            conn,
            generation_id=int(generation_id),
            source_mal_anime_id=int(generation["source_mal_anime_id"]),
            event_type="pause",
            page_url=cursor_url,
            error=error,
        )
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(row)


def resume_mal_public_userrecs_generation(
    db_path: Path,
    *,
    generation_id: int,
    cursor_url: str | None | object = _UNSET,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_generation_status(
            generation,
            _PUBLIC_USERRECS_MUTABLE_GENERATION_STATUSES,
            action="resume",
        )
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="resume")
        if cursor_url is _UNSET:
            conn.execute(
                """
                UPDATE mal_public_userrecs_crawl_generations
                SET status = 'active', last_error = NULL, attempt_count = attempt_count + 1,
                    next_retry_at = NULL, retry_class = NULL, generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
                WHERE generation_id = ?
                """,
                (int(generation_id),),
            )
            event_page_url = generation["cursor_url"]
        else:
            conn.execute(
                """
                UPDATE mal_public_userrecs_crawl_generations
                SET status = 'active', cursor_url = ?, last_error = NULL,
                    attempt_count = attempt_count + 1, next_retry_at = NULL, retry_class = NULL,
                    generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
                WHERE generation_id = ?
                """,
                (cursor_url, int(generation_id)),
            )
            event_page_url = None if cursor_url is None else str(cursor_url)
        _record_public_userrecs_event(
            conn,
            generation_id=int(generation_id),
            source_mal_anime_id=int(generation["source_mal_anime_id"]),
            event_type="resume",
            page_url=event_page_url,
        )
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(row)


def record_mal_public_userrecs_revalidation(
    db_path: Path,
    *,
    generation_id: int,
    checked_boundary: bool,
    validation_fingerprint: str | None = None,
    validated_page_number: int | None = None,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    """Checkpoint bounded snapshot validation, bound to the staged revision."""
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="record revalidation")
        pages, edges = _assert_public_userrecs_generation_coherent(conn, generation, require_terminal=False)
        bound_fingerprint = _public_userrecs_staged_validation_fingerprint(pages, edges)
        page_number = len(pages) if validated_page_number is None else int(validated_page_number)
        if page_number < 1 or page_number > len(pages):
            raise ValueError("public userrecs validation page is outside the staged generation")
        prior_page = int(generation["validation_page_number"] or 0)
        prior_revision = generation["validation_revision"]
        staged_revision = int(generation["staged_revision"] or 0)
        if prior_page and (prior_revision is None or int(prior_revision) != staged_revision):
            raise RuntimeError("public userrecs incremental validation revision is stale")
        if page_number != prior_page + 1:
            raise RuntimeError("public userrecs incremental validation cursor is non-contiguous")
        complete = page_number == len(pages)
        conn.execute(
            """UPDATE mal_public_userrecs_crawl_generations
               SET first_page_revalidated_at=CASE WHEN ?=1 THEN CURRENT_TIMESTAMP ELSE first_page_revalidated_at END,
                   boundary_revalidated_at=CASE WHEN ? AND ? THEN CURRENT_TIMESTAMP ELSE boundary_revalidated_at END,
                   validation_page_number=?, validation_revision=staged_revision,
                   validated_staged_revision=CASE WHEN ? THEN staged_revision ELSE NULL END,
                   validation_fingerprint=CASE WHEN ? THEN ? ELSE NULL END,
                   generation_revision=generation_revision+1, updated_at=CURRENT_TIMESTAMP
               WHERE generation_id=? AND status IN ('active','paused','ready')""",
            (page_number, int(bool(checked_boundary)), int(complete), page_number, int(complete), int(complete), bound_fingerprint, int(generation_id)),
        )
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    return _public_userrecs_generation_from_row(row)


def record_mal_public_userrecs_final_anchor(
    db_path: Path, *, generation_id: int, anchor_step: int, claim_token: str,
    expected_revision: int,
) -> MalPublicUserRecsCrawlGeneration:
    """Checkpoint final page-1/terminal anchors for the exact fully validated staged revision."""
    step = int(anchor_step)
    if step not in {1, 2}:
        raise ValueError("public userrecs final anchor step must be 1 or 2")
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token,
                                       expected_revision=expected_revision, action="record final anchor")
        pages, edges = _assert_public_userrecs_generation_coherent(conn, generation, require_terminal=True)
        staged_revision = int(generation["staged_revision"] or 0)
        if generation["validated_staged_revision"] is None or int(generation["validated_staged_revision"]) != staged_revision:
            raise RuntimeError("public userrecs final anchors require fully validated staged revision")
        prior = int(generation["final_anchor_step"] or 0)
        prior_revision = generation["final_anchor_revision"]
        if prior and (prior_revision is None or int(prior_revision) != staged_revision):
            raise RuntimeError("public userrecs final anchor cursor is stale")
        expected_step = 1 if prior == 0 else 2
        if step != expected_step:
            raise RuntimeError("public userrecs final anchor cursor is non-contiguous")
        conn.execute(
            """UPDATE mal_public_userrecs_crawl_generations
               SET final_anchor_step=?,final_anchor_revision=staged_revision,
                   validation_fingerprint=?,generation_revision=generation_revision+1,updated_at=CURRENT_TIMESTAMP
               WHERE generation_id=?""",
            (step, _public_userrecs_staged_validation_fingerprint(pages, edges), int(generation_id)),
        )
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    return _public_userrecs_generation_from_row(row)


def schedule_mal_public_userrecs_generation_retry(
    db_path: Path,
    *,
    generation_id: int,
    retry_class: str,
    next_retry_at: str,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="schedule retry")
        source_id = int(generation["source_mal_anime_id"])
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET retry_class = ?, next_retry_at = ?, generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ? AND status IN ('active', 'paused')
            """,
            (str(retry_class)[:80], str(next_retry_at), int(generation_id)),
        )
        queue_cursor = conn.execute(
            """
            UPDATE mal_public_userrecs_source_queue
            SET queue_class = 'retry_due', class_entered_at = CASE WHEN queue_class <> 'retry_due' THEN CURRENT_TIMESTAMP ELSE class_entered_at END,
                next_retry_at = ?, last_generation_id = ?, last_outcome = 'retryable_failure',
                last_error_code = ?, updated_at = CURRENT_TIMESTAMP
            WHERE source_mal_anime_id = ? AND (? IS NULL OR claim_token = ?)
            """,
            (str(next_retry_at), int(generation_id), str(retry_class)[:80], source_id, claim_token, claim_token),
        )
        if claim_token is not None and queue_cursor.rowcount != 1:
            raise RuntimeError("public userrecs claim fence rejected retry queue transition")
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    return _public_userrecs_generation_from_row(row)


def _public_userrecs_staged_pages(conn: sqlite3.Connection, generation_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM mal_public_userrecs_staged_pages
        WHERE generation_id = ?
        ORDER BY page_number ASC
        """,
        (int(generation_id),),
    ).fetchall()


def _public_userrecs_staged_edges(conn: sqlite3.Connection, generation_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM mal_public_userrecs_staged_edges
        WHERE generation_id = ?
        ORDER BY page_number ASC, target_mal_anime_id ASC
        """,
        (int(generation_id),),
    ).fetchall()


def _public_userrecs_staged_validation_fingerprint(
    pages: list[sqlite3.Row], edges: list[sqlite3.Row]
) -> str:
    digest = __import__("hashlib").sha256()
    for page in pages:
        digest.update(
            f"{int(page['page_number'])}:{page['page_url']}:{page['page_fingerprint']}:{page['next_url'] or ''}\n".encode()
        )
    for edge in edges:
        digest.update(
            f"{int(edge['page_number'])}:{int(edge['target_mal_anime_id'])}:{edge['num_recommendations']}:{edge['raw_json']}:{edge['provenance_json']}\n".encode()
        )
    return digest.hexdigest()


def _assert_public_userrecs_generation_coherent(
    conn: sqlite3.Connection,
    generation: sqlite3.Row,
    *,
    require_terminal: bool,
) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    generation_id = int(generation["generation_id"])
    source_id = int(generation["source_mal_anime_id"])
    pages = _public_userrecs_staged_pages(conn, generation_id)
    edges = _public_userrecs_staged_edges(conn, generation_id)
    if not pages:
        raise ValueError("public userrecs generation has no staged pages")
    expected_page_numbers = list(range(1, len(pages) + 1))
    actual_page_numbers = [int(row["page_number"]) for row in pages]
    if actual_page_numbers != expected_page_numbers:
        raise ValueError(
            "public userrecs staged pages are not contiguous from page 1: "
            f"{actual_page_numbers!r}"
        )
    if int(generation["pages_fetched"] or 0) != len(pages):
        raise ValueError("public userrecs generation pages_fetched does not match staged pages")
    if int(generation["staged_edge_count"] or 0) != len(edges):
        raise ValueError("public userrecs generation staged_edge_count does not match staged edges")
    edge_counts_by_page: dict[int, int] = {}
    for edge in edges:
        if int(edge["source_mal_anime_id"]) != source_id:
            raise ValueError("public userrecs staged edge source does not match generation source")
        edge_counts_by_page[int(edge["page_number"])] = edge_counts_by_page.get(int(edge["page_number"]), 0) + 1
    page_numbers = set(actual_page_numbers)
    for edge_page in edge_counts_by_page:
        if edge_page not in page_numbers:
            raise ValueError("public userrecs staged edge references a missing page")
    for page in pages:
        if int(page["source_mal_anime_id"]) != source_id:
            raise ValueError("public userrecs staged page source does not match generation source")
        count = edge_counts_by_page.get(int(page["page_number"]), 0)
        if int(page["edge_count"] or 0) != count:
            raise ValueError("public userrecs staged page edge_count does not match staged edges")
    last_page = pages[-1]
    if generation["last_page_url"] != last_page["page_url"]:
        raise ValueError("public userrecs generation last_page_url does not match final staged page")
    if generation["last_page_fingerprint"] != last_page["page_fingerprint"]:
        raise ValueError("public userrecs generation last_page_fingerprint does not match final staged page")
    for previous, current in zip(pages, pages[1:]):
        if previous["next_url"] != current["page_url"]:
            raise ValueError("public userrecs staged next-link chain does not match page order")
    if require_terminal:
        if generation["cursor_url"] is not None:
            raise ValueError("public userrecs generation still has a persisted next-page cursor")
        if last_page["next_url"] is not None:
            raise ValueError("public userrecs final staged page still has a next_url")
        terminal_evidence = _load_json_value(generation["terminal_evidence_json"], {})
        terminal_evidence = terminal_evidence if isinstance(terminal_evidence, dict) else {}
        if not edges and not (
            bool(terminal_evidence.get("document_complete"))
            and bool(terminal_evidence.get("recommendation_surface"))
            and bool(terminal_evidence.get("next_links_consistent"))
            and bool(terminal_evidence.get("explicit_empty"))
            and int(terminal_evidence.get("recommendation_row_count") or 0) == 0
            and int(terminal_evidence.get("next_candidate_count") or 0) == 0
        ):
            raise ValueError("empty public userrecs replacement lacks strong terminal-empty proof")
    return pages, edges


def mark_mal_public_userrecs_generation_ready(
    db_path: Path,
    *,
    generation_id: int,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    """Mark a terminal coherent staged crawl ready for guarded publication."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_generation_status(
            generation,
            _PUBLIC_USERRECS_MUTABLE_GENERATION_STATUSES,
            action="mark ready",
        )
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="mark ready")
        pages, edges = _assert_public_userrecs_generation_coherent(conn, generation, require_terminal=True)
        validation_fingerprint = _public_userrecs_staged_validation_fingerprint(pages, edges)
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'ready', completed_at = CURRENT_TIMESTAMP,
                validated_staged_revision = staged_revision,
                validation_fingerprint = ?, validation_revision = staged_revision,
                validation_page_number = pages_fetched,
                generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ?
            """,
            (validation_fingerprint, int(generation_id)),
        )
        _record_public_userrecs_event(
            conn,
            generation_id=int(generation_id),
            source_mal_anime_id=int(generation["source_mal_anime_id"]),
            event_type="ready",
        )
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(row)


def discard_mal_public_userrecs_generation(
    db_path: Path,
    *,
    generation_id: int,
    reason: str | None = None,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_generation_status(
            generation,
            _PUBLIC_USERRECS_OPEN_GENERATION_STATUSES,
            action="discard",
        )
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="discard")
        normalized_reason = None if reason is None else str(reason).strip()[:1000] or None
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'discarded', discarded_at = CURRENT_TIMESTAMP, last_error = ?, generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ?
            """,
            (normalized_reason, int(generation_id)),
        )
        _record_public_userrecs_event(
            conn,
            generation_id=int(generation_id),
            source_mal_anime_id=int(generation["source_mal_anime_id"]),
            event_type="discard",
            error=normalized_reason,
        )
        row = _get_public_userrecs_generation_row(conn, int(generation_id))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(row)


def restart_mal_public_userrecs_generation_after_drift(
    db_path: Path,
    *,
    generation_id: int,
    reason: str | None = "public userrecs pagination drift detected",
    cursor_url: str | None | object = _UNSET,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsCrawlGeneration:
    """Discard an open generation after drift and start a fresh one for the same source."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        old = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_generation_status(
            old,
            _PUBLIC_USERRECS_OPEN_GENERATION_STATUSES,
            action="restart after drift",
        )
        _require_public_userrecs_claim(conn, old, claim_token=claim_token, expected_revision=expected_revision, action="restart")
        source_id = int(old["source_mal_anime_id"])
        restart_count = int(old["restart_count"] or 0) + 1
        normalized_reason = None if reason is None else str(reason).strip()[:1000] or None
        if restart_count > PUBLIC_USERRECS_MAX_DRIFT_RESTARTS:
            conn.execute(
                """
                UPDATE mal_public_userrecs_crawl_generations
                SET status = 'failed',
                    quarantined_at = CURRENT_TIMESTAMP,
                    quarantine_reason = ?,
                    restart_count = ?,
                    drift_count = drift_count + 1,
                    last_error = ?,
                    generation_revision = generation_revision + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE generation_id = ?
                """,
                (normalized_reason, restart_count, normalized_reason, int(generation_id)),
            )
            conn.execute(
                """
                UPDATE mal_public_userrecs_source_queue
                SET queue_class = 'quarantined', class_entered_at = CURRENT_TIMESTAMP,
                    claim_token = NULL, claim_expires_at = NULL,
                    last_outcome = 'quarantined_drift_livelock', last_error_code = 'pagination_drift',
                    updated_at = CURRENT_TIMESTAMP
                WHERE source_mal_anime_id = ?
                """,
                (source_id,),
            )
            _record_public_userrecs_event(
                conn,
                generation_id=int(generation_id),
                source_mal_anime_id=source_id,
                event_type="fail",
                error=normalized_reason,
            )
            row = _get_public_userrecs_generation_row(conn, int(generation_id))
            conn.commit()
            return _public_userrecs_generation_from_row(row)
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'discarded', discarded_at = CURRENT_TIMESTAMP, last_error = ?, generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ?
            """,
            (normalized_reason, int(generation_id)),
        )
        _record_public_userrecs_event(
            conn,
            generation_id=int(generation_id),
            source_mal_anime_id=source_id,
            event_type="discard",
            error=normalized_reason,
        )
        new_cursor_url = old["source_url"] if cursor_url is _UNSET else cursor_url
        cursor = conn.execute(
            """
            INSERT INTO mal_public_userrecs_crawl_generations (
                source_mal_anime_id, source_title, source_url, cursor_url,
                generation_key, logic_version, restart_count, drift_count, claim_token, claim_expires_at
            ) VALUES (?, ?, ?, ?, lower(hex(randomblob(16))), ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                old["source_title"],
                old["source_url"],
                new_cursor_url,
                PUBLIC_USERRECS_LOGIC_VERSION,
                restart_count,
                int(old["drift_count"] or 0) + 1,
                claim_token,
                old["claim_expires_at"] if claim_token is not None else None,
            ),
        )
        new_generation_id = int(cursor.lastrowid)
        if claim_token is not None:
            changed = conn.execute(
                "UPDATE mal_public_userrecs_source_queue SET last_generation_id = ?, updated_at = CURRENT_TIMESTAMP WHERE source_mal_anime_id = ? AND claim_token = ?",
                (new_generation_id, source_id, str(claim_token)),
            )
            if changed.rowcount != 1:
                raise RuntimeError("public userrecs claim fence rejected restart queue transition")
        _record_public_userrecs_event(
            conn,
            generation_id=new_generation_id,
            source_mal_anime_id=source_id,
            event_type="begin",
            page_url=None if new_cursor_url is None else str(new_cursor_url),
        )
        row = _get_public_userrecs_generation_row(conn, new_generation_id)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _public_userrecs_generation_from_row(row)


def _aggregate_public_userrecs_publication_edges(
    staged_edges: list[sqlite3.Row],
    *,
    source_url: str | None,
    page_count: int,
    generation_id: int,
) -> list[dict[str, Any]]:
    by_target: dict[int, dict[str, Any]] = {}
    best_keys: dict[int, tuple[int, int]] = {}
    for index, row in enumerate(staged_edges):
        target_id = int(row["target_mal_anime_id"])
        count = 0 if row["num_recommendations"] is None else max(0, int(row["num_recommendations"]))
        sort_key = (count, -index)
        if target_id in best_keys and sort_key <= best_keys[target_id]:
            existing = by_target[target_id]
            if not existing.get("target_title") and row["target_title"]:
                existing["target_title"] = row["target_title"]
            continue
        raw_input = _load_json_value(row["raw_json"], {})
        raw_input = raw_input if isinstance(raw_input, dict) else {}
        provenance_input = _load_json_value(row["provenance_json"], {})
        provenance_input = provenance_input if isinstance(provenance_input, dict) else {}
        page_url = (
            _public_userrecs_safe_text(provenance_input.get("page_url"), max_length=2000)
            or _public_userrecs_safe_text(raw_input.get("page_url"), max_length=2000)
        )
        raw = {
            "source": "public_mal_userrecs",
            "target_mal_anime_id": target_id,
            "target_title": row["target_title"],
            "num_recommendations": count,
        }
        if page_url is not None:
            raw["page_url"] = page_url
        provenance = {
            "source": "public_mal_userrecs",
            "source_url": _public_userrecs_safe_text(provenance_input.get("source_url"), max_length=2000) or source_url,
            "page_count": int(page_count),
            "generation_id": int(generation_id),
            "retained_fields": list(_PUBLIC_USERRECS_RETAINED_FIELDS),
            "privacy": "recommendation prose and usernames are parsed only for aggregate counts and are not persisted",
        }
        if page_url is not None:
            provenance["page_url"] = page_url
        by_target[target_id] = {
            "target_mal_anime_id": target_id,
            "target_title": row["target_title"],
            "num_recommendations": count,
            "raw": raw,
            "provenance": provenance,
        }
        best_keys[target_id] = sort_key
    return sorted(
        by_target.values(),
        key=lambda edge: (-int(edge["num_recommendations"] or 0), int(edge["target_mal_anime_id"])),
    )


def _execute_public_userrecs_publication_statement(
    conn: sqlite3.Connection,
    statement: str,
    params: tuple[Any, ...] = (),
) -> sqlite3.Cursor:
    """Narrow test seam proving public-userrecs publication rollback behavior."""
    return conn.execute(statement, params)


def publish_mal_public_userrecs_generation(
    db_path: Path,
    *,
    generation_id: int,
    claim_token: str | None = None,
    expected_revision: int | None = None,
) -> MalPublicUserRecsPublicationResult:
    """Atomically publish one terminal coherent staged public-userrecs generation."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        generation = _get_public_userrecs_generation_row(conn, int(generation_id))
        _require_public_userrecs_generation_status(
            generation,
            frozenset({"ready"}),
            action="publish",
        )
        _require_public_userrecs_claim(conn, generation, claim_token=claim_token, expected_revision=expected_revision, action="publish")
        if generation["completed_at"] is None:
            raise ValueError("public userrecs generation is ready but lacks completed_at")
        pages, staged_edges = _assert_public_userrecs_generation_coherent(conn, generation, require_terminal=True)
        duplicate = conn.execute(
            """
            SELECT target_mal_anime_id, COUNT(DISTINCT page_number) AS page_count
            FROM mal_public_userrecs_staged_edges WHERE generation_id = ?
            GROUP BY target_mal_anime_id HAVING COUNT(DISTINCT page_number) > 1 LIMIT 1
            """,
            (int(generation_id),),
        ).fetchone()
        if duplicate is not None:
            raise ValueError("public userrecs publication rejected duplicate target across staged pages")
        if claim_token is not None and (
            generation["validated_staged_revision"] is None
            or int(generation["validated_staged_revision"]) != int(generation["staged_revision"])
        ):
            raise ValueError("public userrecs publication requires final validation for exact staged revision")
        if claim_token is not None and _public_userrecs_staged_validation_fingerprint(pages, staged_edges) != str(generation["validation_fingerprint"]):
            raise ValueError("public userrecs staged snapshot changed after final validation")
        if claim_token is not None and (
            int(generation["final_anchor_step"] or 0) != 2
            or generation["final_anchor_revision"] is None
            or int(generation["final_anchor_revision"]) != int(generation["staged_revision"])
        ):
            raise ValueError("public userrecs publication requires final page-1 and terminal anchors")
        source_id = int(generation["source_mal_anime_id"])
        source_url = generation["source_url"] or pages[0]["page_url"]
        edges = _aggregate_public_userrecs_publication_edges(
            staged_edges,
            source_url=source_url,
            page_count=len(pages),
            generation_id=int(generation_id),
        )
        _execute_public_userrecs_publication_statement(
            conn,
            "DELETE FROM mal_anime_recommendations WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'",
            (source_id,),
        )
        for edge in edges:
            _execute_public_userrecs_publication_statement(
                conn,
                """
                INSERT INTO mal_anime_recommendations (
                    source_mal_anime_id,
                    target_mal_anime_id,
                    target_title,
                    num_recommendations,
                    hop_distance,
                    source_kind,
                    raw_json,
                    harvest_source,
                    complete_harvest,
                    provenance_json
                ) VALUES (?, ?, ?, ?, 1, 'mal_recommendation', ?, ?, 1, ?)
                """,
                (
                    source_id,
                    int(edge["target_mal_anime_id"]),
                    edge.get("target_title"),
                    int(edge["num_recommendations"] or 0),
                    json.dumps(edge.get("raw") if isinstance(edge.get("raw"), dict) else {}, ensure_ascii=False, sort_keys=True),
                    MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                    json.dumps(edge.get("provenance") if isinstance(edge.get("provenance"), dict) else {}, ensure_ascii=False, sort_keys=True),
                ),
            )
        _execute_public_userrecs_publication_statement(
            conn,
            """
            INSERT INTO mal_recommendation_harvest_status (
                source_mal_anime_id,
                status,
                num_edges,
                fetched_at,
                source_type,
                is_complete,
                pages_fetched,
                source_url,
                last_attempted_at,
                last_error,
                failure_count,
                updated_at
            )
            VALUES (?, 'fetched', ?, CURRENT_TIMESTAMP, ?, 1, ?, ?, CURRENT_TIMESTAMP, NULL, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(source_mal_anime_id) DO UPDATE SET
                status = excluded.status,
                num_edges = excluded.num_edges,
                fetched_at = excluded.fetched_at,
                source_type = excluded.source_type,
                is_complete = excluded.is_complete,
                pages_fetched = excluded.pages_fetched,
                source_url = excluded.source_url,
                last_attempted_at = excluded.last_attempted_at,
                last_error = NULL,
                failure_count = 0,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                source_id,
                len(edges),
                MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                len(pages),
                source_url,
            ),
        )
        _execute_public_userrecs_publication_statement(
            conn,
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'published', published_at = CURRENT_TIMESTAMP, generation_revision = generation_revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ? AND status = 'ready'
            """,
            (int(generation_id),),
        )
        _record_public_userrecs_event(
            conn,
            generation_id=int(generation_id),
            source_mal_anime_id=source_id,
            event_type="publish",
        )
        result = MalPublicUserRecsPublicationResult(
            generation_id=int(generation_id),
            source_mal_anime_id=source_id,
            published_edge_count=len(edges),
            pages_fetched=len(pages),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return result


_PUBLIC_USERRECS_DIAGNOSTIC_TABLES: frozenset[str] = frozenset(
    {
        "mal_user_anime_list_cache",
        "mal_recommendation_harvest_status",
        "mal_public_userrecs_crawl_generations",
        "mal_public_userrecs_staged_pages",
        "mal_public_userrecs_staged_edges",
        "mal_public_userrecs_crawl_events",
        "mal_public_userrecs_source_queue",
    }
)
_PUBLIC_USERRECS_DIAGNOSTIC_STATUSES: frozenset[str] = frozenset({"ok", "degraded", "unknown"})


def _safe_nonnegative_int(value: Any) -> int:
    try:
        if value is None or isinstance(value, bool):
            return 0
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(max(0, int(numerator)) / denominator, 6)


def _utc_iso_from_db_timestamp(value: Any) -> str | None:
    text = _coerce_non_empty_text(value)
    if text is None:
        return None
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _diagnostic_error_code(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unspecified"
    if "rate" in text and "limit" in text:
        return "rate_limited"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "parser" in text or "parse" in text:
        return "parse_error"
    if "fingerprint" in text or "drift" in text or "loop" in text or "cursor" in text:
        return "pagination_drift"
    if "validation" in text or "invalid" in text:
        return "validation_error"
    if "max_pages" in text or "max pages" in text:
        return "page_budget_reached"
    words = re.findall(r"[a-z0-9]+", text)
    return "_".join(words[:4])[:80] or "error"


def _public_userrecs_schema_diagnostic(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    tables = {str(row["name"]) for row in rows}
    missing = sorted(_PUBLIC_USERRECS_DIAGNOSTIC_TABLES - tables)
    return not missing, missing


def unknown_public_userrecs_diagnostics(*, reason: str, status: str = "unknown") -> dict[str, Any]:
    """Return the stable read-only public-userrecs diagnostic shape when data is unavailable."""
    normalized_status = status if status in _PUBLIC_USERRECS_DIAGNOSTIC_STATUSES else "unknown"
    reason_codes = [str(reason or "unknown").strip() or "unknown"]
    return {
        "status": normalized_status,
        "reason_codes": reason_codes,
        "policy": {
            "authorized_source_titles_per_hour": 3,
            "configured_source_titles_per_hour": None,
            "configured_source_titles_per_hour_source": "unknown",
            "max_pages_per_source_per_run": None,
            "stale_horizon_days": None,
            "stale_horizon_source": "unknown",
        },
        "positive_seed_count": 0,
        "coverage": {
            "complete": 0,
            "fresh": 0,
            "stale": 0,
            "failed": 0,
            "unharvested": 0,
            "fresh_ratio": None,
            "total": 0,
            "semantics": "complete public-userrecs harvest sources only",
        },
        "queue": {
            "counts_by_class": {},
            "oldest_never_started_at": None,
            "oldest_open_at": None,
            "last_selected_at": None,
            "max_selection_sequence": 0,
            "fairness_lag_seconds": None,
            "claimed": 0,
            "retry_scheduled": 0,
            "quarantined": 0,
        },
        "open_generations": {
            "active": 0,
            "paused": 0,
            "ready": 0,
            "total": 0,
            "staged_pages": 0,
            "staged_edges": 0,
            "by_status": {
                "active": {"count": 0, "staged_pages": 0, "staged_edges": 0},
                "paused": {"count": 0, "staged_pages": 0, "staged_edges": 0},
                "ready": {"count": 0, "staged_pages": 0, "staged_edges": 0},
            },
        },
        "hourly_throughput": {
            "pages_fetched_last_hour": 0,
            "page_events_last_hour": 0,
            "last_page_fetched_at": None,
            "sources_started_last_hour": 0,
            "sources_published_last_hour": 0,
        },
        "events": {
            "recent_window_hours": 24,
            "page_events_recent": 0,
            "error_events_recent": 0,
            "last_page_event_at": None,
            "last_error_event_at": None,
        },
        "errors": {
            "source_error_counts": {},
            "recent_24h_error_events": 0,
            "last_error": None,
        },
        "backlog": {
            "due_sources": None,
            "source_start_eta": {"eta_hours": None, "reason_code": "unknown"},
            "conservative_source_start_eta": None,
            "conservative_source_start_eta_hours": None,
            "completion_eta": None,
            "completion_eta_hours": None,
            "completion_eta_reason_codes": ["unknown_pages_per_source"],
            "completion_eta_detail": {"eta_hours": None, "reason_code": "unknown_pages_per_source"},
        },
        "sustainability": {
            "authorized_source_titles_per_hour": 3,
            "configured_source_titles_per_hour": None,
            "sources_started_last_hour": 0,
            "within_authorized_rate": None,
            "over_authorized_by": 0,
            "configured_exceeds_authorized": None,
        },
    }


def get_public_userrecs_diagnostics(
    db_path: Path,
    *,
    configured_source_titles_per_hour: int | None = None,
    max_pages_per_source_per_run: int | None = None,
    stale_after_days: int = 45,
    authorized_source_titles_per_hour: int = 3,
) -> dict[str, Any]:
    """Return read-only public MAL userrecs crawl diagnostics for dashboards.

    This intentionally avoids raw URLs, usernames, recommendation prose, and title
    samples. Completion ETA remains unknown because MAL public userrecs exposes no
    durable total-pages-per-source value until crawling reaches the terminal page.
    """
    stale_after_days = max(1, int(stale_after_days))
    authorized = max(0, int(authorized_source_titles_per_hour))
    configured = None if configured_source_titles_per_hour is None else max(0, int(configured_source_titles_per_hour))
    max_pages = None if max_pages_per_source_per_run is None else max(1, int(max_pages_per_source_per_run))
    try:
        with connect(db_path) as conn:
            schema_present, missing_tables = _public_userrecs_schema_diagnostic(conn)
            if not schema_present:
                payload = unknown_public_userrecs_diagnostics(
                    reason="public_userrecs_staging_schema_absent",
                    status="unknown",
                )
                payload["reason_codes"].append("missing_tables:" + ",".join(missing_tables))
                payload["policy"].update(
                    {
                        "configured_source_titles_per_hour": configured,
                        "configured_source_titles_per_hour_source": "task_execute_limit" if configured is not None else "unknown",
                        "max_pages_per_source_per_run": max_pages,
                        "stale_horizon_days": stale_after_days,
                        "stale_horizon_source": "configured_or_default",
                    }
                )
                payload["sustainability"].update(
                    {
                        "configured_source_titles_per_hour": configured,
                        "configured_exceeds_authorized": None if configured is None else configured > authorized,
                    }
                )
                return payload

            seed_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM mal_user_anime_list_cache
                WHERE list_status IN ('completed', 'watching', 'on_hold')
                """
            ).fetchone()
            positive_seed_count = _safe_nonnegative_int(seed_row["count"] if seed_row is not None else 0)
            status_rows = conn.execute(
                """
                SELECT
                    source_mal_anime_id,
                    status,
                    fetched_at,
                    source_type,
                    is_complete,
                    last_attempted_at,
                    last_error,
                    failure_count
                FROM mal_recommendation_harvest_status
                WHERE source_mal_anime_id IN (
                    SELECT mal_anime_id
                    FROM mal_user_anime_list_cache
                    WHERE list_status IN ('completed', 'watching', 'on_hold')
                )
                """
            ).fetchall()
            status_by_source = {int(row["source_mal_anime_id"]): row for row in status_rows}
            seed_rows = conn.execute(
                """
                SELECT mal_anime_id
                FROM mal_user_anime_list_cache
                WHERE list_status IN ('completed', 'watching', 'on_hold')
                ORDER BY mal_anime_id ASC
                """
            ).fetchall()
            threshold = datetime.now(timezone.utc) - timedelta(days=stale_after_days)
            coverage_counts = {"complete": 0, "fresh": 0, "stale": 0, "failed": 0, "unharvested": 0}
            due_sources = 0
            source_error_counts: dict[str, int] = {}
            source_ids = [int(row["mal_anime_id"]) for row in seed_rows]
            for source_id in source_ids:
                row = status_by_source.get(source_id)
                if row is None:
                    coverage_counts["unharvested"] += 1
                    due_sources += 1
                    continue
                last_error = row["last_error"]
                failure_count = _safe_nonnegative_int(row["failure_count"])
                if last_error:
                    source_error_counts[_diagnostic_error_code(last_error)] = source_error_counts.get(_diagnostic_error_code(last_error), 0) + max(1, failure_count)
                is_complete_public = int(row["is_complete"] or 0) == 1 and str(row["source_type"] or "") == MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS
                if is_complete_public:
                    coverage_counts["complete"] += 1
                if str(row["status"] or "") == "failed":
                    coverage_counts["failed"] += 1
                    due_sources += 1
                    continue
                if not is_complete_public:
                    coverage_counts["unharvested"] += 1
                    due_sources += 1
                    continue
                fetched_at = _utc_iso_from_db_timestamp(row["fetched_at"])
                parsed = None
                if fetched_at:
                    try:
                        parsed = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                    except ValueError:
                        parsed = None
                if parsed is not None and parsed.astimezone(timezone.utc) >= threshold:
                    coverage_counts["fresh"] += 1
                else:
                    coverage_counts["stale"] += 1
                    due_sources += 1

            open_rows = conn.execute(
                """
                SELECT
                    status,
                    COUNT(*) AS generation_count,
                    COALESCE(SUM(pages_fetched), 0) AS pages,
                    COALESCE(SUM(staged_edge_count), 0) AS edges
                FROM mal_public_userrecs_crawl_generations
                WHERE status IN ('active', 'paused', 'ready')
                GROUP BY status
                """
            ).fetchall()
            open_counts = {
                "active": 0,
                "paused": 0,
                "ready": 0,
                "total": 0,
                "staged_pages": 0,
                "staged_edges": 0,
                "by_status": {
                    "active": {"count": 0, "staged_pages": 0, "staged_edges": 0},
                    "paused": {"count": 0, "staged_pages": 0, "staged_edges": 0},
                    "ready": {"count": 0, "staged_pages": 0, "staged_edges": 0},
                },
            }
            for row in open_rows:
                status = str(row["status"])
                count = _safe_nonnegative_int(row["generation_count"])
                pages = _safe_nonnegative_int(row["pages"])
                edges = _safe_nonnegative_int(row["edges"])
                if status in {"active", "paused", "ready"}:
                    open_counts[status] = count
                    open_counts["by_status"][status] = {"count": count, "staged_pages": pages, "staged_edges": edges}
                open_counts["total"] += count
                open_counts["staged_pages"] += pages
                open_counts["staged_edges"] += edges
            actual_open_stage_row = conn.execute(
                """
                WITH open_generations AS (
                    SELECT generation_id
                    FROM mal_public_userrecs_crawl_generations
                    WHERE status IN ('active', 'paused', 'ready')
                )
                SELECT
                    (SELECT COUNT(*) FROM mal_public_userrecs_staged_pages p JOIN open_generations g ON g.generation_id = p.generation_id) AS actual_pages,
                    (SELECT COUNT(*) FROM mal_public_userrecs_staged_edges e JOIN open_generations g ON g.generation_id = e.generation_id) AS actual_edges
                """
            ).fetchone()
            open_counts["actual_staged_pages"] = _safe_nonnegative_int(actual_open_stage_row["actual_pages"] if actual_open_stage_row is not None else 0)
            open_counts["actual_staged_edges"] = _safe_nonnegative_int(actual_open_stage_row["actual_edges"] if actual_open_stage_row is not None else 0)

            throughput_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN datetime(fetched_at) >= datetime('now', '-1 hour') THEN 1 ELSE 0 END), 0) AS pages_last_hour,
                    MAX(fetched_at) AS last_page_fetched_at
                FROM mal_public_userrecs_staged_pages
                """
            ).fetchone()
            page_event_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN datetime(created_at) >= datetime('now', '-1 hour') THEN 1 ELSE 0 END), 0) AS page_events_last_hour,
                    COALESCE(SUM(CASE WHEN datetime(created_at) >= datetime('now', '-24 hours') THEN 1 ELSE 0 END), 0) AS page_events_recent,
                    MAX(created_at) AS last_page_event_at
                FROM mal_public_userrecs_crawl_events
                WHERE event_type = 'page_upsert'
                """
            ).fetchone()
            started_row = conn.execute(
                """
                SELECT COUNT(DISTINCT generation_id) AS count
                FROM mal_public_userrecs_crawl_events
                WHERE event_type = 'begin'
                  AND datetime(created_at) >= datetime('now', '-1 hour')
                """
            ).fetchone()
            published_row = conn.execute(
                """
                SELECT COUNT(DISTINCT generation_id) AS count
                FROM mal_public_userrecs_crawl_events
                WHERE event_type = 'publish'
                  AND datetime(created_at) >= datetime('now', '-1 hour')
                """
            ).fetchone()
            error_events_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM mal_public_userrecs_crawl_events
                WHERE error IS NOT NULL
                  AND TRIM(error) <> ''
                  AND datetime(created_at) >= datetime('now', '-24 hours')
                """
            ).fetchone()
            error_code_rows = conn.execute(
                """
                SELECT error, COUNT(*) AS count
                FROM mal_public_userrecs_crawl_events
                WHERE error IS NOT NULL
                  AND TRIM(error) <> ''
                GROUP BY error
                """
            ).fetchall()
            for row in error_code_rows:
                code = _diagnostic_error_code(row["error"])
                source_error_counts[code] = source_error_counts.get(code, 0) + _safe_nonnegative_int(row["count"])
            last_error_row = conn.execute(
                """
                SELECT event_type, created_at, error
                FROM mal_public_userrecs_crawl_events
                WHERE error IS NOT NULL
                  AND TRIM(error) <> ''
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            queue_rows = conn.execute(
                """
                SELECT queue_class, COUNT(*) AS count,
                       MIN(CASE WHEN queue_class = 'never_started' THEN class_entered_at END) AS oldest_never_started_at,
                       MIN(CASE WHEN queue_class = 'resumable' THEN class_entered_at END) AS oldest_open_at,
                       MAX(last_selected_at) AS last_selected_at,
                       MAX(selection_sequence) AS max_selection_sequence,
                       SUM(CASE WHEN claim_token IS NOT NULL AND datetime(claim_expires_at) > datetime('now') THEN 1 ELSE 0 END) AS claimed,
                       SUM(CASE WHEN next_retry_at IS NOT NULL THEN 1 ELSE 0 END) AS retry_scheduled,
                       SUM(CASE WHEN queue_class = 'quarantined' THEN 1 ELSE 0 END) AS quarantined
                FROM mal_public_userrecs_source_queue
                WHERE eligible = 1
                GROUP BY queue_class
                """
            ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower() or "no such column" in str(exc).lower():
            return unknown_public_userrecs_diagnostics(reason="public_userrecs_diagnostic_schema_absent", status="unknown")
        raise

    pages_last_hour = _safe_nonnegative_int(throughput_row["pages_last_hour"] if throughput_row is not None else 0)
    sources_started_last_hour = _safe_nonnegative_int(started_row["count"] if started_row is not None else 0)
    sources_published_last_hour = _safe_nonnegative_int(published_row["count"] if published_row is not None else 0)
    source_start_rate = authorized if configured is None else min(authorized, configured)
    conservative_eta_hours = None
    if due_sources == 0:
        conservative_eta_hours = 0
    elif source_start_rate > 0:
        conservative_eta_hours = int(math.ceil(due_sources / source_start_rate))
    if due_sources == 0:
        source_start_eta = {"eta_hours": 0, "reason_code": "no_due_sources"}
    elif source_start_rate <= 0:
        source_start_eta = {"eta_hours": None, "reason_code": "source_start_rate_zero"}
    else:
        source_start_eta = {
            "eta_hours": conservative_eta_hours,
            "reason_code": "authorized_source_title_rate_estimate",
            "basis": "due source count divided by the authorized/configured hourly source-start rate",
        }
    completion_eta_detail = {
        "eta_hours": None,
        "reason_code": "unknown_pages_per_source",
        "basis": "public MAL userrecs exposes no durable total remaining page count until terminal next-link discovery",
    }
    reason_codes: list[str] = []
    if positive_seed_count == 0:
        reason_codes.append("no_positive_seeds")
    if due_sources > 0:
        reason_codes.append("due_sources")
    if open_counts["total"]:
        reason_codes.append("open_generations")
    if due_sources > 0 or open_counts["total"]:
        reason_codes.append("unknown_pages_per_source")
    if configured is None:
        reason_codes.append("configured_source_titles_per_hour_unknown")
    if max_pages is None:
        reason_codes.append("max_pages_per_source_per_run_unknown")
    if source_start_rate <= 0 and due_sources > 0:
        reason_codes.append("source_start_rate_zero")
    if configured is not None and configured > authorized:
        reason_codes.append("configured_source_titles_exceed_authorized")
    if sources_started_last_hour > authorized:
        reason_codes.append("sources_started_last_hour_exceed_authorized")
    if open_counts["paused"]:
        reason_codes.append("paused_generations")
    if coverage_counts["failed"]:
        reason_codes.append("failed_sources")
    if source_error_counts:
        reason_codes.append("source_errors")
    if _safe_nonnegative_int(error_events_row["count"] if error_events_row is not None else 0):
        reason_codes.append("recent_error_events")
    if positive_seed_count == 0:
        status = "unknown"
    else:
        status = "ok" if not reason_codes else "degraded"
    if not reason_codes:
        reason_codes = ["ok"]
    queue_counts = {str(row["queue_class"]): _safe_nonnegative_int(row["count"]) for row in queue_rows}
    queue_oldest_never = next((row["oldest_never_started_at"] for row in queue_rows if row["oldest_never_started_at"]), None)
    queue_oldest_open = next((row["oldest_open_at"] for row in queue_rows if row["oldest_open_at"]), None)
    queue_last_selected = max((str(row["last_selected_at"]) for row in queue_rows if row["last_selected_at"]), default=None)
    queue_max_selection_sequence = max(
        (_safe_nonnegative_int(row["max_selection_sequence"]) for row in queue_rows), default=0
    )
    fairness_lag_seconds = None
    if queue_oldest_never:
        parsed_oldest = _utc_iso_from_db_timestamp(queue_oldest_never)
        if parsed_oldest:
            try:
                fairness_lag_seconds = max(0, int((datetime.now(timezone.utc) - datetime.fromisoformat(parsed_oldest.replace("Z", "+00:00"))).total_seconds()))
            except ValueError:
                fairness_lag_seconds = None
    return {
        "status": status,
        "reason_codes": reason_codes,
        "policy": {
            "authorized_source_titles_per_hour": authorized,
            "configured_source_titles_per_hour": configured,
            "configured_source_titles_per_hour_source": "task_execute_limit" if configured is not None else "unknown",
            "max_pages_per_source_per_run": max_pages,
            "stale_horizon_days": stale_after_days,
            "stale_horizon_source": "configured_or_default",
        },
        "positive_seed_count": positive_seed_count,
        "coverage": {
            **coverage_counts,
            "fresh_ratio": _safe_ratio(coverage_counts["fresh"], positive_seed_count),
            "total": positive_seed_count,
            "semantics": "complete public-userrecs harvest sources only",
        },
        "queue": {
            "counts_by_class": queue_counts,
            "oldest_never_started_at": _utc_iso_from_db_timestamp(queue_oldest_never),
            "oldest_open_at": _utc_iso_from_db_timestamp(queue_oldest_open),
            "last_selected_at": _utc_iso_from_db_timestamp(queue_last_selected),
            "max_selection_sequence": queue_max_selection_sequence,
            "fairness_lag_seconds": fairness_lag_seconds,
            "claimed": sum(_safe_nonnegative_int(row["claimed"]) for row in queue_rows),
            "retry_scheduled": sum(_safe_nonnegative_int(row["retry_scheduled"]) for row in queue_rows),
            "quarantined": sum(_safe_nonnegative_int(row["quarantined"]) for row in queue_rows),
            "priority": ["never_started", "resumable", "retry_due", "refresh_due"],
            "fairness": "monotonic selection sequence then oldest class entry within strict class priority",
        },
        "open_generations": open_counts,
        "hourly_throughput": {
            "pages_fetched_last_hour": pages_last_hour,
            "page_events_last_hour": _safe_nonnegative_int(page_event_row["page_events_last_hour"] if page_event_row is not None else 0),
            "last_page_fetched_at": _utc_iso_from_db_timestamp(throughput_row["last_page_fetched_at"] if throughput_row is not None else None),
            "sources_started_last_hour": sources_started_last_hour,
            "sources_published_last_hour": sources_published_last_hour,
        },
        "events": {
            "recent_window_hours": 24,
            "page_events_recent": _safe_nonnegative_int(page_event_row["page_events_recent"] if page_event_row is not None else 0),
            "error_events_recent": _safe_nonnegative_int(error_events_row["count"] if error_events_row is not None else 0),
            "last_page_event_at": _utc_iso_from_db_timestamp(page_event_row["last_page_event_at"] if page_event_row is not None else None),
            "last_error_event_at": _utc_iso_from_db_timestamp(last_error_row["created_at"] if last_error_row is not None else None),
        },
        "errors": {
            "source_error_counts": dict(sorted(source_error_counts.items())),
            "recent_24h_error_events": _safe_nonnegative_int(error_events_row["count"] if error_events_row is not None else 0),
            "last_error": None
            if last_error_row is None
            else {
                "code": _diagnostic_error_code(last_error_row["error"]),
                "event_type": last_error_row["event_type"],
                "created_at": _utc_iso_from_db_timestamp(last_error_row["created_at"]),
            },
        },
        "backlog": {
            "due_sources": due_sources,
            "source_start_eta": source_start_eta,
            "conservative_source_start_eta": conservative_eta_hours,
            "conservative_source_start_eta_hours": conservative_eta_hours,
            "completion_eta": completion_eta_detail,
            "completion_eta_hours": None,
            "completion_eta_reason_codes": [str(completion_eta_detail["reason_code"])],
            "completion_eta_detail": completion_eta_detail,
        },
        "sustainability": {
            "authorized_source_titles_per_hour": authorized,
            "configured_source_titles_per_hour": configured,
            "sources_started_last_hour": sources_started_last_hour,
            "within_authorized_rate": sources_started_last_hour <= authorized,
            "over_authorized_by": max(0, sources_started_last_hour - authorized),
            "configured_exceeds_authorized": None if configured is None else configured > authorized,
        },
    }


def get_mal_recommendation_harvest_coverage(db_path: Path, *, stale_after_days: int = 14) -> dict[str, Any]:
    stale_after_days = max(int(stale_after_days), 0)
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH mapped AS (
                SELECT
                    m.mal_anime_id,
                    COUNT(DISTINCT m.provider || ':' || m.provider_series_id) AS mapped_series_count,
                    MAX(CASE WHEN w.provider_series_id IS NOT NULL OR p.provider_episode_id IS NOT NULL THEN 1 ELSE 0 END) AS watched
                FROM mal_series_mapping m
                LEFT JOIN provider_watchlist w
                    ON w.provider = m.provider AND w.provider_series_id = m.provider_series_id
                   AND w.is_active = 1
                LEFT JOIN provider_episode_progress p
                    ON p.provider = m.provider AND p.provider_series_id = m.provider_series_id
                GROUP BY m.mal_anime_id
            ), positive_list AS (
                SELECT mal_anime_id, list_status
                FROM mal_user_anime_list_cache
                WHERE list_status IN ('completed', 'watching', 'on_hold')
            ), seeds AS (
                SELECT
                    mapped.mal_anime_id AS mal_anime_id,
                    mapped.mapped_series_count AS mapped_series_count,
                    mapped.watched AS watched,
                    positive_list.list_status AS list_status,
                    CASE WHEN positive_list.mal_anime_id IS NOT NULL THEN 1 ELSE 0 END AS positive_list_seed
                FROM mapped
                LEFT JOIN positive_list ON positive_list.mal_anime_id = mapped.mal_anime_id
                UNION ALL
                SELECT
                    positive_list.mal_anime_id AS mal_anime_id,
                    0 AS mapped_series_count,
                    0 AS watched,
                    positive_list.list_status AS list_status,
                    1 AS positive_list_seed
                FROM positive_list
                WHERE NOT EXISTS (SELECT 1 FROM mapped WHERE mapped.mal_anime_id = positive_list.mal_anime_id)
            ), edge_counts AS (
                SELECT
                    source_mal_anime_id,
                    COUNT(*) AS edge_count,
                    MAX(fetched_at) AS edge_fetched_at,
                    MAX(CASE WHEN complete_harvest THEN 1 ELSE 0 END) AS has_complete_edges,
                    MAX(harvest_source) AS edge_harvest_source
                FROM mal_anime_recommendations
                WHERE source_kind = 'mal_recommendation'
                GROUP BY source_mal_anime_id
            )
            SELECT
                seeds.mal_anime_id,
                seeds.mapped_series_count,
                seeds.watched,
                seeds.list_status,
                seeds.positive_list_seed,
                COALESCE(status.num_edges, edge_counts.edge_count, 0) AS edge_count,
                COALESCE(status.fetched_at, edge_counts.edge_fetched_at) AS fetched_at,
                status.status AS harvest_status,
                COALESCE(status.source_type, edge_counts.edge_harvest_source) AS source_type,
                COALESCE(status.is_complete, edge_counts.has_complete_edges, 0) AS is_complete,
                COALESCE(status.pages_fetched, 0) AS pages_fetched,
                status.source_url,
                status.last_attempted_at,
                status.last_error,
                COALESCE(status.failure_count, 0) AS failure_count
            FROM seeds
            LEFT JOIN edge_counts ON edge_counts.source_mal_anime_id = seeds.mal_anime_id
            LEFT JOIN mal_recommendation_harvest_status status ON status.source_mal_anime_id = seeds.mal_anime_id
            ORDER BY seeds.mal_anime_id ASC
            """
        ).fetchall()
    items: list[dict[str, Any]] = []
    summary = {
        "mapped_sources": 0,
        "watched_sources": 0,
        "positive_list_sources": 0,
        "confirmed_positive_sources": 0,
        "fresh": 0,
        "stale": 0,
        "failed": 0,
        "unharvested": 0,
        "complete_full_harvest_sources": 0,
        "official_detail_sources": 0,
        "total_edges": 0,
    }
    for row in rows:
        fetched_at = row["fetched_at"]
        stored_status = str(row["harvest_status"] or "")
        if stored_status == "failed":
            status = "failed"
        elif fetched_at:
            status = "fresh"
            if stale_after_days > 0:
                with connect(db_path) as conn:
                    is_stale = conn.execute(
                        "SELECT datetime(?) < datetime('now', ?)",
                        (fetched_at, f"-{stale_after_days} days"),
                    ).fetchone()[0]
                status = "stale" if is_stale else "fresh"
        else:
            status = "unharvested"
        edge_count = int(row["edge_count"] or 0)
        watched = bool(row["watched"])
        mapped = int(row["mapped_series_count"] or 0) > 0
        positive_list_seed = bool(row["positive_list_seed"])
        if mapped:
            summary["mapped_sources"] += 1
        summary["watched_sources"] += 1 if watched else 0
        summary["positive_list_sources"] += 1 if positive_list_seed else 0
        summary["confirmed_positive_sources"] += 1 if positive_list_seed or watched else 0
        summary[status] += 1
        if int(row["is_complete"] or 0):
            summary["complete_full_harvest_sources"] += 1
        if (fetched_at or edge_count > 0) and str(row["source_type"] or "") == MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL:
            summary["official_detail_sources"] += 1
        summary["total_edges"] += edge_count
        items.append(
            {
                "mal_anime_id": int(row["mal_anime_id"]),
                "mapped_series_count": int(row["mapped_series_count"] or 0),
                "watched": watched,
                "positive_list_seed": positive_list_seed,
                "list_status": row["list_status"],
                "edge_count": edge_count,
                "fetched_at": fetched_at,
                "status": status,
                "source_type": str(row["source_type"]) if row["source_type"] else None,
                "is_complete": bool(row["is_complete"]),
                "pages_fetched": int(row["pages_fetched"] or 0),
                "source_url": row["source_url"],
                "last_attempted_at": row["last_attempted_at"],
                "last_error": row["last_error"],
                "failure_count": int(row["failure_count"] or 0),
            }
        )
    coverage_denominator = int(summary["confirmed_positive_sources"] or summary["mapped_sources"])
    coverage = None if coverage_denominator == 0 else (summary["fresh"] / coverage_denominator)
    summary["fresh_coverage_ratio"] = coverage
    return {"summary": summary, "sources": items}


def get_mal_recommendation_edges_map(db_path: Path) -> dict[int, list[MalRecommendationEdge]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                source_mal_anime_id,
                target_mal_anime_id,
                target_title,
                num_recommendations,
                hop_distance,
                source_kind,
                raw_json,
                fetched_at
            FROM mal_anime_recommendations
            ORDER BY source_mal_anime_id ASC, num_recommendations DESC, target_mal_anime_id ASC
            """
        ).fetchall()
    result: dict[int, list[MalRecommendationEdge]] = {}
    for row in rows:
        result.setdefault(int(row["source_mal_anime_id"]), []).append(
            MalRecommendationEdge(
                source_mal_anime_id=int(row["source_mal_anime_id"]),
                target_mal_anime_id=int(row["target_mal_anime_id"]),
                target_title=row["target_title"],
                num_recommendations=None if row["num_recommendations"] is None else int(row["num_recommendations"]),
                hop_distance=int(row["hop_distance"]),
                source_kind=str(row["source_kind"]),
                raw=json.loads(row["raw_json"]),
                fetched_at=str(row["fetched_at"]),
            )
        )
    return result


def get_provider_series_title_map(
    db_path: Path,
    *,
    provider: str,
    provider_series_ids: Iterable[str],
) -> dict[str, dict[str, str | None]]:
    normalized_ids = sorted({value for value in provider_series_ids if isinstance(value, str) and value})
    if not normalized_ids:
        return {}
    placeholders = ", ".join("?" for _ in normalized_ids)
    query = f"""
        SELECT provider_series_id, title, season_title
        FROM provider_series
        WHERE provider = ? AND provider_series_id IN ({placeholders})
    """
    with connect(db_path) as conn:
        rows = conn.execute(query, [provider, *normalized_ids]).fetchall()
    return {
        str(row["provider_series_id"]): {
            "title": row["title"],
            "season_title": row["season_title"],
        }
        for row in rows
    }


def get_provider_series_title_map_by_keys(
    db_path: Path,
    *,
    provider_series_keys: Iterable[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, str | None]]:
    normalized_keys = sorted(
        {
            (provider.strip(), provider_series_id.strip())
            for provider, provider_series_id in provider_series_keys
            if isinstance(provider, str)
            and provider.strip()
            and isinstance(provider_series_id, str)
            and provider_series_id.strip()
        }
    )
    if not normalized_keys:
        return {}

    conditions = " OR ".join("(provider = ? AND provider_series_id = ?)" for _ in normalized_keys)
    query = f"""
        SELECT provider, provider_series_id, title, season_title
        FROM provider_series
        WHERE {conditions}
    """
    params: list[str] = []
    for provider, provider_series_id in normalized_keys:
        params.extend([provider, provider_series_id])

    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return {
        (str(row["provider"]), str(row["provider_series_id"])): {
            "title": row["title"],
            "season_title": row["season_title"],
        }
        for row in rows
    }


def list_review_queue_entries(
    db_path: Path,
    *,
    status: str = "open",
    issue_type: str | None = None,
    provider_series_id: str | None = None,
) -> list[ReviewQueueEntry]:
    query = """
        SELECT
            id,
            provider,
            provider_series_id,
            provider_episode_id,
            issue_type,
            severity,
            payload_json,
            status,
            created_at,
            resolved_at
        FROM review_queue
        WHERE status = ?
    """
    params: list[object] = [status]
    if issue_type is not None:
        query += " AND issue_type = ?"
        params.append(issue_type)
    normalized_provider_series_id = (
        provider_series_id.strip()
        if isinstance(provider_series_id, str) and provider_series_id.strip()
        else None
    )
    if normalized_provider_series_id is not None:
        query += " AND provider_series_id = ?"
        params.append(normalized_provider_series_id)
    query += " ORDER BY created_at DESC, id DESC"
    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        ReviewQueueEntry(
            id=int(row["id"]),
            provider=row["provider"],
            provider_series_id=row["provider_series_id"],
            provider_episode_id=row["provider_episode_id"],
            issue_type=row["issue_type"],
            severity=row["severity"],
            payload=json.loads(row["payload_json"]),
            status=row["status"],
            created_at=row["created_at"],
            resolved_at=row["resolved_at"],
        )
        for row in rows
    ]


def update_review_queue_entry_statuses(
    db_path: Path,
    *,
    entry_ids: Iterable[int],
    status: str,
) -> int:
    normalized_ids = sorted({int(value) for value in entry_ids})
    if not normalized_ids:
        return 0
    placeholders = ", ".join("?" for _ in normalized_ids)
    query = f"""
        UPDATE review_queue
        SET
            status = ?,
            resolved_at = CASE WHEN ? = 'resolved' THEN CURRENT_TIMESTAMP ELSE NULL END
        WHERE id IN ({placeholders})
    """
    with connect(db_path) as conn:
        cursor = conn.execute(query, [status, status, *normalized_ids])
        conn.commit()
        return int(cursor.rowcount or 0)



def get_operational_snapshot(db_path: Path) -> dict[str, Any]:
    with connect(db_path) as conn:
        latest_sync_run_row = conn.execute(
            """
            SELECT id, provider, contract_version, mode, started_at, completed_at, status, summary_json
            FROM sync_runs
            ORDER BY started_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        latest_completed_sync_run_row = conn.execute(
            """
            SELECT id, provider, contract_version, mode, started_at, completed_at, status, summary_json
            FROM sync_runs
            WHERE status = 'completed'
            ORDER BY completed_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        provider_series_counts = {
            str(row["provider"]): int(row["count"])
            for row in conn.execute(
                "SELECT provider, COUNT(*) AS count FROM provider_series GROUP BY provider"
            ).fetchall()
        }
        provider_series_provenance_rows = conn.execute(
            """
            SELECT
                COUNT(*) AS persisted_total,
                SUM(CASE WHEN account_observed_at IS NULL THEN 1 ELSE 0 END) AS catalog_only,
                SUM(CASE WHEN account_observed_at IS NOT NULL THEN 1 ELSE 0 END) AS mapping_eligible
            FROM provider_series
            """
        ).fetchone()
        eligible_mapping_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN m.approved_by_user = 1 THEN 1 ELSE 0 END) AS approved
            FROM mal_series_mapping m
            INNER JOIN provider_series s
                ON s.provider = m.provider
               AND s.provider_series_id = m.provider_series_id
            WHERE s.account_observed_at IS NOT NULL
            """
        ).fetchone()
        provider_progress_counts = {
            str(row["provider"]): int(row["count"])
            for row in conn.execute(
                "SELECT provider, COUNT(*) AS count FROM provider_episode_progress GROUP BY provider"
            ).fetchall()
        }
        provider_watchlist_counts = {
            str(row["provider"]): int(row["count"])
            for row in conn.execute(
                "SELECT provider, COUNT(*) AS count FROM provider_watchlist GROUP BY provider"
            ).fetchall()
        }
        hidive_progress_provenance_row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN progress_observation_kind IN ('position', 'ratio', 'explicit_completed', 'inferred_later_episode') THEN 1 ELSE 0 END) AS confirmed_or_measured,
                SUM(CASE WHEN progress_observation_kind = 'history_membership' OR (completion_assertion = 'unknown' AND progress_source_surface = 'hidive_history') THEN 1 ELSE 0 END) AS history_only_unknown,
                SUM(CASE WHEN progress_source_surface IS NULL AND progress_observation_kind IS NULL
                              AND completion_assertion IS NULL AND normalization_logic_version IS NULL
                              AND completion_ratio = 1.0 AND duration_ms > 0
                              AND playback_position_ms = duration_ms THEN 1 ELSE 0 END) AS legacy_unproven
            FROM provider_episode_progress
            WHERE provider = 'hidive'
            """
        ).fetchone()
        series_freshness_rows = conn.execute(
            "SELECT provider, MAX(last_seen_at) AS last_seen_at FROM provider_series GROUP BY provider"
        ).fetchall()
        progress_freshness_rows = conn.execute(
            "SELECT provider, MAX(last_seen_at) AS last_seen_at FROM provider_episode_progress GROUP BY provider"
        ).fetchall()
        watchlist_freshness_rows = conn.execute(
            "SELECT provider, MAX(last_seen_at) AS last_seen_at FROM provider_watchlist GROUP BY provider"
        ).fetchall()
        review_rows = conn.execute(
            """
            SELECT status, issue_type, COUNT(*) AS count
            FROM review_queue
            GROUP BY status, issue_type
            ORDER BY status ASC, issue_type ASC
            """
        ).fetchall()
        mapping_rows = conn.execute(
            """
            SELECT provider, approved_by_user, mapping_source, COUNT(*) AS count
            FROM mal_series_mapping
            GROUP BY provider, approved_by_user, mapping_source
            ORDER BY provider ASC, approved_by_user DESC, mapping_source ASC
            """
        ).fetchall()
        metadata_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN rank IS NOT NULL THEN 1 ELSE 0 END) AS with_rank,
                SUM(CASE WHEN num_list_users IS NOT NULL THEN 1 ELSE 0 END) AS with_num_list_users,
                SUM(CASE WHEN num_scoring_users IS NOT NULL THEN 1 ELSE 0 END) AS with_num_scoring_users,
                SUM(CASE WHEN rating IS NOT NULL THEN 1 ELSE 0 END) AS with_rating,
                SUM(CASE WHEN average_episode_duration IS NOT NULL THEN 1 ELSE 0 END) AS with_average_episode_duration,
                SUM(CASE WHEN start_date IS NOT NULL THEN 1 ELSE 0 END) AS with_start_date,
                SUM(CASE WHEN end_date IS NOT NULL THEN 1 ELSE 0 END) AS with_end_date,
                SUM(CASE WHEN broadcast_day IS NOT NULL OR broadcast_time IS NOT NULL OR broadcast_timezone IS NOT NULL THEN 1 ELSE 0 END) AS with_broadcast,
                SUM(CASE WHEN nsfw IS NOT NULL THEN 1 ELSE 0 END) AS with_nsfw
            FROM mal_anime_metadata
            """
        ).fetchone()

    def _sync_run_row_to_dict(row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        summary_json = row["summary_json"]
        return {
            "id": int(row["id"]),
            "provider": row["provider"],
            "contract_version": row["contract_version"],
            "mode": row["mode"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "status": row["status"],
            "summary": json.loads(summary_json) if summary_json else None,
        }

    provider_names = sorted(
        set(provider_series_counts)
        | set(provider_progress_counts)
        | set(provider_watchlist_counts)
        | {str(row["provider"]) for row in series_freshness_rows}
        | {str(row["provider"]) for row in progress_freshness_rows}
        | {str(row["provider"]) for row in watchlist_freshness_rows}
        | {str(row["provider"]) for row in mapping_rows}
    )

    provider_counts_by_provider: dict[str, dict[str, int]] = {}
    for provider in provider_names:
        provider_counts_by_provider[provider] = {
            "series": int(provider_series_counts.get(provider, 0)),
            "progress": int(provider_progress_counts.get(provider, 0)),
            "watchlist": int(provider_watchlist_counts.get(provider, 0)),
        }

    provider_freshness_by_provider: dict[str, dict[str, Any]] = {provider: {} for provider in provider_names}
    for row in series_freshness_rows:
        provider_freshness_by_provider.setdefault(str(row["provider"]), {})["series_last_seen_at"] = row["last_seen_at"]
    for row in progress_freshness_rows:
        provider_freshness_by_provider.setdefault(str(row["provider"]), {})["progress_last_seen_at"] = row["last_seen_at"]
    for row in watchlist_freshness_rows:
        provider_freshness_by_provider.setdefault(str(row["provider"]), {})["watchlist_last_seen_at"] = row["last_seen_at"]

    provider_counts = {
        "series": sum(item["series"] for item in provider_counts_by_provider.values()),
        "progress": sum(item["progress"] for item in provider_counts_by_provider.values()),
        "watchlist": sum(item["watchlist"] for item in provider_counts_by_provider.values()),
    }
    provider_series_inventory = {
        "persisted_total": int(provider_series_provenance_rows["persisted_total"] or 0),
        "catalog_only": int(provider_series_provenance_rows["catalog_only"] or 0),
        "mapping_eligible": int(provider_series_provenance_rows["mapping_eligible"] or 0),
        "eligible_mapping_total": int(eligible_mapping_row["total"] or 0),
        "eligible_mapping_approved": int(eligible_mapping_row["approved"] or 0),
    }
    provider_freshness = {
        "series_last_seen_at": max(
            (item.get("series_last_seen_at") for item in provider_freshness_by_provider.values() if item.get("series_last_seen_at")),
            default=None,
        ),
        "progress_last_seen_at": max(
            (item.get("progress_last_seen_at") for item in provider_freshness_by_provider.values() if item.get("progress_last_seen_at")),
            default=None,
        ),
        "watchlist_last_seen_at": max(
            (item.get("watchlist_last_seen_at") for item in provider_freshness_by_provider.values() if item.get("watchlist_last_seen_at")),
            default=None,
        ),
    }

    review_counts: dict[str, dict[str, int]] = {}
    for row in review_rows:
        status_key = str(row["status"])
        issue_type_key = str(row["issue_type"])
        review_counts.setdefault(status_key, {})[issue_type_key] = int(row["count"])

    mapping_counts = {
        "total": 0,
        "approved": 0,
        "by_source": {},
        "by_provider": {},
    }
    for row in mapping_rows:
        provider = str(row["provider"])
        count = int(row["count"])
        mapping_source = str(row["mapping_source"])
        approved_by_user = bool(row["approved_by_user"])
        mapping_counts["total"] += count
        if approved_by_user:
            mapping_counts["approved"] += count
        mapping_counts["by_source"][mapping_source] = mapping_counts["by_source"].get(mapping_source, 0) + count
        provider_bucket = mapping_counts["by_provider"].setdefault(
            provider,
            {"total": 0, "approved": 0, "by_source": {}},
        )
        provider_bucket["total"] += count
        if approved_by_user:
            provider_bucket["approved"] += count
        provider_bucket["by_source"][mapping_source] = provider_bucket["by_source"].get(mapping_source, 0) + count

    return {
        "latest_sync_run": _sync_run_row_to_dict(latest_sync_run_row),
        "latest_completed_sync_run": _sync_run_row_to_dict(latest_completed_sync_run_row),
        "provider_counts": provider_counts,
        "provider_series_inventory": provider_series_inventory,
        "provider_counts_by_provider": provider_counts_by_provider,
        "hidive_progress_provenance": {
            "confirmed_or_measured": int(hidive_progress_provenance_row["confirmed_or_measured"] or 0),
            "history_only_completion_unknown": int(hidive_progress_provenance_row["history_only_unknown"] or 0),
            "legacy_unproven_synthetic_completion": int(hidive_progress_provenance_row["legacy_unproven"] or 0),
        },
        "provider_freshness": provider_freshness,
        "provider_freshness_by_provider": provider_freshness_by_provider,
        "review_queue": review_counts,
        "mappings": mapping_counts,
        "mal_metadata": {
            "total": int(metadata_row["total"] or 0),
            "typed_field_coverage": {
                "rank": int(metadata_row["with_rank"] or 0),
                "num_list_users": int(metadata_row["with_num_list_users"] or 0),
                "num_scoring_users": int(metadata_row["with_num_scoring_users"] or 0),
                "rating": int(metadata_row["with_rating"] or 0),
                "average_episode_duration": int(metadata_row["with_average_episode_duration"] or 0),
                "start_date": int(metadata_row["with_start_date"] or 0),
                "end_date": int(metadata_row["with_end_date"] or 0),
                "broadcast": int(metadata_row["with_broadcast"] or 0),
                "nsfw": int(metadata_row["with_nsfw"] or 0),
            },
        },
    }


def get_latest_completed_sync_run(
    db_path: Path,
    *,
    provider: str,
    mode: str | None = None,
) -> dict[str, Any] | None:
    """Return the latest completed sync run for a provider, optionally by mode."""
    if not provider:
        return None
    conditions = ["provider = ?", "status = 'completed'"]
    params: list[object] = [provider]
    if mode is not None:
        conditions.append("mode = ?")
        params.append(mode)
    query = """
        SELECT id, provider, contract_version, mode, started_at, completed_at, status, summary_json
        FROM sync_runs
        WHERE {conditions}
        ORDER BY datetime(completed_at) DESC, id DESC
        LIMIT 1
    """.format(conditions=" AND ".join(conditions))
    with connect(db_path) as conn:
        row = conn.execute(query, params).fetchone()
    if row is None:
        return None
    summary_json = row["summary_json"]
    return {
        "id": int(row["id"]),
        "provider": row["provider"],
        "contract_version": row["contract_version"],
        "mode": row["mode"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "status": row["status"],
        "summary": json.loads(summary_json) if summary_json else None,
    }


def get_provider_stale_row_counts(db_path: Path, *, provider: str, cutoff: str) -> dict[str, int]:
    """Count cached provider rows not touched since a sync-run cutoff.

    Health checks use this to distinguish a genuinely partial incremental ingest from
    the common full-refresh residue shape where a provider no longer returns a few
    catalog/watchlist/progress rows but the local cache still retains them.
    """
    if not provider or not cutoff:
        return {}
    with connect(db_path) as conn:
        series_count = conn.execute(
            "SELECT COUNT(*) AS count FROM provider_series WHERE provider = ? AND last_seen_at < ?",
            (provider, cutoff),
        ).fetchone()["count"]
        progress_count = conn.execute(
            "SELECT COUNT(*) AS count FROM provider_episode_progress WHERE provider = ? AND last_seen_at < ?",
            (provider, cutoff),
        ).fetchone()["count"]
        watchlist_count = conn.execute(
            "SELECT COUNT(*) AS count FROM provider_watchlist WHERE provider = ? AND last_seen_at < ?",
            (provider, cutoff),
        ).fetchone()["count"]
    return {
        "series": int(series_count or 0),
        "progress": int(progress_count or 0),
        "watchlist": int(watchlist_count or 0),
    }


def get_provider_stale_row_last_seen_ranges(db_path: Path, *, provider: str, cutoff: str) -> dict[str, dict[str, Any]]:
    """Return min/max last_seen_at ranges for stale provider cache rows.

    The ranges give operators age evidence for stale/deleted upstream residue without
    implying any archive/prune policy. Empty row families are represented with a
    zero count and null bounds so JSON and summary consumers can rely on stable keys.
    """
    empty = {
        "series": {"count": 0, "oldest_last_seen_at": None, "newest_last_seen_at": None},
        "progress": {"count": 0, "oldest_last_seen_at": None, "newest_last_seen_at": None},
        "watchlist": {"count": 0, "oldest_last_seen_at": None, "newest_last_seen_at": None},
    }
    if not provider or not cutoff:
        return empty

    queries = {
        "series": "SELECT COUNT(*) AS count, MIN(last_seen_at) AS oldest_last_seen_at, MAX(last_seen_at) AS newest_last_seen_at FROM provider_series WHERE provider = ? AND last_seen_at < ?",
        "progress": "SELECT COUNT(*) AS count, MIN(last_seen_at) AS oldest_last_seen_at, MAX(last_seen_at) AS newest_last_seen_at FROM provider_episode_progress WHERE provider = ? AND last_seen_at < ?",
        "watchlist": "SELECT COUNT(*) AS count, MIN(last_seen_at) AS oldest_last_seen_at, MAX(last_seen_at) AS newest_last_seen_at FROM provider_watchlist WHERE provider = ? AND last_seen_at < ?",
    }
    ranges: dict[str, dict[str, Any]] = {}
    with connect(db_path) as conn:
        for family, query in queries.items():
            row = conn.execute(query, (provider, cutoff)).fetchone()
            count = int(row["count"] or 0) if row is not None else 0
            ranges[family] = {
                "count": count,
                "oldest_last_seen_at": row["oldest_last_seen_at"] if count and row is not None else None,
                "newest_last_seen_at": row["newest_last_seen_at"] if count and row is not None else None,
            }
    return ranges


def get_provider_stale_row_age_buckets(
    db_path: Path,
    *,
    provider: str,
    cutoff: str,
    seven_day_cutoff: str,
    thirty_day_cutoff: str,
) -> dict[str, dict[str, int]]:
    """Count stale provider cache rows by coarse last_seen_at age buckets.

    This keeps stale/deleted row handling diagnostic-only while giving operators a
    policy-neutral distribution of how long residue has been retained.
    """
    empty = {
        "series": {"recent_0_7_days": 0, "older_8_30_days": 0, "older_31_plus_days": 0},
        "progress": {"recent_0_7_days": 0, "older_8_30_days": 0, "older_31_plus_days": 0},
        "watchlist": {"recent_0_7_days": 0, "older_8_30_days": 0, "older_31_plus_days": 0},
    }
    if not provider or not cutoff or not seven_day_cutoff or not thirty_day_cutoff:
        return empty

    tables = {
        "series": "provider_series",
        "progress": "provider_episode_progress",
        "watchlist": "provider_watchlist",
    }
    buckets: dict[str, dict[str, int]] = {}
    with connect(db_path) as conn:
        for family, table in tables.items():
            row = conn.execute(
                f"""
                SELECT
                    SUM(CASE WHEN last_seen_at < ? AND last_seen_at >= ? THEN 1 ELSE 0 END) AS recent_0_7_days,
                    SUM(CASE WHEN last_seen_at < ? AND last_seen_at < ? AND last_seen_at >= ? THEN 1 ELSE 0 END) AS older_8_30_days,
                    SUM(CASE WHEN last_seen_at < ? AND last_seen_at < ? THEN 1 ELSE 0 END) AS older_31_plus_days
                FROM {table}
                WHERE provider = ? AND last_seen_at < ?
                """,
                (
                    cutoff,
                    seven_day_cutoff,
                    cutoff,
                    seven_day_cutoff,
                    thirty_day_cutoff,
                    cutoff,
                    thirty_day_cutoff,
                    provider,
                    cutoff,
                ),
            ).fetchone()
            buckets[family] = {
                "recent_0_7_days": int(row["recent_0_7_days"] or 0) if row is not None else 0,
                "older_8_30_days": int(row["older_8_30_days"] or 0) if row is not None else 0,
                "older_31_plus_days": int(row["older_31_plus_days"] or 0) if row is not None else 0,
            }
    return buckets


def get_provider_stale_row_linkage(
    db_path: Path,
    *,
    provider: str,
    cutoff: str,
    series_cutoff: str | None = None,
) -> dict[str, dict[str, int]]:
    """Classify stale child rows by their linked provider-series row posture.

    This is read-only retention evidence. Before choosing an archive/prune/retain
    policy, operators need to know whether stale progress/watchlist rows are part
    of the same stale-series residue, still point at a currently observed series,
    or are already orphaned from the series cache.
    """
    empty = {
        "progress": {"with_stale_series": 0, "with_current_series": 0, "with_missing_series": 0},
        "watchlist": {"with_stale_series": 0, "with_current_series": 0, "with_missing_series": 0},
    }
    if not provider or not cutoff:
        return empty
    effective_series_cutoff = series_cutoff or cutoff

    queries = {
        "progress": """
            SELECT
                SUM(CASE WHEN s.provider_series_id IS NOT NULL AND s.last_seen_at < ? THEN 1 ELSE 0 END) AS with_stale_series,
                SUM(CASE WHEN s.provider_series_id IS NOT NULL AND s.last_seen_at >= ? THEN 1 ELSE 0 END) AS with_current_series,
                SUM(CASE WHEN s.provider_series_id IS NULL THEN 1 ELSE 0 END) AS with_missing_series
            FROM provider_episode_progress p
            LEFT JOIN provider_series s
                ON s.provider = p.provider AND s.provider_series_id = p.provider_series_id
            WHERE p.provider = ? AND p.last_seen_at < ?
        """,
        "watchlist": """
            SELECT
                SUM(CASE WHEN s.provider_series_id IS NOT NULL AND s.last_seen_at < ? THEN 1 ELSE 0 END) AS with_stale_series,
                SUM(CASE WHEN s.provider_series_id IS NOT NULL AND s.last_seen_at >= ? THEN 1 ELSE 0 END) AS with_current_series,
                SUM(CASE WHEN s.provider_series_id IS NULL THEN 1 ELSE 0 END) AS with_missing_series
            FROM provider_watchlist w
            LEFT JOIN provider_series s
                ON s.provider = w.provider AND s.provider_series_id = w.provider_series_id
            WHERE w.provider = ? AND w.last_seen_at < ?
        """,
    }
    linkage: dict[str, dict[str, int]] = {}
    with connect(db_path) as conn:
        for family, query in queries.items():
            row = conn.execute(query, (effective_series_cutoff, effective_series_cutoff, provider, cutoff)).fetchone()
            linkage[family] = {
                "with_stale_series": int(row["with_stale_series"] or 0) if row is not None else 0,
                "with_current_series": int(row["with_current_series"] or 0) if row is not None else 0,
                "with_missing_series": int(row["with_missing_series"] or 0) if row is not None else 0,
            }
    return linkage


def _provider_stale_sample_linkage(linked_series_last_seen_at: object, *, series_cutoff: str) -> str:
    if not linked_series_last_seen_at:
        return "missing_series"
    if str(linked_series_last_seen_at) < series_cutoff:
        return "stale_series"
    return "current_series"


def list_provider_stale_row_samples(
    db_path: Path,
    *,
    provider: str,
    cutoff: str,
    limit: int = 5,
    series_cutoff: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Return small operator-facing samples of cached provider rows older than a cutoff.

    The samples intentionally remain read-only diagnostics. They give health-check
    output enough context to decide whether stale/deleted upstream rows should be
    left as classified residue, refreshed again, or handled by a future archive/prune
    workflow without making that destructive policy choice automatically. Child-row
    samples also expose whether their linked provider-series row is stale, current,
    or missing so aggregate linkage counts can be audited against concrete examples.
    """
    if not provider or not cutoff:
        return {"series": [], "progress": [], "watchlist": []}
    safe_limit = max(1, min(25, int(limit)))
    effective_series_cutoff = series_cutoff or cutoff
    with connect(db_path) as conn:
        series_rows = conn.execute(
            """
            SELECT provider_series_id, title, season_title, season_number, last_seen_at
            FROM provider_series
            WHERE provider = ? AND last_seen_at < ?
            ORDER BY last_seen_at ASC, title COLLATE NOCASE ASC, provider_series_id ASC
            LIMIT ?
            """,
            (provider, cutoff, safe_limit),
        ).fetchall()
        progress_rows = conn.execute(
            """
            SELECT
                p.provider_episode_id,
                p.provider_series_id,
                s.title AS series_title,
                s.last_seen_at AS linked_series_last_seen_at,
                p.episode_number,
                p.episode_title,
                p.last_watched_at,
                p.last_seen_at
            FROM provider_episode_progress p
            LEFT JOIN provider_series s
                ON s.provider = p.provider AND s.provider_series_id = p.provider_series_id
            WHERE p.provider = ? AND p.last_seen_at < ?
            ORDER BY p.last_seen_at ASC, p.provider_series_id ASC, p.episode_number ASC, p.provider_episode_id ASC
            LIMIT ?
            """,
            (provider, cutoff, safe_limit),
        ).fetchall()
        watchlist_rows = conn.execute(
            """
            SELECT
                w.provider_series_id,
                s.title,
                s.last_seen_at AS linked_series_last_seen_at,
                w.status,
                w.added_at,
                w.last_seen_at
            FROM provider_watchlist w
            LEFT JOIN provider_series s
                ON s.provider = w.provider AND s.provider_series_id = w.provider_series_id
            WHERE w.provider = ? AND w.last_seen_at < ?
            ORDER BY w.last_seen_at ASC, COALESCE(s.title, w.provider_series_id) COLLATE NOCASE ASC
            LIMIT ?
            """,
            (provider, cutoff, safe_limit),
        ).fetchall()

    return {
        "series": [
            {
                "provider_series_id": str(row["provider_series_id"]),
                "title": row["title"],
                "season_title": row["season_title"],
                "season_number": row["season_number"],
                "last_seen_at": row["last_seen_at"],
            }
            for row in series_rows
        ],
        "progress": [
            {
                "provider_episode_id": str(row["provider_episode_id"]),
                "provider_series_id": str(row["provider_series_id"]),
                "series_title": row["series_title"],
                "linked_series_last_seen_at": row["linked_series_last_seen_at"],
                "linked_series_posture": _provider_stale_sample_linkage(
                    row["linked_series_last_seen_at"],
                    series_cutoff=effective_series_cutoff,
                ),
                "episode_number": row["episode_number"],
                "episode_title": row["episode_title"],
                "last_watched_at": row["last_watched_at"],
                "last_seen_at": row["last_seen_at"],
            }
            for row in progress_rows
        ],
        "watchlist": [
            {
                "provider_series_id": str(row["provider_series_id"]),
                "title": row["title"],
                "linked_series_last_seen_at": row["linked_series_last_seen_at"],
                "linked_series_posture": _provider_stale_sample_linkage(
                    row["linked_series_last_seen_at"],
                    series_cutoff=effective_series_cutoff,
                ),
                "status": row["status"],
                "added_at": row["added_at"],
                "last_seen_at": row["last_seen_at"],
            }
            for row in watchlist_rows
        ],
    }


_ALLOWED_ELIGIBILITY_PROVIDERS = {"crunchyroll", "hidive"}
_ELIGIBILITY_STATUSES = {"unknown", "present", "absent", "stale", "review-needed"}
_REVIEW_STATUSES = _ELIGIBILITY_STATUSES | {"verified"}


def _hidive_url_needs_series_backfill(provider_series_id: object, url: object) -> str | None:
    target = canonical_hidive_series_url(provider_series_id)
    if target is None or not isinstance(url, str):
        return None
    current = url.strip()
    if not current or current == target:
        return None
    try:
        parsed = urlparse(current)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme.casefold() not in {"http", "https"}:
        return None
    if parsed.username is not None or parsed.password is not None or port is not None or parsed.fragment:
        return None
    if (parsed.hostname or "").casefold() not in {"hidive.com", "www.hidive.com"}:
        return None
    if parsed.path.startswith("/season/") and len(parsed.path) > len("/season/"):
        return target
    return None


def _hidive_provider_object_url_backfill(item: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    provider = str(item.get("provider") or "").strip().casefold()
    if provider != "hidive":
        return item, None
    provider_series_id = item.get("provider_series_id")
    replacement = _hidive_url_needs_series_backfill(provider_series_id, item.get("provider_url"))
    if replacement is None:
        return item, None
    updated = {**item, "provider_url": replacement}
    return updated, {
        "provider": "hidive",
        "provider_series_id": str(provider_series_id),
        "old_url": item.get("provider_url"),
        "new_url": replacement,
    }


def _backfill_hidive_score_snapshot_context(context: Any) -> tuple[Any, list[dict[str, Any]]]:
    if not isinstance(context, dict):
        return context, []
    updated_context = dict(context)
    changes: list[dict[str, Any]] = []
    for list_key in ("provider_eligibility_evidence", "available_provider_series"):
        raw_items = context.get(list_key)
        if not isinstance(raw_items, list):
            continue
        updated_items: list[Any] = []
        list_changed = False
        for index, item in enumerate(raw_items):
            if not isinstance(item, dict):
                updated_items.append(item)
                continue
            updated_item, change = _hidive_provider_object_url_backfill(item)
            updated_items.append(updated_item)
            if change is not None:
                list_changed = True
                changes.append({"path": f"{list_key}[{index}].provider_url", **change})
        if list_changed:
            updated_context[list_key] = updated_items
    return (updated_context if changes else context), changes


def _backfill_result_section(candidates: list[dict[str, Any]], updated: int, *, hidden_keys: set[str] | None = None) -> dict[str, Any]:
    hidden = hidden_keys or set()
    samples = [{key: value for key, value in item.items() if key not in hidden} for item in candidates[:20]]
    return {
        "matched": len(candidates),
        "updated": updated,
        "sample_count": len(samples),
        "samples": samples,
    }


def backfill_hidive_series_urls(db_path: Path, *, apply: bool = False) -> dict[str, Any]:
    """Dry-run/apply correction of persisted HIDIVE VOD_SERIES URLs.

    HIDIVE generic VOD_SERIES/title-search links should point at /series/{id},
    not /season/{id-or-slug}.  This helper is intentionally idempotent and only
    rewrites rows where the persisted URL is an old HIDIVE /season route and the
    stable provider_series_id is available locally.
    """
    eligibility_candidates: list[dict[str, Any]] = []
    provider_series_candidates: list[dict[str, Any]] = []
    cache_candidates: list[dict[str, Any]] = []
    score_snapshot_candidates: list[dict[str, Any]] = []
    with connect(db_path) as conn:
        for row in conn.execute(
            """
            SELECT provider_series_id, title, raw_json
            FROM provider_series
            WHERE provider = 'hidive' AND raw_json LIKE '%hidive.com/season/%'
            ORDER BY provider_series_id ASC
            """
        ).fetchall():
            raw = _load_json_value(row["raw_json"], None)
            if not isinstance(raw, dict):
                continue
            replacement = _hidive_url_needs_series_backfill(row["provider_series_id"], raw.get("url"))
            if replacement is None:
                continue
            updated_raw = {**raw, "url": replacement}
            provider_series_candidates.append(
                {
                    "provider_series_id": str(row["provider_series_id"]),
                    "title": row["title"],
                    "old_url": raw.get("url"),
                    "new_url": replacement,
                    "updated_raw_json": json.dumps(updated_raw, ensure_ascii=False, sort_keys=True),
                }
            )

        for row in conn.execute(
            """
            SELECT mal_anime_id, provider_series_id, provider_title, provider_url
            FROM recommendation_provider_eligibility_evidence
            WHERE provider = 'hidive' AND provider_url LIKE 'http%://www.hidive.com/season/%'
            ORDER BY mal_anime_id ASC, provider_series_id ASC
            """
        ).fetchall():
            replacement = _hidive_url_needs_series_backfill(row["provider_series_id"], row["provider_url"])
            if replacement is None:
                continue
            eligibility_candidates.append(
                {
                    "mal_anime_id": int(row["mal_anime_id"]),
                    "provider_series_id": str(row["provider_series_id"]),
                    "provider_title": row["provider_title"],
                    "old_url": row["provider_url"],
                    "new_url": replacement,
                }
            )

        for row in conn.execute(
            """
            SELECT provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
                   matches_json, logic_version, search_limit, identity_key
            FROM provider_title_search_cache
            WHERE provider = 'hidive' AND matches_json LIKE '%hidive.com/season/%'
            ORDER BY normalized_query ASC, logic_version ASC, search_limit ASC, identity_key ASC
            """
        ).fetchall():
            try:
                matches = json.loads(row["matches_json"])
            except (TypeError, ValueError):
                continue
            if not isinstance(matches, list):
                continue
            changed = False
            updated_matches: list[Any] = []
            changed_matches: list[dict[str, Any]] = []
            for match in matches:
                if not isinstance(match, dict):
                    updated_matches.append(match)
                    continue
                provider_series_id = match.get("provider_series_id")
                replacement = _hidive_url_needs_series_backfill(provider_series_id, match.get("url"))
                if replacement is None:
                    updated_matches.append(match)
                    continue
                updated_match = {**match, "url": replacement}
                updated_matches.append(updated_match)
                changed = True
                changed_matches.append(
                    {
                        "provider_series_id": str(provider_series_id),
                        "title": match.get("title"),
                        "old_url": match.get("url"),
                        "new_url": replacement,
                    }
                )
            if changed:
                cache_candidates.append(
                    {
                        "provider": row["provider"],
                        "normalized_query": row["normalized_query"],
                        "logic_version": row["logic_version"],
                        "search_limit": int(row["search_limit"]),
                        "identity_key": row["identity_key"],
                        "query": row["query"],
                        "candidate_mal_anime_id": row["candidate_mal_anime_id"],
                        "candidate_title": row["candidate_title"],
                        "changed_matches": changed_matches,
                        "old_matches_json": row["matches_json"],
                        "updated_matches_json": json.dumps(updated_matches, ensure_ascii=False, sort_keys=True),
                    }
                )

        for row in conn.execute(
            """
            SELECT id, run_id, kind, title, provider, provider_series_id, mal_anime_id, context_json
            FROM recommendation_score_snapshots
            WHERE context_json LIKE '%hidive.com/season/%'
            ORDER BY id ASC
            """
        ).fetchall():
            context = _load_json_value(row["context_json"], None)
            updated_context, changes = _backfill_hidive_score_snapshot_context(context)
            if not changes:
                continue
            score_snapshot_candidates.append(
                {
                    "id": int(row["id"]),
                    "run_id": row["run_id"],
                    "kind": row["kind"],
                    "title": row["title"],
                    "provider": row["provider"],
                    "provider_series_id": row["provider_series_id"],
                    "mal_anime_id": row["mal_anime_id"],
                    "changes": changes[:20],
                    "updated_context_json": json.dumps(updated_context, ensure_ascii=False, sort_keys=True),
                    "old_context_json": row["context_json"],
                }
            )

        provider_series_updated = 0
        eligibility_updated = 0
        cache_updated = 0
        score_snapshot_updated = 0
        if apply:
            for item in provider_series_candidates:
                cursor = conn.execute(
                    """
                    UPDATE provider_series
                    SET raw_json = ?, last_seen_at = last_seen_at
                    WHERE provider = 'hidive' AND provider_series_id = ?
                    """,
                    (item["updated_raw_json"], item["provider_series_id"]),
                )
                provider_series_updated += int(cursor.rowcount or 0)
            for item in eligibility_candidates:
                cursor = conn.execute(
                    """
                    UPDATE recommendation_provider_eligibility_evidence
                    SET provider_url = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE provider = 'hidive' AND mal_anime_id = ? AND provider_series_id = ? AND provider_url = ?
                    """,
                    (item["new_url"], item["mal_anime_id"], item["provider_series_id"], item["old_url"]),
                )
                eligibility_updated += int(cursor.rowcount or 0)
            for item in cache_candidates:
                cursor = conn.execute(
                    """
                    UPDATE provider_title_search_cache
                    SET matches_json = ?, fetched_at = fetched_at
                    WHERE provider = 'hidive'
                      AND normalized_query = ?
                      AND logic_version = ?
                      AND search_limit = ?
                      AND identity_key = ?
                      AND matches_json = ?
                    """,
                    (
                        item["updated_matches_json"],
                        item["normalized_query"],
                        item["logic_version"],
                        item["search_limit"],
                        item["identity_key"],
                        item["old_matches_json"],
                    ),
                )
                cache_updated += int(cursor.rowcount or 0)
            for item in score_snapshot_candidates:
                cursor = conn.execute(
                    """
                    UPDATE recommendation_score_snapshots
                    SET context_json = ?
                    WHERE id = ? AND context_json = ?
                    """,
                    (item["updated_context_json"], item["id"], item["old_context_json"]),
                )
                score_snapshot_updated += int(cursor.rowcount or 0)
            conn.commit()

    return {
        "provider": "hidive",
        "dry_run": not apply,
        "canonical_route": "https://www.hidive.com/series/{series_id}",
        "provider_series": _backfill_result_section(provider_series_candidates, provider_series_updated, hidden_keys={"updated_raw_json"}),
        "eligibility": _backfill_result_section(eligibility_candidates, eligibility_updated),
        "provider_title_search_cache": _backfill_result_section(cache_candidates, cache_updated, hidden_keys={"old_matches_json", "updated_matches_json"}),
        "recommendation_score_snapshots": _backfill_result_section(
            score_snapshot_candidates,
            score_snapshot_updated,
            hidden_keys={"updated_context_json", "old_context_json"},
        ),
    }


def _validate_recommendation_eligibility_value(name: str, value: str, allowed: set[str]) -> str:
    normalized = str(value).strip().lower()
    if normalized not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}")
    return normalized


def _validate_recommendation_eligibility_provider(provider: str) -> str:
    normalized = str(provider).strip().lower()
    if normalized not in _ALLOWED_ELIGIBILITY_PROVIDERS:
        raise ValueError("provider must be one of ['crunchyroll', 'hidive']")
    return normalized


def _recommendation_provider_eligibility_from_db(row: sqlite3.Row) -> RecommendationProviderEligibilityEvidence:
    audio_locales = _load_json_value(row["audio_locales_json"], [])
    source_evidence = _load_json_value(row["source_evidence_json"], {})
    return RecommendationProviderEligibilityEvidence(
        mal_anime_id=int(row["mal_anime_id"]),
        provider=str(row["provider"]),
        provider_series_id=str(row["provider_series_id"]),
        provider_title=row["provider_title"],
        provider_url=row["provider_url"],
        identity_match_kind=str(row["identity_match_kind"]),
        match_confidence=None if row["match_confidence"] is None else float(row["match_confidence"]),
        review_status=str(row["review_status"]),
        catalog_status=str(row["catalog_status"]),
        english_dub_status=str(row["english_dub_status"]),
        explicit_dub_evidence_source=row["explicit_dub_evidence_source"],
        audio_locales=audio_locales if isinstance(audio_locales, list) else [],
        source_evidence=source_evidence if isinstance(source_evidence, dict) else {},
        fetched_at=str(row["fetched_at"]),
        expires_at=str(row["expires_at"]),
        last_verified_at=row["last_verified_at"],
        refresh_status=str(row["refresh_status"]),
        failure_count=int(row["failure_count"]),
        next_retry_at=row["next_retry_at"],
        logic_version=str(row["logic_version"]),
        verification_outcome=str(row["verification_outcome"]),
        refresh_due_at=row["refresh_due_at"],
        refresh_schedule_version=str(row["refresh_schedule_version"]),
        refresh_schedule_key=row["refresh_schedule_key"],
        last_successful_positive_at=row["last_successful_positive_at"],
        invalidated_at=row["invalidated_at"],
        invalidation_reason=row["invalidation_reason"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def upsert_recommendation_provider_eligibility_evidence(
    db_path: Path,
    *,
    mal_anime_id: int,
    provider: str,
    provider_series_id: str,
    fetched_at: str,
    expires_at: str,
    provider_title: str | None = None,
    provider_url: str | None = None,
    identity_match_kind: str = "unknown",
    match_confidence: float | None = None,
    review_status: str = "unknown",
    catalog_status: str = "unknown",
    english_dub_status: str = "unknown",
    explicit_dub_evidence_source: str | None = None,
    audio_locales: list[Any] | None = None,
    source_evidence: dict[str, Any] | None = None,
    last_verified_at: str | None = None,
    refresh_status: str = "ok",
    failure_count: int = 0,
    next_retry_at: str | None = None,
    logic_version: str = "legacy-v1",
    verification_outcome: str | None = None,
    refresh_due_at: str | None = None,
    refresh_schedule_version: str = "provider-eligibility-120d-v1",
    refresh_schedule_key: str | None = None,
    last_successful_positive_at: str | None = None,
    invalidated_at: str | None = None,
    invalidation_reason: str | None = None,
) -> RecommendationProviderEligibilityEvidence:
    normalized_provider = _validate_recommendation_eligibility_provider(provider)
    normalized_review_status = _validate_recommendation_eligibility_value("review_status", review_status, _REVIEW_STATUSES)
    normalized_catalog_status = _validate_recommendation_eligibility_value("catalog_status", catalog_status, _ELIGIBILITY_STATUSES)
    normalized_english_dub_status = _validate_recommendation_eligibility_value("english_dub_status", english_dub_status, _ELIGIBILITY_STATUSES)
    if verification_outcome is None:
        if (
            normalized_review_status == "verified"
            and normalized_catalog_status == "present"
            and normalized_english_dub_status == "present"
            and last_verified_at is not None
        ):
            verification_outcome = "positive"
        elif normalized_review_status == "verified" and (
            normalized_catalog_status == "absent" or normalized_english_dub_status == "absent"
        ):
            verification_outcome = "negative"
        else:
            verification_outcome = "unknown"
    normalized_verification_outcome = _validate_recommendation_eligibility_value(
        "verification_outcome", verification_outcome, {"unknown", "positive", "negative"}
    )
    existing = get_recommendation_provider_eligibility_evidence(
        db_path,
        mal_anime_id=mal_anime_id,
        provider=normalized_provider,
        provider_series_id=provider_series_id,
    )
    if normalized_verification_outcome == "unknown" and existing is not None and existing.last_successful_positive_at and not existing.invalidated_at:
        provider_title = existing.provider_title
        provider_url = existing.provider_url
        identity_match_kind = existing.identity_match_kind
        match_confidence = existing.match_confidence
        normalized_review_status = existing.review_status
        normalized_catalog_status = existing.catalog_status
        normalized_english_dub_status = existing.english_dub_status
        explicit_dub_evidence_source = existing.explicit_dub_evidence_source
        audio_locales = existing.audio_locales
        source_evidence = existing.source_evidence
        fetched_at = existing.fetched_at
        expires_at = existing.expires_at
        last_verified_at = existing.last_verified_at
        normalized_verification_outcome = existing.verification_outcome
        last_successful_positive_at = existing.last_successful_positive_at
        invalidated_at = existing.invalidated_at
        invalidation_reason = existing.invalidation_reason
    if (
        existing is not None
        and refresh_due_at is None
        and normalized_verification_outcome == existing.verification_outcome
        and last_verified_at == existing.last_verified_at
    ):
        refresh_due_at = existing.refresh_due_at
        refresh_schedule_version = existing.refresh_schedule_version
        refresh_schedule_key = existing.refresh_schedule_key
    if normalized_verification_outcome == "positive":
        last_successful_positive_at = last_successful_positive_at or last_verified_at or fetched_at
        invalidated_at = None
        invalidation_reason = None
    elif normalized_verification_outcome == "negative":
        invalidated_at = invalidated_at or last_verified_at or fetched_at
    if refresh_schedule_key is None:
        from .provider_eligibility_lifecycle import provider_eligibility_refresh_schedule_key

        refresh_schedule_key = provider_eligibility_refresh_schedule_key(
            mal_anime_id=mal_anime_id,
            provider=normalized_provider,
            provider_series_id=provider_series_id,
            schedule_version=refresh_schedule_version,
        )
    if refresh_due_at is None and normalized_verification_outcome in {"positive", "negative"}:
        from .provider_eligibility_lifecycle import provider_eligibility_refresh_due_at

        refresh_due_at = provider_eligibility_refresh_due_at(
            successful_verified_at=last_verified_at or fetched_at,
            mal_anime_id=mal_anime_id,
            provider=normalized_provider,
            provider_series_id=provider_series_id,
            schedule_version=refresh_schedule_version,
        )
    if match_confidence is not None and not 0.0 <= float(match_confidence) <= 1.0:
        raise ValueError("match_confidence must be between 0.0 and 1.0")
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO recommendation_provider_eligibility_evidence (
                mal_anime_id, provider, provider_series_id, provider_title, provider_url,
                identity_match_kind, match_confidence, review_status, catalog_status, english_dub_status,
                explicit_dub_evidence_source, audio_locales_json, source_evidence_json,
                fetched_at, expires_at, last_verified_at, refresh_status,
                failure_count, next_retry_at, logic_version, verification_outcome,
                refresh_due_at, refresh_schedule_version, refresh_schedule_key,
                last_successful_positive_at, invalidated_at, invalidation_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mal_anime_id, provider, provider_series_id) DO UPDATE SET
                provider_title = excluded.provider_title,
                provider_url = excluded.provider_url,
                identity_match_kind = excluded.identity_match_kind,
                match_confidence = excluded.match_confidence,
                review_status = excluded.review_status,
                catalog_status = excluded.catalog_status,
                english_dub_status = excluded.english_dub_status,
                explicit_dub_evidence_source = excluded.explicit_dub_evidence_source,
                audio_locales_json = excluded.audio_locales_json,
                source_evidence_json = excluded.source_evidence_json,
                fetched_at = excluded.fetched_at,
                expires_at = excluded.expires_at,
                last_verified_at = excluded.last_verified_at,
                refresh_status = excluded.refresh_status,
                failure_count = excluded.failure_count,
                next_retry_at = excluded.next_retry_at,
                logic_version = excluded.logic_version,
                verification_outcome = excluded.verification_outcome,
                refresh_due_at = excluded.refresh_due_at,
                refresh_schedule_version = excluded.refresh_schedule_version,
                refresh_schedule_key = excluded.refresh_schedule_key,
                last_successful_positive_at = excluded.last_successful_positive_at,
                invalidated_at = excluded.invalidated_at,
                invalidation_reason = excluded.invalidation_reason,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(mal_anime_id), normalized_provider, provider_series_id, provider_title, provider_url,
                str(identity_match_kind), None if match_confidence is None else float(match_confidence),
                normalized_review_status, normalized_catalog_status, normalized_english_dub_status,
                explicit_dub_evidence_source,
                json.dumps(audio_locales or [], ensure_ascii=False, sort_keys=True),
                json.dumps(source_evidence or {}, ensure_ascii=False, sort_keys=True),
                fetched_at, expires_at, last_verified_at, str(refresh_status),
                max(0, int(failure_count)), next_retry_at, str(logic_version),
                normalized_verification_outcome, refresh_due_at, str(refresh_schedule_version), refresh_schedule_key,
                last_successful_positive_at, invalidated_at, invalidation_reason,
            ),
        )
        conn.commit()
    evidence = get_recommendation_provider_eligibility_evidence(
        db_path, mal_anime_id=mal_anime_id, provider=normalized_provider, provider_series_id=provider_series_id
    )
    if evidence is None:
        raise RuntimeError("Recommendation eligibility evidence disappeared after upsert")
    return evidence


def record_recommendation_provider_eligibility_lifecycle_result(
    db_path: Path,
    *,
    mal_anime_id: int,
    provider: str,
    provider_series_id: str,
    outcome: str,
    attempted_at: str,
    refresh_due_at: str | None = None,
    refresh_schedule_version: str = "provider-eligibility-120d-v1",
    refresh_schedule_key: str | None = None,
    next_retry_at: str | None = None,
    invalidation_reason: str | None = None,
) -> RecommendationProviderEligibilityEvidence:
    """Apply lifecycle-only state while preserving last-known-good positive evidence on unknown/failure."""
    normalized_outcome = _validate_recommendation_eligibility_value(
        "outcome", outcome, {"positive", "negative", "unknown", "failed", "invalidated"}
    )
    existing = get_recommendation_provider_eligibility_evidence(
        db_path,
        mal_anime_id=mal_anime_id,
        provider=provider,
        provider_series_id=provider_series_id,
    )
    if existing is None:
        raise ValueError("eligibility evidence row does not exist")
    failure_count = existing.failure_count + 1 if normalized_outcome == "failed" else 0
    verification_outcome = normalized_outcome if normalized_outcome in {"positive", "negative"} else existing.verification_outcome
    positive_at = attempted_at if normalized_outcome == "positive" else existing.last_successful_positive_at
    invalidated_at = (
        attempted_at
        if normalized_outcome in {"negative", "invalidated"}
        else None
        if normalized_outcome == "positive"
        else existing.invalidated_at
    )
    reason = (
        invalidation_reason
        if normalized_outcome in {"negative", "invalidated"}
        else None
        if normalized_outcome == "positive"
        else existing.invalidation_reason
    )
    return upsert_recommendation_provider_eligibility_evidence(
        db_path,
        mal_anime_id=existing.mal_anime_id,
        provider=existing.provider,
        provider_series_id=existing.provider_series_id,
        provider_title=existing.provider_title,
        provider_url=existing.provider_url,
        identity_match_kind=existing.identity_match_kind,
        match_confidence=existing.match_confidence,
        review_status=existing.review_status,
        catalog_status=existing.catalog_status,
        english_dub_status=existing.english_dub_status,
        explicit_dub_evidence_source=existing.explicit_dub_evidence_source,
        audio_locales=existing.audio_locales,
        source_evidence=existing.source_evidence,
        fetched_at=existing.fetched_at,
        expires_at=existing.expires_at,
        last_verified_at=existing.last_verified_at,
        refresh_status="failed" if normalized_outcome == "failed" else "ok",
        failure_count=failure_count,
        next_retry_at=next_retry_at if normalized_outcome == "failed" else None,
        logic_version=existing.logic_version,
        verification_outcome=verification_outcome,
        refresh_due_at=refresh_due_at if normalized_outcome in {"positive", "negative"} else existing.refresh_due_at,
        refresh_schedule_version=refresh_schedule_version,
        refresh_schedule_key=refresh_schedule_key or existing.refresh_schedule_key,
        last_successful_positive_at=positive_at,
        invalidated_at=invalidated_at,
        invalidation_reason=reason,
    )


def record_recommendation_provider_eligibility_negative_scope(
    db_path: Path,
    *,
    mal_anime_id: int,
    provider: str,
    attempted_at: str,
    expires_at: str,
    refresh_due_at: str | None,
    refresh_schedule_version: str,
    refresh_schedule_key: str,
    invalidation_reason: str,
    source_evidence: dict[str, Any],
    logic_version: str,
    provider_series_id: str = "__provider_search_no_match__",
) -> int:
    """Atomically revoke prior positives and persist affirmative negative coverage."""
    normalized_provider = _validate_recommendation_eligibility_provider(provider)
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        contradicted = conn.execute(
            """
            UPDATE recommendation_provider_eligibility_evidence
            SET verification_outcome = 'negative', invalidated_at = ?, invalidation_reason = ?,
                refresh_status = 'ok', failure_count = 0, next_retry_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE mal_anime_id = ? AND provider = ?
              AND last_successful_positive_at IS NOT NULL AND invalidated_at IS NULL
            """,
            (attempted_at, invalidation_reason, int(mal_anime_id), normalized_provider),
        ).rowcount
        conn.execute(
            """
            INSERT INTO recommendation_provider_eligibility_evidence (
                mal_anime_id, provider, provider_series_id, identity_match_kind,
                review_status, catalog_status, english_dub_status, audio_locales_json,
                source_evidence_json, fetched_at, expires_at, last_verified_at,
                refresh_status, failure_count, next_retry_at, logic_version,
                verification_outcome, refresh_due_at, refresh_schedule_version,
                refresh_schedule_key, invalidated_at, invalidation_reason
            ) VALUES (?, ?, ?, 'provider_title_search_no_match', 'verified', 'absent', 'absent',
                      '[]', ?, ?, ?, ?, 'ok', 0, NULL, ?, 'negative', ?, ?, ?, ?, ?)
            ON CONFLICT(mal_anime_id, provider, provider_series_id) DO UPDATE SET
                identity_match_kind = excluded.identity_match_kind,
                review_status = excluded.review_status,
                catalog_status = excluded.catalog_status,
                english_dub_status = excluded.english_dub_status,
                audio_locales_json = excluded.audio_locales_json,
                source_evidence_json = excluded.source_evidence_json,
                fetched_at = excluded.fetched_at,
                expires_at = excluded.expires_at,
                last_verified_at = excluded.last_verified_at,
                refresh_status = 'ok', failure_count = 0, next_retry_at = NULL,
                logic_version = excluded.logic_version,
                verification_outcome = 'negative',
                refresh_due_at = excluded.refresh_due_at,
                refresh_schedule_version = excluded.refresh_schedule_version,
                refresh_schedule_key = excluded.refresh_schedule_key,
                invalidated_at = excluded.invalidated_at,
                invalidation_reason = excluded.invalidation_reason,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(mal_anime_id), normalized_provider, provider_series_id,
                json.dumps(source_evidence, ensure_ascii=False, sort_keys=True),
                attempted_at, expires_at, attempted_at, logic_version,
                refresh_due_at, refresh_schedule_version, refresh_schedule_key,
                attempted_at, invalidation_reason,
            ),
        )
        conn.commit()
    return max(0, int(contradicted or 0))


def get_recommendation_provider_eligibility_lifecycle_counts(
    db_path: Path, *, provider: str, now: str
) -> dict[str, int]:
    normalized_provider = _validate_recommendation_eligibility_provider(provider)
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN refresh_due_at IS NOT NULL AND refresh_due_at <= ? THEN 1 ELSE 0 END) AS due,
                SUM(CASE WHEN refresh_due_at IS NOT NULL AND refresh_due_at < ? THEN 1 ELSE 0 END) AS overdue,
                SUM(CASE WHEN refresh_status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN refresh_status = 'failed' AND next_retry_at > ? THEN 1 ELSE 0 END) AS backoff,
                SUM(CASE WHEN last_successful_positive_at IS NOT NULL AND invalidated_at IS NULL THEN 1 ELSE 0 END) AS preserved_positive,
                SUM(CASE WHEN invalidated_at IS NOT NULL THEN 1 ELSE 0 END) AS invalidated
            FROM recommendation_provider_eligibility_evidence
            WHERE provider = ?
            """,
            (now, now, now, normalized_provider),
        ).fetchone()
    return {key: int(row[key] or 0) for key in ("due", "overdue", "failed", "backoff", "preserved_positive", "invalidated")}


def list_due_recommendation_provider_eligibility_evidence(
    db_path: Path,
    *,
    provider: str,
    now: str,
    limit: int,
) -> list[RecommendationProviderEligibilityEvidence]:
    normalized_provider = _validate_recommendation_eligibility_provider(provider)
    bounded_limit = max(0, int(limit))
    if bounded_limit == 0:
        return []
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT * FROM recommendation_provider_eligibility_evidence
            WHERE provider = ?
              AND refresh_due_at IS NOT NULL
              AND refresh_due_at <= ?
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY refresh_due_at ASC, mal_anime_id ASC, provider_series_id ASC
            LIMIT ?
            """,
            (normalized_provider, now, now, bounded_limit),
        ).fetchall()
    return [_recommendation_provider_eligibility_from_db(row) for row in rows]


def get_recommendation_provider_eligibility_evidence(
    db_path: Path,
    *,
    mal_anime_id: int,
    provider: str,
    provider_series_id: str,
) -> RecommendationProviderEligibilityEvidence | None:
    normalized_provider = _validate_recommendation_eligibility_provider(provider)
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT * FROM recommendation_provider_eligibility_evidence
            WHERE mal_anime_id = ? AND provider = ? AND provider_series_id = ?
            """,
            (int(mal_anime_id), normalized_provider, provider_series_id),
        ).fetchone()
    if row is None:
        return None
    return _recommendation_provider_eligibility_from_db(row)


def list_recommendation_provider_eligibility_evidence_for_provider_series_keys(
    db_path: Path,
    provider_series_keys: Iterable[tuple[str, str]],
) -> list[RecommendationProviderEligibilityEvidence]:
    normalized_keys = sorted(
        {
            (provider.strip().lower(), provider_series_id.strip())
            for provider, provider_series_id in provider_series_keys
            if isinstance(provider, str)
            and provider.strip()
            and isinstance(provider_series_id, str)
            and provider_series_id.strip()
        }
    )
    if not normalized_keys:
        return []
    conditions = " OR ".join("(provider = ? AND provider_series_id = ?)" for _ in normalized_keys)
    params: list[str] = []
    for provider, provider_series_id in normalized_keys:
        params.extend([provider, provider_series_id])
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM recommendation_provider_eligibility_evidence
            WHERE {conditions}
            ORDER BY provider ASC, provider_series_id ASC, mal_anime_id ASC
            """,
            params,
        ).fetchall()
    return [_recommendation_provider_eligibility_from_db(row) for row in rows]


def list_recommendation_provider_eligibility_evidence_for_mal_ids(
    db_path: Path,
    mal_anime_ids: Iterable[int],
    *,
    provider: str | None = None,
    actionable_only: bool = False,
    now: str | None = None,
) -> list[RecommendationProviderEligibilityEvidence]:
    ids = sorted({int(value) for value in mal_anime_ids})
    if not ids:
        return []
    normalized_provider = _validate_recommendation_eligibility_provider(provider) if provider is not None else None
    conditions = [f"mal_anime_id IN ({', '.join('?' for _ in ids)})"]
    params: list[object] = list(ids)
    if normalized_provider is not None:
        conditions.append("provider = ?")
        params.append(normalized_provider)
    if actionable_only:
        conditions.extend([
            "review_status = 'verified'",
            "catalog_status = 'present'",
            "english_dub_status = 'present'",
            "last_successful_positive_at IS NOT NULL",
            "invalidated_at IS NULL",
        ])
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM recommendation_provider_eligibility_evidence
            WHERE {' AND '.join(conditions)}
            ORDER BY mal_anime_id ASC, provider ASC, provider_series_id ASC
            """,
            params,
        ).fetchall()
    return [_recommendation_provider_eligibility_from_db(row) for row in rows]


def mark_stale_recommendation_provider_eligibility_evidence(
    db_path: Path,
    *,
    now: str,
    mal_anime_id: int | None = None,
    provider: str | None = None,
) -> int:
    conditions = [
        "expires_at <= ?",
        "(last_successful_positive_at IS NULL OR invalidated_at IS NOT NULL)",
        "(catalog_status != 'stale' OR english_dub_status != 'stale' OR review_status != 'stale')",
    ]
    params: list[object] = [now]
    if mal_anime_id is not None:
        conditions.append("mal_anime_id = ?")
        params.append(int(mal_anime_id))
    if provider is not None:
        conditions.append("provider = ?")
        params.append(_validate_recommendation_eligibility_provider(provider))
    with connect(db_path) as conn:
        cursor = conn.execute(
            f"""
            UPDATE recommendation_provider_eligibility_evidence
            SET review_status = 'stale', catalog_status = 'stale', english_dub_status = 'stale', updated_at = CURRENT_TIMESTAMP
            WHERE {' AND '.join(conditions)}
            """,
            params,
        )
        conn.commit()
        return int(cursor.rowcount or 0)


def delete_recommendation_provider_eligibility_evidence(
    db_path: Path,
    *,
    mal_anime_id: int | None = None,
    provider: str | None = None,
    provider_series_id: str | None = None,
    expired_before: str | None = None,
) -> int:
    conditions: list[str] = []
    params: list[object] = []
    if mal_anime_id is not None:
        conditions.append("mal_anime_id = ?")
        params.append(int(mal_anime_id))
    if provider is not None:
        conditions.append("provider = ?")
        params.append(_validate_recommendation_eligibility_provider(provider))
    if provider_series_id is not None:
        conditions.append("provider_series_id = ?")
        params.append(provider_series_id)
    if expired_before is not None:
        conditions.append("expires_at <= ?")
        params.append(expired_before)
    if not conditions:
        raise ValueError("delete requires at least one selector")
    with connect(db_path) as conn:
        cursor = conn.execute(f"DELETE FROM recommendation_provider_eligibility_evidence WHERE {' AND '.join(conditions)}", params)
        conn.commit()
        return int(cursor.rowcount or 0)


def _normalize_provider_progress_slug(provider: str) -> str:
    normalized = str(provider).strip().lower()
    if not normalized:
        raise ValueError("provider must be a non-empty slug")
    return normalized


def _normalize_non_empty_progress_text(name: str, value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def _stable_json_object(value: dict[str, Any]) -> str:
    if not isinstance(value, dict):
        raise TypeError("rank_key must be a dictionary")
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _decode_json_object_or_none(raw_json: str | None) -> dict[str, Any] | None:
    if raw_json is None:
        return None
    decoded = json.loads(raw_json)
    if not isinstance(decoded, dict):
        raise ValueError("stored rank key JSON is not an object")
    return decoded


def _provider_enrichment_cursor_from_db(row: sqlite3.Row) -> RecommendationProviderEnrichmentCursor:
    return RecommendationProviderEnrichmentCursor(
        provider=str(row["provider"]),
        cursor_mal_anime_id=None if row["cursor_mal_anime_id"] is None else int(row["cursor_mal_anime_id"]),
        cursor_rank_key_json=row["cursor_rank_key_json"],
        cursor_generation=int(row["cursor_generation"]),
        wrapped_at=row["wrapped_at"],
        last_attempted_mal_anime_id=(
            None if row["last_attempted_mal_anime_id"] is None else int(row["last_attempted_mal_anime_id"])
        ),
        last_attempted_rank_key_json=row["last_attempted_rank_key_json"],
        last_attempted_at=row["last_attempted_at"],
        last_selection_class=row["last_selection_class"],
        last_outcome=row["last_outcome"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def get_recommendation_provider_enrichment_cursor(
    db_path: Path,
    *,
    provider: str,
) -> RecommendationProviderEnrichmentCursor | None:
    normalized_provider = _normalize_provider_progress_slug(provider)
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM recommendation_provider_enrichment_cursor WHERE provider = ?",
            (normalized_provider,),
        ).fetchone()
    if row is None:
        return None
    return _provider_enrichment_cursor_from_db(row)


def record_recommendation_provider_enrichment_attempt(
    db_path: Path,
    *,
    provider: str,
    mal_anime_id: int,
    rank_key: dict[str, Any],
    selection_class: str,
    attempted_at: str,
    wrapped: bool = False,
    outcome: str = "selected",
) -> RecommendationProviderEnrichmentCursor:
    """Advance the durable provider enrichment cursor and record one candidate attempt."""
    normalized_provider = _normalize_provider_progress_slug(provider)
    normalized_selection_class = _normalize_non_empty_progress_text("selection_class", selection_class)
    normalized_attempted_at = _normalize_non_empty_progress_text("attempted_at", attempted_at)
    normalized_outcome = _normalize_non_empty_progress_text("outcome", outcome)
    rank_key_json = _stable_json_object(rank_key)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO recommendation_provider_enrichment_cursor (
                provider, cursor_mal_anime_id, cursor_rank_key_json, cursor_generation,
                wrapped_at, last_attempted_mal_anime_id, last_attempted_rank_key_json,
                last_attempted_at, last_selection_class, last_outcome
            ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider) DO UPDATE SET
                cursor_mal_anime_id = excluded.cursor_mal_anime_id,
                cursor_rank_key_json = excluded.cursor_rank_key_json,
                cursor_generation = recommendation_provider_enrichment_cursor.cursor_generation + 1,
                wrapped_at = COALESCE(excluded.wrapped_at, recommendation_provider_enrichment_cursor.wrapped_at),
                last_attempted_mal_anime_id = excluded.last_attempted_mal_anime_id,
                last_attempted_rank_key_json = excluded.last_attempted_rank_key_json,
                last_attempted_at = excluded.last_attempted_at,
                last_selection_class = excluded.last_selection_class,
                last_outcome = excluded.last_outcome,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                normalized_provider,
                int(mal_anime_id),
                rank_key_json,
                normalized_attempted_at if wrapped else None,
                int(mal_anime_id),
                rank_key_json,
                normalized_attempted_at,
                normalized_selection_class,
                normalized_outcome,
            ),
        )
        conn.execute(
            """
            INSERT INTO recommendation_provider_enrichment_attempts (
                provider, mal_anime_id, rank_key_json, selection_class,
                attempted_at, attempt_count, last_outcome
            ) VALUES (?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(provider, mal_anime_id) DO UPDATE SET
                rank_key_json = excluded.rank_key_json,
                selection_class = excluded.selection_class,
                attempted_at = excluded.attempted_at,
                attempt_count = recommendation_provider_enrichment_attempts.attempt_count + 1,
                last_outcome = excluded.last_outcome,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                normalized_provider,
                int(mal_anime_id),
                rank_key_json,
                normalized_selection_class,
                normalized_attempted_at,
                normalized_outcome,
            ),
        )
        conn.commit()
    cursor = get_recommendation_provider_enrichment_cursor(db_path, provider=normalized_provider)
    if cursor is None:
        raise RuntimeError("Recommendation provider enrichment cursor disappeared after upsert")
    return cursor


def update_recommendation_provider_enrichment_attempt_outcome(
    db_path: Path,
    *,
    provider: str,
    mal_anime_id: int,
    outcome: str,
) -> int:
    normalized_provider = _normalize_provider_progress_slug(provider)
    normalized_outcome = _normalize_non_empty_progress_text("outcome", outcome)
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE recommendation_provider_enrichment_attempts
            SET last_outcome = ?, updated_at = CURRENT_TIMESTAMP
            WHERE provider = ? AND mal_anime_id = ?
            """,
            (normalized_outcome, normalized_provider, int(mal_anime_id)),
        )
        rowcount = int(cursor.rowcount or 0)
        conn.execute(
            """
            UPDATE recommendation_provider_enrichment_cursor
            SET last_outcome = ?, updated_at = CURRENT_TIMESTAMP
            WHERE provider = ? AND last_attempted_mal_anime_id = ?
            """,
            (normalized_outcome, normalized_provider, int(mal_anime_id)),
        )
        conn.commit()
    return rowcount


def _provider_enrichment_attempt_from_db(row: sqlite3.Row) -> RecommendationProviderEnrichmentAttempt:
    return RecommendationProviderEnrichmentAttempt(
        provider=str(row["provider"]),
        mal_anime_id=int(row["mal_anime_id"]),
        rank_key_json=str(row["rank_key_json"]),
        selection_class=str(row["selection_class"]),
        attempted_at=str(row["attempted_at"]),
        attempt_count=int(row["attempt_count"]),
        last_outcome=row["last_outcome"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def list_recommendation_provider_enrichment_attempts(
    db_path: Path,
    *,
    provider: str,
    mal_anime_ids: Iterable[int] | None = None,
) -> list[RecommendationProviderEnrichmentAttempt]:
    normalized_provider = _normalize_provider_progress_slug(provider)
    conditions = ["provider = ?"]
    params: list[object] = [normalized_provider]
    if mal_anime_ids is not None:
        ids = sorted({int(value) for value in mal_anime_ids})
        if not ids:
            return []
        conditions.append(f"mal_anime_id IN ({', '.join('?' for _ in ids)})")
        params.extend(ids)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM recommendation_provider_enrichment_attempts
            WHERE {' AND '.join(conditions)}
            ORDER BY provider ASC, mal_anime_id ASC
            """,
            params,
        ).fetchall()
    return [_provider_enrichment_attempt_from_db(row) for row in rows]


def get_recommendation_provider_enrichment_progress(
    db_path: Path,
    *,
    provider: str,
    mal_anime_ids: Iterable[int] | None = None,
) -> RecommendationProviderEnrichmentProgress:
    normalized_provider = _normalize_provider_progress_slug(provider)
    attempts = list_recommendation_provider_enrichment_attempts(
        db_path,
        provider=normalized_provider,
        mal_anime_ids=mal_anime_ids,
    )
    return RecommendationProviderEnrichmentProgress(
        provider=normalized_provider,
        cursor=get_recommendation_provider_enrichment_cursor(db_path, provider=normalized_provider),
        attempts_by_mal_anime_id={attempt.mal_anime_id: attempt for attempt in attempts},
    )


def _provider_title_search_cache_entry_from_row(row: sqlite3.Row) -> ProviderTitleSearchCacheEntry | None:
    matches = _load_json_value(row["matches_json"], None)
    if not isinstance(matches, list):
        return None
    return ProviderTitleSearchCacheEntry(
        provider=str(row["provider"]),
        normalized_query=str(row["normalized_query"]),
        query=str(row["query"]),
        candidate_mal_anime_id=None if row["candidate_mal_anime_id"] is None else int(row["candidate_mal_anime_id"]),
        candidate_title=row["candidate_title"],
        matches=matches,
        status=str(row["status"]),
        fetched_at=str(row["fetched_at"]),
        expires_at=str(row["expires_at"]),
        logic_version=str(row["logic_version"]),
        search_limit=int(row["search_limit"]),
        identity_key=str(row["identity_key"]),
    )


def get_provider_title_search_cache(
    db_path: Path,
    *,
    provider: str,
    normalized_query: str,
    now: str | None = None,
    logic_version: str | None = None,
    search_limit: int | None = None,
    identity_key: str | None = None,
    legacy_lookup: bool = False,
) -> ProviderTitleSearchCacheEntry | None:
    semantic_values = (logic_version, search_limit, identity_key)
    supplied = [value is not None for value in semantic_values]
    if any(supplied) and not all(supplied):
        raise ValueError("provider title search cache lookup requires logic_version, search_limit, and identity_key together")
    if not legacy_lookup and not all(supplied):
        raise ValueError("provider title search cache lookup requires the full semantic key")
    with connect(db_path) as conn:
        clause = "provider = ? AND normalized_query = ?"
        params: list[object] = [provider, normalized_query]
        if all(supplied):
            clause += " AND logic_version = ? AND search_limit = ? AND identity_key = ?"
            params.extend([str(logic_version), int(search_limit), str(identity_key)])
        elif legacy_lookup:
            clause += " AND logic_version = 'legacy-v1' AND search_limit = 10 AND identity_key = ''"
        if now is not None:
            clause += " AND expires_at > ?"
            params.append(now)
        row = conn.execute(
            f"""
            SELECT provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
                   matches_json, status, fetched_at, expires_at, logic_version, search_limit, identity_key
            FROM provider_title_search_cache
            WHERE {clause}
            """,
            params,
        ).fetchone()
    if row is None:
        return None
    return _provider_title_search_cache_entry_from_row(row)


def upsert_provider_title_search_cache(
    db_path: Path,
    *,
    provider: str,
    normalized_query: str,
    query: str,
    candidate_mal_anime_id: int | None,
    candidate_title: str | None,
    matches: list[dict[str, Any]],
    status: str,
    fetched_at: str,
    expires_at: str,
    logic_version: str = "legacy-v1",
    search_limit: int = 10,
    identity_key: str = "",
) -> ProviderTitleSearchCacheEntry:
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_title_search_cache (
                provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
                matches_json, status, fetched_at, expires_at, logic_version, search_limit, identity_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, normalized_query, logic_version, search_limit, identity_key) DO UPDATE SET
                query = excluded.query,
                candidate_mal_anime_id = excluded.candidate_mal_anime_id,
                candidate_title = excluded.candidate_title,
                matches_json = excluded.matches_json,
                status = excluded.status,
                fetched_at = excluded.fetched_at,
                expires_at = excluded.expires_at
            """,
            (provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
             json.dumps(matches, ensure_ascii=False, sort_keys=True), status, fetched_at, expires_at,
             logic_version, int(search_limit), identity_key),
        )
        conn.commit()
    entry = get_provider_title_search_cache(
        db_path,
        provider=provider,
        normalized_query=normalized_query,
        logic_version=logic_version,
        search_limit=int(search_limit),
        identity_key=identity_key,
    )
    if entry is None:
        raise RuntimeError("Provider title search cache disappeared after upsert")
    return entry


def get_mal_anime_search_cache(db_path: Path, *, cache_key: str, now: str | None = None) -> JsonResponseCacheEntry | None:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT status, response_json, fetched_at, expires_at FROM mal_anime_search_cache WHERE cache_key = ?"
            + (" AND expires_at > ?" if now is not None else ""),
            (cache_key, now) if now is not None else (cache_key,),
        ).fetchone()
    if row is None:
        return None
    response = _load_json_value(row["response_json"], None)
    if not isinstance(response, dict):
        return None
    return JsonResponseCacheEntry(str(row["status"]), response, str(row["fetched_at"]), str(row["expires_at"]))


def upsert_mal_anime_search_cache(db_path: Path, *, cache_key: str, normalized_query: str, result_limit: int,
                                  fields: str, logic_version: str, status: str, response: dict[str, Any],
                                  fetched_at: str, expires_at: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""
            INSERT INTO mal_anime_search_cache(cache_key, normalized_query, result_limit, fields, logic_version,
                status, response_json, fetched_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET status=excluded.status, response_json=excluded.response_json,
                fetched_at=excluded.fetched_at, expires_at=excluded.expires_at
        """, (cache_key, normalized_query, int(result_limit), fields, logic_version, status,
              json.dumps(response, ensure_ascii=False, sort_keys=True), fetched_at, expires_at))
        conn.commit()


def get_mal_anime_detail_cache(db_path: Path, *, mal_anime_id: int, fields_key: str, logic_version: str,
                               now: str | None = None) -> JsonResponseCacheEntry | None:
    with connect(db_path) as conn:
        row = conn.execute("""
            SELECT status, response_json, fetched_at, expires_at, failure_count, next_retry_at
            FROM mal_anime_detail_cache WHERE mal_anime_id=? AND fields_key=? AND logic_version=?
        """, (int(mal_anime_id), fields_key, logic_version)).fetchone()
    if row is None or (now is not None and str(row["expires_at"]) <= now):
        return None
    response = _load_json_value(row["response_json"], None)
    if not isinstance(response, dict):
        return None
    return JsonResponseCacheEntry(str(row["status"]), response, str(row["fetched_at"]), str(row["expires_at"]),
                                  int(row["failure_count"]), row["next_retry_at"])


def find_covering_mal_anime_detail_cache(db_path: Path, *, mal_anime_id: int, required_fields: set[str],
                                         logic_version: str, now: str) -> JsonResponseCacheEntry | None:
    with connect(db_path) as conn:
        rows = conn.execute("""
            SELECT status, response_json, fetched_at, expires_at, failure_count, next_retry_at
            FROM mal_anime_detail_cache
            WHERE mal_anime_id=? AND logic_version=? AND status='ok'
              AND (? = '' OR expires_at > ?)
            ORDER BY fetched_at DESC
        """, (int(mal_anime_id), logic_version, now, now)).fetchall()
    for row in rows:
        response = _load_json_value(row["response_json"], None)
        if isinstance(response, dict) and required_fields.issubset(response):
            return JsonResponseCacheEntry(str(row["status"]), response, str(row["fetched_at"]), str(row["expires_at"]),
                                          int(row["failure_count"]), row["next_retry_at"])
    return None


def list_covering_mal_anime_detail_cache_nodes(
    db_path: Path,
    *,
    required_fields: set[str],
    logic_version: str,
    now: str,
) -> list[dict[str, Any]]:
    """List fresh, successful detail nodes that safely cover a field set.

    A MAL anime can have multiple cache rows for different field sets. Keep the
    newest usable response for each MAL ID, while ignoring failed, expired,
    malformed, and incomplete rows.
    """
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT mal_anime_id, fields_key, response_json
            FROM mal_anime_detail_cache
            WHERE logic_version = ? AND status = 'ok' AND expires_at > ?
            ORDER BY mal_anime_id ASC, fetched_at DESC, fields_key ASC
            """,
            (logic_version, now),
        ).fetchall()

    nodes: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for row in rows:
        mal_anime_id = int(row["mal_anime_id"])
        if mal_anime_id in seen_ids:
            continue
        row_fields = {part.strip() for part in str(row["fields_key"] or "").split(",") if part.strip()}
        if not required_fields.issubset(row_fields):
            continue
        node = _load_json_value(row["response_json"], None)
        if not isinstance(node, dict) or not required_fields.issubset(node):
            continue
        node_id = _coerce_positive_int(node.get("id"))
        if node_id != mal_anime_id or not isinstance(node.get("title"), str) or not node["title"].strip():
            continue
        alternative_titles = node.get("alternative_titles")
        if not isinstance(alternative_titles, dict):
            continue
        if node.get("media_type") is not None and not isinstance(node["media_type"], str):
            continue
        if node.get("status") is not None and not isinstance(node["status"], str):
            continue
        num_episodes = node.get("num_episodes")
        if (
            num_episodes is not None
            and (isinstance(num_episodes, bool) or not isinstance(num_episodes, int) or num_episodes < 0)
        ):
            continue
        if node.get("start_season") is not None and not isinstance(node["start_season"], dict):
            continue
        seen_ids.add(mal_anime_id)
        nodes.append(node)
    return nodes


def upsert_mal_anime_detail_cache(db_path: Path, *, mal_anime_id: int, fields_key: str, logic_version: str,
                                  response: dict[str, Any], fetched_at: str, expires_at: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""
            INSERT INTO mal_anime_detail_cache(mal_anime_id, fields_key, logic_version, status, response_json,
                fetched_at, expires_at, failure_count, next_retry_at) VALUES (?, ?, ?, 'ok', ?, ?, ?, 0, NULL)
            ON CONFLICT(mal_anime_id, fields_key, logic_version) DO UPDATE SET status='ok',
                response_json=excluded.response_json, fetched_at=excluded.fetched_at, expires_at=excluded.expires_at,
                failure_count=0, next_retry_at=NULL
        """, (int(mal_anime_id), fields_key, logic_version,
              json.dumps(response, ensure_ascii=False, sort_keys=True), fetched_at, expires_at))
        conn.commit()


def get_provider_enriched_detail_cache(db_path: Path, *, provider: str, provider_series_id: str,
                                       logic_version: str, now: str | None = None) -> JsonResponseCacheEntry | None:
    with connect(db_path) as conn:
        row = conn.execute("""
            SELECT status, detail_json, fetched_at, expires_at, failure_count, next_retry_at
            FROM provider_enriched_detail_cache WHERE provider=? AND provider_series_id=? AND logic_version=?
        """, (provider, provider_series_id, logic_version)).fetchone()
    if row is None or (now is not None and str(row["expires_at"]) <= now):
        return None
    detail = _load_json_value(row["detail_json"], None)
    if not isinstance(detail, dict):
        return None
    return JsonResponseCacheEntry(str(row["status"]), detail, str(row["fetched_at"]), str(row["expires_at"]),
                                  int(row["failure_count"]), row["next_retry_at"])


def upsert_provider_enriched_detail_cache(db_path: Path, *, provider: str, provider_series_id: str,
                                          logic_version: str, detail: dict[str, Any], fetched_at: str,
                                          expires_at: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""
            INSERT INTO provider_enriched_detail_cache(provider, provider_series_id, logic_version, status,
                detail_json, fetched_at, expires_at, failure_count, next_retry_at)
            VALUES (?, ?, ?, 'ok', ?, ?, ?, 0, NULL)
            ON CONFLICT(provider, provider_series_id, logic_version) DO UPDATE SET status='ok',
                detail_json=excluded.detail_json, fetched_at=excluded.fetched_at, expires_at=excluded.expires_at,
                failure_count=0, next_retry_at=NULL
        """, (provider, provider_series_id, logic_version, json.dumps(detail, ensure_ascii=False, sort_keys=True),
              fetched_at, expires_at))
        conn.commit()


def record_provider_enriched_detail_failure(db_path: Path, *, provider: str, provider_series_id: str,
                                            logic_version: str, fetched_at: str, next_retry_at: str,
                                            expires_at: str, error: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""
            INSERT INTO provider_enriched_detail_cache(provider, provider_series_id, logic_version, status,
                detail_json, fetched_at, expires_at, failure_count, next_retry_at)
            VALUES (?, ?, ?, 'failed', ?, ?, ?, 1, ?)
            ON CONFLICT(provider, provider_series_id, logic_version) DO UPDATE SET status=CASE
                    WHEN provider_enriched_detail_cache.status='ok' THEN 'ok'
                    ELSE 'failed'
                END,
                detail_json=CASE
                    WHEN provider_enriched_detail_cache.status='ok' THEN provider_enriched_detail_cache.detail_json
                    ELSE excluded.detail_json
                END,
                fetched_at=CASE
                    WHEN provider_enriched_detail_cache.status='ok' THEN provider_enriched_detail_cache.fetched_at
                    ELSE excluded.fetched_at
                END,
                expires_at=CASE
                    WHEN provider_enriched_detail_cache.status='ok' THEN provider_enriched_detail_cache.expires_at
                    ELSE excluded.expires_at
                END,
                failure_count=MIN(provider_enriched_detail_cache.failure_count + 1, 8),
                next_retry_at=excluded.next_retry_at
        """, (provider, provider_series_id, logic_version, json.dumps({"error": error}), fetched_at, expires_at, next_retry_at))
        conn.commit()


def _mal_user_traversal_generation_from_row(row: sqlite3.Row) -> MalUserAnimeListTraversalGeneration:
    return MalUserAnimeListTraversalGeneration(
        refresh_run_id=str(row["refresh_run_id"]), generation=int(row["generation"]), fetched_at=str(row["fetched_at"]),
        account_key=str(row["account_key"]), account_id=int(row["account_id"]), account_name=str(row["account_name"]),
        query_identity=str(row["query_identity"]), query=json.loads(row["query_json"] or "{}"),
        claim_token=row["claim_token"], claim_expires_at=row["claim_expires_at"], revision=int(row["revision"] or 0),
        requests_attempted=int(row["requests_attempted"] or 0), requests_succeeded=int(row["requests_succeeded"] or 0),
        requests_failed=int(row["requests_failed"] or 0), restart_count=int(row["restart_count"] or 0),
        drift_count=int(row["drift_count"] or 0), quarantined_at=row["quarantined_at"],
        publication_epoch=int(row["publication_epoch"] or 0), identity_assertion_nonce=row["identity_assertion_nonce"],
    )


def _mal_user_traversal_partition_from_row(row: sqlite3.Row) -> MalUserAnimeListTraversalPartition:
    return MalUserAnimeListTraversalPartition(
        generation=int(row["generation"]), partition_key=str(row["partition_key"]), requested_status=row["requested_status"],
        ordinal=int(row["ordinal"]), initial_url=str(row["initial_url"]), next_url=row["next_url"],
        page_sequence=int(row["page_sequence"]), item_count=int(row["item_count"]), terminal=bool(row["terminal"]),
        terminal_explicit=bool(row["terminal_explicit"]), empty_proven=bool(row["empty_proven"]),
        first_page_fingerprint=row["first_page_fingerprint"], final_page_url=row["final_page_url"],
        final_page_fingerprint=row["final_page_fingerprint"], page1_validated_at=row["page1_validated_at"],
        boundary_validated_at=row["boundary_validated_at"], attempt_count=int(row["attempt_count"]),
        retry_count=int(row["retry_count"]), requests_succeeded=int(row["requests_succeeded"]),
        requests_failed=int(row["requests_failed"]), next_retry_at=row["next_retry_at"], retry_class=row["retry_class"],
        fairness_sequence=int(row["fairness_sequence"]), first_started_at=row["first_started_at"], terminal_at=row["terminal_at"],
    )


def _mal_user_list_partition_class(row: sqlite3.Row) -> str:
    if bool(row["terminal"]):
        return "terminal"
    if row["retry_class"] is not None:
        return "retry_due"
    if int(row["page_sequence"] or 0) > 0:
        return "resumable"
    return "never_started"


def claim_or_create_mal_user_anime_list_traversal(
    db_path: Path, *, account_id: int, account_name: str, query_identity: str, query: dict[str, Any],
    partitions: Iterable[dict[str, Any]], claim_token: str, fetched_at: str,
    claim_seconds: int = MAL_USER_LIST_CLAIM_SECONDS, explicit_reinitialize_quarantined: bool = False,
) -> tuple[MalUserAnimeListTraversalGeneration, list[MalUserAnimeListTraversalPartition]]:
    """Resume the exact account/query generation or create it, fencing by an expiring lease."""
    token = str(claim_token).strip()
    if not token:
        raise ValueError("claim_token is required")
    account_id = int(account_id)
    if account_id <= 0 or not str(account_name).strip():
        raise ValueError("authenticated MAL account identity is incomplete")
    account_key = f"mal:{account_id}"
    query_text = json.dumps(query, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    requested = list(partitions)
    if not requested:
        raise ValueError("at least one MAL list partition is required")
    modifier = f"+{max(1, int(claim_seconds))} seconds"
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        quarantined = conn.execute(
            """SELECT generation FROM mal_user_anime_list_refresh_generations
               WHERE account_key=? AND query_identity=?
                 AND (quarantined_at IS NOT NULL OR quarantine_reason IS NOT NULL)
               ORDER BY generation DESC LIMIT 1""",
            (account_key, str(query_identity)),
        ).fetchone()
        if quarantined is not None and not explicit_reinitialize_quarantined:
            raise MalUserAnimeListRefreshConflictError(
                "quarantined MAL list generation is terminal; explicit safe reinitialization is required"
            )
        row = conn.execute(
            """SELECT * FROM mal_user_anime_list_refresh_generations
               WHERE account_key=? AND query_identity=? AND status='active'
                 AND quarantined_at IS NULL AND quarantine_reason IS NULL""",
            (account_key, str(query_identity)),
        ).fetchone()
        if row is None:
            run_id = f"mal-list-{str(query_identity)[:16]}-{uuid.uuid4()}"
            cursor = conn.execute(
                """INSERT INTO mal_user_anime_list_refresh_generations
                   (refresh_run_id,status,fetched_at,account_key,account_id,account_name,query_identity,query_json,logic_version,
                    claim_token,claim_expires_at,revision)
                   VALUES (?,'active',?,?,?,?,?,?,?, ?,datetime('now',?),1)""",
                (run_id, str(fetched_at), account_key, account_id, str(account_name), str(query_identity), query_text,
                 MAL_USER_LIST_PAGINATION_LOGIC_VERSION, token, modifier),
            )
            generation = int(cursor.lastrowid)
            for item in requested:
                conn.execute(
                    """INSERT INTO mal_user_anime_list_refresh_partitions
                       (generation,partition_key,requested_status,ordinal,initial_url,next_url)
                       VALUES (?,?,?,?,?,?)""",
                    (generation, str(item["partition_key"]), item.get("requested_status"), int(item["ordinal"]),
                     str(item["initial_url"]), str(item["initial_url"])),
                )
            row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (generation,)).fetchone()
        else:
            if int(row["account_id"] or 0) != account_id or str(row["account_name"] or "") != str(account_name):
                raise MalUserAnimeListRefreshConflictError("MAL account identity changed for active list generation")
            if str(row["query_json"]) != query_text or str(row["logic_version"]) != MAL_USER_LIST_PAGINATION_LOGIC_VERSION:
                raise MalUserAnimeListRefreshConflictError("MAL list query/logic identity changed for active generation")
            if row["claim_token"] not in (None, token):
                live = conn.execute("SELECT datetime(?) > datetime('now') AS live", (row["claim_expires_at"],)).fetchone()
                if live is not None and bool(live["live"]):
                    raise MalUserAnimeListRefreshConflictError("MAL list generation is claimed by another live worker")
            changed = conn.execute(
                """UPDATE mal_user_anime_list_refresh_generations
                   SET claim_token=?, claim_expires_at=datetime('now',?), revision=revision+1, updated_at=CURRENT_TIMESTAMP
                   WHERE generation=? AND revision=? AND status='active'""",
                (token, modifier, int(row["generation"]), int(row["revision"])),
            )
            if changed.rowcount != 1:
                raise MalUserAnimeListRefreshConflictError("MAL list generation revision changed during claim")
            row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(row["generation"]),)).fetchone()
        authority = conn.execute(
            "SELECT * FROM mal_user_anime_list_account_authority WHERE account_key=?", (account_key,)
        ).fetchone()
        if authority is None:
            epoch = 1
            conn.execute(
                """INSERT INTO mal_user_anime_list_account_authority
                   (account_key,account_id,account_name,publication_epoch,current_generation)
                   VALUES (?,?,?,?,?)""",
                (account_key, account_id, str(account_name), epoch, int(row["generation"])),
            )
        else:
            if int(authority["account_id"]) != account_id or str(authority["account_name"]) != str(account_name):
                raise MalUserAnimeListRefreshConflictError("MAL account authority identity changed")
            epoch = int(authority["publication_epoch"])
            if int(authority["current_generation"]) > int(row["generation"]):
                raise MalUserAnimeListRefreshConflictError(
                    "older MAL list generation cannot retake the account current-generation fence"
                )
            conn.execute(
                "UPDATE mal_user_anime_list_account_authority SET current_generation=?,updated_at=CURRENT_TIMESTAMP WHERE account_key=?",
                (int(row["generation"]), account_key),
            )
        conn.execute(
            "UPDATE mal_user_anime_list_refresh_generations SET publication_epoch=? WHERE generation=?",
            (epoch, int(row["generation"])),
        )
        row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(row["generation"]),)).fetchone()
        partition_rows = conn.execute(
            "SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? ORDER BY ordinal",
            (int(row["generation"]),),
        ).fetchall()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(row), [_mal_user_traversal_partition_from_row(item) for item in partition_rows]


def reinitialize_mal_user_anime_list_traversal(
    db_path: Path, *, account_id: int, account_name: str, query_identity: str,
    query: dict[str, Any], partitions: Iterable[dict[str, Any]], operator_reason: str, fetched_at: str,
) -> MalUserAnimeListTraversalGeneration:
    """Explicitly replace the latest exact-identity quarantine without touching published LKG/history."""
    reason = str(operator_reason).strip()
    account_id = int(account_id)
    account_name = str(account_name).strip()
    if account_id <= 0 or not account_name or not reason:
        raise ValueError("exact account identity and operator_reason are required")
    account_key = f"mal:{account_id}"
    query_text = json.dumps(query, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    requested = list(partitions)
    if not requested:
        raise ValueError("at least one MAL list partition is required")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        old = conn.execute(
            """SELECT * FROM mal_user_anime_list_refresh_generations
               WHERE account_key=? AND query_identity=? ORDER BY generation DESC LIMIT 1""",
            (account_key, str(query_identity)),
        ).fetchone()
        if old is None or (old["quarantined_at"] is None and old["quarantine_reason"] is None):
            raise MalUserAnimeListRefreshConflictError("latest exact account/query generation is not quarantined")
        if int(old["account_id"] or 0) != account_id or str(old["account_name"] or "") != account_name:
            raise MalUserAnimeListRefreshConflictError("MAL account provenance does not match quarantined generation")
        if str(old["query_json"]) != query_text or str(old["logic_version"]) != MAL_USER_LIST_PAGINATION_LOGIC_VERSION:
            raise MalUserAnimeListRefreshConflictError("MAL query provenance does not match quarantined generation")
        authority = conn.execute("SELECT * FROM mal_user_anime_list_account_authority WHERE account_key=?", (account_key,)).fetchone()
        if authority is None or int(authority["account_id"]) != account_id or str(authority["account_name"]) != account_name:
            raise MalUserAnimeListRefreshConflictError("MAL account authority provenance is absent or changed")
        run_id = f"mal-list-{str(query_identity)[:16]}-reinit-{uuid.uuid4()}"
        cursor = conn.execute(
            """INSERT INTO mal_user_anime_list_refresh_generations
               (refresh_run_id,status,fetched_at,account_key,account_id,account_name,query_identity,query_json,logic_version,
                revision,error,publication_epoch)
               VALUES (?,'active',?,?,?,?,?,?,?,1,?,?)""",
            (run_id, str(fetched_at), account_key, account_id, account_name, str(query_identity), query_text,
             MAL_USER_LIST_PAGINATION_LOGIC_VERSION, f"explicit operator reinitialize: {reason[:900]}",
             int(authority["publication_epoch"])),
        )
        generation = int(cursor.lastrowid)
        for item in requested:
            conn.execute(
                """INSERT INTO mal_user_anime_list_refresh_partitions
                   (generation,partition_key,requested_status,ordinal,initial_url,next_url)
                   VALUES (?,?,?,?,?,?)""",
                (generation, str(item["partition_key"]), item.get("requested_status"), int(item["ordinal"]),
                 str(item["initial_url"]), str(item["initial_url"])),
            )
        conn.execute(
            "UPDATE mal_user_anime_list_account_authority SET current_generation=?,updated_at=CURRENT_TIMESTAMP WHERE account_key=?",
            (generation, account_key),
        )
        row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (generation,)).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(row)


def get_mal_user_anime_list_traversal(
    db_path: Path, *, generation: int,
) -> tuple[MalUserAnimeListTraversalGeneration, list[MalUserAnimeListTraversalPartition]]:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        if row is None or row["account_id"] is None:
            raise ValueError("unknown durable MAL list traversal generation")
        partitions = conn.execute(
            "SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? ORDER BY ordinal", (int(generation),)
        ).fetchall()
    return _mal_user_traversal_generation_from_row(row), [_mal_user_traversal_partition_from_row(item) for item in partitions]


def select_mal_user_anime_list_partition_work(
    db_path: Path, *, generation: int, claim_token: str,
) -> tuple[MalUserAnimeListTraversalGeneration, MalUserAnimeListTraversalPartition, str, str] | None:
    """Select one fair work unit: page1 validation, boundary validation, or cursor fetch."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn, owner, claim_token=claim_token, expected_revision=None)
        if owner["quarantined_at"] is not None or owner["quarantine_reason"] is not None:
            raise MalUserAnimeListRefreshConflictError("quarantined MAL list generation has no selectable work")
        row = conn.execute(
            """SELECT * FROM mal_user_anime_list_refresh_partitions
               WHERE generation=? AND (next_retry_at IS NULL OR datetime(next_retry_at)<=datetime('now'))
                 AND (terminal=0 OR page1_validated_at IS NULL OR boundary_validated_at IS NULL OR EXISTS (
                     SELECT 1 FROM mal_user_anime_list_staged_pages p
                     WHERE p.generation=mal_user_anime_list_refresh_partitions.generation
                       AND p.partition_key=mal_user_anime_list_refresh_partitions.partition_key
                       AND p.validated_at IS NULL
                 ))
               ORDER BY
                 CASE
                   WHEN page_sequence=0 AND retry_class IS NULL THEN 0
                   WHEN page_sequence>0 AND retry_class IS NULL THEN 1
                   WHEN retry_class IS NOT NULL THEN 2
                   ELSE 3
                 END,
                 fairness_sequence ASC, ordinal ASC
               LIMIT 1""",
            (int(generation),),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        if int(row["page_sequence"]) == 0:
            work_kind, url = "page", str(row["initial_url"])
        elif not bool(row["terminal"]) and row["next_url"]:
            work_kind, url = "page", str(row["next_url"])
        elif row["page1_validated_at"] is None:
            work_kind, url = "validate_page1", str(row["initial_url"])
        elif (interior := conn.execute(
            """SELECT page_url FROM mal_user_anime_list_staged_pages
               WHERE generation=? AND partition_key=? AND page_sequence>1
                 AND page_sequence<? AND validated_at IS NULL
               ORDER BY page_sequence LIMIT 1""",
            (int(generation), str(row["partition_key"]), int(row["page_sequence"])),
        ).fetchone()) is not None:
            work_kind, url = "validate_interior", str(interior["page_url"])
        elif row["boundary_validated_at"] is None:
            work_kind, url = "validate_boundary", str(row["final_page_url"])
        else:
            raise RuntimeError("MAL list traversal partition has no coherent next work unit")
        sequence = int(conn.execute("SELECT COALESCE(MAX(fairness_sequence),0)+1 AS n FROM mal_user_anime_list_refresh_partitions WHERE generation=?", (int(generation),)).fetchone()["n"])
        changed = conn.execute(
            """UPDATE mal_user_anime_list_refresh_partitions
               SET attempt_count=attempt_count+1, fairness_sequence=?, first_started_at=COALESCE(first_started_at,CURRENT_TIMESTAMP),
                   queue_class=CASE
                       WHEN retry_class IS NOT NULL THEN 'retry_due'
                       WHEN page_sequence>0 THEN 'resumable'
                       ELSE 'never_started'
                   END,
                   updated_at=CURRENT_TIMESTAMP WHERE generation=? AND partition_key=?""",
            (sequence, int(generation), str(row["partition_key"])),
        )
        if changed.rowcount != 1:
            raise MalUserAnimeListRefreshConflictError("MAL list partition changed during work selection")
        conn.execute(
            """UPDATE mal_user_anime_list_refresh_generations
               SET requests_attempted=requests_attempted+1, fairness_sequence=?, revision=revision+1,
                   claim_expires_at=datetime('now',?), updated_at=CURRENT_TIMESTAMP
               WHERE generation=? AND claim_token=?""",
            (sequence, f"+{MAL_USER_LIST_CLAIM_SECONDS} seconds", int(generation), str(claim_token)),
        )
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        selected = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? AND partition_key=?", (int(generation), str(row["partition_key"]))).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(owner), _mal_user_traversal_partition_from_row(selected), work_kind, url


def _require_mal_user_list_traversal_claim(
    conn: sqlite3.Connection, row: sqlite3.Row | None, *, claim_token: str, expected_revision: int | None,
) -> None:
    if row is None or str(row["status"]) != "active":
        raise MalUserAnimeListRefreshConflictError("MAL list generation is not active")
    if row["quarantined_at"] is not None or row["quarantine_reason"] is not None:
        raise MalUserAnimeListRefreshConflictError("quarantined MAL list generation is terminal until explicit reinitialization")
    if row["claim_token"] != str(claim_token):
        raise MalUserAnimeListRefreshConflictError("stale MAL list worker claim token")
    live = conn.execute("SELECT datetime(?) > datetime('now') AS live", (row["claim_expires_at"],)).fetchone()
    if live is None or not bool(live["live"]):
        raise MalUserAnimeListRefreshConflictError("MAL list worker claim expired")
    if expected_revision is not None and int(row["revision"]) != int(expected_revision):
        raise MalUserAnimeListRefreshConflictError("stale MAL list generation revision")


def record_mal_user_anime_list_request_failure(
    db_path: Path, *, generation: int, partition_key: str, claim_token: str, expected_revision: int,
    retry_class: str, error: str, next_retry_at: str | None, quarantine: bool = False,
) -> MalUserAnimeListTraversalGeneration:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn, owner, claim_token=claim_token, expected_revision=expected_revision)
        partition_changed = conn.execute(
            """UPDATE mal_user_anime_list_refresh_partitions
               SET requests_failed=requests_failed+1,retry_count=retry_count+1,retry_class=?,next_retry_at=?,last_error=?,
                   queue_class=CASE WHEN ? THEN 'quarantined' ELSE 'retry_due' END,updated_at=CURRENT_TIMESTAMP
               WHERE generation=? AND partition_key=?""",
            (str(retry_class), next_retry_at, _bounded_mal_user_list_refresh_error(error), int(quarantine), int(generation), str(partition_key)),
        )
        if partition_changed.rowcount != 1:
            raise MalUserAnimeListRefreshConflictError("MAL list request failure referenced a missing partition")
        conn.execute(
            """UPDATE mal_user_anime_list_refresh_generations
               SET requests_failed=requests_failed+1, revision=revision+1,
                   status=CASE WHEN ? THEN 'failed' ELSE status END,
                   completed_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE completed_at END,
                   quarantined_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE quarantined_at END,
                   quarantine_reason=CASE WHEN ? THEN ? ELSE quarantine_reason END,
                   claim_token=CASE WHEN ? THEN NULL ELSE claim_token END,
                   claim_expires_at=CASE WHEN ? THEN NULL ELSE claim_expires_at END,
                   error=?, updated_at=CURRENT_TIMESTAMP WHERE generation=?""",
            (int(quarantine), int(quarantine), int(quarantine), int(quarantine), str(retry_class), int(quarantine), int(quarantine),
             _bounded_mal_user_list_refresh_error(error), int(generation)),
        )
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(owner)


def checkpoint_mal_user_anime_list_page(
    db_path: Path, *, generation: int, partition_key: str, claim_token: str, expected_revision: int,
    page_url: str, next_url: str | None, items: list[dict[str, Any]], fingerprint: str, anchor: dict[str, Any],
    terminal_explicit: bool, empty_proven: bool, fetched_at: str,
) -> tuple[MalUserAnimeListTraversalGeneration, MalUserAnimeListTraversalPartition]:
    """Atomically stage parsed rows and advance the opaque cursor."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn, owner, claim_token=claim_token, expected_revision=expected_revision)
        partition = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? AND partition_key=?", (int(generation), str(partition_key))).fetchone()
        if partition is None or bool(partition["terminal"]):
            raise MalUserAnimeListRefreshConflictError("MAL list partition is missing or terminal")
        expected_url = partition["initial_url"] if int(partition["page_sequence"]) == 0 else partition["next_url"]
        if str(expected_url) != str(page_url):
            raise MalUserAnimeListRefreshConflictError("MAL list cursor changed before checkpoint")
        query = parse_qs(urlparse(str(page_url)).query, keep_blank_values=True)
        offset_text = query.get("offset", ["0"])[0]
        limit_text = query.get("limit", [None])[0]
        if not str(offset_text).isdigit() or not str(limit_text).isdigit() or int(limit_text) < 1:
            raise ValueError("MAL list page URL lacks a valid offset/page size")
        page_offset = int(offset_text)
        expected_page_size = int(limit_text)
        if next_url is not None:
            if len(items) != expected_page_size:
                raise ValueError("MAL list non-terminal page must contain exactly the expected page size")
            next_query = parse_qs(urlparse(str(next_url)).query, keep_blank_values=True)
            next_offset_text = next_query.get("offset", [None])[0]
            if not str(next_offset_text).isdigit() or int(next_offset_text) != page_offset + expected_page_size:
                raise ValueError("MAL list next cursor offset does not equal current offset plus full page size")
        page_sequence = int(partition["page_sequence"]) + 1
        ids = [int((item.get("node") or {}).get("id")) for item in items]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate MAL anime id within page")
        if ids:
            placeholders = ",".join("?" for _ in ids)
            duplicate = conn.execute(
                f"SELECT mal_anime_id FROM mal_user_anime_list_staged_rows WHERE generation=? AND mal_anime_id IN ({placeholders}) LIMIT 1",
                (int(generation), *ids),
            ).fetchone()
            if duplicate is not None:
                raise ValueError(f"duplicate MAL anime id across staged pages: {int(duplicate['mal_anime_id'])}")
        conn.execute(
            """INSERT INTO mal_user_anime_list_staged_pages
               (generation,partition_key,page_sequence,page_url,page_offset,expected_page_size,next_url,item_count,page_fingerprint,anchor_json,terminal_explicit,fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (int(generation), str(partition_key), page_sequence, str(page_url), page_offset, expected_page_size, next_url, len(items), str(fingerprint),
             json.dumps(anchor, sort_keys=True), int(terminal_explicit), str(fetched_at)),
        )
        for order, item in enumerate(items):
            item_status = _normalize_mal_user_list_status((item.get("list_status") or {}).get("status"))
            if item_status is None:
                raise ValueError("MAL list staged row lacks a supported status")
            if partition["requested_status"] is not None and item_status != str(partition["requested_status"]):
                raise ValueError("MAL list staged row status conflicts with its persisted partition")
            conn.execute(
                """INSERT INTO mal_user_anime_list_staged_rows
                   (generation,partition_key,page_sequence,item_order,mal_anime_id,mal_status,item_json) VALUES (?,?,?,?,?,?,?)""",
                (int(generation), str(partition_key), page_sequence, order, ids[order], item_status,
                 json.dumps(item, ensure_ascii=False, sort_keys=True)),
            )
        terminal = next_url is None and terminal_explicit
        conn.execute(
            """UPDATE mal_user_anime_list_refresh_partitions
               SET next_url=?,page_sequence=?,item_count=item_count+?,terminal=?,terminal_explicit=?,empty_proven=?,
                   first_page_fingerprint=COALESCE(first_page_fingerprint,?),
                   first_page_anchor_json=CASE WHEN first_page_fingerprint IS NULL THEN ? ELSE first_page_anchor_json END,
                   final_page_url=?,final_page_fingerprint=?,final_page_anchor_json=?,
                   page1_validated_at=CASE WHEN page_sequence=0 THEN NULL ELSE page1_validated_at END,
                   boundary_validated_at=NULL,requests_succeeded=requests_succeeded+1,
                   retry_class=NULL,next_retry_at=NULL,last_error=NULL,
                   queue_class=CASE WHEN ? THEN 'terminal' ELSE 'resumable' END,
                   terminal_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,updated_at=CURRENT_TIMESTAMP
               WHERE generation=? AND partition_key=?""",
            (next_url, page_sequence, len(items), int(terminal), int(terminal_explicit), int(empty_proven), str(fingerprint),
             json.dumps(anchor, sort_keys=True), str(page_url), str(fingerprint), json.dumps(anchor, sort_keys=True),
             int(terminal), int(terminal), int(generation), str(partition_key)),
        )
        conn.execute(
            """UPDATE mal_user_anime_list_refresh_generations
               SET pages=pages+1,items=items+?,requests_succeeded=requests_succeeded+1,revision=revision+1,
                   staged_revision=staged_revision+1,validated_staged_revision=NULL,
                   validated_at=NULL,validation_fingerprint=NULL,updated_at=CURRENT_TIMESTAMP WHERE generation=?""",
            (len(items), int(generation)),
        )
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        partition = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? AND partition_key=?", (int(generation), str(partition_key))).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(owner), _mal_user_traversal_partition_from_row(partition)


def checkpoint_mal_user_anime_list_revalidation(
    db_path: Path, *, generation: int, partition_key: str, claim_token: str, expected_revision: int,
    kind: str, page_url: str, fingerprint: str, anchor: dict[str, Any],
) -> tuple[MalUserAnimeListTraversalGeneration, MalUserAnimeListTraversalPartition]:
    if kind not in {"validate_page1", "validate_interior", "validate_boundary"}:
        raise ValueError("invalid MAL list revalidation kind")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn, owner, claim_token=claim_token, expected_revision=expected_revision)
        partition = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? AND partition_key=?", (int(generation), str(partition_key))).fetchone()
        if partition is None or not bool(partition["terminal"]):
            raise MalUserAnimeListRefreshConflictError("MAL list partition is missing or not terminal during revalidation")
        staged_page = None
        if kind == "validate_interior":
            staged_page = conn.execute(
                "SELECT * FROM mal_user_anime_list_staged_pages WHERE generation=? AND partition_key=? AND page_url=? AND page_sequence>1 AND page_sequence<?",
                (int(generation), str(partition_key), str(page_url), int(partition["page_sequence"])),
            ).fetchone()
            if staged_page is None:
                raise MalUserAnimeListRefreshConflictError("MAL list interior page is missing during revalidation")
        expected_url = staged_page["page_url"] if staged_page is not None else partition["initial_url"] if kind == "validate_page1" else partition["final_page_url"]
        expected_fingerprint = staged_page["page_fingerprint"] if staged_page is not None else partition["first_page_fingerprint"] if kind == "validate_page1" else partition["final_page_fingerprint"]
        expected_anchor_json = staged_page["anchor_json"] if staged_page is not None else partition["first_page_anchor_json"] if kind == "validate_page1" else partition["final_page_anchor_json"]
        if str(page_url) != str(expected_url) or str(fingerprint) != str(expected_fingerprint) or json.dumps(anchor, sort_keys=True) != json.dumps(json.loads(expected_anchor_json or "{}"), sort_keys=True):
            raise ValueError(f"MAL list snapshot drift during {kind}")
        if kind != "validate_interior":
            if kind == "validate_page1" and int(partition["page_sequence"]) == 1:
                conn.execute(
                    "UPDATE mal_user_anime_list_refresh_partitions SET page1_validated_at=CURRENT_TIMESTAMP,boundary_validated_at=CURRENT_TIMESTAMP,requests_succeeded=requests_succeeded+1,updated_at=CURRENT_TIMESTAMP WHERE generation=? AND partition_key=?",
                    (int(generation), str(partition_key)),
                )
            else:
                field = "page1_validated_at" if kind == "validate_page1" else "boundary_validated_at"
                conn.execute(
                    f"UPDATE mal_user_anime_list_refresh_partitions SET {field}=CURRENT_TIMESTAMP,requests_succeeded=requests_succeeded+1,updated_at=CURRENT_TIMESTAMP WHERE generation=? AND partition_key=?",
                    (int(generation), str(partition_key)),
                )
        else:
            conn.execute(
                "UPDATE mal_user_anime_list_refresh_partitions SET requests_succeeded=requests_succeeded+1,updated_at=CURRENT_TIMESTAMP WHERE generation=? AND partition_key=?",
                (int(generation), str(partition_key)),
            )
        # Bind validation to the exact stored page revision. For an interior
        # page this is set by its own revalidation work item.
        if kind == "validate_page1":
            page_sequence = 1
        elif kind == "validate_interior":
            page_sequence = int(staged_page["page_sequence"])
        else:
            page_sequence = int(partition["page_sequence"])
        validated = conn.execute(
            "UPDATE mal_user_anime_list_staged_pages SET validated_at=CURRENT_TIMESTAMP WHERE generation=? AND partition_key=? AND page_sequence=? AND page_url=? AND page_fingerprint=?",
            (int(generation), str(partition_key), page_sequence, str(page_url), str(fingerprint)),
        )
        if validated.rowcount != 1:
            raise MalUserAnimeListRefreshConflictError("MAL list staged page disappeared during revalidation")
        conn.execute(
            "UPDATE mal_user_anime_list_refresh_generations SET requests_succeeded=requests_succeeded+1,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE generation=?",
            (int(generation),),
        )
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        partition = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? AND partition_key=?", (int(generation), str(partition_key))).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(owner), _mal_user_traversal_partition_from_row(partition)


def restart_or_quarantine_mal_user_anime_list_traversal(
    db_path: Path, *, generation: int, claim_token: str, expected_revision: int, reason: str,
) -> MalUserAnimeListTraversalGeneration:
    """Preserve failed history and create a fresh same-identity generation, bounded by restart limit."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        old = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn, old, claim_token=claim_token, expected_revision=expected_revision)
        restart_count = int(old["restart_count"] or 0) + 1
        if restart_count > MAL_USER_LIST_MAX_DRIFT_RESTARTS:
            conn.execute(
                """UPDATE mal_user_anime_list_refresh_generations SET status='failed',completed_at=CURRENT_TIMESTAMP,
                   restart_count=?,drift_count=drift_count+1,quarantined_at=CURRENT_TIMESTAMP,quarantine_reason=?,error=?,
                   claim_token=NULL,claim_expires_at=NULL,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE generation=?""",
                (restart_count, _bounded_mal_user_list_refresh_error(reason), _bounded_mal_user_list_refresh_error(reason), int(generation)),
            )
            row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        else:
            partitions = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? ORDER BY ordinal", (int(generation),)).fetchall()
            conn.execute(
                """UPDATE mal_user_anime_list_refresh_generations SET status='failed',completed_at=CURRENT_TIMESTAMP,
                   restart_count=?,drift_count=drift_count+1,error=?,claim_token=NULL,claim_expires_at=NULL,
                   revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE generation=?""",
                (restart_count, _bounded_mal_user_list_refresh_error(reason), int(generation)),
            )
            run_id = f"{old['refresh_run_id']}-r{restart_count}"
            cursor = conn.execute(
                """INSERT INTO mal_user_anime_list_refresh_generations
                   (refresh_run_id,status,fetched_at,account_key,account_id,account_name,query_identity,query_json,logic_version,
                    claim_token,claim_expires_at,revision,restart_count,drift_count)
                   VALUES (?,'active',?,?,?,?,?,?,?,?,datetime('now',?),1,?,?)""",
                (run_id, old["fetched_at"], old["account_key"], old["account_id"], old["account_name"], old["query_identity"],
                 old["query_json"], old["logic_version"], str(claim_token), f"+{MAL_USER_LIST_CLAIM_SECONDS} seconds",
                 restart_count, int(old["drift_count"] or 0)+1),
            )
            new_id = int(cursor.lastrowid)
            for item in partitions:
                conn.execute(
                    """INSERT INTO mal_user_anime_list_refresh_partitions
                       (generation,partition_key,requested_status,ordinal,initial_url,next_url)
                       VALUES (?,?,?,?,?,?)""",
                    (new_id,item["partition_key"],item["requested_status"],item["ordinal"],item["initial_url"],item["initial_url"]),
                )
            row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (new_id,)).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(row)


def persist_mal_user_anime_list_identity_assertion(
    db_path: Path, *, generation: int, claim_token: str, expected_revision: int,
    account_id: int, account_name: str, nonce: str,
) -> MalUserAnimeListTraversalGeneration:
    """Persist a one-use local assertion for an identity check performed outside SQLite."""
    assertion_nonce = str(nonce).strip()
    if not assertion_nonce:
        raise ValueError("identity assertion nonce is required")
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        owner = conn.execute(
            "SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)
        ).fetchone()
        _require_mal_user_list_traversal_claim(
            conn, owner, claim_token=claim_token, expected_revision=expected_revision
        )
        authority = conn.execute(
            "SELECT * FROM mal_user_anime_list_account_authority WHERE account_key=?", (str(owner["account_key"]),)
        ).fetchone()
        if (
            authority is None
            or int(authority["current_generation"]) != int(generation)
            or int(authority["publication_epoch"]) != int(owner["publication_epoch"])
            or int(owner["account_id"]) != int(account_id)
            or str(owner["account_name"]) != str(account_name)
        ):
            raise MalUserAnimeListRefreshConflictError("MAL identity assertion failed account publication fence")
        next_revision = int(owner["revision"]) + 1
        changed = conn.execute(
            """UPDATE mal_user_anime_list_refresh_generations
               SET identity_assertion_nonce=?,identity_asserted_at=CURRENT_TIMESTAMP,
                   identity_asserted_revision=?,identity_assertion_consumed_at=NULL,
                   revision=?,updated_at=CURRENT_TIMESTAMP
               WHERE generation=? AND revision=?""",
            (assertion_nonce, next_revision, next_revision, int(generation), int(expected_revision)),
        )
        if changed.rowcount != 1:
            raise MalUserAnimeListRefreshConflictError("MAL identity assertion revision changed")
        owner = conn.execute(
            "SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)
        ).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(owner)


def _mal_user_list_exact_storage_digest(
    conn: sqlite3.Connection, owner: sqlite3.Row, partitions: list[sqlite3.Row],
    pages: list[sqlite3.Row], rows: list[sqlite3.Row], *, revision: int, validated_at: str,
) -> str:
    """Hash exact SQLite text bytes plus all publication-relevant typed state."""
    digest = hashlib.sha256()

    def emit(label: str, value: Any) -> None:
        raw = value if isinstance(value, bytes) else str(value).encode("utf-8", "surrogatepass")
        if not isinstance(raw, bytes):
            raw = raw.encode("utf-8", "surrogatepass")
        digest.update(label.encode("ascii") + b"\0" + len(raw).to_bytes(8, "big") + raw)

    for name in (
        "generation", "refresh_run_id", "account_key", "account_id", "account_name", "query_identity",
        "query_json", "logic_version", "publication_epoch", "staged_revision", "pages", "items",
        "identity_assertion_nonce", "identity_asserted_at",
    ):
        emit(f"generation.{name}", owner[name])
    emit("generation.publish_revision", revision)
    emit("generation.identity_asserted_revision", revision)
    emit("generation.validated_at", validated_at)
    for partition in partitions:
        for name in (
            "partition_key", "requested_status", "ordinal", "initial_url", "next_url", "page_sequence",
            "item_count", "terminal", "terminal_explicit", "empty_proven", "first_page_fingerprint",
            "first_page_anchor_json", "final_page_url", "final_page_fingerprint", "final_page_anchor_json",
            "page1_validated_at", "boundary_validated_at", "terminal_at", "requests_succeeded",
            "requests_failed", "attempt_count", "retry_count",
        ):
            emit(f"partition.{name}", partition[name])
    for page in pages:
        for name in (
            "partition_key", "page_sequence", "page_url", "page_offset", "expected_page_size", "next_url",
            "item_count", "page_fingerprint", "anchor_json", "terminal_explicit", "fetched_at", "validated_at",
        ):
            emit(f"page.{name}", page[name])
    for row in rows:
        for name in (
            "partition_key", "page_sequence", "item_order", "mal_anime_id", "mal_status", "item_json",
        ):
            emit(f"row.{name}", row[name])
    emit("aggregate.partition_count", len(partitions))
    emit("aggregate.page_count", len(pages))
    emit("aggregate.row_count", len(rows))
    emit("aggregate.page_item_count", sum(int(page["item_count"]) for page in pages))
    emit("aggregate.partition_item_count", sum(int(partition["item_count"]) for partition in partitions))
    return digest.hexdigest()


def load_validated_mal_user_anime_list_staging(
    db_path: Path, *, generation: int, claim_token: str, expected_revision: int,
) -> tuple[MalUserAnimeListTraversalGeneration, list[dict[str, Any]], dict[str, Any]]:
    """Final generation-wide validation under the publication claim."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn, owner, claim_token=claim_token, expected_revision=expected_revision)
        partitions = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? ORDER BY ordinal", (int(generation),)).fetchall()
        if owner["quarantined_at"] is not None:
            raise ValueError("quarantined MAL list generation is not publishable")
        if not partitions or any(not bool(row["terminal"]) or not bool(row["terminal_explicit"]) or row["page1_validated_at"] is None or row["boundary_validated_at"] is None for row in partitions):
            raise ValueError("MAL list generation lacks validated terminal proof for every requested partition")
        pages = conn.execute("SELECT * FROM mal_user_anime_list_staged_pages WHERE generation=? ORDER BY partition_key,page_sequence", (int(generation),)).fetchall()
        if not pages or any(row["validated_at"] is None for row in pages):
            raise ValueError("MAL list generation lacks current validation for every staged page")
        authority = conn.execute(
            "SELECT * FROM mal_user_anime_list_account_authority WHERE account_key=?", (str(owner["account_key"]),)
        ).fetchone()
        if (
            authority is None
            or int(authority["current_generation"]) != int(generation)
            or int(authority["publication_epoch"]) != int(owner["publication_epoch"])
            or owner["identity_assertion_nonce"] is None
            or owner["identity_asserted_at"] is None
            or owner["identity_assertion_consumed_at"] is not None
            or int(owner["identity_asserted_revision"] or -1) != int(owner["revision"])
        ):
            raise ValueError("MAL list generation lacks a current one-use identity assertion under the account fence")
        rows = conn.execute("SELECT * FROM mal_user_anime_list_staged_rows WHERE generation=? ORDER BY partition_key,page_sequence,item_order", (int(generation),)).fetchall()
        try:
            items = [json.loads(row["item_json"]) for row in rows]
        except json.JSONDecodeError as exc:
            raise ValueError("MAL list staged row contains malformed JSON") from exc
        page_counts = {(str(page["partition_key"]), int(page["page_sequence"])): int(page["item_count"]) for page in pages}
        actual_counts: dict[tuple[str, int], int] = {}
        for row in rows:
            key = (str(row["partition_key"]), int(row["page_sequence"]))
            actual_counts[key] = actual_counts.get(key, 0) + 1
        if any(actual_counts.get(key, 0) != count for key, count in page_counts.items()):
            raise ValueError("MAL list staged page/item counts no longer match")
        if sum(page_counts.values()) != len(rows) or sum(int(row["item_count"]) for row in partitions) != len(rows):
            raise ValueError("MAL list aggregate staged counts no longer match")
        partition_status = {str(row["partition_key"]): row["requested_status"] for row in partitions}
        for row in rows:
            expected_status = partition_status[str(row["partition_key"])]
            if expected_status is not None and str(row["mal_status"]) != str(expected_status):
                raise ValueError("MAL list staged row status escaped its persisted partition")
            parsed_status = _normalize_mal_user_list_status((json.loads(row["item_json"]).get("list_status") or {}).get("status"))
            if parsed_status != str(row["mal_status"]):
                raise ValueError("MAL list staged row status column and exact JSON bytes disagree")
        if not items and any(not bool(row["empty_proven"]) for row in partitions):
            raise ValueError("empty MAL list generation lacks strong empty proof")
        validated_revision = int(owner["revision"]) + 1
        validated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        validation_fingerprint = _mal_user_list_exact_storage_digest(
            conn, owner, partitions, pages, rows, revision=validated_revision, validated_at=validated_at
        )
        proof = {
            "authenticated_account_id": int(owner["account_id"]), "authenticated_account_name": str(owner["account_name"]),
            "query_identity": str(owner["query_identity"]), "all_partitions_terminal": True,
            "partition_count": len(partitions), "item_count": len(items), "explicit_empty": not items,
            "pagination_unambiguous": True, "final_validation": True,
        }
        conn.execute(
            """UPDATE mal_user_anime_list_refresh_generations
               SET validated_at=?,validation_fingerprint=?,terminal_empty_proof_json=?,
                   validated_staged_revision=staged_revision,revision=?,identity_asserted_revision=?,updated_at=CURRENT_TIMESTAMP
               WHERE generation=?""",
            (validated_at,validation_fingerprint,json.dumps(proof,sort_keys=True),validated_revision,validated_revision,int(generation)),
        )
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return _mal_user_traversal_generation_from_row(owner), items, proof


def publish_mal_user_anime_list_staging(
    db_path: Path, *, generation: int, claim_token: str, expected_revision: int, delete_absent: bool,
) -> MalUserAnimeListRefreshSummary:
    """Atomically publish exact validated staging and prune only from strong complete proof."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        owner = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn, owner, claim_token=claim_token, expected_revision=expected_revision)
        if owner["validated_at"] is None or owner["validation_fingerprint"] is None:
            raise ValueError("MAL list generation has not passed final validation")
        if owner["quarantined_at"] is not None:
            raise ValueError("quarantined MAL list generation is not publishable")
        if owner["validated_staged_revision"] is None or int(owner["validated_staged_revision"]) != int(owner["staged_revision"]):
            raise ValueError("MAL list publication validation is stale for the staged revision")
        authority = conn.execute(
            "SELECT * FROM mal_user_anime_list_account_authority WHERE account_key=?", (str(owner["account_key"]),)
        ).fetchone()
        if (
            authority is None
            or int(authority["current_generation"]) != int(generation)
            or int(authority["publication_epoch"]) != int(owner["publication_epoch"])
            or int(authority["account_id"]) != int(owner["account_id"])
            or str(authority["account_name"]) != str(owner["account_name"])
            or owner["identity_assertion_nonce"] is None
            or owner["identity_assertion_consumed_at"] is not None
            or int(owner["identity_asserted_revision"] or -1) != int(owner["revision"])
        ):
            raise MalUserAnimeListRefreshConflictError("MAL list publication account/identity assertion fence rejected")
        partitions = conn.execute(
            "SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? ORDER BY ordinal", (int(generation),)
        ).fetchall()
        if not partitions or any(
            not bool(row["terminal"])
            or not bool(row["terminal_explicit"])
            or row["page1_validated_at"] is None
            or row["boundary_validated_at"] is None
            for row in partitions
        ):
            raise ValueError("MAL list publication lost terminal validation")
        pages = conn.execute(
            "SELECT * FROM mal_user_anime_list_staged_pages WHERE generation=? ORDER BY partition_key,page_sequence",
            (int(generation),),
        ).fetchall()
        staged = conn.execute(
            "SELECT * FROM mal_user_anime_list_staged_rows WHERE generation=? ORDER BY partition_key,page_sequence,item_order",
            (int(generation),),
        ).fetchall()
        try:
            items = [json.loads(row["item_json"]) for row in staged]
        except json.JSONDecodeError as exc:
            raise ValueError("MAL list staged row contains malformed JSON") from exc
        exact_digest = _mal_user_list_exact_storage_digest(
            conn, owner, partitions, pages, staged, revision=int(owner["revision"]), validated_at=str(owner["validated_at"])
        )
        if exact_digest != str(owner["validation_fingerprint"]):
            raise ValueError("MAL list exact staged storage/state changed after final validation")
        newer = conn.execute(
            "SELECT 1 FROM mal_user_anime_list_refresh_generations WHERE account_key=? AND generation>? AND status IN ('active','completed') LIMIT 1",
            (str(owner["account_key"]), int(generation)),
        ).fetchone()
        fence = conn.execute(
            "SELECT generation FROM mal_user_anime_list_publication_fence WHERE account_key=?",
            (str(owner["account_key"]),),
        ).fetchone()
        if newer is not None or (fence is not None and int(fence["generation"]) >= int(generation)):
            raise MalUserAnimeListRefreshConflictError("MAL list generation is not the current authoritative publication candidate")
        prepared = [_prepare_mal_user_list_cache_item(item, refresh_run_id=str(owner["refresh_run_id"]), generation=int(generation), fetched_at=str(owner["fetched_at"])) for item in items]
        prepared = [row for row in prepared if row is not None]
        if len(prepared) != len(items):
            raise ValueError("validated MAL list staging contains an unpublishable item")
        if prepared:
            conn.executemany(
                """INSERT INTO mal_user_anime_list_cache
                   (mal_anime_id,title,list_status,user_score,num_episodes_watched,start_date,finish_date,list_updated_at,
                    priority,is_rewatching,num_times_rewatched,rewatch_value,tag_count,has_comments,node_json,list_status_json,
                    raw_json,refresh_run_id,refresh_generation,fetched_at,last_seen_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(mal_anime_id) DO UPDATE SET title=excluded.title,list_status=excluded.list_status,user_score=excluded.user_score,
                    num_episodes_watched=excluded.num_episodes_watched,start_date=excluded.start_date,finish_date=excluded.finish_date,
                    list_updated_at=excluded.list_updated_at,priority=excluded.priority,is_rewatching=excluded.is_rewatching,
                    num_times_rewatched=excluded.num_times_rewatched,rewatch_value=excluded.rewatch_value,tag_count=excluded.tag_count,
                    has_comments=excluded.has_comments,node_json=excluded.node_json,list_status_json=excluded.list_status_json,
                    raw_json=excluded.raw_json,refresh_run_id=excluded.refresh_run_id,refresh_generation=excluded.refresh_generation,
                    fetched_at=excluded.fetched_at,last_seen_at=excluded.last_seen_at,updated_at=CURRENT_TIMESTAMP""",
                prepared,
            )
        pruned = 0
        requested_statuses = [row["requested_status"] for row in partitions]
        is_canonical_all = len(requested_statuses) == 1 and requested_statuses[0] is None
        if delete_absent and not is_canonical_all and any(status is None for status in requested_statuses):
            raise ValueError("invalid delete_absent/publication scope combination")
        if delete_absent and is_canonical_all:
            pruned = conn.execute(
                "DELETE FROM mal_user_anime_list_cache WHERE refresh_generation<>?", (int(generation),)
            ).rowcount
        elif delete_absent:
            normalized_scope = sorted({str(status) for status in requested_statuses if status is not None})
            if not normalized_scope or any(status not in _ALLOWED_MAL_USER_LIST_STATUSES for status in normalized_scope):
                raise ValueError("invalid delete_absent/publication scope combination")
            placeholders = ",".join("?" for _ in normalized_scope)
            pruned = conn.execute(
                f"""DELETE FROM mal_user_anime_list_cache
                    WHERE refresh_generation<>? AND list_status IN ({placeholders})""",
                (int(generation), *normalized_scope),
            ).rowcount
        preserved = conn.execute("SELECT COUNT(*) AS n FROM mal_user_anime_list_cache WHERE refresh_generation<>?", (int(generation),)).fetchone()["n"]
        conn.execute(
            """UPDATE mal_user_anime_list_refresh_generations SET status='completed',completed_at=CURRENT_TIMESTAMP,
               upserted=?,pruned=?,preserved_absent=?,claim_token=NULL,claim_expires_at=NULL,
               identity_assertion_consumed_at=CURRENT_TIMESTAMP,revision=revision+1,updated_at=CURRENT_TIMESTAMP
               WHERE generation=? AND status='active'""",
            (len(prepared),int(pruned or 0),int(preserved or 0),int(generation)),
        )
        conn.execute(
            """INSERT INTO mal_user_anime_list_publication_fence(account_key,generation,query_identity)
               VALUES (?,?,?)
               ON CONFLICT(account_key) DO UPDATE SET generation=excluded.generation,
                   query_identity=excluded.query_identity,published_at=CURRENT_TIMESTAMP
               WHERE excluded.generation>mal_user_anime_list_publication_fence.generation""",
            (str(owner["account_key"]), int(generation), str(owner["query_identity"])),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        # A publication failure must preserve LKG/staging rollback while not
        # stranding the generation behind a live lease. A later worker can
        # reclaim, revalidate, and retry after the underlying fault is fixed.
        recovery = connect(db_path)
        try:
            recovery.execute("BEGIN IMMEDIATE")
            recovery.execute(
                """UPDATE mal_user_anime_list_refresh_generations
                   SET claim_token=NULL,claim_expires_at=NULL,revision=revision+1,updated_at=CURRENT_TIMESTAMP
                   WHERE generation=? AND status='active' AND claim_token=? AND quarantined_at IS NULL""",
                (int(generation), str(claim_token)),
            )
            recovery.commit()
        except BaseException:
            recovery.rollback()
        finally:
            recovery.close()
        raise
    finally:
        conn.close()
    by_status, scored, unscored, preference_counts = _summarize_prepared_mal_user_list_rows(prepared)
    return MalUserAnimeListRefreshSummary(status="ok",refresh_run_id=str(owner["refresh_run_id"]),generation=int(generation),
        pages=int(owner["pages"]),items=len(prepared),upserted=len(prepared),pruned=int(pruned or 0),preserved_absent=int(preserved or 0),
        scored=scored,unscored=unscored,preference_counts=preference_counts,by_status=by_status,partial=False)


def release_mal_user_anime_list_traversal_claim(
    db_path: Path, *, generation: int, claim_token: str, expected_revision: int,
) -> bool:
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM mal_user_anime_list_refresh_generations WHERE generation=?", (int(generation),)).fetchone()
        _require_mal_user_list_traversal_claim(conn,row,claim_token=claim_token,expected_revision=expected_revision)
        changed = conn.execute(
            "UPDATE mal_user_anime_list_refresh_generations SET claim_token=NULL,claim_expires_at=NULL,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE generation=? AND claim_token=? AND revision=?",
            (int(generation),str(claim_token),int(expected_revision)),
        )
        conn.commit()
    return changed.rowcount == 1


def get_mal_user_anime_list_refresh_diagnostics(db_path: Path) -> dict[str, Any]:
    with connect(db_path) as conn:
        generation = conn.execute(
            "SELECT * FROM mal_user_anime_list_refresh_generations WHERE account_id IS NOT NULL ORDER BY generation DESC LIMIT 1"
        ).fetchone()
        if generation is None:
            return {"status":"unknown","generation":None,"queue_classes":{},"requests":{"attempted":0,"succeeded":0,"failed":0}}
        parts = conn.execute("SELECT * FROM mal_user_anime_list_refresh_partitions WHERE generation=? ORDER BY ordinal",(int(generation["generation"]),)).fetchall()
    now = datetime.now(timezone.utc)
    def age(value: Any) -> int | None:
        if not value: return None
        try: return max(0,int((now-datetime.fromisoformat(str(value).replace("Z","+00:00")).replace(tzinfo=timezone.utc if datetime.fromisoformat(str(value).replace("Z","+00:00")).tzinfo is None else datetime.fromisoformat(str(value).replace("Z","+00:00")).tzinfo)).total_seconds()))
        except ValueError: return None
    classes = {"never_started":0,"resumable":0,"retry_due":0,"terminal":0}
    for row in parts:
        key = _mal_user_list_partition_class(row)
        classes[key]+=1
    never = [age(row["created_at"]) for row in parts if int(row["page_sequence"])==0]
    resumable = [age(row["updated_at"]) for row in parts if int(row["page_sequence"])>0 and not row["terminal"]]
    return {
        "status":str(generation["status"]),"generation":int(generation["generation"]),"account_key":generation["account_key"],
        "query_identity":generation["query_identity"],"logic_version":generation["logic_version"],"revision":int(generation["revision"]),
        "claimed":bool(generation["claim_token"]),"claim_expires_at":generation["claim_expires_at"],"queue_classes":classes,
        "oldest_never_started_age_seconds":max((v for v in never if v is not None),default=None),
        "oldest_resumable_age_seconds":max((v for v in resumable if v is not None),default=None),
        "fairness_sequence":int(generation["fairness_sequence"]),
        "requests":{"attempted":int(generation["requests_attempted"]),"succeeded":int(generation["requests_succeeded"]),"failed":int(generation["requests_failed"])},
        "restart_count":int(generation["restart_count"]),"drift_count":int(generation["drift_count"]),
        "quarantined_at":generation["quarantined_at"],"quarantine_reason":generation["quarantine_reason"],
        "validated_at":generation["validated_at"],"terminal_empty_proof":json.loads(generation["terminal_empty_proof_json"] or "{}"),
        "partitions":[{"status":row["requested_status"],"queue_class":_mal_user_list_partition_class(row),"page_sequence":int(row["page_sequence"]),"cursor_present":bool(row["next_url"]),
            "terminal":bool(row["terminal"]),"empty_proven":bool(row["empty_proven"]),"next_retry_at":row["next_retry_at"],
            "retry_class":row["retry_class"],"fairness_lag":int(generation["fairness_sequence"])-int(row["fairness_sequence"])} for row in parts],
    }
