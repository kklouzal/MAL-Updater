from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import re
import sqlite3
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path
from typing import Any, Iterable

BROADCAST_COMPATIBILITY_MIGRATION = "013_mal_anime_metadata_broadcast_compatibility.sql"
PROVIDER_ENRICHMENT_CURSOR_MIGRATION = "014_recommendation_provider_enrichment_cursor.sql"
PUBLIC_USERRECS_STAGING_MIGRATION = "015_public_userrecs_resumable_staging.sql"

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

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc_value, traceback))
        finally:
            self.close()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, factory=ManagedConnection)
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


@dataclass(frozen=True, slots=True)
class MalUserAnimeListRefreshGeneration:
    refresh_run_id: str
    generation: int
    fetched_at: str


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


def _next_mal_user_list_generation(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(refresh_generation), 0) + 1 AS generation FROM mal_user_anime_list_cache").fetchone()
    return int(row["generation"] or 1)


def begin_mal_user_anime_list_cache_refresh(
    db_path: Path,
    *,
    refresh_run_id: str,
    fetched_at: str,
) -> MalUserAnimeListRefreshGeneration:
    """Allocate a cache refresh generation without pruning any existing rows."""
    if not str(refresh_run_id).strip():
        raise ValueError("refresh_run_id is required")
    if not str(fetched_at).strip():
        raise ValueError("fetched_at is required")
    conn = connect(db_path)
    try:
        generation = _next_mal_user_list_generation(conn)
    finally:
        conn.close()
    return MalUserAnimeListRefreshGeneration(
        refresh_run_id=str(refresh_run_id),
        generation=generation,
        fetched_at=str(fetched_at),
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
    prepared: list[tuple[Any, ...]] = []
    seen_ids: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        row = _prepare_mal_user_list_cache_item(
            item,
            refresh_run_id=str(refresh_run_id),
            generation=int(generation),
            fetched_at=str(fetched_at),
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
        with conn:
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
                """,
                prepared,
            )
            preserved_absent = conn.execute(
                "SELECT COUNT(*) AS n FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
                (int(generation),),
            ).fetchone()["n"]
    finally:
        conn.close()
    by_status, scored, unscored, preference_counts = _summarize_prepared_mal_user_list_rows(prepared)
    return MalUserAnimeListRefreshSummary(
        status="upserted",
        refresh_run_id=str(refresh_run_id),
        generation=int(generation),
        items=len(prepared),
        upserted=len(prepared),
        preserved_absent=int(preserved_absent or 0),
        scored=scored,
        unscored=unscored,
        preference_counts=preference_counts,
        by_status=by_status,
        partial=True,
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
    if delete_absent and not proven_complete:
        raise ValueError("delete_absent requires proven_complete=True")
    conn = connect(db_path)
    try:
        with conn:
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
                (int(generation), str(refresh_run_id)),
            ).fetchall()
            pruned = 0
            if delete_absent:
                pruned = conn.execute(
                    "DELETE FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
                    (int(generation),),
                ).rowcount
            preserved_absent = conn.execute(
                "SELECT COUNT(*) AS n FROM mal_user_anime_list_cache WHERE refresh_generation < ?",
                (int(generation),),
            ).fetchone()["n"]
    finally:
        conn.close()
    by_status: dict[str, int] = {}
    scored = 0
    unscored = 0
    preference_counts = _empty_preference_counts()
    for row in current:
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
    return MalUserAnimeListRefreshSummary(
        status="ok" if proven_complete else "aborted",
        refresh_run_id=str(refresh_run_id),
        generation=int(generation),
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
    summary = finalize_mal_user_anime_list_cache_refresh(
        db_path,
        refresh_run_id=refresh_run_id,
        generation=generation,
        proven_complete=False,
        delete_absent=False,
    )
    summary.status = "aborted"
    summary.error = error
    return summary


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
    if not prune_absent:
        upsert.status = "ok"
        upsert.partial = False
        return upsert
    finalized = finalize_mal_user_anime_list_cache_refresh(
        db_path,
        refresh_run_id=refresh.refresh_run_id,
        generation=refresh.generation,
        proven_complete=True,
        delete_absent=True,
    )
    finalized.pages = upsert.pages
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


def create_or_get_active_mal_public_userrecs_generation(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    source_title: str | None = None,
    source_url: str | None = None,
    cursor_url: str | None = None,
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
            cursor = conn.execute(
                """
                INSERT INTO mal_public_userrecs_crawl_generations (
                    source_mal_anime_id, source_title, source_url, cursor_url
                ) VALUES (?, ?, ?, ?)
                """,
                (source_id, source_title, source_url, initial_cursor_url),
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
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ?
            """,
            (
                final_page["next_url"],
                int(counts["pages"] or 0),
                int(counts["edges"] or 0),
                final_page["page_url"],
                final_page["page_fingerprint"],
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
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'paused', cursor_url = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
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
        if cursor_url is _UNSET:
            conn.execute(
                """
                UPDATE mal_public_userrecs_crawl_generations
                SET status = 'active', last_error = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE generation_id = ?
                """,
                (int(generation_id),),
            )
            event_page_url = generation["cursor_url"]
        else:
            conn.execute(
                """
                UPDATE mal_public_userrecs_crawl_generations
                SET status = 'active', cursor_url = ?, last_error = NULL, updated_at = CURRENT_TIMESTAMP
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
    return pages, edges


def mark_mal_public_userrecs_generation_ready(
    db_path: Path,
    *,
    generation_id: int,
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
        _assert_public_userrecs_generation_coherent(conn, generation, require_terminal=True)
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'ready', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE generation_id = ?
            """,
            (int(generation_id),),
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
        normalized_reason = None if reason is None else str(reason).strip()[:1000] or None
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'discarded', discarded_at = CURRENT_TIMESTAMP, last_error = ?, updated_at = CURRENT_TIMESTAMP
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
        source_id = int(old["source_mal_anime_id"])
        normalized_reason = None if reason is None else str(reason).strip()[:1000] or None
        conn.execute(
            """
            UPDATE mal_public_userrecs_crawl_generations
            SET status = 'discarded', discarded_at = CURRENT_TIMESTAMP, last_error = ?, updated_at = CURRENT_TIMESTAMP
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
                source_mal_anime_id, source_title, source_url, cursor_url
            ) VALUES (?, ?, ?, ?)
            """,
            (source_id, old["source_title"], old["source_url"], new_cursor_url),
        )
        new_generation_id = int(cursor.lastrowid)
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
        if generation["completed_at"] is None:
            raise ValueError("public userrecs generation is ready but lacks completed_at")
        pages, staged_edges = _assert_public_userrecs_generation_coherent(conn, generation, require_terminal=True)
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
            SET status = 'published', published_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
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
            "authorized_source_titles_per_hour": 2,
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
            "authorized_source_titles_per_hour": 2,
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
    authorized_source_titles_per_hour: int = 2,
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
        "provider_counts_by_provider": provider_counts_by_provider,
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
) -> RecommendationProviderEligibilityEvidence:
    normalized_provider = _validate_recommendation_eligibility_provider(provider)
    normalized_review_status = _validate_recommendation_eligibility_value("review_status", review_status, _REVIEW_STATUSES)
    normalized_catalog_status = _validate_recommendation_eligibility_value("catalog_status", catalog_status, _ELIGIBILITY_STATUSES)
    normalized_english_dub_status = _validate_recommendation_eligibility_value("english_dub_status", english_dub_status, _ELIGIBILITY_STATUSES)
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
                failure_count, next_retry_at, logic_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )
        conn.commit()
    evidence = get_recommendation_provider_eligibility_evidence(
        db_path, mal_anime_id=mal_anime_id, provider=normalized_provider, provider_series_id=provider_series_id
    )
    if evidence is None:
        raise RuntimeError("Recommendation eligibility evidence disappeared after upsert")
    return evidence


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
        conditions.extend(["review_status = 'verified'", "catalog_status = 'present'", "english_dub_status = 'present'"])
        if now is not None:
            conditions.append("expires_at > ?")
            params.append(now)
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
    conditions = ["expires_at <= ?", "(catalog_status != 'stale' OR english_dub_status != 'stale' OR review_status != 'stale')"]
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


def get_provider_title_search_cache(
    db_path: Path,
    *,
    provider: str,
    normalized_query: str,
    now: str | None = None,
    logic_version: str | None = None,
    search_limit: int | None = None,
    identity_key: str | None = None,
) -> ProviderTitleSearchCacheEntry | None:
    with connect(db_path) as conn:
        clause = "provider = ? AND normalized_query = ?"
        params: list[object] = [provider, normalized_query]
        if now is not None:
            clause += " AND expires_at > ?"
            params.append(now)
        if logic_version is not None:
            clause += " AND logic_version = ?"
            params.append(logic_version)
        if search_limit is not None:
            clause += " AND search_limit = ?"
            params.append(int(search_limit))
        if identity_key is not None:
            clause += " AND identity_key = ?"
            params.append(identity_key)
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
    return ProviderTitleSearchCacheEntry(
        provider=str(row["provider"]),
        normalized_query=str(row["normalized_query"]),
        query=str(row["query"]),
        candidate_mal_anime_id=None if row["candidate_mal_anime_id"] is None else int(row["candidate_mal_anime_id"]),
        candidate_title=row["candidate_title"],
        matches=_load_json_value(row["matches_json"], None),
        status=str(row["status"]),
        fetched_at=str(row["fetched_at"]),
        expires_at=str(row["expires_at"]),
        logic_version=str(row["logic_version"]),
        search_limit=int(row["search_limit"]),
        identity_key=str(row["identity_key"]),
    ) if isinstance(_load_json_value(row["matches_json"], None), list) else None


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
            ON CONFLICT(provider, normalized_query) DO UPDATE SET
                query = excluded.query,
                candidate_mal_anime_id = excluded.candidate_mal_anime_id,
                candidate_title = excluded.candidate_title,
                matches_json = excluded.matches_json,
                status = excluded.status,
                fetched_at = excluded.fetched_at,
                expires_at = excluded.expires_at
                , logic_version = excluded.logic_version
                , search_limit = excluded.search_limit
                , identity_key = excluded.identity_key
            """,
            (provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
             json.dumps(matches, ensure_ascii=False, sort_keys=True), status, fetched_at, expires_at,
             logic_version, int(search_limit), identity_key),
        )
        conn.commit()
    entry = get_provider_title_search_cache(db_path, provider=provider, normalized_query=normalized_query)
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
            WHERE mal_anime_id=? AND logic_version=? AND status='ok' AND expires_at>?
            ORDER BY fetched_at DESC
        """, (int(mal_anime_id), logic_version, now)).fetchall()
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
            ON CONFLICT(provider, provider_series_id, logic_version) DO UPDATE SET status='failed',
                detail_json=excluded.detail_json, fetched_at=excluded.fetched_at, expires_at=excluded.expires_at,
                failure_count=MIN(provider_enriched_detail_cache.failure_count + 1, 8),
                next_retry_at=excluded.next_retry_at
        """, (provider, provider_series_id, logic_version, json.dumps({"error": error}), fetched_at, expires_at, next_retry_at))
        conn.commit()
