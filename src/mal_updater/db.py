from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

MIGRATIONS = [
    Path(__file__).resolve().parents[2] / "migrations" / "001_initial.sql",
    Path(__file__).resolve().parents[2] / "migrations" / "002_mal_metadata_cache.sql",
    Path(__file__).resolve().parents[2] / "migrations" / "003_mal_recommendation_edges.sql",
    Path(__file__).resolve().parents[2] / "migrations" / "004_provider_search_cache.sql",
    Path(__file__).resolve().parents[2] / "migrations" / "004_mal_recommendation_harvest_status.sql",
    Path(__file__).resolve().parents[2] / "migrations" / "005_recommendation_score_snapshots.sql",
    Path(__file__).resolve().parents[2] / "migrations" / "006_recommendation_eligibility_evidence.sql",
    Path(__file__).resolve().parents[2] / "migrations" / "007_mal_user_anime_list_cache.sql",
]


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
    created_at: str
    updated_at: str


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def apply_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    for migration in MIGRATIONS:
        version = migration.name
        already_applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?", (version,)
        ).fetchone()
        if already_applied:
            continue
        conn.executescript(migration.read_text(encoding="utf-8"))
        conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))
    conn.commit()


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


def list_latest_recommendation_snapshot_rows(db_path: Path, *, limit: int | None = 100) -> list[RecommendationSnapshotRow]:
    with connect(db_path) as conn:
        run = conn.execute(
            "SELECT run_id FROM recommendation_score_snapshots ORDER BY generated_at DESC, id DESC LIMIT 1"
        ).fetchone()
        if run is None:
            return []
        sql = """
            SELECT * FROM recommendation_score_snapshots
            WHERE run_id = ?
            ORDER BY priority DESC, score DESC, title COLLATE NOCASE ASC, id ASC
            """
        params: tuple[Any, ...]
        if limit is None:
            params = (run["run_id"],)
        else:
            sql += " LIMIT ?"
            params = (run["run_id"], max(1, int(limit)))
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
) -> None:
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
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(raw, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()


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
    return MalUserAnimeListCacheEntry(
        mal_anime_id=int(row["mal_anime_id"]),
        title=str(row["title"]),
        list_status=row["list_status"],
        user_score=None if row["user_score"] is None else int(row["user_score"]),
        num_episodes_watched=None if row["num_episodes_watched"] is None else int(row["num_episodes_watched"]),
        start_date=row["start_date"],
        finish_date=row["finish_date"],
        list_updated_at=row["list_updated_at"],
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
    return (
        mal_anime_id,
        title,
        status,
        score,
        watched,
        start_date,
        finish_date,
        list_updated_at,
        json.dumps(node, ensure_ascii=False, sort_keys=True),
        json.dumps(list_status_raw, ensure_ascii=False, sort_keys=True),
        json.dumps(item, ensure_ascii=False, sort_keys=True),
        str(refresh_run_id),
        int(generation),
        str(fetched_at),
        str(fetched_at),
    )


def _summarize_prepared_mal_user_list_rows(prepared: list[tuple[Any, ...]]) -> tuple[dict[str, int], int, int]:
    by_status: dict[str, int] = {}
    scored = 0
    unscored = 0
    for row in prepared:
        status = row[2]
        score = row[3]
        if status:
            by_status[str(status)] = by_status.get(str(status), 0) + 1
        if score is not None and int(score) > 0:
            scored += 1
        else:
            unscored += 1
    return by_status, scored, unscored


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
                    start_date, finish_date, list_updated_at, node_json, list_status_json,
                    raw_json, refresh_run_id, refresh_generation, fetched_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mal_anime_id) DO UPDATE SET
                    title = excluded.title,
                    list_status = excluded.list_status,
                    user_score = excluded.user_score,
                    num_episodes_watched = excluded.num_episodes_watched,
                    start_date = excluded.start_date,
                    finish_date = excluded.finish_date,
                    list_updated_at = excluded.list_updated_at,
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
    by_status, scored, unscored = _summarize_prepared_mal_user_list_rows(prepared)
    return MalUserAnimeListRefreshSummary(
        status="upserted",
        refresh_run_id=str(refresh_run_id),
        generation=int(generation),
        items=len(prepared),
        upserted=len(prepared),
        preserved_absent=int(preserved_absent or 0),
        scored=scored,
        unscored=unscored,
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
                SELECT list_status, user_score
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
    for row in current:
        status = row["list_status"]
        score = row["user_score"]
        if status:
            by_status[str(status)] = by_status.get(str(status), 0) + 1
        if score is not None and int(score) > 0:
            scored += 1
        else:
            unscored += 1
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
            SELECT COUNT(*) AS total, MAX(refresh_generation) AS generation, MAX(last_seen_at) AS newest_seen_at, MIN(last_seen_at) AS oldest_seen_at
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
    }


def _my_list_status_from_cache_entry(entry: MalUserAnimeListCacheEntry) -> dict[str, Any]:
    payload = dict(entry.list_status_raw)
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
                raw_json,
                fetched_at,
                updated_at
            FROM mal_anime_metadata
            """
        ).fetchall()
    return {
        int(row["mal_anime_id"]): MalAnimeMetadata(
            mal_anime_id=int(row["mal_anime_id"]),
            title=str(row["title"]),
            title_english=row["title_english"],
            title_japanese=row["title_japanese"],
            alternative_titles=json.loads(row["alternative_titles_json"]) if row["alternative_titles_json"] else [],
            media_type=row["media_type"],
            status=row["status"],
            num_episodes=None if row["num_episodes"] is None else int(row["num_episodes"]),
            mean=None if row["mean"] is None else float(row["mean"]),
            popularity=None if row["popularity"] is None else int(row["popularity"]),
            start_season=json.loads(row["start_season_json"]) if row["start_season_json"] else None,
            raw=json.loads(row["raw_json"]),
            fetched_at=str(row["fetched_at"]),
            updated_at=str(row["updated_at"]),
        )
        for row in rows
    }


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


def replace_mal_recommendation_edges(
    db_path: Path,
    *,
    source_mal_anime_id: int,
    hop_distance: int,
    edges: list[dict[str, Any]],
) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "DELETE FROM mal_anime_recommendations WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'",
            (int(source_mal_anime_id),),
        )
        for edge in edges:
            conn.execute(
                """
                INSERT INTO mal_anime_recommendations (
                    source_mal_anime_id,
                    target_mal_anime_id,
                    target_title,
                    num_recommendations,
                    hop_distance,
                    source_kind,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, 'mal_recommendation', ?)
                """,
                (
                    int(source_mal_anime_id),
                    int(edge["target_mal_anime_id"]),
                    edge.get("target_title"),
                    edge.get("num_recommendations"),
                    int(hop_distance),
                    json.dumps(edge["raw"], ensure_ascii=False, sort_keys=True),
                ),
            )
        conn.execute(
            """
            INSERT INTO mal_recommendation_harvest_status (source_mal_anime_id, status, num_edges, fetched_at)
            VALUES (?, 'fetched', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(source_mal_anime_id) DO UPDATE SET
                status = excluded.status,
                num_edges = excluded.num_edges,
                fetched_at = excluded.fetched_at
            """,
            (int(source_mal_anime_id), len(edges)),
        )
        conn.commit()


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
            ), edge_counts AS (
                SELECT source_mal_anime_id, COUNT(*) AS edge_count, MAX(fetched_at) AS edge_fetched_at
                FROM mal_anime_recommendations
                WHERE source_kind = 'mal_recommendation'
                GROUP BY source_mal_anime_id
            )
            SELECT
                mapped.mal_anime_id,
                mapped.mapped_series_count,
                mapped.watched,
                COALESCE(status.num_edges, edge_counts.edge_count, 0) AS edge_count,
                COALESCE(status.fetched_at, edge_counts.edge_fetched_at) AS fetched_at,
                status.status AS harvest_status
            FROM mapped
            LEFT JOIN edge_counts ON edge_counts.source_mal_anime_id = mapped.mal_anime_id
            LEFT JOIN mal_recommendation_harvest_status status ON status.source_mal_anime_id = mapped.mal_anime_id
            ORDER BY mapped.mal_anime_id ASC
            """
        ).fetchall()
    items: list[dict[str, Any]] = []
    summary = {"mapped_sources": 0, "watched_sources": 0, "fresh": 0, "stale": 0, "unharvested": 0, "total_edges": 0}
    for row in rows:
        fetched_at = row["fetched_at"]
        status = "unharvested"
        if fetched_at:
            status = "fresh"
            if stale_after_days > 0:
                with connect(db_path) as conn:
                    is_stale = conn.execute(
                        "SELECT datetime(?) < datetime('now', ?)",
                        (fetched_at, f"-{stale_after_days} days"),
                    ).fetchone()[0]
                status = "stale" if is_stale else "fresh"
        edge_count = int(row["edge_count"] or 0)
        watched = bool(row["watched"])
        summary["mapped_sources"] += 1
        summary["watched_sources"] += 1 if watched else 0
        summary[status] += 1
        summary["total_edges"] += edge_count
        items.append(
            {
                "mal_anime_id": int(row["mal_anime_id"]),
                "mapped_series_count": int(row["mapped_series_count"] or 0),
                "watched": watched,
                "edge_count": edge_count,
                "fetched_at": fetched_at,
                "status": status,
            }
        )
    coverage = None if summary["mapped_sources"] == 0 else (summary["fresh"] / summary["mapped_sources"])
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
                fetched_at, expires_at, last_verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(mal_anime_id), normalized_provider, provider_series_id, provider_title, provider_url,
                str(identity_match_kind), None if match_confidence is None else float(match_confidence),
                normalized_review_status, normalized_catalog_status, normalized_english_dub_status,
                explicit_dub_evidence_source,
                json.dumps(audio_locales or [], ensure_ascii=False, sort_keys=True),
                json.dumps(source_evidence or {}, ensure_ascii=False, sort_keys=True),
                fetched_at, expires_at, last_verified_at,
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


def get_provider_title_search_cache(
    db_path: Path,
    *,
    provider: str,
    normalized_query: str,
    now: str | None = None,
) -> ProviderTitleSearchCacheEntry | None:
    with connect(db_path) as conn:
        clause = "provider = ? AND normalized_query = ?"
        params: list[object] = [provider, normalized_query]
        if now is not None:
            clause += " AND expires_at > ?"
            params.append(now)
        row = conn.execute(
            f"""
            SELECT provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
                   matches_json, status, fetched_at, expires_at
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
        matches=json.loads(row["matches_json"] or "[]"),
        status=str(row["status"]),
        fetched_at=str(row["fetched_at"]),
        expires_at=str(row["expires_at"]),
    )


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
) -> ProviderTitleSearchCacheEntry:
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_title_search_cache (
                provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
                matches_json, status, fetched_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, normalized_query) DO UPDATE SET
                query = excluded.query,
                candidate_mal_anime_id = excluded.candidate_mal_anime_id,
                candidate_title = excluded.candidate_title,
                matches_json = excluded.matches_json,
                status = excluded.status,
                fetched_at = excluded.fetched_at,
                expires_at = excluded.expires_at
            """,
            (provider, normalized_query, query, candidate_mal_anime_id, candidate_title,
             json.dumps(matches, ensure_ascii=False, sort_keys=True), status, fetched_at, expires_at),
        )
        conn.commit()
    entry = get_provider_title_search_cache(db_path, provider=provider, normalized_query=normalized_query)
    if entry is None:
        raise RuntimeError("Provider title search cache disappeared after upsert")
    return entry
