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
        conn.commit()


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


def list_provider_stale_row_samples(
    db_path: Path,
    *,
    provider: str,
    cutoff: str,
    limit: int = 5,
) -> dict[str, list[dict[str, Any]]]:
    """Return small operator-facing samples of cached provider rows older than a cutoff.

    The samples intentionally remain read-only diagnostics. They give health-check
    output enough context to decide whether stale/deleted upstream rows should be
    left as classified residue, refreshed again, or handled by a future archive/prune
    workflow without making that destructive policy choice automatically.
    """
    if not provider or not cutoff:
        return {"series": [], "progress": [], "watchlist": []}
    safe_limit = max(1, min(25, int(limit)))
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
                "status": row["status"],
                "added_at": row["added_at"],
                "last_seen_at": row["last_seen_at"],
            }
            for row in watchlist_rows
        ],
    }
