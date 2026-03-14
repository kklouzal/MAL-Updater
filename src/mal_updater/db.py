from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MIGRATIONS = [
    Path(__file__).resolve().parents[2] / "migrations" / "001_initial.sql",
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


def list_review_queue_entries(
    db_path: Path,
    *,
    status: str = "open",
    issue_type: str | None = None,
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
