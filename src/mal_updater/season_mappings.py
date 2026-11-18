"""Persistence for per-season provider -> MAL mappings (migration 033)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import connect

SEASON_MAPPING_SOURCES = frozenset({"auto_exact", "user_exact", "unresolved"})


@dataclass(frozen=True, slots=True)
class ProviderSeasonMapping:
    provider: str
    provider_series_id: str
    provider_season_key: str
    mal_anime_id: int | None
    mapping_source: str
    confidence: float | None
    approved: bool
    season_title: str | None
    season_number: int | None
    resolver_version: str
    detail: dict[str, Any]
    resolved_at: str


def get_provider_season_mapping(
    db_path: Path, *, provider: str, provider_series_id: str, provider_season_key: str
) -> ProviderSeasonMapping | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT * FROM provider_season_mappings
            WHERE provider = ? AND provider_series_id = ? AND provider_season_key = ?
            """,
            (provider, provider_series_id, provider_season_key),
        ).fetchone()
    if row is None:
        return None
    detail = json.loads(row["detail_json"]) if row["detail_json"] else {}
    return ProviderSeasonMapping(
        provider=str(row["provider"]),
        provider_series_id=str(row["provider_series_id"]),
        provider_season_key=str(row["provider_season_key"]),
        mal_anime_id=None if row["mal_anime_id"] is None else int(row["mal_anime_id"]),
        mapping_source=str(row["mapping_source"]),
        confidence=None if row["confidence"] is None else float(row["confidence"]),
        approved=bool(row["approved"]),
        season_title=row["season_title"],
        season_number=row["season_number"],
        resolver_version=str(row["resolver_version"]),
        detail=detail if isinstance(detail, dict) else {},
        resolved_at=str(row["resolved_at"]),
    )


def upsert_provider_season_mapping(
    db_path: Path,
    *,
    provider: str,
    provider_series_id: str,
    provider_season_key: str,
    mal_anime_id: int | None,
    mapping_source: str,
    confidence: float | None,
    season_title: str | None,
    season_number: int | None,
    resolver_version: str,
    detail: dict[str, Any] | None = None,
) -> None:
    if mapping_source not in SEASON_MAPPING_SOURCES:
        raise ValueError(f"unsupported season mapping source: {mapping_source}")
    approved = mapping_source != "unresolved"
    if approved and mal_anime_id is None:
        raise ValueError("an approved season mapping requires a MAL anime id")
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_season_mappings (
                provider, provider_series_id, provider_season_key, mal_anime_id, mapping_source,
                confidence, approved, season_title, season_number, resolver_version, detail_json, resolved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(provider, provider_series_id, provider_season_key) DO UPDATE SET
                mal_anime_id = excluded.mal_anime_id,
                mapping_source = excluded.mapping_source,
                confidence = excluded.confidence,
                approved = excluded.approved,
                season_title = excluded.season_title,
                season_number = excluded.season_number,
                resolver_version = excluded.resolver_version,
                detail_json = excluded.detail_json,
                resolved_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                provider_series_id,
                provider_season_key,
                mal_anime_id,
                mapping_source,
                confidence,
                int(approved),
                season_title,
                season_number,
                resolver_version,
                json.dumps(detail or {}, sort_keys=True),
            ),
        )
        conn.commit()


def list_provider_season_mappings(db_path: Path, *, unresolved_only: bool = False) -> list[dict[str, Any]]:
    query = "SELECT * FROM provider_season_mappings"
    if unresolved_only:
        query += " WHERE approved = 0"
    query += " ORDER BY provider, provider_series_id, season_number, provider_season_key"
    with connect(db_path) as conn:
        rows = conn.execute(query).fetchall()
    return [
        {
            "provider": row["provider"],
            "provider_series_id": row["provider_series_id"],
            "provider_season_key": row["provider_season_key"],
            "season_number": row["season_number"],
            "season_title": row["season_title"],
            "mal_anime_id": row["mal_anime_id"],
            "mapping_source": row["mapping_source"],
            "approved": bool(row["approved"]),
            "confidence": row["confidence"],
            "resolved_at": row["resolved_at"],
            "detail": json.loads(row["detail_json"]) if row["detail_json"] else {},
        }
        for row in rows
    ]


def count_unresolved_provider_season_mappings(db_path: Path) -> int:
    with connect(db_path) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM provider_season_mappings WHERE approved = 0").fetchone()[0])
