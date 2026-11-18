"""Cache of provider season catalog facts (migration 035).

Filled by the provider fetchers a few seasons per run and read by season
resolution in the sync planner, which matches a provider season's date window
and episode count against MAL entries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .db import connect

# Re-read a season whose last episode is recent: it may still be airing.
CATALOG_AIRING_WINDOW_DAYS = 45
CATALOG_AIRING_REFRESH_DAYS = 7


@dataclass(frozen=True, slots=True)
class ProviderSeasonCatalog:
    provider: str
    provider_season_key: str
    title: str | None
    numbered_episode_count: int | None
    total_episode_count: int | None
    first_date: str | None
    last_date: str | None
    date_kind: str
    complete: bool
    fetched_at: str


def get_provider_season_catalog(db_path: Path, *, provider: str, provider_season_key: str) -> ProviderSeasonCatalog | None:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM provider_season_catalog WHERE provider = ? AND provider_season_key = ?",
            (provider, provider_season_key),
        ).fetchone()
    if row is None:
        return None
    return ProviderSeasonCatalog(
        provider=str(row["provider"]),
        provider_season_key=str(row["provider_season_key"]),
        title=row["title"],
        numbered_episode_count=row["numbered_episode_count"],
        total_episode_count=row["total_episode_count"],
        first_date=row["first_date"],
        last_date=row["last_date"],
        date_kind=str(row["date_kind"]),
        complete=bool(row["complete"]),
        fetched_at=str(row["fetched_at"]),
    )


def season_keys_needing_catalog(
    db_path: Path, *, provider: str, season_keys: list[str], now: datetime | None = None
) -> list[str]:
    """Return keys without a catalog row, or whose season may still be airing and was read a while ago."""
    current = now or datetime.now(timezone.utc)
    airing_cutoff = (current - timedelta(days=CATALOG_AIRING_WINDOW_DAYS)).strftime("%Y-%m-%d")
    refresh_cutoff = (current - timedelta(days=CATALOG_AIRING_REFRESH_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    with connect(db_path) as conn:
        rows = {
            str(row["provider_season_key"]): row
            for row in conn.execute(
                "SELECT provider_season_key, last_date, fetched_at, complete FROM provider_season_catalog WHERE provider = ?",
                (provider,),
            )
        }
    needed: list[str] = []
    for key in dict.fromkeys(season_keys):
        row = rows.get(key)
        if row is None:
            needed.append(key)
            continue
        recent = row["last_date"] is None or str(row["last_date"])[:10] >= airing_cutoff or not row["complete"]
        if recent and str(row["fetched_at"]) < refresh_cutoff:
            needed.append(key)
    return needed


def upsert_provider_season_catalog(db_path: Path, entry: ProviderSeasonCatalog) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_season_catalog (
                provider, provider_season_key, title, numbered_episode_count, total_episode_count,
                first_date, last_date, date_kind, complete, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(provider, provider_season_key) DO UPDATE SET
                title = excluded.title,
                numbered_episode_count = excluded.numbered_episode_count,
                total_episode_count = excluded.total_episode_count,
                first_date = excluded.first_date,
                last_date = excluded.last_date,
                date_kind = excluded.date_kind,
                complete = excluded.complete,
                fetched_at = CURRENT_TIMESTAMP
            """,
            (
                entry.provider,
                entry.provider_season_key,
                entry.title,
                entry.numbered_episode_count,
                entry.total_episode_count,
                entry.first_date,
                entry.last_date,
                entry.date_kind,
                int(entry.complete),
            ),
        )
        conn.commit()


def catalog_candidate_season_keys(db_path: Path, *, provider: str) -> list[str]:
    """Season keys that season resolution may need, unresolved seasons first.

    Only seasons of provider series whose progress spans several seasons are
    resolved per season, so only those are catalogued.
    """
    with connect(db_path) as conn:
        multi = [
            str(row["provider_season_key"])
            for row in conn.execute(
                """
                SELECT DISTINCT p.provider_season_key
                FROM provider_episode_progress AS p
                WHERE p.provider = ? AND p.provider_season_key IS NOT NULL
                  AND p.provider_series_id IN (
                      SELECT provider_series_id FROM provider_episode_progress
                      WHERE provider = ? AND provider_season_key IS NOT NULL
                      GROUP BY provider_series_id HAVING COUNT(DISTINCT provider_season_key) > 1
                  )
                ORDER BY p.provider_season_key
                """,
                (provider, provider),
            )
        ]
        unresolved = {
            str(row["provider_season_key"])
            for row in conn.execute(
                "SELECT provider_season_key FROM provider_season_mappings WHERE provider = ? AND approved = 0",
                (provider,),
            )
        }
    return sorted(multi, key=lambda key: (key not in unresolved, key))
