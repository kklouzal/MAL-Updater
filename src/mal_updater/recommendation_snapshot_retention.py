from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .db import connect


@dataclass(frozen=True, slots=True)
class RecommendationSnapshotPruneReport:
    status: str
    retention_days: int
    min_runs_per_kind: int
    batch_size: int
    cutoff: str
    rows_before: int
    eligible_rows: int
    deleted_rows: int
    rows_after: int
    remaining_eligible_rows: int
    latest_run_id: str | None
    page_count: int
    freelist_count: int
    vacuum_performed: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def prune_recommendation_score_snapshots(
    db_path: Path,
    *,
    retention_days: int = 14,
    min_runs_per_kind: int = 30,
    batch_size: int = 10_000,
    now: datetime | None = None,
) -> RecommendationSnapshotPruneReport:
    """Delete an old bounded batch while retaining each kind's newest runs.

    Rows are eligible only when they are older than the age horizon and their
    run is not one of the newest distinct runs for that row's kind. The delete
    and all selection/count diagnostics share one IMMEDIATE transaction.
    VACUUM is deliberately never performed here.
    """
    retention_days = max(1, int(retention_days))
    min_runs_per_kind = max(1, int(min_runs_per_kind))
    batch_size = max(1, int(batch_size))
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    cutoff = (current.astimezone(timezone.utc) - timedelta(days=retention_days)).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    ranked_runs_cte = """
        WITH ranked_runs AS (
            SELECT kind, run_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY kind
                       ORDER BY datetime(MAX(generated_at)) DESC, run_id DESC
                   ) AS newest_rank
            FROM recommendation_score_snapshots
            GROUP BY kind, run_id
        )
    """
    eligibility_sql = """
        datetime(s.generated_at) < datetime(?)
        AND NOT EXISTS (
            SELECT 1 FROM ranked_runs AS protected
            WHERE protected.kind = s.kind
              AND protected.run_id = s.run_id
              AND protected.newest_rank <= ?
        )
    """
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'recommendation_score_snapshots'"
        ).fetchone()
        if table_exists is None:
            conn.commit()
            return RecommendationSnapshotPruneReport(
                status="skipped_table_missing",
                retention_days=retention_days,
                min_runs_per_kind=min_runs_per_kind,
                batch_size=batch_size,
                cutoff=cutoff,
                rows_before=0,
                eligible_rows=0,
                deleted_rows=0,
                rows_after=0,
                remaining_eligible_rows=0,
                latest_run_id=None,
                page_count=int(conn.execute("PRAGMA page_count").fetchone()[0]),
                freelist_count=int(conn.execute("PRAGMA freelist_count").fetchone()[0]),
            )

        rows_before = int(conn.execute("SELECT COUNT(*) FROM recommendation_score_snapshots").fetchone()[0])
        latest = conn.execute(
            "SELECT run_id FROM recommendation_score_snapshots ORDER BY datetime(generated_at) DESC, id DESC LIMIT 1"
        ).fetchone()
        eligible_rows = int(
            conn.execute(
                f"{ranked_runs_cte} SELECT COUNT(*) FROM recommendation_score_snapshots AS s WHERE {eligibility_sql}",
                (cutoff, min_runs_per_kind),
            ).fetchone()[0]
        )
        conn.execute(
            f"""
            {ranked_runs_cte}
            DELETE FROM recommendation_score_snapshots
            WHERE id IN (
                SELECT s.id FROM recommendation_score_snapshots AS s
                WHERE {eligibility_sql}
                ORDER BY datetime(s.generated_at), s.id
                LIMIT ?
            )
            """,
            (cutoff, min_runs_per_kind, batch_size),
        )
        # sqlite3 reports rowcount=-1 for a DELETE prefixed by a CTE.
        deleted_rows = max(0, int(conn.execute("SELECT changes()").fetchone()[0]))
        rows_after = int(conn.execute("SELECT COUNT(*) FROM recommendation_score_snapshots").fetchone()[0])
        remaining = int(
            conn.execute(
                f"{ranked_runs_cte} SELECT COUNT(*) FROM recommendation_score_snapshots AS s WHERE {eligibility_sql}",
                (cutoff, min_runs_per_kind),
            ).fetchone()[0]
        )
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        conn.commit()

    return RecommendationSnapshotPruneReport(
        status="pruned" if deleted_rows else "no_change",
        retention_days=retention_days,
        min_runs_per_kind=min_runs_per_kind,
        batch_size=batch_size,
        cutoff=cutoff,
        rows_before=rows_before,
        eligible_rows=eligible_rows,
        deleted_rows=deleted_rows,
        rows_after=rows_after,
        remaining_eligible_rows=remaining,
        latest_run_id=None if latest is None else str(latest["run_id"]),
        page_count=page_count,
        freelist_count=freelist_count,
    )
