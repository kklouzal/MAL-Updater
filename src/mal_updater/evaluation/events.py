from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Iterable

EVENT_SCHEMA_VERSION = "mal-eval-event/v1"
NORMALIZATION_VERSION = "resume-observation/v1"
LOCAL_USER_ID = "local-default"


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def normalize_time(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _identity(*parts: object) -> str:
    return hashlib.sha256("\x1f".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def _insert(conn, *, source: str, event_type: str, source_key: str, occurred_at: str,
            observed_at: str, entity_type: str, entity_id: str, series_id: str | None,
            episode_id: str | None, payload: dict[str, Any], sync_run_id: int) -> None:
    payload_json = canonical_json(payload)
    payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    source_event_id = _identity(source, event_type, source_key, occurred_at)
    event_id = _identity(EVENT_SCHEMA_VERSION, source_event_id, observed_at, payload_sha256)
    conn.execute(
        """
        INSERT OR IGNORE INTO evaluation_events (
            event_id, schema_version, user_id, event_type, source, source_event_id,
            source_revision, occurred_at, observed_at, effective_from, entity_type,
            entity_id, provider, provider_series_id, provider_episode_id, payload_json,
            payload_sha256, normalization_version, sync_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (event_id, EVENT_SCHEMA_VERSION, LOCAL_USER_ID, event_type, source,
         source_event_id, occurred_at, observed_at, occurred_at, entity_type,
         entity_id, source, series_id, episode_id, payload_json, payload_sha256,
         NORMALIZATION_VERSION, sync_run_id),
    )


def capture_provider_observations(conn, *, snapshot: Any, sync_run_id: int) -> None:
    """Dual-write normalized observations without retaining raw/account payloads.

    Identity is SHA-256(schema, provider, type, provider entity, occurred time,
    observation time, canonical privacy-safe payload). Re-ingesting the exact same
    snapshot is therefore a no-op while a later observation remains append-only.
    """
    source = snapshot.provider.lower()
    if source not in {"crunchyroll", "hidive"}:
        source = "system"
    observed_at = normalize_time(snapshot.generated_at)
    for entry in snapshot.series:
        payload = {
            "title": entry.title,
            "season_title": entry.season_title,
            "season_number": entry.season_number,
            "normalization_version": NORMALIZATION_VERSION,
        }
        _insert(conn, source=source, event_type="provider_series_observed",
                source_key=entry.provider_series_id, occurred_at=observed_at,
                observed_at=observed_at, entity_type="anime",
                entity_id=f"{source}:series:{entry.provider_series_id}",
                series_id=entry.provider_series_id, episode_id=None,
                payload=payload, sync_run_id=sync_run_id)
    for entry in snapshot.progress:
        occurred_at = normalize_time(entry.last_watched_at) if entry.last_watched_at else observed_at
        payload = {
            "episode_number": entry.episode_number,
            "episode_title": entry.episode_title,
            "playback_position_ms": entry.playback_position_ms,
            "duration_ms": entry.duration_ms,
            "completion_ratio": entry.completion_ratio,
            "audio_locale": entry.audio_locale,
            "progress_source_surface": entry.progress_source_surface,
            "progress_observation_kind": entry.progress_observation_kind,
            "completion_assertion": entry.completion_assertion,
            "provider_normalization_version": entry.normalization_logic_version,
            "normalization_version": NORMALIZATION_VERSION,
        }
        _insert(conn, source=source, event_type="provider_play",
                source_key=entry.provider_episode_id, occurred_at=occurred_at,
                observed_at=observed_at, entity_type="episode",
                entity_id=f"{source}:episode:{entry.provider_episode_id}",
                series_id=entry.provider_series_id, episode_id=entry.provider_episode_id,
                payload=payload, sync_run_id=sync_run_id)
    for entry in snapshot.watchlist:
        occurred_at = normalize_time(entry.added_at) if entry.added_at else observed_at
        payload = {
            "status": entry.status,
            "list_kind": entry.list_kind,
            "provider_item_type": entry.provider_item_type,
            "normalization_version": NORMALIZATION_VERSION,
        }
        source_key = f"{entry.provider_series_id}:{entry.list_id or 'default'}"
        _insert(conn, source=source, event_type="provider_watchlist_state",
                source_key=source_key, occurred_at=occurred_at, observed_at=observed_at,
                entity_type="anime", entity_id=f"{source}:series:{entry.provider_series_id}",
                series_id=entry.provider_series_id, episode_id=None,
                payload=payload, sync_run_id=sync_run_id)


def event_row_to_v1(row: Any) -> dict[str, Any]:
    entity = {
        "entity_type": row["entity_type"], "entity_id": row["entity_id"],
        "mal_anime_id": None, "provider": row["provider"],
        "provider_series_id": row["provider_series_id"],
        "provider_episode_id": row["provider_episode_id"],
    }
    return {
        "schema_version": row["schema_version"], "event_id": row["event_id"],
        "user_id": row["user_id"], "event_type": row["event_type"],
        "source": row["source"], "source_event_id": row["source_event_id"],
        "source_revision": row["source_revision"], "occurred_at": row["occurred_at"],
        "observed_at": row["observed_at"], "effective_from": row["effective_from"],
        "effective_to": row["effective_to"], "supersedes_event_id": row["supersedes_event_id"],
        "entity": entity, "payload": json.loads(row["payload_json"]),
        "payload_sha256": row["payload_sha256"],
    }
