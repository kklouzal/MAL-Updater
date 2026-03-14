from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import AppConfig


class SnapshotValidationError(ValueError):
    pass


_ALLOWED_TOP_LEVEL_KEYS = {
    "contract_version",
    "generated_at",
    "provider",
    "account_id_hint",
    "series",
    "progress",
    "watchlist",
    "raw",
}

_ALLOWED_SERIES_KEYS = {
    "provider_series_id",
    "title",
    "season_title",
    "season_number",
}

_ALLOWED_PROGRESS_KEYS = {
    "provider_episode_id",
    "provider_series_id",
    "episode_number",
    "episode_title",
    "playback_position_ms",
    "duration_ms",
    "completion_ratio",
    "last_watched_at",
    "audio_locale",
    "subtitle_locale",
    "rating",
}

_ALLOWED_WATCHLIST_KEYS = {
    "provider_series_id",
    "added_at",
    "status",
}


def _expect_type(value: Any, expected: type | tuple[type, ...], path: str) -> None:
    if not isinstance(value, expected):
        if isinstance(expected, tuple):
            expected_name = ", ".join(t.__name__ for t in expected)
        else:
            expected_name = expected.__name__
        raise SnapshotValidationError(f"{path} must be {expected_name}")


def _expect_optional_type(value: Any, expected: type | tuple[type, ...], path: str) -> None:
    if value is None:
        return
    _expect_type(value, expected, path)


def _expect_object_keys(obj: dict[str, Any], allowed: set[str], required: set[str], path: str) -> None:
    extra = sorted(set(obj) - allowed)
    if extra:
        raise SnapshotValidationError(f"{path} has unexpected keys: {', '.join(extra)}")
    missing = sorted(required - set(obj))
    if missing:
        raise SnapshotValidationError(f"{path} is missing required keys: {', '.join(missing)}")


def _expect_iso_datetime(value: str | None, path: str) -> None:
    if value is None:
        return
    _expect_type(value, str, path)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotValidationError(f"{path} must be an ISO-8601 datetime") from exc


def _validate_series_entry(entry: Any, index: int) -> str:
    path = f"series[{index}]"
    _expect_type(entry, dict, path)
    _expect_object_keys(entry, _ALLOWED_SERIES_KEYS, {"provider_series_id", "title"}, path)
    _expect_type(entry["provider_series_id"], str, f"{path}.provider_series_id")
    _expect_type(entry["title"], str, f"{path}.title")
    _expect_optional_type(entry.get("season_title"), str, f"{path}.season_title")
    _expect_optional_type(entry.get("season_number"), int, f"{path}.season_number")
    return entry["provider_series_id"]


def _validate_progress_entry(entry: Any, index: int, known_series_ids: set[str]) -> None:
    path = f"progress[{index}]"
    _expect_type(entry, dict, path)
    _expect_object_keys(
        entry,
        _ALLOWED_PROGRESS_KEYS,
        {
            "provider_episode_id",
            "provider_series_id",
            "episode_number",
            "episode_title",
            "playback_position_ms",
            "duration_ms",
            "completion_ratio",
            "last_watched_at",
            "audio_locale",
            "subtitle_locale",
            "rating",
        },
        path,
    )
    _expect_type(entry["provider_episode_id"], str, f"{path}.provider_episode_id")
    _expect_type(entry["provider_series_id"], str, f"{path}.provider_series_id")
    if entry["provider_series_id"] not in known_series_ids:
        raise SnapshotValidationError(
            f"{path}.provider_series_id references unknown series id: {entry['provider_series_id']}"
        )
    _expect_optional_type(entry.get("episode_number"), int, f"{path}.episode_number")
    _expect_optional_type(entry.get("episode_title"), str, f"{path}.episode_title")
    _expect_optional_type(entry.get("playback_position_ms"), int, f"{path}.playback_position_ms")
    _expect_optional_type(entry.get("duration_ms"), int, f"{path}.duration_ms")
    if entry.get("playback_position_ms") is not None and entry["playback_position_ms"] < 0:
        raise SnapshotValidationError(f"{path}.playback_position_ms must be >= 0")
    if entry.get("duration_ms") is not None and entry["duration_ms"] < 0:
        raise SnapshotValidationError(f"{path}.duration_ms must be >= 0")
    _expect_optional_type(entry.get("completion_ratio"), (int, float), f"{path}.completion_ratio")
    if entry.get("completion_ratio") is not None and not 0 <= float(entry["completion_ratio"]) <= 1:
        raise SnapshotValidationError(f"{path}.completion_ratio must be between 0 and 1")
    _expect_iso_datetime(entry.get("last_watched_at"), f"{path}.last_watched_at")
    _expect_optional_type(entry.get("audio_locale"), str, f"{path}.audio_locale")
    _expect_optional_type(entry.get("subtitle_locale"), str, f"{path}.subtitle_locale")
    _expect_optional_type(entry.get("rating"), str, f"{path}.rating")


def _validate_watchlist_entry(entry: Any, index: int, known_series_ids: set[str]) -> None:
    path = f"watchlist[{index}]"
    _expect_type(entry, dict, path)
    _expect_object_keys(entry, _ALLOWED_WATCHLIST_KEYS, {"provider_series_id", "added_at", "status"}, path)
    _expect_type(entry["provider_series_id"], str, f"{path}.provider_series_id")
    if entry["provider_series_id"] not in known_series_ids:
        raise SnapshotValidationError(
            f"{path}.provider_series_id references unknown series id: {entry['provider_series_id']}"
        )
    _expect_iso_datetime(entry.get("added_at"), f"{path}.added_at")
    _expect_optional_type(entry.get("status"), str, f"{path}.status")


def validate_snapshot_payload(payload: Any, config: AppConfig) -> dict[str, Any]:
    _expect_type(payload, dict, "snapshot")
    _expect_object_keys(
        payload,
        _ALLOWED_TOP_LEVEL_KEYS,
        {"contract_version", "generated_at", "provider", "series", "progress", "watchlist", "raw"},
        "snapshot",
    )

    _expect_type(payload["contract_version"], str, "snapshot.contract_version")
    if payload["contract_version"] != config.contract_version:
        raise SnapshotValidationError(
            f"snapshot.contract_version must match configured version {config.contract_version!r}"
        )

    _expect_iso_datetime(payload["generated_at"], "snapshot.generated_at")
    _expect_type(payload["provider"], str, "snapshot.provider")
    if payload["provider"] != "crunchyroll":
        raise SnapshotValidationError("snapshot.provider must be 'crunchyroll'")
    _expect_optional_type(payload.get("account_id_hint"), str, "snapshot.account_id_hint")

    _expect_type(payload["series"], list, "snapshot.series")
    _expect_type(payload["progress"], list, "snapshot.progress")
    _expect_type(payload["watchlist"], list, "snapshot.watchlist")
    _expect_type(payload["raw"], dict, "snapshot.raw")

    known_series_ids: set[str] = set()
    for index, entry in enumerate(payload["series"]):
        series_id = _validate_series_entry(entry, index)
        if series_id in known_series_ids:
            raise SnapshotValidationError(f"series[{index}].provider_series_id is duplicated: {series_id}")
        known_series_ids.add(series_id)

    seen_episode_ids: set[str] = set()
    for index, entry in enumerate(payload["progress"]):
        _validate_progress_entry(entry, index, known_series_ids)
        episode_id = entry["provider_episode_id"]
        if episode_id in seen_episode_ids:
            raise SnapshotValidationError(f"progress[{index}].provider_episode_id is duplicated: {episode_id}")
        seen_episode_ids.add(episode_id)

    seen_watchlist_series: set[str] = set()
    for index, entry in enumerate(payload["watchlist"]):
        _validate_watchlist_entry(entry, index, known_series_ids)
        series_id = entry["provider_series_id"]
        if series_id in seen_watchlist_series:
            raise SnapshotValidationError(f"watchlist[{index}].provider_series_id is duplicated: {series_id}")
        seen_watchlist_series.add(series_id)

    return payload


def load_and_validate_snapshot(path: Path, config: AppConfig) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return validate_snapshot_payload(payload, config)
