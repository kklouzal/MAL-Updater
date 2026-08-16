from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from .contracts import ProviderSnapshot
from .persistence import atomic_write_json


def snapshot_to_dict(snapshot: ProviderSnapshot) -> dict[str, Any]:
    payload = {
        "contract_version": snapshot.contract_version,
        "generated_at": snapshot.generated_at,
        "provider": snapshot.provider,
        "account_id_hint": snapshot.account_id_hint,
        "series": [asdict(item) for item in snapshot.series],
        "progress": [asdict(item) for item in snapshot.progress],
        "watchlist": [asdict(item) for item in snapshot.watchlist],
        "raw": snapshot.raw,
    }
    if snapshot.fetch_provenance:
        payload["fetch_provenance"] = [asdict(item) for item in snapshot.fetch_provenance]
    return payload


def write_snapshot_file(path: Path, snapshot: ProviderSnapshot) -> Path:
    atomic_write_json(path, snapshot_to_dict(snapshot), indent=2)
    return path
