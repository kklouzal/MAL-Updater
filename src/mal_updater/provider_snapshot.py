from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .contracts import ProviderSnapshot


def snapshot_to_dict(snapshot: ProviderSnapshot) -> dict[str, Any]:
    return {
        "contract_version": snapshot.contract_version,
        "generated_at": snapshot.generated_at,
        "provider": snapshot.provider,
        "account_id_hint": snapshot.account_id_hint,
        "series": [asdict(item) for item in snapshot.series],
        "progress": [asdict(item) for item in snapshot.progress],
        "watchlist": [asdict(item) for item in snapshot.watchlist],
        "raw": snapshot.raw,
    }


def write_snapshot_file(path: Path, snapshot: ProviderSnapshot) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot_to_dict(snapshot), indent=2) + "\n", encoding="utf-8")
    return path
