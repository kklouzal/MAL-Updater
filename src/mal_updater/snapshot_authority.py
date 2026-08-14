from __future__ import annotations

# Verification is intentionally the only authority operation exposed by this
# module.  The sentinel and the sole code path that attaches it are closure-bound
# to ``hidive_snapshot.fetch_snapshot``.
from .hidive_snapshot import has_hidive_snapshot_authority

__all__ = ["has_hidive_snapshot_authority"]
