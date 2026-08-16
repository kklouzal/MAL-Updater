from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


COMPLETENESS_VALUES = frozenset({"complete", "partial", "unknown"})


@dataclass(slots=True)
class FetchProvenance:
    """Provider-neutral evidence describing one fetched provider surface."""

    surface: str
    completeness: str = "unknown"
    expected_total: int | None = None
    collected_count: int | None = None
    pages_fetched: int | None = None
    observed_at: str | None = None
    route: str | None = None
    profile: str | None = None
    region: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def complete(self) -> bool:
        return self.completeness == "complete"
