from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Iterable

from .recommendations import Recommendation


def _truthy_provider(providers: Iterable[str], name: str) -> str:
    normalized = {value.strip().lower() for value in providers if isinstance(value, str)}
    return "yes" if name.lower() in normalized else ""


def _english_dub_status(item: Recommendation) -> str:
    raw = item.context.get("english_dub")
    if isinstance(raw, bool):
        return "yes" if raw else ""
    haystack = " ".join(value for value in (item.title, item.season_title or "") if value)
    return "yes" if "english dub" in haystack.lower() or "(dub)" in haystack.lower() else ""


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return value
    return None


def _row(item: Recommendation) -> dict[str, Any]:
    context = item.context
    providers = item.available_providers()
    source_count = _number(context.get("supporting_source_count")) or _number(context.get("source_count")) or 0
    total_votes = _number(context.get("aggregated_recommendation_votes")) or _number(context.get("total_votes")) or 0
    mal_mean = _number(context.get("mean"))
    mal_popularity = _number(context.get("popularity"))
    return {
        "title": item.season_title or item.title,
        "score": item.priority,
        "source_count": source_count,
        "total_votes": total_votes,
        "crunchyroll": _truthy_provider(providers, "crunchyroll"),
        "hidive": _truthy_provider(providers, "hidive"),
        "english_dub": _english_dub_status(item),
        "mal_mean": mal_mean if mal_mean is not None else "",
        "mal_popularity": mal_popularity if mal_popularity is not None else "",
        "reasons": "; ".join(item.reasons),
        "kind": item.kind,
        "providers": ", ".join(providers),
        "provider_series_id": item.provider_series_id,
    }


_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("title", "Title", "text"),
    ("score", "Score", "number"),
    ("source_count", "Source count", "number"),
    ("total_votes", "Total votes", "number"),
    ("crunchyroll", "Crunchyroll", "text"),
    ("hidive", "HIDIVE", "text"),
    ("english_dub", "English dub", "text"),
    ("mal_mean", "MAL mean", "number"),
    ("mal_popularity", "MAL popularity", "number"),
    ("reasons", "Reasons", "text"),
)


def render_recommendation_dashboard(items: Iterable[Recommendation], *, title: str = "MAL-Updater recommendations") -> str:
    rows = [_row(item) for item in items]
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    head_cells = "".join(
        f'<th scope="col" data-key="{escape(key)}" data-type="{escape(kind)}" tabindex="0">{escape(label)}</th>'
        for key, label, kind in _COLUMNS
    )
    body_rows = []
    for row in rows:
        cells = "".join(f'<td data-key="{escape(key)}">{escape(str(row[key]))}</td>' for key, _, _ in _COLUMNS)
        body_rows.append(f'<tr data-kind="{escape(str(row["kind"]))}" data-providers="{escape(str(row["providers"]))}">{cells}</tr>')
    body = "\n".join(body_rows) or f'<tr><td colspan="{len(_COLUMNS)}">No recommendations found.</td></tr>'
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 2rem; background: #101418; color: #eef3f8; }}
    table {{ border-collapse: collapse; width: 100%; background: #161d24; }}
    th, td {{ border: 1px solid #2b3642; padding: .45rem .6rem; vertical-align: top; }}
    th {{ cursor: pointer; position: sticky; top: 0; background: #243140; }}
    tbody tr:nth-child(even) {{ background: #121920; }}
    .meta {{ color: #aebccc; }}
  </style>
</head>
<body>
  <h1>{escape(title)}</h1>
  <p class="meta">Generated {escape(generated_at)} from local recommendation data. Click any column header to sort.</p>
  <table id="recommendations">
    <thead><tr>{head_cells}</tr></thead>
    <tbody>
{body}
    </tbody>
  </table>
  <script>
  (() => {{
    const table = document.getElementById('recommendations');
    const tbody = table.tBodies[0];
    const getValue = (row, key, type) => {{
      const text = row.querySelector(`[data-key="${{key}}"]`)?.textContent.trim() || '';
      return type === 'number' ? (text === '' ? Number.NEGATIVE_INFINITY : Number(text)) : text.toLowerCase();
    }};
    table.querySelectorAll('th').forEach(th => {{
      th.addEventListener('click', () => {{
        const key = th.dataset.key;
        const type = th.dataset.type;
        const direction = th.dataset.direction === 'asc' ? 'desc' : 'asc';
        table.querySelectorAll('th').forEach(other => delete other.dataset.direction);
        th.dataset.direction = direction;
        const rows = Array.from(tbody.rows);
        rows.sort((a, b) => {{
          const av = getValue(a, key, type);
          const bv = getValue(b, key, type);
          if (av < bv) return direction === 'asc' ? -1 : 1;
          if (av > bv) return direction === 'asc' ? 1 : -1;
          return 0;
        }});
        rows.forEach(row => tbody.appendChild(row));
      }});
      th.addEventListener('keydown', event => {{ if (event.key === 'Enter' || event.key === ' ') th.click(); }});
    }});
  }})();
  </script>
</body>
</html>
"""


def write_recommendation_dashboard(path: Path, items: Iterable[Recommendation], *, title: str = "MAL-Updater recommendations") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_recommendation_dashboard(items, title=title), encoding="utf-8")
    return path
