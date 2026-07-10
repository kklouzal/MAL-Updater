from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

from .db import (
    bootstrap_database,
    connect,
    get_mal_recommendation_harvest_coverage,
    get_operational_snapshot,
    list_latest_recommendation_snapshot_rows,
)

from .recommendations import Recommendation

DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT = 120

_SECTION_METADATA: dict[str, dict[str, str]] = {
    "discovery_available_now": {
        "label": "Discovery candidates",
        "description": "Fresh discovery candidates with current Crunchyroll or HIDIVE availability and English dub evidence.",
        "title_label": "English title",
    },
    "discovery_high_confidence": {
        "label": "High-confidence discovery",
        "description": "Fresh MAL/catalog recommendation candidates that do not yet have provider availability evidence.",
        "title_label": "English title",
    },
    "discovery_candidate": {
        "label": "Title recommendations / discovery",
        "description": "All fresh MAL user-recommendation-backed discovery candidates, with provider availability evidence when known.",
        "title_label": "English title",
    },
    "resume_backlog": {
        "label": "Resume backlog",
        "description": "Known provider or MAL-list titles that look ready to resume from existing watch progress.",
        "title_label": "Title",
    },
}

def _section_metadata_for(kind: str) -> dict[str, str]:
    meta = _SECTION_METADATA.get(kind)
    if meta is None:
        label = kind.replace("_", " ").strip().title() if kind else "Unknown"
        meta = {"label": label, "description": "Recommendation rows from the latest persisted scoring snapshot.", "title_label": "Title"}
    return {"kind": kind, **meta}


def _compact_list(value: Any, *, limit: int = 6) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        return [value]
    if isinstance(value, dict):
        value = value.values()
    if not isinstance(value, Iterable):
        return []
    result: list[Any] = []
    for item in value:
        if item is None or isinstance(item, bool):
            continue
        if isinstance(item, (str, int, float)):
            result.append(item)
        elif isinstance(item, dict):
            for key in ("mal_anime_id", "seed_mal_anime_id", "title", "name", "id"):
                if item.get(key) is not None:
                    result.append(item[key])
                    break
        if len(result) >= limit:
            break
    return result


def _watch_status_from_context(context: dict[str, Any]) -> str:
    mal_status = context.get("mal_watch_status") or ""
    mal_watched = _number(context.get("mal_num_episodes_watched"))
    mal_total = _number(context.get("mal_num_episodes"))
    if mal_watched is not None:
        return f"{mal_status or 'unknown'} ({mal_watched}/{mal_total if mal_total is not None else '?'})"
    if context.get("mal_watch_metadata_uncertain"):
        return "unknown/partial"
    return str(mal_status or "")


def _snapshot_evidence(row: dict[str, Any]) -> dict[str, Any]:
    context = row.get("context") if isinstance(row.get("context"), dict) else {}
    merged = {**context, **{k: v for k, v in row.items() if v is not None}}
    providers = row.get("availability_providers") or merged.get("available_via_providers") or merged.get("providers") or []
    if isinstance(providers, str):
        providers = [providers]
    seed_ids = _compact_list(merged.get("supporting_seed_ids") or merged.get("supporting_seed_mal_anime_ids") or merged.get("seed_mal_anime_ids") or merged.get("seed_ids"))
    seed_titles = _compact_list(merged.get("supporting_seed_titles") or merged.get("seed_titles"))
    seed_count = _number(merged.get("supporting_source_count")) or _number(merged.get("source_count")) or len(seed_ids) or len(seed_titles) or 0
    votes = _number(merged.get("aggregated_recommendation_votes")) or _number(merged.get("total_votes")) or _number(merged.get("recommendation_votes")) or 0
    dub = row.get("dub_signal") or merged.get("english_dub") or merged.get("dub_signal") or ""
    if isinstance(dub, bool):
        dub = "yes" if dub else ""
    return {
        "mal_recommendation_votes": votes,
        "seed_count": seed_count,
        "seed_ids": seed_ids,
        "seed_titles": seed_titles,
        "compact_seeds": ", ".join(str(x) for x in (seed_titles or seed_ids)),
        "availability_providers": list(providers) if isinstance(providers, list) else [],
        "availability_provider_label": ", ".join(str(x) for x in providers) if isinstance(providers, list) else "",
        "dub_signal": str(dub or ""),
        "mal_watch_status": _watch_status_from_context(merged),
    }


def _truthy_provider(providers: Iterable[str], name: str) -> str:
    normalized = {value.strip().lower() for value in providers if isinstance(value, str)}
    return "yes" if name.lower() in normalized else ""


def _english_dub_status(item: Recommendation) -> str:
    raw = item.context.get("english_dub")
    signal = item.context.get("english_dub_signal")
    if isinstance(raw, bool):
        return "yes" if raw else ""
    if str(signal).strip().lower() in {"present", "yes", "true", "english dub"}:
        return "yes"
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
    completed = _number(context.get("completed_episode_count"))
    max_episode = _number(context.get("max_episode_number")) or _number(context.get("available_episode_count"))
    provider_progress = f"{completed}/{max_episode}" if completed is not None and max_episode is not None else (str(completed) if completed is not None else "")
    mal_status = _watch_status_from_context(context)
    english_title = context.get("english_title") if isinstance(context.get("english_title"), str) else ""
    genres = context.get("genres") if isinstance(context.get("genres"), list) else []
    display_title = english_title.strip() or item.season_title or item.title
    if english_title and item.season_title and item.season_title != english_title:
        display_title = f"{english_title} ({item.season_title})"
    return {
        "title": display_title,
        "score": item.priority,
        "source_count": source_count,
        "total_votes": total_votes,
        "crunchyroll": _truthy_provider(providers, "crunchyroll"),
        "hidive": _truthy_provider(providers, "hidive"),
        "english_dub": _english_dub_status(item),
        "mal_mean": mal_mean if mal_mean is not None else "",
        "mal_popularity": mal_popularity if mal_popularity is not None else "",
        "genres": ", ".join(str(value) for value in genres),
        "provider_progress": provider_progress,
        "mal_watch_status": mal_status,
        "reasons": "; ".join(item.reasons),
        "kind": item.kind,
        "providers": ", ".join(providers),
        "availability_providers": context.get("available_via_providers") if isinstance(context.get("available_via_providers"), list) else [],
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
    ("genres", "Genres", "text"),
    ("provider_progress", "Provider progress", "text"),
    ("mal_watch_status", "MAL watch status", "text"),
)


_STATIC_SECTION_ORDER: tuple[str, ...] = ("discovery_available_now", "discovery_high_confidence", "resume_backlog")


def _is_displayable_discovery(row: dict[str, Any]) -> bool:
    availability = row.get("availability_providers")
    if not (isinstance(availability, list) and any(str(value).strip() for value in availability)):
        return False
    context = row.get("context") if isinstance(row.get("context"), dict) else {}
    scorecard = row.get("scorecard") if isinstance(row.get("scorecard"), dict) else {}
    features = scorecard.get("features") if isinstance(scorecard.get("features"), dict) else {}
    dub_signal = row.get("english_dub") or context.get("english_dub_signal") or features.get("english_dub_signal") or context.get("english_dub")
    return dub_signal is True or str(dub_signal).strip().lower() in {"present", "yes", "true", "english dub"}


def _static_section_key(row: dict[str, Any]) -> str | None:
    kind = str(row.get("kind") or "unknown")
    if kind == "discovery_candidate":
        return "discovery_available_now" if _is_displayable_discovery(row) else "discovery_high_confidence"
    return kind


def _static_sections(rows: list[dict[str, Any]]) -> list[tuple[dict[str, str], list[dict[str, Any]], int]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        section_key = _static_section_key(row)
        if section_key is not None:
            grouped.setdefault(section_key, []).append(row)
    ordered_keys = list(_STATIC_SECTION_ORDER)
    ordered_keys.extend(sorted(key for key in grouped if key not in _STATIC_SECTION_ORDER))
    return [(_section_metadata_for(key), grouped.get(key, []), len(grouped.get(key, []))) for key in ordered_keys]


def _section_display_budget(limit: int) -> int:
    return max(1, int(limit))


def _cap_sections(
    sections: list[tuple[dict[str, str], list[dict[str, Any]], int]], *, limit: int | None
) -> list[tuple[dict[str, str], list[dict[str, Any]], int]]:
    if limit is None:
        return sections
    budget = _section_display_budget(limit)
    return [(meta, rows[:budget], total) for meta, rows, total in sections]


def _section_count_label(displayed: int, total: int) -> str:
    if displayed < total:
        return f"{displayed} of {total}"
    return str(total)


def _recommendation_table(rows: list[dict[str, Any]], table_id: str, head_cells: str | None = None, *, title_label: str = "Title") -> str:
    if head_cells is None:
        head_cells = _table_head_cells(title_label=title_label)
    body_rows = []
    for row in rows:
        cells = "".join(f'<td data-key="{escape(key)}">{escape(str(row[key]))}</td>' for key, _, _ in _COLUMNS)
        body_rows.append(f'<tr data-kind="{escape(str(row["kind"]))}" data-providers="{escape(str(row["providers"]))}">{cells}</tr>')
    body = "\n".join(body_rows) or f'<tr><td colspan="{len(_COLUMNS)}">No recommendations found.</td></tr>'
    return f"""<table id="{escape(table_id)}" class="recommendations">
    <thead><tr>{head_cells}</tr></thead>
    <tbody>
{body}
    </tbody>
  </table>"""


def _table_head_cells(*, title_label: str = "Title") -> str:
    return "".join(
        f'<th scope="col" data-key="{escape(key)}" data-type="{escape(kind)}" tabindex="0">{escape(title_label if key == "title" else label)}</th>'
        for key, label, kind in _COLUMNS
    )


def render_recommendation_dashboard(items: Iterable[Recommendation], *, title: str = "MAL-Updater recommendations", limit: int | None = None) -> str:
    rows = [_row(item) for item in items]
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if rows:
        sections = []
        for index, (meta, section_rows, total_rows) in enumerate(_cap_sections(_static_sections(rows), limit=limit), start=1):
            description = f'<p class="meta">{escape(meta["description"])}</p>' if meta.get("description") else ""
            count_label = _section_count_label(len(section_rows), total_rows)
            sections.append(
                f'<section><h2>{escape(meta["label"])} ({escape(count_label)})</h2>{description}'
                f'{_recommendation_table(section_rows, f"recommendations-{index}", title_label=meta.get("title_label", "Title"))}</section>'
            )
        body = "\n  ".join(sections)
    else:
        body = _recommendation_table([], "recommendations")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 2rem; background: #101418; color: #eef3f8; }}
    section {{ margin-top: 2rem; }}
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
  {body}
  <script>
  (() => {{
    const getValue = (row, key, type) => {{
      const text = row.querySelector(`[data-key="${{key}}"]`)?.textContent.trim() || '';
      return type === 'number' ? (text === '' ? Number.NEGATIVE_INFINITY : Number(text)) : text.toLowerCase();
    }};
    document.querySelectorAll('table.recommendations').forEach(table => {{
      const tbody = table.tBodies[0];
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
    }});
  }})();
  </script>
</body>
</html>
"""


def write_recommendation_dashboard(path: Path, items: Iterable[Recommendation], *, title: str = "MAL-Updater recommendations", limit: int | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_recommendation_dashboard(items, title=title, limit=limit), encoding="utf-8")
    return path


def _snapshot_row_to_dict(row: Any) -> dict[str, Any]:
    payload = {
        "id": row.id,
        "run_id": row.run_id,
        "generated_at": row.generated_at,
        "kind": row.kind,
        "provider": row.provider,
        "title": row.title,
        "provider_series_id": row.provider_series_id,
        "mal_anime_id": row.mal_anime_id,
        "score": row.score,
        "priority": row.priority,
        "reasons": [] if row.kind == "discovery_candidate" else row.reasons,
        "scorecard": row.scorecard,
        "context": row.context,
        "availability_providers": row.availability_providers,
        "dub_signal": row.dub_signal,
        "availability_confidence": row.availability_confidence,
    }
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    english_title = context.get("english_title")
    genres = context.get("genres")
    if isinstance(english_title, str) and english_title.strip():
        payload["english_title"] = english_title.strip()
        payload["display_title"] = f"{english_title.strip()} ({row.title})" if row.title and row.title != english_title.strip() else english_title.strip()
    else:
        payload["display_title"] = row.title
    payload["genres"] = genres if isinstance(genres, list) else []
    payload["evidence"] = _snapshot_evidence(payload)
    return payload


def _latest_snapshot_summary(db_path: Path) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT run_id, MIN(generated_at) AS first_generated_at, MAX(generated_at) AS generated_at, COUNT(*) AS item_count
            FROM recommendation_score_snapshots
            WHERE run_id = (SELECT run_id FROM recommendation_score_snapshots ORDER BY generated_at DESC, id DESC LIMIT 1)
            GROUP BY run_id
            """
        ).fetchone()
    if row is None:
        return None
    return {"run_id": row["run_id"], "generated_at": row["generated_at"], "first_generated_at": row["first_generated_at"], "item_count": int(row["item_count"] or 0)}


def _recent_sync_runs(db_path: Path, *, limit: int = 8) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, provider, contract_version, mode, started_at, completed_at, status, summary_json
            FROM sync_runs ORDER BY started_at DESC, id DESC LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    runs: list[dict[str, Any]] = []
    for row in rows:
        summary = None
        if row["summary_json"]:
            try:
                summary = json.loads(row["summary_json"])
            except json.JSONDecodeError:
                summary = {"_decode_error": True}
        runs.append({"id": int(row["id"]), "provider": row["provider"], "contract_version": row["contract_version"], "mode": row["mode"], "started_at": row["started_at"], "completed_at": row["completed_at"], "status": row["status"], "summary": summary})
    return runs


def build_dashboard_payload(db_path: Path, *, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, stale_after_days: int = 14) -> dict[str, Any]:
    """Return the current dashboard model directly from SQLite state."""
    bootstrap_database(db_path)
    operational = get_operational_snapshot(db_path)
    coverage = get_mal_recommendation_harvest_coverage(db_path, stale_after_days=stale_after_days)
    rows = [_snapshot_row_to_dict(row) for row in list_latest_recommendation_snapshot_rows(db_path, limit=None)]
    sections: dict[str, list[dict[str, Any]]] = {}
    section_totals: dict[str, int] = {}
    display_limit = _section_display_budget(limit)
    for row in rows:
        section_key = _static_section_key(row)
        if section_key is None:
            continue
        section_totals[section_key] = section_totals.get(section_key, 0) + 1
        if len(sections.setdefault(section_key, [])) < display_limit:
            sections[section_key].append(row)
    latest_snapshot = _latest_snapshot_summary(db_path)
    latest_run = operational.get("latest_sync_run") or {}
    indicators: list[dict[str, str]] = []
    if latest_snapshot is None:
        indicators.append({"level": "warning", "message": "No persisted recommendation snapshot is available yet."})
    elif latest_snapshot.get("item_count", 0) == 0:
        indicators.append({"level": "warning", "message": "Latest recommendation snapshot has no items."})
    if latest_run and latest_run.get("status") not in (None, "completed"):
        indicators.append({"level": "error", "message": f"Latest provider sync run is {latest_run.get('status')}."})
    cov_summary = coverage.get("summary") or {}
    if cov_summary.get("unharvested") or cov_summary.get("stale"):
        indicators.append({"level": "warning", "message": "Recommendation harvest coverage is stale or incomplete."})
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "snapshot": latest_snapshot,
        "recommendations": {"items": [row for section_rows in sections.values() for row in section_rows], "sections": sections, "section_totals": section_totals, "section_metadata": {kind: _section_metadata_for(kind) for kind in sections}, "limit": max(1, int(limit)), "limit_scope": "per_section"},
        "coverage": coverage,
        "operational": operational,
        "recent_sync_runs": _recent_sync_runs(db_path),
        "indicators": indicators,
    }


def render_dynamic_dashboard_html(*, title: str = "MAL-Updater live dashboard") -> str:
    template = """<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>__TITLE__</title><style>
body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:2rem;background:#101418;color:#eef3f8}a{color:#8cc8ff}.muted{color:#aebccc}.bad{color:#ff9b9b}.warn{color:#ffd37a}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:1rem}.card{background:#161d24;border:1px solid #2b3642;border-radius:.6rem;padding:1rem}table{border-collapse:collapse;width:100%;background:#161d24;margin:.75rem 0 1.5rem}th,td{border:1px solid #2b3642;padding:.45rem .6rem;vertical-align:top}th{background:#243140;text-align:left}code{white-space:pre-wrap}
</style></head><body><h1>__TITLE__</h1><p class=\"muted\">Live local view. Data is fetched from <code>/api/dashboard</code> on load and every 60 seconds.</p><div id=\"app\">Loading…</div><script>
const esc = value => String(value ?? '').replace(/[&<>\"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));
const count = obj => Object.entries(obj || {}).map(([k,v]) => `<div><b>${esc(k)}:</b> ${esc(JSON.stringify(v))}</div>`).join('') || '<span class=\"muted\">none</span>';
function recTable(rows, meta = {}){ if(!rows?.length) return '<p class="muted">No rows in latest snapshot.</p>'; const progress = r => { const c = r.context || {}; const done = c.completed_episode_count ?? c.max_completed_episode_number; const max = c.max_episode_number ?? c.available_episode_count; return done != null && max != null ? `${done}/${max}` : (done != null ? `${done}` : ''); }; const titleLabel = meta.title_label || 'Title'; return `<table><thead><tr><th>Priority</th><th>${esc(titleLabel)}</th><th>Genres</th><th>Providers</th><th>MAL rec votes</th><th>Seeds</th><th>Dub</th><th>Provider progress</th><th>MAL watch status</th></tr></thead><tbody>${rows.map(r => { const e = r.evidence || {}; return `<tr><td>${esc(r.priority ?? r.score)}</td><td>${esc(r.display_title || r.english_title || r.title)}</td><td>${esc((r.genres || []).join(', '))}</td><td>${esc(e.availability_provider_label || (r.availability_providers || []).join(', '))}</td><td>${esc(e.mal_recommendation_votes ?? '')}</td><td>${esc(e.seed_count ?? '')}${e.compact_seeds ? ` <span class="muted">(${esc(e.compact_seeds)})</span>` : ''}</td><td>${esc(e.dub_signal || '')}</td><td>${esc(progress(r))}</td><td>${esc(e.mal_watch_status || '')}</td></tr>`; }).join('')}</tbody></table>`; }
function syncTable(runs){ if(!runs?.length) return '<p class=\"muted\">No sync runs recorded.</p>'; return `<table><thead><tr><th>ID</th><th>Provider</th><th>Mode</th><th>Status</th><th>Started</th><th>Completed</th></tr></thead><tbody>${runs.map(r => `<tr><td>${esc(r.id)}</td><td>${esc(r.provider)}</td><td>${esc(r.mode)}</td><td>${esc(r.status)}</td><td>${esc(r.started_at)}</td><td>${esc(r.completed_at)}</td></tr>`).join('')}</tbody></table>`; }
async function refresh(){ const res = await fetch('/api/dashboard', {cache:'no-store'}); const data = await res.json(); const indicators = (data.indicators || []).map(i => `<li class=\"${i.level === 'error' ? 'bad' : 'warn'}\">${esc(i.message)}</li>`).join('') || '<li class=\"muted\">No stale/partial/failure indicators.</li>'; document.getElementById('app').innerHTML = `<div class=\"grid\"><section class=\"card\"><h2>Snapshot</h2><div><b>Run:</b> ${esc(data.snapshot?.run_id || 'none')}</div><div><b>Generated:</b> ${esc(data.snapshot?.generated_at || 'n/a')}</div><div><b>Items:</b> ${esc(data.snapshot?.item_count || 0)}</div></section><section class=\"card\"><h2>Coverage</h2>${count(data.coverage?.summary)}</section><section class=\"card\"><h2>Providers</h2>${count(data.operational?.provider_counts_by_provider)}</section><section class=\"card\"><h2>Mappings</h2>${count(data.operational?.mappings)}</section><section class=\"card\"><h2>Review queue</h2>${count(data.operational?.review_queue)}</section></div><section><h2>Indicators</h2><ul>${indicators}</ul></section><section><h2>Latest recommendation snapshot</h2>${Object.entries(data.recommendations?.sections || {}).map(([name, rows]) => { const meta = data.recommendations?.section_metadata?.[name] || {label:name, description:''}; const total = data.recommendations?.section_totals?.[name] ?? rows.length; const countLabel = rows.length < total ? `${rows.length} of ${total}` : `${total}`; return `<h3>${esc(meta.label || name)} (${esc(countLabel)})</h3>${meta.description ? `<p class="muted">${esc(meta.description)}</p>` : ''}${recTable(rows, meta)}`; }).join('') || recTable([])}</section><section><h2>Recent provider sync runs</h2>${syncTable(data.recent_sync_runs)}</section><p class=\"muted\">Last refreshed ${esc(data.generated_at)} · <a href=\"/api/dashboard\">JSON</a></p>`; }
refresh().catch(err => document.getElementById('app').innerHTML = `<p class=\"bad\">${esc(err.message)}</p>`); setInterval(refresh, 60000);
</script></body></html>"""
    return template.replace("__TITLE__", escape(title))


def make_dashboard_handler(db_path: Path, *, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, stale_after_days: int = 14) -> type[BaseHTTPRequestHandler]:
    class DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, body_text: str) -> None:
            body = body_text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            request_limit = int(query.get("limit", [limit])[0] or limit)
            if parsed.path in ("/", "/dashboard"):
                self._send_html(render_dynamic_dashboard_html())
                return
            if parsed.path == "/api/dashboard":
                self._send_json(build_dashboard_payload(db_path, limit=request_limit, stale_after_days=stale_after_days))
                return
            self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)

    return DashboardHandler


def serve_dashboard(db_path: Path, *, host: str = "127.0.0.1", port: int = 8766, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT) -> None:
    bootstrap_database(db_path)
    server = ThreadingHTTPServer((host, int(port)), make_dashboard_handler(db_path, limit=limit))
    print(f"Serving MAL-Updater dashboard at http://{host}:{server.server_port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
