"""Shared look for the web pages: colour tokens, base styles and the top bar.

The dashboard, debug and settings pages are separate documents served by
different handlers; they share this header and palette so the product reads
as one application.
"""

from __future__ import annotations

from html import escape

FAVICON_LINK = '<link rel="icon" href="/favicon.svg" type="image/svg+xml">'

_BRAND_MARK = (
    '<svg viewBox="0 0 64 64" aria-hidden="true" focusable="false">'
    '<rect width="64" height="64" rx="12" fill="#151b23"/>'
    '<path d="M13 46V18h8l11 15 11-15h8v28h-8V30L32 44 21 30v16z" fill="#8cc8ff"/>'
    '<path d="M13 52h38" stroke="#7ee2a8" stroke-width="5" stroke-linecap="round"/></svg>'
)

SHELL_CSS = """
:root{color-scheme:dark;--bg:#0d1117;--surface:#151b23;--surface-2:#1a222c;--surface-3:#222c38;--border:#2a3542;
--border-strong:#3c4a5a;--text:#e8edf3;--muted:#9aabbd;--faint:#73859a;--accent:#8cc8ff;--accent-strong:#b9ddff;
--accent-ink:#0b1420;--good:#7ee2a8;--warn:#ffd37a;--bad:#ff9b9b;--radius:10px;--page-max:110rem;--topbar-h:3.25rem}
*{box-sizing:border-box}
html{background:var(--bg)}
body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
a{color:var(--accent)}
a:hover{color:var(--accent-strong)}
:focus-visible{outline:3px solid var(--accent);outline-offset:2px}
h1,h2,h3{line-height:1.2;margin:0}
code{font:.9em ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap}
.muted{color:var(--muted)}
.good{color:var(--good)}
.warn{color:var(--warn)}
.bad{color:var(--bad)}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
.topbar{position:sticky;top:0;z-index:30;height:var(--topbar-h);background:rgba(13,17,23,.92);backdrop-filter:blur(8px);border-bottom:1px solid var(--border)}
.topbar-inner{max-width:var(--page-max);height:100%;margin:0 auto;padding:0 1.5rem;display:flex;align-items:center;gap:1.25rem}
.brand{display:flex;align-items:center;gap:.55rem;color:var(--text);font-weight:750;text-decoration:none;white-space:nowrap}
.brand:hover{color:var(--text)}
.brand svg{width:1.55rem;height:1.55rem;display:block}
.primary-nav{display:flex;gap:.2rem}
.primary-nav a{color:var(--muted);text-decoration:none;font-weight:600;padding:.35rem .7rem;border-radius:.45rem}
.primary-nav a:hover{color:var(--text);background:var(--surface-2)}
.primary-nav a[aria-current="page"]{color:var(--text);background:var(--surface-3)}
.topbar-actions{margin-left:auto;display:flex;align-items:center;gap:.5rem}
.button{font:inherit;font-weight:650;color:var(--text);background:var(--surface-3);border:1px solid var(--border-strong);border-radius:.45rem;padding:.38rem .8rem;cursor:pointer;line-height:1.3}
.button:hover{background:#2b3746}
.button.primary{color:var(--accent-ink);background:var(--accent);border-color:var(--accent)}
.button.primary:hover{background:var(--accent-strong)}
.button.small{padding:.22rem .6rem;font-size:.86rem}
.page{max-width:var(--page-max);margin:0 auto;padding:1.5rem}
.page-head{display:flex;flex-wrap:wrap;align-items:baseline;justify-content:space-between;gap:.25rem 1rem;margin:.25rem 0 1.25rem}
.page-head h1{font-size:1.6rem}
@media (max-width:40rem){
.topbar-inner{padding:0 1rem;gap:.6rem}
.brand span{display:none}
.primary-nav a{padding:.35rem .5rem}
.page{padding:1rem}
}
@media (max-width:24rem){
.topbar-inner{padding:0 .6rem;gap:.35rem}
.primary-nav a{padding:.35rem .4rem}
.topbar-actions{margin-left:0}
}
@media print{.topbar{display:none}}
"""


def top_bar(active: str, *, dashboard_href: str = "/", debug_href: str = "/debug", settings_href: str | None = "/settings", actions: str = "") -> str:
    """Brand, primary navigation (current page marked) and optional page actions."""
    links = [("dashboard", dashboard_href, "Dashboard"), ("debug", debug_href, "Debug")]
    if settings_href:
        links.append(("settings", settings_href, "Settings"))
    current = ' aria-current="page"'
    nav = "".join(
        f'<a href="{escape(href, quote=True)}"{current if key == active else ""}>{label}</a>' for key, href, label in links
    )
    return (
        '<header class="topbar"><div class="topbar-inner">'
        f'<a class="brand" href="{escape(dashboard_href, quote=True)}">{_BRAND_MARK}<span>MAL-Updater</span></a>'
        f'<nav class="primary-nav" aria-label="Primary">{nav}</nav>'
        f'<div class="topbar-actions">{actions}</div>'
        "</div></header>"
    )
