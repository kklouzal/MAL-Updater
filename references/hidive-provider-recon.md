# HIDIVE provider notes

Observed behavior of the HIDIVE web API that the provider relies on (`src/mal_updater/hidive_auth.py`, `hidive_snapshot.py`, `providers/hidive.py`). The code is authoritative; these notes explain why it is shaped this way. Facts were probed read-only with the app's own session (last checked 2026-11-18).

## Platform and auth

- HIDIVE runs on IMG/DICE frontoffice infrastructure. API base `https://dce-frontoffice.imggaming.com/api/v2` (newer `/api/v3`, `/api/v4` paths on the same host), realm `dce.hidive`, headers `app: dice`, `Realm`, `x-app-var`, and the public frontend `x-api-key`.
- Login: `POST /login` with JSON `id` + `secret` returns `authorisationToken` and `refreshToken`.
- Refresh: `POST /token/refresh` with `Authorization: Bearer <authorisationToken>` and JSON `refreshToken`. It is accepted only within 90 seconds of access-token expiry (earlier calls answer HTTP 429 with the remaining seconds). An expired refresh token answers HTTP 404, so the provider logs in again with the stored credentials once.
- Account identity: `GET /user/profile`.

## Account surfaces

| Surface | Endpoint and paging | Notes |
| --- | --- | --- |
| Watch history | `GET /customer/history/vod`, `rpp` (max 25) + 1-based `p` | `watchedAt` is the play start time. `watchProgress` (seconds) is present only while an episode is unfinished and is cleared once it is finished, so an item without it is a finished episode. |
| Continue Watching | `GET /content/home`, `bpp=5` buckets per page, `rpp=25` items per bucket | `rpp` above 25 answers HTTP 400 and `size`/`page` are ignored, so one call returns the whole bucket up to 25 items; it is reported partial only beyond that. |
| Favourites | `GET /favourite/vods`, `size` + `page` | Normalized as watchlist entries with `list_id: favorites`, `list_kind: system`. |
| Custom watchlists | `GET /api/v3/user/watchlist` (collection), `GET /api/v4/user/watchlist/{id}` (detail), `rpp` + `lastSeen` cursor | Non-advancing pages or a missing cursor stop the traversal and mark it incomplete. |
| Season catalog | `GET /api/v4/season/{id}`, `rpp` + `lastSeen` | Up to 10 seasons per run fill the provider season catalog; `publishedDate` is HIDIVE's own release date, never earlier than the original broadcast. |
| Title search | Public Algolia index, `type:VOD_SERIES` only | Used for specific recommendation/mapping candidates, linked as `https://www.hidive.com/series/{id}`; episode/video hits are ignored. |

Obvious REST guesses such as `/watchlist`, `/playlist`, `/customer/watchlists` and `/continue-watching/vod` answer 404.

## Hot and full refresh

Hot (hourly) runs page history only down to the stored sync boundary; a run that cannot reach the boundary within 20 history pages escalates to a full refresh in the same run. There is no periodic unattended full refresh and no chunk resume (the Crunchyroll page flags are rejected). Only a full traversal with consistent watchlist evidence may deactivate watchlist memberships that disappeared.

## Health classification boundary

Health treats each HIDIVE surface's completeness separately rather than turning every known endpoint limit into a failure. A hot snapshot is info-level and automation-safe only when its persisted diagnostics prove that the boundary belongs to the current account, page 1 intersects the expected history marker, and the retained watchlist generation is authoritative. In that state the known history limitation and any Continue Watching bucket larger than 25 items stay visible, while a redundant generic `snapshot_partial` diagnostic is suppressed.

A bootstrap/manual run that cannot establish the boundary, an account mismatch, a missing boundary intersection, incomplete watchlist traversal, or any pagination/cursor regression stays warning/unhealthy and fail-closed. Older snapshots without per-surface authority diagnostics are classified conservatively.
