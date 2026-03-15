# Decisions

## 2026-03-14 - Core integration direction

### Decision
Build MAL-Updater as a local Orin-hosted integration using:
- Python application/worker
- SQLite state database
- official MAL OAuth + REST API
- Python-side Crunchyroll auth + live fetches

### Why
- the working Crunchyroll path on this host is Python-side
- Python is better for orchestration, mapping logic, sync policy, and future recommendation work
- SQLite is sufficient and simple for local state
- keeping the implementation in one language leaves the repo smaller and easier to maintain

## 2026-03-14 - Sync direction

### Decision
One-way sync first: Crunchyroll -> MyAnimeList.

### Why
- Crunchyroll is the behavioral source of truth for watched progress
- MAL should be updated conservatively as the public-facing tracking layer
- two-way reconciliation adds unnecessary risk early

## 2026-03-14 - Sync policy

### Decision
This is a **missing-data-first** system.

### Rules
- Do not decrease MAL progress automatically.
- Do not overwrite meaningful existing MAL data automatically.
- Treat MAL `status` as missing only when absent/null.
- Treat MAL watched-progress as missing only when list status is absent; `plan_to_watch` + `0` is meaningful and should be preserved.
- Exception: if Crunchyroll proves completed episode progress (`> 0` watched episodes), a MAL `plan_to_watch` entry may be upgraded forward to `watching` or `completed`.
- Suppress `watching` proposals with `0` watched episodes entirely; partial playback without at least one completed episode is not honest enough to auto-write.
- Treat MAL `score` as missing only when null/absent/`0`.
- Treat MAL `start_date` / `finish_date` as missing only when null/empty.
- Only fill dates when the source evidence is trustworthy enough; currently that means `finish_date` may be filled from Crunchyroll `last_watched_at` only when Crunchyroll-derived status is `completed`.
- Do not auto-resolve ambiguous mappings.
- Queue conflicts for review.
- Dry-run before live writes.

## 2026-03-14 - Completion semantics

### Decision
Treat Crunchyroll episodes as watched only when the evidence is strong enough to explainable justify it:
- `completion_ratio >= 0.95`, or
- known remaining playback time is `<= 120` seconds, or
- the episode is at least `0.85` complete and a later episode in the same Crunchyroll series was watched afterwards.

### Why
The real local dataset showed that a blind `0.90` ratio threshold was too hand-wavy:
- many credit-skip cases cluster around **80-120 seconds remaining** rather than a single stable ratio
- **725 / 775** episodes in the `0.85-0.95` band were followed by a later watched episode in the same series
- **555 / 775** episodes in that band had `<= 120s` remaining
- only **20 / 775** episodes in that band had neither follow-on evidence nor the short remaining-time signature, so those should stay incomplete by default

### Working defaults
- strict completion ratio: `0.95`
- credits-skip remaining-time window: `120` seconds
- follow-on completion floor: `0.85`

## 2026-03-14 - Recommendation priorities

### Highest priority alerts
1. New season released for an anime the user has completed.
2. New dubbed episode released for an in-progress anime the user is currently following.

### Hard recommendation filter
- Do not recommend anime or new episodes that lack English dubs.

## 2026-03-14 - Repo posture

### Decision
Keep `MAL-Updater` private for now.

### Why
Early auth/integration work is exactly where accidental exposure risk is highest. Public release can be considered later after a deliberate sanitization pass.

## 2026-03-14 - Project memory habit

### Decision
Use `docs/` as project-specific durable memory.

### Why
OpenClaw memory is useful, but project-specific knowledge should live with the project repo.

## 2026-03-14 - Crunchyroll implementation choice

### Decision
Use the Python-side impersonated transport as the primary Crunchyroll auth and live fetch path.

### Why
- it is the path that produced real live account/history/watchlist data on this host
- it reuses the already-proven `curl_cffi` browser-TLS impersonation workaround when needed
- it gets real data into the local pipeline now instead of blocking on alternative transport ideas
- it keeps the repo architecture coherent and smaller
