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
- Only auto-approve mappings when the top MAL candidate is an exact normalized-title match, clearly ahead of the runner-up, and there is no contradictory season/episode/installment evidence.
- Expand generic Crunchyroll season labels like `Season 2` / `Part 2` / `2nd Cour` / `Final Season` into `Title ...` search queries before giving up.
- Treat explicit installment cues (season numbers, ordinal seasons, roman numerals, parts, cours, split indexes, `Final Season`) as explainable matching evidence; matching cues can promote a result, conflicting cues must block auto-approval.
- Use stricter exact-title normalization than the similarity scorer so installment-bearing titles like `Part 1` and `Part 2` do not collapse into the same "exact" match.
- When Crunchyroll `season_number` metadata conflicts with an explicit season number inside `season_title`, prefer the human-readable title cue and surface the conflict in rationale instead of silently trusting the integer.
- Keep a default penalty on MAL movie candidates, but waive it when the provider season title itself is an exact movie title; this handles provider collection shells conservatively without making movies broadly preferred.
- Penalize single-episode `special`/`OVA` residue more strongly when Crunchyroll clearly looks like a normal multi-episode series and the provider title did not explicitly ask for auxiliary content.
- If Crunchyroll episode numbering looks aggregated across seasons, do not treat that raw max episode number as a hard contradiction when explicit installment hints line up and the completed-episode count still fits inside the candidate; surface that as explainable `aggregated_episode_numbering_suspected` evidence instead.
- Do not claim episode-title matching exists unless the metadata source is trustworthy enough to explain and maintain. The current official MAL API surface does not expose episode titles directly, so any future episode-title path must be an explicit, justified choice rather than confidence theater.
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

## 2026-03-14 / 2026-03-20 - Repo posture

### Decision
Treat `MAL-Updater` as a public repository and keep all tracked artifacts anonymized.

### Rules
- Do not commit real credentials, tokens, API keys, or account identifiers.
- Do not commit host-specific absolute paths, private workspace paths, or machine-local identifiers.
- Use obviously fake placeholders in examples and tests.
- Runtime-generated state may exist under external runtime directories, but it must not be tracked.
- If identifying residue is accidentally committed, history rewrite is an acceptable remediation.

### Why
Public-repo hygiene needs to be part of the project contract, not an afterthought. Future development should assume anything tracked in git may be published and mirrored.

### Related maintainer channel
Operational bugs, usability issues, and feature requests discovered by third-party users should be reported through the authoritative upstream issue tracker:
- <https://github.com/kklouzal/MAL-Updater/issues>

## 2026-03-14 - Project memory habit

### Decision
Use `references/` as project-specific durable memory.

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

## 2026-03-14 - Crunchyroll incremental boundary posture

### Decision
Treat repeated Crunchyroll fetches as incremental by default.

### Rules
- Persist a local `sync_boundary.json` checkpoint under the resolved runtime state tree (`.MAL-Updater/state/crunchyroll/<profile>/` by default) only after a successful snapshot fetch completes.
- Store only lightweight leading-page markers for watch-history and watchlist, not a second shadow database.
- On the next fetch, stop paging once a previously seen marker appears in the current page; keep the current page, but do not keep walking older pages.
- If the stored boundary belongs to a different Crunchyroll account, ignore it.
- Provide an explicit operator escape hatch (`crunchyroll-fetch-snapshot --full-refresh`) for full pagination when needed.
- Prefer explainable overlap-based stopping over more aggressive heuristics about dates/count deltas/order stability.

### Why
- the recurring durability problem is still fresh full-cycle Crunchyroll paging, especially `watch-history` returning `401` before the run finishes
- an overlap checkpoint directly reduces request count on repeated runs without pretending deleted/reordered remote history is solved
- keeping the boundary file small and local makes the behavior auditable and easy to reset

## 2026-03-20 - Daemon budget backoff posture

### Decision
When the daemon hits a provider budget critical threshold, persist a per-task cooldown window and stop re-checking that task every loop until enough request history ages out of the hourly window.

### Why
- repeated every-loop budget skips create noisy logs without making progress
- recovery should be based on the observed request window, not a hand-wavy fixed sleep
- surfacing `budget_backoff_until` in service state/status makes unattended behavior easier to reason about during debugging

## 2026-03-22 - Adaptive provider failure backoff posture

### Decision
When a daemon task tied to a provider fails, persist an adaptive failure-backoff window with the failure reason and consecutive-failure streak so the lane cools down before retrying instead of immediately thrashing.

### Why
- auth-fragile provider fetches can fail repeatedly for a while after a bad login/session state transition
- every-loop retries create noisy logs and extra pressure without improving recovery odds
- surfacing `failure_backoff_until`, `failure_backoff_reason`, and consecutive failures in service state/status makes unattended debugging clearer

## 2026-03-22 / 2026-03-23 - Provider full-refresh escalation posture

### Decision
Keep provider fetch lanes incremental by default, but let the unattended daemon both:
- persist a provider-specific full-refresh anchor and force a conservative `--full-refresh` sweep whenever `service.full_refresh_every_seconds` elapses (default 24 hours), and
- honor the latest health-check `refresh_full_snapshot` recommendation for a provider so partial-coverage warnings can trigger the next unattended fetch to self-upgrade into a full refresh immediately.

### Why
- incremental fetches are the right steady-state posture for fragile providers, but unattended sync quality still needs occasional canonical resweeps
- a persisted cadence anchor gives the daemon an explainable, low-complexity way to recover from stale provider state without requiring operators to remember ad-hoc maintenance commands
- wiring health-check maintenance recommendations back into daemon execution closes a real gap between diagnosis and remediation for partial-coverage residue
- surfacing fetch mode plus last successful full refresh in task state keeps the behavior auditable instead of hiding refresh policy in operator folklore

## 2026-03-24 - Generic source-provider budget default posture

### Decision
Keep MAL as its own explicit daemon budget lane, but let non-MAL source providers inherit shared source-provider hourly/backoff/auth-failure defaults unless a per-provider override is configured.

### Why
- the provider architecture should not silently treat every new source provider as if it were Crunchyroll
- shared source-provider defaults make new providers safer to onboard before their final tuned per-provider numbers are known
- explicit per-provider overrides still win, so operators can keep Crunchyroll/HIDIVE-specific tuning where it matters

## 2026-03-23 - Auth-style provider failure cooldown posture

### Decision
Keep adaptive provider failure backoff for all daemon task errors, but classify auth-style provider failures separately and allow a stronger provider-specific cooldown floor for that class. Persist the failure class and effective floor in service state/status.

### Why
- repeated auth/login/refresh failures are a different recovery shape than generic subprocess or network residue
- brittle provider auth should cool down longer before retrying so unattended loops stop re-poking broken sessions
- surfacing the failure class and floor keeps the daemon's retry posture explainable during debugging instead of hiding it in timing folklore

## 2026-03-22 - Same-title split-bundle suffix posture

### Decision
Allow exact-title split-bundle auto-resolution when the base candidate is an exact TV match and the bundle companion is a same-title TV suffix variant (for example a year-tagged entry like `Title (2009)`), **but only** when provider episode evidence fits the combined bundle length and there is no stronger non-bundle rival nearby.

### Why
- some real MAL franchises split one provider shell across multiple TV entries without advertising a plain `Season 2` / `Part 2` hint
- this residue is still explainable when the provider title is exact, the companion stays in the same normalized title family, and the episode count only makes sense as the combined bundle
- keeping the rule tied to same-title TV suffix companions preserves conservative behavior while removing a class of manual-review busywork

## 2026-03-20 - Supplemental mapping candidate posture

### Decision
Treat hard-coded supplemental MAL candidate IDs as conservative rescue inputs, not as enough evidence by themselves to auto-resolve exact-title overflow cases.

### Why
- supplemental IDs help recover titles that MAL search fails to surface at all
- overflow on top of a supplemental-only hit can still mean a multi-entry bundle or broader franchise residue
- keeping this path conservative preserves explainability and avoids overconfident auto-approval when the search surface was already weak
