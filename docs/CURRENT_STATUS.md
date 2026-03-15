# Current Status

## Working today

- Python worker/application exists
- versioned normalized snapshot contract exists
- SQLite bootstrap + initial schema exist
- local config/secrets/data directory conventions exist
- MAL OAuth flow is working end-to-end:
  - auth URL generation
  - callback listener
  - LAN callback reception on the Orin
  - token exchange
  - token persistence
  - `/users/@me` verification
- Python-side Crunchyroll credential bootstrap exists:
  - reads local `secrets/crunchyroll_username.txt` and `secrets/crunchyroll_password.txt`
  - stages `state/crunchyroll/<profile>/refresh_token.txt`
  - stages matching `device_id.txt`
  - updates `session.json`
  - exposes `crunchyroll-auth-login` in the CLI
- Python-side live Crunchyroll snapshot fetching exists:
  - refreshes the staged Crunchyroll token through the Python transport
  - uses browser-TLS impersonation when optional `curl_cffi` is installed
  - fetches `GET /accounts/v1/me`, `GET /content/v2/{account_id}/watch-history`, and `GET /content/v2/discover/{account_id}/watchlist`
  - normalizes the live responses into the existing `1.0` snapshot contract
  - exposes `crunchyroll-fetch-snapshot` in the CLI, with optional direct ingestion
  - now treats Crunchyroll `401` responses as auth-state failures instead of terminal fetch failures: one fresh credential rebootstrap is attempted from the staged local secrets, then the snapshot fetch is retried once and stops there
  - now persists an incremental `sync_boundary.json` checkpoint under `state/crunchyroll/<profile>/` after successful fetches and uses it on later runs to stop history/watchlist paging once already-seen overlap is reached; `--full-refresh` bypasses that checkpoint deliberately
- the Python fetch path has produced real live Crunchyroll data on this host
- a live snapshot was validated and ingested into SQLite with:
  - `series_count=245`
  - `progress_count=4311`
  - `watchlist_count=197`
- the Crunchyroll watchlist path was corrected to use the provider's actual pagination shape (`n` + `start`) instead of assuming page/page_size semantics; the prior `watchlist_count=10` was incomplete

## Not implemented yet

- broad unattended/full-fidelity MAL sync beyond the first narrow exact-approved cadence
- recommendation engine
- OpenClaw skill wrapper for the integration

## Newly added downstream progress

- MAL-side search + candidate scoring exists on top of the ingested Crunchyroll SQLite dataset
- the Python CLI exposes:
  - `map-series`
  - `review-mappings`
  - `list-mappings`
  - `approve-mapping`
  - `dry-run-sync`
  - `list-review-queue`
  - `apply-sync`
- `map-series` reports conservative mapping confidence (`exact`, `strong`, `ambiguous`, `weak`, `no_candidates`) instead of silently persisting guesses
- mapping now leans on deeper Crunchyroll evidence when available: explicit season-title/season-number cues, generic `Season N` / `Part N` / `Nth Cour` / `Final Season` query expansion, installment-hint matching (`Season 2`, `Second Season`, roman numerals, parts, cours, shared split indexes, `Final Season`), and episode-count evidence can break ties between similarly named seasons/sequels
- exact-title logic is stricter than the similarity scorer on purpose: it still strips dub noise, but it no longer collapses installment-bearing titles like `Part 1` vs `Part 2` into the same "exact" match
- provider season metadata is now treated as fallible: when Crunchyroll's `season_number` conflicts with an explicit season number inside `season_title`, the title cue wins for matching and the conflict is surfaced in rationale instead of silently trusting the provider integer
- movie candidates are still penalized by default, but that penalty is waived when the provider season title itself is an exact movie title; this keeps collection shells like `Dragon Ball Movies` from hiding a clearly named movie match
- `review-mappings` provides the first durable operator workflow: preserved approved mappings stay fixed, truly exact/unique season-consistent matches are auto-approved into durable `auto_exact` mappings, explicit installment conflicts block auto-approval, strong-but-not-exact suggestions are surfaced for explicit approval, weaker cases remain review-only, unresolved items can be persisted into `review_queue`, and per-series MAL search timeouts now degrade into local review residue instead of aborting the full scan
- `approve-mapping` persists a user-approved Crunchyroll -> MAL mapping into `mal_series_mapping`
- `dry-run-sync` prefers approved persisted mappings before falling back to live MAL search, can auto-promote the same safe exact matches into durable `auto_exact` approvals, `--approved-mappings-only` gives the safe gate for execution, and unresolved review/skip results can be persisted into `review_queue`
- `list-review-queue` exposes the durable backlog from SQLite for operator follow-up
- `apply-sync` is the first guarded live-write path:
  - only consumes durably approved mappings (`user_approved` or safe `auto_exact`)
  - revalidates live MAL state immediately before acting
  - only applies proposals that remain forward-safe
  - never decreases MAL watched-episode counts
  - never downgrades a `completed` MAL entry
  - now uses explicit missing-data-only field rules:
    - `status` is only filled when MAL has no status yet, except that an existing MAL `plan_to_watch` may be upgraded when Crunchyroll proves completed episode progress
    - progress is only filled when the proposal is backed by completed-episode evidence; `watching + 0 episodes` proposals are suppressed entirely
    - `score` is treated as meaningful whenever it is non-zero and is never overwritten by Crunchyroll today
    - `start_date` is preserved because the current Crunchyroll snapshot does not prove the true first-watch date
    - `finish_date` may be filled only when Crunchyroll safely implies completion and MAL does not already have one
  - only sends the fields it intends to change, so meaningful existing MAL metadata is left alone
- candidate scoring now also suppresses several noisy-but-explainable false-near-ties: sequel/spin-off entries with extra installment hints are penalized when the Crunchyroll title has no such hint, obvious auxiliary titles (`PV`, `extras`, `picture drama`, etc.) are penalized unless the provider title explicitly asks for them, single-episode `special`/`OVA` candidates are penalized harder when Crunchyroll clearly looks like a normal multi-episode mainline series, and `max_episode_number > MAL num_episodes` is no longer treated as a full contradiction when explicit later-season installment hints match and the Crunchyroll completed-episode count still fits inside the candidate cleanly (`aggregated_episode_numbering_suspected`)
- live Crunchyroll evidence is now feeding the completion policy directly:
  - strict ratio completion defaults to `0.95`
  - episodes with `<= 120s` remaining count as watched to cover the dominant credits-skip pattern seen in the dataset
  - episodes in the `0.85-0.95` band also count when a later episode in the same series was watched afterwards
  - progress is deduplicated by `episode_number` when available so alternate dub/sub variants do not inflate MAL watched counts

## Current Crunchyroll state

The practical live Crunchyroll path has worked on this machine, but is not fully stable yet.

What was verified locally:

- Crunchyroll username/password secrets are staged locally
- Python credential login can mint a real refresh token when using browser-like TLS impersonation (`curl_cffi`)
- the minted refresh token also refreshes successfully through the same Python impersonated transport
- the Python fetch path can retrieve real live Crunchyroll data on this host and normalize it honestly
- validated live snapshots ingest cleanly into SQLite
- the first real guarded exact-approved MAL apply run was executed against the latest successfully ingested live Crunchyroll dataset on 2026-03-15:
  - 179 exact-approved mappings considered
  - 126 MAL entries updated successfully
  - applied status mix: 78 `completed`, 36 `watching`, 12 `plan_to_watch`
  - 46 entries were skipped by the forward-safe rules
  - 7 entries remained unapplied due to recoverable live API issues (6 MAL detail timeouts, 1 MAL update redirect/error)
- latest live mapping audit over the current 245-series Crunchyroll dataset now lands at:
  - `83` preserved already-approved mappings
  - `85` fresh safe `auto_exact` mappings
  - `13` `ready_for_approval` suggestions
  - `64` unresolved review items
- that means `168 / 245` series are now durably auto-approvable/preserved without human intervention, versus `156 / 245` before the latest rule pass
- the remaining review residue is now concentrated in a few honest buckets: same-franchise low-margin ties, aggregated-episode-count conflicts across multi-cour/combined Crunchyroll entries, weak MAL search/alt-title gaps, and movie/collection shells where the title is clear but provider episode evidence still spans more than one MAL entry

Current blocker note: a fresh live Crunchyroll fetch is presently failing reproducibly at `watch-history` with HTTP 401 even after a fresh credential-based re-login. The new incremental boundary should reduce how often later pages are needed on repeated successful runs, but it does **not** eliminate the need for `watch-history` to survive long enough to return at least the overlap page. So the latest successful exact-approved MAL apply still had to run from the most recent already-ingested real live snapshot (`sync_runs.id=5`, 245 series / 4311 progress / 197 watchlist) rather than from a brand-new fetch.

That means the remaining work is now split between: (1) re-stabilizing the fresh Crunchyroll fetch path, and (2) continuing downstream sync-policy hardening, review UX, and recommendation work.

## Next practical milestone

Keep reducing the genuinely ambiguous mapping residue on top of the now-live local Crunchyroll dataset:
- richer audit/debug surfacing for why an episode counted as completed
- better review resolution UX for the remaining hard buckets
- targeted handling for subtitle/arc-title variants and franchise/movie collection naming that still need human review when the provider title is not explicit enough
- continue tightening same-franchise low-margin ties with explainable canonical-entry cues (today: auxiliary/special penalties are stronger when Crunchyroll clearly looks like the main multi-episode series)
- continue decomposing combined Crunchyroll entries more honestly for split-cour / multi-season / movie-shell cases (today: aggregate numbering is softened when explicit installment cues align and only the raw numbering looks inflated)
- investigate episode-title matching only if a trustworthy episode-metadata source can be used without turning the mapper into an opaque scraper; the current official MAL API surface does not expose episode-title data directly enough to justify pretending this is solved

- the first recurring local automation path now exists:
  - wrapper: `scripts/run_exact_approved_sync_cycle.sh`
  - scope: fetch + ingest + `apply-sync --limit 0 --exact-approved-only --execute`
  - overlap guard: `flock` lock file under `state/locks/`
  - logs: `state/logs/`
  - user-level systemd units checked in under `ops/systemd-user/` for a persistent jittered ~8-hour average cadence (effective spread: roughly 6-10 hours)
- the live Crunchyroll fetch path now spaces individual Crunchyroll requests by `request_spacing_seconds ± request_spacing_jitter_seconds` (default `22.5 ± 7.5`, i.e. randomized 15-30 seconds)
