# MAL-Updater

Local Crunchyroll → MyAnimeList sync and recommendation groundwork for OpenClaw on the Orin.

## Current status

This repo is now a **Python-only** implementation.

What it does today:
- real MAL OAuth and MAL API reads/writes
- real Crunchyroll auth bootstrap from local username/password secrets
- real live Crunchyroll snapshot fetches through the Python transport
- strict snapshot validation
- SQLite ingestion of Crunchyroll watch history/watchlist data
- conservative MAL mapping review workflows
- guarded dry-run sync planning
- a first live MAL executor that only applies approved, forward-safe updates
- a first local recommendation pass for dubbed episode and later-season alerts (kept deliberately conservative: bare `Part N` wording only counts as sequel evidence when the same title also carries explicit season-style wording like `Season 2`, `Second Season`, or `Final Season Part 2`; episode alerts now require a contiguous tail gap, treat only roughly the last 3 weeks of watch activity as truly `fresh`, use basic recency-aware ranking, and split older tail gaps into `resume_backlog` instead of pretending they are fresh releases)
- a local MAL metadata/relation cache for mapped anime, plus `recommend-refresh-metadata`, so continuation recommendations can progressively move beyond title-only heuristics
- a first graph-backed discovery lane from cached MAL recommendation edges, scored by convergence across watched/mapped seed titles

What it does **not** do yet:
- complete unattended end-to-end sync behavior
- auto-resolve ambiguous mappings
- perform broad or reckless MAL mutations
- ship a richer taste-model / metadata-heavy recommendation engine yet

That line is intentional. The working path is real, and the remaining gaps are not being faked.

## Repository layout

- `src/mal_updater/` — Python application package
- `migrations/` — SQLite schema migrations
- `docs/JSON_CONTRACT.md` — normalized Crunchyroll snapshot contract
- `docs/MAL_OAUTH.md` — MAL OAuth flow, callback model, and secret conventions
- `docs/PYTHON_WORKER.md` — worker/config/bootstrap notes
- `docs/DECISIONS.md` — durable architectural and policy decisions
- `docs/CURRENT_STATUS.md` — current implemented state vs missing pieces
- `docs/OPERATIONS.md` — local operational commands and expectations
- `docs/contracts/` — JSON schema for snapshot payloads
- `config/` — non-secret local config examples
- `secrets/` — local-only secret material (ignored by git)
- `examples/` — environment override examples

## Local directory conventions

Safe defaults are relative to the repo root:

- `config/` — committed examples and optional local non-secret config
- `secrets/` — credentials/tokens only, gitignored
- `data/` — SQLite DB and durable application data, gitignored
- `state/` — transient run state, checkpoints, gitignored
- `cache/` — fetch/cache scratch space, gitignored

Optional environment overrides are documented in `examples/env.example`.

## Python application

### Requirements

- Python 3.10+

### Commands

Run from the repo root:

```bash
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli health-check --review-issue-type mapping_review --review-worklist-limit 5
PYTHONPATH=src python3 -m mal_updater.cli health-check --strict
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --full-refresh
./scripts/run_exact_approved_sync_cycle.sh
PYTHONPATH=src python3 -m mal_updater.cli validate-snapshot path/to/snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli ingest-snapshot path/to/snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli map-series --limit 20 --mapping-limit 5
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --approved-only
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-123 16498 --confidence 0.995 --notes "manual approval"
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-456 5114 --exact --confidence 1.0 --notes "manual exact approval"
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --summary
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --provider-series-id G5PHNM4WD
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --title-cluster "Example Show"
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --decision needs_manual_match
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --reason same_franchise_tie
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --reason-family same_franchise
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --fix-strategy "needs_manual_match | ambiguous_candidates | same_franchise_tie"
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --fix-strategy-family "needs_manual_match | ambiguous_candidates | same_franchise"
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --cluster-strategy "example show || needs_manual_match | ambiguous_candidates | same_franchise_tie"
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --cluster-strategy-family "example show || needs_manual_match | ambiguous_candidates | same_franchise"
PYTHONPATH=src python3 -m mal_updater.cli review-queue-next --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-next --issue-type mapping_review --reason same_franchise_tie --bucket title-cluster
PYTHONPATH=src python3 -m mal_updater.cli review-queue-worklist --issue-type mapping_review --limit 5
PYTHONPATH=src python3 -m mal_updater.cli review-queue-apply-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 10
PYTHONPATH=src python3 -m mal_updater.cli review-queue-refresh-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 10
PYTHONPATH=src python3 -m mal_updater.cli refresh-mapping-review-queue --all-open
PYTHONPATH=src python3 -m mal_updater.cli refresh-mapping-review-queue --reason-family exact_normalized_title
PYTHONPATH=src python3 -m mal_updater.cli refresh-mapping-review-queue --cluster-strategy-family "example show || needs_manual_match | ambiguous_candidates | same_franchise"
PYTHONPATH=src python3 -m mal_updater.cli resolve-review-queue --issue-type mapping_review --reason same_franchise_tie --limit 10
PYTHONPATH=src python3 -m mal_updater.cli reopen-review-queue --issue-type mapping_review --title-cluster "Example Show" --limit 10
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20 --execute
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat
```

Or install editable and use the console script:

```bash
pip install -e .
# optional but currently practical for Crunchyroll auth on this host:
pip install -e '.[crunchyroll]'
mal-updater status
mal-updater init
mal-updater mal-auth-url
```

### Tests

Run the current stdlib smoke/integration tests with:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

This repo currently relies on the built-in `unittest` runner for local verification; `pytest` is not required or assumed to be installed on the host.

### Current behavior

- `status` prints resolved paths/config plus MAL and Crunchyroll secret/state presence
- `health-check` emits a JSON operational summary for auth material, latest ingest freshness, provider row counts, mapping counts, and open review backlog; it warns when the newest completed ingest snapshot is older than the chosen `--stale-hours` threshold, when the latest incremental ingest only refreshed a subset of the cached provider rows (and therefore needs a `--full-refresh` repair cycle before the local cache can be considered globally fresh), and now also when approved mapping coverage falls below `--mapping-coverage-threshold` (default `0.8`) so a false-green `healthy=true` state does not hide large unmapped provider residue. Open `mapping_review` rows now carry a `mapper_revision` stamp too, and `health-check` will flag stale/unknown-revision mapping backlog before you waste time triaging queue entries produced by older heuristics. When backlog exists it also includes a ranked `recommended_next` bucket, a short `recommended_worklist` of copy/pasteable queue triage commands, a matching `recommended_apply_worklist` batch action, and a matching `recommended_refresh_worklist` batch refresh command so maintenance output can point directly at the bulk resolve/reopen and stale-residue refresh helpers instead of only individual drilldowns; low-coverage maintenance recommendations now use a bounded `review-mappings --persist-review-queue` shape so the suggested pass leaves durable residue behind instead of only printing ephemeral review output; maintenance recommendations also carry `automation_safe` / `requires_auth_interaction` metadata plus preselected `recommended_command` / `recommended_automation_command` entries so wrappers and operators can distinguish the top overall repair from the first safely automatable one, and repo-owned user systemd automation now checks five failure modes instead of just file presence: missing unit files, outdated installed copies, installed-but-not-enabled timers, miswired timer symlinks that point at the wrong unit file, and enabled timers that are present on disk but not actually active in the user systemd runtime. `--format summary` now also surfaces repo-owned automation install/update/enable hints directly (`automation_all_units_installed`, `automation_all_units_current`, `automation_all_timers_enabled`, `automation_missing_units`, `automation_outdated_units`, `automation_disabled_timers`, `automation_inactive_timers`, `automation_timer_runtime`, `automation_install_command`) so automation drift, runtime inactivity, and next-trigger timing are actionable without switching back to full JSON; pass `--strict` when automation should still print the JSON payload but exit with code `2` whenever warnings are present
- `scripts/run_health_check_cycle.sh` is the first repo-owned maintenance wrapper around that health summary: it logs each run under `state/logs/`, archives the JSON output under `state/health/`, keeps `state/health/latest-health-check.json` updated, can optionally return non-zero on warnings when `MAL_UPDATER_HEALTH_STRICT=1`, now logs whether a safe maintenance candidate existed but was skipped by the reason-code allowlist when auto-remediation is enabled, and can execute direct repo-owned shell recommendations such as `scripts/install_user_systemd_units.sh` instead of assuming every safe repair is a Python CLI subcommand
- `config/settings.toml` can define path layout, MAL endpoint settings (including polite MAL request pacing), Crunchyroll locale, and secret file locations
- `validate-snapshot` checks normalized Crunchyroll JSON shape and cross-reference sanity before ingestion work touches SQLite
- `recommend` now emits grouped category sections by default (`continue_next`, `fresh_dubbed_episodes`, `discovery_candidates`, `resume_backlog`); pass `--flat` for the legacy single-list JSON shape
- `recommend-refresh-metadata` can optionally hydrate top discovery-target metadata too (`--include-discovery-targets`, `--discovery-target-limit`) so recommendation ranking/suppression can avoid resurfacing titles that are already on the MAL list; the cached metadata now also carries MAL genres, studios, and source-material type so discovery candidates can get a small overlap bias against the watched seed set, limited discovery-target hydration now ranks targets by aggregate multi-seed support instead of a single outlier recommendation edge, and discovery output suppresses targets whose MAL title/aliases already match something in the current Crunchyroll catalog or whose MAL relation graph shows they are a direct same-franchise follow-on from any watched seed title
- `init` creates local directories and applies SQLite migrations
- `mal-auth-url` generates a real PKCE pair plus MAL authorization URL
- `mal-auth-login` starts a local callback listener bound to the configured host, exchanges the returned code for tokens, persists them under `secrets/`, and verifies the token with `GET /users/@me`
- `mal-refresh` refreshes a persisted MAL token pair and writes updated token files back to `secrets/`
- `mal-whoami` exercises the current access token against MAL `GET /users/@me`
- `crunchyroll-auth-login` uses local Crunchyroll username/password secrets to fetch a real refresh token + device id and stage them into `state/crunchyroll/<profile>/`; if optional `curl_cffi` support is installed, it uses browser-TLS impersonation to get through Crunchyroll's Cloudflare layer
- `crunchyroll-fetch-snapshot` is the live Crunchyroll path: it refreshes auth through the Python impersonated transport, fetches account/history/watchlist data, and if a Crunchyroll `401` happens mid-run it performs one bounded credential rebootstrap, retries the failed request in place, and continues the same fetch cycle before normalizing the result into the JSON contract; it can write a snapshot file and/or ingest it directly
- successful fetches now persist a lightweight `state/crunchyroll/<profile>/sync_boundary.json` checkpoint containing leading watch-history/watchlist markers; later runs use that checkpoint to stop paging once already-seen overlap is reached, while `--full-refresh` intentionally bypasses the checkpoint and walks the full currently reachable pages
- the live Crunchyroll fetch path now intentionally spaces individual Crunchyroll HTTP requests by `crunchyroll.request_spacing_seconds` with `crunchyroll.request_spacing_jitter_seconds` (default: `22.5 ± 7.5s`, i.e. randomized 15-30 seconds between requests) so the unattended cadence stays conservative
- a real live run on this host succeeds through the Python path and ingests into SQLite (`series_count=245`, `progress_count=4311`, `watchlist_count=197` on the latest local snapshot)
- `ingest-snapshot` validates then upserts normalized snapshot data into SQLite, recording a summary row in `sync_runs`
- `map-series` searches MAL for conservative candidate matches for recently seen Crunchyroll series and reports confidence / ambiguity instead of silently persisting guesses; it now expands generic Crunchyroll labels like `Season 2`, `Part 2`, `2nd Cour`, or `Final Season` into `Title ...` queries and scores explicit installment hints (season numbers, ordinal seasons, roman numerals, parts, cours, split indexes, `Final Season`)
- matcher scoring now stays conservative around the messy residue buckets: strict exact-title checks no longer collapse `Part 1` and `Part 2` into the same "exact" title, part-vs-cour titles can still align through a shared split index, exact movie titles are allowed even when Crunchyroll grouped them under a movie collection shell, but exact one-shot movie / prologue / TV-special sidecars are now pushed back down again when the provider evidence clearly describes a multi-episode series, explicit season-title cues can override obviously noisy provider `season_number` metadata, sequel/spin-off entries with extra installment hints are penalized when Crunchyroll did not express that hint, obvious auxiliary titles like `PV` / `extras` / `picture drama` are penalized unless the provider title explicitly points at them, single-episode `special`/`OVA` residue is penalized harder when Crunchyroll clearly looks like a normal multi-episode series, exact-title base-series TV matches can now still auto-promote when the only near runner-up is explainably weaker one-off OVA/auxiliary residue, non-exact same-franchise TV sequel suffix variants (`S`, `T`, trailing sequel numbers, etc.) also no longer block that exact base-series auto-promotion when they are just extension noise rather than a true competing exact match, risky OVA/ONA/special-first candidates still do not get to bypass that safeguard just because they won by raw score margin, near-identical franchise extensions (`: Another Story`, `TV Specials`, sequel-suffix variants like trailing `R`) are now also penalized when the provider only names the base series, alpha-digit spacing differences like `PERSONA5` vs `Persona 5` now normalize cleanly instead of creating false near-ties, relation-assisted expansion now also chases suffix/auxiliary residue candidates like `Shuffle! Memories` even when Crunchyroll only names the base show, while deliberately skipping plain `Season 1` TV cases so it does not promote side stories just because a base-installment label appeared in the provider title, explicitly later-season Crunchyroll titles now penalize base-installment candidates that do not carry matching later-season evidence, arc/subtitle-only provider embellishments like `Title: Egghead Arc` can now boost an otherwise exact base-title candidate into review-ready territory without loosening auto-approval, pipe/em-dash arc subtitle variants now fall back to the same base-title search path instead of missing the franchise search entirely, split-specific installment matches can now auto-promote over otherwise tied broader same-season TV candidates when only the top result carries the provider’s part/cour-style evidence, and inflated Crunchyroll episode numbering is softened into explainable `aggregated_episode_numbering_suspected` evidence when the completed-episode count still cleanly fits an explicitly matched later season. Combined-entry residue can now also flag likely small two- or three-entry MAL bundles via `multi_entry_bundle_suspected=...` so review rows cluster more honestly during triage, including exact-title base rows, explicit later-season winners that obviously spill into the next season-sized entry, lower-scoring same-franchise companions that still share the same title family plus installment evidence (for example, generic provider rows like `Space Dandy` that actually span part of `2nd Season`), and mixed bundle candidates where higher-scoring one-shot movie/prologue/recap sidecar noise should not displace the real multi-season TV companions. Both `map-series` / `review-mappings` style output now preserve the strongest suspected companion plus the full suspected companion set so operators can see the likely bundle shape directly instead of reverse-engineering it from runner-ups
- `review-mappings` turns those candidates into an operator-facing review list, preserves already approved mappings, auto-approves truly exact/unique season-consistent matches as `auto_exact`, blocks auto-approval when installment hints conflict, can replace open `mapping_review` rows in `review_queue`, and now survives per-series MAL search timeouts instead of aborting the entire audit
- `list-mappings` shows the durable Crunchyroll -> MAL mappings already stored in SQLite
- `approve-mapping` persists an explicit user-approved mapping into `mal_series_mapping`
- MAL API traffic is now paced politely by `mal.request_spacing_seconds ± mal.request_spacing_jitter_seconds` (default `1.0 ± 0.2s`) so repeated detail/search/update calls stay explainable and bounded instead of hammering the API
- MAL reads/writes now do one bounded retry on timeout before surfacing a normal per-title error/review residue, so a single transient detail timeout no longer has to stall an entire sync cycle
- `dry-run-sync` prefers approved persisted mappings first, can optionally require approved mappings only, auto-promotes truly exact/unique season-consistent matches into durable `auto_exact` approvals, only suggests forward-safe updates, applies explicit missing-data-only merge rules, and can replace open `sync_review` rows in `review_queue`
- `list-review-queue` exposes the durable unresolved review backlog stored in SQLite, and `--summary` gives a compact decision/severity/top-reason triage view with a few example rows per decision/reason; when queue payloads do not carry a title, those examples now fall back to the stored provider-series title/season title so the triage output stays operator-friendly instead of anonymous, the summary also surfaces repeated title/franchise clusters, repeated decision+reason fix-strategy patterns, and repeated combined franchise+fix-strategy buckets so the remaining residue is easier to batch-triage, `--provider-series-id` can drill into one exact Crunchyroll series row when health-check or refresh output already identified the specific backlog item to inspect, `--title-cluster` / `--decision` / `--reason` / `--fix-strategy` / `--cluster-strategy` let the operator drill directly into one of those buckets or remediation slices without dumping the whole queue first, and the summary payload now includes exact `drilldown_args` plus matching `resolve_args` snippets that preserve any already-active summary filters (including non-default `--status` for drilldowns) so follow-up inspection and backlog cleanup both stay inside the slice you were inspecting. Family-level residue is now directly operable too: `--reason-family`, `--fix-strategy-family`, and `--cluster-strategy-family` can slice the queue by normalized family buckets, and the corresponding summary sections now emit ready-to-run drilldown / resolve / reopen commands instead of only aggregate counts
- `review-queue-next` turns that summary into a one-shot operator helper by selecting the highest-signal remaining bucket (now auto-preferring broader family-level combined franchise+fix-strategy residue before falling back to narrower exact buckets), emitting the exact follow-up command string to run next, honoring/preserving optional existing queue filters plus non-default `--status` scope so it can keep narrowing a specific residue slice instead of always jumping back to the global queue, and now also surfacing a bounded `refresh-mapping-review-queue` command for mapping-review buckets when persisted residue should be re-evaluated under newer mapper heuristics instead of just resolved/reopened
- `refresh-mapping-review-queue` no longer needs raw `provider_series_id` lists for every maintenance pass: it can still refresh explicit ids or `--all-open`, but it now also accepts the same queue-slice filters used by triage (`--decision`, `--reason`, `--reason-family`, `--title-cluster`, `--fix-strategy`, `--fix-strategy-family`, `--cluster-strategy`, `--cluster-strategy-family`) so stale mapping residue can be re-run directly from the bucket you were inspecting
- `review-queue-worklist` extends that into a short ranked worklist of the next several drilldowns (default 5), preserving any active queue filters/status so batch review can start from a copy/pasteable stack instead of repeatedly re-running summary selection by hand; those ranked helpers now surface the broader family buckets first when they batch more residue than a narrower exact slice, and mapping-review worklist entries still carry matching targeted refresh commands alongside the usual drilldown and resolve/reopen actions
- `review-queue-apply-worklist` now closes the remaining batch-maintenance gap by taking the top ranked worklist buckets and resolving/reopening them in one shot with a bounded `--per-bucket-limit`, including the same family-first ranking and deduplicating overlapping rows so repeated fix-strategy/decision buckets do not double-touch the same residue
- `review-queue-refresh-worklist` is the matching batch-refresh helper for stale mapping residue: it refreshes the highest-signal ranked mapping-review buckets in one bounded pass, preserves any active family/exact scope filters, skips overlap-only picks that would not add new provider rows, and is intentionally limited to `mapping_review` rather than pretending sync-review rows can be recomputed through the mapper
- `resolve-review-queue` is the main single-slice queue-maintenance write helper: after triage, it can mark the next matching open residue rows as resolved using the same `--decision` / `--reason` / `--title-cluster` / `--fix-strategy` / `--cluster-strategy` filters, with a bounded `--limit` (or `0` for all) so the backlog can actually be cleared from the CLI instead of only inspected
- `reopen-review-queue` is the matching escape hatch when residue was cleared too aggressively or needs another pass: it reopens filtered resolved rows back into the active queue using the same scope filters and `--limit` behavior
- the first in-repo OpenClaw skill wrapper now exists under `skills/mal-updater/`; it packages cleanly and documents the repo-local operator flows for status, fetch/ingest, review triage, guarded apply, recommendations, and test verification while keeping the real sync logic in the Python CLI, and the same thin wrapper is now installed into the main OpenClaw workspace at `~/.openclaw/workspace/skills/mal-updater/` so direct OpenClaw sessions can use it without re-deriving the repo workflow
- repo-owned user systemd units now exist for both the exact-approved sync cadence and the standalone health-check cadence under `ops/systemd-user/`, `scripts/install_user_systemd_units.sh` now installs/updates them idempotently for the current user, that installer path now has direct regression coverage for dry-run/copy/preserve-local-env behavior and now reports which units were installed/updated/unchanged plus whether the optional health env file was installed/preserved/skipped, and the health-check service reads an optional `~/.config/mal-updater-health-check.env` EnvironmentFile so strict mode / allowed self-remediation can be configured without editing the installed unit
- `apply-sync` is the first guarded live executor: it revalidates live MAL state, only consumes durably approved mappings (`user_approved` or safe `auto_exact`), only submits forward-safe updates, and only fills MAL fields that are still genuinely missing
- `apply-sync --exact-approved-only --limit 0 --execute` is the unattended-safe executor gate for the first recurring cadence; today that means persisted approved mappings whose source is `auto_exact` or `user_exact`
- `sync` remains a reserved umbrella entrypoint and points at the explicit review/apply commands

## SQLite schema

Initial migration: `migrations/001_initial.sql`

Current tables:
- `provider_series`
- `provider_episode_progress`
- `provider_watchlist`
- `mal_series_mapping`
- `review_queue`
- `sync_runs`
- `schema_migrations`

This is enough to support read-only ingestion, durable mapping review, guarded sync planning, and the first safe MAL apply path.

## JSON contract

Current version: `1.0`

See:
- `docs/JSON_CONTRACT.md`
- `docs/AUTOMATION.md`
- `docs/contracts/crunchyroll_snapshot.schema.json`

Boundary rule:
- the Python Crunchyroll fetch path produces normalized provider snapshots
- the rest of the Python app validates, persists, maps, and decides MAL-side actions

## Security notes

- Do not commit real credentials
- Keep secrets under `secrets/`
- `config/settings.toml` is treated as local-only
- The repo is intended to remain private for now

## Planned architecture

- Python Crunchyroll auth + live fetch path
- Python orchestration worker
- SQLite state database
- MAL official OAuth/API client
- Safe dry-run sync before live writes
- Conservative live apply path for approved mappings
- Local recommendation and alert layer on top of the ingested state
