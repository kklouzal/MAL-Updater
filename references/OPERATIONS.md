# Operations

Commands are written as `mal-updater <command>`. From a source checkout run `PYTHONPATH=src python3 -m mal_updater.cli <command>` in the repo root; in the container run `docker compose exec -u 10001:10001 mal-updater mal-updater <command>`. The full command list is in `references/cli-recipes.md`.

## Scheduler lanes and defaults

Service configuration is read at process start: restart the container (or `mal-updater.service`) after editing settings before trusting `service-status` as the effective-policy view. Inspect the loaded policy with `service-status` (`niceness_policy` in JSON, `niceness_*` / `cache_*` in summary). Budgets and request spacing are local safety controls, not claims about provider limits.

| Lane | Default cadence and bounds |
| --- | --- |
| Provider fetch (`sync_fetch_<provider>`) | Hourly hot/incremental fetch per credentialed provider. Crunchyroll full refresh weekly, capped at 10 history + 2 watchlist pages. HIDIVE has no periodic full refresh; a hot run that cannot reach its stored history boundary within 20 history pages (`rpp=25`) escalates to a full refresh in the same run. A due full refresh that is over budget falls back to an incremental fetch and stays due. |
| MAL token refresh (`mal_refresh`) | Checked hourly; renews only when the recorded access-token expiry is within 7 days or unknown (`mal-refresh --force` renews immediately). |
| MAL list refresh (`mal_list_refresh`) | Every 8 hours; 10 network attempts per run against a 14-request task-hour cap, `limit=1000` pages. The service passes `--complete`, so rows removed on MAL are pruned only after a traversal reaches its terminal page. |
| MAL apply (`sync_apply`) | **Disabled by default** (`service.task_execute_limits.sync_apply = 0`). With a positive opt-in, each run writes at most that many entries and applies only providers whose fetch lane succeeded since the previous apply (`apply-sync --provider ...`); with no fresh provider the run is skipped (`fresh_provider_fetch_required`). Per-item MAL failures make the run exit non-zero, which feeds failure backoff and the `scheduled_task_failures_repeated` health warning. |
| Recommendation metadata | Every 12 hours; 3 seed rows plus 5 discovery targets. |
| Public MAL userrecs harvest (`recommend_full_harvest`) | Hourly; 3 source titles, 10 network attempts per source per run including final revalidation, 120 +/-15-day refresh horizon; 30-request cold projection against a 40-request task-hour cap. |
| Provider eligibility | Hourly, one lane per credentialed provider; 5 candidates (uncovered first), 1 alias query each, 5 returned matches. |
| Recommendation snapshot and health | Hourly, DB/local only. |
| Housekeeping | See the table below. |

All MAL lanes also share the 120-request/hour global MAL budget, request pacing, and learned-history projection gates. Eligibility and userrecs lanes are bounded by both their task budget and the provider-global budget.

## Cache and evidence freshness

Normal maintenance skips fresh complete MAL/provider evidence. Durable positive evidence (MAL search/detail, provider detail, recommendation metadata, complete public userrecs, provider eligibility) uses a deterministic per-key 120-day +/-15-day refresh target. Refresh due is separate from usability: last-known-good data stays readable through a due or failed refresh, but malformed JSON, logic/identity mismatch, auth failure, or unverified evidence never becomes usable. Negative/no-match MAL search is a short 3-day claim; TTL 0 disables cache reuse. Only successful contradictory evidence or explicit semantic invalidation revokes a positive state, and stale evidence never authorizes MAL/provider writes or strict availability by itself.

Use `recommend-refresh-metadata --force-refresh` only for explicit repair of official MAL metadata rows; that API exposes only a top-10 `recommendations` set and must not overwrite a complete public userrecs harvest. A 365-day provider title-search hit is identity-only and never creates availability; availability comes from independent eligibility evidence, whose refresh records carry an `ok`/`failed` lifecycle with bounded exponential retry.

## Runtime layout and configuration

The runtime root is `/data` in the container and `.MAL-Updater/` in the workspace for native installs (layout in `ARCHITECTURE.md`; `status` or `bootstrap-audit` prints the resolved paths). Settings load from `MAL_UPDATER_SETTINGS_PATH` when set, otherwise `<runtime root>/config/settings.toml`; relative paths in it resolve against the settings file. All settings and environment overrides are listed in `references/settings.toml.example` and `references/env.example`. Only runtime state may contain operator secrets; everything tracked in git stays anonymized.

## First-line inspection

```bash
mal-updater status
mal-updater bootstrap-audit --summary
mal-updater service-status --format summary
mal-updater service-status --strict --format summary
mal-updater health-check --format summary
mal-updater runtime-retention-audit --format summary
```

`runtime-retention-audit` is diagnostic-only. It checks the runtime root for nested `.MAL-Updater` markers, repo/source overlap, symlink escapes, and missing managed paths, then reports bounded aggregate inventory for DB backups, health snapshots, logs/request events, tmp, cache, and artifacts. It never lists secret names or contents and never deletes anything. `--strict` fails only on layout errors; scan and warning thresholds are tunable with the `--max-*` and `--warn-*` options.

## Housekeeping and retention

| Surface | Cadence | Policy |
| --- | --- | --- |
| Recommendation score snapshots | Every scheduler pass | Logical deletion: 14-day horizon, newest 30 runs per kind protected, at most 10,000 rows per pass; never VACUUMs. Reported as `recommendation_snapshot_retention` in service state. |
| Recommendation decision ledger | Every scheduler pass | Whole runs older than 30 days, at most 25 runs per pass; SQL triggers reject deleting rows younger than 7 days, and the configured retention is floored there. |
| API request telemetry (`api-request-events.jsonl`) | Every scheduler pass | Atomic pruning of events older than 14 days; malformed telemetry blocks pruning and preserves the file. |
| SQLite compaction (`db_compaction`) | Checked weekly; successful runs at least 30 days apart | Details below. |
| Health artifacts | Retention daily | `latest-health-check.json` is never deleted. Timestamped history keeps 30 days and at least the newest 168 snapshots, deleting at most 1,000 files per pass; unsafe names, symlinks or filesystem errors block the pass. |
| Service log | Before every append | `service.log` rotates before an append would exceed 16 MiB; five generations are kept. |
| Database backups | Weekly audit | `db_compaction` deletes its own `mal-updater-pre-vacuum-<epoch>.tar.gz` archives older than 30 days (never the one it just wrote). Other backups are removed only by an explicit operator decision. |
| Other runtime families | Weekly audit | Read-only inventory; any archive/prune/delete is human-gated. |

Deleting rows does not shrink the SQLite file. `db_compaction` VACUUMs only when every gate passes: the last successful compaction is at least 30 days old; the freelist is at least 128 MiB and at least 10% of pages; the scheduler singleton and the shared DB writer lock are held; the DB volume has room for a rewrite plus margin; and a fresh backup under `<runtime root>/state/backups/` verifies by manifest and checksum. Free space is checked again after that backup and immediately before VACUUM. VACUUM runs in a child process with its temporary files on the database volume (not a small `/tmp` tmpfs); a failure is reported as a task error with failure backoff. VACUUM holds an exclusive lock, so expect a pause while it runs; stop non-repo SQLite writers first or disable it with `service.db_compaction_every_seconds = 0`. `service-status` shows cadence, last result, backup path, bytes reclaimed, skip/block reason, and next due.

## Provider auth and fetch

```bash
mal-updater provider-auth-login --provider crunchyroll
mal-updater crunchyroll-link
mal-updater provider-auth-login --provider hidive
mal-updater provider-fetch-snapshot --provider crunchyroll --ingest
mal-updater provider-fetch-snapshot --provider crunchyroll --full-refresh --ingest
mal-updater provider-fetch-snapshot --provider hidive --ingest
```

`provider-auth-login` logs in and writes local token/session state. `crunchyroll-link` is the password-free alternative: it prints a TV activation code to approve at crunchyroll.com/activate, and the linked session renews itself. `provider-fetch-snapshot` reads the provider and writes local snapshot/boundary state; `--ingest` also updates the local SQLite provider cache (never MAL). Crunchyroll accepts the page chunk flags `--max-history-pages`, `--max-watchlist-pages`, `--history-start-page` and `--watchlist-start`; HIDIVE rejects them. HIDIVE surfaces and health rules are in `references/hidive-provider-recon.md`.

Request spacing and retry caps live in `[mal]`, `[crunchyroll]` and `[hidive]`, separate from hourly budgets. Request starts are coordinated per provider across processes through `<runtime root>/state/provider-request-gates/`. Retries apply only to safe GET/read requests; MAL authorization-code/refresh POSTs, Crunchyroll and HIDIVE login/token POSTs, and MAL PUT writes are always single-attempt. Investigate an ambiguous timeout before authenticating or applying again.

## Review, sync and seasons

```bash
mal-updater review-mappings --limit 0 --mapping-limit 5 --persist-review-queue
mal-updater list-review-queue --summary --issue-type mapping_review
mal-updater dry-run-sync --limit 20 --approved-mappings-only
mal-updater apply-sync --limit 8 --exact-approved-only
mal-updater apply-sync --limit 8 --exact-approved-only --execute
mal-updater season-mappings --unresolved
mal-updater season-mappings --approve --provider crunchyroll --provider-series-id <id> --provider-season-key <key> --mal-anime-id <mal-id>
```

`dry-run-sync`, queue listing/worklist commands and mapping review are local planning surfaces. `apply-sync` without `--execute` revalidates and prints what it would write; `--execute` is the only MAL-mutating path. It plans every approved series from cached MAL state; `--limit` bounds the live revalidations/writes (0 means unbounded), most recently watched first, and the remainder is reported as `deferred` for later runs. `--provider` (repeatable) restricts it to evidence from the named providers. It exits non-zero when any item failed.

A provider series whose progress spans several canonical seasons (Crunchyroll dubs collapse onto their original-language season) is planned per season, because MAL lists each season as its own entry. Seasons are matched by the provider season catalog's air/publish dates and episode counts against MAL entries in the series' franchise, with the series mapping engine's auto-approval rules; at most `--mapping-limit` new searches run per pass and unresolved seasons are retried weekly. Seasons that cannot be matched are never written: they go to review and raise the `unresolved_season_mappings` health warning until approved with `season-mappings --approve`. Each MAL entry uses the provider's episode numbers when an earlier provider season maps to the same entry, and the in-season position otherwise; split-cour seasons are divided between MAL Part 1 and Part 2. Unnumbered items (PVs, recaps, OVAs, movies inside a series) do not count as episodes.

Upgrade note (migrations 032 and later): progress rows ingested before season capture carry no season key, and once a series has keyed rows its unkeyed rows are ignored. Right after upgrading, run `provider-fetch-snapshot --provider crunchyroll --full-refresh --max-history-pages 100 --ingest` (repeat until history is complete) and the same full refresh for `hidive`, so every row is re-observed with its season.

## MAL user-list refresh

```bash
mal-updater mal-list-refresh --max-pages 10
```

`mal-list-refresh` persists an account/query generation and a fair per-status traversal queue (never-started, resumable, retry-due, then routine refresh). Every parsed page, staged row set, opaque same-origin cursor, fingerprint and anchor commits atomically, and bounded runs resume the exact cursor. `--max-pages` is a request-attempt budget over every MAL call, including the authenticated identity lookup, continuation, revalidation and the pre-publication identity recheck; `--max-pages 0` makes no network call and changes no refresh state. Pages request `limit=1000` (MAL's maximum). Fingerprints cover only retained semantic fields (node id/title/num_episodes/media_type/status and `list_status`), never presentation fields such as `main_picture`.

Publication is generation-wide and atomic. It requires a revalidated account identity, unchanged query/logic identity, complete `data` + `paging` structure, strict offset cursor progression, an explicit terminal state for every partition, no duplicate MAL IDs, and current fingerprints for every staged page. `paging.next` may be absent only on a validated short terminal page. A per-account publication epoch prevents an older generation from overwriting a newer one. Only the canonical `all` query can prune globally; a status subset prunes only its own statuses.

MAL exposes offset pagination, not snapshot tokens, so concurrent insertion/deletion/reorder is detected rather than accepted: drift restarts a fresh generation up to 3 times per run, then ends the run as a resumable partial. Auth (401/403) and network failures retry on a later run. Only contract violations (malformed rows/cursors, unsupported statuses) quarantine. The published list stays last-known-good throughout. Inspect the JSON `traversal` object (`references/JSON_CONTRACT.md`) for queue, cursor, request, drift and validation state.

## Public MAL userrecs harvest

```bash
mal-updater recommend-refresh-full-userrecs --limit 3 --max-pages 10
mal-updater recommend-refresh-full-userrecs --limit 5 --force-refresh --format summary
mal-updater recommend-coverage
```

Seeds are the cached MAL `@me` list entries that count as watched-positive (`completed`, `watching`, `on_hold`); `dropped`, `plan_to_watch` and provider-only mappings are excluded. The client reads public HTML from `mal.public_base_url`, accepts only the same HTTPS origin, the source anime `/userrecs` path and a positive-integer `p` cursor, follows only advertised same-origin next links, and stores only target MAL ID, title and recommender count; prose and usernames are not retained.

Pages are staged in an open generation and stay non-authoritative until a complete recognized document, unambiguous pagination, a coherent terminal chain, and immediate revalidation of every staged page prove the exact staged revision; publication then replaces the source's graph in one transaction. `--max-pages` counts every network attempt including resume and final revalidation; exhaustion pauses rather than publishing, and zero returns before any queue or generation mutation. Malformed or truncated HTML, unsafe or conflicting next links, loops, drift, oversize bodies and transient HTTP failures preserve the previous edges. MAL's real empty page (its exact empty-state sentence, the title's make-recommendation link, no rows and no next page) publishes an empty source after final revalidation.

Source scheduling is durable and strict uncovered-first: never started, resumable, retry-due, then routine refresh, with a monotonic selection sequence for fairness. A source claim is an expiring lease; its token, expiry, source/generation identity and revision fence every mutation and publication.

## Quarantine recovery

Quarantine is durable; ordinary runs never clear it. After verifying the account/query or public source, use the audited reinitialize commands:

```bash
mal-updater mal-list-reinitialize --status all --page-size 1000 --reason "<review/ticket and operator rationale>"
mal-updater recommend-reinitialize-full-userrecs --source-mal-anime-id 1 --source-url https://myanimelist.net/anime/1/Cowboy_Bebop/userrecs --reason "<review/ticket and operator rationale>"
```

Both keep published last-known-good rows and prior generation history and only create a fresh fenced generation; they fetch and publish nothing. The MAL-list command makes one authenticated identity lookup that must match the quarantined account/query, and replaces the latest quarantined generation of that account/status lineage under the current list logic version.

## Manual shadow audits

These are descriptive diagnostics, not part of ranking or automation:

- `recommend-suggestions-audit [--limit 100] [--output report.json]` makes one OAuth `GET /v2/anime/suggestions` request (no pagination), reads the database query-only, and emits only cohort sizes, overlap/novelty counts and aggregate quality distributions; no suggestion IDs, titles, ranks or raw payload.
- `MAL_UPDATER_CRUNCHYROLL_RECOMMENDATION_SHADOW_ENABLED=true mal-updater crunchyroll-recommendation-shadow-audit --access-token-file TOKEN --account-id-file ACCOUNT --output report.json` makes bounded GET-only native-recommendation and home-feed probes. Both input files must be mode `0600`; the database is opened query-only and verified unchanged; output is aggregate counts only.

## Request budgets and telemetry

Every outbound MAL, Crunchyroll and HIDIVE attempt (including auth helpers, retries/timeouts and HIDIVE Algolia title search) is written to `<runtime root>/state/api-request-events.jsonl` with task/run attribution. Query values, credentials, headers and bodies are never logged (format in `references/JSON_CONTRACT.md`). Learned projections count events attributed to the exact run. Task hourly limits apply in addition to the provider-global limit, never instead of it; legacy unattributed events count toward both. Stop the scheduler before inspecting malformed telemetry by hand rather than editing the live file.

## Tests

See `references/QUALITY.md`.
