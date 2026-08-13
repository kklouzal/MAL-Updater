# Operations

Cache operations: normal maintenance skips fresh complete MAL/provider evidence. Durable positive MAL search/detail, provider detail, recommendation metadata/harvest, complete public userrecs, and provider-eligibility evidence use a 120-day deterministic per-key +/-15-day refresh target where applicable. Refresh due is separate from usability: read/display last-known-good data survives a due or failed refresh, but malformed JSON, logic/identity mismatch, auth failure, or unverified evidence never becomes usable. Negative/no-match MAL search stays a short 3-day claim and TTL 0 preserves cache-disable behavior. Use `recommend-refresh-metadata --force-refresh` only for explicit repair/upstream verification of official MAL metadata/detail rows; that official anime-detail surface exposes only a practical top-10 `recommendations` set and must not overwrite a complete public userrecs harvest. Use `recommend-refresh-full-userrecs` for the bounded complete aggregate public MAL `/userrecs` cold path. A 365-day provider title-search hit remains identity-only and never creates availability; availability is governed by independent eligibility evidence. Only successful contradictory evidence or explicit semantic invalidation revokes a positive state.

Effective daemon ownership defaults are intentionally separated: provider hot/incremental fetch hourly; MAL token refresh hourly; bounded MAL user-list refresh every 8 hours (3 pages); recommendation metadata every 12 hours (3 seed rows plus 5 discovery targets); complete public MAL userrecs cold harvest hourly (2 source titles, 3 advertised same-origin pages per source per run, resumable staging, deterministic 120 +/-15-day complete-refresh horizon); provider eligibility hourly in one credentialed-provider-specific lane (2 candidates, 1 alias each, 5 returned matches); and DB/local recommendation snapshot/health hourly. Unattended `sync_apply` remains disabled by default; when explicitly enabled it now requires every configured provider fetch lane to succeed in that same scheduler cycle before applying, so an old provider cache cannot authorize writes merely because the apply task is due. Eligibility lanes and the full-userrecs lane use both their task budget and the provider-global MAL/provider budget. Crunchyroll cold/full refresh is weekly and capped at 10 history + 2 watchlist pages; HIDIVE full refresh remains manual because it has no chunk controls.

Inspect the exact loaded policy and cache horizons with `service-status` (`niceness_policy` in JSON, `niceness_*` / `cache_*` in summary). These budgets and request-spacing values are local safety controls, not claims about provider limits.

Run commands from the repo root.

## Runtime layout

MAL-Updater keeps runtime state outside the skill tree under `.MAL-Updater/` by default.

Because this repository is public, only external runtime state may contain operator-specific secrets or machine-local residue. Anything tracked in git must stay anonymized and safe to publish.

MAL-Updater runtime state lives under:

- `.MAL-Updater/config/`
- `.MAL-Updater/secrets/`
- `.MAL-Updater/data/`
- `.MAL-Updater/state/`
- `.MAL-Updater/cache/`

Use `bootstrap-audit` or `status` to see the resolved paths for the current install.

## First-line inspection

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit --summary
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli runtime-retention-audit
PYTHONPATH=src python3 -m mal_updater.cli runtime-retention-audit --format summary
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli service-status --strict --format summary
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
```

`runtime-retention-audit` is diagnostic-only. It validates the configured runtime root for nested `.MAL-Updater` markers, repo/source overlap, symlink escapes, and missing/non-directory managed top-level paths, then reports bounded aggregate retention inventory for DB backups, health snapshots, logs/request events, tmp, cache, and artifacts. It does not enumerate secret filenames or contents and does not produce archive/delete/prune/chmod/move commands. Use `--strict` only when layout errors should fail an automation gate; old retained files and threshold review candidates stay warning-only, and scan/threshold caps are tunable with the `--max-*` and `--warn-*` options.

### Housekeeping and retention inventory

| Surface | Cadence | Current policy |
| --- | --- | --- |
| Recommendation score snapshots | Every scheduler pass (normally hourly) | Logical deletion only: 14-day horizon, newest 30 runs/kind protected, 10,000-row cap per pass; never triggers hourly `VACUUM`. |
| API request telemetry (`api-request-events.jsonl`) | Every scheduler pass (normally hourly) | Atomic, fail-closed pruning of events older than 14 days; malformed telemetry blocks pruning and preserves the original file. |
| SQLite physical compaction | Checked weekly; successful runs at least 30 days apart | Requires both 128 MiB and 10% freelist, verified fresh backup, pre/post-backup free-space gates, and writer exclusion. |
| Health artifacts | Health task normally hourly; retention daily | `latest-health-check.json` is never deleted. Timestamped JSON history keeps 90 days and at least the newest 168 snapshots, deleting at most 100 safe-name regular files/pass; any unsafe name, symlink, or filesystem error blocks the pass. |
| Service logs | Written continuously; checked before every append | `service.log` rotates before an append would exceed 16 MiB and retains five numbered generations. Symlinks/special files block appends; failures are not recursively logged. |
| Database backups | Created by operators and before compaction/restore; audited weekly | High-value manual retention only. The automatic local/read-only runtime inventory reports review candidates, but backups are never automatically deleted and require an explicit owner-approved archival/removal decision. |
| Other runtime families (auth/provider data, tmp, cache, artifacts, ambiguous files) | Audited weekly | Automatic bounded read-only inventory only. Any archive/prune/delete action remains explicit and human-gated; the audit never mutates these families. |

Every listed surface now has recurring automatic maintenance or a recurring
read-only audit with explicit human-gated action. Automatic deletion remains
limited to established telemetry/snapshot policy and safe timestamped health
history; backups, auth/provider data, ambiguous mappings, and arbitrary files
are never selected by these housekeeping lanes.

## Initialize runtime / DB

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli init
```

## MAL auth

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

## Provider auth / fetch

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider crunchyroll
PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider hidive
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --full-refresh
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json --full-refresh

Provider eligibility refresh evidence carries a versioned `ok`/`failed` lifecycle with bounded exponential retry timestamps; a successful refresh resets the failure count and retry timestamp, while a failed expired refresh remains non-actionable until its next retry window.

Provider request spacing and retry caps live in `[mal]`, `[crunchyroll]`, and
`[hidive]`. They are local niceness policy, separate from daemon hourly budgets
and not representations of provider-published limits. Request starts coordinate
through `.MAL-Updater/state/provider-request-gates/` across client instances and
processes. Retry telemetry counts each network attempt. Retries are restricted
to safe GET/read requests: MAL authorization-code/refresh POSTs, Crunchyroll
login/token POSTs, HIDIVE login/refresh POSTs, and MAL PUT writes are always
single-attempt. Investigate an ambiguous timeout before authenticating or
applying again; recoverable token/credential state is left in place.
```

`provider-auth-login` is live/provider-auth-mutating for the named provider because it logs in and writes local token/session state. `provider-fetch-snapshot` is live/provider-read and may write local cache/state; adding `--ingest` also mutates the local SQLite provider cache. HIDIVE auth/session state plus account history, Continue Watching, favourites-as-watchlist, and custom-list collection/detail snapshots are supported; HIDIVE recommendation enrichment/title lookup remains bounded to specific-title search, and HIDIVE full refresh is manual/non-chunked. Crunchyroll additionally supports the page chunk/resume flags documented in CLI help.

## Review / sync

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 0 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --summary --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 8 --exact-approved-only --execute
```

`dry-run-sync`, queue listing/worklist commands, and mapping review generation are local planning/diagnostic surfaces. `apply-sync --execute` is the live MAL-mutating path; keep it exact-approved and bounded unless an operator explicitly approves broader catch-up.

## Recommendation full userrecs harvest

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-list-refresh --max-pages 3
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-full-userrecs --limit 5 --max-pages 3
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-full-userrecs --limit 5 --force-refresh --format summary
PYTHONPATH=src python3 -m mal_updater.cli recommend-coverage
```

`recommend-refresh-full-userrecs` seeds only from the cached official MAL `@me` anime list statuses that count as watched-positive for recommendation semantics: `completed`, `watching`, and `on_hold`. It intentionally excludes `dropped` and `plan_to_watch` and does not use provider-only mappings as full-harvest seeds. The client reads public MAL HTML from the configured `mal.public_base_url`, validates same HTTPS origin plus the source anime `/userrecs` path and its positive-integer `p` page cursor, follows only advertised same-origin next links, dedupes target MAL IDs deterministically, and stores only target MAL ID/title plus aggregate recommender count. Recommendation prose and usernames are not retained.

Atomicity guarantee: fetched pages/edges are staged in an open generation and remain non-authoritative until a terminal page with no next link makes the staged chain coherent; publication then replaces the source's recommendation graph in one transaction. `--max-pages` is a per-source per-run budget, so max-page exhaustion pauses the staged generation with its next-page cursor instead of publishing partial data. Malformed HTML, out-of-origin next links, loops/drift, oversize bodies, and transient HTTP failures preserve the previous edges; drift discards the stale staged generation and restarts from the source URL. Once a complete public userrecs harvest exists for a source, the normal 12h official-detail metadata refresh will not clobber it with the official top-10 subset.

## User-systemd daemon

The scheduler also bounds `recommendation_score_snapshots` during each task
pass. By default it deletes at most 10,000 rows older than 14 days while always
preserving the newest 30 distinct runs for each recommendation kind (including
the current dashboard run). Fourteen days is the bounded operational history
horizon; the independent run floor protects sparse kinds and immediately makes
older high-volume history eligible instead of waiting through a growth-prone
90-day window. The result is recorded in service state as
`recommendation_snapshot_retention`, including deleted/remaining eligible rows
and SQLite page/freelist counts. Configure the three
`service.recommendation_snapshot_*` settings shown in
`references/settings.toml.example` if the defaults need adjustment.

Deletion bounds future logical growth but SQLite does not shrink the database
file automatically. Physical compaction is a separate low-frequency housekeeping
lane named `db_compaction`, not part of the hourly logical snapshot prune. By
default the daemon checks weekly but VACUUMs only when all fail-closed gates pass:
the previous successful compaction is at least 30 days old, SQLite freelist bytes
are at least 128 MiB and at least 10% of pages, the scheduler singleton is held,
repo-native DB writers are excluded by the shared DB lock, the DB volume has
space for a SQLite rewrite plus margin, and a fresh container-native backup under
`.MAL-Updater/state/backups/` verifies by manifest/checksum. Free space is checked
again after retaining that archive and immediately before `VACUUM`. The 128 MiB
and 10% defaults deliberately qualify the observed 175.3 MiB / 14.43% steady-state
freelist while still requiring both meaningful absolute and proportional waste. Backup retention remains high-value/manual-policy; this lane
creates recovery evidence and does not delete old archives.

SQLite `VACUUM` takes an exclusive database lock and rewrites the database file,
so expect a maintenance pause while `db_compaction` is running. It is safe to
leave enabled for the repo-native daemon because normal scheduler tasks cannot
race it; if non-repo/manual SQLite writers are active, stop them first or disable
`service.db_compaction_every_seconds`. Inspect cadence, last result, backup path,
bytes reclaimed, skip/block reason, and next due through `service-status` (JSON
or summary) before and after rollout.

```bash
cd <repo-root>
scripts/install_user_systemd_units.sh
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli service-status --strict --format summary
PYTHONPATH=src python3 -m mal_updater.cli restart-service
```

`service-run-once` is a live/mutating-capable manual daemon pass, not a routine inspection command. It can run due provider fetches, local ingest, and exact-approved MAL apply lanes; use it only after explicit operator intent, and use `service-status` / `health-check` for read-only verification.

Use one install path, not both, for routine setup. Prefer `scripts/install_user_systemd_units.sh` for production bootstrap because it renders the host-specific `mal-updater.service` from the repo template under `ops/systemd-user/` and preserves/installs the service env file. The CLI `install-service` path is a compatibility/direct service-manager path for targeted repair or tests. The tracked daemon unit uses conservative user-service sandboxing (`UMask=0077`, `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectSystem=strict`, `ProtectHome=read-only`, and a narrow address-family allowlist) while allowing the resolved MAL-Updater runtime/config/secrets/data/state/cache/DB directories to remain writable for SQLite, logs, token refreshes, and service state through a de-duplicated `ReadWritePaths` allowlist. The deterministic renderer intentionally supports absolute systemd paths without whitespace/control characters and fails fast on unsupported path values instead of emitting ambiguous unit syntax.

The optional dashboard unit `mal-updater-dashboard.service` is also tracked and rendered by the same installer, but it is not installed or enabled unless explicitly requested. Use `scripts/install_user_systemd_units.sh --install-dashboard` to render it without enabling it, or `--enable-dashboard` to opt in to enablement. Its template runs `dashboard-serve --host 127.0.0.1` through the repo venv and the same service env file; do not change it to a non-loopback bind unless you are deliberately reviewing a separate exposure decision. `--start-service` restarts only the main daemon, not the optional dashboard.

`service-status` remains a non-failing inspection command by default. Add `--strict` for automation gates; strict mode exits 2 when the main daemon unit/env/systemctl/runtime parse status is not automation-ready. A stopped, disabled, or absent optional dashboard is not considered a main-daemon failure.

### Request budgets and telemetry

Every outbound MAL, Crunchyroll, and HIDIVE attempt made by the supported clients—including auth helpers, retries/timeouts, and HIDIVE Algolia title search—is written to `.MAL-Updater/state/api-request-events.jsonl`. Daemon subprocesses inherit task/run attribution; direct CLI commands create their own CLI run context. Query values, credential material, request headers, and request bodies are not logged.

Daemon learned projections count events attributed to the exact run rather than subtracting two rolling-window totals, so events aging out during a long run cannot erase new attempts. Task-specific hourly limits are enforced in addition to the provider-global limit, never instead of it. Legacy unattributed telemetry remains globally budgeted and is included conservatively in task gates. `service-status` exposes the resulting request delta/projection and global/task counts; stop the daemon and inspect malformed JSONL manually rather than editing live telemetry under an active service.

## MAL redirect configuration

Use the redirect URI reported by:

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli status
```

Specifically use the `mal.redirect_uri` value when creating/configuring the MAL app.

## Issue reporting / feedback

If MAL-Updater misbehaves during real usage — whether in the OpenClaw skill flow or the Python back-end daemon/runtime — report it upstream via:

- <https://github.com/kklouzal/MAL-Updater/issues>

Use GitHub issues for bugs, regressions, portability problems, onboarding friction, and feature requests so upstream maintenance stays informed by real-world usage.

## Tests

```bash
cd <repo-root>
QUALITY_TMP="$(mktemp -d /tmp/mal-updater-quality.XXXXXX)"
mkdir -p "$QUALITY_TMP/home" "$QUALITY_TMP/tmp" "$QUALITY_TMP/runtime/config"
HOME="$QUALITY_TMP/home" \
TMPDIR="$QUALITY_TMP/tmp" \
MAL_UPDATER_RUNTIME_ROOT="$QUALITY_TMP/runtime" \
MAL_UPDATER_RUNTIME_DIR="$QUALITY_TMP/runtime" \
MAL_UPDATER_SETTINGS_PATH="$QUALITY_TMP/runtime/config/settings.toml" \
MAL_UPDATER_CONFIG="$QUALITY_TMP/runtime/config/settings.toml" \
MAL_UPDATER_QUALITY_TMP="$QUALITY_TMP" \
scripts/quality.sh
```
