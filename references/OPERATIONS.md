# Operations

Cache operations: normal maintenance skips fresh complete MAL/provider evidence. Use `recommend-refresh-metadata --force-refresh` only for explicit repair/upstream verification. Invalidation is TTL plus logic-version based; malformed JSON is ignored and refetched. A 365-day provider title-search hit never extends provider eligibility: expired eligibility evidence is not a current availability claim.

Effective daemon ownership defaults are intentionally separated: provider hot/incremental fetch hourly; MAL token refresh hourly; bounded MAL user-list refresh every 8 hours (3 pages); recommendation metadata every 12 hours (3 seed rows plus 5 discovery targets); provider eligibility daily in one credentialed-provider-specific lane (2 candidates, 1 alias each, 5 returned matches); and DB/local recommendation snapshot/health hourly. Eligibility lanes use both their task budget and the provider-global budget. Fresh actionable 7-day eligibility evidence plus the 365-day identity cache yields a successful zero-network-request run. Crunchyroll cold/full refresh is weekly and capped at 10 history + 2 watchlist pages; HIDIVE full refresh remains manual because it has no chunk controls.

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
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
```

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

`provider-auth-login` is live/provider-auth-mutating for the named provider because it logs in and writes local token/session state. `provider-fetch-snapshot` is live/provider-read and may write local cache/state; adding `--ingest` also mutates the local SQLite provider cache. HIDIVE snapshots cover account history, continue-watching, and favourites-as-watchlist surfaces; Crunchyroll additionally supports the page chunk/resume flags documented in CLI help.

## Review / sync

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 0 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --summary --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 8 --exact-approved-only --execute
```

`dry-run-sync`, queue listing/worklist commands, and mapping review generation are local planning/diagnostic surfaces. `apply-sync --execute` is the live MAL-mutating path; keep it exact-approved and bounded unless an operator explicitly approves broader catch-up.

## User-systemd daemon

```bash
cd <repo-root>
scripts/install_user_systemd_units.sh
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli restart-service
PYTHONPATH=src python3 -m mal_updater.cli service-run-once
```

Use one install path, not both, for routine setup. Prefer `scripts/install_user_systemd_units.sh` for production bootstrap because it renders the host-specific `mal-updater.service` from the repo template under `ops/systemd-user/` and preserves/installs the service env file. The CLI `install-service` path is a compatibility/direct service-manager path for targeted repair or tests.

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
PYTHONPATH=src python3 -m unittest discover -s tests -v
```
