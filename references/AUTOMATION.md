# Automation

MAL-Updater now uses a **long-lived user-systemd daemon** rather than timer-driven one-shot jobs as the primary unattended model.

## Daemon model

The installed service runs:

```bash
python3 -m mal_updater.cli --project-root <repo-root> service-run
```

That foreground process owns its own internal loop cadence and recurring lanes for:

- MAL token refresh
- bounded exact-approved sync batches
- recurring health-check/report generation
- API request logging / budget awareness

## User systemd install

Repo-owned templates live in `ops/systemd-user/`.

Install the daemon with:

```bash
cd <repo-root>
scripts/install_user_systemd_units.sh
```

Important:
- the committed `.service` file is a template, not a host-bound final unit
- the installer renders the current repo root and service env path into the installed copy
- this keeps the repo portable while still producing a valid host-specific user daemon

Useful variants:

```bash
scripts/install_user_systemd_units.sh --dry-run
scripts/install_user_systemd_units.sh --no-enable
scripts/install_user_systemd_units.sh --start-service
```

## Direct service commands

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli install-service
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli restart-service
PYTHONPATH=src python3 -m mal_updater.cli service-run-once
```

`service-status` is now the main structured observability surface for unattended debugging. In addition to user-systemd enabled/active state, it reports:

- recent daemon loop timing (`last_loop_at`)
- per-lane task summaries from `service-state.json` (including last decision time plus last-run start/finish/duration when available)
- persisted budget backoff details, including whether a lane is cooling down at `warn` or `critical` level, whether the active cooldown policy came from task-level vs provider-level budget settings, and which learned projection source (configured / smoothed / percentile / auto-bursty percentile) the daemon is currently trusting for pre-run request budgeting
- each provider fetch lane's currently planned fetch mode plus any current full-refresh reason, so operator checks can immediately see when periodic or health-driven full-refresh pressure is active even before the next fetch executes; cadence-driven overdue resweeps now also expose when they first became due plus the current overdue age in seconds, and when the last run had to budget-defer that heavier resweep and fall back to incremental, service-status now also marks that deferred posture explicitly
- bounded unattended execution posture for `sync_apply` via `service.task_execute_limits`, plus automatic reset of stale projected-request/backoff state when that execution posture changes so old full-pass history does not keep poisoning the lane
- persisted failure-backoff details for task errors, including retry countdown, last failure reason, and consecutive-failure streaks for auth-fragile provider lanes
- health-check-driven auth recovery hints: repeated auth-style provider fetch failures recorded in daemon state now surface as explicit re-bootstrap recommendations (`crunchyroll-auth-login` / `provider-auth-login --provider hidive`) instead of only opaque backoff residue
- health-driven full-refresh escalation is disabled by default with `service.full_refresh_every_seconds = 0`; if an operator explicitly enables full-refresh cadence, health recommendations can still upgrade the next unattended provider fetch to `--full-refresh`
- current API-usage snapshot when available
- recent `service.log` tail lines
- parsed `latest-health-check.json` state (or parse errors when the artifact is malformed)
- a terse `service-status --format summary` view for quick operator checks and log-friendly output; that summary now also preserves the parsed health artifact's top maintenance command metadata (`reason_code`, automation-safe posture, auth-interaction posture, and auth failure/remediation classification when present) instead of only echoing the bare command string

## Repo-native manual cycle commands and compatibility wrappers

The daemon-first unattended model now keeps both maintenance lanes on repo-native CLI surfaces:

- `python3 -m mal_updater.cli exact-approved-sync-cycle` (manual guarded fetch+apply cycle across all staged providers)
- `python3 -m mal_updater.cli health-check-cycle` (repo-native health lane)

Compatibility wrappers still exist:

- `scripts/run_exact_approved_sync_cycle.sh` → thin shim to `exact-approved-sync-cycle`
- `scripts/run_health_check_cycle.sh` → thin shim to `health-check-cycle`

Those commands still write runtime artifacts under `.MAL-Updater/state/` and `.MAL-Updater/cache/`, but they are subordinate to the daemon-first orchestration model.

## Recommended bootstrap order

1. `bootstrap-audit`
2. `init`
3. MAL / Crunchyroll auth setup
4. `scripts/install_user_systemd_units.sh`
5. `service-status`
6. `service-run-once`
7. `health-check --format summary`

`health-check --format summary` now mirrors the top maintenance command metadata from the richer JSON payload too, so shell/log consumers can see not just the recommended command but also why it was chosen and whether it is automation-safe or auth-interactive.

Do not claim unattended automation is installed or healthy until the installer, service-status, and health-check all agree.
