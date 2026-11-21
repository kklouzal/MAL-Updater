# Native daemon (user systemd)

The container runs the scheduler itself (`references/CONTAINERS.md`). On a native source checkout the same scheduler runs as a long-lived user-level systemd service:

```bash
python3 -m mal_updater.cli --project-root <repo-root> service-run
```

Lane cadences, budgets and housekeeping are documented once, in `references/OPERATIONS.md`.

## Install

Templates live in `ops/systemd-user/`. Render and install them with:

```bash
cd <repo-root>
scripts/install_user_systemd_units.sh            # render, install, enable mal-updater.service
scripts/install_user_systemd_units.sh --dry-run
scripts/install_user_systemd_units.sh --no-enable
scripts/install_user_systemd_units.sh --start-service
scripts/install_user_systemd_units.sh --install-dashboard   # also render the optional dashboard unit
scripts/install_user_systemd_units.sh --enable-dashboard    # and enable it
```

The committed `.service` files are templates; the installer substitutes the repo root, Python binary, service env file and a de-duplicated `ReadWritePaths` allowlist of the resolved runtime directories. It supports absolute paths without whitespace/control characters and fails fast otherwise. It also installs `ops/systemd-user/mal-updater-service.env.example` as the service env file (mode 0600) when none exists. Units run with `UMask=0077`, `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectSystem=strict`, `ProtectHome=read-only` and an `AF_UNIX`/`AF_INET`/`AF_INET6` address-family allowlist.

`mal-updater-dashboard.service` runs `dashboard-serve --host 127.0.0.1`; keep it on loopback unless you deliberately review a wider exposure. `--start-service` restarts only the main daemon.

`python3 -m mal_updater.cli install-service` is a lower-level service-manager path for targeted repair or tests; do not run both install paths as routine setup.

## Inspect

```bash
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli service-status --strict --format summary
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
PYTHONPATH=src python3 -m mal_updater.cli restart-service
```

`service-status` is the main observability surface. Besides systemd enabled/active state it reports the last loop time; per-lane state from `service-state.json` (cadence, last decision, last run start/finish/duration, next due, and a derived execution state: `waiting_until_due`, `due_now`, `cooling_down_for_budget`, `cooling_down_after_failure` or `awaiting_schedule`); budget and failure backoff with their source (task vs provider policy) and the projection estimator in use; each provider fetch lane's planned fetch mode and any full-refresh reason, including overdue age and budget deferral; the effective `niceness_policy`; API usage; the `service.log` tail; and the parsed latest health artifact with its top maintenance command and that command's `reason_code`, automation-safety and auth posture.

By default `service-status` never fails. `--strict` exits 2 when the main daemon unit, env file, systemctl or runtime state is not automation-ready; a stopped or absent optional dashboard is not a failure.

`service-run-once` is not an inspection command: it runs due lanes once, which can include provider fetches, ingest and (when opted in) MAL apply. Use it only with explicit operator intent.

## Manual cycle commands

- `exact-approved-sync-cycle [--full-refresh]`: fetch every credentialed provider, then run a bounded exact-approved apply. If any provider fetch fails it skips the apply unless `--allow-stale-provider-apply` is given.
- `health-check-cycle`: write a health artifact and optionally run safe recommended maintenance (`--auto-run-recommended`); accepts the same tuning flags as `health-check`.

`scripts/run_exact_approved_sync_cycle.sh` and `scripts/run_health_check_cycle.sh` are thin wrappers around these commands.

## Recommended bootstrap order

1. `bootstrap-audit`
2. `init`
3. MAL OAuth (`references/MAL_OAUTH.md`) and provider auth (`provider-auth-login --provider crunchyroll|hidive`, or `crunchyroll-link`)
4. `scripts/install_user_systemd_units.sh`
5. `service-status` and `health-check --format summary`
6. Only with operator consent: set `service.task_execute_limits.sync_apply` to a positive value and `restart-service` to enable unattended MAL writes.

Do not claim unattended automation is installed or healthy until the installer, `service-status` and `health-check` agree.
