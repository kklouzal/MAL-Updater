# Automation

MAL-Updater now uses a **long-lived user-systemd daemon** rather than timer-driven one-shot jobs as the primary unattended model.

## Daemon model

The installed service runs:

```bash
python3 -m mal_updater.cli --project-root <repo-root> service-run
```

That foreground process owns its own internal loop cadence and recurring lanes for:

- MAL token refresh
- exact-approved sync passes
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
- current API-usage snapshot when available
- recent `service.log` tail lines
- parsed `latest-health-check.json` state (or parse errors when the artifact is malformed)
- a terse `service-status --format summary` view for quick operator checks and log-friendly output

## Legacy wrappers still reused by the daemon

The daemon currently reuses these guarded wrapper scripts for some lanes:

- `scripts/run_exact_approved_sync_cycle.sh`
- `scripts/run_health_check_cycle.sh`

Those still write runtime artifacts under `.MAL-Updater/state/` and `.MAL-Updater/cache/`, but they are now subordinate to the daemon-first orchestration model.

## Recommended bootstrap order

1. `bootstrap-audit`
2. `init`
3. MAL / Crunchyroll auth setup
4. `scripts/install_user_systemd_units.sh`
5. `service-status`
6. `service-run-once`
7. `health-check --format summary`

Do not claim unattended automation is installed or healthy until the installer, service-status, and health-check all agree.
