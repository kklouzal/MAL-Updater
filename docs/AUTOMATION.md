# Automation

MAL-Updater ships repo-owned automation wrappers plus portable systemd templates.

## Exact-approved recurring sync

```bash
cd <repo-root>
./scripts/run_exact_approved_sync_cycle.sh
```

Current flow:

```bash
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute
```

Behavior:
- external lock file under `.MAL-Updater/state/locks/`
- per-run logs under `.MAL-Updater/state/logs/`
- snapshot artifact under `.MAL-Updater/cache/`
- guarded MAL apply path only
- bounded Crunchyroll auth recovery

## Health-check recurring maintenance

```bash
cd <repo-root>
./scripts/run_health_check_cycle.sh
```

Behavior:
- external lock file under `.MAL-Updater/state/locks/`
- per-run logs under `.MAL-Updater/state/logs/`
- archived JSON snapshots under `.MAL-Updater/state/health/`
- `latest-health-check.json` maintained under the same runtime tree
- optional strict mode via `MAL_UPDATER_HEALTH_STRICT=1`
- optional allowlisted self-remediation for safe maintenance commands

## User systemd install

Repo-owned templates live in `ops/systemd-user/`.

Install them with:

```bash
cd <repo-root>
scripts/install_user_systemd_units.sh
```

Important:
- the committed `.service` files are templates, not host-bound final units
- the installer renders the current repo root and health env path into the installed copies
- this keeps the repo portable while still producing valid host-specific units at bootstrap time

Useful variants:

```bash
scripts/install_user_systemd_units.sh --dry-run
scripts/install_user_systemd_units.sh --no-enable
scripts/install_user_systemd_units.sh --start-services
```

## Recommended bootstrap order

1. `bootstrap-audit`
2. `init`
3. MAL / Crunchyroll auth setup
4. `health-check --format summary`
5. `scripts/install_user_systemd_units.sh`

Do not claim unattended automation is installed or healthy until the install script and health-check both agree.
