# Operations

## Local layout

Expected local directories relative to repo root:

- `config/` - non-secret local configuration
- `secrets/` - credentials/tokens only (gitignored)
- `data/` - SQLite database and durable local data (gitignored)
- `state/` - transient state/checkpoints (gitignored), including Crunchyroll auth/session material and the incremental `sync_boundary.json` overlap checkpoint
- `cache/` - fetch/cache scratch space (gitignored)

## Useful commands

### Worker status
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli health-check --review-issue-type mapping_review --review-worklist-limit 5
```

### Initialize local DB and directories
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli init
```

### Start MAL auth flow
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
```

### Verify current MAL token
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

MAL request pacing defaults to `~1.0s ± 0.2s` between requests (`[mal] request_spacing_seconds` / `request_spacing_jitter_seconds` in `config/settings.toml`). Timeout-prone MAL reads/writes retry once, then degrade back into the existing per-title skip/review behavior.

### Refresh MAL token
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
```

### Run tests
```bash
cd <repo-root>
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

### Stage Crunchyroll auth material from local credentials
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
```

### Fetch and optionally ingest a live Crunchyroll snapshot
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --full-refresh
```

### Inspect MAL mapping candidates
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli map-series --limit 20 --mapping-limit 5
```

### Build and persist a mapping review queue
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type mapping_review --reason-family same_franchise
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type mapping_review --fix-strategy-family "needs_manual_match | ambiguous_candidates | same_franchise"
PYTHONPATH=src python3 -m mal_updater.cli review-queue-worklist --issue-type mapping_review --limit 5
PYTHONPATH=src python3 -m mal_updater.cli review-queue-apply-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 20
PYTHONPATH=src python3 -m mal_updater.cli review-queue-refresh-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 20
# These ranked helpers now prefer broader family-level buckets before narrower exact slices when that clears more residue per command.
# When a selected mapping-review bucket looks stale after mapper changes, either apply the ranked refresh worklist or use the bucket's refresh_command directly
PYTHONPATH=src python3 -m mal_updater.cli refresh-mapping-review-queue --provider-series-id G63VMKVQY --provider-series-id GYMGDK3GY
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --approved-only
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-123 16498 --confidence 0.995 --notes "manual approval"
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-456 5114 --exact --confidence 1.0 --notes "manual exact approval"
```

### Generate and persist guarded sync review results
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type sync_review
```

### Revalidate and apply approved safe MAL updates
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20 --execute
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute
```

Use the stdlib `unittest` invocation above as the baseline Python test command. Do not assume `pytest` is installed on the host.

## MAL app settings

Current expected redirect URI for this Orin:
- `http://192.168.1.117:8765/callback`

If the Orin's LAN IP changes, this must be updated both:
- in MAL app registration
- in local `config/settings.toml`

## Secret handling rules

- never commit real tokens or credentials
- keep raw secrets only in `secrets/`
- token files should remain mode `0600`
- do not paste secrets into GitHub issues or docs

## Unattended exact-approved cadence

Use the repo wrapper for the first narrow unattended run path:

```bash
cd <repo-root>
./scripts/run_exact_approved_sync_cycle.sh
```

Install the user-level systemd timer set with `scripts/install_user_systemd_units.sh` (or use the manual `ops/systemd-user/` copy flow) as documented in `docs/AUTOMATION.md`.

After install, `health-check --format summary` now gives a fast runtime sanity check too: if the timers are enabled on disk but not actually active in the user manager, it emits `automation_inactive_timers=...`; when runtime state is available it also prints `automation_timer_runtime=...` with the timer active state plus next/last trigger timestamps.

## Health-check maintenance cadence

Use the repo wrapper for the standalone maintenance/triage snapshot:

```bash
cd <repo-root>
./scripts/run_health_check_cycle.sh
MAL_UPDATER_HEALTH_STALE_HOURS=48 ./scripts/run_health_check_cycle.sh
MAL_UPDATER_HEALTH_STRICT=1 ./scripts/run_health_check_cycle.sh
MAL_UPDATER_HEALTH_AUTO_RUN_RECOMMENDED=1 ./scripts/run_health_check_cycle.sh
MAL_UPDATER_HEALTH_AUTO_RUN_RECOMMENDED=1 MAL_UPDATER_HEALTH_AUTO_RUN_REASON_CODES=refresh_full_snapshot ./scripts/run_health_check_cycle.sh
MAL_UPDATER_HEALTH_AUTO_RUN_RECOMMENDED=1 MAL_UPDATER_HEALTH_AUTO_RUN_REASON_CODES=install_user_systemd_units ./scripts/run_health_check_cycle.sh
```

Artifacts:
- per-run log: `state/logs/health-check-*.log`
- per-run JSON snapshot: `state/health/health-check-*.json`
- latest JSON snapshot: `state/health/latest-health-check.json`
- queue guidance in the JSON/log now includes `recommended_next`, `recommended_worklist`, `recommended_apply_worklist`, and `recommended_refresh_worklist` when open review residue exists
- maintenance guidance in the JSON/log now also distinguishes the top overall repair (`recommended_command`) from the first safely automatable repair (`recommended_automation_command`)
- automation guidance now also distinguishes between missing-vs-outdated-vs-disabled repo-owned automation state, so the summary can tell you whether the next fix is a first install, a repo-sync refresh of stale user units, or simply enabling timers that were copied but never turned on

For the installed user service, optional runtime policy lives in `~/.config/mal-updater-health-check.env` (copy the example from `ops/systemd-user/mal-updater-health-check.env.example`).

Install the matching user-level systemd timer with `scripts/install_user_systemd_units.sh` (or the manual `ops/systemd-user/` copy flow) as documented in `docs/AUTOMATION.md`.
