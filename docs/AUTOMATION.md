# Automation

## Exact-approved recurring sync

The first unattended cadence is intentionally narrow:

- fetch a fresh live Crunchyroll snapshot
- ingest it into SQLite
- apply MAL updates only for exact approved mappings
- do not fall back to live MAL mapping search during execution

### Exact run path

```bash
cd <repo-root>
./scripts/run_exact_approved_sync_cycle.sh
```

That wrapper currently does:

```bash
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute
```

Safety properties:

- overlap-safe via `flock` lock file at `state/locks/exact-approved-sync.lock`
- logs per run under `state/logs/`
- live MAL writes remain constrained by the guarded executor
- exact-only currently means approved mappings whose persisted `mapping_source` is `auto_exact` or `user_exact`
- Crunchyroll auth recovery is bounded and conservative: the fetch path first tries the staged refresh token, and if Crunchyroll answers `401` on the content API mid-run (or the refresh token itself has expired), it performs one credential rebootstrap from the locally staged secrets, retries the failed request, and resumes the same fetch cycle instead of restarting from scratch or looping indefinitely
- successful fetches now update `state/crunchyroll/<profile>/sync_boundary.json`, so later unattended runs can stop history/watchlist paging once they hit already-synced overlap instead of walking the full older tail every time
- if a fresh fetch still cannot be salvaged, the exact-approved wrapper now continues with the most recent already-ingested Crunchyroll state instead of aborting the entire MAL apply pass

## Health-check recurring maintenance

The first maintenance-only cadence is now separate from live sync execution:

```bash
cd <repo-root>
./scripts/run_health_check_cycle.sh
```

That wrapper currently does:

```bash
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli health-check --stale-hours "${MAL_UPDATER_HEALTH_STALE_HOURS:-72}"
```

For a terse operator-facing view without the full JSON payload, the CLI also now supports:

```bash
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
```

Additional behavior:

- overlap-safe via `flock` lock file at `state/locks/health-check.lock`
- logs per run under `state/logs/`
- archives JSON snapshots under `state/health/`
- refreshes `state/health/latest-health-check.json` for the newest result
- emits a short shell-log summary of `healthy`, approved mapping coverage, warning codes, the top overall maintenance command, the first safely automatable maintenance command, the next suggested review drilldown, the matching single-slice queue action, and the batch `review-queue-apply-worklist` / `review-queue-refresh-worklist` commands when backlog exists
- optional strict mode: set `MAL_UPDATER_HEALTH_STRICT=1` to return non-zero when the health payload contains warnings
- direct CLI strict mode also exists now via `PYTHONPATH=src python3 -m mal_updater.cli health-check --strict`, which still prints the JSON payload but exits `2` when warnings are present
- recommendation-focused automation can scope queue suggestions with `--review-issue-type mapping_review|sync_review` and tune output size with `--review-worklist-limit N`
- health-check now also flags partial incremental coverage when the latest ingest touched only a subset of the cached provider rows and recommends a `crunchyroll-fetch-snapshot --full-refresh --ingest` repair cycle so the maintenance cadence does not mistake a one-page overlap refresh for a globally fresh local cache
- when partial coverage is present, that full-refresh repair now takes precedence over the weaker incremental refresh recommendation so auto-remediation does not waste a cycle on the wrong fetch shape first
- optional self-remediation now exists in the wrapper: set `MAL_UPDATER_HEALTH_AUTO_RUN_RECOMMENDED=1` to let the script execute the first safe maintenance command from `maintenance.recommended_commands`, then re-run `health-check` so the archived JSON reflects the post-remediation state
- health-check now also exposes `maintenance.recommended_command` and `maintenance.recommended_automation_command`, so automation/logging can distinguish the top overall suggestion from the first non-interactive safe repair
- when this repo includes the checked-in `ops/systemd-user/` assets, `health-check` now reports whether those exact user units are installed under `~/.config/systemd/user/` for the current user, compares installed copies against the current checked-in content, and recommends `scripts/install_user_systemd_units.sh` if that automation layer is missing or has drifted stale from the repo version
- health-check now also warns when approved mapping coverage falls below `--mapping-coverage-threshold` (default `0.8`) and recommends a bounded `review-mappings --persist-review-queue` pass so an empty persisted queue cannot hide a large unmapped provider residue while still leaving durable follow-up rows behind
- when repo-owned timers are both installed and enabled, health-check now also asks the live user systemd runtime whether those timers are actually active and when they next elapse; that closes the false-green gap where symlinks exist on disk but the user manager never loaded or restarted them. Summary output now includes `automation_inactive_timers` plus `automation_timer_runtime=...` lines when runtime state is available
- health recommendation payloads now explicitly mark each command with `automation_safe` and `requires_auth_interaction`, and the wrapper refuses to auto-run anything that is not explicitly automation-safe
- allowed self-remediation commands are still gated by reason code through `MAL_UPDATER_HEALTH_AUTO_RUN_REASON_CODES` (default: `refresh_ingested_snapshot,refresh_full_snapshot`), which gives a second allowlist layer on top of the command metadata and intentionally does **not** auto-run the new mapping-review backlog rebuild unless you opt into it; when auto-remediation is enabled but blocked by that allowlist, the wrapper log now says so explicitly instead of falling back to an ambiguous no-op
- the wrapper can now execute both MAL-Updater CLI subcommands **and** direct repo-owned shell recommendations (for example `scripts/install_user_systemd_units.sh`) when those commands are marked automation-safe by `health-check` and their reason code is present in the allowlist

## Crunchyroll pacing

`config/settings.toml` now supports:

```toml
[crunchyroll]
locale = "en-US"
request_spacing_seconds = 22.5
request_spacing_jitter_seconds = 7.5
```

The live fetch path spaces individual Crunchyroll HTTP requests by roughly this amount (default: `22.5 ± 7.5s`, i.e. randomized 15-30 seconds). With the current flow, a normal run usually issues:

1. refresh-token request
2. account lookup
3. watch-history request(s)
4. watchlist request(s)

So larger histories/watchlists will naturally take longer than the minimum 6-10 hour cadence envelope.

## User systemd timer

Repo-owned units live in `ops/systemd-user/`:

- `mal-updater-exact-approved-sync.service`
- `mal-updater-exact-approved-sync.timer`
- `mal-updater-health-check.service`
- `mal-updater-health-check.timer`

Install/update them into the user manager with the repo-owned helper:

```bash
scripts/install_user_systemd_units.sh
```

That helper copies the checked-in units into `~/.config/systemd/user/`, preserves an existing `~/.config/mal-updater-health-check.env`, runs `systemctl --user daemon-reload`, enables both timers, and prints the resulting timer state. It now also reports which units were newly installed, updated in place, or already unchanged plus whether the optional health env file was installed, preserved, or skipped, so the remediation path is auditable instead of opaque.

Useful variants:

```bash
scripts/install_user_systemd_units.sh --dry-run
scripts/install_user_systemd_units.sh --no-enable
scripts/install_user_systemd_units.sh --start-services
```

If you want the manual equivalent instead, it is:

```bash
mkdir -p ~/.config/systemd/user
cp ops/systemd-user/mal-updater-exact-approved-sync.service ~/.config/systemd/user/
cp ops/systemd-user/mal-updater-exact-approved-sync.timer ~/.config/systemd/user/
cp ops/systemd-user/mal-updater-health-check.service ~/.config/systemd/user/
cp ops/systemd-user/mal-updater-health-check.timer ~/.config/systemd/user/
cp ops/systemd-user/mal-updater-health-check.env.example ~/.config/mal-updater-health-check.env
systemctl --user daemon-reload
systemctl --user enable --now mal-updater-exact-approved-sync.timer
systemctl --user enable --now mal-updater-health-check.timer
systemctl --user list-timers mal-updater-exact-approved-sync.timer mal-updater-health-check.timer
```

Timer behavior:

- exact-approved sync runs on an intentionally conservative jittered cadence of about 8 hours on average, implemented as `OnUnitInactiveSec=6h` plus `RandomizedDelaySec=4h` (effective spread: roughly 6-10 hours after the previous run finishes)
- health-check runs on a lighter jittered cadence of about 12 hours on average, implemented as `OnUnitInactiveSec=10h` plus `RandomizedDelaySec=4h`
- `mal-updater-health-check.service` now reads an optional `EnvironmentFile=-%h/.config/mal-updater-health-check.env`, so installed timers can enable strict mode or narrow self-remediation without editing the unit itself
- copy `ops/systemd-user/mal-updater-health-check.env.example` to `~/.config/mal-updater-health-check.env` before enabling the timer if you want repo-owned defaults for `MAL_UPDATER_HEALTH_STALE_HOURS`, `MAL_UPDATER_HEALTH_STRICT`, or `MAL_UPDATER_HEALTH_AUTO_RUN_*`
- recommended unattended posture if self-remediation is enabled: keep `MAL_UPDATER_HEALTH_AUTO_RUN_REASON_CODES=refresh_full_snapshot` so the timer only performs the stronger cache-freshness repair that the live health-check is already warning about, instead of broadening into extra fetch shapes by accident
- because both timers are based on `OnUnitInactiveSec`, a brand-new install does not force an immediate catch-up run by itself; start either service manually once if you want an immediate first cycle
- `Persistent=true` so missed intervals catch up once when the machine/user manager comes back
