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

Install/update them into the user manager with:

```bash
mkdir -p ~/.config/systemd/user
cp ops/systemd-user/mal-updater-exact-approved-sync.service ~/.config/systemd/user/
cp ops/systemd-user/mal-updater-exact-approved-sync.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now mal-updater-exact-approved-sync.timer
systemctl --user list-timers mal-updater-exact-approved-sync.timer
```

Timer behavior:

- subsequent runs land on an intentionally conservative jittered cadence of about 8 hours on average, implemented as `OnUnitInactiveSec=6h` plus `RandomizedDelaySec=4h` (effective spread: roughly 6-10 hours after the previous run finishes)
- because the timer is based on `OnUnitInactiveSec=6h`, a brand-new install does not force an immediate catch-up run by itself; start the service manually once if you want an immediate first cycle
- `Persistent=true` so missed intervals catch up once when the machine/user manager comes back
he machine/user manager comes back
