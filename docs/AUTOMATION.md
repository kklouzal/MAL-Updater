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

## Crunchyroll pacing

`config/settings.toml` now supports:

```toml
[crunchyroll]
locale = "en-US"
request_spacing_seconds = 10.0
```

The live fetch path spaces individual Crunchyroll HTTP requests by roughly this amount. With the current flow, a normal run usually issues:

1. refresh-token request
2. account lookup
3. watch-history request(s)
4. watchlist request(s)

So larger histories/watchlists will naturally take longer than the minimum 6-hour cadence.

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

- subsequent runs about every 6 hours after the previous run finishes
- because the timer is based on `OnUnitInactiveSec=6h`, a brand-new install does not force an immediate catch-up run by itself; start the service manually once if you want an immediate first cycle
- `Persistent=true` so missed intervals catch up once when the machine/user manager comes back
