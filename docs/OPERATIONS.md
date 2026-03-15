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

Install the user-level systemd timer from `ops/systemd-user/` as documented in `docs/AUTOMATION.md`.
