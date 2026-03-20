# Python Worker

## Current model

The Python package under `src/mal_updater/` is the canonical implementation.

Use it from the repo root with:

```bash
PYTHONPATH=src python3 -m mal_updater.cli ...
```

## Configuration model

Default runtime paths resolve under `.MAL-Updater/` in the workspace, not inside the repo tree.

The loader reads:
- `MAL_UPDATER_SETTINGS_PATH` when provided
- otherwise `.MAL-Updater/config/settings.toml`

`settings.toml` may override:
- runtime path layout
- MAL endpoint/bind/redirect settings
- request pacing
- secret file names

Path values in `settings.toml` may be absolute or relative to the settings file location.

## Useful commands

```bash
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute
```
