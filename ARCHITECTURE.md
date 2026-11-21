# Architecture

## Repository layout

- `src/mal_updater/`: the Python application (CLI, scheduler, container control plane, provider integrations, SQLite migrations under `migrations/`).
- `Dockerfile`, `compose.yaml`, `compose.build.yaml`, `docker-entrypoint.sh`: the container product; see `references/CONTAINERS.md`.
- `SKILL.md`: OpenClaw skill entrypoint for native installs; the repo root is the skill package.
- `references/`: operator and agent references.
- `scripts/`: bootstrap, install, quality and distribution-check helpers.
- `ops/systemd-user/`: portable user-systemd unit templates rendered at install time.
- `tests/`: the full test suite, kept in the repo for third-party auditing.

## Runtime state

Runtime state never lives in the repo tree. The container uses the `/data` volume (`MAL_UPDATER_RUNTIME_ROOT=/data`); native installs default to `.MAL-Updater/` in the workspace. Both contain:

- `config/settings.toml`: non-secret settings (`references/settings.toml.example`)
- `secrets/`: MAL and provider credentials/tokens (mode 0600)
- `data/mal_updater.sqlite3`: the database; schema comes only from the migration runner (`init`, or container startup)
- `state/`: `service-state.json`, `logs/service.log`, `api-request-events.jsonl`, `health/latest-health-check.json`, per-provider session and sync-boundary files, `backups/`
- `cache/`: disposable provider snapshots

## Execution model

- **Container:** `tini` runs a Python supervisor (`container_runtime.py`) that serves the trusted-LAN web UI and keeps the scheduler running whenever MAL setup is complete.
- **Native:** a user-level systemd unit runs `python3 -m mal_updater.cli --project-root <repo-root> service-run`.

Either way the scheduler (`service_runtime.py`) owns recurring lanes: per-provider fetch and ingest, MAL token refresh, MAL list refresh, bounded exact-approved MAL apply, recommendation metadata, public MAL userrecs harvest, provider eligibility, recommendation snapshots, health checks, and housekeeping. Network lanes run as `mal_updater.cli` subprocesses under request budgets and backoff. Lane defaults are in `references/OPERATIONS.md`.

## Pipeline

1. **Fetch:** provider modules (`providers/crunchyroll.py`, `providers/hidive.py`, registered in `provider_registry.py`) authenticate and emit a provider-neutral snapshot (`contracts/provider.py`, validated by `validation.py`). Fetchers also fill the provider season catalog (`season_catalog.py`).
2. **Ingest:** `ingestion.py` persists series, per-episode progress (with canonical season identity), and watchlists.
3. **Map:** `mapping.py` matches each provider series to MAL candidates; per-season matches (`season_mappings.py`) use catalog air dates, episode counts and the MAL franchise graph. Ambiguous results go to the review queue.
4. **Plan and apply:** `sync_planner.py` proposes forward-only MAL changes per season; `apply-sync --execute` revalidates each against live MAL and writes only exact-approved, non-regressing updates.
5. **Recommend:** `recommendations.py` and the dashboard build ranked continue/new-episode/discovery lists from MAL metadata, public userrecs and provider availability evidence.

The provider boundary assumes no particular auth style or paging shape; a new provider only has to emit the normalized snapshot contract (`references/JSON_CONTRACT.md`).

## Guardrails

- Keep sync conservative: missing-data-first, never decrease progress, never auto-resolve ambiguity (`references/DECISIONS.md`).
- Keep runtime state out of the repo; avoid host-specific assumptions in committed files.
- Because this repo is public, keep all tracked artifacts anonymized: no personal identities, personal emails, host-specific absolute paths, private workspace references, real account identifiers, or real secrets in code, references, tests or history.
- Prefer the scheduler's own lanes over ad-hoc timers or OpenClaw cron.
