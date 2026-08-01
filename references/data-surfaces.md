# Data surfaces / operator map

Use this file when the task is about *what backend data MAL-Updater exposes* and *which CLI surfaces to use*.

## Bootstrap / installation state

Use:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit --summary
PYTHONPATH=src python3 -m mal_updater.cli status
```

These expose:
- resolved workspace/runtime paths
- dependency readiness
- credential/app-setup readiness
- daemon install readiness
- whether manual foreground operation is still acceptable vs the daemon now being the expected unattended path
- redirect URI / auth-material presence

## Daemon / unattended runtime state

Use:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
```

These expose:
- service install/enable/active state
- health warnings and recommended remediation commands
- automation/runtime drift
- persisted task cadence/backoff details, including provider-floor cooldown provenance when budget pacing was extended on purpose

Use `service-run-once` only as an opt-in live/manual daemon pass. It can run due provider fetch, local ingest, and exact-approved MAL apply lanes; use `service-status`/`health-check` for inspection only.

## Recommendations

Use:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat --include-dormant
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-metadata
PYTHONPATH=src python3 -m mal_updater.cli push-recommendations-webhook --limit 20
```

Use this surface when the user wants:
- recommended anime to resume/watch next
- new season / dubbed-episode style recommendation output
- dormant-but-cached discovery candidates for debugging / audit
- recommendation metadata refresh
- pushing a recommendation digest into OpenClaw's webhook ingress for chat delivery
- choosing manual delivery posture (`--delivery-mode fresh|digest|all`) when you want a broader or narrower send than the daemon default
- checking whether the optional daemon-backed recommendation webhook push lane is enabled and current via `service-status`, including current delivery mode / dedupe posture

## Review queue / mapping state

Use:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --summary
PYTHONPATH=src python3 -m mal_updater.cli review-queue-next --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-worklist --issue-type mapping_review --limit 5
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --provider all
```

Use this surface when the user wants:
- mapping backlog state
- next recommended mapping-review slice
- grouped review worklists
- mapping inventory / approved coverage context across all currently supported providers, with optional per-provider filtering via `--provider`

## Guarded sync state / execution

Use:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 8 --exact-approved-only --execute
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json --ingest
```

Use this surface when the user wants:
- proposed MAL mutations before live writes
- exact-approved live MAL apply behavior (`apply-sync --execute`; omit `--execute`/use `dry-run-sync` for review-only planning)
- live provider snapshot refresh / local ingest state (`provider-fetch-snapshot --ingest` reads the provider and mutates local SQLite/cache, not MAL)
- health-driven recovery posture, including whether the daemon is planning an incremental repair fetch vs a heavier full-refresh recovery pass

## Auth surfaces

Use:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider crunchyroll
PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider hidive
```

Use this surface when the user wants:
- MAL OAuth bootstrap
- MAL token refresh
- Crunchyroll or HIDIVE staged auth bootstrap

## General rule

Prefer the smallest read-only surface that answers the question before reaching for live auth, snapshot, or apply commands.
