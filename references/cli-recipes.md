# CLI recipes

Run commands from the skill root (`{baseDir}` / repo root).

## Bootstrap / install / audit

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit --summary
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
PYTHONPATH=src python3 -m mal_updater.cli install-service
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli service-run-once
scripts/install_user_systemd_units.sh
```

## MAL auth

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

## Provider auth / fetch

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider crunchyroll
PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider hidive
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --full-refresh
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json --full-refresh
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider all --format summary
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider all --older-than-days 30 --format summary
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider crunchyroll
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider crunchyroll --format summary
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider hidive --cutoff "2026-04-25 17:59:00"
```

`provider-stale-rows` is read-only. JSON and summary output include per-family stale counts, oldest/newest `last_seen_at` bounds, exact oldest/newest age-in-days ranges, coarse age-bucket counts (`recent_0_7_days`, `older_8_30_days`, `older_31_plus_days`), and linkage counts for stale progress/watchlist rows (`with_stale_series`, `with_current_series`, `with_missing_series`); JSON sample rows also include exact `age_days`, and child progress/watchlist samples include `linked_series_posture` plus `linked_series_last_seen_at`, so operators can judge concrete residue examples and dependency shape before deciding whether to leave, archive, or prune rows later. The payload and terse summary now also include a policy-neutral `retention_review` posture (`recent_residue_observe`, `aging_residue_observe`, `current_series_child_residue`, or `manual_retention_policy_candidate`) with review-candidate and next-step fields; this is diagnostic guidance only and still preserves `diagnostic_only_no_archive_or_prune`. Add `--older-than-days N` when you only want rows that are stale since the cutoff and have also been absent for at least `N` days.

Compatibility wrappers still exist for Crunchyroll-specific debugging/bootstrap:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
```

## Review queue triage

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --summary --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-next --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-worklist --issue-type mapping_review --limit 5
PYTHONPATH=src python3 -m mal_updater.cli review-queue-apply-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 10
PYTHONPATH=src python3 -m mal_updater.cli review-queue-refresh-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 10
```

## Sync / apply

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider all --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider all --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider hidive --limit 20
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider crunchyroll --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20 --execute
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 8 --exact-approved-only --execute
PYTHONPATH=src python3 -m mal_updater.cli exact-approved-sync-cycle
PYTHONPATH=src python3 -m mal_updater.cli exact-approved-sync-cycle --full-refresh

# Unattended daemon posture: keep exact-approved apply runs bounded unless you are deliberately doing a full manual catch-up pass.
```

## Recommendations

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat --include-dormant
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-metadata
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-metadata --include-discovery-targets --discovery-target-limit 50
```

## Tests

```bash
cd {baseDir}
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m unittest tests.test_config -v
PYTHONPATH=src python3 -m unittest tests.test_install_user_systemd_units -v
PYTHONPATH=src python3 -m unittest tests.test_health_cli -v
```
