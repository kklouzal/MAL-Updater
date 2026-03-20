# CLI recipes

Run from the MAL-Updater repo root.

## Bootstrap / status / health

```bash
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli health-check
```

## Crunchyroll live fetch

```bash
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --full-refresh
```

Use `--full-refresh` only when deliberately bypassing the incremental overlap boundary.

## Mapping review / queue triage

```bash
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --summary --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-next --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-worklist --issue-type mapping_review --limit 5
PYTHONPATH=src python3 -m mal_updater.cli resolve-review-queue --issue-type mapping_review --reason same_franchise_tie --limit 10
```

Common drilldowns:

```bash
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --title-cluster "example show"
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --decision needs_manual_match
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --reason same_franchise_tie
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --fix-strategy "needs_manual_match | ambiguous_candidates | same_franchise_tie"
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --cluster-strategy "example show || needs_manual_match | ambiguous_candidates | same_franchise_tie"
```

## Guarded sync planning / apply

```bash
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20 --execute
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute
./scripts/run_exact_approved_sync_cycle.sh
```

Prefer `apply-sync` without `--execute` first unless the task explicitly calls for a live write.

## Recommendations

```bash
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-metadata
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-metadata --include-discovery-targets --discovery-target-limit 50
```

## Maintenance wrapper

```bash
./scripts/run_health_check_cycle.sh
MAL_UPDATER_HEALTH_STALE_HOURS=48 ./scripts/run_health_check_cycle.sh
MAL_UPDATER_HEALTH_STRICT=1 ./scripts/run_health_check_cycle.sh
```

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m unittest tests.test_review_queue_cli -v
PYTHONPATH=src python3 -m unittest tests.test_health_cli -v
```
