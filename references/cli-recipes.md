# CLI recipes

Run commands from the skill root (`{baseDir}` / repo root). In the container, replace `PYTHONPATH=src python3 -m mal_updater.cli` with `docker compose exec -u 10001:10001 mal-updater mal-updater`. `python3 -m mal_updater.cli --help` lists every command.

## Bootstrap / install / audit

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit --summary
PYTHONPATH=src python3 -m mal_updater.cli runtime-retention-audit
PYTHONPATH=src python3 -m mal_updater.cli runtime-retention-audit --format summary
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
scripts/install_user_systemd_units.sh
```

Use `scripts/install_user_systemd_units.sh` as the routine install path. `install-service` remains a lower-level compatibility/repair command; do not run both install paths during normal bootstrap.

`service-run-once` is intentionally excluded from routine/read-only bootstrap checks. It can run due provider fetch, local ingest, and exact-approved MAL apply lanes; use it only as an opt-in manual daemon pass after explicit operator intent.

`runtime-retention-audit` is read-only and local-only; see `references/OPERATIONS.md` for what it checks.

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
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-link
PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider hidive
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --full-refresh
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider hidive --out .MAL-Updater/cache/live-hidive-snapshot.json --full-refresh
PYTHONPATH=src python3 -m mal_updater.cli backfill-hidive-series-urls --format summary
# Apply only after reviewing the dry-run output above:
PYTHONPATH=src python3 -m mal_updater.cli backfill-hidive-series-urls --apply --format summary
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider all --format summary
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider all --older-than-days 30 --format summary
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider crunchyroll
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider crunchyroll --format summary
PYTHONPATH=src python3 -m mal_updater.cli provider-stale-rows --provider hidive --cutoff "2026-04-25 17:59:00"
```

`provider-auth-login` performs live provider login/bootstrap and writes local provider token/session state; `crunchyroll-link` does the same without a password through a TV activation code approved at crunchyroll.com/activate. `provider-fetch-snapshot` performs live provider reads and writes local snapshot/sync-boundary state; `--ingest` also mutates the local SQLite provider cache. Crunchyroll page chunk controls (`--max-history-pages`, `--max-watchlist-pages`, `--history-start-page`, `--watchlist-start`) are Crunchyroll-only; HIDIVE surfaces, paging and health rules are in `references/hidive-provider-recon.md`.

`backfill-hidive-series-urls` is a local SQLite maintenance helper for old HIDIVE title-search/dashboard links that used `/season/{id-or-slug}` for generic `VOD_SERIES` hits. It defaults to a read-only dry run and is idempotent; `--apply` rewrites only eligible local `provider_series.raw_json.url`, `recommendation_provider_eligibility_evidence.provider_url`, provider-title-search `matches_json[].url`, and HIDIVE provider-object `provider_url` values inside `recommendation_score_snapshots.context_json` (`provider_eligibility_evidence` / `available_provider_series`) to `https://www.hidive.com/series/{series_id}` when a `provider_series_id` is present. It does not call HIDIVE or MAL, and JSON/summary output includes matched, updated, and returned-sample counts for each covered row family.

`provider-stale-rows` is read-only. JSON and summary output include per-family stale counts, oldest/newest `last_seen_at` bounds, exact oldest/newest age-in-days ranges, coarse age-bucket counts (`recent_0_7_days`, `older_8_30_days`, `older_31_plus_days`), and linkage counts for stale progress/watchlist rows (`with_stale_series`, `with_current_series`, `with_missing_series`); JSON sample rows also include exact `age_days`, and child progress/watchlist samples include `linked_series_posture` plus `linked_series_last_seen_at`, so operators can judge concrete residue examples and dependency shape before deciding whether to leave, archive, or prune rows later. The payload and terse summary also include a policy-neutral `retention_review` posture (`recent_residue_observe`, `aging_residue_observe`, `current_series_child_residue`, or `manual_retention_policy_candidate`) with review-candidate and next-step fields; this is diagnostic guidance only and still preserves `diagnostic_only_no_archive_or_prune`. Add `--older-than-days N` when you only want rows that are stale since the cutoff and have also been absent for at least `N` days.

Crunchyroll-specific aliases of the generic commands:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out .MAL-Updater/cache/live-crunchyroll-snapshot.json --ingest
```

## Review queue triage

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 0 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --summary --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-next --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli review-queue-worklist --issue-type mapping_review --limit 5
PYTHONPATH=src python3 -m mal_updater.cli review-queue-apply-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 10
PYTHONPATH=src python3 -m mal_updater.cli review-queue-refresh-worklist --issue-type mapping_review --limit 3 --per-bucket-limit 10
```

## Sync / apply

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider all --limit 0 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider all --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider hidive --limit 20
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider crunchyroll --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20 --execute
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 8 --exact-approved-only --execute
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 8 --exact-approved-only --provider hidive --execute
PYTHONPATH=src python3 -m mal_updater.cli exact-approved-sync-cycle
PYTHONPATH=src python3 -m mal_updater.cli exact-approved-sync-cycle --full-refresh
PYTHONPATH=src python3 -m mal_updater.cli season-mappings --unresolved
PYTHONPATH=src python3 -m mal_updater.cli season-mappings --approve --provider crunchyroll --provider-series-id <id> --provider-season-key <key> --mal-anime-id <mal-id>
```

`apply-sync` writes only with `--execute`; `--limit` bounds live writes (0 = unbounded) and the rest is reported as `deferred`. `exact-approved-sync-cycle` skips the apply when any provider fetch fails unless `--allow-stale-provider-apply` is given. Unattended apply is off until `service.task_execute_limits.sync_apply` is positive. Semantics: `references/OPERATIONS.md`.

## Recommendations

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20 --flat --include-dormant
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-metadata
PYTHONPATH=src python3 -m mal_updater.cli recommend-refresh-metadata --include-discovery-targets --discovery-target-limit 50
PYTHONPATH=src python3 -m mal_updater.cli push-recommendations-webhook --limit 20
PYTHONPATH=src python3 -m mal_updater.cli push-recommendations-webhook --limit 20 --delivery-mode digest
PYTHONPATH=src python3 -m mal_updater.cli push-recommendations-webhook --limit 20 --dry-run
PYTHONPATH=src python3 -m mal_updater.cli recommend-snapshots --limit 16 --format summary
PYTHONPATH=src python3 -m mal_updater.cli dashboard-serve
PYTHONPATH=src python3 -m mal_updater.cli dashboard-serve --limit 16
# optional daemon lane: set service.recommendations_webhook_push_every_seconds > 0, keep openclaw webhook settings populated, and leave delivery_mode on `fresh` unless you intentionally want noisier unattended posts
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
```

Without `--include-dormant`, discovery requires verified Crunchyroll/HIDIVE availability (in any audio language; each row lists `context.audio_locales`). With no source provider connected (MAL-only), that proof cannot exist, so the default ranks from the MAL list and MAL recommendation graph with availability marked unknown, exactly as `--include-dormant` does.

`dashboard-serve` is local/read-only (default `127.0.0.1:8766`) and serves the latest persisted recommendation snapshot with the same pages as the container (`/`, `/debug`) and JSON at `/api/dashboard` (`?limit=N` per-request override, default 120 rows per section; `?view=ui` is the slim payload the pages use).

## Tests

See `references/QUALITY.md`.
