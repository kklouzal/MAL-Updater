# MAL-Updater

MAL-Updater is a **skill-first** OpenClaw repository for conservative **multi-provider anime → MyAnimeList sync and recommendations**, with mapping review, guarded apply runs, and unattended maintenance. Current source providers: **Crunchyroll** and **HIDIVE**.

This repository is the skill package.

## Repository contract

- `SKILL.md` at the repo root is the canonical skill entrypoint.
- The Python CLI under `src/mal_updater/` contains the real business logic.
- Runtime state lives **outside** the skill tree under the workspace runtime root `.MAL-Updater/` by default.
- The repo keeps code, references, scripts, templates, tests, and supporting artifacts bundled so third parties can audit the whole artifact.
- Background work now centers on a **long-lived user-level systemd daemon**, not user timers or OpenClaw cron.

## Default runtime layout

MAL-Updater externalizes runtime state to the workspace root:

- `.MAL-Updater/config/`
- `.MAL-Updater/secrets/`
- `.MAL-Updater/data/`
- `.MAL-Updater/state/`
- `.MAL-Updater/cache/`

Override paths only when the operator explicitly wants a different layout.

## First commands on a new install

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli status
```

Use `bootstrap-audit --summary` when you only need a terse onboarding checklist. The default JSON now also includes provider readiness, provider-specific operation-mode guidance/next-command hints, runtime-initialization readiness, daemon install/drift readiness, explicit manual-vs-daemon operation expectations, provider-intent/partial-bootstrap counts, secrets-dir permission posture, blocking/non-blocking onboarding counts, and explicit recommended commands for automation-friendly consumers. Those onboarding/recommended-command entries now preserve stable `reason_code`, `automation_safe`, and `requires_auth_interaction` metadata too, and auth-driven reauth/rebootstrap steps carry `auth_failure_kind` / `auth_remediation_kind` so bootstrap automation does not have to reverse-engineer the reason from freeform detail text. Staged auth is now treated conservatively there across both MAL and source providers: if repeated unattended `mal_refresh` failures or provider session residue/fetch failures already suggest auth degradation, bootstrap-audit downgrades that lane from healthy/ready posture to explicit re-auth or re-bootstrap guidance instead of trusting token-file presence alone. Those auth-degradation surfaces now also classify the residue into more specific operator-readable buckets such as revoked/invalid token, missing refresh material, malformed token payload, or generic session/login auth failure so the next step is better justified, and `bootstrap-audit --summary` surfaces the same terse auth-failure/remediation kinds for quick operator triage.

## What bootstrap-audit covers

- resolved skill root, workspace root, and runtime root
- runtime path layout
- dependency checks (`python3`, `systemctl`, optional provider/runtime extras)
- MAL client id / token presence, including repeated unattended MAL token-refresh failures that already imply the staged refresh material needs a fresh `mal-auth-login`
- Crunchyroll credentials / staged auth-state presence
- HIDIVE credentials / staged auth-state presence
- staged provider auth degradation signals from provider session residue or repeated unattended auth-style failures, so bootstrap can recommend re-bootstrap before the daemon is treated as healthy
- current MAL redirect URI
- whether the repo-owned **user-systemd daemon service** can be installed on this host
- whether the repo-owned user-systemd daemon is missing, outdated, disabled, inactive, or missing its rendered env file for this user
- whether manual foreground CLI operation is merely acceptable during bootstrap/spot checks or the daemon is now the expected unattended path

## Bootstrap / onboarding flow

1. Run `bootstrap-audit`
2. Create the external runtime dirs and SQLite DB with `init`
3. Create the MyAnimeList app and configure the redirect URI reported by `status`
4. Stage the MAL client id in `.MAL-Updater/secrets/`
5. Run `mal-auth-login` to persist MAL access/refresh tokens
6. For each source provider you want enabled, stage that provider's credentials in `.MAL-Updater/secrets/`
7. Run the provider bootstrap command at the point the audit/onboarding flow says that provider is ready:
   - Crunchyroll: `provider-auth-login --provider crunchyroll` (or the compatibility wrapper `crunchyroll-auth-login`)
   - HIDIVE: `provider-auth-login --provider hidive`
8. Install the unattended daemon with `scripts/install_user_systemd_units.sh` when the host supports user systemd

Normal unattended operation now assumes **all credentialed providers stay enabled** and are swept by separate background fetch lanes before aggregate MAL planning/apply runs.

See `references/bootstrap-onboarding.md` for the detailed agent-facing flow.

## Core commands

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli health-check-cycle
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-status --format summary
PYTHONPATH=src python3 -m mal_updater.cli service-run-once
PYTHONPATH=src python3 -m mal_updater.cli exact-approved-sync-cycle
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --provider all
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --provider all --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 8 --exact-approved-only --execute
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20
```

Grouped recommendation output now includes per-section provider metadata (`providers`, `provider_counts`, `provider_label`, `mixed_providers`, `multi_provider_item_count`) so operators can see when a section mixes Crunchyroll/HIDIVE/MAL-derived items or contains merged cross-provider availability. Individual recommendation items now also expose effective availability fields (`providers`, `provider_count`, `multi_provider`, `provider_label`) rather than only the surviving primary provider. Equivalent mapped continue/episode recommendations that appear from multiple source providers are also merged conservatively into one primary item with alternate-provider context instead of emitting duplicate operator alerts. Global recommendation ranking now also uses that effective availability as a tie-break before applying `--limit`, and merged cross-provider continue/episode items now get a small raw-priority bonus plus explicit reason/context metadata for that broader availability, so consensus availability can matter slightly even before the tie-break without overwhelming the existing scoring model. Discovery-candidate scoring now also surfaces support-balance evidence (`best_single_source_votes`, `cross_seed_support_votes`, `support_balance_bonus`) so bursty one-seed recommendation spikes do not crowd out steadier cross-seed consensus when totals are otherwise close, and it now adds modest freshness / seed-activity evidence too (`start_season`, `start_season_label`, `freshness_bucket`, `freshness_bonus`, `recent_seed_activity_bonus`, `freshest_supporting_seed_days`) so tied discovery candidates prefer newer catalog entries and recommendations backed by more recently active watch history rather than relying almost entirely on aggregate votes + overlap metadata. Discovery ranking now also carries small explainable seed-quality calibration (`seed_quality_bonus`, `supporting_seed_scores`, `best_supporting_seed_score`) so support from highly scored or more deeply completed seed titles can break near-ties without overpowering vote totals, overlap metadata, or broader consensus, and it now also records the symmetric low-confidence side (`seed_quality_penalty`, `penalized_seed_scores`, `lowest_supporting_seed_score`) so discovery candidates supported mainly by poorly scored/disliked seeds get tempered slightly instead of reading as equally strong taste matches. Candidates backed only by explicitly dropped or otherwise disliked-only seed titles are now filtered out entirely instead of being shown with a cosmetic penalty, mixed-signal candidates now also discount negative supporting seeds from their support-count boost while recording `negative_supporting_seed_ids`, `negative_support_ratio`, `effective_supporting_seed_count`, and `mixed_signal_penalty`, and explicitly neutral MAL seed scores now count more conservatively via `neutral_supporting_seed_ids`, `neutral_support_ratio`, and `neutral_support_penalty` so neutral-heavy support no longer looks like strong positive consensus. Metadata refresh target selection follows that same recommendation ordering.

`health-check-cycle` now accepts the same review/maintenance tuning knobs as `health-check` (`--review-issue-type`, `--review-worklist-limit`, `--mapping-coverage-threshold`, `--maintenance-review-limit`) so unattended/manual cycle runs can reuse the same backlog and coverage policy instead of silently falling back to defaults.

The full command cookbook lives in `references/cli-recipes.md`.

Review-queue backlog triage surfaces are now provider-aware across Crunchyroll and HIDIVE as well, so `health-check`, `review-queue-next`, `review-queue-worklist`, `list-review-queue`, and related queue filters/summary labels resolve series titles from the matching provider instead of silently assuming Crunchyroll-only catalog rows. `list-mappings` now follows that same multi-provider posture: it lists all persisted mappings by default and can optionally be narrowed with `--provider crunchyroll` or `--provider hidive`.

## Automation model

Repo-owned automation/runtime files live under:

- `scripts/install_user_systemd_units.sh`
- `src/mal_updater/service_manager.py`
- `src/mal_updater/service_runtime.py`
- `ops/systemd-user/mal-updater.service`

The installed daemon is a **user-level systemd service** that runs `mal_updater.cli service-run` in the foreground and owns its own internal loop cadence for:

`service-status` / `service-status --format summary` now surface persisted per-task cadence, decision timing, last-run start/finish/duration, next-due timing, budget-provider labels, budget backoff level (`warn` vs `critical`), adaptive failure-backoff state (reason / class / floor / countdown / consecutive failures), active cooldown countdowns, and whether a provider-specific cooldown floor extended the wait so unattended behavior is inspectable without reading raw state files. Service budget defaults are now also provider-generic: new source providers can inherit shared source-provider hourly/backoff defaults without being implicitly treated as Crunchyroll unless you add a provider-specific override. The repo now also ships opinionated provider defaults for the currently supported source providers, so Crunchyroll gets a deeper learned-history / conservative percentile posture by default and HIDIVE now also ships a conservative learned p90 percentile baseline alongside its quieter hourly/backoff/auth-failure limits instead of relying only on hard-coded request counts. The daemon can now also honor optional task-specific budget overrides (for example a stricter `sync_apply` MAL lane) while still falling back to provider/shared defaults when no per-task policy is configured, and the repo now seeds conservative built-in task defaults for unattended MAL lanes: `sync_apply` ships hourly budget / projected request cost / learned-history depth / conservative learned p90 percentile / cooldown floors plus a bounded unattended execution posture via `service.task_execute_limits`, while `mal_refresh` now also ships a small explicit cold-start projected request seed plus a shallow learned-history window so new installs do not treat token refresh as a zero-cost mystery lane. Unattended `sync_apply` no longer attempts a full aggregate pass by default; it advances MAL in bounded exact-approved batches so one oversized historical run is less likely to poison the lane. Budgeting is now also modestly projection-aware: each budgeted lane can take an explicit `service.task_projected_request_counts` override, a fetch-mode-specific `service.task_projected_request_counts_by_mode` override for expensive paths like full refreshes, or learn from short rolling observed request-delta history (including fetch-mode-specific incremental vs full-refresh history), then warn/skip before a run that would likely push the provider over its configured hourly threshold instead of only reacting after the raw count is already too high. Learned projections are now tunable per task and per provider, so burstier providers can keep deeper history and/or conservative percentile baselines by default while specific lanes still override that policy when needed. When neither task nor provider percentile is configured, the daemon still auto-switches bursty lanes onto a conservative learned p90 baseline once the observed request history shows clear spikes, so unattended budgeting reacts earlier without forcing operators to pre-tune every lane. The repo now also ships built-in mode-specific projection defaults for both Crunchyroll (`sync_fetch_crunchyroll`: `4` incremental / `55` full refresh) and HIDIVE (`sync_fetch_hidive`: `4` incremental / `71` full refresh) so fresh unattended installs do not treat ordinary fetches or heavier cold-start resweeps like unknown zero-cost paths; those shipped defaults now behave as cold-start seeds rather than permanent hard overrides, so learned request history can take over once the daemon has real evidence. When a lane's execution posture changes materially (for example bounded unattended apply replacing an old full-pass posture), the daemon now resets stale projected-request/backoff state for that lane so old pathological history does not keep poisoning future unattended decisions. If an overdue full refresh is budget-blocked the daemon also now degrades gracefully to an incremental fetch instead of starving the lane completely until the heavier run fits.

- MAL token refresh
- one fetch lane per credentialed source provider (currently Crunchyroll + HIDIVE)
- one shared aggregate MAL apply lane using bounded exact-approved batches by default
- recurring health-check/report generation
- API request logging / budget awareness
- periodic provider full-refresh cadence (`service.full_refresh_every_seconds`, default 24h) so unattended runs can remain incremental by default while still forcing occasional conservative resweeps; failed attempted full refreshes do not advance the success markers/anchor, so the overdue canonical resweep stays due until one actually succeeds
- health-driven full-refresh escalation: if the latest health-check artifact recommends `refresh_full_snapshot` for a provider because cached coverage is only partial, the next unattended fetch lane for that provider automatically upgrades itself to `--full-refresh` instead of waiting for the periodic cadence window
- broader auth-fragility health escalation: health-check now treats repeated unattended provider failures as auth-related not just for obvious `401`/`unauthorized` residue, but also for refresh/login failure text and provider session-state `auth_failed` residue so re-bootstrap guidance triggers sooner on brittle hosts

## Security / boundaries

- Do not commit real credentials.
- Keep live secrets in `.MAL-Updater/secrets/`.
- Restrict secrets-dir permissions appropriately for the local user before staging long-lived credentials or tokens there.
- Keep generated runtime state out of the repo tree.
- This is a **public GitHub repository**. Any code, references, examples, tests, commit metadata, or other tracked artifacts that could be uploaded must stay anonymized: no personal identities, personal email addresses, host-specific absolute paths, private workspace paths, real account identifiers, real API keys/tokens, or machine-local secrets.
- Use obviously fake placeholders in tracked examples/tests, and treat history rewrites as acceptable when needed to remove accidentally committed identifying residue.
- Prefer `dry-run-sync` before live `apply-sync --execute` unless a live apply is explicitly intended.
- Treat Crunchyroll auth/fetch instability as real operational residue.
- Manually review the rendered user-systemd daemon/unit behavior before enabling unattended operation on a host you care about.

## License / attribution

This project is released under the **MIT License**. You can use, modify, and redistribute it freely as long as the license/copyright notice is preserved.

If you reuse or adapt MAL-Updater, attribution to the original project/repo is appreciated:
- <https://github.com/kklouzal/MAL-Updater>

## Testing

The repo now bootstraps its own local import paths for test runs, so root-level validation works without extra environment setup.

```bash
cd <repo-root>
pytest -q
python3 -m unittest discover -s tests -v
```

## Issue reporting / feedback

If you encounter problems while using MAL-Updater — whether in the OpenClaw skill surface or the Python back-end daemon/runtime — report them upstream via a GitHub issue at:

- <https://github.com/kklouzal/MAL-Updater/issues>

Use the upstream issue tracker for bug reports, integration problems, unexpected runtime behavior, and feature requests so the maintainer can continue improving both the skill and the back-end.

## References

- Skill entrypoint: `SKILL.md`
- Bootstrap flow: `references/bootstrap-onboarding.md`
- Command cookbook: `references/cli-recipes.md`
- Operations: `references/OPERATIONS.md`
- Automation: `references/AUTOMATION.md`
- MAL OAuth details: `references/MAL_OAUTH.md`
