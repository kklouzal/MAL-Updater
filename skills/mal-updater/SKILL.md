---
name: mal-updater
description: Operate the local MAL-Updater project for Crunchyroll→MyAnimeList sync, review-queue triage, guarded apply runs, recommendations, and maintenance health checks. Use when working inside the MAL-Updater repo to inspect status, run or verify CLI flows, diagnose sync or fetch issues, manage mapping/review backlog, execute safe approved sync steps, or surface recommendation/health information through OpenClaw.
---

# MAL-Updater

Use the repo-local Python CLI from the project root:

```bash
PYTHONPATH=src python3 -m mal_updater.cli ...
```

## Workflow

1. Start with `status`, `health-check`, or the specific read-only command that answers the request.
2. Prefer read-only inspection before any live write:
   - `list-mappings`
   - `list-review-queue`
   - `dry-run-sync`
   - `recommend`
3. Treat `apply-sync --execute` and live Crunchyroll fetch/auth flows as state-changing operations.
4. When a request involves review backlog triage, use the queue helpers instead of dumping the whole queue first:
   - `list-review-queue --summary`
   - `review-queue-next`
   - `review-queue-worklist`
   - `review-queue-apply-worklist` for bounded bulk resolve/reopen passes across the top ranked buckets
   - `resolve-review-queue` / `reopen-review-queue` for single-slice maintenance once the intended slice is clear
5. When a request is operational/maintenance-focused, use:
   - `health-check`
   - `health-check --format summary` when you only need the terse operator view
   - `scripts/run_health_check_cycle.sh`
6. Re-run targeted tests after code changes:
   - `PYTHONPATH=src python3 -m unittest discover -s tests -v`

## Guardrails

- Keep sync behavior conservative. Do not invent broader write scope than the CLI already supports.
- Prefer `dry-run-sync` before `apply-sync --execute` unless the task explicitly asks for a live apply.
- Treat Crunchyroll auth/fetch instability as real operational residue; document what failed instead of pretending the live path succeeded.
- Do not claim unattended/full automation is finished; the project still has explicit TODOs around fetch stability and richer OpenClaw integration.
- Keep OpenClaw-facing output short and actionable: summarize counts, blockers, next command, and whether a human decision is needed.

## High-value commands

Read `references/cli-recipes.md` for copy/paste command patterns covering:
- repo bootstrap/status/health
- live Crunchyroll fetch + ingest
- mapping review and queue triage
- guarded sync apply
- recommendation checks
- maintenance wrapper runs
- test verification

## OpenClaw wrapper shape

When building or improving OpenClaw integration around this repo, prefer wrappers that expose a few clear operator intents instead of mirroring every raw CLI flag:

- health/status
- fetch/ingest
- mapping review summary
- next review work item / worklist
- guarded dry-run or exact-approved apply
- recommendation summary

Map those intents onto the existing CLI rather than duplicating business logic outside the repo.
