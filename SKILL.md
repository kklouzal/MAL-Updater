---
name: mal-updater
description: Crunchyroll→MyAnimeList sync and maintenance skill for OpenClaw. Use when installing, auditing, bootstrapping, operating, testing, or troubleshooting MAL-Updater from this repository as a skill-first package. Start with bootstrap-audit for new installs, onboarding, dependency checks, runtime layout, credentials, redirect settings, and service setup; use the CLI for status, health checks, review queue triage, guarded sync, recommendations, and maintenance.
metadata: {"openclaw":{"requires":{"bins":["python3","flock"]}}}
---

# MAL-Updater

Treat `{baseDir}` as the skill root. This repository is the skill package.

## Core model

- Keep business logic in the repo-local Python CLI.
- Keep runtime state outside the skill tree under the workspace runtime root `.MAL-Updater/` unless the operator explicitly overrides paths.
- Do not assume host-specific absolute paths, IPs, or preexisting local copies under `~/.openclaw/workspace/skills/`.
- For new installs or portability audits, start with `bootstrap-audit` before doing any live auth or sync work.

## Bootstrap / onboarding workflow

1. `cd {baseDir}`
2. Run `PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit`
3. Read `{baseDir}/references/bootstrap-onboarding.md`
4. Use the audit output to:
   - verify required binaries
   - check whether the optional Crunchyroll transport extra is missing
   - confirm the external runtime layout under `.MAL-Updater/`
   - identify which user-provided secrets/app settings are still missing
   - decide whether repo-owned user systemd timers can be installed on this host
5. If bootstrap is incomplete, guide the user through the missing steps instead of pretending install is finished.

## Operational workflow

Use the repo-local Python CLI from `{baseDir}`:

```bash
PYTHONPATH=src python3 -m mal_updater.cli ...
```

Prefer read-only inspection before live writes:
- `status`
- `bootstrap-audit`
- `health-check`
- `list-mappings`
- `list-review-queue --summary`
- `dry-run-sync`
- `recommend`

Treat these as state-changing:
- `mal-auth-login`
- `mal-refresh`
- `crunchyroll-auth-login`
- `crunchyroll-fetch-snapshot --ingest`
- `apply-sync --execute`
- `scripts/install_user_systemd_units.sh`

## High-value references

- Read `{baseDir}/references/bootstrap-onboarding.md` for install/onboarding/bootstrap expectations.
- Read `{baseDir}/references/cli-recipes.md` for copy/paste command patterns.

## Guardrails

- Keep sync behavior conservative; do not invent broader write scope than the CLI already supports.
- Prefer `dry-run-sync` before `apply-sync --execute` unless the task explicitly asks for a live apply.
- Treat Crunchyroll auth/fetch instability as real residue; document it plainly.
- When a host cannot satisfy the unattended automation path, say so clearly instead of silently skipping system-service setup.
- Keep outputs short and actionable: counts, blockers, next command, and whether user input is needed.
