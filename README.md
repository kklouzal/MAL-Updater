# MAL-Updater

MAL-Updater is a **skill-first** OpenClaw repository for conservative Crunchyroll → MyAnimeList sync, mapping review, guarded apply runs, recommendations, and unattended maintenance.

This repository is the skill package.

## Repository contract

- `SKILL.md` at the repo root is the canonical skill entrypoint.
- The Python CLI under `src/mal_updater/` contains the real business logic.
- Runtime state lives **outside** the skill tree under the workspace runtime root `.MAL-Updater/` by default.
- The repo keeps code, docs, scripts, templates, tests, and references bundled so third parties can audit the whole artifact.
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

Use `bootstrap-audit --summary` when you only need a terse onboarding checklist.

## What bootstrap-audit covers

- resolved skill root, workspace root, and runtime root
- runtime path layout
- dependency checks (`python3`, `flock`, `systemctl`, optional `curl_cffi`)
- MAL client id / token presence
- Crunchyroll credentials / staged auth-state presence
- current MAL redirect URI
- whether the repo-owned **user-systemd daemon service** can be installed on this host

## Bootstrap / onboarding flow

1. Run `bootstrap-audit`
2. Create the external runtime dirs and SQLite DB with `init`
3. Create the MyAnimeList app and configure the redirect URI reported by `status`
4. Stage the MAL client id in `.MAL-Updater/secrets/`
5. Run `mal-auth-login` to persist MAL access/refresh tokens
6. Stage Crunchyroll username/password in `.MAL-Updater/secrets/`
7. Run `crunchyroll-auth-login` to mint the staged Crunchyroll refresh token/device id
8. Install the unattended daemon with `scripts/install_user_systemd_units.sh` when the host supports user systemd

See `references/bootstrap-onboarding.md` for the detailed agent-facing flow.

## Core commands

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
PYTHONPATH=src python3 -m mal_updater.cli health-check
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-run-once
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute
PYTHONPATH=src python3 -m mal_updater.cli recommend --limit 20
```

The full command cookbook lives in `references/cli-recipes.md`.

## Automation model

Repo-owned automation/runtime files live under:

- `scripts/install_user_systemd_units.sh`
- `src/mal_updater/service_manager.py`
- `src/mal_updater/service_runtime.py`
- `ops/systemd-user/mal-updater.service`

The installed daemon is a **user-level systemd service** that runs `mal_updater.cli service-run` in the foreground and owns its own internal loop cadence for:

- MAL token refresh
- exact-approved sync passes
- recurring health-check/report generation
- API request logging / budget awareness

## Security / boundaries

- Do not commit real credentials.
- Keep live secrets in `.MAL-Updater/secrets/`.
- Keep generated runtime state out of the repo tree.
- This is a **public GitHub repository**. Any code, docs, examples, tests, commit metadata, or other tracked artifacts that could be uploaded must stay anonymized: no personal identities, personal email addresses, host-specific absolute paths, private workspace paths, real account identifiers, real API keys/tokens, or machine-local secrets.
- Use obviously fake placeholders in tracked examples/tests, and treat history rewrites as acceptable when needed to remove accidentally committed identifying residue.
- Prefer `dry-run-sync` before live `apply-sync --execute` unless a live apply is explicitly intended.
- Treat Crunchyroll auth/fetch instability as real operational residue.

## Testing

```bash
cd <repo-root>
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Issue reporting / feedback

If you encounter problems while using MAL-Updater — whether in the OpenClaw skill surface or the Python back-end daemon/runtime — report them upstream via a GitHub issue at:

- <https://github.com/kklouzal/MAL-Updater/issues>

Use the upstream issue tracker for bug reports, integration problems, unexpected runtime behavior, and feature requests so the maintainer can continue improving both the skill and the back-end.

## References

- Skill entrypoint: `SKILL.md`
- Bootstrap flow: `references/bootstrap-onboarding.md`
- Command cookbook: `references/cli-recipes.md`
- Operations: `docs/OPERATIONS.md`
- Automation: `docs/AUTOMATION.md`
- MAL OAuth details: `docs/MAL_OAUTH.md`
