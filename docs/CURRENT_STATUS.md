# Current Status

## Repository / packaging state

- The repo is treated as the canonical skill package (`SKILL.md` at repo root).
- Runtime state is externalized to `.MAL-Updater/` by default instead of living under the repo tree.
- The bootstrap/onboarding surface starts with `bootstrap-audit`.
- Repo-owned systemd automation is committed as a portable `mal-updater.service` template and rendered at install time by `scripts/install_user_systemd_units.sh`.
- The unattended model is now user-level systemd **daemon-first**, with internal Python control loops rather than timer-driven one-shot jobs.

## Working today

- Python worker/application exists
- SQLite bootstrap + migrations exist
- Crunchyroll auth bootstrap and live snapshot fetch exist
- MAL OAuth and guarded MAL apply exist
- mapping review / queue triage workflows exist
- long-lived daemon runtime + service manager exist
- request-event logging / budget awareness scaffolding exists
- recommendation generation and metadata refresh exist
- tests remain bundled in the repo for third-party auditing

## Open work

- continue tightening daemon orchestration and request-budget behavior
- continue reducing genuinely ambiguous mapping residue
- continue stabilizing fresh Crunchyroll fetches on hostile/auth-fragile hosts
- continue improving recommendation quality and review UX
