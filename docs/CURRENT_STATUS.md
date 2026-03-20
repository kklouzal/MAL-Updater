# Current Status

## Repository / packaging state

- The repo is now treated as the canonical skill package (`SKILL.md` at repo root).
- Runtime state is externalized to `.MAL-Updater/` by default instead of living under the repo tree.
- The bootstrap/onboarding surface now starts with `bootstrap-audit`.
- Repo-owned systemd units are committed as portable templates and rendered at install time by `scripts/install_user_systemd_units.sh`.

## Working today

- Python worker/application exists
- SQLite bootstrap + migrations exist
- Crunchyroll auth bootstrap and live snapshot fetch exist
- MAL OAuth and guarded MAL apply exist
- mapping review / queue triage workflows exist
- health-check and unattended wrapper scripts exist
- recommendation generation and metadata refresh exist
- tests remain bundled in the repo for third-party auditing

## Open work

- continue tightening bootstrap/onboarding ergonomics
- continue reducing genuinely ambiguous mapping residue
- continue stabilizing fresh Crunchyroll fetches on hostile/auth-fragile hosts
- continue improving recommendation quality and review UX
