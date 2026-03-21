# Current Status

## Repository / packaging state

- The repo is treated as the canonical skill package (`SKILL.md` at repo root).
- Runtime state is externalized to `.MAL-Updater/` by default instead of living under the repo tree.
- The bootstrap/onboarding surface starts with `bootstrap-audit`, which now exposes provider readiness, blocking vs non-blocking onboarding counts, and explicit recommended commands for automation-friendly consumers.
- Repo-owned systemd automation is committed as a portable `mal-updater.service` template and rendered at install time by `scripts/install_user_systemd_units.sh`.
- The unattended model is now user-level systemd **daemon-first**, with internal Python control loops rather than timer-driven one-shot jobs.
- Public-repo hygiene is now part of the project contract: tracked code/references/tests/history should remain anonymized and free of personal identifiers, host-local absolute paths, and real secrets.

## Working today

- Python worker/application exists
- SQLite bootstrap + migrations exist
- Crunchyroll auth bootstrap and live snapshot fetch exist
- MAL OAuth and guarded MAL apply exist
- mapping review / queue triage workflows exist
- long-lived daemon runtime + service manager exist
- service-status now exposes recent daemon loop/task state, API-usage snapshots, health snapshot parsing, and log-tail context for unattended debugging, plus a terse `--format summary` operator view for quick checks
- service task state now persists per-task cadence metadata (`every_seconds`), decision timing, last-run start/finish/duration, next-due timing, budget provider labels, and active backoff countdowns so unattended operators can see when a lane should run next vs when it is intentionally cooling down
- request-event logging / budget awareness scaffolding exists
- daemon budget skips now compute a recovery window, persist per-task backoff state, and avoid re-check/log spam until the provider budget has room again
- recommendation generation and metadata refresh exist
- tests remain bundled in the repo for third-party auditing

## Open work

- continue tightening daemon orchestration and request-budget behavior
- continue reducing genuinely ambiguous mapping residue
- continue stabilizing fresh Crunchyroll fetches on hostile/auth-fragile hosts
- continue improving recommendation quality and review UX
- keep encouraging third-party bug reports and feature requests through the authoritative upstream issue tracker: <https://github.com/kklouzal/MAL-Updater/issues>
