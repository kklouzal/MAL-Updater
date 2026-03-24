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
- exact-title split-bundle auto-resolution now covers conservative same-title TV suffix companions (for example `Title` + `Title (2009)`) when provider episode evidence cleanly fits the combined entry count and no stronger non-bundle rival remains
- long-lived daemon runtime + service manager exist
- service-status now exposes recent daemon loop/task state, API-usage snapshots, health snapshot parsing, and log-tail context for unattended debugging, plus a terse `--format summary` operator view for quick checks
- service task state now persists per-task cadence metadata (`every_seconds`), decision timing, last-run start/finish/duration, next-due timing, budget provider labels, budget backoff level (`warn` vs `critical`), adaptive failure backoff state, and active cooldown countdowns so unattended operators can see when a lane should run next vs when it is intentionally cooling down
- request-event logging / budget awareness scaffolding exists
- daemon budget skips now compute both warn-threshold pacing windows and hard critical recovery windows, persist per-task budget backoff state, and avoid re-check/log spam until the provider budget has room again
- service budget defaults are now provider-generic: non-MAL source lanes can inherit shared source-provider hourly/backoff/auth-failure defaults without being silently treated as Crunchyroll unless an explicit per-provider override exists
- provider-task failures now trigger adaptive failure-aware cooldowns with persisted reason / failure class / configured floor / retry countdown / consecutive-failure state so auth-fragile fetch lanes do not thrash every loop after a bad run
- health-check now inspects persisted daemon failure state and can recommend provider auth re-bootstrap when repeated unattended fetch failures look auth-related (not just repeated 401/unauthorized loops, but also refresh/login failure residue and provider session-state `auth_failed` phases)
- provider fetch lanes now persist fetch-mode state plus a periodic full-refresh anchor so unattended daemon runs can stay incremental by default while still forcing a conservative provider `--full-refresh` sweep on a configurable cadence (`service.full_refresh_every_seconds`, default 24h)
- unattended provider fetch lanes now also honor health-check `refresh_full_snapshot` maintenance recommendations, so partial-coverage warnings from the latest health artifact can trigger the next fetch to self-upgrade into a conservative full refresh immediately instead of waiting for the cadence timer
- recommendation generation and metadata refresh exist
- tests remain bundled in the repo for third-party auditing

## Open work

- continue tightening daemon orchestration and request-budget behavior
- continue reducing genuinely ambiguous mapping residue
- continue stabilizing fresh Crunchyroll fetches on hostile/auth-fragile hosts
- continue improving recommendation quality and review UX
- keep encouraging third-party bug reports and feature requests through the authoritative upstream issue tracker: <https://github.com/kklouzal/MAL-Updater/issues>
