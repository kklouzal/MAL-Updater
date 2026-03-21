# TODO

## Packaging / bootstrap

- [x] Treat the repo root as the canonical skill package
- [x] Externalize default runtime state to `.MAL-Updater/`
- [x] Add `bootstrap-audit` as the first install/onboarding readiness check
- [x] Replace committed absolute-path systemd units with install-time rendered templates
- [x] Replace timer-first unattended automation with a user-level daemon-first model
- [x] Establish public-repo anonymization as an explicit project constraint for tracked code/references/tests/history
- [ ] Keep tightening the bootstrap/onboarding UX for new OpenClaw installs
- [x] Make bootstrap/install metadata more machine-readable with provider readiness and explicit recommended commands in `bootstrap-audit`

## Daemon / operations

- [ ] Tighten daemon control loops and per-lane state tracking (cadence / decision timing / last-run start-finish-duration / next-due / active-backoff observability now persists in service state, including warn-vs-critical backoff level; next likely step is provider-specific cooldown heuristics)
- [ ] Continue refining request-budget accounting / backoff behavior for MAL and Crunchyroll (warn-threshold pacing + recovery-window backoff now exist; next likely step is provider-specific cooldown heuristics and smarter lane-specific budgeting)
- [ ] Decide whether to retire the remaining transitional wrapper scripts after more daemon logic moves in-process
- [x] Add richer service-state/observability surfaces for debugging unattended failures
- [x] Add `service-status --format summary` as a terse/operator-summary mode alongside the rich JSON surface

## Product / sync quality

- [ ] Continue reducing genuinely ambiguous mapping residue
- [ ] Continue stabilizing fresh Crunchyroll fetches on auth-fragile hosts
- [ ] Keep improving recommendation quality and review UX
- [ ] Keep the upstream GitHub issue tracker active as the canonical channel for third-party bug reports and feature requests
