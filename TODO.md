# TODO

## Packaging / bootstrap

- [x] Treat the repo root as the canonical skill package
- [x] Externalize default runtime state to `.MAL-Updater/`
- [x] Add `bootstrap-audit` as the first install/onboarding readiness check
- [x] Replace committed absolute-path systemd units with install-time rendered templates
- [x] Replace timer-first unattended automation with a user-level daemon-first model
- [x] Establish public-repo anonymization as an explicit project constraint for tracked code/references/tests/history
- [ ] Keep tightening the bootstrap/onboarding UX for new OpenClaw installs
- [ ] Consider whether bootstrap/install metadata should become more machine-readable beyond the current CLI audit surface

## Daemon / operations

- [ ] Tighten daemon control loops and per-lane state tracking
- [ ] Improve request-budget accounting / backoff behavior for MAL and Crunchyroll
- [ ] Decide whether to retire the remaining transitional wrapper scripts after more daemon logic moves in-process
- [x] Add richer service-state/observability surfaces for debugging unattended failures
- [x] Add `service-status --format summary` as a terse/operator-summary mode alongside the rich JSON surface

## Product / sync quality

- [ ] Continue reducing genuinely ambiguous mapping residue
- [ ] Continue stabilizing fresh Crunchyroll fetches on auth-fragile hosts
- [ ] Keep improving recommendation quality and review UX
- [ ] Keep the upstream GitHub issue tracker active as the canonical channel for third-party bug reports and feature requests
