# Architecture

## Shape

MAL-Updater is a **skill-first repository**:

- `SKILL.md` at the repo root is the canonical skill entrypoint
- `src/mal_updater/` contains the Python application and CLI
- `references/` contains agent-facing bootstrap/operation references
- `scripts/` contains deterministic wrappers/install helpers
- `ops/systemd-user/` contains portable systemd templates
- `tests/` remains bundled for auditability

## Runtime boundary

Live runtime state is external to the repo tree by default and resolves under `.MAL-Updater/` in the workspace:

- `.MAL-Updater/config/`
- `.MAL-Updater/secrets/`
- `.MAL-Updater/data/`
- `.MAL-Updater/state/`
- `.MAL-Updater/cache/`

## Execution model

- Use the repo-local Python CLI for business logic.
- Use `bootstrap-audit` as the first install/onboarding compatibility check.
- Use the wrapper scripts for unattended exact-approved sync and health-check cycles.
- Render host-specific systemd units from repo templates at install time; do not commit absolute host paths.

## Bootstrap model

A new OpenClaw instance should be able to:

1. inspect this repo as a skill package
2. run `bootstrap-audit`
3. detect missing dependencies and user-provided inputs
4. guide the user through MAL app creation / redirect configuration
5. stage secrets into the external runtime tree
6. install rendered long-lived services/timers when supported

## Guardrails

- keep sync conservative
- keep runtime out of the skill tree
- avoid host-specific assumptions in committed files
- preserve full-repo auditability for third parties
