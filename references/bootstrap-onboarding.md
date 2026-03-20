# Bootstrap / onboarding

Use this when MAL-Updater is being installed, reviewed for portability, or prepared on a new OpenClaw instance.

## Goal

Turn this repo into a working MAL-Updater installation without storing runtime state inside the skill tree.

Default runtime layout lives under the workspace root:
- `.MAL-Updater/config/`
- `.MAL-Updater/secrets/`
- `.MAL-Updater/data/`
- `.MAL-Updater/state/`
- `.MAL-Updater/cache/`

Override only when the operator explicitly wants a different layout.

## First command

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli bootstrap-audit
```

Use `--summary` for terse line-oriented output.

## What bootstrap-audit tells you

- resolved skill root, workspace root, and runtime root
- runtime path layout
- whether `python3`, `flock`, `systemctl`, and optional `curl_cffi` are available
- whether MAL client id, MAL auth tokens, Crunchyroll credentials, and Crunchyroll staged auth state exist
- whether the user-level systemd daemon can be installed
- the current MAL redirect URI that the user must configure in their MAL app

## Required user-facing bootstrap steps

### 1. Dependency check

- Require `python3`
- Require `flock` for the wrapper scripts the daemon still reuses for some guarded lanes
- Treat `curl_cffi` as strongly recommended for live Crunchyroll auth/fetch reliability
- Treat `systemctl` as required only for the unattended user-systemd daemon path

If missing:
- explain what is missing
- remediate if safe and available
- otherwise tell the user exactly what must be installed

### 2. Initialize runtime dirs / DB

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli init
```

This creates the external runtime tree and SQLite database.

### 3. MAL app setup

The user must create a MyAnimeList app and configure the redirect URI reported by:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli status
```

Specifically use `mal.redirect_uri`.

Then store the MAL client id in the runtime secrets dir.

### 4. MAL OAuth bootstrap

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
```

This persists MAL access/refresh tokens under the runtime secrets dir.

### 5. Crunchyroll credentials

Before staging secrets, verify the runtime secrets dir is outside version control and has appropriately restrictive local permissions for the user account.

Stage the user’s Crunchyroll username/password in the runtime secrets dir, then run:

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
```

This creates the long-lived staged Crunchyroll auth state under `.MAL-Updater/state/crunchyroll/<profile>/`.

### 6. Install the unattended daemon if supported

```bash
cd {baseDir}
scripts/install_user_systemd_units.sh
```

That installer renders a host-specific `mal-updater.service` unit from the repo template, preserving repo portability while still producing a valid installed daemon service.

Before enabling unattended operation, manually review the rendered unit behavior, env-file location, and any host-specific implications so the daemon only runs with the scope you actually want.

### 7. Verify daemon health

```bash
cd {baseDir}
PYTHONPATH=src python3 -m mal_updater.cli service-status
PYTHONPATH=src python3 -m mal_updater.cli service-run-once
PYTHONPATH=src python3 -m mal_updater.cli health-check --format summary
```

## Issue reporting / feedback

If a third-party install hits bugs, missing dependencies, confusing bootstrap behavior, daemon/runtime failures, or other rough edges, report them upstream via:

- <https://github.com/kklouzal/MAL-Updater/issues>

That issue tracker is the authoritative place for bug reports and feature requests affecting either the OpenClaw skill experience or the Python back-end.

## Non-goals

- Do not copy runtime state back into the repo.
- Do not claim unattended automation is healthy unless bootstrap-audit/status/service-status/health-check all agree.
- Do not invent an installer outside the documented bootstrap flow.
