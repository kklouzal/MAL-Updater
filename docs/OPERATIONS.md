# Operations

## Local layout

Expected local directories relative to repo root:

- `config/` - non-secret local configuration
- `secrets/` - credentials/tokens only (gitignored)
- `data/` - SQLite database and durable local data (gitignored)
- `state/` - transient state/checkpoints (gitignored)
- `cache/` - fetch/cache scratch space (gitignored)

## Useful commands

### Worker status
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli status
```

### Initialize local DB and directories
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli init
```

### Start MAL auth flow
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
```

### Verify current MAL token
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

### Refresh MAL token
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
```

### Run tests
```bash
cd <repo-root>
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## MAL app settings

Current expected redirect URI for this Orin:
- `http://192.168.1.117:8765/callback`

If the Orin's LAN IP changes, this must be updated both:
- in MAL app registration
- in local `config/settings.toml`

## Secret handling rules

- never commit real tokens or credentials
- keep raw secrets only in `secrets/`
- token files should remain mode `0600`
- do not paste secrets into GitHub issues or docs
