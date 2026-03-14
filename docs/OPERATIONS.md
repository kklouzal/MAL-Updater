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
cd rust/crunchyroll_adapter && cargo build
```

### Inspect MAL mapping candidates
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli map-series --limit 20 --mapping-limit 5
```

### Build a mapping review queue and preserve approved mappings
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --approved-only
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-123 16498 --confidence 0.995 --notes "manual approval"
```

### Generate guarded read-only sync proposals
```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
```

Use the stdlib `unittest` invocation above as the baseline Python test command. Do not assume `pytest` is installed on the host.

### Inspect Crunchyroll adapter auth-material state
```bash
cd <repo-root>/rust/crunchyroll_adapter
cargo run -- auth status
```

### Stage a Crunchyroll refresh token for the adapter
```bash
cd <repo-root>/rust/crunchyroll_adapter
cargo run -- auth save-refresh-token --refresh-token-file /path/to/refresh_token.txt
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
