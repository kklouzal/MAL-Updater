# Python worker notes

## What exists now

- directory/bootstrap handling
- TOML settings loading from `config/settings.toml`
- environment override support
- secret file conventions under `secrets/`
- SQLite bootstrap helper
- MAL API client scaffold for:
  - PKCE generation
  - authorization URL building
  - token exchange
  - refresh flow
  - `GET /users/@me`

## Important boundary

The MAL client scaffold is **real transport/config structure**, but the project still does **not** claim end-to-end sync is finished.
Nothing here fakes Crunchyroll ingestion or live MAL writes.

## Config precedence

1. explicit environment variables
2. local `config/settings.toml`
3. code defaults

## DB bootstrap path

Use:

```python
from mal_updater.db import bootstrap_database
```

This is the clean entrypoint used by the CLI and should stay the main schema-init path unless migrations become more sophisticated.

## Near-term next steps

- callback listener for OAuth code capture
- token persistence helper with safe file permissions
- JSON schema validation on adapter snapshots
- provider snapshot ingestion into SQLite
