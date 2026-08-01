# MAL OAuth

MAL-Updater keeps MAL auth material outside the repo under `.MAL-Updater/secrets/` by default.

Use `status` to see the currently resolved paths and redirect URI.

## Required redirect URI

The redirect URI is derived from the current runtime settings:

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli status
```

Use the reported `mal.redirect_uri` value when creating/configuring the MAL app.

Default fallback:
- `http://127.0.0.1:8765/callback`

The local callback listener binds to loopback by default. If an operator intentionally
sets `mal.bind_host` to a non-loopback address such as `0.0.0.0`, they must also
set `mal.non_loopback_callback_ack = true` (or
`MAL_UPDATER_MAL_NON_LOOPBACK_CALLBACK_ACK=true`); the login prompt will warn
that the temporary listener is reachable beyond the local machine.

## Typical flow

```bash
cd <repo-root>
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

## Secret locations

Default files under `.MAL-Updater/secrets/`:
- `mal_client_id.txt`
- `mal_client_secret.txt` (optional)
- `mal_access_token.txt`
- `mal_refresh_token.txt`

The skill reads these from the resolved runtime secrets dir, not from inside the repo tree.

## Notes

- Keep secrets out of committed config.
- `mal-auth-login` persists tokens locally after the callback exchange.
- `mal-refresh` updates the stored token pair in place.
