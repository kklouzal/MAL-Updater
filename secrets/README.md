# Secrets directory

Put local-only credentials here. Nothing in this directory should be committed.

Suggested files:

- `mal_client_id.txt`
- `mal_client_secret.txt` (optional; used if present)
- `mal_access_token.txt`
- `mal_refresh_token.txt`
- `crunchyroll_username.txt`
- `crunchyroll_password.txt`

Current Python auth helpers write MAL token files atomically and chmod them to `0600`.
The Crunchyroll credential bootstrap also reads local-only username/password files from here.
Keep permissions tight on this directory.
