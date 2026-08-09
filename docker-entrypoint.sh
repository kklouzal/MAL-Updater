#!/bin/sh
set -eu

umask "${MAL_UPDATER_UMASK:-077}"

if [ "$(id -u)" = 0 ]; then
  uid="${MAL_UPDATER_UID:-10001}"
  gid="${MAL_UPDATER_GID:-10001}"
  case "$uid:$gid" in *[!0-9:]*|:*) echo "MAL_UPDATER_UID/GID must be numeric" >&2; exit 64;; esac
  install -d -o "$uid" -g "$gid" -m 0700 /data /data/config /data/secrets /data/data /data/state /data/cache
  exec setpriv --reuid="$uid" --regid="$gid" --clear-groups -- "$@"
fi

exec "$@"
