from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from mal_updater.config import load_config, load_mal_secrets


class ConfigLoadingTests(unittest.TestCase):
    def test_defaults_resolve_under_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)

            config = load_config(root)
            secrets = load_mal_secrets(config)

            self.assertEqual(config.settings_path, (root / ".MAL-Updater" / "config" / "settings.toml").resolve())
            self.assertEqual(config.config_dir, (root / ".MAL-Updater" / "config").resolve())
            self.assertEqual(config.secrets_dir, (root / ".MAL-Updater" / "secrets").resolve())
            self.assertEqual(config.data_dir, (root / ".MAL-Updater" / "data").resolve())
            self.assertEqual(config.state_dir, (root / ".MAL-Updater" / "state").resolve())
            self.assertEqual(config.cache_dir, (root / ".MAL-Updater" / "cache").resolve())
            self.assertEqual(config.db_path, (root / ".MAL-Updater" / "data" / "mal_updater.sqlite3").resolve())
            self.assertEqual(config.mal.bind_host, "0.0.0.0")
            self.assertEqual(config.mal.redirect_uri, "http://127.0.0.1:8765/callback")
            self.assertEqual(secrets.client_id_path, (root / ".MAL-Updater" / "secrets" / "mal_client_id.txt").resolve())

    def test_settings_file_overrides_paths_and_secret_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    completion_threshold = 0.95
                    contract_version = "2.0"

                    [paths]
                    config_dir = "./"
                    secrets_dir = "../private"
                    data_dir = "../var/data"
                    state_dir = "../var/state"
                    cache_dir = "../var/cache"
                    db_path = "../var/custom.sqlite3"

                    [mal]
                    bind_host = "127.0.0.1"
                    redirect_host = "127.0.0.50"
                    redirect_port = 9999

                    [secret_files]
                    mal_client_id = "ids/client-id.txt"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            config = load_config(root)
            secrets = load_mal_secrets(config)

            self.assertEqual(config.completion_threshold, 0.95)
            self.assertEqual(config.contract_version, "2.0")
            self.assertEqual(config.config_dir, (root / ".MAL-Updater" / "config").resolve())
            self.assertEqual(config.secrets_dir, (root / ".MAL-Updater" / "private").resolve())
            self.assertEqual(config.data_dir, (root / ".MAL-Updater" / "var" / "data").resolve())
            self.assertEqual(config.state_dir, (root / ".MAL-Updater" / "var" / "state").resolve())
            self.assertEqual(config.cache_dir, (root / ".MAL-Updater" / "var" / "cache").resolve())
            self.assertEqual(config.db_path, (root / ".MAL-Updater" / "var" / "custom.sqlite3").resolve())
            self.assertEqual(config.mal.bind_host, "127.0.0.1")
            self.assertEqual(config.mal.redirect_uri, "http://127.0.0.50:9999/callback")
            self.assertEqual(secrets.client_id_path, (root / ".MAL-Updater" / "private" / "ids" / "client-id.txt").resolve())


if __name__ == "__main__":
    unittest.main()
