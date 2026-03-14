from __future__ import annotations

import stat
import tempfile
import textwrap
import unittest
from pathlib import Path

from mal_updater.auth import format_auth_flow_prompt, persist_token_response, write_secret_file
from mal_updater.config import load_config, load_mal_secrets
from mal_updater.mal_client import TokenResponse


class ConfigTests(unittest.TestCase):
    def test_load_config_reads_bind_host_and_redirect_host_from_settings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    [mal]
                    bind_host = "192.168.1.50"
                    redirect_host = "animebox.local"
                    redirect_port = 9999
                    """
                ),
                encoding="utf-8",
            )

            config = load_config(root)

            self.assertEqual(config.mal.bind_host, "192.168.1.50")
            self.assertEqual(config.mal.redirect_host, "animebox.local")
            self.assertEqual(config.mal.redirect_uri, "http://animebox.local:9999/callback")

    def test_load_mal_secrets_reads_secret_file_overrides_from_settings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "secrets").mkdir()
            (root / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    [secret_files]
                    mal_client_id = "../secrets/custom_client_id.txt"
                    """
                ),
                encoding="utf-8",
            )
            (root / "secrets" / "custom_client_id.txt").write_text("client-123\n", encoding="utf-8")

            secrets = load_mal_secrets(load_config(root))

            self.assertEqual(secrets.client_id, "client-123")
            self.assertEqual(secrets.client_id_path, (root / "secrets" / "custom_client_id.txt").resolve())


class AuthHelperTests(unittest.TestCase):
    def test_format_auth_flow_prompt_includes_bind_host_and_redirect_uri(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    [mal]
                    bind_host = "0.0.0.0"
                    redirect_host = "192.168.1.117"
                    redirect_port = 8765
                    """
                ),
                encoding="utf-8",
            )
            config = load_config(root)

            prompt = format_auth_flow_prompt(config, "https://example.test/auth", 300)

            self.assertIn("bind_host=0.0.0.0", prompt)
            self.assertIn("redirect_uri=http://192.168.1.117:8765/callback", prompt)
            self.assertIn("https://example.test/auth", prompt)

    def test_write_secret_file_sets_0600_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "secret.txt"

            write_secret_file(path, "top-secret")

            self.assertEqual(path.read_text(encoding="utf-8"), "top-secret\n")
            mode = stat.S_IMODE(path.stat().st_mode)
            self.assertEqual(mode, 0o600)

    def test_persist_token_response_writes_access_and_refresh_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            secrets = load_mal_secrets(config)
            token = TokenResponse(
                access_token="access-abc",
                token_type="Bearer",
                expires_in=3600,
                refresh_token="refresh-xyz",
                scope=None,
                raw={},
            )

            persisted = persist_token_response(token, secrets)

            self.assertEqual(persisted.access_token_path.read_text(encoding="utf-8"), "access-abc\n")
            self.assertEqual(persisted.refresh_token_path.read_text(encoding="utf-8"), "refresh-xyz\n")


if __name__ == "__main__":
    unittest.main()
