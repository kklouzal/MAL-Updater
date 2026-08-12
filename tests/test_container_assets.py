from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ContainerAssetTests(unittest.TestCase):
    def test_dockerfile_production_invariants(self) -> None:
        text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn(" AS builder", text)
        self.assertIn(" AS runtime", text)
        self.assertIn("python:3.13.7-slim-bookworm@sha256:", text)
        self.assertIn("pip install -c ci.txt", text)
        self.assertIn("tini=0.19.0-1", text)
        self.assertIn('ENTRYPOINT ["/usr/bin/tini"', text)
        self.assertIn('VOLUME ["/data"]', text)
        self.assertIn("EXPOSE 8080", text)

    def test_compose_safe_defaults_and_hardening(self) -> None:
        text = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        self.assertNotIn("MAL_UPDATER_CONTAINER_ENABLE_DAEMON", text)
        self.assertIn('${MAL_UPDATER_HTTP_PORT:-80}:8080', text)
        self.assertIn("read_only: true", text)
        self.assertIn("no-new-privileges:true", text)
        self.assertIn("/healthz", text)
        self.assertIn('profiles: ["tools"]', text)
        self.assertIn("ghcr.io/kklouzal/mal-updater:0.2.9", text)
        self.assertEqual(2, len(re.findall(r"\bKILL\b", text)))
        self.assertIn("- KILL\n      - SETGID", text)
        self.assertIn('["CHOWN", "DAC_OVERRIDE", "KILL", "SETGID", "SETUID"]', text)
        self.assertEqual(2, text.count("cap_drop"))
        self.assertEqual(2, text.count("ALL"))
        self.assertNotIn("first_run_setup_token", text)
        self.assertNotIn("container_auth.json", text)

    def test_build_context_excludes_runtime_and_secrets(self) -> None:
        ignored = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        self.assertIn(".MAL-Updater", ignored)
        self.assertIn(".env", ignored)
        self.assertIn(".env.*", ignored)
        self.assertIn("tests", ignored)
        self.assertIn(".env.*", ignored)


if __name__ == "__main__":
    unittest.main()
