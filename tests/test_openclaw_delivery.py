from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main as cli_main
from mal_updater.config import ensure_directories, load_config
from mal_updater.openclaw_delivery import build_recommendation_delivery_payload, deliver_recommendations_via_openclaw


class _Response:
    def __init__(self, status: int = 200, body: str = '{"ok":true}') -> None:
        self.status = status
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class OpenClawDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True)
        (self.project_root / ".MAL-Updater" / "config" / "settings.toml").write_text(
            """
[openclaw]
recommendations_webhook_enabled = true
recommendations_webhook_url = "http://127.0.0.1:18789/hooks/agent"
recommendations_webhook_timeout_seconds = 7.0
recommendations_webhook_channel = "discord"
recommendations_webhook_to = "channel:1487239758487748761"
recommendations_webhook_delivery_mode = "fresh"

[openclaw.recommendations_webhook_section_limits]
continue_next = 5
fresh_dubbed_episodes = 5
discovery_candidates = 3
resume_backlog = 2

[secret_files]
openclaw_hook_token = "openclaw_hook_token.txt"
""".strip()
            + "\n",
            encoding="utf-8",
        )
        self.config = load_config(self.project_root)
        ensure_directories(self.config)
        (self.config.secrets_dir / "openclaw_hook_token.txt").write_text("test-token\n", encoding="utf-8")

    def test_build_recommendation_delivery_payload_applies_delivery_policy(self) -> None:
        fake_sections = [
            {"key": "continue_next", "count": 2, "items": [{"title": "A"}, {"title": "B"}]},
            {"key": "resume_backlog", "count": 2, "items": [{"title": "C"}, {"title": "D"}]},
        ]
        with (
            patch("mal_updater.openclaw_delivery.build_recommendations", return_value=[object(), object()]),
            patch("mal_updater.openclaw_delivery.group_recommendations", return_value=fake_sections),
        ):
            payload = build_recommendation_delivery_payload(self.config, limit=5, include_dormant=False)

        self.assertEqual("mal_updater.recommendations", payload["event"])
        self.assertEqual(5, payload["limit"])
        self.assertFalse(payload["include_dormant"])
        self.assertEqual("fresh", payload["delivery_mode"])
        self.assertEqual(1, payload["section_count"])
        self.assertEqual(2, payload["item_count"])
        self.assertEqual("continue_next", payload["sections"][0]["key"])
        self.assertEqual("fresh", payload["sections"][0]["delivery_tier"])
        self.assertEqual(2, len(payload["item_fingerprints"]))

    def test_deliver_recommendations_via_openclaw_posts_payload(self) -> None:
        fake_sections = [{"key": "continue_next", "count": 1, "items": [{"title": "Test Show"}]}]
        with (
            patch("mal_updater.openclaw_delivery.build_recommendations", return_value=[object()]),
            patch("mal_updater.openclaw_delivery.group_recommendations", return_value=fake_sections),
            patch("mal_updater.openclaw_delivery.urlopen", return_value=_Response()) as urlopen_mock,
        ):
            result = deliver_recommendations_via_openclaw(self.config, limit=10, include_dormant=False)

        self.assertEqual("delivered", result.status)
        self.assertEqual(200, result.http_status)
        request = urlopen_mock.call_args.args[0]
        self.assertEqual("POST", request.get_method())
        self.assertEqual("Bearer test-token", request.headers["Authorization"])
        self.assertEqual("application/json", request.headers["Content-type"])
        posted = json.loads(request.data.decode("utf-8"))
        self.assertTrue(posted["deliver"])
        self.assertEqual("discord", posted["channel"])
        self.assertEqual("channel:1487239758487748761", posted["to"])
        self.assertIn("MAL-Updater recommendation webhook event.", posted["message"])
        self.assertIn("Test Show", posted["message"])
        self.assertIn("Delivery posture:", posted["message"])
        self.assertEqual("fresh", result.payload["structured_payload"]["delivery_mode"])
        self.assertEqual(7.0, urlopen_mock.call_args.kwargs["timeout"])

    def test_deliver_recommendations_via_openclaw_returns_no_recommendations_without_post(self) -> None:
        with (
            patch("mal_updater.openclaw_delivery.build_recommendations", return_value=[]),
            patch("mal_updater.openclaw_delivery.group_recommendations", return_value=[]),
            patch("mal_updater.openclaw_delivery.urlopen") as urlopen_mock,
        ):
            result = deliver_recommendations_via_openclaw(self.config, limit=10, include_dormant=False)

        self.assertEqual("no_recommendations", result.status)
        urlopen_mock.assert_not_called()

    def test_push_recommendations_webhook_cli_dry_run(self) -> None:
        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "push-recommendations-webhook",
            "--dry-run",
        ]
        fake_sections = [{"key": "continue_next", "count": 1, "items": [{"title": "CLI Show"}]}]
        with (
            patch("sys.argv", argv),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
            patch("mal_updater.openclaw_delivery.build_recommendations", return_value=[object()]),
            patch("mal_updater.openclaw_delivery.group_recommendations", return_value=fake_sections),
        ):
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual("dry_run", payload["status"])
        self.assertTrue(payload["payload"]["hook_request"]["deliver"])
        self.assertEqual("fresh", payload["payload"]["structured_payload"]["delivery_mode"])
        self.assertIn("CLI Show", payload["payload"]["hook_request"]["message"])


if __name__ == "__main__":
    unittest.main()
