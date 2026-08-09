from __future__ import annotations
import json, os, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from mal_updater.config import ensure_directories, load_config
from mal_updater.container_web import ControlStore, RateLimiter

class ContainerWebTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir="/tmp"); root=Path(self.tmp.name)/"data"
        self.env=patch.dict(os.environ,{"MAL_UPDATER_RUNTIME_ROOT":str(root),"MAL_UPDATER_SETTINGS_PATH":str(root/"config/settings.toml")},clear=False); self.env.start()
        self.config=load_config(Path(__file__).resolve().parents[1]); ensure_directories(self.config); self.store=ControlStore(self.config)
    def tearDown(self): self.env.stop(); self.tmp.cleanup()
    def test_process_local_csrf_is_random_and_not_persisted(self):
        other=ControlStore(self.config)
        self.assertTrue(self.store.csrf_token); self.assertNotEqual(self.store.csrf_token,other.csrf_token)
        self.assertFalse((self.config.secrets_dir/"container_auth.json").exists())
        self.assertNotIn(self.store.csrf_token,json.dumps(self.store.status()))
    def test_rate_limit(self):
        limiter=RateLimiter(2,60); self.assertTrue(limiter.allow("x",now=1)); self.assertTrue(limiter.allow("x",now=2)); self.assertFalse(limiter.allow("x",now=3))
    def test_secret_delete_blocks_automation_and_rejects_overlap(self):
        self.store.save_secrets({"mal_client_id":"fake-client"})
        with self.assertRaises(ValueError): self.store.save_secrets({"mal_client_id":"new"},["mal_client_id"])
        self.store.save_secrets({},["mal_client_id"])
        status = self.store.status()
        self.assertTrue(status["automation_desired"])
        self.assertFalse(status["automation_prerequisites_satisfied"])
        self.assertEqual("blocked", status["automation_state"])
        self.assertFalse((self.config.secrets_dir/"mal_client_id.txt").exists())
    def test_redaction_atomic_secrets_and_allowlist(self):
        self.store.save_secrets({"mal_client_id":"fake-client"}); status=self.store.status()
        self.assertTrue(status["secrets_present"]["mal_client_id"]); self.assertNotIn("fake-client",json.dumps(status)); self.assertNotIn("claimed",status); self.assertEqual(0o600,(self.config.secrets_dir/"mal_client_id.txt").stat().st_mode&0o777)
        with self.assertRaises(ValueError): self.store.save_settings({"arbitrary_toml":"evil"})
    def test_automation_gate_and_mocked_oauth_state(self):
        self.store.save_secrets({"mal_client_id":"fake-client"})
        self.assertEqual(["mal_oauth_tokens"], self.store.status()["automation_blockers"])
        flow=self.store.begin_oauth("http://127.0.0.1:8080/oauth/mal/callback"); self.assertNotIn("fake-client",json.dumps(self.store.status()))
        from urllib.parse import urlparse,parse_qs
        state=parse_qs(urlparse(flow["authorization_url"]).query)["state"][0]; item=self.store.consume_oauth(state); self.assertEqual("http://127.0.0.1:8080/oauth/mal/callback",item["redirect_uri"])
        with self.assertRaises(ValueError): self.store.consume_oauth(state)
if __name__=="__main__": unittest.main()
