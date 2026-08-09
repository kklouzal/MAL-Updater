from __future__ import annotations
import json, os, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from mal_updater.config import ensure_directories, load_config
from mal_updater.container_web import ControlStore, RateLimiter, hash_password, verify_password

class ContainerWebTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir="/tmp"); root=Path(self.tmp.name)/"data"
        self.env=patch.dict(os.environ,{"MAL_UPDATER_RUNTIME_ROOT":str(root),"MAL_UPDATER_SETTINGS_PATH":str(root/"config/settings.toml")},clear=False); self.env.start()
        self.config=load_config(Path(__file__).resolve().parents[1]); ensure_directories(self.config); self.store=ControlStore(self.config,setup_token="one-time-token")
    def tearDown(self): self.env.stop(); self.tmp.cleanup()
    def test_password_hash_is_salted_and_verifies(self):
        a=hash_password("StrongPassword9"); b=hash_password("StrongPassword9")
        self.assertNotEqual(a,b); self.assertNotIn("StrongPassword9",a); self.assertTrue(verify_password("StrongPassword9",a)); self.assertFalse(verify_password("wrong",a))
    def test_first_run_claim_session_csrf_material_and_permissions(self):
        with self.assertRaises(ValueError): self.store.claim("wrong","StrongPassword9")
        self.store.claim("one-time-token","StrongPassword9"); self.assertTrue(self.store.claimed)
        self.assertEqual(0o600,self.store.auth_path.stat().st_mode&0o777)
        sid,csrf=self.store.login("StrongPassword9","client"); self.assertTrue(sid); self.assertEqual(csrf,self.store.session(sid).csrf)
        stored=json.loads(self.store.auth_path.read_text()); self.assertNotIn("StrongPassword9",str(stored)); self.assertNotIn(sid,str(stored))
    def test_rate_limit(self):
        limiter=RateLimiter(2,60); self.assertTrue(limiter.allow("x",now=1)); self.assertTrue(limiter.allow("x",now=2)); self.assertFalse(limiter.allow("x",now=3))
    def test_password_hash_parameters_are_bounded(self):
        good=hash_password("StrongPassword9")
        parts=good.split("$"); parts[1]=str(2**20)
        self.assertFalse(verify_password("StrongPassword9","$".join(parts)))
        self.assertFalse(verify_password("X"*2048,good))
    def test_secret_delete_disables_daemon_and_rejects_overlap(self):
        self.store.claim("one-time-token","StrongPassword9")
        self.store.save_secrets({"mal_client_id":"fake-client"})
        with self.assertRaises(ValueError): self.store.save_secrets({"mal_client_id":"new"},["mal_client_id"])
        self.store.save_secrets({},["mal_client_id"])
        self.assertFalse(self.store.status()["daemon_enabled"])
        self.assertFalse((self.config.secrets_dir/"mal_client_id.txt").exists())
    def test_redaction_atomic_secrets_and_allowlist(self):
        self.store.claim("one-time-token","StrongPassword9")
        self.store.save_secrets({"mal_client_id":"fake-client"}); status=self.store.status()
        self.assertTrue(status["secrets_present"]["mal_client_id"]); self.assertNotIn("fake-client",json.dumps(status)); self.assertEqual(0o600,(self.config.secrets_dir/"mal_client_id.txt").stat().st_mode&0o777)
        with self.assertRaises(ValueError): self.store.save_settings({"arbitrary_toml":"evil"})
    def test_daemon_gate_and_mocked_oauth_state(self):
        self.store.claim("one-time-token","StrongPassword9"); self.store.save_secrets({"mal_client_id":"fake-client"})
        with self.assertRaises(ValueError): self.store.set_daemon(True)
        flow=self.store.begin_oauth("http://127.0.0.1:8080/oauth/mal/callback"); self.assertNotIn("fake-client",json.dumps(self.store.status()))
        from urllib.parse import urlparse,parse_qs
        state=parse_qs(urlparse(flow["authorization_url"]).query)["state"][0]; item=self.store.consume_oauth(state); self.assertEqual("http://127.0.0.1:8080/oauth/mal/callback",item["redirect_uri"])
        with self.assertRaises(ValueError): self.store.consume_oauth(state)
if __name__=="__main__": unittest.main()
