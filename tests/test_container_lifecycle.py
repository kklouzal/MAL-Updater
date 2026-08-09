from __future__ import annotations
import json, os, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from mal_updater.config import ensure_directories, load_config
from mal_updater.container_lifecycle import backup, inspect, restore, support, about, main
from mal_updater.db import bootstrap_database

ROOT=Path(__file__).resolve().parents[1]
class LifecycleTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(dir='/tmp'); self.runtime=Path(self.t.name)/'runtime'
  self.env=patch.dict(os.environ,{'MAL_UPDATER_RUNTIME_ROOT':str(self.runtime),'MAL_UPDATER_SETTINGS_PATH':str(self.runtime/'config/settings.toml')},clear=False); self.env.start()
  self.config=load_config(ROOT); ensure_directories(self.config); bootstrap_database(self.config.db_path)
  (self.config.secrets_dir/'mal_client_id.txt').write_text('fake-secret')
 def tearDown(self): self.env.stop(); self.t.cleanup()
 def test_backup_verify_restore_and_support_redaction(self):
  arc=Path(self.t.name)/'backup.tar.gz'; backup(ROOT,arc); rep=inspect(arc,True)
  self.assertTrue(rep['valid']); self.assertTrue(any(x['path'].endswith('mal_client_id.txt') for x in rep['manifest']['files']))
  self.assertTrue(restore(ROOT,arc,dry_run=True)['dry_run'])
  (self.config.secrets_dir/'mal_client_id.txt').write_text('changed')
  out=restore(ROOT,arc,yes=True); self.assertIn('pre_restore_backup',out)
  self.assertEqual('fake-secret',(self.config.secrets_dir/'mal_client_id.txt').read_text())
  sup=Path(self.t.name)/'support.tar.gz'; support(ROOT,sup)
  import tarfile
  with tarfile.open(sup) as tf: data=tf.extractfile('mal-updater-support/diagnostics.json').read().decode()
  self.assertNotIn('fake-secret',data)
 def test_rejects_traversal_symlink_and_undeclared_payload(self):
  import io, tarfile
  bad=Path(self.t.name)/'bad.tar.gz'
  with tarfile.open(bad,'w:gz') as tf:
   info=tarfile.TarInfo('../escape'); info.size=1; tf.addfile(info,io.BytesIO(b'x'))
  with self.assertRaises(ValueError): inspect(bad,True)
  link=self.config.state_dir/'bad-link'; link.symlink_to('/etc/passwd')
  with self.assertRaises(ValueError): backup(ROOT,Path(self.t.name)/'link.tar.gz')
 def test_about_and_admin_reset_cli(self):
  self.assertEqual('MAL-Updater',about()['product'])
  self.assertEqual(0,main(['--project-root',str(ROOT),'version']))
if __name__=='__main__': unittest.main()
