from __future__ import annotations
import json, os, tempfile, threading, unittest, urllib.error, urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from mal_updater.config import ensure_directories, load_config
from mal_updater.container_runtime import make_container_handler
from mal_updater.container_web import ControlStore
from mal_updater.db import bootstrap_database
ROOT=Path(__file__).resolve().parents[1]
def post(url,payload,csrf=None,cookie=None):
 data=json.dumps(payload).encode(); headers={'Content-Type':'application/json'}
 if csrf: headers['X-CSRF-Token']=csrf
 if cookie: headers['Cookie']=cookie
 req=urllib.request.Request(url,data=data,headers=headers,method='POST')
 with urllib.request.urlopen(req,timeout=5) as r: return r,json.loads(r.read())
class HttpTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(dir='/tmp'); runtime=Path(self.t.name)/'runtime'
  self.env=patch.dict(os.environ,{'MAL_UPDATER_RUNTIME_ROOT':str(runtime),'MAL_UPDATER_SETTINGS_PATH':str(runtime/'config/settings.toml')},clear=False); self.env.start()
  self.config=load_config(ROOT); ensure_directories(self.config); bootstrap_database(self.config.db_path)
  self.store=ControlStore(self.config,setup_token='claim-token')
  self.calls=[]
  def tester(kind,timeout): self.calls.append((kind,timeout)); return 'mock ok'
  self.server=ThreadingHTTPServer(('127.0.0.1',0),make_container_handler(self.config,[None],self.store,connection_tester=tester))
  self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start(); self.base=f'http://127.0.0.1:{self.server.server_port}'
 def tearDown(self): self.server.shutdown(); self.server.server_close(); self.env.stop(); self.t.cleanup()
 def test_polished_pages_claim_login_connection_and_readiness(self):
  with urllib.request.urlopen(self.base+'/',timeout=5) as r: html=r.read().decode()
  self.assertIn('Make it yours',html); self.assertIn('aria-live',html)
  with self.assertRaises(urllib.error.HTTPError) as cm: urllib.request.urlopen(self.base+'/readyz',timeout=5)
  self.assertEqual(503,cm.exception.code)
  post(self.base+'/api/setup/claim',{'setup_token':'claim-token','password':'StrongPassword9'})
  r,j=post(self.base+'/api/login',{'password':'StrongPassword9'}); csrf=j['csrf_token']; cookie=r.headers['Set-Cookie'].split(';',1)[0]
  _,j=post(self.base+'/api/connections/test',{'kind':'mal'},csrf,cookie)
  self.assertEqual('Connection succeeded',j['message']); self.assertNotIn('mock ok',json.dumps(j)); self.assertEqual([('mal',10)],self.calls)
  with urllib.request.urlopen(urllib.request.Request(self.base+'/api/settings',headers={'Cookie':cookie}),timeout=5) as r: status=json.loads(r.read())
  self.assertTrue(status['claimed']); self.assertNotIn('StrongPassword9',json.dumps(status))
if __name__=='__main__': unittest.main()
