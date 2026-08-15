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
def post(url,payload,csrf=None,origin=None,content_type='application/json'):
 data=json.dumps(payload).encode(); headers={'Content-Type':content_type}
 if csrf: headers['X-CSRF-Token']=csrf
 if origin: headers['Origin']=origin
 req=urllib.request.Request(url,data=data,headers=headers,method='POST')
 with urllib.request.urlopen(req,timeout=5) as r: return r,json.loads(r.read())
class HttpTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory(dir='/tmp'); runtime=Path(self.t.name)/'runtime'
  self.env=patch.dict(os.environ,{'MAL_UPDATER_RUNTIME_ROOT':str(runtime),'MAL_UPDATER_SETTINGS_PATH':str(runtime/'config/settings.toml')},clear=False); self.env.start()
  self.config=load_config(ROOT); ensure_directories(self.config); bootstrap_database(self.config.db_path)
  self.store=ControlStore(self.config); self.calls=[]
  def tester(kind,timeout): self.calls.append((kind,timeout)); return 'mock ok'
  self.server=ThreadingHTTPServer(('127.0.0.1',0),make_container_handler(self.config,[None],self.store,connection_tester=tester))
  self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start(); self.base=f'http://127.0.0.1:{self.server.server_port}'
 def tearDown(self): self.server.shutdown(); self.server.server_close(); self.env.stop(); self.t.cleanup()
 def get_json(self,path):
  with urllib.request.urlopen(self.base+path,timeout=5) as r:return r,json.loads(r.read())
 def http_error(self,request):
  with self.assertRaises(urllib.error.HTTPError) as cm: urllib.request.urlopen(request,timeout=5)
  return cm.exception
 def test_dashboard_routes_are_db_backed_and_settings_is_the_control_page(self):
  for path in ('/','/dashboard'):
   with urllib.request.urlopen(self.base+path,timeout=5) as r:
    html=r.read().decode(); self.assertEqual('nosniff',r.headers['X-Content-Type-Options']); self.assertIn("default-src 'self'",r.headers['Content-Security-Policy'])
   self.assertIn('MAL-Updater live dashboard',html); self.assertIn('/api/dashboard',html); self.assertIn('href="/debug"',html); self.assertIn('href="/settings"',html); self.assertNotIn('Trusted LAN control plane',html)
  with urllib.request.urlopen(self.base+'/debug',timeout=5) as r:
   debug_html=r.read().decode(); self.assertEqual('no-store',r.headers['Cache-Control']); self.assertEqual('DENY',r.headers['X-Frame-Options']); self.assertIn("default-src 'self'",r.headers['Content-Security-Policy'])
  self.assertIn('MAL-Updater debug',debug_html); self.assertIn('Snapshot',debug_html); self.assertIn('Recent provider sync runs',debug_html); self.assertIn('href="/"',debug_html); self.assertIn('href="/settings"',debug_html)
  response,dashboard=self.get_json('/api/dashboard')
  self.assertIn('recommendations',dashboard); self.assertEqual('no-store',response.headers['Cache-Control']); self.assertEqual('DENY',response.headers['X-Frame-Options'])
  with urllib.request.urlopen(self.base+'/settings',timeout=5) as r: settings_html=r.read().decode()
  self.assertIn('Trusted LAN control plane',settings_html); self.assertIn('aria-live',settings_html); self.assertIn('href="/"',settings_html); self.assertNotIn('Claim installation',settings_html); self.assertNotIn('Sign in',settings_html)
  self.assertNotIn('Enable automation',settings_html); self.assertNotIn('Disable automation',settings_html); self.assertNotIn('/api/daemon',settings_html)
  self.assertIn("api('/api/status')",settings_html)
 def test_mutations_use_bootstrap_csrf(self):
  _,settings=self.get_json('/api/settings'); self.assertNotIn('claimed',settings)
  _,csrf_payload=self.get_json('/api/csrf'); csrf=csrf_payload['csrf_token']
  _,j=post(self.base+'/api/connections/test',{'kind':'mal'},csrf,self.base)
  self.assertEqual('Connection succeeded',j['message']); self.assertNotIn('mock ok',json.dumps(j)); self.assertEqual([('mal',10)],self.calls)
  self.assertFalse((self.config.secrets_dir/'container_auth.json').exists())
 def test_readiness_requires_mal_setup_and_running_scheduler(self):
  error=self.http_error(self.base+'/readyz'); self.assertEqual(503,error.code)
  self.store.save_secrets({'mal_client_id':'fake-client'})
  (self.config.secrets_dir/'mal_access_token.txt').write_text('access\n',encoding='utf-8')
  (self.config.secrets_dir/'mal_refresh_token.txt').write_text('refresh\n',encoding='utf-8')
  self.assertTrue(self.store.status()['setup_complete'])
  error=self.http_error(self.base+'/readyz'); self.assertEqual(503,error.code)
  blocked=json.loads(error.read()); self.assertEqual('blocked',blocked['automation_state']); self.assertEqual(['scheduler_not_running'],blocked['automation_blockers'])

  running=unittest.mock.Mock(); running.poll.return_value=None
  self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)
  self.server=ThreadingHTTPServer(('127.0.0.1',0),make_container_handler(self.config,[running],self.store,connection_tester=lambda kind,timeout: None))
  self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start(); self.base=f'http://127.0.0.1:{self.server.server_port}'
  _,ready=self.get_json('/readyz'); self.assertTrue(ready['ready']); self.assertEqual('running',ready['automation_state'])
 def test_csrf_origin_json_limits_and_dead_auth_routes(self):
  missing=self.http_error(urllib.request.Request(self.base+'/api/settings',data=b'{}',headers={'Content-Type':'application/json'},method='POST')); self.assertEqual(403,missing.code)
  cross=self.http_error(urllib.request.Request(self.base+'/api/settings',data=b'{}',headers={'Content-Type':'application/json','X-CSRF-Token':self.store.csrf_token,'Origin':'http://evil.example'},method='POST')); self.assertEqual(403,cross.code)
  media=self.http_error(urllib.request.Request(self.base+'/api/settings',data=b'{}',headers={'Content-Type':'text/plain'},method='POST')); self.assertEqual(415,media.code)
  oversized=self.http_error(urllib.request.Request(self.base+'/api/settings',data=b'{}',headers={'Content-Type':'application/json','Content-Length':str(64*1024+1)},method='POST')); self.assertEqual(413,oversized.code)
  for path in ('/api/daemon','/api/login','/api/logout','/api/password','/api/setup/claim'):
   error=self.http_error(urllib.request.Request(self.base+path,data=b'{}',headers={'Content-Type':'application/json','X-CSRF-Token':self.store.csrf_token},method='POST')); self.assertEqual(404,error.code)
 def test_trusted_host_validation_covers_pages_dashboard_api_and_oauth_callback(self):
  for path in ('/','/dashboard','/debug','/api/dashboard','/settings','/api/csrf','/oauth/mal/callback?state=x&code=fake'):
   error=self.http_error(urllib.request.Request(self.base+path,headers={'Host':'public.example'})); self.assertEqual(400,error.code)
if __name__=='__main__': unittest.main()
