import ast
import copy
import threading
import unittest
from pathlib import Path

source=ast.parse(Path('dev_server.py').read_text())
method=next(n for n in ast.walk(source) if isinstance(n,ast.FunctionDef) and n.name=='_handle_tmdb_config')
code=compile(ast.Module(body=[method],type_ignores=[]),'tmdb_config_endpoint','exec')

class TmdbConfigTests(unittest.TestCase):
    def run_save(self,config,env=False):
        store={'embyConfig':{'serverUrl':'http://media','apiKey':'media-key','tmdbToken':'old','tmdbEnabled':True,'tmdbLanguage':'zh-CN','tmdbRegion':'CN'},'networkConfig':{'proxyUrl':'http://proxy'}}
        writes=[];responses=[]
        def apply(raw):
            result=dict(raw)
            if env:result.update(tmdbToken='env-token',tmdbEnabled=True)
            return result
        scope={'STORE_LOCK':threading.Lock(),'_read_store_unlocked':lambda:copy.deepcopy(store),'_write_store_unlocked':lambda s:writes.append(s),'_apply_emby_env_overrides':apply,'_env_override_value':lambda k:'env-token' if env and k=='APP_TMDB_TOKEN' else ''}
        exec(code,scope)
        class Handler:
            _read_json_body=lambda self:{'config':config}
            _send_json=lambda self,status,payload:responses.append((status,payload))
        scope['_handle_tmdb_config'](Handler(),True)
        return writes,responses
    def test_preserves_media_and_proxy_fields(self):
        writes,responses=self.run_save({'tmdbToken':'new','serverUrl':'http://overwrite','apiKey':'overwrite'})
        self.assertEqual(responses[0][0],200)
        self.assertEqual(writes[0]['embyConfig']['serverUrl'],'http://media')
        self.assertEqual(writes[0]['embyConfig']['apiKey'],'media-key')
        self.assertEqual(writes[0]['networkConfig']['proxyUrl'],'http://proxy')
        self.assertEqual(writes[0]['embyConfig']['tmdbToken'],'new')
    def test_env_credentials_remain_managed(self):
        writes,responses=self.run_save({'tmdbToken':'overwrite','tmdbEnabled':False,'tmdbRegion':'us'},True)
        self.assertEqual(writes[0]['embyConfig']['tmdbToken'],'old')
        self.assertEqual(responses[0][1]['config']['tmdbToken'],'env-token')
        self.assertEqual(responses[0][1]['config']['tmdbRegion'],'US')
    def test_rejects_enabled_without_token(self):
        writes,responses=self.run_save({'tmdbToken':'','tmdbEnabled':True})
        self.assertEqual(writes,[])
        self.assertEqual(responses[0][0],400)
