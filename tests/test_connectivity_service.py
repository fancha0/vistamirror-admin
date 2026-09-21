import json
import socket
import unittest
import urllib.error
from unittest.mock import Mock, patch
from backend_modules.connectivity_service import targets, public_target, probe, NoRedirect

class ConnectivityTests(unittest.TestCase):
    def row(self):
        return targets({'tmdbToken':'secret-token'}, {}, {}, '')[0]

    def test_public_metadata_never_contains_credentials(self):
        rows=targets({'tmdbToken':'secret-token','serverUrl':'http://localhost:8096','apiKey':'private-key'}, {'channels':{'telegram':{'botToken':'secret-bot','proxyUrl':'http://user:password@proxy:8080'}}}, {}, '')
        payload=json.dumps([public_target(row) for row in rows])
        for secret in ['secret-token','private-key','secret-bot','password']:
            self.assertNotIn(secret,payload)

    def test_unconfigured_server_does_not_open_network(self):
        row=next(r for r in targets({}, {}, {}, '') if r['id']=='media')
        with patch('urllib.request.build_opener') as op:
            self.assertEqual(probe(row)['status'],'not_configured')
            op.assert_not_called()

    def test_auth_failure_is_not_network_failure(self):
        opener=Mock();opener.open.side_effect=urllib.error.HTTPError('https://secret',401,'unauthorized',{},None)
        with patch('urllib.request.build_opener',return_value=opener):
            result=probe(self.row())
        self.assertEqual(result['status'],'auth_error')
        self.assertEqual(result['httpStatus'],401)
        self.assertNotIn('secret',json.dumps(result))

    def test_http_404_reports_reachable_but_not_success(self):
        opener=Mock();opener.open.side_effect=urllib.error.HTTPError('',404,'missing',{},None)
        with patch('urllib.request.build_opener',return_value=opener):
            self.assertEqual(probe(self.row())['status'],'http_error')

    def test_dns_errors_are_sanitized(self):
        opener=Mock();opener.open.side_effect=urllib.error.URLError(socket.gaierror('secret proxy'))
        with patch('urllib.request.build_opener',return_value=opener):
            result=probe(self.row())
        self.assertEqual(result['message'],'域名解析失败')
        self.assertNotIn('secret',json.dumps(result))

    def test_deadline_covers_blocked_dns(self):
        import threading
        released=threading.Event()
        opener=Mock();opener.open.side_effect=lambda *a,**k: released.wait(2)
        try:
            with patch('urllib.request.build_opener',return_value=opener):
                self.assertEqual(probe(self.row(),timeout=.01)['status'],'timeout')
        finally:released.set()

    def test_success_and_elapsed_time(self):
        from unittest.mock import MagicMock
        response=MagicMock();response.__enter__.return_value.status=200
        opener=Mock();opener.open.return_value=response
        with patch('urllib.request.build_opener',return_value=opener):
            result=probe(self.row())
        self.assertEqual(result['status'],'connected')
        self.assertGreaterEqual(result['elapsedMs'],0)
        self.assertEqual(result['httpStatus'],200)

    def test_credentials_not_forwarded_on_redirect(self):
        self.assertIsNone(NoRedirect().redirect_request(None,None,302,'',{},'https://other.invalid'))

if __name__=='__main__':unittest.main()
