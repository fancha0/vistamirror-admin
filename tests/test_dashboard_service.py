from __future__ import annotations
import unittest
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit
from backend_modules.dashboard_service import LibraryTrendService


class TrendTests(unittest.TestCase):
    def test_paginates_beyond_1000_and_respects_local_date_and_cache(self):
        items = [{'Id': str(i), 'Type': 'Movie' if i % 2 else 'Episode', 'DateCreated': '2026-09-09T23:30:00Z'} for i in range(1201)]
        requests = []
        def fetch(path):
            requests.append(path)
            start = int(parse_qs(urlsplit(path).query)['StartIndex'][0])
            return {'Items': items[start:start+500], 'TotalRecordCount': len(items)}
        service = LibraryTrendService()
        now = datetime(2026, 9, 10, 2, tzinfo=timezone.utc)
        result = service.get('http://emby', 'key', 480, fetch, now=now)
        self.assertFalse(result['partial'])
        self.assertEqual(sum(d['movies']+d['episodes'] for d in result['days']), 1201)
        self.assertEqual(result['days'][-1]['movies']+result['days'][-1]['episodes'], 1201)
        self.assertEqual(len(requests), 3)
        result['days'].clear()
        self.assertEqual(len(service.get('http://emby','key',480,fetch,now=now)['days']),30)
        self.assertEqual(len(requests),3)

    def test_pagination_limit_is_disclosed(self):
        fetch=lambda path: {'Items':[{'Id':str(i),'Type':'Movie','DateCreated':'2026-09-10T00:00:00Z'} for i in range(500)],'TotalRecordCount':5000}
        result=LibraryTrendService().get('x','k',0,fetch,now=datetime(2026,9,10,tzinfo=timezone.utc),max_pages=1)
        self.assertTrue(result['partial'])

    def test_old_data_stops_scan_and_bad_dates_disclosed(self):
        calls=[]
        def fetch(path):
            calls.append(path)
            return {'Items':[{'Id':'old','Type':'Movie','DateCreated':'2025-01-01T00:00:00Z'}, {'Id':'invalid','DateCreated':'bad'}]}
        result=LibraryTrendService().get('x','k',0,fetch,now=datetime(2026,9,10,tzinfo=timezone.utc))
        self.assertTrue(result['partial'])
        self.assertEqual(len(calls),1)
        self.assertEqual(sum(d['movies'] for d in result['days']),0)

    def test_failure_is_not_cached_as_zero(self):
        service=LibraryTrendService()
        with self.assertRaises(ValueError):
            service.get('x','k',0,lambda _: {})
        result=service.get('x','k',0,lambda _: {'Items':[]})
        self.assertFalse(result['partial'])


class TrendHandlerTests(unittest.TestCase):
    def test_handler_requires_config_and_returns_aggregation(self):
        from unittest.mock import Mock, patch
        import dev_server
        handler = Mock()
        handler.headers = {}
        dev_server.AppHandler._handle_dashboard_library_trend(handler, '')
        self.assertEqual(handler._send_json.call_args.args[0], 400)
        handler.headers = {'X-Emby-Base-Url': 'http://fixture/emby', 'X-Emby-Api-Key': 'key'}
        handler._infra_query_value.return_value = '480'
        with patch.object(dev_server.LIBRARY_TREND_SERVICE, 'get', return_value={'days': [], 'partial': False}) as get:
            dev_server.AppHandler._handle_dashboard_library_trend(handler, 'offset=480')
            self.assertEqual(handler._send_json.call_args.args[0], 200)
            self.assertEqual(get.call_args.args[:3], ('http://fixture/emby', 'key', 480))
