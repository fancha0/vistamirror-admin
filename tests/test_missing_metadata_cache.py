import io
import json
import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import date as RealDate
from unittest.mock import patch

from backend_modules.missing_metadata_cache import MissingMetadataCache
from backend_modules.missing_episode_service import MissingEpisodeService
from backend_modules.media_identity_service import MediaIdentityService


class MissingCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'metadata.sqlite3'
        self.calls = []
        self.payloads = {
            '/tv/1': {'id': 1, 'status': 'Returning Series', 'seasons': [{'season_number': 1, 'episode_count': 1}]},
            '/tv/1/season/1': {'id': 10, 'episodes': [{'episode_number': 1, 'air_date': '2099-01-02'}]},
        }

    def fetch(self, request, timeout):
        from urllib.parse import urlsplit
        path = urlsplit(request.full_url).path.removeprefix('/3')
        self.calls.append(path)
        return io.BytesIO(json.dumps(self.payloads[path]).encode())

    def service(self, **kw):
        return MissingEpisodeService(emby_fetcher=lambda p: {}, tmdb_token='secret-test-token', metadata_cache_path=self.path, urlopen=self.fetch, **kw)

    def test_persistent_hit_force_refresh_and_locale_isolation(self):
        path = '/tv/1?language=zh-CN'
        self.service()._tmdb_get_json(path)
        cached = self.service()
        cached._tmdb_get_json(path)
        self.assertEqual(cached._cache_stats['hits'], 1)
        self.assertEqual(len(self.calls), 1)
        self.service(force_refresh=True)._tmdb_get_json(path)
        self.service(tmdb_language='en-US')._tmdb_get_json('/tv/1?language=en-US')
        self.assertEqual(len(self.calls), 3)
        self.assertNotIn(b'secret-test-token', self.path.read_bytes())

    def test_expiry_and_corrupt_cache_fall_back(self):
        now = [100.0]
        cache = MissingMetadataCache(self.path, clock=lambda: now[0])
        cache.put('x', {'ok': True}, 10)
        self.assertIsNotNone(cache.get('x'))
        now[0] = 110
        self.assertIsNone(cache.get('x'))
        bad = Path(self.temp.name) / 'bad.sqlite3'
        bad.write_text('broken')
        self.assertIsNone(MissingMetadataCache(bad).get('x'))

    def test_failures_and_empty_responses_not_cached(self):
        self.payloads['/tv/1'] = {'success': False, 'status_message': 'unavailable'}
        service = self.service()
        service._tmdb_get_json('/tv/1')
        service._tmdb_get_json('/tv/1')
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(service._cache_stats['writes'], 0)
        with patch.object(service, '_urlopen', side_effect=TimeoutError('timeout')):
            with self.assertRaises(TimeoutError):
                service._tmdb_get_json('/tv/1')
        self.assertEqual(service._cache_stats['writes'], 0)

    def test_current_season_short_history_long_and_future_short(self):
        service = self.service()
        service._metadata_ttl('/tv/1', {'status': 'Returning Series', 'seasons': [{'season_number': 1, 'episode_count': 1}, {'season_number': 2, 'episode_count': 1}]})
        old = {'episodes': [{'air_date': '2020-01-01'}]}
        self.assertEqual(service._metadata_ttl('/tv/1/season/1', old), 7 * 86400)
        self.assertEqual(service._metadata_ttl('/tv/1/season/2', old), 7200)
        self.assertEqual(service._metadata_ttl('/tv/1/season/1', {'episodes': [{'air_date': '2099-01-01'}]}), 7200)

    def test_cached_air_dates_recomputed_on_new_day(self):
        class Before(RealDate):
            @classmethod
            def today(cls): return cls(2099, 1, 1)
        class After(RealDate):
            @classmethod
            def today(cls): return cls(2099, 1, 2)
        service = self.service()
        identity = MediaIdentityService(emby_fetcher=lambda p: {}, tmdb_fetcher=service._tmdb_get_json)
        with patch('backend_modules.media_identity_service.date', Before):
            before = identity.query_media_detail('1', 'tv')
        with patch('backend_modules.media_identity_service.date', After):
            after = identity.query_media_detail('1', 'tv')
        self.assertEqual(len(self.calls), 2)
        self.assertNotEqual(before['airedSeasonMap'], after['airedSeasonMap'])

    def test_new_local_episodes_rechecked_with_cached_tmdb(self):
        from urllib.parse import parse_qs, urlsplit
        series = {'Id': 's1', 'Name': 'Test', 'Type': 'Series', 'ProductionYear': 2020, 'ProviderIds': {'Tmdb': '1'}}
        episodes = [{'ParentIndexNumber': 1, 'IndexNumber': 1}]
        def emby(path):
            if path.startswith('/Items/s1'):
                return series
            query = parse_qs(urlsplit(path).query)
            kind = query.get('IncludeItemTypes', [''])[0]
            rows = episodes if kind == 'Episode' else [{'Id': 'season1', 'IndexNumber': 1}] if kind == 'Season' else [series]
            return {'Items': rows, 'TotalRecordCount': len(rows)}
        self.payloads['/tv/1'].update(name='Test', first_air_date='2020-01-01', number_of_episodes=2)
        self.payloads['/tv/1']['seasons'][0]['episode_count'] = 2
        self.payloads['/tv/1/season/1']['episodes'] = [{'episode_number': n, 'air_date': '2020-01-01'} for n in (1, 2)]
        def scan():
            service = self.service()
            service.emby_fetcher = emby
            return service.scan()
        first = scan()
        episodes.append({'ParentIndexNumber': 1, 'IndexNumber': 2})
        second = scan()
        self.assertEqual(first['summary']['missingEpisodeCount'], 1)
        self.assertEqual(second['summary']['missingEpisodeCount'], 0)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(second['summary']['metadataCache']['hits'], 2)

    def test_parallel_writes_are_readable(self):
        cache = MissingMetadataCache(self.path)
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda i: cache.put(str(i), {'id': i}, 100), range(30)))
        self.assertTrue(all(results))
        self.assertEqual(cache.get('29'), {'id': 29})


if __name__ == '__main__':
    unittest.main()
