import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from backend_modules.strm_event_log import recent_events, sanitize
from tests import test_strm115_service as fixtures


class StrmEventLogTests(unittest.TestCase):
    def test_tail_order_filter_incomplete_line_and_chunk_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'events.jsonl'
            rows = [{'id': i, 'module': 'strm115' if i % 2 == 0 else 'system', 'message': '影片' * 500} for i in range(130)]
            path.write_text('\n'.join(json.dumps(row, ensure_ascii=False) for row in rows) + '\n{"partial":', encoding='utf-8')
            result = recent_events(path, 30)
            self.assertEqual([row['id'] for row in result['events']], list(range(128, 68, -2)))
            self.assertTrue(result['limited'])
            self.assertEqual(len(recent_events(path, 1000)['events']), 65)

    def test_errors_remain_visible_but_credentials_and_urls_are_redacted(self):
        data = sanitize({'error': '拒绝 https://cdn.test/video?token=secret&sig=abc', 'cookie':'session', 'pickCode':'abc', 'name':'毒蜂.mkv', 'path':'/d/s_private.mkv', 'nested': {'authorization': 'Bearer x'}})
        text = json.dumps(data)
        self.assertNotIn('token=secret', text)
        self.assertNotIn('s_private', text)
        self.assertEqual(data['name'], '毒蜂.mkv')
        self.assertEqual(data['cookie'], '[REDACTED]')
        self.assertEqual(data['nested']['authorization'], '[REDACTED]')

    def test_process_events_include_skip_and_write_failure(self):
        with tempfile.TemporaryDirectory() as root:
            service = fixtures.Strm115ServiceTests().make_service(Path(root))
            service.config['requestIntervalMs'] = 0
            events = []
            service.event_callback = lambda **event: events.append(event)
            service.sync()
            service.sync()
            messages = [e['message'] for e in events]
            self.assertIn('跳过非视频文件。', messages)
            self.assertIn('已生成 STRM。', messages)
            self.assertIn('跳过：STRM 内容未变化。', messages)
            self.assertIn('扫描目录。', messages)
            service.config['publicBaseUrl'] = 'https://changed.example'
            with patch.object(Path, 'write_text', side_effect=OSError('只读目录')):
                with self.assertRaises(OSError):
                    service.sync()
            self.assertTrue(any(e['level']=='error' and e['detail'].get('error')=='只读目录' for e in events))

    def test_log_failure_does_not_break_playback_or_work(self):
        with tempfile.TemporaryDirectory() as root:
            service = fixtures.Strm115ServiceTests().make_service(Path(root))
            service.event_callback = lambda **event: (_ for _ in ()).throw(OSError('log full'))
            service._emit('test', 'test')
