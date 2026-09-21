from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from backend_modules.host_metrics_service import HostMetricsService, parse_metrics


def fixture(step=0, reset=False):
    metrics = [
        'node_uname_info{nodename="syfnos",release="6.12"} 1',
        'node_memory_MemTotal_bytes 34359738368',
        'node_memory_MemAvailable_bytes 21474836480',
        'node_boot_time_seconds 1700000000',
    ]
    for core in (0, 1):
        for mode, start, delta in [('idle', 100, 4), ('user', 10, 1), ('guest', 3, 1)]:
            value = 0 if reset else start + delta * step
            metrics.append(f'node_cpu_seconds_total{{cpu="{core}",mode="{mode}"}} {value}')
    for device, name, size, free in [('/dev/md1', '/vol3', 1000, 250), ('/dev/md1', '/vol3/media', 1000, 250), ('/dev/sda1', '/', 200, 100), ('tmpfs', '/run', 500, 250)]:
        labels=f'device="{device}",fstype="{"tmpfs" if device=="tmpfs" else "ext4"}",mountpoint="{name}"'
        for metric, value in [('size', size), ('free', free), ('avail', free-10)]:
            metrics.append(f'node_filesystem_{metric}_bytes{{{labels}}} {value}')
        metrics.append(f'node_filesystem_device_error{{{labels}}} 0')
    for device in ['bond0', 'eth0', 'eth1', 'docker0', 'veth123', 'br-abc', 'lo']:
        rx,tx = (0,0) if reset else (1000+1000*step, 500+500*step)
        metrics += [f'node_network_receive_bytes_total{{device="{device}"}} {rx}',
                    f'node_network_transmit_bytes_total{{device="{device}"}} {tx}',
                    f'node_network_info{{device="{device}",operstate="up"}} 1']
    metrics.append('node_scrape_collector_success{collector="filesystem"} 1')
    return '\n'.join(metrics)


class HostMetricsTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.fetch = Mock(return_value=fixture())
        self.service = HostMetricsService('http://fixture/metrics', fetch=self.fetch, clock=lambda:self.now)

    def test_first_sample_has_gauges_then_second_has_rates(self):
        self.service.sample()
        first=self.service.snapshot()
        self.assertEqual(first['status'],'warming')
        self.assertIsNone(first['cpuPercent'])
        self.assertEqual(first['hostname'],'syfnos')
        self.assertEqual(first['cpuCount'],2)
        self.assertEqual(first['memory']['percent'],37.5)
        self.assertIsNone(first['interfaces'][0]['receivePerSecond'])
        self.now+=5;self.fetch.return_value=fixture(1);self.service.sample()
        second=self.service.snapshot()
        self.assertEqual(second['status'],'ready')
        self.assertEqual(second['cpuPercent'],20)
        self.assertEqual(second['interfaces'][0]['name'],'bond0')
        self.assertEqual(second['interfaces'][0]['receivePerSecond'],200)
        self.assertEqual(second['interfaces'][0]['transmitPerSecond'],100)

    def test_real_disks_deduplicate_bind_mounts_and_exclude_virtual(self):
        self.service.sample()
        disks=self.service.snapshot()['disks']
        self.assertEqual([d['mountpoint'] for d in disks],['/vol3','/'])
        self.assertEqual(disks[0]['percent'],75)
        self.assertEqual(disks[0]['available'],240)

    def test_interfaces_remain_separate_without_double_counting(self):
        self.service.sample()
        names=[n['name'] for n in self.service.snapshot()['interfaces']]
        self.assertEqual(names,['bond0','eth0','eth1'])

    def test_failure_retains_readings_and_recovery_rewarms_after_gap(self):
        self.service.sample();self.now+=5
        self.fetch.side_effect=OSError('secret-internal-address')
        self.service.sample()
        stale=self.service.snapshot()
        self.assertEqual(stale['status'],'stale')
        self.assertEqual(stale['memory']['percent'],37.5)
        self.assertNotIn('secret',stale['error'])
        self.now+=30;self.fetch.side_effect=None;self.fetch.return_value=fixture(5)
        self.service.sample()
        self.assertEqual(self.service.snapshot()['status'],'warming')
        self.now+=5;self.fetch.return_value=fixture(6);self.service.sample()
        self.assertEqual(self.service.snapshot()['status'],'ready')

    def test_counter_reset_never_becomes_negative_rate_or_peak(self):
        self.service.sample();self.now+=5;self.fetch.return_value=fixture(reset=True);self.service.sample()
        payload=self.service.snapshot()
        self.assertIsNone(payload['cpuPercent'])
        self.assertIsNone(payload['interfaces'][0]['receivePerSecond'])

    def test_history_is_bounded_to_one_minute_and_snapshots_are_isolated(self):
        for step in range(30):
            self.now+=5;self.fetch.return_value=fixture(step);self.service.sample()
        payload=self.service.snapshot()
        self.assertLessEqual(len(payload['history']),13)
        payload['history'].clear()
        self.assertTrue(self.service.snapshot()['history'])

    def test_disk_refresh_is_slower_than_cpu(self):
        self.service.sample();self.now+=5
        self.fetch.return_value=fixture(1).replace('} 1000','} 2000')
        self.service.sample();self.assertEqual(self.service.snapshot()['disks'][0]['total'],1000)
        self.now+=25;self.service.sample()
        self.assertEqual(self.service.snapshot()['disks'][0]['total'],2000)

    def test_collector_failure_preserves_old_disks(self):
        self.service.sample();self.now+=30
        self.fetch.return_value=fixture(6).replace('collector="filesystem"} 1','collector="filesystem"} 0')
        self.service.sample();payload=self.service.snapshot()
        self.assertTrue(payload['disksStale']);self.assertEqual(len(payload['disks']),2)

    def test_missing_config_and_invalid_metrics_are_explicit(self):
        self.assertEqual(HostMetricsService().snapshot()['status'],'disabled')
        self.assertEqual(self.service.snapshot()['status'],'starting')
        self.fetch.return_value='<html>error</html>';self.service.sample()
        self.assertEqual(self.service.snapshot()['status'],'unavailable')

    def test_expired_sample_is_stale_even_without_fetch_error(self):
        self.service.sample();self.now+=16
        self.assertEqual(self.service.snapshot()['status'],'stale')

    def test_prometheus_parser_labels_scientific_notation_and_nonfinite(self):
        parsed=parse_metrics('node_test{mountpoint="/vol3/a\\\\b",name="a\\"b"} 1.2e3\nnode_bad NaN\nnode_bad +Inf\n# HELP ignored')
        self.assertEqual(parsed['node_test'][0],({'mountpoint':'/vol3/a\\b','name':'a"b'},1200))
        self.assertNotIn('node_bad',parsed)


class DevDeployTests(unittest.TestCase):
    def test_deploy_helper_discovers_bridge_without_real_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            docker=root/'docker'
            docker.write_text('''#!/usr/bin/env python3
import json,os,sys
with open(os.environ['TEST_DOCKER_LOG'],'a') as output:
 output.write(json.dumps({'args':sys.argv[1:],'ip':os.environ.get('FNOS_NODE_EXPORTER_BIND_IP')})+'\\n')
if sys.argv[1:3]==['network','inspect']: print('172.30.0.1')
''')
            docker.chmod(0o755)
            env={**os.environ,'PATH':str(root)+':'+os.environ['PATH'],'TEST_DOCKER_LOG':str(root/'calls')}
            env.pop('FNOS_NODE_EXPORTER_BIND_IP',None)
            script=Path(__file__).resolve().parents[1]/'scripts/fnos_dev_compose.sh'
            result=subprocess.run(['bash',str(script)],cwd=root,env=env,text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            calls=[json.loads(line) for line in (root/'calls').read_text().splitlines()]
            self.assertEqual(calls[0]['args'][:2],['network','inspect'])
            self.assertTrue(all(c['ip']=='172.30.0.1' for c in calls[1:]))
            self.assertTrue(any('exec' in c['args'] for c in calls))


class CollectorTransportTests(unittest.TestCase):
    def test_fast_scrapes_are_separate_from_filesystem_and_ignore_proxies(self):
        from unittest.mock import patch
        from urllib.parse import parse_qs, urlsplit
        class Response:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def read(self,limit): return b'node_example 1\n'
        times=[100.0]
        opened=[]
        class Opener:
            def open(self,request,timeout):
                opened.append(parse_qs(urlsplit(request.full_url).query)['collect[]'])
                return Response()
        service=HostMetricsService('http://fixture/metrics',clock=lambda:times[0])
        with patch('backend_modules.host_metrics_service.urllib.request.build_opener',return_value=Opener()) as builder:
            service._fetch_metrics()
            times[0]+=5;service._fetch_metrics()
            times[0]+=25;service._fetch_metrics()
            self.assertEqual(opened,[['cpu','meminfo','netdev','netclass','uname','stat'],['filesystem'],
                                     ['cpu','meminfo','netdev','netclass','uname','stat'],
                                     ['cpu','meminfo','netdev','netclass','uname','stat'],['filesystem']])
            self.assertEqual(builder.call_args.args[0].proxies,{})

    def test_filesystem_transport_failure_does_not_hide_cpu_memory(self):
        from unittest.mock import patch
        class Response:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def read(self,limit): return fixture().encode()
        opener=Mock()
        opener.open.side_effect=[Response(),TimeoutError('disk timed out')]
        service=HostMetricsService('http://fixture/metrics')
        with patch('backend_modules.host_metrics_service.urllib.request.build_opener',return_value=opener):
            service.sample()
        result=service.snapshot()
        self.assertEqual(result['memory']['percent'],37.5)
        self.assertTrue(result['disksStale'])

    def test_route_obeys_admin_auth_before_returning_metrics(self):
        from unittest.mock import patch
        import dev_server
        handler=Mock()
        handler.path='/api/dashboard/host-metrics'
        handler._enforce_admin_auth.return_value=False
        with patch.object(dev_server.HOST_METRICS_SERVICE,'snapshot',return_value={'status':'ready'}) as snapshot:
            dev_server.AppHandler.do_GET(handler)
            snapshot.assert_not_called()
            handler._enforce_admin_auth.return_value=True
            dev_server.AppHandler.do_GET(handler)
            snapshot.assert_called_once()
            self.assertEqual(handler._send_json.call_args.args,(200,{'ok':True,'status':{'status':'ready'}}))
