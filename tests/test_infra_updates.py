from __future__ import annotations

import copy
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from backend_modules.infra_service import (
    InfraError, InfraOperationManager, InfraService, LocalDockerClient,
    _demux_docker_log_stream, _local_image_digest, _split_image_ref,
)


class RecreateDocker(LocalDockerClient):
    def __init__(self, running=True):
        self.old = {'Id': 'old-id', 'Name': '/web', 'Image': 'sha256:old',
                    'State': {'Running': running}, 'Config': {'Image': 'nginx:latest'},
                    'HostConfig': {}, 'Mounts': [{'Type': 'volume', 'Name': 'anonymous-data',
                    'Destination': '/data', 'RW': True}], 'NetworkSettings': {'Networks': {
                    'media': {'Aliases': ['web', 'old-id'], 'NetworkID': 'dynamic-id',
                              'IPAddress': '172.16.0.4', 'IPAMConfig': {'IPv4Address': '172.16.0.4'}}}}}
        self.inspect_container = Mock(side_effect=lambda target: copy.deepcopy(self.old))
        self.pull_image_stream = Mock(return_value={'exitCode': 0})
        self.container_action = Mock(return_value={'exitCode': 0})
        self.rename_container = Mock()
        self.create_container = Mock(return_value='new-id')
        self.remove_container = Mock()
        self.wait_container_ready = Mock()
        self._request = Mock(return_value=(200, {}, b'{}'))


class RecreateTests(unittest.TestCase):
    def test_success_preserves_volume_and_sanitizes_network(self):
        client = RecreateDocker()
        client.recreate_container('web', 'nginx:latest')
        payload = client.create_container.call_args.args[1]
        self.assertIn('anonymous-data:/data:rw', payload['HostConfig']['Binds'])
        endpoint = payload['NetworkingConfig']['EndpointsConfig']['media']
        self.assertNotIn('NetworkID', endpoint)
        self.assertNotIn('IPAddress', endpoint)
        self.assertEqual(endpoint['Aliases'], ['web'])
        self.assertEqual(endpoint['IPAMConfig']['IPv4Address'], '172.16.0.4')
        client.wait_container_ready.assert_called_once_with('new-id')
        client.remove_container.assert_called_once_with('old-id')

    def test_stopped_container_stays_stopped(self):
        client = RecreateDocker(running=False)
        client.recreate_container('web', 'nginx:latest')
        client.container_action.assert_not_called()
        client.wait_container_ready.assert_not_called()

    def test_rename_failure_restores_running_without_removing_original(self):
        client = RecreateDocker()
        client.rename_container.side_effect = RuntimeError('rename failed')
        with self.assertRaisesRegex(RuntimeError, 'rename failed'):
            client.recreate_container('web', 'nginx:latest')
        client.remove_container.assert_not_called()
        self.assertEqual(client.container_action.call_args.args, ('old-id', 'start'))

    def test_unhealthy_new_container_rolls_back_old_name_network_and_running(self):
        client = RecreateDocker()
        client.wait_container_ready.side_effect = InfraError('unhealthy')
        with self.assertRaisesRegex(InfraError, 'unhealthy'):
            client.recreate_container('web', 'nginx:latest')
        client.remove_container.assert_called_once_with('new-id')
        self.assertEqual(client.rename_container.call_args.args, ('old-id', 'web'))
        self.assertEqual(client.container_action.call_args.args, ('old-id', 'start'))
        self.assertTrue(any('/connect' in c.args[1] for c in client._request.call_args_list))

    def test_rollback_error_is_visible(self):
        client = RecreateDocker()
        client.wait_container_ready.side_effect = RuntimeError('bad health')
        client.remove_container.side_effect = RuntimeError('delete failed')
        with self.assertRaises(InfraError) as caught:
            client.recreate_container('web', 'nginx:latest')
        self.assertEqual(caught.exception.code, 'rollback_failed')
        self.assertIn('delete failed', str(caught.exception))

    def test_cleanup_failure_is_warning_not_failed_update(self):
        client = RecreateDocker()
        client.remove_container.side_effect = RuntimeError('cleanup denied')
        result = client.recreate_container('web', 'nginx:latest')
        self.assertIn('cleanup denied', result['warning'])
        self.assertEqual(result['exitCode'], 0)

    def test_pull_failure_does_not_stop_container(self):
        client = RecreateDocker()
        client.pull_image_stream.side_effect = InfraError('pull failed')
        with self.assertRaises(InfraError):
            client.recreate_container('web', 'nginx:latest')
        client.container_action.assert_not_called()

    def test_self_and_auto_remove_protected_before_pull(self):
        for key, value in [('Config', {'Image': 'docker.io/lishiya003/vistamirror-admin:latest'}),
                           ('HostConfig', {'AutoRemove': True})]:
            with self.subTest(key=key):
                client = RecreateDocker()
                client.old[key] = value
                with self.assertRaises(InfraError) as caught:
                    client.recreate_container('web', 'nginx:latest')
                self.assertEqual(caught.exception.code, 'container_update_protected')
                client.pull_image_stream.assert_not_called()
                client.container_action.assert_not_called()

    def test_wait_checks_health_and_restart_loop(self):
        client = LocalDockerClient()
        client.inspect_container = Mock(return_value={'State': {'Running': True, 'Health': {'Status': 'healthy'}}})
        client.wait_container_ready('new')
        client.inspect_container.return_value = {'State': {'Running': True, 'Restarting': True}}
        with self.assertRaises(InfraError):
            client.wait_container_ready('new')
        client.inspect_container.return_value = {'State': {'Running': True, 'Health': {'Status': 'starting'}}}
        with self.assertRaises(InfraError) as caught:
            client.wait_container_ready('new', timeout=0)
        self.assertEqual(caught.exception.code, 'container_health_timeout')


class DetectionTests(unittest.TestCase):
    def test_official_tags_and_explicit_hub(self):
        for ref in ['nginx:latest', 'docker.io/nginx:latest', 'index.docker.io/library/nginx']:
            self.assertEqual(_split_image_ref(ref), ('library/nginx', 'latest'))
        self.assertEqual(_split_image_ref('redis:7'), ('library/redis', '7'))
        for ref in ['ghcr.io/user/image:tag', 'localhost:5000/nginx', 'nginx@sha256:aaa']:
            self.assertIsNone(_split_image_ref(ref))
        self.assertEqual(_local_image_digest(['docker.io/nginx@sha256:one'], 'library/nginx'), 'sha256:one')

    def test_digest_matches_running_image_not_tag_and_unknown_is_not_update(self):
        with tempfile.TemporaryDirectory() as directory:
            service = InfraService(data_dir=Path(directory))
            client = Mock()
            client.inspect_image.side_effect = lambda image: {'RepoDigests': [] if image == 'missing' else ['nginx@sha256:' + image]}
            service._local_docker = lambda: client
            service.docker_inventory = lambda host: {'containers': [
                {'Names': 'old', 'Image': 'nginx:latest', 'ImageID': 'old'},
                {'Names': 'new', 'Image': 'nginx:latest', 'ImageID': 'new'},
                {'Names': 'unknown', 'Image': 'nginx:latest', 'ImageID': 'missing'},
                {'Names': 'private', 'Image': 'ghcr.io/app/image:latest', 'ImageID': 'private'},
            ]}
            with patch('backend_modules.infra_service._docker_hub_remote_digest', return_value={'sha256:new', 'sha256:index'}) as remote:
                result = service.check_image_updates('local-docker')['updates']
                self.assertEqual(result['old']['status'], 'update')
                self.assertEqual(result['new']['status'], 'current')
                self.assertEqual(result['unknown']['status'], 'unknown')
                self.assertFalse(result['unknown']['updateAvailable'])
                self.assertEqual(result['private']['status'], 'unsupported')
                service.check_image_updates('local-docker')
                remote.assert_called_once()
            with patch('backend_modules.infra_service._docker_hub_remote_digest', side_effect=OSError('offline')):
                result = service.check_image_updates('local-docker', force=True)['updates']
                self.assertEqual(result['new']['status'], 'unknown')
            service.operations._executor.shutdown(wait=True)

    def test_inventory_enrichment_cache_and_policy_invalidation(self):
        with tempfile.TemporaryDirectory() as directory:
            service = InfraService(data_dir=Path(directory))
            client = Mock()
            client.version.return_value = {'Version': '27'}
            client.restart_policies.return_value = {'web': 'always'}
            client.inventory.side_effect = lambda: {'containers': [{'ID': '1', 'Names': 'web'}]}
            service._local_docker = lambda: client
            service.docker_inventory('local-docker')
            service.docker_inventory('local-docker')
            client.restart_policies.assert_called_once()
            service.set_restart_policy('local-docker', 'web', 'no')
            service.docker_inventory('local-docker')
            self.assertEqual(client.restart_policies.call_count, 2)
            service.operations._executor.shutdown(wait=True)

    def test_duplicate_updates_are_rejected_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = InfraOperationManager(Path(directory) / 'ops.json')
            gate = threading.Event()
            args = dict(host_id='local-docker', action='container_update', target='web', description='update', callback=lambda: (gate.wait(2) and {'exitCode': 0}))
            try:
                manager.submit(**args)
                with self.assertRaises(InfraError) as caught:
                    manager.submit(**args)
                self.assertEqual(caught.exception.code, 'update_already_pending')
            finally:
                gate.set()
                manager._executor.shutdown(wait=True)

    def test_restart_marks_abandoned_task_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ops.json'
            path.write_text(json.dumps([{'id': 'abandoned', 'status': 'running', 'action': 'container_update'}]))
            manager = InfraOperationManager(path)
            try:
                row = manager.list()[0]
                self.assertEqual(row['status'], 'failed')
                self.assertIn('服务重启', row['error'])
            finally:
                manager._executor.shutdown(wait=True)

    def test_registry_accepts_index_and_platform_digests(self):
        from backend_modules.infra_service import _docker_hub_remote_digest
        token = Mock()
        token.__enter__ = Mock(return_value=token)
        token.__exit__ = Mock(return_value=False)
        token.read.return_value = b'{"token":"test"}'
        manifest = Mock()
        manifest.__enter__ = Mock(return_value=manifest)
        manifest.__exit__ = Mock(return_value=False)
        manifest.headers = {'Docker-Content-Digest': 'sha256:index'}
        manifest.read.return_value = b'{"manifests":[{"digest":"sha256:arm"},{"digest":"sha256:amd"}]}'
        with patch('backend_modules.infra_service._REGISTRY_URLOPEN', side_effect=[token, manifest]):
            self.assertEqual(_docker_hub_remote_digest('library/nginx', 'latest'),
                             {'sha256:index', 'sha256:arm', 'sha256:amd'})

    def test_demux_preserves_tty_and_invalid_frames(self):
        frame = lambda stream, payload: bytes([stream, 0, 0, 0]) + len(payload).to_bytes(4, 'big') + payload
        self.assertEqual(_demux_docker_log_stream(frame(1, b'ok\n') + frame(2, b'bad\n')), b'ok\nbad\n')
        for raw in [b'normal tty\n', frame(1, b'abc')[:-1], frame(1, b'abc') + b'junk']:
            self.assertEqual(_demux_docker_log_stream(raw), raw)


if __name__ == '__main__':
    unittest.main()
