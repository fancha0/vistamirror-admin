from __future__ import annotations

import json
import pathlib
import tempfile
import time
import unittest

from backend_modules.infra_service import InfraCredentialCipher, InfraError, InfraService, LocalDockerClient


class FakeRunner:
    def __init__(self, responses: dict[str, dict] | None = None) -> None:
        self.responses = responses or {}
        self.commands: list[str] = []

    def run(self, command: str, *, timeout: int = 45) -> dict:
        self.commands.append(command)
        for needle, response in self.responses.items():
            if needle in command:
                return dict(response)
        return {"exitCode": 0, "stdout": "ok\n", "stderr": "", "fingerprint": "SHA256:test"}


class FakeLocalDocker:
    def __init__(self) -> None:
        self.actions: list[tuple[str, str]] = []

    def info(self) -> dict:
        return {"Name": "nas-local", "NCPU": 4, "MemTotal": 2048, "Containers": 3}

    def version(self) -> dict:
        return {"Version": "27.1"}

    def inventory(self) -> dict:
        return {
            "containers": [{"Names": "emby", "Image": "emby/embyserver", "State": "running", "Status": "Up", "Ports": "8096/tcp"}],
            "images": [{"Repository": "emby", "Tag": "latest", "ID": "sha256:one", "Size": "1 GB"}],
            "compose": [{"Name": "media", "Containers": 1, "Status": "running", "Source": "Docker Socket"}],
        }

    def containers(self) -> list[dict]:
        return [{"ID": "container-one", "Names": "emby", "State": "running"}]

    def stats(self, containers: list[dict]) -> dict[str, dict]:
        return {"container-one": {"CPUPerc": "0.25%", "MemUsage": "100 MB / 1 GB", "MemPerc": "9.77%"}}

    def logs(self, container: str, *, tail: int) -> str:
        return f"{container} tail={tail}"

    def container_action(self, container: str, action: str) -> dict:
        self.actions.append((container, action))
        return {"exitCode": 0, "output": "ok"}

    def pull_image(self, image: str) -> dict:
        return {"exitCode": 0, "output": image}


class InfraServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = pathlib.Path(self.temp_dir.name)
        self.runners: dict[str, FakeRunner] = {}

        def factory(host: dict) -> FakeRunner:
            return self.runners.setdefault(str(host["id"]), FakeRunner())

        self.service = InfraService(
            data_dir=self.data_dir,
            runner_factory=factory,
            master_key="test-master-key",
        )
        self.host = self.service.save_host(
            {
                "id": "nas01",
                "name": "NAS",
                "address": "192.168.5.9",
                "port": 22,
                "username": "root",
                "authMode": "password",
                "password": "secret-value",
            }
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_credentials_are_encrypted_and_redacted(self) -> None:
        raw = json.loads((self.data_dir / "infra_config.json").read_text(encoding="utf-8"))
        saved = raw["hosts"][0]
        self.assertNotEqual(saved["passwordEncrypted"], "secret-value")
        public = next(item for item in self.service.public_config()["hosts"] if item["id"] == "nas01")
        self.assertTrue(public["hasPassword"])
        self.assertNotIn("passwordEncrypted", public)

    def test_cipher_requires_master_key(self) -> None:
        cipher = InfraCredentialCipher("")
        with self.assertRaises(InfraError) as context:
            cipher.encrypt("secret")
        self.assertEqual(context.exception.code, "master_key_required")

    def test_host_status_parses_remote_metrics(self) -> None:
        self.runners["nas01"] = FakeRunner(
            {
                "printf 'hostname": {
                    "exitCode": 0,
                    "stdout": (
                        "hostname\tnas\nkernel\tLinux 6.1 x86_64\ncpuCount\t4\n"
                        "load\t0.10 0.20 0.30\nuptime\t3600.2\n"
                        "memory\t1000 400\ndisk\t5000 2000 3000\n"
                        "docker\t27.1\ncompose\t2.29\n"
                    ),
                    "stderr": "",
                }
            }
        )
        status = self.service.host_status("nas01")
        self.assertEqual(status["hostname"], "nas")
        self.assertEqual(status["memory"]["used"], 600)
        self.assertEqual(status["disk"]["available"], 3000)
        self.assertEqual(status["dockerVersion"], "27.1")

    def test_inventory_supports_json_lines_and_arrays(self) -> None:
        self.runners["nas01"] = FakeRunner(
            {
                "docker ps": {"exitCode": 0, "stdout": '{"Names":"emby","State":"running"}\n', "stderr": ""},
                "docker image": {"exitCode": 0, "stdout": '[{"Repository":"emby","Tag":"latest"}]', "stderr": ""},
                "docker compose": {"exitCode": 0, "stdout": '[{"Name":"media","Status":"running(1)"}]', "stderr": ""},
                "docker stats": {"exitCode": 0, "stdout": '{"Name":"emby","Container":"abc","CPUPerc":"0.24%","MemUsage":"122 MiB / 1 GiB","MemPerc":"11.9%"}\n', "stderr": ""},
            }
        )
        inventory = self.service.docker_inventory("nas01")
        stats = self.service.docker_stats("nas01")
        self.assertEqual(inventory["containers"][0]["Names"], "emby")
        self.assertNotIn("CPUPerc", inventory["containers"][0])
        self.assertEqual(stats["stats"][0]["CPUPerc"], "0.24%")
        self.assertEqual(stats["stats"][0]["MemPerc"], "11.9%")
        self.assertEqual(inventory["images"][0]["Tag"], "latest")
        self.assertEqual(inventory["compose"][0]["Name"], "media")

    def test_local_docker_stats_calculates_cpu_and_memory(self) -> None:
        stats = LocalDockerClient._container_stats(
            {
                "cpu_stats": {"system_cpu_usage": 2000, "online_cpus": 2, "cpu_usage": {"total_usage": 1200}},
                "precpu_stats": {"system_cpu_usage": 1000, "cpu_usage": {"total_usage": 1000}},
                "memory_stats": {"usage": 600, "limit": 1000, "stats": {"inactive_file": 100}},
                "pids_stats": {"current": 7},
            }
        )
        self.assertEqual(stats["CPUPerc"], "40.00%")
        self.assertEqual(stats["MemoryUsageBytes"], 500)
        self.assertEqual(stats["MemPerc"], "50.00%")
        self.assertEqual(stats["PIDs"], 7)

    def test_container_target_rejects_shell_injection(self) -> None:
        with self.assertRaises(InfraError):
            self.service.submit_container_action("nas01", "emby; reboot", "restart")
        self.assertNotIn("nas01", self.runners)

    def test_compose_update_is_queued_and_finishes(self) -> None:
        project = self.service.save_project(
            {"id": "media", "hostId": "nas01", "name": "media", "composePath": "/volume1/docker/media/compose.yml"}
        )
        operation = self.service.submit_compose_action(project["id"], "update")
        deadline = time.monotonic() + 2
        current = {}
        while time.monotonic() < deadline:
            current = next(item for item in self.service.operations.list() if item["id"] == operation["id"])
            if current["status"] in {"success", "failed"}:
                break
            time.sleep(0.02)
        self.assertEqual(current["status"], "success")
        self.assertIn("docker compose -f", self.runners["nas01"].commands[-1])
        self.assertIn("pull", self.runners["nas01"].commands[-1])

    def test_local_docker_is_automatically_discovered_and_operated(self) -> None:
        local = FakeLocalDocker()
        self.service._local_docker = lambda: local  # type: ignore[method-assign]
        config = self.service.public_config()
        self.assertEqual(config["hosts"][0]["id"], "local-docker")
        self.assertEqual(self.service.host_status("local-docker")["hostname"], "nas-local")
        self.assertEqual(self.service.docker_inventory("local-docker")["containers"][0]["Names"], "emby")
        self.assertEqual(self.service.docker_stats("local-docker")["stats"][0]["CPUPerc"], "0.25%")
        operation = self.service.submit_container_action("local-docker", "emby", "restart")
        deadline = time.monotonic() + 2
        current = {}
        while time.monotonic() < deadline:
            current = next(item for item in self.service.operations.list() if item["id"] == operation["id"])
            if current["status"] in {"success", "failed"}:
                break
            time.sleep(0.02)
        self.assertEqual(current["status"], "success")
        self.assertEqual(local.actions, [("emby", "restart")])


if __name__ == "__main__":
    unittest.main()
