import fcntl
import os
from pathlib import Path
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from zipfile import ZipFile

import matrix


class FixtureCleanupTest(TestCase):
    def test_stops_fixture_and_removes_its_docker_containers_before_bootstrap_succeeds(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            jar = directory / "db.jar"
            with ZipFile(jar, "w") as archive:
                archive.writestr("version.properties", "version=1\nhash=revision\n")
            args = SimpleNamespace(java="java", fixture_root=temporary, web_port=37659, prism_port=32928,
                                   startup_lock=None, startup_timeout=1)
            process = Mock()
            process.poll.return_value = None

            with (patch.object(matrix.subprocess, "Popen", return_value=process) as launch,
                  patch.object(matrix, "wait_for_fixture", side_effect=RuntimeError("startup failed")),
                  patch.object(matrix, "remove_owned_containers") as remove):
                with self.assertRaisesRegex(RuntimeError, "startup failed"):
                    with matrix.fixture(args, jar, "neo4j", "digest", directory):
                        self.fail("Fixture unexpectedly started")

            process.terminate.assert_called_once()
            process.wait.assert_called_once()
            _, selector, environment, prefix = remove.call_args.args
            self.assertRegex(prefix, r"^polypheny_[0-9a-f-]{36}_$")
            self.assertEqual(selector, f"name={prefix}")
            self.assertEqual(environment["POLYPHENY_HOME"], str(launch.call_args.kwargs["cwd"]))

    def test_removes_only_container_names_with_the_fixture_prefix(self):
        prefix = "polypheny_fixture_"
        listed = subprocess.CompletedProcess([], 0, f"owned {prefix}hsqldb\nother unrelated_{prefix}hsqldb\n", "")
        removed = subprocess.CompletedProcess([], 0, "owned\n", "")
        with patch.object(matrix.subprocess, "run", side_effect=[listed, removed]) as docker:
            matrix.remove_owned_containers("docker", f"name={prefix}", {}, prefix)
        self.assertEqual(docker.call_args.args[0], ["docker", "container", "rm", "--force", "--volumes", "owned"])


class DriverCleanupTest(TestCase):
    def test_timeout_kills_a_bridge_that_ignores_sigterm(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "run.py").write_text(
                "import shlex,subprocess,sys\n"
                "subprocess.run(shlex.split(sys.argv[sys.argv.index('--driver-command') + 1]))\n",
                encoding="utf-8",
            )
            bridge = directory / "bridge.py"
            bridge.write_text(
                "import fcntl,os,pathlib,signal,sys,time\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                "with open(sys.argv[1], 'w') as lock:\n"
                "    fcntl.flock(lock, fcntl.LOCK_EX)\n"
                "    pathlib.Path(sys.argv[2]).write_text(str(os.getpid()))\n"
                "    time.sleep(60)\n",
                encoding="utf-8",
            )
            pidfile = directory / "bridge.pid"
            lockfile = directory / "bridge.lock"
            args = SimpleNamespace(features=directory, server_version="revision", prism_port=32928, driver_timeout=2)
            driver = {"name": "hung", "version": "revision",
                      "command": [sys.executable, str(bridge), str(lockfile), str(pidfile)]}
            with patch.object(matrix, "SUITE", directory):
                result = matrix.run_driver(args, "hsqldb", "version", directory, driver)

            self.assertEqual(result["status"], "blocked")
            pid = int(pidfile.read_text())
            deadline = monotonic() + 2
            with lockfile.open() as lock:
                while True:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if monotonic() >= deadline:
                            os.kill(pid, signal.SIGKILL)
                            self.fail("Timed-out native bridge still holds its lock")
                        sleep(0.02)
