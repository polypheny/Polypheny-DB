#!/usr/bin/env python3
"""Start a fresh Polypheny-DB per store and run the selected drivers against it."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from urllib.request import Request, urlopen
from zipfile import ZipFile

from run import TOTAL
from values import wire_json


SUITE = Path(__file__).resolve().parent
STORE_NAMES = {
    "hsqldb": "HSQLDB", "file": "File", "mongodb": "MongoDB",
    "monetdb": "MonetDB", "postgresql": "PostgreSQL", "neo4j": "Neo4j",
}
EMBEDDED = {"hsqldb", "file"}


def wait_for_fixture(process, path, log, timeout):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Polypheny exited with code {process.returncode}. See {log}")
        contents = log.read_text(encoding="utf-8", errors="replace")
        if "ERROR [main]: Error:" in contents:
            raise RuntimeError(f"Polypheny reported a startup error. See {log}")
        if path.exists() and "Polypheny-DB successfully started" in contents:
            return
        sleep(0.25)
    raise RuntimeError(f"Polypheny did not start within {timeout}s. See {log}")


def bootstrap(socket_path, port, web_port, store, release_default_ports):
    import polypheny

    try:
        with closing(polypheny.connect(str(socket_path), transport="unix", username="pa", password="")) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(f"""ALTER INTERFACES ADD 'm23_tcp' USING 'Prism Interface (Plain transport)' WITH '{{"port":"{port}"}}'""")
                if release_default_ports:
                    for interface in ("http interface", "rest interface", "prism interface (plain transport)"):
                        cursor.execute(f"ALTER INTERFACES DROP '{interface}'")
    except polypheny.Error as error:
        raise RuntimeError(f"Could not configure the Prism test port: {error}") from error
    request = Request(f"http://127.0.0.1:{web_port}/getSnapshot", data=b"-1",
                      headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=10) as response:
        snapshot = json.load(response)
    writable = [
        {k: adapter[k] for k in ("id", "name", "adapterName", "type", "mode")}
        for adapter in snapshot["adapters"] if adapter["type"] == "STORE"
    ]
    if len(writable) != 1 or writable[0]["adapterName"] != STORE_NAMES[store]:
        raise RuntimeError(f"Expected sole writable store {store}, found {writable}")
    return writable[0]


def verify_store_image(home, adapter, expected):
    uuid = (home / ".polypheny/uuid").read_text(encoding="utf-8").strip()
    name = f"polypheny_{uuid}_{adapter['name']}"
    try:
        image = subprocess.check_output(["docker", "inspect", "--type", "container", "--format", "{{.Image}}", name],
                                        text=True, stderr=subprocess.PIPE).strip()
        digests = json.loads(subprocess.check_output(["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", image],
                                                     text=True, stderr=subprocess.PIPE)) or []
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"Could not verify the image of {name}: {error.stderr.strip()}") from error
    if expected != image and expected not in digests:
        raise RuntimeError(f"Expected image {expected} for {name}, found {image} and {digests}")
    return {"container": name, "image": image, "repo_digests": digests}


@contextmanager
def startup_lock(path):
    if path is None:
        yield
        return
    from fcntl import LOCK_EX, flock

    with path.open("ab") as lock:
        flock(lock, LOCK_EX)
        yield


@contextmanager
def fixture(args, jar, store, store_version, logs):
    with ZipFile(jar) as archive:
        settings = dict(line.split("=", 1) for line in archive.read("version.properties").decode().splitlines() if "=" in line)
    prefix = f'{settings["version"]}-{settings["hash"]}'
    server_log = logs / "server.log"
    with TemporaryDirectory(prefix="m23-", dir=args.fixture_root) as temporary:
        home = Path(temporary)
        socket_path = home / ".polypheny/polypheny-prism.sock"
        config = home / ".polypheny" / prefix / "config/polypheny.conf"
        config.parent.mkdir(parents=True)
        config.write_text(f'"runtime/webuiServerPort" = {args.web_port}\n', encoding="utf-8")
        command = [args.java, "-ea", "-Xmx2g", "-jar", str(jar), "-mode", "dev", "-daemon", "-defaultStore", store]
        if store in EMBEDDED:
            command.append("-noAutoDocker")
        environment = dict(os.environ, POLYPHENY_HOME=temporary, TZ="UTC")
        with server_log.open("w", encoding="utf-8") as output:
            process = None
            try:
                with startup_lock(args.startup_lock):
                    process = subprocess.Popen(command, cwd=home, env=environment,
                                               stdout=output, stderr=subprocess.STDOUT)
                    wait_for_fixture(process, socket_path, server_log, args.startup_timeout)
                    adapter = bootstrap(socket_path, args.prism_port, args.web_port, store, args.startup_lock is not None)
                    if store not in EMBEDDED:
                        adapter.update(verify_store_image(home, adapter, store_version))
                yield adapter
            finally:
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


def run_driver(args, store, store_version, logs, driver):
    label = driver["name"]
    detail = logs / f"{label}.jsonl"
    output_path = logs / f"{label}.log"
    command = [
        sys.executable, str(SUITE / "run.py"), "--features", str(args.features),
        "--driver-command", shlex.join(driver["command"]), "--driver-version", driver["version"],
        "--server-version", args.server_version, "--store", store, "--store-version", store_version,
        "--jsonl", str(detail),
    ]
    environment = dict(os.environ, **driver.get("env", {}))
    environment.update(M23_HOST="127.0.0.1", M23_PORT=str(args.prism_port), M23_USERNAME="pa", M23_PASSWORD="")
    environment.pop("M23_SOCKET", None)
    exit_status, reason = None, None
    with output_path.open("w", encoding="utf-8") as output:
        try:
            exit_status = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT,
                                         env=environment, timeout=args.driver_timeout).returncode
        except subprocess.TimeoutExpired:
            reason = f"Driver run exceeded {args.driver_timeout}s"
    summary = {}
    if detail.exists():
        for line in detail.read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            if event["event"] == "summary":
                summary = {key: event[key] for key in ("passed", "failed", "skipped", "excluded")}
    if not summary and reason is None:
        reason = f"Runner exited with {exit_status} before reporting a summary"
    status = "blocked" if reason else "passed" if summary["passed"] == TOTAL and exit_status == 0 else "failed"
    return {
        "store": store, "store_version": store_version, "driver": label, "driver_version": driver["version"],
        "status": status, "reason": reason, "log": str(output_path), **summary,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--drivers", type=Path, required=True, help="JSON array of drivers (name, version, command and optional env)")
    parser.add_argument("--stores", type=Path, required=True, help="JSON object mapping store names to exact versions or image digests")
    parser.add_argument("--out", type=Path, required=True, help="New directory for run, server and evidence logs")
    parser.add_argument("--server-version", required=True)
    parser.add_argument("--java", default="java")
    parser.add_argument("--fixture-root", default="/tmp", help="Short directory path for the Prism Unix socket")
    parser.add_argument("--prism-port", type=int, default=32928)
    parser.add_argument("--web-port", type=int, default=37659)
    parser.add_argument("--startup-timeout", type=int, default=180)
    parser.add_argument("--startup-lock", type=Path, help="Shared lock for concurrent fixtures using distinct Prism and web ports")
    parser.add_argument("--driver-timeout", type=int, default=1200)
    parser.add_argument("--parallel-drivers", type=int, default=2)
    args = parser.parse_args()
    args.features = args.features.resolve()
    drivers = json.loads(args.drivers.read_text(encoding="utf-8"))
    stores = json.loads(args.stores.read_text(encoding="utf-8"))
    if not stores or not stores.keys() <= STORE_NAMES.keys() or not drivers:
        parser.error("Supply known stores and at least one driver")
    driver_names = {driver["name"] for driver in drivers}
    if len(driver_names) != len(drivers) or any(not name.replace("-", "").isalnum() for name in driver_names):
        parser.error("Driver names must be distinct and contain only letters, numbers or hyphens")
    if not all(1 <= port <= 65535 for port in (args.prism_port, args.web_port)):
        parser.error("Prism and web ports must be in 1–65535")
    args.out = args.out.resolve()
    args.out.mkdir(parents=True)
    success = True
    with TemporaryDirectory(prefix="m23-jar-") as temporary, (args.out / "matrix.jsonl").open("w", encoding="utf-8") as summary:
        jar = Path(temporary) / "polypheny-db.jar"
        shutil.copyfile(args.jar, jar)
        with jar.open("rb") as source:
            digest = sha256()
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        print(wire_json({"event": "run", "server_version": args.server_version, "jar_sha256": digest.hexdigest()}),
              file=summary, flush=True)
        for store, store_version in stores.items():
            logs = args.out / store
            logs.mkdir()
            try:
                with fixture(args, jar, store, store_version, logs) as adapter:
                    (logs / "adapter.json").write_text(wire_json(adapter) + "\n", encoding="utf-8")
                    with ThreadPoolExecutor(max_workers=args.parallel_drivers) as workers:
                        results = workers.map(lambda driver: run_driver(args, store, store_version, logs, driver), drivers)
                        for result in results:
                            print(wire_json(result), file=summary, flush=True)
                            print(wire_json(result), flush=True)
                            success &= result["status"] == "passed"
            except (OSError, RuntimeError) as error:
                for driver in drivers:
                    result = {"store": store, "store_version": store_version, "driver": driver["name"],
                              "driver_version": driver["version"], "status": "blocked", "reason": str(error),
                              "log": str(logs / "server.log")}
                    print(wire_json(result), file=summary, flush=True)
                    print(wire_json(result), flush=True)
                success = False
    return int(not success)


if __name__ == "__main__":
    sys.exit(main())
