# openCypher M23 conformance

The default `full` suite runs 3,896 active scenarios across all 220 feature
files at the upstream openCypher `1.0.0-M23` revision
`007895aff5f33097d67b2e48a0a2babd6bd18590`. The runner verifies the feature
digest and the two named graph scripts. It records the one upstream `@ignore`
scenario as excluded. Use `--suite legacy` to reproduce the earlier selection
of 1,747 active scenarios across 70 graph, pattern, boolean, conditional,
quantifier, MATCH, MERGE, SET and ORDER BY features.

The tests create and drop a fresh graph namespace for each scenario. The
client submits setup, tested statements, control statements and graph
snapshots through the same driver. The runner verifies column names,
typed values, row multiplicity, row order where requested, path direction,
graph side effects, and error category, phase and detail.

## Set up

Use JDK 17, Python 3.9 or later, and Docker for MongoDB, MonetDB, Neo4j and
PostgreSQL. HSQLDB and File are embedded. The Polypheny Docker connector
must be reachable from the DB process for Docker-backed stores.

```sh
git clone --branch 1.0.0-M23 --depth 1 https://github.com/opencypher/openCypher.git
python3 -m venv .venv-m23
. .venv-m23/bin/activate
python -m pip install -r tools/opencypher-tck/requirements.txt
python -m pip install 'polypheny @ git+https://github.com/polypheny/Polypheny-Connector-Python.git@99e3934ff4c05b56457fe2bfd4abc143329d0b75'
./gradlew :dbms:shadowJar -PwithoutpullingUi=true
```

See [native driver builds](drivers/usage.md) for driver revisions and build
commands. JDBC 3.3 from the Prism 2.4 follow-up supplies the error details
required by strict mode.

The full suite supplies table-backed procedures to its CALL scenarios through
a test-only provider. Compile that provider against the server jar.

```sh
DBMS_JAR="$PWD/dbms/build/libs/dbms-0.10.1-SNAPSHOT.jar"
TCK_PROCEDURE_CLASSES=$(mktemp -d)
javac -cp "$DBMS_JAR" -d "$TCK_PROCEDURE_CLASSES" \
    tools/opencypher-tck/procedures/org/polypheny/tck/FixtureProcedureProvider.java
cp -R tools/opencypher-tck/procedures/META-INF "$TCK_PROCEDURE_CLASSES/"
```

`matrix.py --procedure-fixture-classpath` loads the provider and creates an
isolated fixture directory for each store. For a standalone fixture server,
add the provider to the Java classpath when launching `org.polypheny.db.PolyphenyDb`
and give the server and runner the same existing private directory through
`POLYPHENY_TCK_PROCEDURES_DIR`.

## Run one driver against a test instance

```sh
export M23_HOST=127.0.0.1 M23_PORT=20590 M23_USERNAME=pa M23_PASSWORD=
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
python tools/opencypher-tck/run.py --features openCypher/tck/features \
    --server-version "$SERVER_REVISION" --driver-version "$DRIVER_REVISION" \
    --store hsqldb --store-version 2.7.3 --jsonl m23-python-hsqldb.jsonl
```

The default driver is Python. Pass `--driver-command` to use one of the
other adapters. The test bridge defaults to the pure Python Protobuf
implementation so it can decode the depth-40 map in `Literals8`. The
connector's regular Protobuf 5.27.2 upb decoder rejected this shape at depth 32.
Append feature path globs to run a subset of the selected suite. Use `--list`
to validate the fixture digests, step forms and value literals without
connecting. `--legacy-errors` reproduces the historical check for a raised
error and is reserved for baseline comparisons.

## Run the store and driver matrix

Use a JSON driver manifest. Each entry supplies a name, an exact version or
revision, a command as an array of arguments, and optional environment variables.
Add all six driver commands from the driver build guide.

```json
[
  {
    "name": "python",
    "version": "99e3934ff4c05b56457fe2bfd4abc143329d0b75",
    "command": ["python", "tools/opencypher-tck/drivers/python.py"],
    "env": {"PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION": "python"}
  }
]
```

The [store manifest](stores-m23.json) pins the two embedded stores and the
four Docker images used for the acceptance run. The adapters request local
image tags. On the dedicated Docker daemon used by the Polypheny connector,
pull the manifest's pinned images and assign the adapter tags before starting
the launcher. MonetDB requires Linux amd64 or amd64 emulation.

```sh
python - <<'PY'
import json
from pathlib import Path
import subprocess

stores = json.loads(Path("tools/opencypher-tck/stores-m23.json").read_text())
tags = {
    "mongodb": "polypheny/mongo:latest",
    "monetdb": "polypheny/monet:latest",
    "neo4j": "polypheny/neo:latest",
    "postgresql": "polypheny/postgres-pgvector-postgis:17-debian",
}
for store, tag in tags.items():
    image = stores[store]
    platform = ["--platform", "linux/amd64"] if store == "monetdb" else []
    subprocess.run(["docker", "pull", *platform, image], check=True)
    subprocess.run(["docker", "image", "tag", image, tag], check=True)
PY
```

The runner verifies each launched Docker store against the manifest before
starting its clients. Set `DOCKER_HOST`, if needed, so the Docker CLI used by
the runner inspects the same daemon as the Polypheny connector.

```sh
python tools/opencypher-tck/matrix.py \
    --jar dbms/build/libs/dbms-0.10.1-SNAPSHOT.jar \
    --features openCypher/tck/features --drivers drivers.json \
    --stores tools/opencypher-tck/stores-m23.json \
    --procedure-fixture-classpath "$TCK_PROCEDURE_CLASSES" \
    --server-version "$SERVER_REVISION" --out m23-results
```

The launcher creates a fresh DB profile for each store and verifies the
sole writable adapter through the catalog before running the clients.
It saves the DB jar's SHA-256, the observed adapter and image identity, a
server log, and text and JSONL scenario reports. Each JSONL scenario uses
the upstream path and the scenario and example line numbers. The aggregated
`matrix.jsonl` marks an entry passed only when all active scenarios in the
requested suite pass. Use a dedicated host or CI worker with the regular Polypheny HTTP
and REST ports free. Prism clients use the launcher port, configurable via
`--prism-port`. If the host requires a shorter temporary directory for
Unix sockets, set `--fixture-root` accordingly. To run several store groups
on the same Unix host, give each runner distinct `--prism-port` and
`--web-port` values and pass the same `--startup-lock` file. The fixture
then releases the default query interfaces after configuring its test port.

For a containerized driver bridge, put `docker run` or `docker container run`
directly in the driver's command array and keep stdin open. This lets the
launcher identify and remove its bridge container during teardown.
