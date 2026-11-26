#!/usr/bin/env python3
"""Run the unchanged openCypher 1.0.0-M23 scenarios."""

import argparse
from collections import Counter
from contextlib import ExitStack, contextmanager
from fnmatch import fnmatchcase
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
from uuid import uuid4

from gherkin.parser import Parser
from gherkin.pickles.compiler import Compiler

from errors import classify
from values import normalize, parse_value, wire_json


FEATURE_DIRS = (
    "expressions/pattern", "expressions/graph", "expressions/boolean",
    "expressions/conditional", "expressions/quantifier",
    "clauses/match", "clauses/match-where", "clauses/merge", "clauses/set",
    "clauses/return-orderby", "clauses/with-orderBy",
)
ERROR_STEP = re.compile(r"an? (\w+) should be raised at (runtime|compile time|any time): (\w+|\*)")
NAMED_GRAPH_STEP = re.compile(r"the (binary-tree-[12]) graph")
PROCEDURE_STEP = re.compile(r"there exists a procedure (.+?)\s*:")
RESULT_STEPS = {
    "the result should be, in any order:": (False, False),
    "the result should be, in order:": (True, False),
    "the result should be (ignoring element order for lists):": (False, True),
    "the result should be, in order (ignoring element order for lists):": (True, True),
}
QUERY_STEPS = {"having executed:", "executing query:", "executing control query:"}
SUITE_TOTALS = {"full": 3896, "legacy": 1747}
TOTAL = SUITE_TOTALS["full"]
TCK_COMMIT = "007895aff5f33097d67b2e48a0a2babd6bd18590"
FEATURE_DIGESTS = {
    "full": "725229bbd78131e04d976cf5380ca2d7e45403aea98ecba1525136f2e86b7a10",
    "legacy": "2811f813ea96ab0f6625fc42ed3d3b614221ed58b8bc237c679ec00892fda9af",
}
NAMED_GRAPH_DIGESTS = {
    "binary-tree-1": "fbcada6966edb9e2d66b1a11a4f8a4c906a9da6afd622640ab08c686962d42da",
    "binary-tree-2": "923bcaf5686ea9051f46ad5c440a286381fd111daad5dbd6377aa3edd7dbfc4c",
}
PROCEDURE_FIXTURES_ENV = "POLYPHENY_TCK_PROCEDURES_DIR"
DISCONNECTED_MESSAGES = {
    "Connection is closed.", "driver: bad connection", "context deadline exceeded",
    "The Prism connection timed out while reading.",
}


class DriverError(Exception):
    def __init__(self, details):
        self.details = details
        super().__init__(f"{details.get('code') or details.get('class') or 'Driver error'}: {details['message']}")


class DriverProtocolError(RuntimeError):
    pass


class ScenarioConnectionError(AssertionError):
    def __init__(self, message, namespace):
        super().__init__(message)
        self.namespace = namespace


class Driver:
    def __init__(self, process):
        self.process = process
        self.had_response = False

    def call(self, op, **arguments):
        stdin, stdout = self.process.stdin, self.process.stdout
        try:
            print(wire_json(dict(op=op, **arguments)), file=stdin, flush=True)
            response = stdout.readline()
        except OSError as error:
            raise DriverProtocolError(f"Could not communicate with driver during {op}: {error}") from error
        if not response:
            raise DriverProtocolError(f"Driver disconnected during {op} with status {self.process.poll()}")
        try:
            message = json.loads(response)
        except ValueError as error:
            raise DriverProtocolError(f"Driver returned invalid JSON during {op}: {error}") from error
        if "error" in message:
            error = DriverError(message["error"])
            if (error.details.get("code") or "").startswith("08") or error.details["message"] in DISCONNECTED_MESSAGES:
                raise DriverProtocolError(str(error))
            self.had_response = True
            raise error
        self.had_response = True
        return message

    def query(self, namespace, statement, params=None):
        result = self.call("query", language="cypher" if namespace else "sql",
                           namespace=namespace, statement=statement, params=params)
        return result["columns"], result["rows"]

    @classmethod
    @contextmanager
    def connect(cls, command):
        with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True, encoding="utf-8") as process:
            try:
                yield cls(process)
            finally:
                try:
                    process.stdin.close()
                except BrokenPipeError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def location_index(value):
    if isinstance(value, dict):
        if "id" in value and "location" in value:
            yield value["id"], value["location"]["line"]
        for child in value.values():
            yield from location_index(child)
    elif isinstance(value, list):
        for child in value:
            yield from location_index(child)


def scenarios(root, patterns, suite="full"):
    files = sorted(root.rglob("*.feature") if suite == "full" else
                   (path for directory in FEATURE_DIRS for path in (root / directory).glob("*.feature")))
    digest = sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(sha256(path.read_bytes().replace(b"\r\n", b"\n")).digest())
    if digest.hexdigest() != FEATURE_DIGESTS[suite]:
        raise ValueError(f"{suite} feature files in {root} do not match openCypher 1.0.0-M23 ({TCK_COMMIT})")
    for path in files:
        relative = path.relative_to(root).as_posix()
        if patterns and not any(fnmatchcase(relative, pattern) for pattern in patterns):
            continue
        document = Parser().parse(path.read_text(encoding="utf-8"))
        document["uri"] = relative
        locations = dict(location_index(document))
        for scenario in Compiler().compile(document):
            scenario["key"] = relative + ":" + ":".join(str(locations[i]) for i in scenario["astNodeIds"])
            if any(tag["name"] == "@ignore" for tag in scenario["tags"]):
                yield scenario, True
            else:
                yield scenario, False


def table(step):
    return [[cell["value"] for cell in row["cells"]] for row in step["argument"]["dataTable"]["rows"]]


def named_graph_queries(features):
    graphs = {}
    for name, expected in NAMED_GRAPH_DIGESTS.items():
        script = features.parent / "graphs" / name / f"{name}.cypher"
        data = script.read_bytes().replace(b"\r\n", b"\n")
        if sha256(data).hexdigest() != expected:
            raise ValueError(f"Named graph {script} does not match openCypher 1.0.0-M23 ({TCK_COMMIT})")
        graphs[name] = [query.strip() for query in data.decode("utf-8").split(";") if query.strip()]
    return graphs


def parse_records(step):
    columns, *rows = table(step)
    if any(len(row) != len(columns) for row in rows):
        raise ValueError(f"Rows do not match the header in {step['text']!r}")
    if columns == [""] and not rows:
        columns = []
    return columns, [[parse_value(item) for item in row] for row in rows]


def validate_literals(scenario):
    for step in scenario["steps"]:
        task = step["text"]
        if task == "parameters are:":
            for _, value in table(step):
                parse_value(value)
        elif task in RESULT_STEPS or PROCEDURE_STEP.fullmatch(task):
            parse_records(step)
        elif task in QUERY_STEPS:
            if not step.get("argument", {}).get("docString", {}).get("content", "").strip():
                raise ValueError(f"Expected a query for {task!r}")
        elif task == "the side effects should be:":
            for _, count in table(step):
                int(count)
        elif (task not in {"an empty graph", "any graph", "the result should be empty", "no side effects"}
              and not NAMED_GRAPH_STEP.fullmatch(task) and not ERROR_STEP.fullmatch(task)):
            raise ValueError(f"Unrecognized step {task!r}")


def register_procedure(file, signature, step):
    columns, rows = parse_records(step)
    if file is None:
        raise ValueError("Procedure setup requires the server's M23 test fixture provider")
    registered = json.loads(file.read_text(encoding="utf-8")) if file.exists() else {"procedures": []}
    registered["procedures"].append({"signature": signature, "columns": columns, "rows": rows})
    temporary = file.with_suffix(".tmp")
    try:
        temporary.write_text(wire_json(registered), encoding="utf-8")
        temporary.replace(file)
    finally:
        temporary.unlink(missing_ok=True)


def snapshot(driver, namespace):
    state = {kind: set() for kind in ("nodes", "relationships", "properties", "labels")}
    for kind, statement in (("nodes", "MATCH (n) RETURN n"),
                            ("relationships", "MATCH ()-[r]->() RETURN r")):
        _, rows = driver.query(namespace, statement)
        for (element,) in rows:
            expected_kind = "node" if kind == "nodes" else "edge"
            if element.get("$type") != expected_kind:
                raise ValueError(f"Expected {expected_kind} from {statement}, got {element!r}")
            identity = element["id"]
            state[kind].add(identity)
            if kind == "nodes":
                state["labels"].update(element["labels"])
            for key, value in element["properties"].items():
                state["properties"].add((kind, identity, key, normalize(value)))
    return state


def effects(before, after):
    return {
        sign + kind: count
        for kind in before
        for sign, count in (("+", len(after[kind] - before[kind])), ("-", len(before[kind] - after[kind])))
        if count
    }


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def error_matches(expected, actual):
    kind, phase, detail = expected
    actual_kind, actual_phase, actual_detail = actual
    return (kind == actual_kind and
            (phase == actual_phase or phase == "any time" and actual_phase in {"compile time", "runtime"}) and
            (detail == actual_detail or detail == "*"))


def run(driver, scenario, legacy_errors, named_graphs=None, procedure_fixtures=None):
    namespace = f"tck_{uuid4().hex}"
    procedure_file = procedure_fixtures / f"{namespace}.json" if procedure_fixtures is not None else None
    params = None
    before = None
    columns, rows = [], []
    query_error = None
    statement = None
    created = False
    try:
        driver.query(None, f"CREATE GRAPH NAMESPACE {namespace}")
        created = True
        for step in scenario["steps"]:
            task = step["text"]
            if task == "an empty graph":
                continue
            if (graph := NAMED_GRAPH_STEP.fullmatch(task)) or task == "any graph":
                if named_graphs is not None:
                    graph_name = graph[1] if graph else "binary-tree-1"
                    for statement in named_graphs[graph_name]:
                        driver.query(namespace, statement)
                    driver.call("commit")
                elif graph:
                    raise ValueError(f"Missing pinned fixture for {task!r}")
            elif procedure := PROCEDURE_STEP.fullmatch(task):
                register_procedure(procedure_file, procedure[1], step)
            elif task == "having executed:":
                statement = step["argument"]["docString"]["content"]
                driver.query(namespace, statement)
                driver.call("commit")
            elif task == "parameters are:":
                params = {key: parse_value(value) for key, value in table(step)}
            elif task in ("executing query:", "executing control query:"):
                if query_error:
                    raise query_error
                statement = step["argument"]["docString"]["content"]
                if task == "executing query:" or named_graphs is None:
                    before = snapshot(driver, namespace)
                columns, rows = [], []
                try:
                    columns, rows = driver.query(namespace, statement, params)
                    driver.call("commit")
                except DriverError as error:
                    query_error = error
                    try:
                        driver.call("rollback")
                    except (DriverError, DriverProtocolError) as rollback_error:
                        raise ScenarioConnectionError(f"{error} | {statement!r} | Rollback failed: {rollback_error}", namespace) from error
            elif error := ERROR_STEP.fullmatch(task):
                check(query_error is not None, f"Expected {task}, got {rows!r}")
                check(snapshot(driver, namespace) == before, f"Graph changed on error {query_error}")
                if not legacy_errors:
                    actual = classify(query_error.details)
                    check(error_matches(error.groups(), actual), f"Expected {task}, got {actual!r}: {query_error}")
                query_error = None
            elif query_error:
                raise query_error
            elif task == "the result should be empty":
                check(not rows, f"Expected empty result, got {rows!r}")
            elif task in RESULT_STEPS:
                ordered, unordered_lists = RESULT_STEPS[task]
                expected_columns, data = parse_records(step)
                expected = [tuple(normalize(item, unordered_lists) for item in row) for row in data]
                actual = [tuple(normalize(item, unordered_lists) for item in row) for row in rows]
                check(expected_columns == columns, f"Columns expected {expected_columns!r}, got {columns!r}")
                check((expected == actual) if ordered else (Counter(expected) == Counter(actual)),
                      f"{'Ordered result' if ordered else 'Result'} expected {expected!r}, got {actual!r}")
            elif task in ("the side effects should be:", "no side effects"):
                expected = {key: int(value) for key, value in table(step) if int(value)} if task.endswith(":") else {}
                actual = effects(before, snapshot(driver, namespace))
                check(expected == actual, f"Side effects expected {expected!r}, got {actual!r}")
            else:
                raise ValueError(f"Unrecognized step {task!r}")
        if query_error:
            raise query_error
    except DriverProtocolError as error:
        raise ScenarioConnectionError(f"{error} | {statement!r}", namespace) from error
    except ScenarioConnectionError:
        raise
    except (AssertionError, DriverError, ValueError) as error:
        raise AssertionError(f"{error} | {statement!r}") from error
    finally:
        prior = sys.exc_info()[1]
        try:
            if created and not isinstance(prior, ScenarioConnectionError):
                driver.call("rollback")
                driver.query(None, f"DROP NAMESPACE {namespace}")
        except (DriverError, DriverProtocolError) as error:
            message = f"{prior} | Cleanup failed: {error}" if prior else f"Cleanup failed: {error}"
            raise ScenarioConnectionError(message, namespace) from (prior or error)
        finally:
            if procedure_file is not None:
                procedure_file.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", required=True, type=Path, help="Path to the pinned tck/features directory")
    parser.add_argument("--suite", default="full", choices=SUITE_TOTALS, help="Pinned M23 scenario suite")
    parser.add_argument("--driver-command", default=shlex.join([sys.executable, str(Path(__file__).parent / "drivers/python.py")]))
    parser.add_argument("--driver-version", help="Version or commit of the driver under test")
    parser.add_argument("--server-version", help="Version or commit of the Polypheny-DB instance")
    parser.add_argument("--store", help="Name of the sole writable store on the instance")
    parser.add_argument("--store-version", help="Version or image digest of the store under test")
    parser.add_argument("--jsonl", type=Path, help="Write the scenario results and run metadata to this file")
    parser.add_argument("--list", action="store_true", help="Validate TCK literals and list scenario keys without connecting")
    parser.add_argument("--legacy-errors", action="store_true", help="Reproduce the historical check for any raised error")
    parser.add_argument("patterns", nargs="*", help="Optional feature path globs from the selected groups")
    args = parser.parse_args()

    try:
        selection = list(scenarios(args.features, args.patterns, args.suite))
        named_graphs = named_graph_queries(args.features) if args.suite == "full" else None
    except (ValueError, OSError) as error:
        parser.error(str(error))
    active = sum(not ignored for _, ignored in selection)
    total = SUITE_TOTALS[args.suite]
    if not active or (not args.patterns and active != total):
        parser.error(f"Found {active} active scenarios. Expected {total} in the {args.suite} suite from 1.0.0-M23.")
    procedure_dir = os.environ.get(PROCEDURE_FIXTURES_ENV)
    procedure_fixtures = Path(procedure_dir) if procedure_dir else None
    if procedure_fixtures is not None and not procedure_fixtures.is_dir():
        parser.error(f"{PROCEDURE_FIXTURES_ENV} must name an existing directory")
    counts = Counter(passed=0, failed=0, skipped=0, excluded=0)
    metadata = {
        "event": "run", "tck": "1.0.0-M23", "tck_commit": TCK_COMMIT, "server_version": args.server_version,
        "store": args.store, "store_version": args.store_version, "driver_version": args.driver_version,
        "suite": args.suite, "mode": "validation" if args.list else "execution",
        "active_scenarios": active, "legacy_errors": args.legacy_errors,
    }

    @contextmanager
    def log():
        if args.jsonl:
            with args.jsonl.open("w", encoding="utf-8") as output:
                yield lambda record: print(wire_json(record), file=output, flush=True)
        else:
            yield lambda record: None

    driver_command = shlex.split(args.driver_command)
    with log() as record, ExitStack() as sessions:
        driver = None
        record(metadata)
        for scenario, ignored in selection:
            key, name = scenario["key"], scenario["name"]
            error = None
            try:
                if args.list:
                    validate_literals(scenario)
                elif not ignored:
                    if driver is None:
                        driver = sessions.enter_context(Driver.connect(driver_command))
                    run(driver, scenario, args.legacy_errors, named_graphs, procedure_fixtures)
            except ScenarioConnectionError as failure:
                if not driver.had_response:
                    raise
                status, error = "failed", str(failure)
                sessions.close()
                driver = sessions.enter_context(Driver.connect(driver_command))
                try:
                    driver.query(None, f"DROP NAMESPACE IF EXISTS {failure.namespace}")
                except (DriverError, DriverProtocolError) as cleanup_error:
                    error += f" | Cleanup retry failed: {cleanup_error}"
                    sessions.close()
                    driver = None
            except (AssertionError, DriverError, ValueError) as failure:
                status, error = "failed", str(failure)
            else:
                status = "excluded" if ignored else "passed"
            counts[status] += 1
            print(f"{status.upper()} {key} {name}" + (f" | {error!r}" if error else ""), flush=True)
            record({"event": "scenario", "key": key, "name": name, "status": status, "error": error})
        record(dict(event="summary", **counts))
    print(wire_json(dict(counts)))
    return int(bool(counts["failed"]))


if __name__ == "__main__":
    sys.exit(main())
