#!/usr/bin/env python3
"""Run the selected, unchanged openCypher 1.0.0-M23 scenarios."""

import argparse
from collections import Counter
from contextlib import contextmanager
from fnmatch import fnmatchcase
from hashlib import sha256
import json
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
ERROR_STEP = re.compile(r"an? (\w+) should be raised at (runtime|compile time): (\w+)")
TOTAL = 1747
TCK_COMMIT = "007895aff5f33097d67b2e48a0a2babd6bd18590"
FEATURE_DIGEST = "2811f813ea96ab0f6625fc42ed3d3b614221ed58b8bc237c679ec00892fda9af"


class DriverError(Exception):
    def __init__(self, details):
        self.details = details
        super().__init__(f"{details.get('code') or details.get('class') or 'Driver error'}: {details['message']}")


class Driver:
    def __init__(self, process):
        self.process = process

    def call(self, op, **arguments):
        stdin, stdout = self.process.stdin, self.process.stdout
        print(wire_json(dict(op=op, **arguments)), file=stdin, flush=True)
        response = stdout.readline()
        if not response:
            raise RuntimeError(f"Driver exited with status {self.process.poll()}")
        message = json.loads(response)
        if "error" in message:
            raise DriverError(message["error"])
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
                process.stdin.close()
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


def scenarios(root, patterns):
    files = sorted(path for directory in FEATURE_DIRS for path in (root / directory).glob("*.feature"))
    digest = sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(sha256(path.read_bytes().replace(b"\r\n", b"\n")).digest())
    if digest.hexdigest() != FEATURE_DIGEST:
        raise ValueError(f"Selected feature files in {root} do not match openCypher 1.0.0-M23 ({TCK_COMMIT})")
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


def validate_literals(scenario):
    for step in scenario["steps"]:
        task = step["text"]
        if task == "parameters are:":
            values = [value for _, value in table(step)]
        elif task.startswith("the result should be") and task != "the result should be empty":
            values = [value for row in table(step)[1:] for value in row]
        else:
            continue
        for value in values:
            parse_value(value)


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


def run(driver, scenario, legacy_errors):
    namespace = f"tck_{uuid4().hex}"
    params = None
    before = None
    columns, rows = [], []
    query_error = None
    statement = None
    driver.query(None, f"CREATE GRAPH NAMESPACE {namespace}")
    try:
        for step in scenario["steps"]:
            task = step["text"]
            if task in ("an empty graph", "any graph"):
                continue
            if task == "having executed:":
                statement = step["argument"]["docString"]["content"]
                driver.query(namespace, statement)
                driver.call("commit")
            elif task == "parameters are:":
                params = {key: parse_value(value) for key, value in table(step)}
            elif task in ("executing query:", "executing control query:"):
                if query_error:
                    raise query_error
                statement = step["argument"]["docString"]["content"]
                before = snapshot(driver, namespace)
                columns, rows = [], []
                try:
                    columns, rows = driver.query(namespace, statement, params)
                    driver.call("commit")
                except DriverError as error:
                    query_error = error
                    driver.call("rollback")
            elif error := ERROR_STEP.fullmatch(task):
                check(query_error is not None, f"Expected {task}, got {rows!r}")
                check(snapshot(driver, namespace) == before, f"Graph changed on error {query_error}")
                if not legacy_errors:
                    actual = classify(query_error.details)
                    check(error.groups() == actual, f"Expected {task}, got {query_error}")
                query_error = None
            elif query_error:
                raise query_error
            elif task == "the result should be empty":
                check(not rows, f"Expected empty result, got {rows!r}")
            elif task.startswith("the result should be"):
                unordered_lists = "ignoring element order for lists" in task
                expected_columns, *data = table(step)
                expected = [tuple(normalize(parse_value(item), unordered_lists) for item in row) for row in data]
                actual = [tuple(normalize(item, unordered_lists) for item in row) for row in rows]
                check(expected_columns == columns, f"Columns expected {expected_columns!r}, got {columns!r}")
                ordered = "in order" in task
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
    except (AssertionError, DriverError, ValueError) as error:
        raise AssertionError(f"{error} | {statement!r}") from error
    finally:
        driver.call("rollback")
        driver.query(None, f"DROP NAMESPACE {namespace}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", required=True, type=Path, help="Path to the pinned tck/features directory")
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
        selection = list(scenarios(args.features, args.patterns))
    except ValueError as error:
        parser.error(str(error))
    active = sum(not ignored for _, ignored in selection)
    if not active or (not args.patterns and active != TOTAL):
        parser.error(f"Found {active} active scenarios. Expected {TOTAL} in the full selection from 1.0.0-M23.")
    counts = Counter(passed=0, failed=0, skipped=0, excluded=0)
    metadata = {
        "event": "run", "tck": "1.0.0-M23", "tck_commit": TCK_COMMIT, "server_version": args.server_version,
        "store": args.store, "store_version": args.store_version, "driver_version": args.driver_version,
        "active_scenarios": active, "legacy_errors": args.legacy_errors,
    }

    @contextmanager
    def log():
        if args.jsonl:
            with args.jsonl.open("w", encoding="utf-8") as output:
                yield lambda record: print(wire_json(record), file=output, flush=True)
        else:
            yield lambda record: None

    @contextmanager
    def connection():
        if args.list:
            yield None
        else:
            with Driver.connect(shlex.split(args.driver_command)) as driver:
                yield driver

    with log() as record, connection() as driver:
        record(metadata)
        for scenario, ignored in selection:
            key, name = scenario["key"], scenario["name"]
            error = None
            if ignored:
                status = "excluded"
            else:
                try:
                    if args.list:
                        validate_literals(scenario)
                    else:
                        run(driver, scenario, args.legacy_errors)
                except (AssertionError, DriverError, ValueError) as failure:
                    status, error = "failed", str(failure)
                else:
                    status = "passed"
            counts[status] += 1
            print(f"{status.upper()} {key} {name}" + (f" | {error!r}" if error else ""), flush=True)
            record({"event": "scenario", "key": key, "name": name, "status": status, "error": error})
        record(dict(event="summary", **counts))
    print(wire_json(dict(counts)))
    return int(bool(counts["failed"]))


if __name__ == "__main__":
    sys.exit(main())
