from contextlib import nullcontext, redirect_stdout
import io
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

import run


class RunnerTest(TestCase):
    def test_error_wildcards_still_require_the_expected_error_type(self):
        expected = ("TypeError", "any time", "*")
        self.assertTrue(run.error_matches(expected, ("TypeError", "compile time", "InvalidArgumentType")))
        self.assertTrue(run.error_matches(expected, ("TypeError", "runtime", "SomethingElse")))
        self.assertFalse(run.error_matches(expected, ("SyntaxError", "compile time", "InvalidArgumentType")))
        self.assertFalse(run.error_matches(expected, (None, None, None)))

    def test_listing_rejects_an_unknown_step(self):
        with self.assertRaisesRegex(ValueError, "Unrecognized step"):
            run.validate_literals({"steps": [{"text": "an unsupported step"}]})

    def test_named_graph_script_is_checked_against_the_upstream_digest(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            graph = root / "graphs/binary-tree-1/binary-tree-1.cypher"
            graph.parent.mkdir(parents=True)
            graph.write_text("CREATE (:Changed);", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not match openCypher"):
                run.named_graph_queries(root / "features")

    def test_bridge_reported_disconnect_is_not_a_query_error(self):
        for details in ({"message": "socket lost", "code": "08006"}, {"message": "Connection is closed."}):
            with self.subTest(details=details):
                bridge = Mock(stdin=io.StringIO(), stdout=io.StringIO(json.dumps({"error": details}) + "\n"))
                driver = run.Driver(bridge)
                with self.assertRaisesRegex(run.DriverProtocolError, details["message"]):
                    driver.call("query")
                self.assertFalse(driver.had_response)

    def test_restart_preserves_original_diagnostic_and_continues(self):
        cases = [({"name": name, "key": name, "steps": [
            {"text": "executing query:", "argument": {"docString": {"content": "RETURN 1"}}},
            {"text": "the result should be empty"},
        ]}, False) for name in ("disconnected", "subsequent")]
        original = run.DriverError({"class": "InterfaceError", "message": "Could not parse the response"})

        def first_query(namespace, statement, params=None):
            if statement == "RETURN 1":
                raise original
            return [], []

        dead = Mock(had_response=True)
        dead.query.side_effect = first_query
        dead.call.side_effect = run.DriverError({"class": "OperationalError", "message": "Connection lost"})
        restored = Mock(had_response=True)
        restored.query.return_value = ([], [])

        with TemporaryDirectory() as directory:
            output = Path(directory) / "cases.jsonl"
            arguments = ["run.py", "--features", directory, "--suite", "legacy", "--jsonl", str(output), "fake.feature"]
            with (patch.object(sys, "argv", arguments),
                  patch.object(run, "scenarios", return_value=cases),
                  patch.object(run.Driver, "connect", side_effect=[nullcontext(dead), nullcontext(restored)]) as connect,
                  redirect_stdout(io.StringIO())):
                self.assertEqual(run.main(), 1)
            results = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(connect.call_count, 2)
        self.assertIn("Could not parse the response", results[1]["error"])
        self.assertIn("Connection lost", results[1]["error"])
        self.assertEqual(results[1]["status"], "failed")
        self.assertEqual(results[2]["status"], "passed")
        self.assertTrue(restored.query.call_args_list[0].args[1].startswith("DROP NAMESPACE IF EXISTS tck_"))
