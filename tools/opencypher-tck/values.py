"""Parse openCypher TCK result and parameter literals and compare driver values."""

import ast
from collections import Counter
from decimal import Decimal
import json
import math
import re


TOKEN = re.compile(
    r"""\s*(<\-|->|[()\[\]{}<>:,-]|'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"|"""
    r"""\x60(?:\x60\x60|[^\x60])*\x60|(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?|[^\W\d]\w*)"""
)
NUMBER = re.compile(r"(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?")
ATOMS = {"true": True, "false": False, "null": None, "NaN": float("nan"), "Inf": float("inf")}


class ValueParser:
    def __init__(self, source):
        self.source = source.strip()
        self.position = 0

    def peek(self):
        if self.position == len(self.source):
            return None
        match = TOKEN.match(self.source, self.position)
        if match is None:
            raise ValueError(f"Invalid TCK value at {self.source[self.position:]!r}")
        return match[1]

    def take(self, expected=None):
        token = self.peek()
        if not token or (expected is not None and token != expected):
            raise ValueError(f"Expected {expected or 'token'} at {self.source[self.position:]!r}")
        self.position = TOKEN.match(self.source, self.position).end()
        return token

    def accept(self, token):
        if self.peek() == token:
            self.take()
            return True
        return False

    @staticmethod
    def name(token):
        if token.startswith(('"', "'")):
            return ast.literal_eval(token.replace("\n", r"\n").replace("\r", r"\r"))
        if token.startswith(chr(96)):
            return token[1:-1].replace(chr(96) * 2, chr(96))
        if re.fullmatch(r"[^\W\d]\w*", token):
            return token
        raise ValueError(f"Invalid TCK name {token!r}")

    def mapping(self):
        result = {}
        if not self.accept("}"):
            while True:
                key = self.name(self.take())
                self.take(":")
                result[key] = self.value()
                if self.accept("}"):
                    break
                self.take(",")
        return result

    def element(self, kind, end):
        labels = []
        while self.accept(":"):
            labels.append(self.name(self.take()))
        properties = self.mapping() if self.accept("{") else {}
        self.take(end)
        return {"$type": kind, "labels": labels, "properties": properties}

    def path(self):
        self.take("(")
        nodes = [self.element("node", ")")]
        edges, directions = [], []
        while not self.accept(">"):
            reverse = self.accept("<-")
            if not reverse:
                self.take("-")
            self.take("[")
            edges.append(self.element("edge", "]"))
            self.take("-" if reverse else "->")
            directions.append("left" if reverse else "right")
            self.take("(")
            nodes.append(self.element("node", ")"))
        return {"$type": "path", "nodes": nodes, "edges": edges, "directions": directions}

    def value(self):
        token = self.take()
        if token == "[":
            if self.peek() == ":":
                return self.element("edge", "]")
            items = []
            if not self.accept("]"):
                items.append(self.value())
                while self.accept(","):
                    items.append(self.value())
                self.take("]")
            return items
        if token == "{":
            return self.mapping()
        if token == "(":
            return self.element("node", ")")
        if token == "<":
            return self.path()
        if token.startswith(("'", '"')):
            return self.name(token)
        negative = token == "-"
        if negative:
            token = self.take()
        if NUMBER.fullmatch(token):
            number = float(token) if any(c in token for c in ".eE") else int(token)
            return -number if negative else number
        if token in ATOMS and (not negative or token == "Inf"):
            return -ATOMS[token] if negative else ATOMS[token]
        raise ValueError(f"Invalid TCK literal {token!r}")


def parse_value(source):
    parser = ValueParser(source)
    result = parser.value()
    if parser.peek() is not None:
        raise ValueError(f"Unexpected text in TCK value {source!r}")
    return result


def normalize(value, unordered_lists=False):
    if isinstance(value, list):
        elements = tuple(normalize(item, unordered_lists) for item in value)
        if unordered_lists:
            elements = frozenset(Counter(elements).items())
        return ("list", elements)
    if isinstance(value, dict):
        kind = value.get("$type")
        if kind in ("node", "edge"):
            return (kind, frozenset(value["labels"]), normalize(value["properties"], unordered_lists))
        if kind == "path":
            nodes, edges = value["nodes"], value["edges"]
            if len(nodes) != len(edges) + 1:
                raise ValueError(f"Malformed path with {len(nodes)} nodes and {len(edges)} edges")
            directions = value.get("directions")
            if directions is None:
                directions = []
                for a, edge, b in zip(nodes, edges, nodes[1:]):
                    if (a["id"], b["id"]) == (edge["left"], edge["right"]):
                        directions.append("right")
                    elif (a["id"], b["id"]) == (edge["right"], edge["left"]):
                        directions.append("left")
                    else:
                        raise ValueError(f"Path edge {edge['id']} does not join adjacent nodes")
            return (kind, tuple(map(normalize, nodes)), tuple(map(normalize, edges)), tuple(directions))
        if kind == "float":
            number = value["value"]
            if number not in ("NaN", "Infinity", "-Infinity"):
                number = Decimal(number)
            return ("float", number)
        if kind == "map":
            value = value["entries"]
        return ("map", frozenset((key, normalize(item, unordered_lists)) for key, item in value.items()))
    if type(value) is float:
        if not math.isfinite(value):
            value = "NaN" if math.isnan(value) else "Infinity" if value > 0 else "-Infinity"
        else:
            value = Decimal(str(value))
        return ("float", value)
    return (type(value).__name__, value)


def wire_json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
