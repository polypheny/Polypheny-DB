"""JSONL bridge using the Polypheny Python connector's DB-API cursor."""

from contextlib import closing
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import sys

import polypheny

sys.path.insert(0, str(Path(__file__).absolute().parent.parent))
from values import wire_json


def encode(value):
    if isinstance(value, list):
        return [encode(element) for element in value]
    if isinstance(value, dict):
        if {"id", "name", "labels", "properties"} <= value.keys():
            return dict(value, **{"$type": "edge" if "left" in value else "node",
                                  "properties": {key: encode(item) for key, item in value["properties"].items()}})
        if set(value) == {"nodes", "edges"}:
            return {"$type": "path", "nodes": encode(value["nodes"]), "edges": encode(value["edges"])}
        return {"$type": "map", "entries": {key: encode(item) for key, item in value.items()}}
    if isinstance(value, Decimal):
        return {"$type": "float", "value": str(value)}
    if isinstance(value, float) and not math.isfinite(value):
        return {"$type": "float", "value": "NaN" if math.isnan(value) else "Infinity" if value > 0 else "-Infinity"}
    return value


def handle(connection, request):
    if request["op"] == "query":
        with closing(connection.cursor()) as cursor:
            cursor.executeany(request["language"], request["statement"], request["params"], namespace=request["namespace"])
            if cursor.description is None:
                rows = cursor.fetchall() if cursor.result.frame is not None else []
                return {"columns": [], "rows": [[encode(row)] for row in rows]}
            return {"columns": [column[0] for column in cursor.description], "rows": encode(cursor.fetchall())}
    if request["op"] == "commit":
        connection.commit()
    elif request["op"] == "rollback":
        connection.rollback()
    else:
        raise ValueError(f"Unsupported operation {request['op']!r}")
    return {}


def main():
    socket_path = os.getenv("M23_SOCKET")
    address = socket_path or (os.getenv("M23_HOST", "127.0.0.1"), int(os.environ["M23_PORT"]))
    with closing(polypheny.connect(address, username=os.getenv("M23_USERNAME", "pa"),
                                   password=os.getenv("M23_PASSWORD", ""), transport="unix" if socket_path else "plain")) as connection:
        settings = connection.con.new_request()
        settings.set_client_info_properties_request.properties["CypherResultFormat"] = "relational"
        connection.con.call(settings)
        for line in sys.stdin:
            try:
                response = handle(connection, json.loads(line))
            except polypheny.Error as error:
                response = {"error": {"message": str(error), "class": type(error).__name__,
                                      "code": getattr(error, "sqlstate", None)}}
            print(wire_json(response), flush=True)


if __name__ == "__main__":
    main()
