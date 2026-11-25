"""Replay the shared vector search cases through the public Python ORM."""

from array import array
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
from threading import Thread
import unittest

from nabedra import BatchError, Database, NabedraError, Vector, col

VECTORS = Path(__file__).resolve().parents[2] / "contract/vector/vectors.json"
REQUEST_ID = "@request_id"
SCALARS = {"utf8": str, "int64": int, "float64": float, "bool": bool}
COMPARE = {
    "eq": lambda c, v: c == v, "ne": lambda c, v: c != v, "gt": lambda c, v: c > v,
    "ge": lambda c, v: c >= v, "lt": lambda c, v: c < v, "le": lambda c, v: c <= v,
    "contains": lambda c, v: c.contains(v), "contains_any": lambda c, v: c.contains_any(v),
    "starts_with": lambda c, v: c.starts_with(v),
}


class Peer(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"])).decode("utf-8")
        request = json.loads(raw)
        response = self.server.response
        if request["operation"] == "table.describe" and self.server.table is not None:
            response, self.server.table = {"data": self.server.table}, None
        else:
            self.server.requests.append(raw.replace(request["request_id"], REQUEST_ID, 1))
        if "drop" in response:
            self.close_connection = True
            self.connection.close()
            return
        status = response.get("status", 200)
        request_id = json.dumps(request["request_id"])
        body = response.get("raw")
        if body is None and "error" in response:
            body = f'{{"request_id":{request_id},"error":{json.dumps(response["error"])}}}'
        elif body is None:
            body = f'{{"request_id":{request_id},"data":{response["data"]}}}'
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def column_type(wire):
    """Build a public column type from its wire form, without the decoder under test."""
    if isinstance(wire, str):
        return SCALARS[wire]
    if wire["type"] == "list":
        return list[column_type(wire["item"])]
    return Vector(**{name: value for name, value in wire.items() if name != "type"})


def schema_of(wire):
    return {column: column_type(typ) for column, typ in wire.items()}


def value_of(item):
    if isinstance(item, list):
        return [value_of(child) for child in item]
    if not isinstance(item, dict):
        return item
    if len(item) != 1 or not next(iter(item)).startswith("$"):
        return {name: value_of(child) for name, child in item.items()}
    (kind, content), = item.items()
    match kind:
        case "$int":
            return int(content)
        case "$float":
            return float(content)
        case "$list":
            return [value_of(child) for child in content]
        case "$float32":
            return array("f", map(float, content))
        case "$float64":
            return array("d", content)
        case "$bytes":
            return bytes(content)
        case "$sparse" | "$object":
            return {index: value_of(weight) for index, weight in content}
        case "$repeat":
            return [value_of(content[0])] * content[1]
        case "$text":
            return content[0] * content[1]
        case "$type":
            return column_type(content)
    raise AssertionError(kind)


def normal(value):
    """Describe a result without language types. Float32 bits stay exact as float64 values."""
    if type(value) is array:
        assert value.typecode == "f"
        return {"$float32": list(value)}
    if type(value) is list:
        return [normal(item) for item in value]
    if type(value) is dict:
        if value and all(type(key) is int for key in value):
            return {"$sparse": [[index, normal(weight)] for index, weight in value.items()]}
        return {key: normal(item) for key, item in value.items()} or {"$sparse": []}
    if type(value) is int:
        return {"$int": str(value)}
    if type(value) is float:
        return {"$float": value}
    return value


def predicate(node):
    op, *args = node
    if op in ("and", "or"):
        left, right = map(predicate, args)
        return left & right if op == "and" else left | right
    if op == "not":
        return ~predicate(args[0])
    column = predicate(args[0]) if isinstance(args[0], list) else col(args[0])
    return COMPARE[op](column, value_of(args[1]))


def ranking(node):
    if node[0] == "filter":
        return predicate(node[1])
    column, op, query = node
    return getattr(predicate(column) if isinstance(column, list) else col(column), op)(value_of(query))


def run_query(table, options):
    query = table.query()
    rank_options = value_of(options.get("rank_options", {}))
    if "where" in options:
        query = query.where(predicate(options["where"]))
    if "where_ranking" in options:
        query = query.where(ranking(options["where_ranking"]))
    if "order_by_first" in options:
        query = query.order_by(options["order_by_first"])
    if "rank" in options:
        query = query.rank(*map(ranking, options["rank"]), **rank_options)
    if "then_rank" in options:
        query = query.rank(*map(ranking, options["then_rank"]))
    if "rerank" in options:
        node, *candidates = options["rerank"]
        query = query.rerank(ranking(node), **({"candidates": value_of(candidates[0])} if candidates else {}))
    if "order_by" in options:
        query = query.order_by(options["order_by"])
    if options.get("select") is not None:
        query = query.select(*options["select"])
    if options.get("limit") is not None:
        query = query.limit(options["limit"])
    if options["terminal"] == "rank_many":
        nodes = options["rankings"]
        rankings = ([ranking(nodes["$repeat"][0])] * nodes["$repeat"][1] if isinstance(nodes, dict)
                    else [ranking(node) for node in nodes])
        return query.rank_many(rankings, limit=options["limit_many"], **rank_options)
    if options["terminal"] == "stream_snapshot":
        return query.stream_snapshot(shards=1)
    return getattr(query, options["terminal"])()


def run_call(table, call):
    options = call.get("options", {})
    if call["method"] == "query":
        return run_query(table, options)
    result = getattr(table, call["method"])(*map(value_of, call["args"]), **value_of(options))
    return result


def expected_message(error):
    return error["message"]["python"] if isinstance(error["message"], dict) else error["message"]


class VectorContract(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(VECTORS.read_text(encoding="utf-8"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Peer)
        cls.server.daemon_threads = True
        cls.thread = Thread(target=cls.server.serve_forever, kwargs={"poll_interval": 0.01})
        cls.thread.start()
        cls.db = Database(f"http://127.0.0.1:{cls.server.server_port}", "fixture", timeout=5)

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def exchange(self, case, run, expected):
        """Run one case. Requests, result or error must be exactly as recorded."""
        self.server.requests = []
        self.server.response = case.get("response", {"data": case.get("data", "null")})
        if "error" not in case:
            self.assertEqual(run(), expected)
        else:
            error = case["error"]
            kind = {"TypeError": TypeError}.get(error["type"], NabedraError)
            with self.assertRaises(kind) as caught:
                run()
            self.assert_error(caught.exception, error)
        operation = case.get("operation")
        body = f'{{"request_id":"{REQUEST_ID}","operation":"{operation}","payload":{case.get("payload")}}}'
        self.assertEqual(self.server.requests, [body] if operation else [])

    def assert_error(self, error, expected):
        actual = {"type": type(error).__name__, "message": str(error)}
        for field in ("code", "status_code", "outcome"):
            if field in expected:
                actual[field] = getattr(error, field)
        if type(error) is BatchError:
            actual["applied"] = error.applied
            actual["outcomes"] = [
                [row["key"], row["outcome"], row["error"] and type(row["error"]).__name__,
                 row["error"] and row["error"].outcome] for row in error.outcomes]
            self.assertIsNotNone(error.__cause__)
            for row in error.outcomes:
                self.assertEqual(row["error"] is None or row["error"].request_id == error.request_id, True)
        self.assertEqual(actual, {**expected, "message": expected_message(expected)})

    def test_vs18_create_requests_and_options(self):
        for case in self.contract["create"]:
            with self.subTest(case=case["name"]):
                def run():
                    table = self.db.create_table("made", schema_of(case["schema"]), "id",
                                                 **value_of(case["options"]))
                    return table.schema
                operation = {**case, "operation": "table.create"} if "payload" in case else case
                try:
                    expected = None if "error" in case else schema_of(case["schema"])
                    self.exchange(operation, run, expected)
                except NabedraError as error:  # Vector() itself rejects a bad type.
                    self.assert_error(error, case["error"])

    def test_vs18_strict_descriptors(self):
        for case in self.contract["describe"]:
            with self.subTest(case=case["name"]):
                described = {"operation": "table.describe", "payload": '{"name":"made"}', "response": {"data": case["data"]}}
                if "error" in case:
                    described["error"] = {"type": "ProtocolError", "message": "Invalid operation result"}
                self.server.table = None
                self.exchange(described, lambda: self.db.table("made").schema,
                              None if "error" in case else schema_of(case["result"]))

    def test_vs18_values_queries_batches_and_columns(self):
        for case in self.contract["calls"]:
            with self.subTest(case=case["name"]):
                self.server.table = self.contract["tables"][case["table"]]
                table = self.db.table(case["table"])
                raw = case["call"]["method"] == "search_status"

                def run():
                    result = run_call(table, case["call"])
                    return result if raw or type(result) is int else normal(result)
                self.exchange(case, run, case.get("result"))
                if "then" in case:
                    self.assertEqual(table.schema, schema_of(case["then"]["$schema"]))

    def test_fixture_names_are_unique_and_nothing_is_skipped(self):
        for group in ("create", "describe", "calls"):
            names = [case["name"] for case in self.contract[group]]
            self.assertEqual(len(set(names)), len(names))
            self.assertGreater(len(names), 30)
        self.assertTrue(math.isnan(value_of({"$float": "nan"})))


if __name__ == "__main__":
    unittest.main()
