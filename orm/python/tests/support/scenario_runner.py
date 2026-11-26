"""Run the adversarial scenarios of orm/contract/vector/adversarial.json through the public Python ORM.

Run as a script to print one JSON transcript, which the TypeScript differential test compares.
"""
from array import array
import hashlib
import json
import math
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).parent))
from fake_server import TABLES, Fake  # noqa: E402

from nabedra import BatchError, Database, NabedraError, Vector, col  # noqa: E402

SCENARIOS = Path(__file__).resolve().parents[3] / "contract/vector/adversarial.json"
SCALARS = {"utf8": str, "int64": int, "float64": float, "bool": bool}
COMPARE = {
    "eq": lambda c, v: c == v, "ne": lambda c, v: c != v, "gt": lambda c, v: c > v,
    "ge": lambda c, v: c >= v, "lt": lambda c, v: c < v, "le": lambda c, v: c <= v,
    "contains": lambda c, v: c.contains(v), "contains_any": lambda c, v: c.contains_any(v),
    "starts_with": lambda c, v: c.starts_with(v),
}


def column_type(wire):
    if isinstance(wire, str):
        return SCALARS[wire]
    if wire["type"] == "list":
        return list[column_type(wire["item"])]
    return Vector(**{name: value for name, value in wire.items() if name != "type"})


def special(value):
    return {"nan": math.nan, "inf": math.inf, "-inf": -math.inf}.get(value, value)


def value_of(item, index=None):
    if isinstance(item, list):
        return [value_of(child, index) for child in item]
    if isinstance(item, str):
        return item if index is None else item.replace("{i}", str(index))
    if not isinstance(item, dict):
        return item
    if len(item) != 1 or not next(iter(item)).startswith("$"):
        return {name: value_of(child, index) for name, child in item.items()}
    (kind, content), = item.items()
    match kind:
        case "$int":
            return int(content if index is None else content.replace("{i}", str(index)))
        case "$float":
            return float(special(content))
        case "$list":
            return [value_of(child, index) for child in content]
        case "$float32":
            return array("f", map(float, map(special, content)))
        case "$sparse" | "$object":
            return {value_of(i, index): value_of(weight, index) for i, weight in content}
        case "$repeat":
            return [value_of(content[0], index) for _ in range(content[1])]
        case "$text":
            return content[0] * content[1]
        case "$concat":
            return [row for part in content for row in value_of(part, index)]
        case "$cat":
            return "".join(value_of(child, index) for child in content)
        case "$seq":
            return [value_of(content[1], number) for number in range(content[0])]
        case "$type":
            return column_type(content)
    raise AssertionError(kind)


def key_text(key):
    return {"$int": str(key)} if type(key) is int else key


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
    for node, *candidates in options.get("reranks", []):
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
    return getattr(query, options["terminal"])()


def run_call(table, call, db=None):
    if call["method"] == "create_table":
        schema = {name: column_type(wire) for name, wire in call["schema"].items()}
        db.create_table("made", schema, call.get("key", "id"), **call.get("options", {}))
        return None
    if call["method"] == "noop":
        return None
    if call["method"] == "query":
        return run_query(table, call["options"])
    return getattr(table, call["method"])(*map(value_of, call["args"]), **value_of(call.get("options", {})))


def normal(value):
    if type(value) is array:
        return {"$float32": [float(x).hex() for x in value]}
    if type(value) is list:
        return [normal(item) for item in value]
    if type(value) is dict:
        if value and all(type(key) is int for key in value):
            return {"$sparse": [[index, normal(weight)] for index, weight in value.items()]}
        return {key: normal(item) for key, item in value.items()} or {"$sparse": []}
    if type(value) is int:
        return {"$int": str(value)}
    if type(value) is float:
        return {"$float": float(value).hex()}
    return value


def mask(raw):
    text = raw.decode("utf-8")
    text = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "ID", text, count=1)
    if len(text) > 4000:
        return {"len": len(raw), "sha": hashlib.sha256(text.encode()).hexdigest()}
    return text


def describe_error(error):
    summary = {"type": type(error).__name__ if isinstance(error, NabedraError) else "RAW:" + type(error).__name__,
               "message": str(error)}
    if isinstance(error, NabedraError):
        summary.update(code=error.code, outcome=error.outcome, status=error.status_code)
    if isinstance(error, BatchError):
        summary["applied"] = error.applied
        summary["outcomes"] = [[key_text(row["key"]), row["outcome"], row["error"] and type(row["error"]).__name__,
                                row["error"] and row["error"].outcome] for row in error.outcomes]
        summary["cause"] = type(error.__cause__).__name__ if error.__cause__ else None
    return summary


def shape(request):
    payload = request["request"]["payload"]
    return {"operation": request["request"]["operation"],
            "items": len(payload.get("rows", payload.get("keys", []))) if isinstance(payload, dict) else None,
            "bytes": len(request["raw"])}


def run_one(fake, db, scenario):
    table = scenario["table"]
    if isinstance(table, dict):
        if "$nest" in table:
            depth = table["$nest"]
            table = {"$raw": ('{"id":"00000000-0000-0000-0000-000000000031","name":"custom","key":"id",'
                              '"format":"parquet","schema":{"id":"utf8","x":'
                              + '{"type":"list","item":' * depth + '"utf8"' + "}" * depth + "}}")}
        TABLES["custom"] = table["$raw"] if "$raw" in table else {"name": "custom", **table}
        table = "custom"
    fake.reset(*scenario.get("script", []))
    try:
        handle = None if scenario["call"]["method"] == "create_table" else db.table(table)
        result = {"ok": normal(run_call(handle, scenario["call"], db))}
        if scenario["call"]["method"] == "search_status":
            result = {"ok": run_call(handle, scenario["call"])}
    except BaseException as error:  # a raw exception is itself a finding
        result = {"error": describe_error(error)}
    cut = lambda item: item["request"]["operation"] == "cut"  # noqa: E731
    return {"requests": ["CUT" if cut(item) else mask(item["raw"]) for item in fake.requests],
            "shapes": [{"operation": "cut", "items": 0, "bytes": 0} if cut(item) else shape(item)
                       for item in fake.requests], "result": result}


def run_all(only=None):
    scenarios = json.loads(SCENARIOS.read_text(encoding="utf-8"))
    fake = Fake()
    # 8 MiB requests must not time out on a loaded machine.
    db = Database(fake.url, "fixture", timeout=30)
    try:
        return scenarios, {item["name"]: run_one(fake, db, item) for item in scenarios
                           if only is None or item["name"] in only}
    finally:
        db.close()
        fake.close()


if __name__ == "__main__":
    json.dump(run_all()[1], sys.stdout, ensure_ascii=True, sort_keys=True)


def problems(scenario, run):
    """Compare one run with the scenario's expectations. Returns a list of messages."""
    expect, found, result = scenario["expect"], [], run["result"]
    error = result.get("error")
    kinds = expect.get("type") if isinstance(expect.get("type"), list) else [expect.get("type")]
    if error and error["type"].startswith("RAW:") and error["type"] not in kinds:
        found.append(f"raw exception {error['type']}: {error['message']}")
    kind = expect.get("type")
    if kind is not None:
        allowed = kind if isinstance(kind, list) else [kind]
        actual = "ok" if error is None else error["type"]
        if actual not in allowed:
            found.append(f"result {actual}, expected {allowed}: {error and error['message']}")
    if "requests" in expect and len(run["requests"]) != expect["requests"]:
        found.append(f"{len(run['requests'])} requests, expected {expect['requests']}")
    if "items" in expect and [shape["items"] for shape in run["shapes"]] != expect["items"]:
        found.append(f"items {[shape['items'] for shape in run['shapes']]}, expected {expect['items']}")
    if "bytes" in expect and [shape["bytes"] for shape in run["shapes"]] != expect["bytes"]:
        found.append(f"bytes {[shape['bytes'] for shape in run['shapes']]}, expected {expect['bytes']}")
    if "bytes" not in expect and any(shape["bytes"] > 8 * 1024 * 1024 for shape in run["shapes"]):
        found.append("a request over 8 MiB was sent")
    if "result_count" in expect and result.get("ok") != {"$int": str(expect["result_count"])}:
        found.append(f"count {result.get('ok')}, expected {expect['result_count']}")
    if "result_len" in expect and len(result.get("ok", [])) != expect["result_len"]:
        found.append(f"{len(result.get('ok', []))} rows, expected {expect['result_len']}")
    texts = [item if isinstance(item, str) else "" for item in run["requests"]]
    for needle in expect.get("request_contains", []):
        if not any(needle in text for text in texts):
            found.append(f"no request contains {needle}")
    for needle in expect.get("request_lacks", []):
        if any(needle in text for text in texts):
            found.append(f"a request contains {needle}")
    if error and error["type"] == "BatchError":
        for field in ("applied", "outcome"):
            if field in expect and error[field] != expect[field]:
                found.append(f"{field} {error[field]!r}, expected {expect[field]!r}")
        names = [row[1] for row in error["outcomes"]]
        if "outcomes" in expect and names != expect["outcomes"]:
            found.append(f"outcomes {names}, expected {expect['outcomes']}")
        if "outcomes_all" in expect and set(names) != {expect["outcomes_all"]}:
            found.append(f"outcomes {sorted(set(names))}, expected only {expect['outcomes_all']}")
        if "keys" in expect and [row[0] for row in error["outcomes"]] != expect["keys"]:
            found.append(f"keys {[row[0] for row in error['outcomes']]}")
        # An applied row needs proof: the server must have said so in a reply for this chunk.
    return found
