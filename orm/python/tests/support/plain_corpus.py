"""Record the request bytes of every operation that existed before vector search.

    python plain_corpus.py            prints the corpus of the package on PYTHONPATH
    test_plain_corpus.py compares it with tests/fixtures/plain_corpus.json, recorded on the base branch.
"""
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).parent))
from fake_server import Fake, ok  # noqa: E402

from nabedra import Database, col  # noqa: E402

ID = "00000000-0000-0000-0000-000000000022"
PLAIN = {"id": ID, "name": "plain", "key": "id", "format": "parquet",
         "schema": {"id": "utf8", "n": "int64", "f": "float64", "b": "bool"}}
FLOATS = [1.0, -0.0, 0.1, 1e16, 1e-7, 5e-324, 1.7976931348623157e308, 123456789.123456789, 2.5e-5, 1e22, 1e21, 1e15]
INTS = [0, -1, 2 ** 53, 2 ** 53 + 1, 2 ** 63 - 1, -2 ** 63]
TEXTS = ["", "a", "é", "é", "\U0001f600", "\u0000", "\u001f", "\u007f", " ", "\"\\/\b\f\n\r\t", "x" * 5000]


def descriptor_for(request):
    payload = request["payload"]
    return {"data": json.dumps({"id": "00000000-0000-0000-0000-000000000040", "name": payload["name"],
                                "schema": payload["schema"], "key": payload["key"], "format": payload["format"]})}


def calls(db):
    plain = db.table("plain")
    yield "create all scalars", lambda: db.create_table("t", {"id": str, "n": int, "f": float, "b": bool}, "id")
    yield "create int key", lambda: db.create_table("t", {"id": int, "name": str}, "id")
    yield "create parquet", lambda: db.create_table("t", {"id": str}, "id", format="parquet")
    yield "create unicode column", lambda: db.create_table("t", {"id": str, "café": str}, "id")
    yield "table", lambda: db.table("plain")
    yield "tables", lambda: db.tables()
    yield "drop", lambda: plain.drop()
    for index, text in enumerate(TEXTS):
        row = {"id": text, "n": 1, "f": 1.0, "b": True}
        yield f"insert text {index}", lambda row=row: plain.insert(row)
        yield f"upsert text {index}", lambda row=row: plain.upsert(row)
        yield f"get text {index}", lambda text=text: plain.get(text)
        yield f"delete text {index}", lambda text=text: plain.delete(text)
        yield f"update text {index}", lambda text=text: plain.update(text, {"n": 2})
    for index, number in enumerate(FLOATS):
        row = {"id": "a", "n": 1, "f": number, "b": False}
        yield f"insert float {index}", lambda row=row: plain.insert(row)
        yield f"upsert float {index}", lambda row=row: plain.upsert(row)
        yield f"update float {index}", lambda number=number: plain.update("a", {"f": number})
        yield f"where float {index}", lambda number=number: plain.where(col("f") > number).collect()
    for index, number in enumerate(INTS):
        row = {"id": "a", "n": number, "f": 1.0, "b": False}
        yield f"insert int {index}", lambda row=row: plain.insert(row)
        yield f"upsert int {index}", lambda row=row: plain.upsert(row)
        yield f"update int {index}", lambda number=number: plain.update("a", {"n": number})
        yield f"where int {index}", lambda number=number: plain.where(col("n") == number).collect()
    rows = [{"id": f"k{i}", "n": i, "f": i / 3, "b": i % 2 == 0} for i in range(50)]
    yield "insert_many", lambda: plain.insert_many(rows)
    yield "insert_many one", lambda: plain.insert_many(rows[:1])
    yield "insert_many unicode", lambda: plain.insert_many([{"id": text, "n": 1, "f": 1.0, "b": True} for text in TEXTS])
    yield "insert_many floats", lambda: plain.insert_many(
        [{"id": f"f{i}", "n": 1, "f": number, "b": True} for i, number in enumerate(FLOATS)])
    yield "insert_many generator", lambda: plain.insert_many(dict(row) for row in rows)
    yield "update many fields", lambda: plain.update("a", {"n": 2, "f": 3.5, "b": False})
    yield "collect", lambda: plain.query().collect()
    yield "collect where", lambda: plain.where(col("n") >= 3).collect()
    yield "collect kwargs", lambda: plain.where(n=3, b=True).collect()
    yield "collect and or not", lambda: plain.where(((col("n") > 1) & (col("f") < 2.5)) | ~(col("b") == True)).collect()  # noqa: E712
    for op in ("eq", "ne", "gt", "ge", "lt", "le"):
        yield f"where {op}", lambda op=op: plain.where(getattr(col("n"), f"__{op}__")(5)).collect()
    yield "where string", lambda: plain.where(col("id") == "café").collect()
    yield "where bool", lambda: plain.where(col("b") != False).collect()  # noqa: E712
    yield "select", lambda: plain.query().select("id", "f").collect()
    yield "order asc", lambda: plain.query().order_by("n").collect()
    yield "order desc", lambda: plain.query().order_by("n", desc=True).limit(7).collect()
    yield "limit 0", lambda: plain.query().limit(0).collect()
    yield "count", lambda: plain.where(col("n") > 1).count()
    yield "count limit", lambda: plain.query().limit(5).count()
    yield "explain", lambda: plain.where(col("n") > 1).order_by("n").explain()
    yield "deep", lambda: plain.where(((col("n") > 1) & (col("n") < 9)) & ((col("f") > 0.5) & (col("b") == True))).collect()  # noqa: E712


def reply(name):
    if name.startswith("create"):
        return descriptor_for
    if name in ("table", "tables"):
        return ok(json.dumps(PLAIN if name == "table" else [PLAIN]))
    if name.startswith("drop"):
        return ok("null")
    if name.startswith("insert_many"):
        return lambda request: ok(str(len(request["payload"]["rows"])))
    if name.startswith(("insert", "upsert")):
        return ok("null")
    if name.startswith(("update", "delete")):
        return ok("true")
    if name.startswith("get"):
        return ok("null")
    if name.startswith("count"):
        return ok("0")
    if name.startswith("explain"):
        return ok('"plan"')
    return ok("[]")


def record():
    fake = Fake()
    db = Database(fake.url, "fixture", timeout=3)
    corpus = {}
    try:
        for name, call in calls(db):
            fake.reset(reply(name))
            try:
                call()
                failure = None
            except Exception as error:  # a changed validation message is a change too
                failure = f"{type(error).__name__}: {error}"
            corpus[name] = {"requests": [re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "ID", item["raw"].decode(), count=1)
                                         for item in fake.requests], "failure": failure}
    finally:
        db.close()
        fake.close()
    return corpus


if __name__ == "__main__":
    json.dump(record(), sys.stdout, ensure_ascii=True, indent=0, sort_keys=True)
