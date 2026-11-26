"""Write orm/contract/vector/adversarial.json. Run: python gen_adversarial.py

Every scenario is replayed by both SDKs. The differential test compares their transcripts, and each language
checks the scenario's `expect` field. Sizes are computed here so that boundaries are exact.
"""
import base64
import json
from pathlib import Path
import struct

OUT = Path(__file__).resolve().parents[3] / "contract/vector/adversarial.json"
S = []
MiB = 1024 * 1024
DOCS_ID = "00000000-0000-0000-0000-000000000021"
PLAIN_ID = "00000000-0000-0000-0000-000000000022"
LEAN_ID = "00000000-0000-0000-0000-000000000024"


def F(x):
    return {"$float": x}


def V(*xs):
    return {"$list": [F(x) for x in xs]}


def I(x):
    return {"$int": str(x)}


def applied(n=1):
    return {"applied": n}


def failed(code, outcome="not_applied", status=None, message="m"):
    codes = {"validation_error": 400, "unauthorized": 401, "not_found": 404, "conflict": 409, "limit_exceeded": 413,
             "overloaded": 429, "storage_unavailable": 503, "search_unavailable": 503, "outcome_unknown": 503}
    return {"status": status or codes.get(code, 500),
            "error": {"code": code, "message": message, "outcome": outcome}}


def raw(text, status=200, kind="application/json"):
    return {"raw": text, "status": status, "type": kind}


def add(name, table, call, script=(), **expect):
    S.append({"name": name, "table": table, "call": call, "script": list(script), "expect": expect})


def docs_row(**over):
    return {"id": "a", "n": I(1), "f": F(1.0), "b": True, "vec": V(1, 2), "sp": {"$sparse": []}, "mu": [],
            "tags": [], **over}


def upsert(rows):
    return {"method": "upsert_many", "args": [rows]}


def delete(keys):
    return {"method": "delete_many", "args": [keys]}


# --- A. float32 rounding ----------------------------------------------------------------------------------------
FLOATS = [0.1, -0.0, 0.0, 1e-50, -1e-50, 5e-324, 1.401298464324817e-45, 7.006492321624085e-46,
          7.006492321624087e-46, 1.1754943508222875e-38, 1.1754942106924411e-38, 3.4028234663852886e38,
          3.4028235677973362e38, 3.4028235677973366e38, 3.402823567797337e38, 1e39, -1e39, 16777217.0,
          0.30000000000000004, 1e30, 8388609.5, -3.4028235677973366e38, 1.7976931348623157e308]
for number in FLOATS:
    try:
        wire = base64.b64encode(struct.pack("<ff", number, 0.0)).decode()
        outcome = {"type": "ok", "request_contains": [f'"vec":{{"f32":"{wire}"}}']}
    except OverflowError:
        outcome = {"type": "ValidationError", "requests": 0}
    S.append({"name": f"f32 dense {number!r}", "table": "docs", "call": upsert([docs_row(vec=V(number, 0.0))]),
              "script": [applied()], "expect": outcome})
    S.append({"name": f"f32 sparse {number!r}", "table": "docs",
              "call": upsert([docs_row(sp={"$sparse": [[3, F(number)]]})]), "script": [applied()],
              "expect": {"type": outcome["type"]}})
    S.append({"name": f"f32 multi {number!r}", "table": "docs", "call": upsert([docs_row(mu=[V(number, 1.0)])]),
              "script": [applied()], "expect": {"type": outcome["type"]}})
for token in ("nan", "inf", "-inf"):
    add(f"f32 dense {token}", "docs", upsert([docs_row(vec=V(token, 0.0))]), [applied()],
        type="ValidationError", requests=0)
add("f32 huge python int", "docs", upsert([docs_row(vec={"$list": [{"$int": str(10 ** 400)}, F(0.0)]})]),
    [applied()], type="ValidationError", requests=0)

# --- B. sparse and multi ----------------------------------------------------------------------------------------
add("sparse out of order", "docs",
    upsert([docs_row(sp={"$sparse": [[5, F(1.0)], [1, F(2.0)], [4294967295, F(3.0)], [0, F(4.0)], [70000, F(5.0)],
                                      [4294967294, F(6.0)], [10, F(7.0)], [9, F(8.0)]]})]), [applied()],
    type="ok", request_contains=[base64.b64encode(struct.pack("<8I", 0, 1, 5, 9, 10, 70000, 4294967294,
                                                               4294967295)).decode()])
for index in (4294967296, -1, 1.5):
    add(f"sparse index {index}", "docs", upsert([docs_row(sp={"$sparse": [[index, F(1.0)]]})]), [applied()],
        type="ValidationError", requests=0)
for text in ("01", "1.0", "-1", "+1", " 1", "0x1", "1e3", "", "4294967296", "00", "1 ", "١"):
    add(f"sparse string index {text!r}", "docs", upsert([docs_row(sp={"$sparse": [[text, F(1.0)]]})]), [applied()],
        type="ValidationError", requests=0)
add("sparse negative zero weight", "docs", upsert([docs_row(sp={"$sparse": [[1, F(-0.0)]]})]), [applied()], type="ok")
add("multi with zero vectors", "docs", upsert([docs_row(mu=[])]), [applied()], type="ok")
add("multi 1024 vectors", "docs", upsert([docs_row(mu={"$repeat": [V(1, 2), 1024]})]), [applied()], type="ok")
add("multi 1025 vectors", "docs", upsert([docs_row(mu={"$repeat": [V(1, 2), 1025]})]), [applied()],
    type="LimitExceededError", requests=0)
add("multi with a short vector", "docs", upsert([docs_row(mu=[V(1)])]), [applied()],
    type="ValidationError", requests=0)
add("dense with 3 values", "docs", upsert([docs_row(vec=V(1, 2, 3))]), [applied()], type="ValidationError", requests=0)

# --- C. Unicode -------------------------------------------------------------------------------------------------
add("unicode keys that normalize alike", "docs",
    upsert([docs_row(id="é"), docs_row(id="é")]), [applied(2)], type="ok", items=[2])
for label, key in (("astral", "\U0001f600"), ("nul", "\u0000"), ("line separators", "  "),
                   ("delete", "\u007f"), ("controls", "\u001f\"\\/\b\f\n\r\t"), ("noncharacters", "￿\U0010ffff"),
                   ("bom", "﻿x")):
    add(f"unicode key {label}", "docs", upsert([docs_row(id=key)]), [applied()], type="ok", items=[1])
    add(f"unicode delete key {label}", "docs", delete([key]), [applied()], type="ok", items=[1])
for label, text in (("high", "a\ud800"), ("low", "\udc00b"), ("reversed", "\udc00\ud800"), ("alone", "\ud83d")):
    add(f"lone surrogate in key {label}", "docs", upsert([docs_row(id=text)]), [applied()],
        type="ValidationError", requests=0)
    add(f"lone surrogate in delete key {label}", "docs", delete([text]), [applied()],
        type="ValidationError", requests=0)
    add(f"lone surrogate in tags {label}", "docs", upsert([docs_row(tags=[text])]), [applied()],
        type="ValidationError", requests=0)
add("surrogate pair in tags", "docs", upsert([docs_row(tags=["😀"])]), [applied()], type="ok")

# --- D. big integers --------------------------------------------------------------------------------------------
for value in (2 ** 53, 2 ** 53 + 1, 2 ** 63 - 1, -2 ** 63, -2 ** 53 - 1):
    add(f"int64 value {value}", "docs", upsert([docs_row(n=I(value))]), [applied()], type="ok",
        request_contains=[f'"n":{value},'])
for value in (2 ** 63, -2 ** 63 - 1, 10 ** 30):
    add(f"int64 out of range {value}", "docs", upsert([docs_row(n=I(value))]), [applied()],
        type="ValidationError", requests=0)
add("int keys beyond 2^53 stay distinct", "numbered", delete([I(2 ** 53), I(2 ** 53 + 1)]), [applied(2)],
    type="ok", items=[2], request_contains=['"keys":[9007199254740992,9007199254740993]'])
add("int key twice", "numbered", delete([I(2 ** 63 - 1), I(2 ** 63 - 1)]), [applied(2)],
    type="ValidationError", requests=0)
add("int key out of range", "numbered", delete([I(2 ** 63)]), [applied()], type="ValidationError", requests=0)
add("string key on an int table", "numbered", delete(["1"]), [applied()], type="ValidationError", requests=0)
add("float key on an int table", "numbered", delete([F(1.5)]), [applied()], type="ValidationError", requests=0)
add("bool key on an int table", "numbered", delete([True]), [applied()], type="ValidationError", requests=0)
add("int key on a string table", "docs", delete([I(1)]), [applied()], type="ValidationError", requests=0)
add("big int outcome keys", "numbered", delete([I(2 ** 53 + 1), I(2 ** 53 + 2)]),
    [{"data": '{"outcomes":[{"o":"applied"},{"o":"not_applied","error":{"code":"conflict","message":"m",'
              '"outcome":"not_applied"}}]}'}],
    type="BatchError", applied=1, outcomes=["applied", "not_applied"], keys=[I(2 ** 53 + 1), I(2 ** 53 + 2)])

# --- E. chunk boundaries ----------------------------------------------------------------------------------------
seq_row = {"id": "k{i}", "n": I("{i}"), "f": F(1.0), "b": True, "vec": V(1, 2), "sp": {"$sparse": []}, "mu": [],
           "tags": []}
add("10000 rows is one request", "docs", upsert({"$seq": [10000, seq_row]}), [applied(10000)],
    type="ok", items=[10000], result_count=10000)
add("10001 rows are two requests", "docs", upsert({"$seq": [10001, seq_row]}), [applied(10000), applied(1)],
    type="ok", items=[10000, 1], result_count=10001)
add("20001 rows with an unknown second chunk", "docs", upsert({"$seq": [20001, seq_row]}),
    [applied(10000), {"status": 500}], type="BatchError", applied=10000, items=[10000, 10000],
    outcomes_all="outcome_unknown", outcome="unknown")
add("20001 rows with a rejected second chunk", "docs", upsert({"$seq": [20001, seq_row]}),
    [applied(10000), failed("validation_error")], type="BatchError", applied=10000, items=[10000, 10000],
    outcomes_all="not_applied", outcome="partial")
add("10001 keys are two requests", "docs", delete({"$seq": [10001, "k{i}"]}), [applied(10000), applied(1)],
    type="ok", items=[10000, 1], result_count=10001)
add("10000 keys are one request", "docs", delete({"$seq": [10000, "k{i}"]}), [applied(10000)],
    type="ok", items=[10000], result_count=10000)


def envelope(table_id, operation, rows):
    return len(('{"request_id":"' + "0" * 36 + '","operation":"' + operation + '","payload":{"table_id":"'
                + table_id + '","rows":[' + ",".join(rows) + ']}}').encode())


def plain_row(text):
    return json.dumps({"id": text, "n": 1, "f": 1.0, "b": True}, ensure_ascii=False, separators=(",", ":"))


def pad_pair(target, glyph):
    """Two plain rows whose request is exactly target bytes, with ids of glyphs and ASCII."""
    width = len(glyph.encode())
    spare = target - envelope(PLAIN_ID, "table.upsert_many", [plain_row(""), plain_row("")])
    rows, texts = [], []
    for letter, share in (("a", spare // 2 - 1), ("b", spare - spare // 2 - 1)):
        count = share // width
        rest = share - count * width
        rows.append({"id": {"$cat": [letter, {"$text": [glyph, count]}, {"$text": ["x", rest]}]},
                     "n": I(1), "f": F(1.0), "b": True})
        texts.append(letter + glyph * count + "x" * rest)
    assert envelope(PLAIN_ID, "table.upsert_many", [plain_row(t) for t in texts]) == target
    return rows


for glyph in ("x", "é", "😀"):
    add(f"body exactly 8 MiB with {glyph!r}", "plain", upsert(pad_pair(8 * MiB, glyph)), [applied(2)],
        type="ok", items=[2], bytes=[8 * MiB])
    add(f"body one byte over 8 MiB with {glyph!r}", "plain", upsert(pad_pair(8 * MiB + 1, glyph)),
        [applied(1), applied(1)], type="ok", items=[1, 1])
add("one row alone over 8 MiB", "plain",
    upsert([{"id": {"$text": ["x", 8 * MiB]}, "n": I(1), "f": F(1.0), "b": True}]), [applied()],
    type="LimitExceededError", requests=0)
add("one row exactly filling 8 MiB", "plain",
    upsert([{"id": {"$text": ["x", 8 * MiB - envelope(PLAIN_ID, "table.upsert_many", [plain_row("")])]},
             "n": I(1), "f": F(1.0), "b": True}]), [applied()], type="ok", items=[1], bytes=[8 * MiB])
add("nine 1 MB rows", "plain", {"method": "upsert_many", "args": [
    {"$seq": [9, {"id": {"$cat": ["k{i}", {"$text": ["x", 1_000_000]}]}, "n": I(1), "f": F(1.0), "b": True}]}]},
    [applied(8), applied(1)], type="ok", items=[8, 1])

# 1 MiB rows on a table with search columns: the limit counts bytes without the base64 overhead.
LEAN_BASE = len(json.dumps({"id": "a", "body": "", "emb": None}, separators=(",", ":")))
for delta, label in ((0, "exactly"), (1, "one byte over")):
    pad = MiB - LEAN_BASE + delta
    row = {"id": "a", "body": {"$text": ["x", pad]}, "emb": None}
    if delta == 0:
        add(f"search row {label} 1 MiB", "lean", upsert([row]), [applied()], type="ok", items=[1])
    else:
        add(f"search row {label} 1 MiB", "lean", upsert([row]), [applied()], type="LimitExceededError", requests=0)
    with_vector = len(json.dumps({"id": "a", "body": "", "emb": {"f32": "A" * 16}}, separators=(",", ":"))) - 4
    pad = MiB - with_vector + delta
    row = {"id": "a", "body": {"$text": ["é", pad // 2]}, "emb": V(1, 2, 3)}
    # the pad is counted in bytes, and an e-acute is two of them
    pad_bytes = pad // 2 * 2 + (0)
    row["body"] = {"$cat": [{"$text": ["é", pad // 2]}, {"$text": ["x", pad - pad // 2 * 2]}]}
    if delta == 0:
        add(f"search row with a vector {label} 1 MiB", "lean", upsert([row]), [applied()], type="ok", items=[1])
    else:
        add(f"search row with a vector {label} 1 MiB", "lean", upsert([row]), [applied()],
            type="LimitExceededError", requests=0)
add("plain row over 1 MiB is fine", "plain",
    upsert([{"id": {"$text": ["x", MiB + 5]}, "n": I(1), "f": F(1.0), "b": True}]), [applied()],
    type="ok", items=[1])
add("search rows of 1 MiB fill requests", "lean",
    {"method": "upsert_many", "args": [{"$seq": [9, {"id": "k{i}", "body": {"$text": ["x", MiB - LEAN_BASE - 2 + 0]},
                                                      "emb": None}]}]},
    [applied(7), applied(2)], type="ok", items=[7, 2])

# Float64 counts as one byte in the row limit, while the request keeps its wire token.
for extra in (0, 1):
    row = docs_row(f=F(1.2345678901234567), tags=[{"$text": ["x", MiB - 125 + extra]}])
    add(f"search row with float64 at 1 MiB plus {extra}", "docs", upsert([row]), [applied()],
        **({"type": "LimitExceededError", "requests": 0} if extra else {"type": "ok", "items": [1]}))

# Base64 padding does not count against the 1 MiB row limit.
def padded_row(pad):
    wire = {"id": "a", "n": 1, "f": 1.0, "b": True, "vec": {"f32": base64.b64encode(struct.pack("<2f", 1, 2)).decode()},
            "sp": {"sparse": {"i": base64.b64encode(struct.pack("<I", 3)).decode(),
                              "f32": base64.b64encode(struct.pack("<f", 1)).decode()}},
            "mu": {"f32s": base64.b64encode(struct.pack("<2f", 1, 2)).decode(), "n": 1}, "tags": [pad]}
    return len(json.dumps(wire, ensure_ascii=False, separators=(",", ":")).encode()) - 16


for delta, label in ((0, "exactly"), (1, "one byte over")):
    pad = MiB - padded_row("") + delta
    row = docs_row(sp={"$sparse": [[3, F(1.0)]]}, mu=[V(1, 2)], tags=[{"$text": ["x", pad]}])
    if delta == 0:
        add(f"padded base64 row {label} 1 MiB", "docs", upsert([row]), [applied()], type="ok", items=[1])
    else:
        add(f"padded base64 row {label} 1 MiB", "docs", upsert([row]), [applied()], type="LimitExceededError",
            requests=0)

# Four rows of which any two are one byte over 8 MiB go out one by one: the envelope counts for every request.
EMPTY = envelope(PLAIN_ID, "table.upsert_many", [])
SUM = 8 * MiB - EMPTY
LENGTHS = [SUM // 2, SUM - SUM // 2]
FIXED = len(plain_row(""))
TEXTS4 = [chr(97 + number) + "x" * (LENGTHS[number % 2] - FIXED - 1) for number in range(4)]
rows4 = [{"id": {"$cat": [text[0], {"$text": ["x", len(text) - 1]}]}, "n": I(1), "f": F(1.0), "b": True}
         for text in TEXTS4]
for first in range(3):
    assert envelope(PLAIN_ID, "table.upsert_many", [plain_row(TEXTS4[first]), plain_row(TEXTS4[first + 1])]) == 8 * MiB + 1
add("four rows, any two one byte over 8 MiB", "plain", upsert(rows4), [applied(1)] * 4, type="ok",
    items=[1, 1, 1, 1], result_count=4)
add("sparse index true", "docs", upsert([docs_row(sp={"$sparse": [[True, F(1.0)]]})]), [applied()],
    type="ValidationError", requests=0)

# --- F. hostile replies to writes -------------------------------------------------------------------------------
two = upsert([docs_row(id="a"), docs_row(id="b")])
NA = {"code": "conflict", "message": "m", "outcome": "not_applied"}
UN = {"code": "outcome_unknown", "message": "m", "outcome": "unknown"}


def outcomes(*items):
    return {"data": json.dumps({"outcomes": list(items)}, separators=(",", ":"))}


A = {"o": "applied"}
unknown_both = dict(type="BatchError", applied=0, outcomes_all="outcome_unknown", outcome="unknown")
add("both applied", "docs", two, [applied(2)], type="ok", result_count=2)
add("second row rejected", "docs", two, [outcomes(A, {"o": "not_applied", "error": NA})], type="BatchError",
    applied=1, outcomes=["applied", "not_applied"], outcome="partial")
add("first row unknown second applied", "docs", two, [outcomes({"o": "outcome_unknown", "error": UN}, A)],
    type="BatchError", applied=1, outcomes=["outcome_unknown", "applied"], outcome="unknown")
add("not_applied with the unknown code", "docs", two,
    [outcomes({"o": "not_applied", "error": {**NA, "code": "outcome_unknown"}}, A)], type="BatchError", applied=1)
add("outcome_unknown with a known code", "docs", two,
    [outcomes({"o": "outcome_unknown", "error": {**UN, "code": "conflict"}}, A)], type="BatchError", applied=1)
add("not_applied with an unauthorized code", "docs", two,
    [outcomes({"o": "not_applied", "error": {**NA, "code": "unauthorized"}}, A)], type="BatchError", applied=1)
add("one outcome for two rows", "docs", two, [applied(1)], **unknown_both)
add("three outcomes for two rows", "docs", two, [applied(3)], **unknown_both)
add("no outcomes for two rows", "docs", two, [applied(0)], **unknown_both)
add("10001 outcomes for one row", "docs", upsert([docs_row()]), [applied(10001)], **unknown_both)
for label, bad in (("an unknown string", {"o": "maybe"}), ("a capital", {"o": "Applied"}),
                   ("an empty string", {"o": ""}), ("a null", {"o": None}), ("a number", {"o": 1}),
                   ("applied with null error", {"o": "applied", "error": None}),
                   ("applied with an error", {"o": "applied", "error": NA}),
                   ("applied with an extra field", {"o": "applied", "x": 1}),
                   ("not_applied without error", {"o": "not_applied"}),
                   ("not_applied with null error", {"o": "not_applied", "error": None}),
                   ("not_applied with an applied outcome", {"o": "not_applied", "error": {**NA, "outcome": "applied"}}),
                   ("not_applied claiming unknown", {"o": "not_applied", "error": {**NA, "outcome": "unknown"}}),
                   ("unknown claiming not_applied", {"o": "outcome_unknown", "error": {**UN, "outcome": "not_applied"}}),
                   ("an error with extra fields", {"o": "not_applied", "error": {**NA, "extra": 1}}),
                   ("an error with a numeric message", {"o": "not_applied", "error": {**NA, "message": 5}}),
                   ("an error with a null code", {"o": "not_applied", "error": {**NA, "code": None}}),
                   ("a string outcome", "applied"), ("a null outcome", None), ("a list outcome", [A]),
                   ("a numeric outcome", 1), ("an empty object", {})):
    add(f"outcome is {label}", "docs", upsert([docs_row()]), [outcomes(bad)], **unknown_both)
for label, data in (("null", "null"), ("a string", '"applied"'), ("a list", "[]"), ("an object without outcomes", "{}"),
                    ("outcomes as an object", '{"outcomes":{}}'), ("outcomes as null", '{"outcomes":null}'),
                    ("an extra field", '{"outcomes":[{"o":"applied"}],"extra":1}'),
                    ("a count", '{"applied":1}'), ("true", "true"), ("a number", "1")):
    add(f"data is {label}", "docs", upsert([docs_row()]), [{"data": data}], **unknown_both)
add("a 1 MB error message", "docs", upsert([docs_row()]),
    [failed("conflict", message="$FILL")], type="BatchError", applied=0, outcomes_all="not_applied")
for status in (400, 401, 404, 409, 413, 429):
    add(f"status {status} with a body", "docs", two,
        [failed({400: "validation_error", 401: "unauthorized", 404: "not_found", 409: "conflict",
                 413: "limit_exceeded", 429: "overloaded"}[status])],
        type="BatchError", applied=0, outcomes_all="not_applied", outcome="not_applied", requests=1)
add("status 503 storage_unavailable", "docs", two, [failed("storage_unavailable")], type="BatchError",
    applied=0, outcomes_all="not_applied")
add("status 503 search_unavailable", "docs", two, [failed("search_unavailable")], type="BatchError",
    applied=0, outcomes_all="not_applied")
add("status 503 outcome_unknown", "docs", two, [failed("outcome_unknown", "unknown")], **unknown_both)
add("error envelope with a stranger's request id", "docs", two,
    [{"status": 400, "raw": '{"request_id":"00000000-0000-0000-0000-000000000000","error":{"code":"validation_error",'
                            '"message":"m","outcome":"not_applied"}}'}], **unknown_both)
add("error envelope with a null request id", "docs", two,
    [{"status": 400, "raw": '{"request_id":null,"error":{"code":"validation_error","message":"m","outcome":"not_applied"}}'}],
    type="BatchError", applied=0, outcomes_all="not_applied")
add("error envelope with a numeric request id", "docs", two,
    [{"status": 400, "raw": '{"request_id":5,"error":{"code":"validation_error","message":"m","outcome":"not_applied"}}'}],
    **unknown_both)
add("error envelope without a request id", "docs", two,
    [{"status": 400, "raw": '{"error":{"code":"validation_error","message":"m","outcome":"not_applied"}}'}],
    **unknown_both)
add("status 500 unknown code claiming not_applied", "docs", two, [failed("weird")], type="BatchError", applied=0)
add("status 500 unknown code claiming unknown", "docs", two, [failed("weird", "unknown")], **unknown_both)
add("status 400 claiming unknown", "docs", two, [failed("validation_error", "unknown")], **unknown_both)
add("status 400 claiming applied", "docs", two, [failed("validation_error", "applied")], **unknown_both)
add("status 400 with the wrong code", "docs", two, [failed("conflict", status=400)], **unknown_both)
for label, answer in (("500 without a body", {"status": 500}), ("502 without a body", {"status": 502}),
                      ("400 without a body", {"status": 400}), ("401 without a body", {"status": 401}),
                      ("413 without a body", {"status": 413}), ("429 without a body", {"status": 429}),
                      ("503 without a body", {"status": 503}),
                      ("500 with text", raw("boom", 500, "text/plain")),
                      ("500 with html", raw("<html>", 500, "text/html")),
                      ("500 with empty json", raw("{}", 500)), ("500 with a null body", raw("null", 500)),
                      ("500 with a list", raw("[]", 500)),
                      ("200 with text", raw("ok", 200, "text/plain")), ("200 without a body", {"status": 200, "raw": ""}),
                      ("200 with an error envelope", {"status": 200, "error": NA}),
                      ("204", {"status": 204, "raw": ""}), ("302", {"status": 302, "raw": ""}),
                      ("100", {"status": 100, "raw": ""}), ("200 with a stranger's request id", raw(
                          '{"request_id":"00000000-0000-0000-0000-000000000000","data":{"outcomes":'
                          '[{"o":"applied"},{"o":"applied"}]}}')),
                      ("200 with a null request id", raw(
                          '{"request_id":null,"data":{"outcomes":[{"o":"applied"},{"o":"applied"}]}}')),
                      ("200 without a request id", raw('{"data":{"outcomes":[{"o":"applied"},{"o":"applied"}]}}')),
                      ("200 with data twice", raw(
                          '{"request_id":$ID,"data":{"outcomes":[{"o":"not_applied","error":' + json.dumps(NA)
                          + '},{"o":"applied"}]},"data":{"outcomes":[{"o":"applied"},{"o":"applied"}]}}')),
                      ("200 with outcomes twice", raw(
                          '{"request_id":$ID,"data":{"outcomes":[{"o":"applied"},{"o":"applied"}],"outcomes":'
                          '[{"o":"applied"},{"o":"applied"}]}}')),
                      ("200 with NaN", raw('{"request_id":$ID,"data":{"outcomes":[{"o":"applied"},NaN]}}')),
                      ("200 with a BOM", raw('﻿{"request_id":$ID,"data":{"outcomes":[{"o":"applied"},'
                                               '{"o":"applied"}]}}')),
                      ("200 with trailing data", raw('{"request_id":$ID,"data":{"outcomes":[{"o":"applied"},'
                                                     '{"o":"applied"}]}} x')),
                      ("200 with a wrong content type", raw('{"request_id":$ID,"data":{"outcomes":[{"o":"applied"},'
                                                            '{"o":"applied"}]}}', 200, "text/json")),
                      ("200 with no content type", {"status": 200, "type": "", "raw":
                          '{"request_id":$ID,"data":{"outcomes":[{"o":"applied"},{"o":"applied"}]}}'}),
                      ("200 with a charset", raw('{"request_id":$ID,"data":{"outcomes":[{"o":"applied"},'
                                                 '{"o":"applied"}]}}', 200, "application/json; charset=utf-8")),
                      ("a reply over 8 MiB", {"fill": 9 * MiB}),
                      ("a dropped connection", {"drop": True}), ("a partial reply", {"partial": True}),
                      ("a reply after the timeout", {"sleep": 2.5, "data": '{"outcomes":[{"o":"applied"},'
                                                                           '{"o":"applied"}]}'})):
    if label.startswith("200 with a charset"):
        add(f"reply: {label}", "docs", two, [answer], type="ok", result_count=2)
    elif label.startswith("200 with data twice"):
        add(f"reply: {label}", "docs", two, [answer], **unknown_both)
    else:
        add(f"reply: {label}", "docs", two, [answer], **unknown_both)
add("a rejected row then a drop does not resend", "docs", upsert({"$seq": [20001, seq_row]}),
    [applied(10000), {"drop": True}, applied(1)], type="BatchError", applied=10000, requests=2, outcome="unknown")
add("a failed first chunk sends nothing more", "docs", upsert({"$seq": [20001, seq_row]}),
    [{"status": 500}, applied(10000)], type="BatchError", applied=0, requests=1, outcome="unknown")
add("one chunk of three is partially applied", "docs", upsert({"$seq": [30000, seq_row]}),
    [applied(10000), {"applied": 9999, "tail": [{"o": "not_applied", "error": NA}]}, applied(10000)],
    type="BatchError", applied=19999, requests=2, outcome="partial")
add("connection cut while sending 8 MiB", "plain", upsert(pad_pair(8 * MiB, "x")), [{"cut": True}],
    type="BatchError", applied=0, outcomes_all="outcome_unknown", outcome="unknown", requests=1)
add("connection cut on a small request", "docs", two, [{"cut": True}], **unknown_both)
add("connection cut on the second chunk", "docs", upsert({"$seq": [20001, seq_row]}),
    [applied(10000), {"cut": True}], type="BatchError", applied=10000, outcomes_all="outcome_unknown",
    outcome="unknown", requests=2)
add("delete_many dropped", "docs", delete(["a", "b"]), [{"drop": True}], **unknown_both)
add("delete_many rejects a row", "docs", delete(["a", "b"]), [outcomes(A, {"o": "not_applied", "error": NA})],
    type="BatchError", applied=1, outcomes=["applied", "not_applied"])
add("empty upsert_many sends nothing", "docs", upsert([]), [applied(0)], type="ok", requests=0, result_count=0)
add("empty delete_many sends nothing", "docs", delete([]), [applied(0)], type="ok", requests=0, result_count=0)
add("upsert_many of a string", "docs", {"method": "upsert_many", "args": ["abc"]}, type="ValidationError",
    requests=0)
add("upsert_many of a mapping of rows", "docs", {"method": "upsert_many", "args": [{"a": 1}]},
    type="ValidationError", requests=0)
add("upsert_many of null rows", "docs", {"method": "upsert_many", "args": [[None]]}, type="ValidationError",
    requests=0)
add("upsert_many with an unknown column", "docs", upsert([docs_row(zzz=1)]), type="ValidationError", requests=0)
add("upsert_many with the second row bad", "docs", upsert([docs_row(), docs_row(id="b", vec=V(1))]),
    [applied(2)], type="ValidationError", requests=0)
add("delete_many of a key twice", "docs", delete(["a", "b", "a"]), [applied(3)], type="ValidationError",
    requests=0)
add("upsert_many of a key twice", "docs", upsert([docs_row(), docs_row()]), [applied(2)],
    type="ValidationError", requests=0)
add("a duplicate where an upsert chunk would split", "docs",
    upsert({"$concat": [{"$seq": [10000, seq_row]}, [docs_row(id="k0")]]}),
    type="ValidationError", requests=0)
add("a duplicate after an upsert chunk was applied", "docs",
    upsert({"$concat": [{"$seq": [10001, seq_row]}, [docs_row(id="k0")]]}),
    [applied(10000)], type="BatchError", applied=10000, outcomes=[], outcome="partial", requests=1)
add("a duplicate where a delete chunk would split", "docs",
    delete({"$concat": [{"$seq": [10000, "k{i}"]}, ["k0"]]}), type="ValidationError", requests=0)
add("a duplicate after a delete chunk was applied", "docs",
    delete({"$concat": [{"$seq": [10001, "k{i}"]}, ["k0"]]}),
    [applied(10000)], type="BatchError", applied=10000, outcomes=[], outcome="partial", requests=1)
add("an invalid row in the second chunk", "docs",
    {"method": "upsert_many", "args": [{"$concat": [{"$seq": [10001, seq_row]}, [docs_row(id="bad", vec=V(1))]]}]},
    [applied(10000)], type="BatchError", applied=10000, outcomes=[], outcome="partial", requests=1)
add("an invalid first row", "docs", {"method": "upsert_many", "args": [[docs_row(id="bad", vec=V(1))]]},
    [applied(1)], type="ValidationError", requests=0)
add("an invalid row after nothing was sent", "docs",
    {"method": "upsert_many", "args": [{"$concat": [{"$seq": [10000, seq_row]}, [docs_row(id="bad", vec=V(1))]]}]},
    [applied(10000)], type="ValidationError", requests=0)
add("an invalid key in the second chunk of deletes", "docs",
    {"method": "delete_many", "args": [{"$concat": [{"$seq": [10001, "k{i}"]}, [I(1)]]}]},
    [applied(10000)], type="BatchError", applied=10000, outcomes=[], outcome="partial", requests=1)


# --- waiting for embeddings -------------------------------------------------------------------------------------
def status(pending, failed=0, state="ready"):
    return {"data": '{"columns":{"emb":{"state":"%s","lag_ms":0,"pending_embeddings":%d,"failed_embeddings":%d,'
                    '"recall":0.5}}}' % (state, pending, failed)}


chunk_row = {"id": "a", "body": "text", "note": "n", "year": I(1), "sp": {"$sparse": []}, "mu": [], "tags": []}
waiting = {"method": "upsert_many", "args": [[chunk_row]], "options": {"wait_for_embedding": True}}
add("wait until nothing is pending", "chunks", waiting, [applied(1), status(2), status(1), status(0)],
    type="ok", requests=4, result_count=1)
add("wait ends on failed embeddings", "chunks", waiting, [applied(1), status(0, failed=3, state="failed")],
    type="ok", requests=2, result_count=1)
add("wait with a missing column", "chunks", waiting,
    [applied(1), {"data": '{"columns":{}}'}], type="BatchError", applied=1, outcome="applied", outcomes=[])
add("wait that never ends", "chunks", waiting, [applied(1), status(1)], type="BatchError", applied=1,
    outcome="applied", outcomes=[])
add("wait with a failing status request", "chunks", waiting, [applied(1), {"status": 500}], type="BatchError",
    applied=1, outcome="applied", requests=2)
add("wait with a malformed status", "chunks", waiting, [applied(1), {"data": "null"}], type="BatchError",
    applied=1, outcome="applied", requests=2)
add("wait without rows sends nothing", "chunks", {**waiting, "args": [[]]}, [applied(0)], type="ok", requests=0,
    result_count=0)
add("wait on a table without embeddings", "docs", {**waiting, "args": [[docs_row()]]}, [applied(1)],
    type="ValidationError", requests=0)
add("wait that is not a bool", "chunks", {**waiting, "options": {"wait_for_embedding": 1}}, [applied(1)],
    type="ValidationError", requests=0)
add("wait after a rejected row does not poll", "chunks", {**waiting, "args": [[chunk_row, {**chunk_row, "id": "b"}]]},
    [{"data": '{"outcomes":[{"o":"applied"},{"o":"not_applied","error":{"code":"conflict","message":"m",'
              '"outcome":"not_applied"}}]}'}, status(0)], type="BatchError", applied=1, requests=1)
add("an embedded column in a row", "chunks", {"method": "upsert_many", "args": [[{**chunk_row, "emb": V(1, 2, 3)}]]},
    [applied(1)], type="ValidationError", requests=0)
add("an embedded column set to null in a row", "chunks",
    {"method": "upsert_many", "args": [[{**chunk_row, "emb": None}]]}, [applied(1)], type="ValidationError", requests=0)

# --- G. ranked replies and misuse -------------------------------------------------------------------------------
cos = {"terminal": "collect", "limit": 3, "rank": [["emb", "cosine", V(1, 0, 0)]]}
good_row = {"id": "a", "body": "b", "note": "n", "year": 1, "tags": []}


def rows_reply(*rows):
    return {"data": json.dumps(list(rows), separators=(",", ":"))}


def row_text(**over):
    return json.dumps({**good_row, "_score": 0.5, **over}, separators=(",", ":"))


def ranked(label, text, **expect):
    add(f"ranked reply {label}", "chunks", {"method": "query", "options": cos}, [{"data": text}], **expect)


bad = dict(type=["ProtocolError"])
ranked("good", "[" + row_text() + "]", type="ok")
ranked("empty", "[]", type="ok", result_len=0)
for label, score in (("1e999", "1e999"), ("-1e999", "-1e999"), ("NaN", "NaN"), ("-Infinity", "-Infinity"),
                     ("Infinity", "Infinity"), ("an int", "1"), ("a big int", "100000000000000000000"),
                     ("a string", '"1.0"'), ("null", "null"), ("true", "true"), ("a list", "[]"), ("an object", "{}")):
    ranked(f"with _score {label}", "[" + row_text().replace("0.5", score) + "]", **bad)
for label, score in (("1e308", "1e308"), ("-0.0", "-0.0"), ("5e-324", "5e-324"), ("1.0", "1.0"), ("1E2", "1E2"),
                     ("1e2", "1e2"), ("0.1e1", "0.1e1")):
    ranked(f"with _score {label}", "[" + row_text().replace("0.5", score) + "]", type="ok")
ranked("without _score", json.dumps([good_row]), **bad)
ranked("with an extra column", "[" + row_text(extra=1) + "]", **bad)
ranked("with a vector column nobody selected", "[" + row_text(emb={"f32": "AAAAAAAAAAAAAAAA"}) + "]", **bad)
for column in good_row:
    row = {**good_row, "_score": 0.5}
    del row[column]
    ranked(f"without {column}", json.dumps([row]), **bad)
for label, over in (("null id", {"id": None}), ("numeric id", {"id": 1}), ("float year", {"year": 1.5}),
                    ("string year", {"year": "1"}), ("null year", {"year": None}), ("huge year", {"year": 2 ** 64}),
                    ("null body", {"body": None}), ("tags of numbers", {"tags": [1]}), ("null tags", {"tags": None}),
                    ("a bool year", {"year": True})):
    ranked(f"with {label}", "[" + row_text(**over) + "]", **bad)
for label, year in (("int64 max", "9223372036854775807"), ("int64 min", "-9223372036854775808"),
                    ("zero", "0"), ("minus zero", "-0")):
    ranked(f"with year {label}", "[" + row_text().replace('"year":1', f'"year":{year}') + "]", type="ok")
for label, year in (("one past int64 max", "9223372036854775808"), ("one below int64 min", "-9223372036854775809"),
                    ("an exponent", "1e2"), ("a decimal", "1.0"), ("a leading zero", "01"), ("a plus sign", "+1")):
    ranked(f"with year {label}", "[" + row_text().replace('"year":1', f'"year":{year}') + "]", **bad)
ranked("with a lone surrogate escape in a string", "[" + row_text().replace('"body":"b"', '"body":"' + chr(92) + 'ud800"') + "]",
       type=["ok", "ProtocolError"])


def rawbytes(prefix, suffix):
    return prefix.encode().hex() + "$ID" + suffix.hex()


add("ranked reply with invalid UTF-8", "chunks", {"method": "query", "options": cos},
    [{"rawhex": rawbytes('{"request_id":', b',"data":[{"id":"\xff","body":"b","note":"n","year":1,"tags":[],"_score":0.5}]}')}], **bad)
add("write reply with invalid UTF-8 in an extra field", "docs", two,
    [{"rawhex": rawbytes('{"request_id":', b',"data":{"outcomes":[{"o":"applied"},{"o":"applied"}],"x":"\xff"}}')}],
    **unknown_both)
add("write reply with invalid UTF-8 and a valid shape", "docs", two,
    [{"rawhex": rawbytes('{"request_id":', b',"data":{"outcomes":[{"o":"applied"},{"o":"applied"}]}}\xff')}],
    **unknown_both)
ranked("with 4 rows for limit 3", "[" + ",".join([row_text()] * 4) + "]", **bad)
ranked("with 3 rows for limit 3", "[" + ",".join([row_text()] * 3) + "]", type="ok", result_len=3)
add("ranked reply with 10001 rows", "chunks", {"method": "query", "options": cos},
    [{"repeat": [row_text(), 10001]}], **bad)
ranked("as an object", "{}", **bad)
ranked("as null", "null", **bad)
ranked("as a row", row_text(), **bad)
ranked("with a null row", "[null]", **bad)
ranked("with duplicate fields", '[{"id":"a","id":"b","body":"b","note":"n","year":1,"tags":[],"_score":0.5}]', **bad)
sel = {"terminal": "collect", "limit": 3, "select": ["id", "emb"], "rank": [["emb", "cosine", V(1, 0, 0)]]}


def vec_b64(*xs, raw_bytes=None):
    return base64.b64encode(raw_bytes if raw_bytes is not None else struct.pack(f"<{len(xs)}f", *xs)).decode()


def vector_reply(label, field, **expect):
    add(f"ranked vector {label}", "chunks", {"method": "query", "options": sel},
        [{"data": '[{"id":"a","emb":' + field + ',"_score":0.5}]'}], **expect)


good_vec = vec_b64(1, 2, 3)
vector_reply("good", '{"f32":"' + good_vec + '"}', type="ok")
vector_reply("null", "null", type="ok")
vector_reply("with NaN", '{"f32":"' + vec_b64(1, float("nan"), 3) + '"}', **bad)
vector_reply("with a signalling NaN", '{"f32":"' + vec_b64(raw_bytes=b"\x00\x00\x00\x3f\x01\x00\x80\x7f\x00\x00\x00\x00") + '"}', **bad)
vector_reply("with infinity", '{"f32":"' + vec_b64(1, float("inf"), 3) + '"}', **bad)
vector_reply("with -infinity", '{"f32":"' + vec_b64(1, float("-inf"), 3) + '"}', **bad)
vector_reply("with huge finite values", '{"f32":"' + vec_b64(3.4028234663852886e38, -3.4028234663852886e38, 0) + '"}',
             type="ok")
vector_reply("with -0.0 and subnormals", '{"f32":"' + vec_b64(-0.0, 1.401298464324817e-45, 0) + '"}', type="ok")
vector_reply("with two floats", '{"f32":"' + vec_b64(1, 2) + '"}', **bad)
vector_reply("with four floats", '{"f32":"' + vec_b64(1, 2, 3, 4) + '"}', **bad)
vector_reply("with no floats", '{"f32":""}', **bad)
vector_reply("truncated by one character", '{"f32":"' + good_vec[:-1] + '"}', **bad)
vector_reply("truncated by two characters", '{"f32":"' + good_vec[:-2] + '"}', **bad)
vector_reply("truncated to a partial float", '{"f32":"' + vec_b64(raw_bytes=b"\x00" * 11) + '"}', **bad)
vector_reply("without padding", '{"f32":"' + good_vec.rstrip("=") + '"}', type="ok" if "=" not in good_vec else "ProtocolError")
vector_reply("with a newline", '{"f32":"' + good_vec + '\\n"}', **bad)
vector_reply("with a space", '{"f32":"' + good_vec[:4] + " " + good_vec[4:] + '"}', **bad)
vector_reply("with a NUL", '{"f32":"' + good_vec[:4] + "\\u0000" + good_vec[4:] + '"}', **bad)
vector_reply("with non-ASCII digits", '{"f32":"' + "Ａ" + good_vec[1:] + '"}', **bad)
vector_reply("in urlsafe base64", '{"f32":"' + vec_b64(raw_bytes=b"\xfb\xff\xbf" * 4).replace("+", "-").replace("/", "_") + '"}', **bad)
vector_reply("in standard base64 with plus and slash", '{"f32":"' + vec_b64(raw_bytes=b"\x00\x00\x00\x3f\xfb\xef\xff\x3d\x00\x00\x00\x00") + '"}', type="ok")
sparse_spare = '{"sparse":{"i":"AQAAAB==","f32":"' + vec_b64(1) + '"}}'
add("ranked sparse with spare bits set", "chunks", {"method": "query", "options": {
    "terminal": "collect", "limit": 3, "select": ["id", "sp"], "rank": [["sp", "dot", {"$sparse": [[1, F(1.0)]]}]]}},
    [{"data": '[{"id":"a","sp":' + sparse_spare + ',"_score":0.5}]'}], **bad)
vector_reply("as a number", "1", **bad)
vector_reply("as a string", '"' + good_vec + '"', **bad)
vector_reply("as a list", "[1,2,3]", **bad)
vector_reply("with an extra field", '{"f32":"' + good_vec + '","x":1}', **bad)
vector_reply("with a null payload", '{"f32":null}', **bad)
vector_reply("with a numeric payload", '{"f32":1}', **bad)
vector_reply("with a list payload", '{"f32":["' + good_vec + '"]}', **bad)
vector_reply("with the sparse shape", '{"sparse":{"i":"","f32":""}}', **bad)
vector_reply("with duplicate payloads", '{"f32":"' + good_vec + '","f32":"' + vec_b64(1, 2) + '"}', **bad)
spsel = {"terminal": "collect", "limit": 3, "select": ["id", "sp"], "rank": [["sp", "dot", {"$sparse": [[1, F(1.0)]]}]]}


def sparse_reply(label, field, **expect):
    add(f"ranked sparse {label}", "chunks", {"method": "query", "options": spsel},
        [{"data": '[{"id":"a","sp":' + field + ',"_score":0.5}]'}], **expect)


def sp(indices, weights):
    return ('{"sparse":{"i":"' + base64.b64encode(struct.pack(f"<{len(indices)}I", *indices)).decode()
            + '","f32":"' + vec_b64(*weights) + '"}}')


sparse_reply("good", sp([1, 5, 4294967295], [1, 2, 3]), type="ok")
sparse_reply("empty", sp([], []), type="ok")
sparse_reply("descending", sp([5, 1], [1, 2]), **bad)
sparse_reply("equal indices", sp([1, 1], [1, 2]), **bad)
sparse_reply("fewer weights", sp([1, 2], [1]), **bad)
sparse_reply("more weights", sp([1], [1, 2]), **bad)
sparse_reply("with a NaN weight", sp([1], [float("nan")]), **bad)
sparse_reply("with an infinite weight", sp([1], [float("inf")]), **bad)
sparse_reply("with partial indices", '{"sparse":{"i":"' + vec_b64(raw_bytes=b"\x01\x00\x00") + '","f32":"' + vec_b64(1) + '"}}', **bad)
sparse_reply("with a null sparse", '{"sparse":null}', **bad)
sparse_reply("with a mapping", '{"sparse":{"1":1.0}}', **bad)
sparse_reply("with a list", '{"sparse":[[1,1.0]]}', **bad)
sparse_reply("with extra fields", '{"sparse":{"i":"","f32":"","x":1}}', **bad)
musel = {"terminal": "collect", "limit": 3, "select": ["id", "mu"], "rank": [["mu", "cosine", [V(1, 0)]]]}


def multi_reply(label, field, **expect):
    add(f"ranked multi {label}", "chunks", {"method": "query", "options": musel},
        [{"data": '[{"id":"a","mu":' + field + ',"_score":0.5}]'}], **expect)


for label, count, values in (("good", "2", (1, 2, 3, 4)), ("empty", "0", ()), ("count too high", "3", (1, 2, 3, 4)),
                             ("count too low", "1", (1, 2, 3, 4)), ("negative count", "-1", (1, 2)),
                             ("float count", "2.0", (1, 2, 3, 4)), ("string count", '"2"', (1, 2, 3, 4)),
                             ("bool count", "true", (1, 2)), ("null count", "null", (1, 2)),
                             ("ragged", "1", (1, 2, 3)), ("a NaN", "2", (1, 2, 3, float("nan"))),
                             ("a huge count", "9223372036854775808", (1, 2))):
    multi_reply(label, '{"f32s":"' + vec_b64(*values) + '","n":' + count + "}",
                **({"type": "ok"} if label in ("good", "empty") else bad))
multi_reply("with a missing count", '{"f32s":"' + vec_b64(1, 2) + '"}', **bad)
multi_reply("as a list of vectors", '[{"f32":"' + vec_b64(1, 2) + '"}]', **bad)

# rank_many replies
many = {"terminal": "rank_many", "limit_many": 2, "rankings": [["emb", "cosine", V(1, 0, 0)], ["emb", "dot", V(1, 0, 0)]]}
good_list = "[" + row_text() + "]"
for label, text, expect in (("good", f"[{good_list},[]]", {"type": "ok", "result_len": 2}),
                            ("one list", f"[{good_list}]", bad), ("three lists", "[[],[],[]]", bad),
                            ("a flat list", good_list, bad), ("an inner row over the limit", f"[[{row_text()},{row_text()},{row_text()}],[]]", bad),
                            ("a null list", "[[],null]", bad), ("an object", "{}", bad)):
    add(f"rank_many reply {label}", "chunks", {"method": "query", "options": many}, [{"data": text}], **expect)

# misuse
base = {"terminal": "collect", "limit": 3}


def misuse(label, table="chunks", expect=None, script=None, **options):
    add(f"misuse {label}", table, {"method": "query", "options": {**base, **options}},
        script if script is not None else [{"data": "[]"}], **(expect or {"type": ["ValidationError", "TypeError"], "requests": 0}))


misuse("ranking in where", where_ranking=["emb", "cosine", V(1, 0, 0)])
misuse("order_by after rank", rank=[["emb", "cosine", V(1, 0, 0)]], order_by="year")
misuse("order_by before rank", order_by_first="year", rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("rank twice replaces", expect={"type": "ok", "request_contains": ['"op":"dot"'], "request_lacks": ['"op":"cosine"']},
       rank=[["emb", "cosine", V(1, 0, 0)]], then_rank=[["emb", "dot", V(1, 0, 0)]])
misuse("rerank without rank", reranks=[[["emb", "cosine", V(1, 0, 0)]]])
misuse("rerank twice", expect={"type": "ok"}, rank=[["emb", "cosine", V(1, 0, 0)]],
       reranks=[[["mu", "dot", [V(1, 0)]]], [["mu", "cosine", [V(1, 0)]]]])
misuse("rerank with l2 on a sparse column", rank=[["emb", "cosine", V(1, 0, 0)]], reranks=[[["sp", "l2", {"$sparse": []}]]])
misuse("rank with nothing", rank=[])
misuse("l2 on sparse", rank=[["sp", "l2", {"$sparse": [[1, F(1.0)]]}]])
misuse("l2 on multi", rank=[["mu", "l2", [V(1, 0)]]])
misuse("cosine on a zero dense query", rank=[["emb", "cosine", V(0, 0, 0)]])
misuse("cosine on a negative zero query", rank=[["emb", "cosine", V(-0.0, -0.0, -0.0)]])
misuse("cosine on an empty sparse query", rank=[["sp", "cosine", {"$sparse": []}]])
misuse("cosine on a sparse query of zeros", rank=[["sp", "cosine", {"$sparse": [[1, F(0.0)], [2, F(-0.0)]]}]])
misuse("cosine on a multi query with a zero vector", rank=[["mu", "cosine", [V(1, 0), V(0, 0)]]])
misuse("cosine on a subnormal that rounds to zero", rank=[["emb", "cosine", V(1e-50, 0, 0)]])
misuse("dot on a zero dense query", expect={"type": "ok"}, rank=[["emb", "dot", V(0, 0, 0)]])
misuse("dot on an empty multi query", rank=[["mu", "dot", []]])
misuse("text on a column without a model", table="lean", rank=[["emb", "cosine", "hello"]])
misuse("text on a sparse column", rank=[["sp", "dot", "hello"]])
misuse("match on a column without full text", rank=[["note", "match", "hello"]])
misuse("match on a vector column", rank=[["emb", "match", "hello"]])
misuse("cosine on a full-text column", rank=[["body", "cosine", V(1, 0, 0)]])
misuse("cosine text on the embedded column", expect={"type": "ok", "request_contains": ['"value":"hello"']},
       rank=[["emb", "cosine", "hello"]])
misuse("match", expect={"type": "ok"}, rank=[["body", "match", "hello"]])
misuse("match with an empty string", expect={"type": "ok"}, rank=[["body", "match", ""]])
misuse("cosine text empty", expect={"type": "ok"}, rank=[["emb", "cosine", ""]])
misuse("a query with a vector for the wrong column", rank=[["emb", "cosine", V(1, 0)]])
misuse("an unknown column", rank=[["nope", "cosine", V(1, 0, 0)]])
misuse("a ranking on _score", rank=[["_score", "cosine", V(1, 0, 0)]])
misuse("no limit", expect={"type": ["ValidationError"], "requests": 0}, limit=None, rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("limit 1000", expect={"type": "ok"}, limit=1000, rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("limit 1001", expect={"type": ["LimitExceededError"], "requests": 0}, limit=1001, rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("limit 0", expect={"type": "ok"}, limit=0, rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("count of a ranked query", terminal="count", rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("explain of a ranked query", terminal="explain", rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("select _score", select=["_score"], rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("select nothing", select=[], rank=[["emb", "cosine", V(1, 0, 0)]])
misuse("select a vector", expect={"type": "ok"}, select=["emb"], rank=[["emb", "cosine", V(1, 0, 0)]],
       script=[{"data": '[{"emb":{"f32":"' + good_vec + '"},"_score":0.5}]'}])
two_rankings = [["emb", "cosine", V(1, 0, 0)], ["body", "match", "x"]]
for label, option, expect in (
        ("fusion weights", {"fusion": {"$list": [F(1), F(2)]}}, {"type": "ok", "request_contains": ['"fusion":[1.0,2.0]']}),
        ("fusion weights of ints", {"fusion": [1, 2]}, {"type": "ok", "request_contains": ['"fusion":[1.0,2.0]']}),
        ("fusion weights of 1e16", {"fusion": {"$list": [F(1e16), F(1)]}}, {"type": "ok"}),
        ("fusion weights of 1e-7", {"fusion": {"$list": [F(1e-7), F(1)]}}, {"type": "ok"}),
        ("fusion weights of the maximum", {"fusion": {"$list": [F(1.7976931348623157e308), F(5e-324)]}}, {"type": "ok"}),
        ("fusion all zero", {"fusion": {"$list": [F(0.0), F(0.0)]}}, {"type": "ok"}),
        ("fusion negative zero", {"fusion": {"$list": [F(-0.0), F(1.0)]}}, {"type": "ok"}),
        ("fusion one weight for two rankings", {"fusion": {"$list": [F(1)]}}, {"type": "ValidationError", "requests": 0}),
        ("fusion three weights for two rankings", {"fusion": {"$list": [F(1), F(1), F(1)]}}, {"type": "ValidationError", "requests": 0}),
        ("fusion a string", {"fusion": "rrf"}, {"type": "ok", "request_contains": ['"fusion":"rrf"']}),
        ("fusion a nested list", {"fusion": [[1], [2]]}, {"type": "ValidationError", "requests": 0}),
        ("recall 1", {"recall": 1}, {"type": "ok"}), ("recall 1.0", {"recall": F(1.0)}, {"type": "ok"}),
        ("recall 1e-7", {"recall": F(1e-7)}, {"type": "ok"}), ("recall 5e-324", {"recall": F(5e-324)}, {"type": "ok"}),
        ("recall 0.95", {"recall": F(0.95)}, {"type": "ok"}),
        ("recall 0.30000000000000004", {"recall": F(0.30000000000000004)}, {"type": "ok"}),
        ("candidates 4294967295", {"candidates": 4294967295}, {"type": "ok"}),
        ("candidates 1", {"candidates": 1}, {"type": "ok"}),
        ("candidates 2^53 + 1", {"candidates": 9007199254740993}, {"type": "ValidationError", "requests": 0}),
        ("exact", {"exact": True}, {"type": "ok", "request_contains": ['"exact":true']}),
        ("consistency snapshot", {"consistency": "snapshot"}, {"type": "ok"}),
        ("consistency SNAPSHOT", {"consistency": "SNAPSHOT"}, {"type": "ValidationError", "requests": 0}),
        ("an unknown option", {"fusions": "rrf"}, {"type": ["RAW:TypeError", "ValidationError"], "requests": 0})):
    misuse(label, expect=expect, rank=two_rankings, rank_options=option)
for label, option, expect in (
        ("rank_many with weights for one", {"fusion": {"$list": [F(1)]}}, {"type": ["ok", "ValidationError"]}),
        ("rank_many with weights for two", {"fusion": {"$list": [F(1), F(1)]}}, {"type": ["ok", "ValidationError"]}),
        ("rank_many recall 1", {"recall": 1}, {"type": "ok"}),
        ("rank_many exact", {"exact": True}, {"type": "ok"})):
    add(f"misuse {label}", "chunks", {"method": "query", "options": {**many, "rank_options": option}},
        [{"data": "[[],[]]"}], **expect)
for count, expected in ((256, {"type": "ok", "requests": 1}), (257, {"type": "LimitExceededError", "requests": 0})):
    add(f"rank with {count} rankings", "chunks", {"method": "query", "options": {
        "terminal": "collect", "limit": 1, "rank": [["emb", "cosine", V(1, 0, 0)]] * count}},
        [{"data": "[]"}], **expected)
add("misuse rank_many of 256", "chunks", {"method": "query", "options": {
    "terminal": "rank_many", "limit_many": 2, "rankings": {"$repeat": [["emb", "cosine", V(1, 0, 0)], 256]}}},
    [{"data": json.dumps([[]] * 256)}], type="ok")
add("misuse rank_many of 257", "chunks", {"method": "query", "options": {
    "terminal": "rank_many", "limit_many": 2, "rankings": {"$repeat": [["emb", "cosine", V(1, 0, 0)], 257]}}},
    [{"data": json.dumps([[]] * 257)}], type="LimitExceededError", requests=0)
add("misuse rank_many of none", "chunks", {"method": "query", "options": {
    "terminal": "rank_many", "limit_many": 2, "rankings": []}}, [{"data": "[]"}], type=["ValidationError"], requests=0)
add("misuse rank_many after rank", "chunks", {"method": "query", "options": {
    **many, "rank": [["emb", "cosine", V(1, 0, 0)]]}}, [{"data": "[[],[]]"}], type=["ValidationError"], requests=0)
add("misuse rank_many limit 0", "chunks", {"method": "query", "options": {**many, "limit_many": 0}},
    [{"data": "[[],[]]"}], type="ok")
add("misuse rank_many limit 1001", "chunks", {"method": "query", "options": {**many, "limit_many": 1001}},
    [{"data": "[[],[]]"}], type=["LimitExceededError"], requests=0)
add("misuse rank_many with a filter", "chunks", {"method": "query", "options": {
    **many, "where": ["eq", "year", I(2024)]}}, [{"data": "[[],[]]"}], type="ok")

# --- create_table limits ----------------------------------------------------------------------------------------
def create(label, schema, options=None, key="id", **expect):
    add(f"create {label}", "docs", {"method": "create_table", "schema": schema, "options": options or {}, "key": key},
        [{"echo_create": True}], **expect)


def vectors(count, **extra):
    return {"id": "utf8", **{f"v{i}": {"type": "vector", "dim": 2} for i in range(count)}, **extra}


DENSE = {"type": "vector", "dim": 2}
create("with eight vector columns", vectors(8), type="ok", requests=1)
create("with nine vector columns", vectors(9), type="LimitExceededError", requests=0)
create("with seven vectors and one full text", vectors(7, body="utf8"), {"full_text": {"body": "english"}},
       type="ok", requests=1)
create("with eight vectors and one full text", vectors(8, body="utf8"), {"full_text": {"body": "english"}},
       type="LimitExceededError", requests=0)
create("with eight full text columns", {"id": "utf8", **{f"t{i}": "utf8" for i in range(8)}},
       {"full_text": {f"t{i}": "english" for i in range(8)}}, type="ok", requests=1)
create("with nine full text columns", {"id": "utf8", **{f"t{i}": "utf8" for i in range(9)}},
       {"full_text": {f"t{i}": "english" for i in range(9)}}, type="LimitExceededError", requests=0)
create("with sparse and multi columns counted",
       {"id": "utf8", "s": {"type": "vector", "sparse": True}, "m": {"type": "list", "item": DENSE},
        **{f"v{i}": DENSE for i in range(6)}}, type="ok", requests=1)
create("with tags not counted", {"id": "utf8", "t": {"type": "list", "item": "utf8"},
                                 **{f"v{i}": DENSE for i in range(8)}}, type="ok", requests=1)
create("with _score and no search column", {"id": "utf8", "_score": "float64"}, type="ok", requests=1)
create("with _score and a vector", {"id": "utf8", "_score": "float64", "v": DENSE}, type="ValidationError", requests=0)
create("with _score and full text", {"id": "utf8", "_score": "utf8"}, {"full_text": {"_score": "english"}},
       type="ValidationError", requests=0)
create("with 128 columns", {"id": "utf8", **{f"c{i}": "int64" for i in range(127)}}, type="ok", requests=1)
create("with 129 columns", {"id": "utf8", **{f"c{i}": "int64" for i in range(128)}},
       type=["LimitExceededError", "ValidationError"], requests=0)
create("with a vector key", {"id": DENSE}, type="ValidationError", requests=0)
create("with a list key", {"id": {"type": "list", "item": "utf8"}}, type="ValidationError", requests=0)
create("with partition_by a bool", {"id": "utf8", "b": "bool"}, {"partition_by": "b"}, type="ValidationError", requests=0)
create("with partition_by a float", {"id": "utf8", "f": "float64"}, {"partition_by": "f"}, type="ValidationError",
       requests=0)
create("with partition_by an int", {"id": "utf8", "n": "int64"}, {"partition_by": "n"}, type="ok", requests=1)
create("with partition_by the key", {"id": "utf8"}, {"partition_by": "id"}, type="ok", requests=1)
create("with partition_by unknown", {"id": "utf8"}, {"partition_by": "zz"}, type="ValidationError", requests=0)
create("with embed from itself", {"id": "utf8", "v": DENSE}, {"embed": {"v": {"source": "v", "model": "m"}}},
       type="ValidationError", requests=0)
create("with embed on a sparse column", {"id": "utf8", "t": "utf8", "v": {"type": "vector", "sparse": True}},
       {"embed": {"v": {"source": "t", "model": "m"}}}, type="ValidationError", requests=0)
create("with embed on a multi column", {"id": "utf8", "t": "utf8", "v": {"type": "list", "item": DENSE}},
       {"embed": {"v": {"source": "t", "model": "m"}}}, type="ValidationError", requests=0)
create("with embed extra keys", {"id": "utf8", "t": "utf8", "v": DENSE},
       {"embed": {"v": {"source": "t", "model": "m", "x": 1}}}, type="ValidationError", requests=0)
create("with embed and full text on one column", {"id": "utf8", "t": "utf8", "v": DENSE},
       {"embed": {"v": {"source": "t", "model": "m"}}, "full_text": {"t": "english"}}, type="ok", requests=1)
for language in ("English", "en-US", "klingon"):
    create(f"with unsupported language {language}", {"id": "utf8", "t": "utf8"}, {"full_text": {"t": language}},
           type="ValidationError", requests=0)
    add(f"add full text with unsupported language {language}", "chunks",
        {"method": "add_full_text", "args": ["note", language]}, type="ValidationError", requests=0)
for model in ("docs v1", "-m", "m" * 65, "m\n"):
    create(f"with malformed model {model!r}", {"id": "utf8", "t": "utf8", "v": DENSE},
           {"embed": {"v": {"source": "t", "model": model}}}, type="ValidationError", requests=0)
    add(f"add a column with malformed model {model!r}", "chunks",
        {"method": "add_column", "args": ["new_emb", {"$type": DENSE}],
         "options": {"embed": {"source": "body", "model": model}}}, type="ValidationError", requests=0)
create("with full text on a vector", {"id": "utf8", "v": DENSE}, {"full_text": {"v": "english"}},
       type="ValidationError", requests=0)
create("with an empty language", {"id": "utf8", "t": "utf8"}, {"full_text": {"t": ""}}, type="ValidationError",
       requests=0)
create("with dim 4096", {"id": "utf8", "v": {"type": "vector", "dim": 4096}}, type="ok", requests=1)
create("with dim 4097", {"id": "utf8", "v": {"type": "vector", "dim": 4097}}, type=["ValidationError"], requests=0)

# --- H. hostile descriptors -------------------------------------------------------------------------------------
CHUNKS = {"id": "00000000-0000-0000-0000-000000000031", "key": "id", "format": "parquet",
          "schema": {"id": "utf8", "body": "utf8", "year": "int64", "emb": {"type": "vector", "dim": 3}}}


def descriptor(label, ok=False, **over):
    table = {**CHUNKS, **over}
    for name in [name for name, value in over.items() if value == "$drop"]:
        del table[name]
    if "3.0" in label:  # JavaScript cannot write 3.0, so the descriptor goes out as text
        table = {"$raw": json.dumps({"name": "custom", **table})}
    add(f"descriptor {label}", table, {"method": "noop", "args": []},
        **({"type": "ok"} if ok else {"type": ["ProtocolError", "ValidationError"]}))


def with_schema(**columns):
    return {**CHUNKS["schema"], **columns}


descriptor("good", True)
descriptor("with options", True, partition_by="id", full_text={"body": "english"},
           embed={"emb": {"source": "body", "model": "m"}})
for label, wire in (("dim 0", {"type": "vector", "dim": 0}), ("dim 4097", {"type": "vector", "dim": 4097}),
                    ("dim 4096", {"type": "vector", "dim": 4096}), ("dim -1", {"type": "vector", "dim": -1}),
                    ("dim as a string", {"type": "vector", "dim": "3"}), ("dim 3.0", {"type": "vector", "dim": 3.0}),
                    ("dim 3.5", {"type": "vector", "dim": 3.5}), ("dim true", {"type": "vector", "dim": True}),
                    ("dim null", {"type": "vector", "dim": None}), ("dim huge", {"type": "vector", "dim": 2 ** 64}),
                    ("no dim", {"type": "vector"}), ("sparse false", {"type": "vector", "sparse": False}),
                    ("sparse and dim", {"type": "vector", "sparse": True, "dim": 3}),
                    ("dim and sparse false", {"type": "vector", "dim": 3, "sparse": False}),
                    ("a metric", {"type": "vector", "dim": 3, "metric": "cosine"}),
                    ("sparse as a string", {"type": "vector", "sparse": "true"}),
                    ("an unknown type", {"type": "matrix", "dim": 3}), ("type as a list", {"type": ["vector"], "dim": 3}),
                    ("a bare string vector", "vector"), ("a null type", None), ("a number type", 3),
                    ("a list of lists", {"type": "list", "item": {"type": "list", "item": "utf8"}}),
                    ("a list of sparse", {"type": "list", "item": {"type": "vector", "sparse": True}}),
                    ("a list of ints", {"type": "list", "item": "int64"}),
                    ("a list without an item", {"type": "list"}),
                    ("a list with extra fields", {"type": "list", "item": "utf8", "max": 3}),
                    ("a list of a bad vector", {"type": "list", "item": {"type": "vector", "dim": 0}}),
                    ("a list of utf8 as a list", {"type": "list", "item": ["utf8"]}),
                    ("an unknown scalar", "decimal")):
    descriptor(f"column {label}", ok=label in ("dim 4096",), schema=with_schema(extra=wire))
descriptor("a list of vectors", True, schema=with_schema(extra={"type": "list", "item": {"type": "vector", "dim": 2}}))
for depth in (50, 400, 900, 3000, 100000):
    add(f"descriptor with lists nested {depth} deep", {"$nest": depth}, {"method": "noop", "args": []},
        type=["ProtocolError", "ValidationError"])
descriptor("full text on an unknown column", full_text={"nope": "english"})
descriptor("full text on a vector", full_text={"emb": "english"})
descriptor("full text on an int", full_text={"year": "english"})
descriptor("full text with an empty language", full_text={"body": ""})
descriptor("full text with a null language", full_text={"body": None})
descriptor("full text with a numeric language", full_text={"body": 1})
descriptor("full text as a list", full_text=["body"])
descriptor("full text null", full_text=None)
descriptor("full text empty", full_text={})
descriptor("full text a string", full_text="body")
descriptor("embed null", embed=None)
descriptor("embed empty", embed={})
descriptor("embed on an unknown column", embed={"nope": {"source": "body", "model": "m"}})
descriptor("embed on a string column", embed={"body": {"source": "body", "model": "m"}})
descriptor("embed from a vector", embed={"emb": {"source": "emb", "model": "m"}})
descriptor("embed from an unknown column", embed={"emb": {"source": "nope", "model": "m"}})
descriptor("embed from an int", embed={"emb": {"source": "year", "model": "m"}})
descriptor("embed without a model", embed={"emb": {"source": "body"}})
descriptor("embed with an empty model", embed={"emb": {"source": "body", "model": ""}})
descriptor("embed with extra fields", embed={"emb": {"source": "body", "model": "m", "x": 1}})
descriptor("embed with a null model", embed={"emb": {"source": "body", "model": None}})
descriptor("embed as a list", embed=[{"source": "body", "model": "m"}])
descriptor("embed with a string", embed={"emb": "body"})
descriptor("partition_by an unknown column", partition_by="nope")
descriptor("partition_by a vector", partition_by="emb")
descriptor("partition_by null", partition_by=None)
descriptor("partition_by empty", partition_by="")
descriptor("partition_by a number", partition_by=1)
descriptor("partition_by a string column", True, partition_by="body")
descriptor("partition_by an int column", True, partition_by="year")
descriptor("an unknown option", metric="cosine")
descriptor("an unknown null option", metric=None)
descriptor("_score with a vector", schema=with_schema(_score={"type": "vector", "dim": 2}))
descriptor("_score as a float", schema=with_schema(_score="float64"))
descriptor("nine search columns", schema={**CHUNKS["schema"], **{f"v{i}": {"type": "vector", "dim": 2} for i in range(8)}})
descriptor("eight search columns", True, schema={**CHUNKS["schema"], **{f"v{i}": {"type": "vector", "dim": 2} for i in range(7)}})
descriptor("two thousand columns", schema={"id": "utf8", **{f"c{i}": "int64" for i in range(2000)}})

descriptor("a mismatched name", name="other")
descriptor("a bad id", id="nope")
descriptor("an uppercase id", True, id=CHUNKS["id"].upper())
descriptor("a null key", key=None)
descriptor("a key that is a vector", key="emb")
descriptor("a key that is a list", schema=with_schema(tags={"type": "list", "item": "utf8"}), key="tags")
descriptor("a key that is a float", schema={"id": "float64", "emb": {"type": "vector", "dim": 3}})
descriptor("without a format", format="$drop")
descriptor("a bad format", format="csv")
descriptor("an extra field", extra=1)

S.sort(key=lambda item: 0)
names = [item["name"] for item in S]
assert len(set(names)) == len(names), [name for name in names if names.count(name) > 1]
OUT.write_text(json.dumps(S, ensure_ascii=True, indent=None, separators=(",", ":")) + "\n", encoding="ascii")
print(len(S), "scenarios", OUT.stat().st_size, "bytes")
