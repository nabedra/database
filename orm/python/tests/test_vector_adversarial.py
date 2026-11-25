"""Adversarial tests of the vector search SDK: VEC-G12 (parity), VEC-G09 (outcomes) and VEC-G11 (limits).

The scenarios live in orm/contract/vector/adversarial.json, written by tests/support/gen_adversarial.py. The
TypeScript differential test runs the same file and compares both transcripts byte for byte.
"""
from array import array
import base64
import json
from pathlib import Path
import re
import struct
import sys
from threading import Thread
import time
import unittest

sys.path.insert(0, str(Path(__file__).parent / "support"))

import plain_corpus  # noqa: E402
import scenario_runner  # noqa: E402
from fake_server import Fake, ok  # noqa: E402

from nabedra import BatchError, Database, NabedraError, ValidationError, col  # noqa: E402

DOCS = Path(__file__).resolve().parents[2] / "python/docs/reference.md"
README = Path(__file__).resolve().parents[2] / "python/README.md"
ROW = {"id": "a", "n": 1, "f": 1.0, "b": True, "vec": [1, 2], "sp": {}, "mu": [], "tags": []}


def row(**over):
    return {**ROW, **over}


class Scenarios(unittest.TestCase):
    """Every scenario meets its expectations: no raw exceptions, exact outcomes, exact request sizes."""

    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.scenarios, cls.runs = scenario_runner.run_all()

    def test_every_scenario_meets_its_expectations(self):
        self.assertGreater(len(self.scenarios), 500)
        for scenario in self.scenarios:
            with self.subTest(scenario["name"]):
                self.assertEqual(scenario_runner.problems(scenario, self.runs[scenario["name"]]), [])

    def test_a_row_is_never_applied_without_proof(self):
        """A row is applied only if a well-formed reply to its own request said so."""
        for scenario in self.scenarios:
            run = self.runs[scenario["name"]]
            error = run["result"].get("error")
            if error is None or error["type"] != "BatchError" or not error["outcomes"]:
                continue
            reply = scenario["script"][min(len(run["requests"]), len(scenario["script"])) - 1]
            well_formed = "applied" in reply or ("data" in reply and '"outcomes":[' in reply["data"]
                                                 and "outcomes\":{" not in reply["data"])
            with self.subTest(scenario["name"]):
                if not well_formed:
                    self.assertEqual([item for item in error["outcomes"] if item[1] == "applied"], [])


class PlainTables(unittest.TestCase):
    def test_every_pre_search_operation_keeps_its_request_bytes(self):
        """Recorded on nabedra/oltp-vector: 160 operations, ten kinds of float, big ints, Unicode."""
        golden = json.loads((Path(__file__).parent / "fixtures/plain_corpus.json").read_text(encoding="utf-8"))
        corpus = plain_corpus.record()
        self.assertEqual(sorted(corpus), sorted(golden))
        for name in golden:
            with self.subTest(name):
                self.assertEqual(corpus[name], golden[name])
        self.assertGreater(len(golden), 150)


class Harness(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fake = Fake()
        cls.db = Database(cls.fake.url, "fixture", timeout=3)
        cls.docs = cls.db.table("docs")
        cls.chunks = cls.db.table("chunks")

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        cls.fake.close()

    def sent(self, index=0):
        return self.fake.requests[index]["request"]["payload"]


class Inputs(Harness):
    def test_values_are_copied_when_the_row_is_checked(self):
        vec, tags, sparse, multi = array("f", [1, 2]), ["x"], {1: 1.0}, [array("f", [1, 2])]

        def rows():
            for number in range(3):
                yield row(id=f"k{number}", vec=vec, tags=tags, sp=sparse, mu=multi)
                vec[0] = 99
                tags.append("y")
                sparse[2] = 2.0
                multi[0][0] = 77
                multi.append(array("f", [0, 0]))

        self.fake.reset({"applied": 3})
        self.assertEqual(self.docs.upsert_many(rows()), 3)
        first, second = self.sent()["rows"][:2]
        self.assertEqual(first["tags"], ["x"])
        self.assertEqual(first["sp"], {"sparse": {"i": "AQAAAA==", "f32": "AACAPw=="}})
        self.assertEqual(first["mu"]["n"], 1)
        self.assertEqual(second["tags"], ["x", "y"])

    def test_a_ranking_copies_its_query_vector(self):
        query = array("f", [1, 0, 0])
        ranking = col("emb").cosine(query)
        query[0] = 0.0
        query[1] = 1.0
        self.fake.reset(ok("[]"))
        self.chunks.query().rank(ranking).limit(1).collect()
        self.assertEqual(self.sent()["rank"]["rankings"][0]["value"], {"f32": "AACAPwAAAAAAAAAA"})

    def test_a_one_shot_iterator_is_consumed_once(self):
        iterator = iter([row(id="a"), row(id="b")])
        self.fake.reset({"applied": 2})
        self.assertEqual(self.docs.upsert_many(iterator), 2)
        self.assertEqual(self.docs.upsert_many(iterator), 0)
        self.assertEqual(len(self.fake.requests), 1)

    def test_a_generator_that_raises_in_the_first_chunk_sends_nothing(self):
        def rows():
            yield row(id="a")
            raise RuntimeError("boom")

        self.fake.reset({"applied": 1})
        with self.assertRaisesRegex(RuntimeError, "boom"):
            self.docs.upsert_many(rows())
        self.assertEqual(self.fake.requests, [])

    def test_a_generator_that_raises_after_a_chunk_reports_the_applied_rows(self):
        def rows():
            for number in range(10_005):
                yield row(id=f"k{number}")
            raise RuntimeError("boom")

        self.fake.reset({"applied": 10_000})
        with self.assertRaises(BatchError) as caught:
            self.docs.upsert_many(rows())
        self.assertEqual((caught.exception.applied, caught.exception.outcomes), (10_000, []))
        self.assertIsInstance(caught.exception.__cause__, RuntimeError)
        self.assertEqual(len(self.fake.requests), 1)

    def test_a_handle_serves_sixteen_threads_with_distinct_request_ids(self):
        self.fake.reset({"applied": 100})
        errors = []

        def work(number):
            try:
                self.assertEqual(self.docs.upsert_many([row(id=f"t{number}-{index}") for index in range(100)]), 100)
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [Thread(target=work, args=(number,)) for number in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        ids = {item["request"]["request_id"] for item in self.fake.requests}
        self.assertEqual(len(ids), 16)
        self.assertEqual({len(item["request"]["payload"]["rows"]) for item in self.fake.requests}, {100})

    def test_a_sparse_reply_of_half_a_million_entries_decodes_quickly(self):
        count = 500_000
        indices = base64.b64encode(struct.pack(f"<{count}I", *range(count))).decode()
        weights = base64.b64encode(struct.pack(f"<{count}f", *([1.0] * count))).decode()
        reply = '[{"id":"a","sp":{"sparse":{"i":"' + indices + '","f32":"' + weights + '"}},"_score":0.5}]'
        self.fake.reset(ok(reply))
        started = time.monotonic()
        hits = (self.chunks.query().rank(col("sp").dot({1: 1.0})).select("id", "sp").limit(1).collect())
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(len(hits[0]["sp"]), count)

    def test_a_float_that_rounds_up_to_the_largest_float32_is_kept(self):
        self.fake.reset({"applied": 1})
        self.docs.upsert_many([row(vec=[3.4028235677973362e38, -3.4028235677973362e38])])
        largest = struct.pack("<ff", 3.4028234663852886e38, -3.4028234663852886e38)
        self.assertEqual(self.sent()["rows"][0]["vec"], {"f32": base64.b64encode(largest).decode()})

    def test_dense_values_equal_their_float32_buffer(self):
        for number in (0.1, 1 / 3, 1e-45, 16777217.0, -0.0, 5e-324):
            with self.subTest(number=number):
                self.fake.reset({"applied": 2})
                self.docs.upsert_many([row(id="a", vec=[number, 0]), row(id="b", vec=array("f", [number, 0]))])
                first, second = self.sent()["rows"]
                self.assertEqual(first["vec"], second["vec"])


class Documentation(Harness):
    """The examples in the README and the reference run against a fake server."""

    @staticmethod
    def blocks(path, heading):
        text = path.read_text(encoding="utf-8")
        section = text.split(heading, 1)[1]
        section = re.split(r"\n## ", section, maxsplit=1)[0]
        return re.findall(r"```python\n(.*?)```", section, flags=re.S)

    def run_block(self, source, names, script):
        self.fake.reset(*script)
        scope = dict(names)
        exec(compile(source.replace("http://127.0.0.1:8000", self.fake.url), "doc", "exec"), scope)  # noqa: S102
        return scope

    def test_the_readme_example_runs(self):
        (source,) = self.blocks(README, "## Vector search")
        rows = '[{"id":"a","acl":["staff"],"body":"reset a password","_score":0.5}]'
        scope = self.run_block(source, {"db": self.db, "col": col}, [{"echo_create": True}, {"applied": 1}, ok(rows)])
        self.assertEqual(scope["hits"], [{"id": "a", "acl": ["staff"], "body": "reset a password", "_score": 0.5}])
        self.assertEqual([item["request"]["operation"] for item in self.fake.requests],
                         ["table.create", "table.upsert_many", "query.rank"])
        rank = self.sent(2)["rank"]
        self.assertEqual([item["op"] for item in rank["rankings"]], ["cosine", "match"])

    def test_the_reference_example_runs(self):
        source = next(block for block in self.blocks(DOCS, "## Vector search") if "create_table" in block)
        rows = '[{"id":"doc7#0","body":"text","_score":0.88}]'
        import os
        os.environ.setdefault("NABEDRA_TOKEN", "fixture")
        scope = self.run_block(source, {}, [{"echo_create": True}, ok(rows)])
        self.assertEqual(scope["hits"], [{"id": "doc7#0", "body": "text", "_score": 0.88}])
        create, ranked = (item["request"] for item in self.fake.requests)
        self.assertEqual(create["payload"]["partition_by"], "tenant")
        self.assertEqual(ranked["payload"]["rank"]["rankings"][0]["value"], "reset my password")
        scope["db"].close()

    def test_every_documented_name_is_importable(self):
        import nabedra
        for name in ("Vector", "BatchError", "SearchUnavailableError", "col"):
            self.assertTrue(hasattr(nabedra, name), name)


class Limits(Harness):
    def test_nothing_over_eight_mebibytes_is_ever_sent(self):
        text = "x" * 1_000_000
        self.fake.reset({"applied": 8}, {"applied": 1})
        rows = [{"id": f"k{number}{text}", "n": 1, "f": 1.0, "b": True} for number in range(9)]
        self.assertEqual(self.db.table("plain").upsert_many(rows), 9)
        self.assertTrue(all(len(item["raw"]) <= 8 * 1024 * 1024 for item in self.fake.requests))
        self.assertEqual([len(item["request"]["payload"]["rows"]) for item in self.fake.requests], [8, 1])

    def test_errors_are_nabedra_errors_never_raw(self):
        for call in (lambda: self.docs.upsert_many([None]), lambda: self.docs.upsert_many([row(vec=object())]),
                     lambda: self.docs.upsert_many([row(sp={1: object()})]),
                     lambda: self.docs.upsert_many([row(mu=[object()])]),
                     lambda: self.docs.upsert_many([row(tags=[1])]),
                     lambda: self.docs.delete_many([object()]),
                     lambda: self.docs.upsert_many([row(vec=memoryview(b"abc"))]),
                     lambda: self.docs.upsert_many([row(vec=memoryview(array("f", [1, 2, 3, 4]))[::2])]),
                     lambda: self.docs.upsert_many([row(n=2 ** 64)]),
                     lambda: self.docs.upsert_many([row(f=float("nan"))])):
            with self.subTest(call=call):
                with self.assertRaises((ValidationError, NabedraError)):
                    call()


if __name__ == "__main__":
    unittest.main()


class KnownDefects(unittest.TestCase):
    """Each test fails until the code is fixed."""

    def test_defect_contains_any_of_a_set_sends_a_hash_seed_dependent_order(self):
        # A set has no order. The request must not depend on PYTHONHASHSEED, or two processes and the
        # TypeScript SDK send different bytes for one query.
        import os
        import subprocess
        code = ("import json\nfrom nabedra import col\n"
                "print(json.dumps(col('tags').contains_any({'alpha', 'beta', 'gamma', 'delta', 'eps'})"
                "._to_wire({'tags': list[str]})))")
        source = str(Path(__file__).resolve().parents[1] / "src")
        outputs = {
            subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True,
                           env={**os.environ, "PYTHONHASHSEED": seed, "PYTHONPATH": source}).stdout
            for seed in ("1", "2", "3")
        }
        self.assertEqual(len(outputs), 1, outputs)
