#!/usr/bin/env python3
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import catalog  # noqa: E402
from test_matrix import commit, git  # noqa: E402

RUST = r'''
fn helper() { let s = "}"; let c = '{'; }

#[test]
fn plain() {
    let v = vec![1, 2];
    assert_eq!(v.len(), 2); // a } in a comment
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs [ci g7] on Linux"]
async fn tokio_ignored() {
    if true { assert!(true); }
}

mod inner {
    #[test]
    pub(crate) fn plain() { assert!(true); }
}

#[cfg(test)]
fn not_a_test() {}
'''

PY = '''
import unittest

def test_module_level():
    assert True

class ATests(unittest.TestCase):
    def setUp(self):
        pass

    def test_one(self):
        self.assertTrue(True)

    async def test_two(self):
        pass
'''

NODE = '''
import test from "node:test";
test("adds numbers", () => { assert.equal(1 + 1, 2); });
test('quotes "inside"', async () => {});
it(`template name`, () => {});
describe("group", () => { it("nested", () => {}); });
'''


class ScanTests(unittest.TestCase):
    def test_rust(self):
        entries = catalog.scan_file("server/src/x.rs", RUST)
        self.assertEqual([e["id"] for e in entries],
                         ["server/src/x.rs::plain", "server/src/x.rs::tokio_ignored", "server/src/x.rs::plain#2"])
        self.assertTrue(all(e["lang"] == "rust" and len(e["hash"]) == 16 for e in entries))
        body = catalog._rust_body(RUST, RUST.index("#[test]"))
        self.assertTrue(body.rstrip().endswith("}"))
        self.assertIn("assert_eq!(v.len(), 2)", body)
        self.assertNotIn("tokio_ignored", body)

    def test_rust_hash_changes_with_body_only(self):
        a = {e["id"]: e["hash"] for e in catalog.scan_file("x.rs", RUST)}
        b = {e["id"]: e["hash"] for e in catalog.scan_file("x.rs", RUST.replace("vec![1, 2]", "vec![1, 2, 3]").replace("len(), 2", "len(), 3"))}
        self.assertNotEqual(a["x.rs::plain"], b["x.rs::plain"])
        self.assertEqual(a["x.rs::tokio_ignored"], b["x.rs::tokio_ignored"])

    def test_python(self):
        ids = [e["id"] for e in catalog.scan_file("orm/python/tests/test_a.py", PY)]
        self.assertEqual(ids, ["orm/python/tests/test_a.py::test_module_level",
                               "orm/python/tests/test_a.py::ATests::test_one",
                               "orm/python/tests/test_a.py::ATests::test_two"])
        self.assertEqual(catalog.scan_file("orm/python/tests/helper.py", PY), [])

    def test_node(self):
        ids = [e["id"].split("::", 1)[1] for e in catalog.scan_file("orm/typescript/tests/a.test.mjs", NODE)]
        self.assertEqual(ids, ["adds numbers", 'quotes "inside"', "template name", "nested"])

    def test_repository_catalog_builds(self):
        built = catalog.dumps(catalog.build())
        # The orchestrator regenerates the committed file after merges, so only its shape is checked.
        self.assertGreater(json.loads(built)["count"], 1000)
        committed = json.loads(catalog.DEFAULT_OUT.read_text(encoding="utf-8"))
        self.assertEqual(committed["schema"], "nabedra.catalog/1")
        self.assertEqual(committed["count"], len(committed["tests"]))


class NewTests(unittest.TestCase):
    def test_new_and_changed(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            git(repo, "init", "-q", "-b", "main")
            base = commit(repo, {"server/src/x.rs": RUST, "orm/python/tests/test_a.py": PY}, "base")
            head = commit(repo, {
                "server/src/x.rs": RUST.replace("assert_eq!(v.len(), 2)", "assert_eq!(v.len(), 2 + 0)")
                + "\n#[test]\nfn added() {}\n",
                "orm/python/tests/test_a.py": PY,
                "orm/typescript/tests/b.test.mjs": NODE,
            }, "head")
            found = {t["id"]: t["status"] for t in catalog.new_tests(base, head, repo)}
            self.assertEqual(found["server/src/x.rs::plain"], "changed")
            self.assertEqual(found["server/src/x.rs::added"], "new")
            self.assertEqual(found["orm/typescript/tests/b.test.mjs::adds numbers"], "new")
            self.assertNotIn("server/src/x.rs::tokio_ignored", found)
            self.assertFalse(any(k.startswith("orm/python") for k in found))


if __name__ == "__main__":
    unittest.main()
