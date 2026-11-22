"""Exercise the manifest CLI against the repository and isolated Python changes."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path("scripts/testing/api_manifest.py")

# Reuse the TypeScript baseline for fixtures that change only Python. The
# repository check below compiles the real TypeScript declarations.
RUN_PYTHON_FIXTURE = """
import json
import sys
from unittest.mock import patch
import api_manifest

manifest = json.loads((api_manifest.ROOT / "orm/api-manifest.json").read_text(encoding="utf-8"))
with patch.object(api_manifest, "typescript_symbols", return_value=manifest["typescript"]):
    sys.exit(api_manifest.main(sys.argv[1:]))
"""


class ApiManifestTests(unittest.TestCase):
    def run_manifest(self, root, command):
        if root == ROOT:
            return subprocess.run([sys.executable, str(root / SCRIPT), command],
                                  capture_output=True, text=True, timeout=120)
        return subprocess.run([sys.executable, "-c", RUN_PYTHON_FIXTURE, command],
                              cwd=root / SCRIPT.parent, capture_output=True, text=True, timeout=15)

    def fixture(self):
        temp = tempfile.TemporaryDirectory(prefix="nabedra-api-fixture-")
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        (root / SCRIPT.parent).mkdir(parents=True)
        shutil.copyfile(ROOT / SCRIPT, root / SCRIPT)
        shutil.copytree(ROOT / "orm/python/src", root / "orm/python/src",
                        ignore=shutil.ignore_patterns("__pycache__"))
        (root / "orm/typescript").mkdir()
        for path in ("python/pyproject.toml", "typescript/package.json", "api-manifest.json"):
            shutil.copyfile(ROOT / "orm" / path, root / "orm" / path)
        return root

    def test_committed_manifest_matches(self):
        result = self.run_manifest(ROOT, "check")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("API manifest matches. ORM versions: ", result.stdout)

    def test_rejects_planted_python_signature_drift(self):
        root = self.fixture()
        query = root / "orm/python/src/nabedra/query.py"
        source = query.read_text(encoding="utf-8")
        default = "desc: bool = False"
        self.assertEqual(source.count(default), 1)
        query.write_text(source.replace(default, "desc: bool = True"), encoding="utf-8")
        result = self.run_manifest(root, "check")
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIn('"symbol": "Query.orderBy"', result.stderr)
        self.assertIn('"default": "False"', result.stderr)
        self.assertIn('"default": "True"', result.stderr)

    def test_rejects_new_gap_in_parity_report(self):
        root = self.fixture()
        query = root / "orm/python/src/nabedra/query.py"
        with query.open("a", encoding="utf-8") as file:
            file.write('\n    def fixture_only(self) -> None:\n        pass\n')
        result = self.run_manifest(root, "parity")
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIn("python only: nabedra.query:Query.fixtureOnly (unallowed)", result.stderr)
        self.assertIn("typescript only: nabedra.events:PublishOptions", result.stdout)

    def test_rejects_unsynced_versions(self):
        root = self.fixture()
        metadata = root / "orm/python/pyproject.toml"
        source = metadata.read_text(encoding="utf-8")
        version_start = source.index('version = "') + len('version = "')
        version_end = source.index('"', version_start)
        metadata.write_text(source[:version_start] + "999.0.0" + source[version_end:], encoding="utf-8")
        result = self.run_manifest(root, "check")
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIn("ORM versions differ: Python 999.0.0, TypeScript ", result.stderr)


if __name__ == "__main__":
    unittest.main()
