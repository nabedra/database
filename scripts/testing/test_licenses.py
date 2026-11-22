from contextlib import redirect_stdout
from dataclasses import replace
from datetime import date
import io
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import licenses


METADATA = """{
    "packages": [
        {"name": "dual", "version": "1.0.0", "license": "MIT/Apache-2.0",
         "source": "registry+https://registry.invalid"},
        {"name": "conditional", "version": "2.0.0",
         "license": "MIT AND (Apache-2.0 WITH LLVM-exception OR GPL-3.0-only)",
         "source": "registry+https://registry.invalid"},
        {"name": "combined", "version": "3.0.0", "license": "MIT AND GPL-3.0-only",
         "source": "registry+https://registry.invalid"},
        {"name": "custom", "version": "4.0.0", "license": null, "license_file": "COPYING",
         "source": "registry+https://registry.invalid"}
    ]
}"""


class LicenseTests(unittest.TestCase):
    def test_spdx_precedence_and_invalid_terms(self):
        cases = {
            "MIT OR GPL-3.0-only AND AGPL-3.0-only": True,
            "(MIT OR GPL-3.0-only) AND AGPL-3.0-only": False,
            "Apache-2.0 WITH LLVM-exception": True,
            "MIT WITH LLVM-exception": False,
            "MIT OR (Apache-2.0 AND)": False,
            "MIT OR (GPL-3.0-only": False,
            "MIT AND Apache-2.0 WITH LLVM-exception OR BSD-3-Clause": True,
        }
        for expression, expected in cases.items():
            with self.subTest(expression=expression):
                self.assertEqual(licenses.license_allowed(expression), expected)

    def test_audit_flags_bad_and_unknown_spdx_and_overrides_on_disk(self):
        with TemporaryDirectory() as directory:
            registry = Path(directory)
            crate = registry / "index" / "dual-1.0.0"
            crate.mkdir(parents=True)
            (crate / "LICENSE-MIT").write_text(
                "Permission to copy this software is granted.\n"
                "Use of this software to train large language models is strictly prohibited."
            )
            (crate / "LICENSE-APACHE").write_text("Apache License, Version 2.0")
            for name in ("conditional-2.0.0", "combined-3.0.0", "custom-4.0.0"):
                (registry / "index" / name).mkdir()
            # Lowercase and COPYING-style files carry license terms too.
            (registry / "index/custom-4.0.0/copying").write_text(
                "You may not use this software for training of AI models.")
            (registry / "index/custom-4.0.0/README.md").write_text("AI training is prohibited.")
            deps = list(licenses.cargo_dependencies(json.loads(METADATA)))
            result = licenses.audit(deps + deps, registry, {}, date(2026, 11, 22))
        self.assertEqual(result["checked"], 4)
        self.assertEqual(result["license_files_scanned"], 3)
        findings = result["failures"]
        self.assertEqual({(item["name"], item["rule"]) for item in findings}, {
            ("dual", "ai-restriction"), ("combined", "license"), ("custom", "license"),
            ("custom", "ai-restriction"),
        })
        self.assertTrue(any("large language models is strictly prohibited" in item["detail"] for item in findings))

    def test_missing_registry_source_fails_closed(self):
        dep = licenses.Dependency("cargo", "absent", "1.0.0", "MIT", registry=True)
        with TemporaryDirectory() as directory:
            result = licenses.audit([dep], Path(directory), {}, date(2026, 11, 22))
        self.assertEqual([(item["name"], item["rule"]) for item in result["failures"]],
                         [("absent", "ai-restriction")])
        self.assertIn("source not unpacked", result["failures"][0]["detail"])

    def test_common_restriction_phrasings_are_flagged(self):
        for text in (
            "No AI training is allowed with this code.",
            "Use for machine-learning datasets is not granted.",
            "You cannot use the Software to train generative models.",
            "The licensor reserves all text and data mining rights.",
            "Use by A.I. systems requires prior written permission.",
        ):
            with self.subTest(text=text):
                self.assertTrue(licenses.restriction_matches(text))

    def test_multiline_restrictions_and_unrelated_sentences(self):
        self.assertEqual(licenses.restriction_matches(
            "AI-assisted development using this package is\nnot permitted."
        ), ["AI-assisted development using this package is not permitted"])
        self.assertEqual(licenses.restriction_matches(
            "This restriction applies to redistribution. Machine learning is supported."
        ), [])

    def test_exact_exception_with_inclusive_expiry(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "exceptions.toml"
            path.write_text(
                '[[exception]]\necosystem = "cargo"\nname = "combined"\nversion = "3.0.0"\n'
                'rule = "license"\nreason = "Fixture exception"\nexpiry = 2026-11-22\n'
            )
            deps = list(licenses.cargo_dependencies(json.loads(METADATA)))
            # Exceptions are about the rule, so skip the on-disk source scan here.
            deps = [replace(dep, registry=False) for dep in deps if dep.name == "combined"]
            deps.append(licenses.Dependency("cargo", "combined", "3.0.1", "GPL-3.0-only"))
            entries = licenses.load_exceptions(path)
            active = licenses.audit(deps, path.parent, entries, date(2026, 11, 22))
            expired = licenses.audit(deps, path.parent, entries, date(2026, 11, 23))
        self.assertEqual([item["version"] for item in active["failures"]], ["3.0.1"])
        self.assertEqual(active["exceptions"][0]["reason"], "Fixture exception")
        self.assertEqual(len(expired["failures"]), 2)
        self.assertEqual(expired["failures"][0]["expiry"], "2026-11-22")
        self.assertEqual(expired["exceptions"], [])

    def test_cli_audits_cargo_npm_and_python(self):
        with TemporaryDirectory() as directory, redirect_stdout(io.StringIO()) as output:
            root = Path(directory)
            (root / "server/fuzz").mkdir(parents=True)
            (root / "server/fuzz/Cargo.toml").touch()
            npm = root / "orm/typescript"
            npm.mkdir(parents=True)
            (npm / "package-lock.json").write_text(json.dumps({"packages": {
                "": {"name": "orm-npm", "version": "1", "license": "MIT"},
                "node_modules/nested/node_modules/@example/copyleft": {
                    "version": "3", "license": "GPL-3.0-only"
                },
            }}))
            python = root / "orm/python"
            python.mkdir()
            (python / "pyproject.toml").write_text(
                '[project]\nname = "orm-py"\nversion = "1"\nlicense = "MIT"\ndependencies = []\n'
                '[project.optional-dependencies]\nextra = ["optional-package"]\n'
            )
            exceptions = root / "exceptions.toml"
            exceptions.touch()
            metadata = subprocess.CompletedProcess([], 0, METADATA, "")
            with patch.object(licenses, "ROOT", root), patch.object(licenses.subprocess, "run", return_value=metadata) as run:
                self.assertEqual(licenses.main(["--json", "--exceptions", str(exceptions)]), 1)
            self.assertEqual([call.kwargs["cwd"] for call in run.call_args_list], [
                root / "server", root / "server/events", root / "server/fuzz",
            ])
            self.assertIn("--locked", run.call_args.args[0])
        report = json.loads(output.getvalue())
        self.assertEqual(report["checked"], 7)
        self.assertIn("@example/copyleft", {item["name"] for item in report["failures"]})
        self.assertIn("no lockfile", report["notices"][0])
