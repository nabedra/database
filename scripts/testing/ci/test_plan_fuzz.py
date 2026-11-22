#!/usr/bin/env python3

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from plan_fuzz import fuzz_matrix


class FuzzMatrixTests(unittest.TestCase):
    def test_absent_fuzz_workspace_is_not_run(self):
        with TemporaryDirectory() as folder:
            self.assertEqual(fuzz_matrix(Path(folder) / "fuzz"), [])

    def test_target_features_keep_incompatible_engines_separate(self):
        with TemporaryDirectory() as folder:
            directory = Path(folder)
            (directory / "fuzz_targets").mkdir()
            (directory / "fuzz_targets/cache.rs").touch()
            (directory / "fuzz_targets/olap.rs").touch()
            (directory / "Cargo.toml").write_text(
                '[[bin]]\nname = "cache_target"\npath = "fuzz_targets/cache.rs"\n'
                'required-features = ["libfuzzer", "cache"]\n'
                '[[bin]]\nname = "olap"\npath = "fuzz_targets/olap.rs"\n'
                'required-features = ["libfuzzer", "olap"]\n'
            )
            self.assertEqual(fuzz_matrix(directory), [
                {"target": "cache_target", "features": "libfuzzer,cache"},
                {"target": "olap", "features": "libfuzzer,olap"},
            ])
            (directory / "fuzz_targets/undeclared.rs").touch()
            with self.assertRaisesRegex(ValueError, "has no"):
                fuzz_matrix(directory)


if __name__ == "__main__":
    unittest.main()
