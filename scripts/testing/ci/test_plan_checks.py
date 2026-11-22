#!/usr/bin/env python3

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from plan_checks import check_matrix


class CheckMatrixTests(unittest.TestCase):
    def test_deduplicates_ids_and_preserves_requirements(self):
        with TemporaryDirectory() as folder:
            directory = Path(folder)
            (directory / "s3-check.toml").write_text('id = "s3-check"\nneeds = ["s3"]\n')
            (directory / "test-one.toml").write_text('id = "test-one"\nneeds = []\ntimeout_s = 900\n')
            (directory / "long-one.toml").write_text('id = "long-one"\ntimeout_s = 86400\n')
            self.assertEqual(check_matrix(" test-one, s3-check,test-one,long-one ", directory), [
                {"id": "test-one", "tools": [], "timeout_minutes": 45},
                {"id": "s3-check", "tools": ["s3"], "timeout_minutes": 90},
                {"id": "long-one", "tools": [], "timeout_minutes": 360},
            ])
            self.assertEqual(check_matrix(" ", directory), [])

    def test_rejects_unregistered_ids_and_invalid_manifests(self):
        with TemporaryDirectory() as folder:
            directory = Path(folder)
            (directory / "wrong-id.toml").write_text('id = "different"\n')
            (directory / "wrong-tool.toml").write_text('id = "wrong-tool"\nneeds = ["sudo"]\n')
            (directory / "wrong-timeout.toml").write_text('id = "wrong-timeout"\ntimeout_s = "1h"\n')
            for request, error in (
                ("unknown", "unknown check"),
                ("../outside", "invalid check ID"),
                ("wrong-id", "id must match"),
                ("wrong-tool", "unsupported needs"),
                ("wrong-timeout", "timeout_s must be"),
            ):
                with self.subTest(request=request), self.assertRaisesRegex(ValueError, error):
                    check_matrix(request, directory)


if __name__ == "__main__":
    unittest.main()
