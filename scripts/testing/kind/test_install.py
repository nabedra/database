#!/usr/bin/env python3

import io
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from . import install


FIXTURE = Path(__file__).with_name("fixtures") / "kind-binary"
DIGEST = "0c4763866eb987178a63a54498d0c6a8b171d33a22aa925c9f8d2c360ead11ec"


class InstallerTests(unittest.TestCase):
    def test_digest_checks_file_contents(self):
        install.verify_checksum(FIXTURE, DIGEST)
        with TemporaryDirectory() as directory:
            modified = Path(directory) / "binary"
            modified.write_bytes(FIXTURE.read_bytes() + b"tampered")
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                install.verify_checksum(modified, DIGEST)

    def test_install_selects_release_asset_and_checks_before_replacing(self):
        with (TemporaryDirectory() as directory,
              patch.object(install.platform, "system", return_value="Darwin"),
              patch.object(install.platform, "machine", return_value="arm64"),
              patch.dict(install.SHA256, {"darwin-arm64": DIGEST}),
              patch.object(install, "urlopen") as download):
            target = Path(directory) / "bin" / "kind"
            download.side_effect = [io.BytesIO(FIXTURE.read_bytes()), io.BytesIO(b"wrong")]
            install.install(target)
            download.assert_called_once_with(f"{install.RELEASE}/kind-darwin-arm64", timeout=30)
            self.assertTrue(target.stat().st_mode & 0o111)
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                install.install(target)
            self.assertEqual(target.read_bytes(), FIXTURE.read_bytes())
            self.assertEqual(list(target.parent.iterdir()), [target])
