import contextlib
import io
from pathlib import Path
import tempfile
import unittest

import fs_sites


class FilesystemSitesTest(unittest.TestCase):
    def test_direct_std_fs_including_import_aliases(self):
        source = """\
std :: fs /* comment */ ::read(path);
use std::{io, fs::{File as NativeFile, self as native}};
use {std::fs as disk};
use std::{os::unix::fs::FileExt, path::Path};
"""
        self.assertEqual(
            [site.line for site in fs_sites.sites(source) if site.kind == "direct std::fs"],
            [1, 2, 3],
        )

    def test_comments_literals_and_test_items_are_skipped(self):
        source = """\
// use std::fs;
 /* outer /* std::fs::remove_file(path) */ comment */
let text = r#"std::fs::File::open(path)"#;
#[cfg(test)]
mod tests {
    use std::fs;
    fn nested() { fs::read(path); }
}
#[cfg(test)]
use std::fs;
use nabedra_rt::fs::os as fs;
fs::read(path);
"""
        self.assertEqual(list(fs_sites.sites(source)), [fs_sites.Site(12, "call", "fs::read")])

    def test_guard_covers_current_and_extracted_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            examples = {
                "server/src/oltp/pool/snapshot.rs": "fn f() { std::fs::remove_file(path); }",
                "server/src/oltp/pool/resources.rs": "fn f() { std::fs::remove_file(path); }",
                "server/src/oltp/pool/sim_tests.rs": "fn f() { std::fs::remove_file(path); }",
                "server/src/oltp_replication/manager/local/restore.rs": "use std::fs;",
                "server/oltp-storage/src/oltp/pool.rs": "fn f() { path.try_exists(); }",
                "server/oltp-replication/src/oltp_replication/manager/local/registered/new.rs":
                    "use std::{fs};",
            }
            for name, source in examples.items():
                file = root / name
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(source, encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(fs_sites.main(["--check"], root), 1)
            failures = [line for line in output.getvalue().splitlines() if ": FAIL:" in line]
            self.assertEqual(len(failures), 3)
            for name in ("snapshot.rs", "pool.rs", "new.rs"):
                self.assertTrue(any(f"/{name}:" in line for line in failures))

            for name in examples:
                if any(f"{name}:" in line for line in failures):
                    (root / name).write_text("use nabedra_rt::fs::os as fs;", encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(fs_sites.main(["--check"], root), 0)


if __name__ == "__main__":
    unittest.main()
