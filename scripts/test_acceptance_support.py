"""Local safety guard; never contacts AWS."""

import tempfile
import unittest
from pathlib import Path

import acceptance_support as support


class ConfigurationGuard(unittest.TestCase):
    def test_dotenv_and_namespace_stay_inside_configured_bucket(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "must-not-exist"
            dotenv = root / ".env"
            dotenv.write_text(
                "# Assignment forms only\n"
                "export AWS_REGION = us-east-1 # comment\n"
                "AWS_ACCESS_KEY_ID='fake-key'\n"
                f'AWS_SECRET_ACCESS_KEY="$(touch {marker})${{HOME}}`id`"\n'
                "BUCKET_NAME=meshdb-test\n"
                "MESHDB_S3_BUCKET=wrong-bucket\n",
                encoding="utf-8",
            )
            config = support.load_config(dotenv)
            namespace = config.allocate_prefix(1, "safe-config", run_id="guard01")
            self.assertEqual(namespace.prefix, "test-1-safe-config-guard01/")
            self.assertEqual(namespace.key("fixtures/rows.parquet"), namespace.prefix + "fixtures/rows.parquet")
            env = config.server_env(namespace)
            self.assertEqual(env["MESHDB_S3_BUCKET"], "meshdb-test")
            self.assertEqual(env["MESHDB_S3_PREFIX"], namespace.prefix)
            self.assertEqual(env["AWS_SECRET_ACCESS_KEY"], f"$(touch {marker})${{HOME}}`id`")
            self.assertEqual(env["MESHDB_TOKEN"], config.server_env(namespace)["MESHDB_TOKEN"])
            self.assertNotIn("fake-key", repr(config) + repr(env) + repr(namespace))
            self.assertFalse(marker.exists())
            with self.assertRaises(TypeError):
                config.allocate_prefix(1, "safe", bucket="wrong-bucket")
            for unsafe_prefix in ("", "meshdb-v0/", "../escape/"):
                with self.subTest(prefix=unsafe_prefix), self.assertRaises(support.ConfigError):
                    config.server_env(support.TestNamespace(unsafe_prefix, config))
            for unsafe in ("", "../escape", "/root", "a//b", "a/../b", "a\\b", "%2e%2e/x"):
                with self.subTest(unsafe=unsafe), self.assertRaises(support.ConfigError):
                    namespace.key(unsafe)
            for unsafe in ("../escape", "", "meshdb-v0/", "a/b", "a b"):
                with self.subTest(segment=unsafe), self.assertRaises(support.ConfigError):
                    config.allocate_prefix(1, unsafe)
            with self.assertRaises(support.ConfigError):
                config.allocate_prefix(1, "safe", run_id="../escape")
            dotenv.write_text("source /tmp/anything\n", encoding="utf-8")
            with self.assertRaises(support.ConfigError) as failure:
                support.load_config(dotenv)
            self.assertNotIn("/tmp/anything", str(failure.exception))


class CatalogShapeGuard(unittest.TestCase):
    def test_current_references_across_catalog_versions(self):
        from acceptance_concurrency import _catalog_references
        from acceptance_http import CheckFailure
        table_id = '00000000-0000-0000-0000-000000000001'
        commit = '00000000-0000-0000-0000-000000000002'
        snapshot = f'tables/{table_id}/{commit}.parquet'
        partition = f'tables/{table_id}/{commit}/0.parquet'
        config = support.AcceptanceConfig('meshdb-testing', 'us-east-1', {})
        namespace = config.allocate_prefix(10, 'partition-workers', run_id='local')
        base = dict(id=table_id, key='id', schema={'id': 'int64'}, format='parquet', row_count=1)
        file = dict(object=partition, row_count=1, byte_size=100, payload_bytes=12,
                    range=dict(lower=None, upper=None))
        shapes = [(1, dict(base, snapshot=snapshot), [snapshot]),
                  (2, dict(base, data=dict(layout='legacy', snapshot=snapshot)), [snapshot]),
                  (2, dict(base, data=dict(layout='partitioned', files=[file])), [partition]),
                  (2, dict(base, row_count=0, data=dict(layout='partitioned', files=[])), [])]
        for version, entry, refs in shapes:
            with self.subTest(version=version, refs=refs):
                catalog = dict(format_version=version, revision=1, tables=dict(rows=entry))
                expected = dict(rows=(table_id, entry['row_count']))
                self.assertEqual(_catalog_references(catalog, expected,
                    {snapshot: 100, partition: 100}, namespace), dict(rows=refs))
                if refs:
                    with self.assertRaises(CheckFailure):
                        _catalog_references(catalog, expected, {}, namespace)
        for size in [67_108_864, 67_108_865]:
            entry = dict(base, data=dict(layout='partitioned', files=[dict(file, byte_size=size)]))
            catalog = dict(format_version=2, revision=1, tables=dict(rows=entry))
            if size == 67_108_864:
                self.assertEqual(_catalog_references(catalog, dict(rows=(table_id, 1)),
                    {partition: size}, namespace), dict(rows=[partition]))
            else:
                with self.assertRaises(CheckFailure):
                    _catalog_references(catalog, dict(rows=(table_id, 1)),
                                        {partition: size}, namespace)
        bad = dict(file, byte_size=99)
        catalog = dict(format_version=2, revision=1,
                       tables=dict(rows=dict(base, data=dict(layout='partitioned', files=[bad]))))
        with self.assertRaises(CheckFailure):
            _catalog_references(catalog, dict(rows=(table_id, 1)), {partition: 100}, namespace)
        for boundaries in [(None, 50, 20, None), (None, '50', None),
                           (None, True, None), (None, 2**63, None)]:
            files = [dict(file, object=f'tables/{table_id}/{commit}/{index}.parquet',
                          range=dict(lower=lower, upper=upper))
                     for index, (lower, upper) in enumerate(zip(boundaries, boundaries[1:]))]
            catalog = dict(format_version=2, revision=1,
                           tables=dict(rows=dict(base, row_count=len(files),
                               data=dict(layout='partitioned', files=files))))
            with self.subTest(boundaries=boundaries), self.assertRaises(CheckFailure):
                _catalog_references(catalog, dict(rows=(table_id, len(files))),
                                    {item['object']: 100 for item in files}, namespace)


if __name__ == "__main__":
    unittest.main()
