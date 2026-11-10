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


if __name__ == "__main__":
    unittest.main()
