"""Focused checks for private serve declaration and config-path validation."""
import functools
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from nabedra import _serve_args
from nabedra.errors import ValidationError


class ServeArgsTests(unittest.TestCase):
    def test_policy_normalization_and_callback_identity_without_invocation(self):
        class Callback:
            def __call__(self, *args):
                raise AssertionError("validation must not invoke callbacks")

            def __repr__(self):
                raise AssertionError("validation must not render callbacks")

        callbacks = (Callback(), Callback(), Callback())
        for nodes, mode, desired, low, high in (
            (0, "fixed", 0, 0, 0), (1, "fixed", 1, 1, 1),
            (1024, "fixed", 1024, 1024, 1024),
            ((1, 1024), "autoscale", 1, 1, 1024),
            ((1024, 1024), "autoscale", 1024, 1024, 1024),
        ):
            result = _serve_args._validate_serve_args("_cluster9", nodes, *callbacks)
            self.assertEqual(result["name"], "_cluster9")
            self.assertEqual(result["provisioning"], "managed")
            self.assertEqual(result["initial_policy"], dict(
                mode=mode, desired=desired, min_nodes=low, max_nodes=high))
            for actual, original in zip(result["callbacks"], callbacks):
                self.assertIs(actual, original)
        self.assertEqual(_serve_args._validate_serve_args("a" * 64)["initial_policy"],
                         dict(mode="fixed", desired=1, min_nodes=1, max_nodes=1))
        for nodes in (0, 1024):
            result = _serve_args._validate_serve_args("cluster", nodes)
            self.assertEqual(result["provisioning"], "external")
            self.assertEqual(result["callbacks"], (None, None, None))
        # Native callables need not expose inspectable Python signatures.
        _serve_args._validate_serve_args("cluster", 1, dict, iter, len)

    def test_invalid_names_counts_and_callback_combinations(self):
        class IntSubclass(int):
            pass

        class TupleSubclass(tuple):
            pass

        def callback(*args):
            raise AssertionError("callback entered")

        async def async_callback(*args):
            raise AssertionError("async callback entered")

        class AsyncCallback:
            async def __call__(self, *args):
                raise AssertionError("async callable entered")

        for name in (None, True, "", "bad-name", "1cluster", "a" * 65, "é"):
            with self.subTest(name=name), self.assertRaises(ValidationError):
                _serve_args._validate_serve_args(name)
        for nodes in (True, False, -1, 1025, 1.0, "1", None, IntSubclass(1),
                      [], [1, 2], (), (1,), (1, 2, 3), (0, 1), (2, 1),
                      (1, 1025), (True, 2), (1, False), (1.0, 2),
                      (1, IntSubclass(2)), TupleSubclass((1, 2))):
            with self.subTest(nodes=nodes), self.assertRaises(ValidationError):
                _serve_args._validate_serve_args("cluster", nodes, callback, callback, callback)
        with self.assertRaises(ValidationError):
            _serve_args._validate_serve_args("cluster", (1, 2))
        for invalid in (None, 0, async_callback, AsyncCallback(),
                        functools.partial(async_callback),
                        functools.partial(AsyncCallback())):
            for position in range(3):
                callbacks = [callback] * 3
                callbacks[position] = invalid
                with self.subTest(position=position), self.assertRaises(ValidationError):
                    _serve_args._validate_serve_args("cluster", 1, *callbacks)
        for position in range(3):
            callbacks = [None] * 3
            callbacks[position] = callback
            with self.assertRaises(ValidationError):
                _serve_args._validate_serve_args("cluster", 1, *callbacks)

    def test_explicit_literal_config_paths_and_sanitized_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config with spaces.json"
            config.write_text("invalid JSON is Rust's responsibility")
            with patch.dict(os.environ, {"NABEDRA_CONFIG": str(config)}):
                self.assertEqual(_serve_args._resolve_config_path(), str(config))
            with patch("os.getcwd", return_value=directory):
                for name in (config.name, "$NABEDRA_LITERAL", "~", "$(literal)"):
                    (root / name).touch()
                    with patch.dict(os.environ, {"NABEDRA_CONFIG": name}):
                        self.assertEqual(_serve_args._resolve_config_path(), str(root / name))
            failures = []
            for value in (None, "", str(root), str(root / "private-missing"), "private\0path"):
                # The OS forbids putting NUL in a real environment value.
                environment = {} if value is None else {"NABEDRA_CONFIG": value}
                with patch("os.environ", environment):
                    with self.assertRaises(ValidationError) as caught:
                        _serve_args._resolve_config_path()
                failures.append(str(caught.exception))
                self.assertIsNone(caught.exception.__cause__)
                self.assertIsNone(caught.exception.__context__)
            config.chmod(0)
            try:
                with patch.dict(os.environ, {"NABEDRA_CONFIG": str(config)}):
                    if os.access(config, os.R_OK):
                        # Root can read chmod(0); emulate denial only there.
                        with patch("os.access", return_value=False), \
                             self.assertRaises(ValidationError) as caught:
                            _serve_args._resolve_config_path()
                    else:
                        with self.assertRaises(ValidationError) as caught:
                            _serve_args._resolve_config_path()
                failures.append(str(caught.exception))
            finally:
                config.chmod(0o600)
            self.assertEqual(len(set(failures)), 1)
            self.assertIn("NABEDRA_CONFIG", failures[0])
            self.assertNotIn(directory, failures[0])
            self.assertNotIn("private", failures[0])


if __name__ == "__main__":
    unittest.main()
