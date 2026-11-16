"""Installer command contract; real clean-prefix build belongs to integration."""
import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('install_nabedra', Path(__file__).resolve().parents[2] / 'scripts' / 'install_nabedra.py')
installer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(installer)


class InstallerTests(unittest.TestCase):
    def test_build_once_locked_local_install_and_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            prefix = Path(temporary) / 'install'
            with patch.object(installer.shutil, 'which', return_value='/tool'), patch.object(installer.venv.EnvBuilder, 'create') as create, patch.object(installer.subprocess, 'run') as run, patch.object(installer.shutil, 'copy2') as copy, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(installer.main(['--prefix', str(prefix)]), 0)
                calls = [call.args[0] for call in run.call_args_list]
                cargo = [call for call in calls if call[0] == 'cargo']
                self.assertEqual(cargo, [['cargo', 'build', '--locked', '--release', '--bin', 'nabedra-db']])
                self.assertEqual(calls[1][1:4], ['-m', 'pip', 'install'])
                self.assertEqual(calls[2][1:], ['--version', '--json'])
                self.assertEqual(calls[2][0], str(prefix / 'bin' / 'nabedra'))
                create.assert_called_once_with(prefix)
                self.assertEqual(copy.call_args.args[1], prefix / 'bin' / 'nabedra-db')

    def test_custom_target_directory_selects_matching_cargo_output(self):
        import os
        for target in ('/tmp/shared-nabedra-target', 'relative-target'):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as temporary:
                prefix = Path(temporary) / 'install'
                with patch.dict(os.environ, {'CARGO_TARGET_DIR': target}), patch.object(installer.shutil, 'which', return_value='/tool'), patch.object(installer.venv.EnvBuilder, 'create'), patch.object(installer.subprocess, 'run'), patch.object(installer.shutil, 'copy2') as copy, contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(installer.main(['--prefix', str(prefix)]), 0)
                    expected = Path(target)
                    if not expected.is_absolute():
                        expected = Path(installer.__file__).resolve().parents[1] / 'server' / expected
                    self.assertEqual(copy.call_args.args[0], expected / 'release' / 'nabedra-db')

    def test_missing_tools_fails_before_environment_creation(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(installer.shutil, 'which', return_value=None), patch.object(installer.venv.EnvBuilder, 'create') as create, contextlib.redirect_stderr(io.StringIO()) as error:
            with self.assertRaises(SystemExit) as failure:
                installer.main(['--prefix', str(Path(temporary) / 'install')])
            self.assertEqual(failure.exception.code, 2)
            self.assertIn('Cargo and rustc', error.getvalue())
            create.assert_not_called()

    def test_existing_prefix_refused(self):
        with tempfile.TemporaryDirectory() as temporary, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as failure:
                installer.main(['--prefix', temporary])
            self.assertEqual(failure.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
