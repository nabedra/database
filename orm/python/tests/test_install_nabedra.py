"""Installer command contract; real clean-prefix build belongs to integration."""
import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

SPEC = importlib.util.spec_from_file_location('install_nabedra', Path(__file__).resolve().parents[3] / 'scripts' / 'install_nabedra.py')
installer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(installer)


class InstallerTests(unittest.TestCase):
    def test_build_once_locked_local_install_and_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            prefix = Path(temporary) / 'install'
            with patch.object(installer.shutil, 'which', return_value='/tool'), patch.object(installer.venv.EnvBuilder, 'create') as create, patch.object(installer.subprocess, 'run', return_value=SimpleNamespace(stdout='nabedra-db 0.1.1 olap')) as run, patch.object(installer.shutil, 'copy2') as copy, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(installer.main(['--prefix', str(prefix)]), 0)
                calls = [call.args[0] for call in run.call_args_list]
                cargo = [call for call in calls if call[0] == 'cargo']
                self.assertEqual(cargo, [['cargo', 'build', '--locked', '--release', '--bin', 'nabedra-db', '--no-default-features', '--features', 'olap,s3']])
                self.assertEqual(calls[2][1:4], ['-m', 'pip', 'install'])
                self.assertEqual(calls[2][4], str(Path(installer.__file__).resolve().parents[1] / 'orm' / 'python'))
                self.assertEqual(calls[1][1:], ['--version'])
                self.assertEqual(calls[3][1:], ['--version', '--json'])
                self.assertEqual(calls[3][0], str(prefix / 'bin' / 'nabedra'))
                create.assert_called_once_with(prefix)
                self.assertEqual(copy.call_args.args[1], prefix / 'bin' / 'nabedra-db')

    def test_custom_target_directory_selects_matching_cargo_output(self):
        import os
        for target in ('/tmp/shared-nabedra-target', 'relative-target'):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as temporary:
                prefix = Path(temporary) / 'install'
                with patch.dict(os.environ, {'CARGO_TARGET_DIR': target}), patch.object(installer.shutil, 'which', return_value='/tool'), patch.object(installer.venv.EnvBuilder, 'create'), patch.object(installer.subprocess, 'run', return_value=SimpleNamespace(stdout='nabedra-db 0.1.1 olap')), patch.object(installer.shutil, 'copy2') as copy, contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(installer.main(['--prefix', str(prefix)]), 0)
                    expected = Path(target)
                    if not expected.is_absolute():
                        expected = Path(installer.__file__).resolve().parents[1] / 'server' / expected
                    self.assertEqual(copy.call_args.args[0], expected / 'release' / 'nabedra-db')

    def test_engine_selection_and_mismatched_artifact(self):
        for with_s3 in (False, True):
            with self.subTest(with_s3=with_s3), tempfile.TemporaryDirectory() as temporary:
                prefix = Path(temporary) / 'install'
                args = ['--prefix', str(prefix), '--engine', 'oltp']
                if with_s3:
                    args.append('--with-s3')
                with patch.object(installer.shutil, 'which', return_value='/tool'), patch.object(installer.venv.EnvBuilder, 'create'), patch.object(installer.subprocess, 'run', return_value=SimpleNamespace(stdout='nabedra-db 0.1.1 oltp')) as run, patch.object(installer.shutil, 'copy2'), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(installer.main(args), 0)
                    self.assertEqual(run.call_args_list[0].args[0][-1], 'oltp-backup-s3' if with_s3 else 'oltp')
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(installer.shutil, 'which', return_value='/tool'), patch.object(installer.venv.EnvBuilder, 'create') as create, patch.object(installer.subprocess, 'run', return_value=SimpleNamespace(stdout='nabedra-db 0.1.1 olap')), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(installer.main(['--prefix', str(Path(temporary) / 'install'), '--engine', 'oltp']), 1)
                create.assert_not_called()

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
