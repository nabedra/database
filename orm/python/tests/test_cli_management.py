"""Focused CLI01/CLI02 checks; runtime acceptance is separate."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from nabedra import cli
from nabedra._binary import resolve_server_binary
from nabedra._cli_config import ConfigError, load, protected_write, validate


class ManagementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = self.root / 'nabedra-database.json'

    def run_cli(self, *args):
        with contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()) as err:
            status = cli.main(['database', *args])
        return status, out.getvalue(), err.getvalue()

    def init(self):
        return self.run_cli('init', '--mode', 'serverless', '--control-storage', 's3://test-bucket/prod/', '--region', 'eu-west-1', '--output', str(self.config), '--non-interactive')

    def test_init_secrets_and_overwrite(self):
        status, out, err = self.init()
        self.assertEqual((status, err), (0, ''))
        config = load(self.config)
        self.assertEqual((config['version'], config['mode']), (2, 'serverless'))
        auth = Path(config['auth_file'])
        self.assertEqual(auth.stat().st_mode & 0o777, 0o600)
        tokens = json.loads(auth.read_text())
        self.assertEqual(len(set(tokens.values())), 3)
        for token in tokens.values():
            self.assertNotIn(token, out)
        before = auth.read_bytes(), self.config.read_bytes()
        self.assertEqual(self.init()[0], 2)
        self.assertEqual(before, (auth.read_bytes(), self.config.read_bytes()))

    def test_missing_and_conflicting_settings(self):
        for args in (('init', '--non-interactive'), ('init', '--mode', 'fixed', '--control-storage', 's3://test-bucket/db/', '--region', 'eu-west-1')):
            self.assertEqual(self.run_cli(*args)[0], 2)
        self.assertFalse(self.config.exists())

    def test_strict_config_tls_and_v1(self):
        self.init()
        original = load(self.config)
        for mutate in (lambda c: c.update(extra=1), lambda c: c.update(mode='bad'), lambda c: c.update(bind='0.0.0.0:8000'), lambda c: c['worker'].update(extra=1)):
            config = copy.deepcopy(original)
            mutate(config)
            with self.assertRaises(ValueError):
                validate(config)
        self.config.write_text('{"version":2,"version":2}')
        with self.assertRaises(ConfigError):
            load(self.config)
        original['version'] = 1
        del original['mode']
        validate(original)

    def test_local_oltp_config_and_public_cli_composition(self):
        auth = self.root / 'auth.json'
        protected_write(auth, dict(admin_token='A' * 43, data_token='B' * 43,
                                  enrollment_token='C' * 43))
        config = dict(version=1, engine='oltp', bind='127.0.0.1:0', database='application',
                      auth_file='auth.json', topology=dict(mode='local', data_dir='volume'))
        protected_write(self.config, config)
        validate(config)
        with patch('nabedra.cli.resolve_server_binary', return_value='/binary'), \
             patch('nabedra._cli_supervise.supervise', return_value=0) as supervise:
            self.assertEqual(self.run_cli('serve', '--config', str(self.config),
                                         '--threads-per-worker', '8')[0], 0)
            actual = supervise.call_args.args[1]
            self.assertEqual(actual['topology']['data_dir'], str(self.root / 'volume'))
            self.assertEqual(actual['auth_file'], str(auth))
            self.assertEqual(actual['worker']['resources']['max_threads'], 8)
            self.assertNotIn('partition_slots', actual['worker'])
            self.assertNotIn('storage', actual)
        for mutate in (lambda c: c['topology'].update(mode='remote'),
                       lambda c: c.update(bind='0.0.0.0:8000'),
                       lambda c: c.update(storage={}),
                       lambda c: c.update(worker={'resources': dict(max_threads=0,
                           memory_limit_bytes=8 << 30, task_memory_limit_bytes=512 << 20,
                           control_memory_reserve_bytes=256 << 20)})):
            invalid = copy.deepcopy(config)
            mutate(invalid)
            with self.assertRaises(ValueError):
                validate(invalid)

    def test_doctor_read_only_and_secret_permissions(self):
        self.init()
        before = self.config.read_bytes()
        with patch('nabedra.cli.resolve_server_binary', return_value='/binary'), patch('os.execv') as execute:
            self.run_cli('doctor', '--config', str(self.config), '--json')
            self.assertEqual(execute.call_args.args[1], ['/binary', 'doctor', '--config', str(self.config), '--json'])
        self.assertEqual(self.config.read_bytes(), before)
        Path(load(self.config)['auth_file']).chmod(0o644)
        with patch('os.execv') as execute:
            self.assertEqual(self.run_cli('doctor', '--config', str(self.config))[0], 2)
            execute.assert_not_called()

    def test_admin_uses_single_attempt_transport_and_tls(self):
        token = self.root / 'admin'
        protected_write(token, b'A' * 43)
        with patch('nabedra._control_transport._ControlTransport') as transport:
            transport.return_value.request.return_value = ('id', {'desired': 2})
            self.assertEqual(self.run_cli('cluster', 'scale', '2', '--url', 'https://compute.example', '--admin-token-file', str(token), '--json')[0], 0)
            self.assertEqual(transport.return_value.request.call_args.args, ('cluster.scale', {'nodes': 2}))
        self.assertEqual(self.run_cli('cluster', 'status', '--url', 'http://compute.example', '--admin-token-file', str(token))[0], 2)

    def test_generated_auth_file_can_administer_cluster(self):
        self.init()
        config = load(self.config)
        with patch('nabedra._control_transport._ControlTransport') as transport:
            transport.return_value.request.return_value = ('id', {})
            self.assertEqual(self.run_cli('cluster', 'status', '--url', 'https://compute.example', '--admin-token-file', config['auth_file'], '--json')[0], 0)
            expected = json.loads(Path(config['auth_file']).read_text())['admin_token']
            self.assertEqual(transport.call_args.args[1], expected)

    def test_migration_delegates_confirmed_command(self):
        with self.assertRaises(SystemExit) as error:
            self.run_cli('migrate', '--storage', 's3://test-bucket/db/', '--region', 'eu-west-1')
        self.assertEqual(error.exception.code, 2)
        with patch('nabedra.cli.resolve_server_binary', return_value='/binary'), patch('os.execv') as execute:
            self.run_cli('migrate', '--storage', 's3://test-bucket/db/', '--region', 'eu-west-1', '--cluster-id', '11111111-1111-4111-8111-111111111111', '--confirm-writers-stopped')
            self.assertEqual(execute.call_args.args[1][-3:], ['--cluster-id', '11111111-1111-4111-8111-111111111111', '--confirm-writers-stopped'])
            execute.reset_mock()
            self.assertEqual(self.run_cli('migrate', '--storage', 's3://test-bucket/db/', '--region', 'eu-west-1', '--cluster-id', 'invalid', '--confirm-writers-stopped')[0], 2)
            execute.assert_not_called()

    def test_environment_binary_precedes_path_and_override_is_final(self):
        import sys
        envbin = self.root / 'bin'
        envbin.mkdir()
        binary = envbin / 'nabedra-db'
        binary.write_text('#!' + sys.executable + '\nprint("nabedra-db 0.1.1")\n')
        binary.chmod(0o700)
        with patch('nabedra._binary.sys.prefix', str(self.root)), patch.dict(os.environ, {}, clear=True):
            for suffix in ('', ' olap', ' oltp'):
                with self.subTest(engine=suffix):
                    binary.write_text('#!' + sys.executable + '\nprint("nabedra-db 0.1.1' + suffix + '")\n')
                    self.assertEqual(resolve_server_binary(check=True), str(binary))
                    with contextlib.redirect_stdout(io.StringIO()) as output:
                        self.assertEqual(cli.main(['--version', '--json']), 0)
                    versions = json.loads(output.getvalue())
                    self.assertEqual(versions['cli'], '0.1.0')
                    self.assertEqual(versions['binary'], '0.1.1')
            with patch.dict(os.environ, {'NABEDRA_SERVER_BINARY': 'absent'}):
                with self.assertRaises(RuntimeError):
                    resolve_server_binary(check=True)
            for invalid in ('nabedra-db 9.0.0', 'nabedra-db 0.1.1 hybrid'):
                binary.write_text('#!' + sys.executable + '\nprint("' + invalid + '")\n')
                with self.assertRaises(RuntimeError):
                    resolve_server_binary(check=True)


if __name__ == '__main__':
    unittest.main()
