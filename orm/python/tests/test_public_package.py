"""Public constructor, renamed package and command routing contracts."""
import contextlib
import io
import json
import os
from pathlib import Path
import tomllib
import unittest
from unittest.mock import Mock, patch

from nabedra import AuthenticationError, Database, DB, NabedraError, ValidationError
from nabedra import cli


class DatabaseInterfaceTests(unittest.TestCase):
    def test_modes_are_read_only_and_construction_is_inert(self):
        with patch.dict(os.environ, {}, clear=True), patch('nabedra.client.build_opener') as opener:
            for mode in ('olap', 'oltp'):
                db = Database(url='https://example.com', mode=mode)
                self.assertEqual(db.mode, mode)
                with self.assertRaises(AttributeError):
                    db.mode = 'oltp'
            self.assertEqual(Database(url='https://example.com').mode, 'oltp')
            opener.return_value.open.assert_not_called()
        self.assertIs(DB, Database)

    def test_invalid_modes_and_duplicate_urls_fail_locally(self):
        for mode in ('OLAP', '', None, True, [], {}):
            with self.subTest(mode=mode), self.assertRaises(ValidationError):
                Database(url='https://example.com', mode=mode)
        with self.assertRaises(ValidationError):
            Database(url='https://example.com', api_url='https://other.example')
        self.assertEqual(Database(api_url='https://example.com', token='explicit').mode, 'oltp')

    def test_missing_auth_fails_before_reads_mutations_and_receipt_io(self):
        with patch.dict(os.environ, {}, clear=True), patch('nabedra.client.build_opener') as opener:
            db = Database(url='https://example.com', mode='olap')
            identity = '11111111-1111-4111-8111-111111111111'
            calls = [db.tables, lambda: db.submit('table.insert', {}),
                     lambda: db.receipt(identity, identity),
                     lambda: db.create_table('users', {'id': str}, 'id')]
            for call in calls:
                with self.assertRaises(AuthenticationError) as failure:
                    call()
                self.assertIsInstance(failure.exception, NabedraError)
                self.assertEqual(failure.exception.outcome, 'not_applied')
                self.assertIn('NABEDRA_TOKEN', str(failure.exception))
            opener.return_value.open.assert_not_called()

    def test_environment_auth_and_explicit_override_reach_new_selector_header(self):
        with patch.dict(os.environ, {'NABEDRA_TOKEN': 'environment-token'}, clear=True):
            for explicit, expected in ((None, 'environment-token'), ('explicit-token', 'explicit-token')):
                db = Database(url='https://example.com', token=explicit, database='tenant-a')
                def respond(request, **kwargs):
                    self.assertEqual(request.get_header('Authorization'), 'Bearer ' + expected)
                    self.assertEqual(request.get_header('X-nabedra-database'), 'tenant-a')
                    request_id = json.loads(request.data)['request_id']
                    body = json.dumps({'request_id': request_id, 'data': []}).encode()
                    return Mock(status=200, headers={'Content-Type': 'application/json'},
                                read=Mock(return_value=body))
                with patch.object(db._transport._opener, 'open', side_effect=respond):
                    self.assertEqual(db.tables(), [])
        for token in ('', 'unsafe\nsecret'):
            with patch.dict(os.environ, {'NABEDRA_TOKEN': token}, clear=True):
                with self.assertRaises(ValidationError):
                    Database(url='https://example.com')


class PackageCommandTests(unittest.TestCase):
    def test_distribution_and_console_entrypoint(self):
        manifest = Path(__file__).resolve().parents[1] / 'pyproject.toml'
        project = tomllib.loads(manifest.read_text())['project']
        self.assertEqual(project['name'], 'nabedra')
        self.assertEqual(project['scripts'], {'nabedra': 'nabedra.cli:main'})
        self.assertTrue(callable(cli.main))

    def test_database_routes_are_grouped_once(self):
        parser = cli._parser()
        for command in ('init', 'serve', 'doctor', 'worker', 'cluster', 'migrate'):
            with self.subTest(command=command), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(SystemExit) as result:
                    parser.parse_args(['database', command, '--help'])
                self.assertEqual(result.exception.code, 0)
        for args in (['init'], ['database', 'database', 'migrate']):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as result:
                parser.parse_args(args)
            self.assertEqual(result.exception.code, 2)

    def test_root_and_database_versions(self):
        for args in (['--version', '--json'], ['database', '--version', '--json']):
            with patch('nabedra.cli.resolve_server_binary', return_value='/server'), patch('nabedra.cli.binary_version', return_value='0.1.0'), contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(cli.main(args), 0)
                self.assertEqual(json.loads(output.getvalue())['cli'], '0.2.0')


if __name__ == '__main__':
    unittest.main()
