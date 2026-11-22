"""Exercise the upgrade CLI against the archive replies sent by the old workers."""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError, URLError
from uuid import uuid4


SCRIPT = Path(__file__).resolve().parents[2] / 'scripts' / 'oltp_upgrade_export.py'
sys.path.insert(0, str(SCRIPT.parent))
from oltp_upgrade_export import export_attempt


class UpgradeExport(unittest.TestCase):
    def run_export(self, respond, *calls):
        requests = []

        class Worker(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                worker = self.path.split('/')[1]
                requests.append((worker, request['payload']))
                status, reply = respond(worker, request['payload'])
                if status is None:
                    return  # The connection drops before the caller sees the result.
                body = json.dumps({'request_id': request['request_id'], **reply}).encode()
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        with TemporaryDirectory() as state, ThreadingHTTPServer(('127.0.0.1', 0), Worker) as server:
            thread = Thread(target=server.serve_forever)
            thread.start()
            try:
                url = f'http://127.0.0.1:{server.server_port}'
                command = [sys.executable, str(SCRIPT), '--worker', f'{url}/follower', '--worker', f'{url}/owner',
                           '--database', 'test', '--cluster-id', str(uuid4()), '--backup-id', str(uuid4()),
                           '--repository', '/archives']
                results = [subprocess.run(command + list(args),
                           env=dict(os.environ, NABEDRA_ADMIN_TOKEN='test', XDG_STATE_HOME=state),
                           capture_output=True, text=True, timeout=15) for args in calls]
            finally:
                server.shutdown()
                thread.join()
        return results, requests

    @staticmethod
    def error(status, code, message):
        return status, {'error': {'code': code, 'message': message, 'outcome': 'not_applied'}}

    def test_mig2_o19_cancelled_pin_starts_one_fresh_attempt(self):
        def respond(worker, payload):
            if worker == 'follower':
                return self.error(503, 'storage_unavailable', 'worker runtime is not accepting requests')
            if payload['owner']['attempt'] == 1:
                return self.error(409, 'conflict', 'timestamp transition rejected: Rejected(PinExpired)')
            return 200, {'data': {'owner': payload['owner'], 'key': {'archive_id': Path(payload['output']).name}}}

        [result], requests = self.run_export(respond, ['--insecure'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([worker for worker, _ in requests], ['follower', 'owner'] * 2)
        first, second = requests[0][1], requests[2][1]
        self.assertEqual(first, requests[1][1])
        self.assertEqual(second, requests[3][1])
        self.assertEqual(second['owner'], first['owner'] | {'attempt': 2})
        self.assertNotEqual(second['output'], first['output'])
        self.assertEqual(json.loads(result.stdout)['key']['archive_id'], Path(second['output']).name)

    def test_mig2_o19_lost_response_does_not_start_a_second_archive(self):
        [result], requests = self.run_export(lambda *_: (None, None), ['--insecure'])
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0][1], requests[1][1])
        self.assertEqual(requests[0][1]['owner']['attempt'], 1)
        self.assertEqual(result.stdout, '')

    def test_mig2_o19_rerun_starts_at_the_confirmed_next_attempt(self):
        owner_attempts = []

        def respond(worker, payload):
            if worker == 'follower':
                return self.error(503, 'storage_unavailable', 'worker runtime is not accepting requests')
            attempt = payload['owner']['attempt']
            owner_attempts.append(attempt)
            if attempt == 1:
                return self.error(409, 'conflict', 'timestamp transition rejected: Rejected(PinExpired)')
            if owner_attempts == [1, 2]:
                return None, None
            return 200, {'data': {'owner': payload['owner'], 'key': {'archive_id': Path(payload['output']).name}}}

        [interrupted, resumed], requests = self.run_export(respond, ['--insecure'], ['--insecure'])
        self.assertEqual(interrupted.returncode, 1, interrupted.stderr)
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertEqual(owner_attempts, [1, 2, 2])
        self.assertEqual(requests[3], requests[-1])
        self.assertEqual(json.loads(resumed.stdout)['owner']['attempt'], 2)

    def test_mig2_rejects_http_before_sending_the_token(self):
        [result], requests = self.run_export(lambda *_: (None, None), [])
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('--insecure', result.stderr)
        self.assertEqual(requests, [])

    def test_mig2_timeout_then_non_owner_503_gives_same_backup_id_hint(self):
        workers = ['https://owner', 'https://follower']
        owner = {'backup_id': str(uuid4()), 'attempt': 2}
        for timeout in (TimeoutError('read timed out'), URLError(TimeoutError('connect timed out'))):
            with self.subTest(timeout=timeout):
                opener = Mock()
                unavailable = HTTPError(workers[1], 503, 'not accepting requests', {}, BytesIO(b'not accepting requests'))
                opener.open.side_effect = [timeout, unavailable]
                with self.assertRaisesRegex(RuntimeError, f'rerun with the same --backup-id {owner["backup_id"]}'):
                    export_attempt(opener, workers, 'test', 'secret', Path('/archives/example'), owner, 1)
                self.assertEqual(opener.open.call_count, 2)


if __name__ == '__main__':
    unittest.main()
