"""Cancellation must not start more acceptance servers or write more fixtures."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import acceptance_http
import acceptance_lifecycle
import acceptance_partitions


class CancellationTest(unittest.TestCase):
    def test_report_setup_interrupt_saves_without_starting_work(self):
        for runner, method_name in ((runner, method) for runner in
                                    (acceptance_lifecycle, acceptance_partitions)
                                    for method in ('mkdir', 'open')):
            with self.subTest(method_name=method_name), tempfile.TemporaryDirectory() as directory:
                report = Path(directory) / 'report.json'
                original = getattr(Path, method_name)
                interrupted = False

                def interrupt_once(path, *args, **kwargs):
                    nonlocal interrupted
                    if not interrupted:
                        interrupted = True
                        raise KeyboardInterrupt()
                    return original(path, *args, **kwargs)

                with patch.object(Path, method_name, interrupt_once), \
                        patch.dict('sys.modules', {'acceptance_support': None}), \
                        contextlib.redirect_stdout(io.StringIO()):
                    result = runner.main(['--report', str(report)])
                data = json.loads(report.read_text())
                self.assertEqual(result, 130)
                self.assertEqual(data['status'], 'failed')
                self.assertTrue(data['cancelled'])
                self.assertEqual(data['scenarios'], [])
                self.assertEqual(data['failure'], {
                    'label': 'report_setup', 'failure_type': 'KeyboardInterrupt'})

    def test_interrupt_stops_scheduling_and_saves_report(self):
        for interrupt_at in ('scenario', 'stop'):
            with self.subTest(interrupt_at=interrupt_at), tempfile.TemporaryDirectory() as directory:
                events = []

                class Process:
                    api_url = 'http://127.0.0.1:8000'
                    private_token = 'local-test-token'

                    def __init__(self, config, namespace, *, binary):
                        events.append('construct')

                    def start(self):
                        events.append('start')

                    def stop(self):
                        events.append('stop')
                        if interrupt_at == 'stop':
                            raise KeyboardInterrupt()
                        return 0

                def scenario(db, checks, server):
                    events.append('scenario')
                    if interrupt_at == 'scenario':
                        raise KeyboardInterrupt()

                config = SimpleNamespace(
                    bucket='meshdb-testing', region='us-east-1',
                    allocate_prefix=lambda *args: SimpleNamespace(prefix='test-1-cancel-local/'))
                report = Path(directory) / 'report.json'
                scenarios = {name: (index, name, scenario)
                             for index, name in enumerate(('first', 'second', 'third'), 1)}
                with patch.dict('sys.modules', {
                    'acceptance_support': SimpleNamespace(load_config=lambda: config),
                    'server_process': SimpleNamespace(ServerProcess=Process),
                }), patch.object(acceptance_http, 'SCENARIOS', scenarios), \
                        contextlib.redirect_stdout(io.StringIO()):
                    result = acceptance_http.main(['--scenario', 'all', '--report', str(report)])
                data = json.loads(report.read_text())
                self.assertEqual(events, ['construct', 'start', 'scenario', 'stop'])
                self.assertEqual(result, 130)
                self.assertEqual(data['status'], 'failed')
                self.assertTrue(data['cancelled'])
                self.assertEqual(len(data['scenarios']), 1)
                self.assertTrue(data['scenarios'][0]['cancelled'])
                self.assertIn('KeyboardInterrupt', [item.get('failure_type')
                              for item in data['scenarios'][0]['checks']])


if __name__ == '__main__':
    unittest.main()
