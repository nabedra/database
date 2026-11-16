"""Keep TLS material until process exit is confirmed, including stop failures."""
import gc
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import acceptance_lifecycle


class CleanupTest(unittest.TestCase):
    def test_tls_cleanup_depends_on_observed_exit(self):
        for returncode in (None, -9, 0):
            with self.subTest(returncode=returncode):
                roots = []

                class Process:
                    api_url = 'https://127.0.0.1:8443'
                    private_token = 'local-test-token'
                    pid = 123

                    def __init__(self, *args, **kwargs):
                        self.returncode = returncode

                    def start(self):
                        return self

                    def stop(self):
                        if returncode != 0:
                            raise OSError()
                        return 0

                def material(path):
                    path.mkdir()
                    roots.append(path.parent)
                    return SimpleNamespace(server_cert=path / 'cert.pem',
                                           server_key=path / 'key.pem', ca_cert=path / 'ca.pem')

                def no_client(*args, **kwargs):
                    raise RuntimeError()

                config = SimpleNamespace(bucket='meshdb-testing', region='us-east-1',
                    allocate_prefix=lambda *args: SimpleNamespace(prefix='test-5-cleanup-local/'))
                try:
                    with patch.dict('sys.modules', {
                        'server_process': SimpleNamespace(ServerProcess=Process),
                        'test_tls_material': SimpleNamespace(create_test_tls=material),
                    }), patch('nabedra.Database', no_client):
                        record = acceptance_lifecycle.run_scenario('https', config, None)
                    gc.collect()
                    self.assertEqual(len(roots), 1)
                    self.assertEqual(roots[0].exists(), returncode is None)
                    self.assertEqual(record['status'], 'failed')
                    if returncode is None:
                        self.assertIn('local_tls_cleanup_exit_unconfirmed',
                                      [item['label'] for item in record['checks']])
                finally:
                    for root in roots:
                        if root.exists():
                            import shutil
                            shutil.rmtree(root)


if __name__ == '__main__':
    unittest.main()
