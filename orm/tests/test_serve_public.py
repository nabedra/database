"""Public configured-provider path composes the reviewed bridge/process owners."""
import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from meshdb import ValidationError

module = importlib.import_module('meshdb.serve')
C = '00000000-0000-4000-8000-000000000001'


class PublicServeTests(unittest.TestCase):
    def test_a01_no_adapter_rejects_autoscaling_before_start(self):
        with patch.object(module, 'run_process') as process:
            with self.assertRaises(ValidationError):
                module.serve('cluster', (1, 3))
            process.assert_not_called()

    def test_a02_callbacks_reach_active_bridge_and_flags_reach_child(self):
        called = []
        callbacks = (lambda value: called.append(value), lambda value: called.append(value), lambda: [])
        records = []

        def run(config, startup, *, prepared, stop, _receipt, cold_start, migrate_catalog):
            records.append(startup)
            self.assertEqual(startup['provisioning'], 'managed')
            self.assertEqual(startup['initial_policy']['mode'], 'autoscale')
            self.assertEqual((cold_start, migrate_catalog), (True, True))
            prepared(dict(cluster_id=C, authorize_url='http://127.0.0.1:1234/private/v1/authorize'))
            stop()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text('{}')
            with patch.dict('os.environ', {'MESHDB_CONFIG': str(path)}), patch.object(module, 'run_process', side_effect=run):
                # Repeated ownership after cleanup proves the first bridge stopped.
                for _ in range(2):
                    module.serve('cluster', (1, 3), start_node=callbacks[0], stop_node=callbacks[1],
                                 list_nodes=callbacks[2], cold_start=True, migrate_catalog=True)
        self.assertEqual(called, [])
        self.assertNotEqual(records[0]['bridge']['callback_token'], records[0]['bridge']['authorize_token'])

    def test_callback_declaration_failure_does_not_launch(self):
        with patch.object(module, 'run_process') as process:
            with self.assertRaises(ValidationError):
                module.serve('cluster', start_node=lambda _: 'resource')
            process.assert_not_called()


if __name__ == '__main__':
    unittest.main()
