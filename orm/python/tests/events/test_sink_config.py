"""Direct OLAP endpoints and gateways select the destination differently."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import UUID

from nabedra.events.sink import _Config


class SinkConfigTests(unittest.TestCase):
    def test_selector_is_optional_for_fixed_endpoints_and_validated_for_gateways(self):
        olap = {
            "url": "https://destination.example",
            "token_env": "TEST_SINK_DESTINATION",
            "table": "events",
            "table_id": str(UUID(int=1)),
        }
        config = {
            "events": {"url": "https://source.example", "token_env": "TEST_SINK_SOURCE",
                       "topic": "orders", "subscription": "warehouse"},
            "olap": olap,
            "partitions": {"start": 0, "stop": 1},
        }
        with TemporaryDirectory() as folder, patch.dict("os.environ", {
            "TEST_SINK_SOURCE": "fixture-source", "TEST_SINK_DESTINATION": "fixture-destination",
        }):
            path = Path(folder) / "sink.json"

            def load():
                path.write_text(json.dumps(config), encoding="utf-8")
                return _Config.load(path)

            self.assertIsNone(load().database)
            olap["database"] = "analytics-v1"
            self.assertEqual(load().database, "analytics-v1")
            for selector in (None, "", "analytics/other"):
                with self.subTest(selector=selector), self.assertRaisesRegex(ValueError, "database selector"):
                    olap["database"] = selector
                    load()
