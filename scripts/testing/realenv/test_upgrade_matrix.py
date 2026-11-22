#!/usr/bin/env python3

import argparse
import unittest
from unittest.mock import Mock

from . import upgrade_matrix


class UpgradeTests(unittest.TestCase):
    def test_explicit_images_keep_oldest_to_head_order(self):
        self.assertEqual(upgrade_matrix.parse_images("release:a,release:b,release:c"),
                         ("release:a", "release:b", "release:c"))
        for bad in ("a,b", "a,,c", "a, b,c"):
            with self.subTest(bad=bad), self.assertRaises(argparse.ArgumentTypeError):
                upgrade_matrix.parse_images(bad)

    def test_image_change_waits_for_new_ready_container(self):
        fixture = Mock()
        fixture.pod.side_effect = [
            {"spec": {"containers": [{"name": "broker", "image": "old"}]}},
            {"status": {"containerStatuses": [{"name": "broker", "containerID": "old", "ready": True}]}},
            {"status": {"containerStatuses": [{"name": "broker", "containerID": "old", "ready": True}]}},
            {"status": {"containerStatuses": [{"name": "broker", "containerID": "new", "ready": False}]}},
            {"status": {"containerStatuses": [{"name": "broker", "containerID": "new", "ready": True}]}},
        ]
        upgrade_matrix.set_image(fixture, 2, "head")
        fixture.cluster.kube.assert_called_once_with("set", "image", "pod/events-2", "broker=head")
        self.assertEqual(fixture.pod.call_count, 5)
