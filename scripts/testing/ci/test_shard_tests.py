#!/usr/bin/env python3

import unittest

from shard_tests import shard_tests


class ShardTests(unittest.TestCase):
    def test_shards_are_stable_disjoint_and_cover_every_suite(self):
        entries = [
            "stream::write_retry: test",
            "engine::read_after_commit: test",
            "stream::write: test",
            "integration_commit: test",
            "engine::read: test",
            "stream::write: test",  # The name may occur in two test binaries.
            "throughput: benchmark",
            "",
            "5 tests, 1 benchmark",
        ]
        listing = "\n".join(entries)
        excluded = {"engine::read"}
        first = shard_tests(listing, 1, 2, excluded)
        second = shard_tests(listing, 2, 2, excluded)
        self.assertEqual(first, ["engine::read_after_commit", "stream::write"])
        self.assertEqual(second, ["integration_commit", "stream::write_retry"])
        self.assertEqual(shard_tests("\n".join(reversed(entries)), 2, 2, excluded), second)
        self.assertFalse(set(first) & set(second))

    def test_empty_listing_does_not_select_the_full_suite(self):
        self.assertEqual(shard_tests("0 tests, 0 benchmarks", 1, 2, set()), [])


if __name__ == "__main__":
    unittest.main()
