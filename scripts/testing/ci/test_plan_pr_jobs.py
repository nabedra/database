import unittest

from plan_pr_jobs import BASE_JOBS, select


class PullRequestJobsTests(unittest.TestCase):
    def test_engine_only_changes_select_their_own_suites(self):
        for path, jobs in (
            ("server/events/src/lib.rs", {"events"}),
            ("server/src/cache/controller.rs", {"cache", "clippy"}),
            ("server/src/engine/query.rs", {"olap", "clippy"}),
            ("server/src/oltp_runtime/direct/mod.rs", {"oltp", "clippy"}),
        ):
            with self.subTest(path=path):
                self.assertEqual(select([path]), jobs)

    def test_suites_combine_across_paths_and_include_known_consumers(self):
        self.assertEqual(
            select(["server/src/cache.rs", "server/kv/src/lib.rs"]),
            {"cache", "clippy", "events", "kv", "oltp"},
        )
        self.assertEqual(select(["server/src/aws_http.rs"]), {"olap", "clippy", "features"})
        self.assertEqual(
            select(["server/check/src/lib.rs"]),
            {"cache", "check-tests", "clippy", "events", "olap", "oltp"},
        )

    def test_build_inputs_and_unclassified_paths_select_full_baseline(self):
        for path in (
            ".github/workflows/nabedra-build.yml",
            "server/events/Cargo.lock",
            "server/src/main.rs",
            "server/src/storage/shared.rs",
            "server/src/cache/new/build.rs",
            "server/src/cache.rs.other",
            "scripts/new-helper.py",
        ):
            with self.subTest(path=path):
                self.assertEqual(select(["server/events/src/lib.rs", path]), BASE_JOBS)
        self.assertNotIn("quarantine", BASE_JOBS)

    def test_documentation_and_simulation_only_changes_skip_baseline(self):
        self.assertEqual(select([
            "docs/testing/README.md",
            ".agents/skills/nabedra-change/SKILL.md",
            "server/sim/src/lib.rs",
            "server/sim-oltp/src/lib.rs",
        ]), set())
        self.assertEqual(select([]), set())


if __name__ == "__main__":
    unittest.main()
