"""Unit tests of sim_triage.py: what may not count as a named cause (review of PR #115, H3)."""

import unittest

import sim_triage as triage


def usage(node, now, before, threads=0):
    def shown(size, threads):
        return (f"ResourceUsage {{ working_decode_bytes: {size}, native_threads: {threads}, "
                "direct_in_flight: 0, disk: {} }")
    return (f"t=9ms ResourceBaseline: n{node} has {shown(now, threads)} after quiescence, "
            f"baseline {shown(before, 0)}")


def diagnosis(kinds, *lines):
    out = triage.Diagnosis()
    out.kinds = set(kinds)
    for line in lines:
        out.add(line)
    return out


def lease(label, lease, size, stack, node=1):
    return f"SCR {label} n{node} #{lease} {size}B t5 {stack}"


ROUTE = "repl::authority::control::ControlClient::replica_routes <- x <- y ## z"
BATCH = "repl::typed_log::TypedLogStore::next_stored <- x ## z"


def held(*stacks):
    """Every sample holds one 700 byte lease per stack, and the baseline holds another lease."""
    out = [lease("base", 1, 100, ROUTE)]
    for label in triage.SAMPLES:
        out.append(lease(label, 1, 100, ROUTE))
        out += [lease(label, 2 + i, 700, stack) for i, stack in enumerate(stacks)]
    return out


def red(*kinds):
    return {"verdict": "red", "violations": list(kinds)}


class Triage(unittest.TestCase):
    def classify(self, violations, known, kinds=("ResourceBaseline",)):
        return triage.classify(red(*kinds), violations, known)[0]

    def test_a_held_route_with_the_exact_bytes_is_named(self):
        known = diagnosis(["ResourceBaseline"], *held(ROUTE))
        self.assertEqual(self.classify([usage(1, 792, 100)], known), "named")

    def test_unmatched_bytes_are_undiagnosed(self):
        known = diagnosis(["ResourceBaseline"], *held(ROUTE))
        for now in (791, 800, 792 + 7312):
            self.assertEqual(self.classify([usage(1, now, 100)], known), "undiagnosed")

    def test_an_unknown_site_is_undiagnosed(self):
        known = diagnosis(["ResourceBaseline"], *held("some::other::job <- x ## z"))
        self.assertEqual(self.classify([usage(1, 800, 100)], known), "undiagnosed")

    def test_an_inferred_holder_is_undiagnosed(self):
        known = diagnosis(["ResourceBaseline"], *held(BATCH))
        self.assertEqual(self.classify([usage(1, 800, 100)], known), "undiagnosed")

    def test_a_route_that_is_not_held_is_undiagnosed(self):
        lines = [line for line in held(ROUTE) if not line.startswith("SCR e6")]
        known = diagnosis(["ResourceBaseline"], *lines)
        self.assertEqual(self.classify([usage(1, 792, 100)], known), "undiagnosed")

    def test_another_gauge_is_undiagnosed(self):
        known = diagnosis(["ResourceBaseline"], *held(ROUTE))
        self.assertEqual(self.classify([usage(1, 792, 100, threads=1)], known), "undiagnosed")

    def test_a_node_without_a_sample_is_undiagnosed(self):
        known = diagnosis(["ResourceBaseline"], *held(ROUTE))
        self.assertEqual(self.classify([usage(2, 792, 100)], known), "undiagnosed")

    def test_one_violation_without_evidence_makes_the_seed_undiagnosed(self):
        known = diagnosis(["ResourceBaseline", "NotAppliedNeverCommitted"], *held(ROUTE))
        violations = [usage(1, 792, 100), "t=9ms NotAppliedNeverCommitted: answered not_applied"]
        kinds = ("ResourceBaseline", "NotAppliedNeverCommitted")
        self.assertEqual(self.classify(violations, known, kinds), "undiagnosed")

    def test_a_diagnostic_run_with_other_violations_names_nothing(self):
        known = diagnosis(["ResourceBaseline", "Liveness"], *held(ROUTE))
        self.assertEqual(self.classify([usage(1, 792, 100)], known), "undiagnosed")
        self.assertEqual(self.classify([usage(1, 792, 100)], None), "undiagnosed")
        self.assertEqual(self.classify([], known, ("ResourceBaseline", "Liveness")), "undiagnosed")

    def test_a_split_is_named_only_when_every_plan_was_refused_at_the_freeze(self):
        text = "t=9ms Liveness: the split did not publish within 60s after the heal: {\"last\":%s,\"plans\":2}"
        known = diagnosis(["Liveness"])
        refused = f'"Conflict {triage.FREEZE}"'
        for last, want in ((f"[{refused},{refused}]", "named"), (f'[{refused},"n1 unknown"]', "undiagnosed"),
                           (f'[{refused},"n1 [odd] text"]', "undiagnosed"), ("[]", "undiagnosed")):
            self.assertEqual(self.classify([text % last], known, ("Liveness",)), want)

    def test_a_lost_worker_needs_the_refused_fences(self):
        text = "t=9ms Liveness: survivors never dropped n1: readiness deadline"
        busy = "DIAG t=5 n=2 CONTROL-FAULT Busy oltp-replication/src/authority/control/machine.rs:320"
        known = diagnosis(["Liveness"], *[busy] * 101)
        self.assertEqual(self.classify([text], known, ("Liveness",)), "named")
        self.assertEqual(self.classify([text], diagnosis(["Liveness"], busy), ("Liveness",)), "undiagnosed")

    def test_a_seed_that_was_not_replayed_or_did_no_work_is_not_clean(self):
        row = {"violations": [], "replayed": False, "deterministic": None}
        self.assertEqual(triage.verdict(row), "unreplayed")
        row["replayed"], row["deterministic"] = True, True
        self.assertEqual(triage.verdict(row), "unreplayed")
        row.update(chaos_writes=1, chaos_reads=1)
        self.assertEqual(triage.verdict(row), "clean")
        row["deterministic"] = False
        self.assertEqual(triage.verdict(row), "red")
        self.assertEqual(triage.classify({"verdict": "unreplayed"}, [], None), ("unreplayed", []))


if __name__ == "__main__":
    unittest.main()
