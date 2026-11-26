#!/usr/bin/env python3
"""Verify the declared Cache recovery schedules and their clean/mutant twins."""

import json
import sys
import tempfile
from pathlib import Path

import tla_check as check
import tla_common as tc
from evidence import sha256_file

MODEL = "CacheRecoveryPolicy.tla"
SPEC = tc.SPECS_DIR / "cache" / "recovery-policy"
BUDGET = 30
# Distinct states and completed depth, including the initial state.
# The additional states in the replacement case are the late-report branch.
CASES = {
    "CacheRecoveryPolicy.cfg": (57, 57),
    "CacheRecoveryPolicy.skewed-failover.cfg": (14, 14),
    "CacheRecoveryPolicy.frozen-move.cfg": (17, 17),
    "CacheRecoveryPolicy.retired-replication.cfg": (11, 11),
    "CacheRecoveryPolicy.failover-after-window.cfg": (13, 13),
    "CacheRecoveryPolicy.replace-after-window.cfg": (15, 12),
    "CacheRecoveryPolicy.staged-reset.cfg": (15, 15),
    "CacheRecoveryPolicy.operator-reset.cfg": (17, 9),
    "CacheRecoveryPolicy.transfer-at-limit.cfg": (46, 46),
    "CacheRecoveryPolicy.transfer-over-limit.cfg": (47, 47),
}


def main() -> int:
    report = check.Report()
    out_root = tc.REPO_ROOT / "target" / "tla" / "cache-recovery-policy"
    out_root.mkdir(parents=True, exist_ok=True)
    out = Path(tempfile.mkdtemp(prefix="run-", dir=out_root))
    workers = tc.default_workers()
    model = check.Model(SPEC, MODEL)
    baselines = [SPEC / name for name in CASES]
    mutants = sorted((SPEC / "mutants").glob("*.cfg"))
    check.search_check(report, model, baselines)
    runs = []
    for cfg in baselines:
        report.check(tc.parse_cfg(cfg).check_deadlock, cfg.name + " deadlock check")
        # TLC's expression coverage instrumentation expands this scheduled
        # model excessively. A schedule must reach its exact terminal depth
        # and state count, with deadlock checking on and an empty queue.
        _, fact, reason = check.run_cfg(
            SPEC, MODEL, cfg, cfg.name, out, BUDGET, ["-workers", workers],
        )
        expected = CASES[cfg.name]
        actual = (fact["distinct_states"], fact["depth"])
        if fact["verdict"] == "error":
            report.error(cfg.name, reason)
        elif fact["verdict"] == "incomplete":
            report.incomplete = True
            report.skip(cfg.name, reason)
        else:
            matches = fact["verdict"] == "pass" and fact["states_left"] == 0 and actual == expected
            report.check(matches, cfg.name, reason or f"{actual[0]} states, depth {actual[1]}, expected {expected}")
            if not matches:
                fact["verdict"] = "fail"
        fact["detail"] = reason
        runs.append(fact)

    controls = check.Controls(SPEC, MODEL, out, BUDGET, workers, baselines, runs)
    caught = [
        check.run_mutant(report, SPEC, MODEL, cfg, out, BUDGET, workers, model, baselines, controls)
        for cfg in mutants
    ]
    report.check(len(caught) == 10, "policy mutants", f"{len(caught)} of 10 configured")
    facts = {
        "mode": "exhaustive",
        "spec": "specs/cache/recovery-policy/CacheRecoveryPolicy",
        "tier": "T1",
        "tools": tc.tool_versions(),
        "budget_s": BUDGET,
        "limits": ["Declared recovery schedules only, from the cluster Init."],
        "runs": runs,
        "controls": controls.facts,
        "mutants": caught,
        **check.settings_facts(model, baselines, mutants),
    }
    verdict = report.verdict()
    full_settings = facts.pop("settings_full")
    summary = {
        "verdict": verdict,
        "executed": report.executed,
        "failed": report.failed,
        "not_run": [{"id": "tlc-error", "reason": item} for item in report.errors] + report.not_run,
        "tla": facts,
    }
    (out / "summary.json").write_text(
        json.dumps({**summary, "settings": full_settings}, indent=2) + "\n", encoding="utf-8",
    )
    tc.print_summary({
        **summary,
        "artifacts": [
            {"name": f"tla/{file.name}", "sha256": sha256_file(file)}
            for file in sorted(out.iterdir()) if file.is_file()
        ],
    })
    return check.EXIT[verdict]


if __name__ == "__main__":
    sys.exit(main())
