---
name: code-review
description: Use when asked to do QA, code review or testing
---

# Review protocol

## Find the affected guarantees

Start with the diff. Set `BASE_SHA` and `HEAD_SHA` to the commits under
review and select checks when the matrix tool is available.

```sh
python3 scripts/testing/matrix.py select --base "$BASE_SHA" --head "$HEAD_SHA"
```

For every affected guarantee, read its required check kinds and the matrix
cells for the base commit. Add guarantees the path map misses. The rubric
lists the affected guarantee IDs, their modules' assurance levels and the
selected checks. Cover each guarantee or explain why it does not apply.
Give a reason for every check you add or skip. See [guarantees](references/guarantees.md).

For a docs-only diff, review the content, examples and links and record the
local results. Use the [docs-only CI rule](../nabedra-change/SKILL.md).

## Build the review rubric

- Run the applicable build and syntax checks. For dependency changes, run
  `python3 scripts/testing/licenses.py` to check the allowlist.
- Cover the main use cases, likely edge cases and interactions with existing
  behavior. For UI changes, compare before and after with screenshots or a
  recording, and exercise the affected interactions. Check for layout and
  rendering regressions.
- For logic changes, vary inputs, workload size and concurrency within the
  intended CPU and memory budgets. Check the affected behavior at each scale.
- For ORM changes and database changes that affect ORM callers, include the
  [ORM parity checks](../../../docs/testing/orm-parity.md).
- Check compatibility against the approved plan. Resolve any unspecified
  change to a public contract before approving it.

Use [the sample rubrics](references/example.md) to pick 3 to 10 essential
tests per use case. For variations, express each test as a property or
simulation workload with 3 to 5 named axes, such as scale, concurrency,
fault placement or mode. Run the axes across generated seeds. Sample
configuration knobs from the register using the same seed and record the
choices, seeds, case counts and checker verdict. Write a separate case by
hand when the generator cannot reach it.
[Simulation](references/simulation.md) covers running, replaying and shrinking.

## Run the checks

Use the [feedback tiers](../nabedra-change/SKILL.md#feedback-tiers) to choose
where to run the checks. Dispatch heavy checks on GitHub Actions, pinned to
the reviewed commit. Follow [GitHub Actions for checks](references/gh-actions.md)
and record the run URL, tested SHA and executed assertions.

Run real-environment compute in Docker or kind. Report checks that require
cloud compute as `not-run: cloud-deferred`. Use real S3 only through the
dedicated test bucket. For setup, see `scripts/testing/s3/README.md`.
Label simulation evidence as a fault model and run any real-environment
checks the register requires. Follow the [evidence rules](references/evidence.md).

## Verification rules

- The implementer reports commands and raw results. A reviewer verifies
  guarantees using attested evidence on the reviewed SHA.
- The adversarial tester uses a different model from the implementer and
  writes tests from the guarantees and diff, independently of the
  implementer's notes.
- Before counting a new test, show that it fails on a mutant of the touched
  code or on the reverted fix. Record the mutant or reverted revision and
  the failing result.
- For each bug finding, identify the check that should have caught it and
  add the missing invariant. Follow the [bug loop](references/bug-loop.md).
- Update the plan before changing a public interface, such as a wire
  protocol, HTTP API, ORM signature, configuration, CLI or on-disk format.
  The plan gives a success measure and the scenario IDs.
- Merge only after a non-author reviewer approves and all blocking findings
  are resolved.
- Put every new or changed test through `scripts/testing/repeat_gate.py`.
  Run unit tests 50 times and real-environment cases 5 times. Run simulation
  workloads with 5 times their usual seeds. The test catalog at
  `docs/testing/catalog.json` identifies new and changed tests.
- Use `scripts/testing/triage.py` for real-environment failures. Retry up to
  10 times and block if the pass rate falls more than one percentage point
  below the 14-day rate on `main`. A panic, abort or OOM kill blocks without
  retry. Replay a failing simulation seed to diagnose the bug. The failure
  stays recorded.
- Use `scripts/testing/ab.py` for performance claims. It alternates base and
  head on one runner and flags a regression of 5% or more that exceeds the
  bootstrap threshold. Use dedicated hardware for absolute latency targets.

## Pull request body

Every pull request body carries the following four lines.
`scripts/testing/pr_lint.py` checks them in the pull-request gate.

```text
Guarantees: <affected guarantee IDs, or none>
Evidence: <commands and run URLs, each with its raw result lines>
Not run: <every selected check that did not execute, and why, or none>
Reviewers: <non-author reviewers and their verdicts, or pending cross-model review>
```

## Rubric-style reviewing

Record the review tasks in a Markdown checklist or table. When coordinating
parallel work, group independent checks and assign them using the
[subagent workflow](../nabedra-change/references/subagent-driven-development.md).
Each tester uses its own request ID for a dispatch and reruns or cancels only
runs it started. Start dependent checks after their prerequisites pass.
