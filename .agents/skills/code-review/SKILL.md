---
name: code-review
description: Use when asked to do QA, code review or testing
---

When you are asked to review a PR, an implementation or the like, do the following:

0. Make the absolute most fundamental rigorous checks.
Here goes making sure the code compiles, it has no syntax errors, etc.
When the diff changes a dependency, run `python3 scripts/testing/licenses.py`. It fails
on any license outside the allowlist, including terms that restrict AI-assisted use.

1. Find the affected guarantees.
Before writing the rubric, select the checks for the diff:

```sh
python3 scripts/testing/matrix.py select --base <base sha> --head <head sha>
```

It maps the changed paths through `docs/testing/modules.toml` to modules,
guarantees and checks, and prints the T1 check set and the guarantee IDs the
review must cover. For each guarantee, read its row in
`docs/testing/guarantees/` (its required check kinds and `n/a` reasons) and its
cells for the base commit in `docs/testing/coverage-matrix.md`. Add any
guarantee the paths miss, such as one that depends on a shared type or a
protocol the diff changes, and say why. The rubric lists the selected checks,
the module's assurance level and the affected guarantees. It covers each
affected guarantee or states why it does not apply, and it gives a reason for
every check it adds to or drops from the selected set. See
[guarantees](references/guarantees.md).

2. Come up with the 20% of tests that will assess 99% of the quality.
Create a rubric with the minimal set of tests that comprises a full quality assessment of the changes made that rigorously make sure there are no bugs, errors, unintended behavior or efficiency issues. Do the following and include it in the PR or response to the user that asked your code review:
- When a change is made to the UI, take a screenshot or record a video (if it includes animations or other moving parts) of the before and after. Use it to compare if the new UI renders correctly, has not padding/margin issues, font style issues, and perform style checks for the changed components. Of course, also make sure that nothing in the UI crashes or gives dramatic errors under plausible user interactions. 
- When a change is made to the logic, test the main use cases at different scales. If a module for a database is coded, test it for a myriad of possible data and at different possible scales; for example, inserting 1,000 rows vs 10,000. NOTE: Ask for CPU and RAM budgets if you are not given them, and use realistic numbers. It might not be realistic for a testing to run a batch of a billion inserts, but it might be sensible to run a batch of 10, then 100, then 1,000. Here what you want to test is the scaling capability and that the system does not break down at: (1) all its intended scales, (2) all its intended input and output data types [if it is some regex module, try out with strings that are expected and unexpected, those that the code planned for and it didn't plan for...], (3) all intended interactions between modules [e.g., a query that uses multiple intrincate filters in a complicated order, multiple workers handling a big query and how those workers map and reduce the data, if they give any bug, crash, inefficiency or malformed data, that the loaded data fits on the RAM of the device, that CPU demands don't exceed the available CPU...]
- In either case, test the efficiency of the implementation by checking unnecessary renders in the case of the UI (e.g., too many rerenderings for a page or component, too much time for the page to first load, too much lag between interactions and page transitions...) and unnecessary or suboptimal functionality for logic (e.g., laggish queries, laggish joins, laggish pipelines or data processing...)
- In either case, also test the main edge cases that users will most likely face. E.g., clicking on buttons, sliding or scrolling for UIs; scaling to multiple workers, doing complicated queries to a db or doing Stripe transactions in erratic manners for logic.

3. Regression tests
Make sure the new changes do not break anything of what we previously had. This means that the new implementation of a sidebar or module does not break the dashboard, bugs a module already implemented, etc.

For ORM changes and database changes that affect ORM callers, include the
[ORM parity checks](../../../docs/testing/orm-parity.md) in the review rubric.

If there is some compatibility issue (i.e., a change of specs; e.g., migrating from SQLite local db in a mobile phone to JSON files in the app data folder), ask the user if it is intended to break regression compatibility and thus not work well in previous implementations. If not intended, e.g. perhaps losing the previous data, act in consequence. Anyway make sure to ask the user if backwards compatibility is expected to be retained or not, because it might depend on the project (it's not the same a beta app used by virtually no one versus a production and famous mobile app)

4. ZERO Slop policy
Do not overcheck. Never ever create AI slop tests that do not add a ton of value to the code and quality review. These are relatively useless because compilation, syntax errors, etc. are already covered in step 0 and in order to make the actually valuable tests (those that imply user interaction with the UI, intercommunication between logic modules, scaling, AWS/Stripe/Third party integration and API usage, memory/CPU/GPU usage...) the "fundamentals" of the repo (it complies, etc) must indeed work well. 

Do check step 0's most fundamental checks for compilation, syntax errors, plausible errors that might occur during code execution in some edge case scenario...

5. Work in realistic environments
Use a realistic test environment that fits the workload and reproduces how
users run the system. For remote builds, heavy Docker and Kubernetes checks,
and authorized S3 testing, follow the
[Nabedra builds skill](../nabedra-builds/SKILL.md).

Run full suites, acceptance, campaigns and image checks on GitHub Actions by
deliberate dispatch pinned to the reviewed commit, following
[GitHub Actions for checks](references/gh-actions.md). Pushes start nothing.
A check passes only when its run URL, tested SHA and executed assertions are
recorded. List not-run and quarantined checks separately; never count them as
passing.

Simulation is a fault model. Its evidence never replaces a real-environment
check that the register requires. Until cloud compute is approved, real
environments are local Docker and kind, and a check that needs cloud compute
reports `not-run: cloud-deferred`. Real S3 is used only through the dedicated
test bucket (see `scripts/testing/s3/README.md`). Record every result as
[evidence](references/evidence.md).

# Number of tests
Pick 3 to 10 essential tests per use case, as `references/example.md` shows:
each essential test covers one big use case or edge case. Express each as a
property or a simulation workload with named variation axes, such as scale,
concurrency, fault placement and mode (`direct_oltp` on and off). Write 3 to 5
axes. The system runs each axis across generated seeds, and configuration knobs
from the register are sampled from the same seed. Record the seeds, the case
counts and the checker verdict. A hand-written variation is needed only when no
generator can reach the case. See [simulation](references/simulation.md) for
running, replaying and shrinking seeds.

# Verification rules

- The implementer never records its own work as verified. Its report lists what
  it ran and the raw results. Only a reviewer's verdict, backed by attested
  evidence on the reviewed SHA, marks a guarantee as verified.
- Tests for a change are written by an adversarial tester on a different model
  from the implementer. The tester works from the guarantee and the diff, not
  from the implementer's notes.
- A new test counts only after it fails on a mutant of the touched code or on a
  reverted fix. Record the mutant or the reverted commit with the failing
  result.
- Each finding records which existing check should have caught it, and the fix
  adds the missing invariant. Follow the [bug loop](references/bug-loop.md).
- A change to a public interface (wire protocol, HTTP API, ORM signature,
  configuration, CLI or on-disk format) needs an updated plan before
  implementation. The plan says how success will be measured and lists the
  scenario IDs.
- A change merges only with an approval from a reviewer that did not author it.
  An open blocking finding stops the merge.
- A new or changed test counts only after it passes the repetition gate
  (`scripts/testing/repeat_gate.py`): 50 runs for a unit test, 5 for a
  real-environment case, and 5 times the usual seeds for a new simulation
  workload. The test catalog, `docs/testing/catalog.json`, says which tests are
  new.
- A real-environment failure reruns only under the statistical rule
  (`scripts/testing/triage.py`). It reruns up to 10 times and blocks if its pass
  rate falls more than 1 point below the 14-day rate on `main`. A crash (a
  panic, an abort or an OOM kill) never reruns. A simulation failure never
  reruns either, because it is deterministic.
- Performance claims come only from the paired A/B runner
  (`scripts/testing/ab.py`), which alternates base and head on one runner. A
  regression is a change of 5% or more that is also above the bootstrap
  threshold. Absolute numbers from shared runners are not evidence.

# Pull request body

Every pull request body carries these four lines, and
`scripts/testing/pr_lint.py` checks them in the pull-request gate:

```text
Guarantees: <affected guarantee IDs, or none>
Evidence: <commands and run URLs, each with its raw result lines>
Not run: <every selected check that did not execute, and why, or none>
Reviewers: <non-author reviewers and their verdicts, or pending cross-model review>
```

# Rubric-style reviewing

When you are asked to make a code/QA review/testing, start by listing in the form of tasks or a TODO table in markdown all tests you are going to make according to the guidelines above. Then, spawn one agent per testing task. You should expect the user to specify to you the model they wanna use; if not, assume you shall use the latest GPT Sol model on high or the latest Claude Opus model on high, depending on whether you're Codex/GPT or Claude Code. The adversarial tester that writes new tests runs on a different model from the implementer. Each subagent shouldn't have to do TOO much but not TOO little either: the goal is to keep mid-size tasks done by subagents to limit context window usage of the orchestration agent (you).

Each test subagent dispatches its own GitHub Actions run with its own request ID, and cancels or reruns only runs it dispatched. Try to spawn as many subagents as possible without interfering with the tests of each other. E.g., if Test 2a has to wait for Test 1a to be done, then don't run a subagent for Test 2a until Test 1a is done; but if Test 1a and Test 1b, and 1c and 1d... can be done in parallel, spawn a subagent for each.

On `references/example.md` you shall find developer test prompts as example lists of relevant tests grouped by use case. Consult the matching use case when choosing tests for a review
