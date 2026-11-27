---
name: nabedra-change
description: Always use
---

When you are given a task or asked to make a change, do the following:

1. Make sure your thread has a worktree for itself such that different threads can work on the same codebase without interrupting each other
2. For the thread, create a branch in git called `nabedra/<task-title-name>` and continue the work of your thread in that branch and worktree
3. Before implementing a plan that changes a topology or a protocol, or touches an L2 or L3 module, run the `design-review` skill of this repo. Implement only after its [gate](../design-review/SKILL.md#gate-implementation) holds. The plan, the spec and the implementation share the branch and the PR. A change to `specs/` is not docs-only.
4. Always when you are done with the implementation, tell the user you are ready to run the code review (he might want you to add more features). If he asks for the review, run a deep code review/QA using the `code-review` skill of this repo
5. When you are done, file a PR in GitHub
6. Review and verify changes during development.
   - For docs-only changes, review the diff, examples and links locally. Skip
     tests and GitHub Actions. The author can merge after the local review.
   - For other changes, review the diff and run applicable local checks. Manually dispatch
     focused Actions checks on the PR head when useful during development. Merge after the
     applicable [review](../code-review/SKILL.md#verification-rules), then run broad Actions verification once on the resulting `main` SHA
     after a batch of PRs merges.
7. After the merge, retarget PRs stacked on the branch (`gh pr edit <n> --base main`), or GitHub closes them when the branch goes. Remove the PR branch and worktree if they are no longer needed. Then run `git pull` on the base branch.

Use the [Nabedra builds skill](../nabedra-builds/SKILL.md) to choose where to
run builds and expensive checks. Apply it when coordinating concurrent agents.

When the user asks to release, publish, tag or ship a version, or whether a release is due,
follow [Releasing Nabedra](references/releasing.md). Never publish outside that process.

When agents work in parallel on one codebase, every hot file has a single owner
per wave. See the hot-file rule in the
[subagent-driven development workflow](references/subagent-driven-development.md#prepare-the-work).

# Keep CI waits out of Opus

Opus owns decisions about which checks to run, what failures mean, and whether
the evidence supports merging. Use a CI completion event or a standalone
watcher for the wait. If the harness requires an agent, use Codex Luna High
for waiting alone, or Codex Sol when the job includes collecting and
summarizing evidence. Prefer either over Haiku, which is the fallback when
Codex is unavailable.

After dispatch, record the tested SHA, run ID or link, expected checks, and
the decision that needs the result. Continue independent work or end the Opus
turn while CI runs. Use an asynchronous handoff and report once at completion
or timeout. Avoid synchronous delegate calls, sleeps, and status loops in Opus.
A blocking handoff can still expire the prompt cache and force a large rewrite
when Opus resumes, even when a cheaper model does the waiting.

Return the tested SHA, outcome, and relevant failure excerpts or artifact links.
If the Opus context is large, resume in a fresh or compacted context from the
checkpoint and result. Keep the required checks and review gates in force.

# Make trade-offs for long-term speed

Use [long-term speed](../harness-review/references/long-term-speed.md) as
the decision rule for ordinary changes. Optimize for finished work reaching
`main` with less total time spent deciding, building, checking, waiting,
and reworking, within the user's time and cost budgets.

For database changes, judge improvements against the properties they can
affect. When a decision depends on measured performance, evaluate both
sides of the trade-off under the same workload.

- **Performance and reliability.** Measure latency and throughput with the
  required durability, consistency, and recovery guarantees in force.
- **Scalability and performance.** Check how adding nodes or workers changes
  sustainable throughput and tail latency.
- **Performance and resource use.** Seek lower latency or higher goodput
  within the same CPU, memory, disk, and network budget, or the same service
  level with fewer resources.

Preserve ease of use for the SDK, CLI, and API. Make routine, reversible
choices within the task scope and verify the outcome. Bring decisions to
the user when they require approval, change a public contract, or are
costly to undo. See [Decide fast](../harness-review/references/long-term-speed.md#decide-fast).

# Contracts for the change

- For changes to request admission, queues, scheduling, workers, partition
  memory, mutation publication, recovery, or autoscaling, use the
  [scheduler test contract](../../../docs/testing/scheduler-contract.md).
- For OLTP request-path changes, start with the
  [OLTP direct-path plan](../../../plans/oltp-direct-path.md).
- For ORM changes and database changes that affect ORM callers, follow the
  [ORM parity checks](../../../docs/testing/orm-parity.md).

# Feedback tiers

For changes that need runtime checks, use the tiers below and follow the
[testing guide](../../../docs/testing/README.md) and
[verification rules](../code-review/SKILL.md#verification-rules).
`python3 scripts/testing/matrix.py select --base <sha> --head <sha>` picks
the checks for a diff. `python3 scripts/testing/run_check.py --list --tier <tier>`
lists a tier's checks.

| Tier | Where and when | Contents |
| --- | --- | --- |
| T0 | Locally, before each commit | In-process checks of touched modules, each under 10 s. Aim for under 60 s including the incremental build. Simulation uses one thread and about 20 seeds. |
| T1 | On Actions, dispatched on the final SHA before review | The set from `matrix.py select`, the repetition gate for new or changed tests, and paired A/B for hot paths listed in `docs/testing/modules.toml`. Run the Valkey differential when Cache changes. |
| T2 | Manually dispatched on the SHA of `main` | Long seed runs, fuzzing, real S3 and kind suites |
| Weekly | Manually dispatched on `main` | The upgrade and downgrade matrix |
| T3 | Release SHA | Every in-scope matrix cell green on that SHA |

Never start a T2 or T3 run from a task branch. Record what each tier ran as
[evidence](../code-review/references/evidence.md).

# Fixing a bug

Classify the bug, then write a regression at the lowest level that
reproduces it. Use a seed if possible. Confirm the regression fails before
the fix and passes with it. Identify why no check caught the bug, add the
missing invariant, and record it in `docs/testing/bugs/<date>-<slug>.toml`.
Follow the [bug loop](../code-review/references/bug-loop.md).

To file the PR, use the `file-pr` skill in this repo. Include before and after pictures or videos for UI changes and give a concise description of the problem and solution of the PR

Also always include screenshots and videos of the functional implementation of logic, or other methods to visualize the feature (diagrams, charts, etc.) and efficiency and time benchmarks (jitter, lag, time-to-first-token, results from the tests for scaling or load scenarios...)

# Rubric policy for big tasks

For large implementations with independent subtasks, follow the
[subagent-driven development workflow](references/subagent-driven-development.md).
Keep a temporary Markdown TODO table and delegate implementation, test harnesses,
acceptance runners, and integration work. The lead coordinates interfaces, tracks
progress, and assesses results. Handle isolated changes directly.

# For logic implementations
You should expect to receive the input and/or output and/or examples of how the module should look. E.g., an interface and example usage:
```python
from module import database

db = database("https://example.com/abc")
table = db.table("table_example")
table.select(...)
```
If you don't have this, start to make a plan along the developer and seek his approval. The plan should focus on how the input and the output looks; primarily, the usage that the user of the SDK or of the functionality will hae or experience. In the case of an SDK it's the interface with a straight to the point definition of the modules, classes, methods, their arguments, etc. Then it also matters any diagram or functionality like reducing and mapping to take the output of X workers and unify into one, and follow the same procedure again and again until all the data is reduced until a single major worker (e.g.). For this case of tasks include a diagram of the functionality on the plan/during the planning. In general, include visual material when appropriate

Again, this should only be done for big tasks/projects; for isolated, small or medium tasks (like "create a sign up page" or "create this simple module") this plan is not generally required unless asked by the developer

Do NOT file a PR if the only changes you have made in your thread is the creation of a plan. Rather, prompt the user to start the plan either in that thread or in another thread, in which case you shall remove the worktree and branch of the other thread, settle it, and continue in a new branch and worktree. If not, you just continue in the same thread, worktree and branch
