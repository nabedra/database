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
6. For docs-only changes, review the diff, examples and links locally. Skip
   runtime tests and GitHub Actions.
   Put `[skip ci]` in the PR head commit message and in the commit message that
   lands on the base branch. For other changes, run the applicable checks and
   tests, including GitHub Actions dispatched on the PR's head commit when
   appropriate. Wait for the applicable PR gate, fix failures, and rerun
   affected checks. Before merging any PR, confirm that the applicable checks
   pass and the [non-author reviewer](../code-review/SKILL.md#verification-rules)
   approves.
7. After the merge, remove the PR branch and worktree if they are no longer needed. Then run `git pull` on the base branch.

Use the [Nabedra builds skill](../nabedra-builds/SKILL.md) to choose where to
run builds and expensive checks. Apply it when coordinating concurrent agents.
Pushes alone start no CI. Follow the dispatch procedure in
[GitHub Actions for checks](../code-review/references/gh-actions.md).

When agents work in parallel on one codebase, every hot file has a single owner
per wave. See the hot-file rule in the
[subagent-driven development workflow](references/subagent-driven-development.md#prepare-the-work).

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
| T2 | Nightly, scheduled on the SHA of `main` | Long seed runs, fuzzing, real S3 and kind suites |
| Weekly | Scheduled on `main` | The upgrade and downgrade matrix |
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
