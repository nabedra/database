# Subagent-driven development in Nabedra

Use this workflow for a large implementation with enough distinct work to
delegate. The lead owns the task breakdown, interfaces, integration order, and
acceptance decisions. Delegate implementation, test harnesses, acceptance
runners, and integration changes as tasks where the plan calls for them.

## Prepare the work

1. Set up the thread's branch and worktree using [nabedra-change](../SKILL.md).
   Read the approved plan, its spec, and applicable repository instructions.
   If the work needs an implementation plan, follow the planning rules in that
   skill before dispatching.
2. Keep a Markdown ledger in an ignored directory such as
   `.nabedra/workflows/<branch-and-plan>/`. Record the plan or task brief, base
   commit, and a TODO table with columns for task ID, dependencies, file or
   resource ownership, acceptance checks, status, and agent or commit. Put task
   briefs and reports beside it when agents can access that directory. After an
   interruption, reconcile the ledger with Git and agent status before dispatching.
3. Check shared files, interfaces, fixtures, and test environments before work
   starts. Assign shared contracts to one owner and record when downstream tasks
   can use them. Resolve routine ambiguities against the spec and record decisions
   that affect other tasks. Keep work that depends on an unresolved requirement
   queued while independent tasks proceed.

For worker execution or recovery changes, follow [AGENTS.md](../../../../AGENTS.md).
Use the [scheduling plan](../../../../plans/resource-aware-scheduling.md) and
[scheduler test contract](../../../../docs/testing/scheduler-contract.md) in
the affected task briefs and acceptance checks. Record the scenario IDs and
actual assertion results. An unexecuted scenario remains unverified.

## Keep independent tasks running

- Dispatch a fresh agent per substantive task. Group repeated mechanical edits
  with the same acceptance check. Give test harnesses and acceptance runs their
  own tasks when they can proceed independently.
- Fill available agent slots with ready work. As agents finish, use the freed
  slots for task reviews or newly unblocked tasks. A large plan can use many
  agents over its lifetime. Active agents, including reviewers, are limited by
  the harness and by the work that can safely run together.
- Serialize writers that share files or use the harness's isolated checkouts
  when parallel work on them makes sense. Review the combined result after
  integrating overlapping changes and before unblocking dependents. For
  concurrent writers with disjoint files in a shared checkout, give each agent
  exclusive paths, leave edits unstaged, and have the lead stage only the
  approved paths for each task. Isolate resource-heavy test runs or schedule them around shared
  CPU, memory, services, and fixtures.
- Use the requested model and settings. Nabedra's default is the latest GPT
  Sol on high for Codex or the latest Claude Opus on high for Claude Code,
  subject to harness availability and dispatch rules. The lead dispatches
  implementations and reviews. Children work directly on their assignment.

Give each agent a brief with the task ID, relevant requirements, agreed
interfaces, owned paths, base revision or checkout, and acceptance checks.
Include any repo rules or earlier decisions that affect the task. Use a file
path when the agent can access it and supply the necessary text otherwise.
Ask the agent to return the status, changed files or commits, commands and
results, and unresolved concerns. Record the plan and report paths in the ledger so
subsequent agents can find the relevant evidence.

## Review each task before integration

1. When the implementer reports completion, give a fresh reviewer the brief,
   implementation report, and task diff. Use the task's recorded base and
   final commit for an isolated checkout. In a shared checkout, scope the diff
   to the task's owned paths, including new files.
2. Ask for separate verdicts on compliance with the brief and code quality.
   Apply the relevant checks from [code-review](../../code-review/SKILL.md),
   including regression coverage. The lead coordinates the review rubric.
   Each reviewer checks the assigned diff and flags any requirement the
   evidence leaves open. Reuse test results from the same revision and run
   targeted checks when evidence is missing or the code changed.
3. If a task needs context or is blocked, identify the missing decision,
   dependency, permission, or resource before retrying. Keep other ready work
   moving. Fix substantive findings and verify the changed behavior. Use a
   scoped re-review for fixes that need independent verification. If a retry
   stalls, narrow the task or resolve the underlying interface or plan issue
   before sending another agent.
4. Record the review verdicts, verified revision, test results, and any
   deferred findings. Mark a task complete after integration and keep
   dependents queued while their required interface or behavior remains
   unverified.

## Review the integrated branch

Once the tasks are integrated, give a fresh reviewer the full diff from the
branch's merge base, the spec, and the ledger's results and deferred findings.
Check the main end-to-end behavior and cross-task regressions using the
applicable [code-review](../../code-review/SKILL.md) rubric. Combine related
fixes and verify the affected behavior on the final revision. Report any
remaining gaps and the decisions that affected scope or behavior.

Follow [nabedra-change](../SKILL.md) for the review handoff and
[file-pr](../../file-pr/SKILL.md) for PR filing when in scope. The lead
handles publication. Keep the ledger, briefs, and generated reports out of
the PR, and include the verification results and applicable evidence.
