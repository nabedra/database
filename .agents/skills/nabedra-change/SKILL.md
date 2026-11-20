---
name: nabedra-change
description: Always use
---

When you are given a task or asked to make a change, do the following:

1. Make sure your thread has a worktree for itself such that different threads can work on the same codebase without interrupting each other
2. For the thread, create a branch in git called `nabedra/<task-title-name>` and continue the work of your thread in that branch and worktree
3. Always when you are done with the implementation, tell the user you are ready to run the code review (he might want you to add more features). If he asks for the review, run a deep code review/QA using the `code-review` skill of this repo
4. When you are done, file a PR in GitHub

Use the [Nabedra builds skill](../nabedra-builds/SKILL.md) to choose where to
run builds and expensive checks. Apply it when coordinating concurrent agents.

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
