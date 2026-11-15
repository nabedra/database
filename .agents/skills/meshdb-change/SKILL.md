---
name: meshdb-change
description: Always use
---

When you are given a task or asked to make a change, do the following:

1. Make sure your thread has a worktree for itself such that different threads can work on the same codebase without interrupting each other
2. For the thread, create a branch in git called `meshdb/<task-title-name>` and continue the work of your thread in that branch and worktree
3. Always when you are done with the implementation, tell the user you are ready to run the code review (he might want you to add more features). If he asks for the review, run a deep code review/QA using the `code-review` skill of this repo
4. When you are done, file a PR in GitHub

To file the PR, use the `file-pr` skill in this repo. Include before and after pictures or videos for UI changes and give a concise description of the problem and solution of the PR

Also always include screenshots and videos of the functional implementation of logic, or other methods to visualize the feature (diagrams, charts, etc.) and efficiency and time benchmarks (jitter, lag, time-to-first-token, results from the tests for scaling or load scenarios...)