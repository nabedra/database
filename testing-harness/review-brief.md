# Cross-model review and merge brief (wave 1)

You review one PR that a different model wrote, fix the findings, and merge it.

1. Read the common brief: `common-brief.md` in this directory. Read the PR, its task report (given to you) and
   the plan sections the task cites (`/Users/antoniopenapena/.claude/plans/your-only-job-is-harmonic-unicorn.md`).
2. Check out the PR branch in your own worktree (`gh pr checkout <n>` inside it). Use the same build rules:
   the shared `CARGO_TARGET_DIR`, `-j 2`, `python3.11`.
3. Review it using `.agents/skills/code-review/SKILL.md` (correctness first), and give separate verdicts on:
   - compliance with the task brief and plan;
   - correctness and code quality;
   - whether every test really fails when its deliverable is broken. Run at least one planted bug yourself:
     edit the code locally, confirm the test fails, then revert.
   - ownership: the PR may touch only its owned paths, plus the allowed exceptions.
4. Fix blocking and important findings yourself, in commits on the PR branch (`[testing] review: ...`). Add the
   trailer `Co-Authored-By: <your model> <noreply@...>`. List the nits you did not fix in the PR comment.
5. Run the PR's own checks locally again. Rebase on `origin/main` if it conflicts.
   - For Rust PRs, a PR gate run starts when you push. Wait for it with
     `gh run watch <id> --interval 90 --exit-status` in the background, and read the raw `test result:` lines.
     If a failure is caused by the PR, fix it. If it is pre-existing on main or a flake, record it with the run
     link and continue.
   - Docs-only PRs keep `[skip ci]` and need no gate.
6. Merge when your verdicts are "approve": `gh pr merge <n> --merge --delete-branch`.
   - Before merging, edit the PR body so the `Reviewers:` line names you (model) and the verdict.
   - Post one review comment with your verdicts, the evidence and the deferred nits.
   - If a blocking finding remains that you cannot fix by 21:30 Madrid, do not merge. Report it instead.
7. The final report covers:
   - the verdicts;
   - the fixes, with their commits;
   - the planted-bug result;
   - the CI links with raw lines;
   - the merge SHA, or why there was no merge.

   The last line is `PR <URL>, head <SHA>, checks and reviews <state>.`

**Never end your turn while waiting on a background command.** Nothing reliably wakes you. Run builds, tests and `gh run watch` in the foreground with a perl alarm timeout, then keep going until your final report.
