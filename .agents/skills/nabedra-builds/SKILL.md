---
name: nabedra-builds
description: Offload Nabedra builds and expensive verification to free GitHub Actions runners when they would slow down the local machine. Use for full server builds, Docker images, full Linux suites, crash campaigns, and concurrent agent builds.
---

# Nabedra builds

Use standard GitHub-hosted Linux runners for builds and verification that are
slow or resource-heavy on this machine. Keep quick checks, focused tests, and
short `nabedra-kv` runs local. GitHub Actions is for building, compiling, and
expensive verification. Keep deployments and comparative performance benchmarks
on their designated infrastructure. AWS infrastructure is user-managed.
Agent use of AWS is limited to authorized S3 work. Keep those tests small.

- When a change needs Docker build, packaging, or image acceptance checks,
  run them on Actions. For the Nabedra pilot, include `[ci docker]` in the
  pushed checkpoint's commit message or select `docker` in a manual dispatch
  once available. Build and test each image in the same runner job. Use the
  run logs and acceptance artifacts for evidence. Export an image only when
  another environment needs to run it.
- Run independent OLTP and Events jobs separately. Put long crash campaigns and
  full Linux acceptance in on-demand jobs. Reuse a build within a job, cap Cargo
  and test concurrency to fit the runner, and cache dependencies by toolchain,
  target, and lockfile. Keep caches within GitHub's free allowance and use
  standard runners. Upload compact logs, test evidence, and requested binaries.
- The compiler is pinned in the repository-root `rust-toolchain.toml` (1.94.1,
  the same as CI and the Docker build images); keep the workflow's
  `RUST_TOOLCHAIN` equal to it. Caches are saved only from the integration
  branch (and `main` once the workflow is there), right after each job's build
  step and without the workspace crates' outputs. Task branches restore and
  never save, so their dependency builds start cold until the workflow is on
  `main`. Cargo `--timings` reports for the OLTP test build and both images are
  uploaded as run artifacts.
- Push an authorized checkpoint and launch the relevant workflow for that
  revision. If the workflow is new and only on the task branch, pilot it with a
  push trigger scoped to that branch. Manual dispatch becomes available once the
  workflow exists on the default branch. Cancel runs superseded on the same
  branch without canceling runs on other agents' branches.
- When coordinating agents, move queued heavy work and the next heavy reruns to
  Actions. Let healthy local runs near completion finish. Move a stalled or
  resource-starved local run once its inputs and equivalent CI job are ready.
  Continue independent work while CI runs.
- Use `gh run list --commit <sha>` to find the run and
  `gh run view <run-id> --log-failed` to inspect failures. Check the tested
  revision and record the run URL, the assertions that ran, and the result.
  Run final acceptance on the combined commit.
