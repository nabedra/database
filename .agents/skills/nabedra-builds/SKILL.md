---
name: nabedra-builds
description: Decide where Nabedra builds and expensive verification run. Offload full server builds, Docker images, full Linux suites, acceptance, crash campaigns and concurrent agent builds to free GitHub Actions runners; keep quick checks local.
---

# Nabedra builds

Use standard GitHub-hosted Linux runners for builds and verification that are
slow or resource-heavy on this machine. The repository is public, so they cost
nothing. The limits are 20 concurrent jobs, the 10 GB cache and the shared API
rate limit. Runs start only by dispatch, pinned to a commit.
[GitHub Actions for checks](../code-review/references/gh-actions.md) covers dispatching,
finding, waiting on, rerunning and recording runs, and the quarantine.

Keep these local:
- incremental `cargo check --lib`;
- one filtered test on an already built test binary;
- short `nabedra-kv` runs and Events tests;
- a single acceptance scenario on an already built binary
  (`scripts/acceptance_oltp.py --scenario <ID> --test-binary <exe>`).

When worktrees share `CARGO_TARGET_DIR`, Cargo can link a stale path-dependency
rlib from another worktree, so touch the dependency's sources or use a private
target directory for small dev-only crates.

Send these to Actions:
- the full OLTP library suite, the integration tests and acceptance categories;
- crash campaigns, Docker and anything that rebuilds the server.

Do not build through the local acceptance lane. Keep deployments and
comparative performance benchmarks on their designated infrastructure. AWS
infrastructure is user-managed, and agent use of AWS is limited to authorized
S3 work. Keep those tests small.

- When a change needs a Docker build, packaging or image acceptance checks, run
  them on Actions (`-f jobs=docker`). Build and test each image in the same
  runner job. Use the run logs and acceptance artifacts for evidence. Export an
  image only when another environment needs to run it.
- Keep independent OLTP and Events jobs separate. Put long crash campaigns and
  full Linux acceptance in opt-in jobs. Build once and share the result: the
  workflow's `build` job uploads the debug OLTP server and test executables,
  and the jobs that test them download it. Cap Cargo and test concurrency to
  fit the runner. Upload compact logs, test evidence and requested binaries,
  with short retention for binaries.
- The compiler is pinned in the repository-root `rust-toolchain.toml` (1.94.1,
  the same as CI and the Docker build images). Keep the workflow's
  `RUST_TOOLCHAIN` equal to it. Cargo caches are keyed by toolchain and
  manifests. They are saved only from dispatches on `main`, right after each
  job's build step and without the workspace crates' outputs. Every other
  branch restores from `main` and never saves. Cargo `--timings` reports for
  the OLTP build and both images are uploaded as run artifacts.
- When coordinating agents, move queued heavy work and the next heavy reruns to
  Actions. Let healthy local runs near completion finish. Move a stalled or
  resource-starved local run once its inputs and the equivalent CI job are
  ready. Continue independent work while CI runs. Run final acceptance on the
  combined commit.
