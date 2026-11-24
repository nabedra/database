# Fast builds and test feedback

A one-line change in `server/src/oltp` takes 24 minutes to reach a runnable test binary with the settings agents use today on the shared development Mac, and 44 minutes to reach a local Docker image. Incremental builds with line-table debug info cut the first number to about 1.5 minutes, and a GitHub Actions runner builds the image in 7.4 minutes. This plan moves heavy builds and full suites to Actions, makes the local edit loop incremental, and removes the dependencies that cost the most.

Status: measured 2026-11-20 at `b2196d2` (local) and in Actions runs `44707640355` (`7f1e328`) and `44711723599` (`fb8187f`). Nothing here is implemented. RocksDB is already gone. Every number below says where it comes from and the load it ran under. Lines marked "estimate" give their method. Phase 8 (2026-11-24) did BLD9 and measured BLD6 to BLD10 on Actions; see [measured results](#bld6-to-bld10-measured-results).

## Decisions already made

| Topic | Decision | Source |
| --- | --- | --- |
| Where heavy work runs | Docker builds, image acceptance, full Linux suites and crash campaigns run on standard GitHub-hosted runners. Quick checks, focused tests and short `nabedra-kv` runs stay local. | User, `.agents/skills/nabedra-builds/SKILL.md` (`fb8187f`) |
| Docker | Only on Actions. Each image is built and tested in its own runner job and exported only when another environment needs it. | User, `fb8187f` |
| Crypto | Never in-house. TLS and hashing stay on vetted crates. | User, 2026-11-19 |
| Dependencies | Prefer in-tree code over heavy crates. | User, 2026-11-19 |

## User decisions (2026-11-20)

1. BLD5 approved: `ring` as the only TLS provider is acceptable.
2. BLD4 rejected: AWS SDKs (`aws-sdk-s3`, `aws-config`, boto3) are permitted and encouraged, so S3 stays on the SDK. Its compile cost only affects S3-enabled builds; CI absorbs it.
3. AWS credentials for authorized, small S3 tests come from the git-ignored `.env` in the main checkout.

---

## Measured starting point

Local runs used an 8-CPU x86_64 Mac with 16 GB, rustc 1.99.1 and 4 Cargo jobs, shared with 3 to 5 other agents. CI runs used `ubuntu-24.04` with 4 vCPU, rustc 1.94.1, `CARGO_BUILD_JOBS=4`, `CARGO_INCREMENTAL=0` and debug info 0.

| Measurement | Local (load average, 1 min) | Actions |
| --- | --- | --- |
| OLTP image, cold Docker build | 43m49s at 2 jobs (NS12) | 7m24s; cargo 7m04s, dependencies done at 58 s; 237 MB |
| Events image, cold Docker build | 13m08s at 2 jobs (NS12) | 2m00s; cargo 1m37s; 109 MB |
| Clean release `--features oltp-backup-s3`, `--timings` | 35m22s (load 9 to 62, median 16) | not run |
| `cargo check --bin nabedra-db`, cold, per engine (one job, shared deps) | not run | olap,s3 54 s, oltp 49 s, oltp-backup-s3 53 s, cache 12 s |
| Same, warm dependency cache | not run | olap 11 s, oltp 47 s, oltp-backup-s3 47 s, cache 2 s |
| `cargo test --features oltp --lib --tests --no-run`, warm deps, cold workspace crates | 27m27s (load 9 to 35) | 3m15s with cold deps (deps 53 s, workspace crates 140 s) |
| One-line edit in `src/oltp/pool.rs`, then `cargo check --lib --tests`, incremental off | 3m41s (load 10 to 12) | not run |
| Same edit, then `cargo test --lib --tests --no-run`, incremental off | 24m17s (load 9 to 20) | not run |
| Same edit, incremental on: `check --lib --tests` / `test --lib --tests --no-run` | 43 s / 7m55s (load 6 to 10) | not run |
| Same edit, incremental on, server crates at `debug = "line-tables-only"`: `test --lib --no-run` | 96 s and 87 s on two edits (load 5 to 7) | not run |
| OLTP library suite | about 400 s (ledger) | 91 s: 384 pass, 2 fail, 11 ignored |
| Events build and tests | not run | cold 72 s, warm 20 s |
| `nabedra-kv` tests | not run | 67 s |

Where the time goes:

- **The server crate.** `nabedra-database` is one library of 262k lines, and the OLTP build compiles about 172k of them: `oltp_replication` 77k, `oltp_runtime` 47k, `oltp` 37k and shared modules. In the local release build the library took 956 s (front end 311 s, codegen 645 s) and the `nabedra-db` binary another 437 s. That is 66% of the wall time. In debug the library's unit-test target is the critical path: 1,644 s cold and 1,443 s after a one-line edit, while the plain library compiles in parallel (1,060 s and 1,216 s). On Actions the server crate is 86% of the OLTP image build.
- **The shared machine.** Load averages of 9 to 62 make local builds far slower than a 4-vCPU runner. The server crate's test build takes 140 s on CI and 27 min locally, although part of that gap is CI's debug info 0. The three local edit-loop configurations ran at different loads (A 9 to 20, B 6 to 10, C 5 to 7), so their ratios include some load effect.
- **Incremental off.** Agents run with `CARGO_INCREMENTAL=0` to save disk. That costs 3 to 15 times more per edit than incremental builds in the table above. The cost of incremental is disk: 11 GB for a `--lib --tests` loop with full debug info, or 3.2 GB for a `--lib` loop with line tables.
- **Dependencies.** The release critical path ran from `pkg-config` through the `aws-lc-sys` build script (322 s), `aws-lc-rs`, `rustls`, `hyper-rustls`, `aws-smithy-http-client`, `aws-smithy-runtime` and `aws-runtime` to `aws-sdk-s3` (272 s). The library could only start at 729 s.
- **Docker.** The root `Dockerfile` copies all of `server/` before `cargo build`. It has no dependency layer and no cache mount, so any change, including to docs, rebuilds every crate.

Dependency weight, measured with `cargo tree -e normal,build` (unique crates) and summed `--timings` unit time from the local release build (4,105 s in total):

| Build or subtree | Crates | Summed unit time | Notes |
| --- | --- | --- | --- |
| `oltp` / `oltp-backup-s3` / `olap,s3` / `cache` / Events | 163 / 276 / 258 / 167 / 197 | | The default OLTP image uses `oltp`, without S3 |
| `aws-sdk-s3` + `aws-config`, only theirs | 103 (108 in olap,s3) | 1,282 s (31%) | `aws-sdk-s3` 272 s, `aws-smithy-http-client` 144 s, `sts`/`sso`/`ssooidc` 207 s |
| `aws-lc-rs` and its build tools, only theirs | 6 | 356 s (9%) | `aws-lc-sys` build script 322 s, the longest dependency unit |
| `openraft` 0.10, only its own | 38 (48 in oltp) | 174 s (4%) | `openraft` 55 s; pulls `chrono`, `chrono-tz`, `phf`, `rand` 0.8, `sha2` 0.10 |
| arrow and parquet | 21, OLAP only | | none in the `oltp` or `cache` trees |
| Proc-macro crates (`syn` 2 and 3, `serde_derive`, `clap_derive`, ...) | 17 | 173 s (4%) | expansion inside the server crate not measured |
| `nabedra-database` library + binary | 1 | 1,393 s (34%) | |

Duplicate versions in `oltp`: `sha2` 0.10/0.11 (via `openraft`'s `quorum-set`), `digest`, `rand` 0.8/0.10, `getrandom` 0.2/0.4, `syn` 2/3. `oltp-backup-s3` adds `hyper` 0.14/1, `http` 0.2/1, `h2` 0.3/0.4, `rustls` 0.21/0.23 and `tokio-rustls` 0.24/0.26, all from the AWS SDK's legacy connector. Events adds `base64` 0.22/0.23 and `thiserror` 1/2.

---

## CI split

Actions is the source of truth for full suites. It has already caught Linux-only failures that local macOS runs miss: the durability fixture that only compiles on Linux (`34ce7f4`), O05 thread-exit lag read from `/proc`, and timing-shifted NS13 crash points (`8641cc1`). The repository is public, so standard-runner minutes are free. Caches are capped at 10 GB per repository.

| Tier | Where | Budget | Contents |
| --- | --- | --- | --- |
| T0 edit loop | Local | under 1 min | Incremental `cargo check --features oltp --lib`, `cargo test -p nabedra-kv <filter>`, one test filter on an already built test binary |
| T1 focused | Local | under 5 min | Incremental `cargo test --features oltp --lib <module filter>` (build `--lib` only: `--tests` also builds the plain library and 6 integration binaries), Events tests |
| T2 base (dispatch or PR gate) | Actions, parallel jobs on one shared build | under 8 min wall | `kv` (clippy, tests), `features` (O02, NS26, `cargo check` per engine), `oltp` (lib suite + integration), `events` |
| T3 on request | Actions, dispatch with `jobs=docker`, `campaign`, `acceptance`, `direct`, or `scenarios` / `tests` | 3 to 240 min | Both images plus Events acceptance (NS27), release crash campaign, Linux OLTP acceptance |
| T4 designated | Benchmark infrastructure | | NS11 and the ScyllaDB comparison. Not on shared runners. |

Load-sensitive tests run alone when they fail locally. The full suite runs on Actions. Current list (ledger): `o14_c07_m02`, `o05_o28_typed_mtls_snapshot`, `o06_o16_actual_learner_replacement`, `q03_r05_mtls_probe`, `o03_o04_native_query_plan`, D07, and the `typed_log` cancellation tests (15 s watchdog).

`scripts/acceptance_oltp.py` sets `CARGO_INCREMENTAL=0`, debug info 0 and `CARGO_BUILD_JOBS=2`, and holds an `flock` lane. Its default target is the main checkout's shared `server/target`, so the lane stops worktrees from building into one directory at the same time, and incremental off keeps that shared directory from filling with per-source incremental caches. `RUST_TEST_THREADS=1` and `TOKIO_WORKER_THREADS=2` keep the resource assertions stable. On a dedicated runner the lane is uncontended and costs nothing. Since 2026-11-22 agents do not build through the lane: CI passes `--test-binary` from its shared build, and local runs use one `--scenario` on an already built binary.

Fixes already made by the lead: `shell: bash` with pipefail and `--no-fail-fast`, so `cargo test | tee` fails the step (`762d0df`), and the Events acceptance output path (`9af7bdb`).

Cache budget, measured from the repository's cache list on 2026-11-20 (compressed sizes):

| Cache | Size | Plan |
| --- | --- | --- |
| `kv` | 29 MiB | keep |
| `events` | 189 MiB | keep |
| `features` | 264 MiB | keep |
| `oltp` (1.3 GB uncompressed) | 312 MiB | delete the workspace crates' outputs before saving. They are stale on the next commit. |
| `campaign` | registry only | add the release dependency target, estimated 300 MiB |
| Docker `type=gha` dependency layers | none | estimated 300 MiB per image if BLD3 adds them |
| Total per branch | about 0.8 GB today, 2.3 GB used | |

GitHub lets a run restore caches only from its own branch and the default branch. Each `rr-*` task branch therefore starts cold and saves its own copy of identical keys. About 12 task branches fill the 10 GB, after which GitHub evicts the least recently used entries. Since 2026-11-22 caches are saved only from dispatches on `main`, and runs are started only by dispatch ([GitHub Actions for checks](../.agents/skills/code-review/references/gh-actions.md)).

---

## Tasks

Ranked by the time a developer waits. The savings come from the tables above unless marked "estimate".

| ID | Task | Expected saving | Owned paths | Risk | Acceptance check |
| --- | --- | --- | --- | --- | --- |
| BLD1 | CI as the source of truth (details below) | Test build 27 min local to 3m15s; library suite 400 s to 91 s; images 44 and 13 min to 7m24s and 2m00s | `.github/workflows/nabedra-build.yml`, `rust-toolchain.toml` (new, repository root), `.agents/skills/nabedra-builds/SKILL.md`, `scripts/acceptance_oltp.py` | Low | T2 finishes in under 8 min wall on a push with warm caches. Total cache use stays under 4 GB with 5 task branches. |
| BLD2 | Local dev profile and incremental builds (details below) | Edit plus check from 221 s to 43 s; edit plus test build from 1,457 s to 87 to 96 s (measured, different loads) | `server/Cargo.toml` `[profile.*]`, `server/events/Cargo.toml`, ledger and skill guidance | Low | A one-line edit in `server/src/oltp`, then `cargo test --features oltp --lib --no-run`, finishes in under 2 min at load average under 10 |
| BLD3 | Faster CI images (details below) | Up to 58 s of 444 s cold from a dependency layer; about 100 MB smaller OLTP image (estimate) | `Dockerfile`, `.dockerignore`, `server/events/deploy/Dockerfile`, workflow `docker-*` jobs | Low | A one-line change in `server/src/oltp` rebuilds the OLTP image on Actions in under 7 min. `ldd` lists only `libc`, `libm`, `libgcc_s`. |
| BLD4 | In-tree S3 client to replace `aws-sdk-s3` and `aws-config` (details below) | 103 crates and 31% of release CPU time. The critical path shortens by about 250 s locally (estimate: the library could start once `openraft` and `clap` finish, about 480 s instead of 729 s). No effect on the default OLTP image, which builds without S3. | `server/src/storage/s3.rs`, `server/oltp-replication/src/oltp_backup_repository/s3.rs`, new `server/src/s3/` | Medium: security-critical | `cargo tree` for `oltp-backup-s3` and `olap,s3` has no `aws-*` crate. The SigV4 test vectors pass. The `oltp_backup_repository` S3 contract tests and the live-context probe pass against real S3 with explicit authorization. |
| BLD5 | `ring` as the only TLS provider (details below) | 6 crates, 356 s of release CPU time, and the `cmake` build tool. Cold builds only. | `server/Cargo.toml`, `server/events/Cargo.toml`, the 9 `aws_lc_rs::default_provider()` call sites | Low code risk; needs the user's decision | No `aws-lc-*` or `cmake` crate in any engine or in Events (add both to `FORBIDDEN_NATIVE` in `scripts/check_engine_features.py`). The mTLS, gateway and Events TLS tests pass. |
| BLD6 | Find out why the server crate is slow to compile | Measured baseline at the pre-split `53a5716f` (see [measured results](#bld6-to-bld10-measured-results)): every OLTP edit rebuilt the one server crate, whose unit took 69.3 s of the 70.6 s cold OLTP library build and 104.5 s of the 109.3 s cold test build | report only | None | A report that maps the module cycles, ranks codegen by function (`cargo llvm-lines` on a CI runner, not a project dependency), and explains why the release binary takes 437 s. The rebuild sets and per-unit times are done; `cargo llvm-lines` is not run. |
| BLD7 | One OLAP integration-test binary instead of 33 | Baseline measured: 39 OLAP (`olap,s3`) test targets at `53a5716f` and after the split. The saving is not measured until BLD7 lands. | `server/tests/*.rs`, `server/Cargo.toml` `[[test]]` | Low | `cargo test --no-run` for the default features produces 1 OLAP integration binary. The test counts match before and after. |
| BLD8 | Events dependency trims | Small: Events builds in 67 s debug and 97 s release on CI. Its cold library unit measured 8.6 s with warm dependencies, before and after the split. | `server/events/**` | Low | Fewer crates in Events' `cargo tree`. Events tests and NS27 pass. |
| BLD9 | Split the server crate (only if BLD6 justifies it) | Done in phase 8. Measured: an edit in `oltp_runtime` rebuilds only the server crate, and its check falls from 7.7 s to 1.6 s and its test build from 18.8 s to 5.4 s; the cold OLTP library build falls from 70.6 s to 58.5 s (Xeon runner; the ratios repeat on the EPYC runner class) | `server/src/**`, new workspace members | High: wide refactor | Done: see [measured results](#bld6-to-bld10-measured-results) |
| BLD10 | Small dependency cleanup | `tracing-attributes` (7 s of release CPU time). `syn` 2 stays, because `openraft`'s `derive_more` needs it. Measured duplicate list: 15 packages in each of `oltp`, `olap,s3` and `cache`, unchanged by the split. | `server/Cargo.toml` | Low | `tracing` with `default-features = false, features = ["std"]` builds. Duplicate list shrinks. |

Not recommended:

- **Replacing `openraft`.** It costs 4% of release CPU time and is not on the critical path.
- **Removing arrow or parquet for OLTP speed.** OLTP and cache builds already exclude them.
- **Release profile changes for production images.** LTO is already off and `codegen-units` is the default 16. An opt-level 1 profile for acceptance-only images could cut the 645 s of library codegen. That is an estimate, so measure it on Actions before adopting it. Benchmarks and published images stay on `release`.

### BLD1: CI as the source of truth

- Add a repository-root `rust-toolchain.toml` (scripts run Cargo from the root with `--manifest-path`) pinning `channel = "1.94.1"` with `components = ["clippy", "rustfmt"]`. Local rustc 1.99.1 passed clippy where CI's 1.94.1 failed (`nonminimal_bool` in `nabedra-kv`, run `44707640355`), and Docker uses `rust:1.94.1-bookworm`.
- Split `actions/cache` into `actions/cache/restore` and `actions/cache/save`. Save right after the build step, so a test failure still leaves a cache (today the failed first OLTP run saved nothing). Save only on a key miss, after deleting the workspace crates' outputs: `target/debug/deps/*nabedra_database_server*`, `target/debug/deps/nabedra_db-*`, `target/debug/.fingerprint/nabedra-*`, and the same for `nabedra_kv`.
- Save caches only from `nabedra/remove-rocksdb`, and from `main` once the workflow is there. `rr-*` branches restore and never save. While the workflow is not on `main`, task branches build their dependencies cold, which costs about 1 min per job.
- Compile the release server library once per T3 job and reuse it within the job, as the skill requires. The image build and the campaign's `kv_crash` each compile it (about 6 min on CI). Measure whether merging them into one job saves that second compile.
- Keep `CARGO_INCREMENTAL=0` and debug info 0 on CI. Every CI build starts from a fresh workspace crate.
- Upload `--timings` HTML as an artifact for the `oltp` and image builds, so later tasks get per-crate CI numbers.

### BLD2: local dev profile

Add to `server/Cargo.toml`:

```toml
[profile.dev]
debug = "line-tables-only"   # backtraces keep file:line; smaller objects and links
[profile.dev.package."*"]
debug = false                # dependencies are not stepped through in a debugger
```

The measured run (config C) set line tables on the workspace crates only and kept full debug info on dependencies. Turning dependency debug info off rebuilds every dependency once, and its gain is an estimate (smaller objects to link). On Linux, where debug info is not split out of the binary as it is on macOS, the gain should be larger. Measure it on CI before keeping it.

- Developers and agents build into a per-worktree target with incremental on (Cargo's default). The `CARGO_INCREMENTAL=0` rule in the ledger exists to save disk. Replace it with: incremental on in the worktree you are editing, `cargo clean -p nabedra-database` when you finish, and APFS clones (`cp -cR`) to seed dependencies. Measured incremental directory sizes: 11 GB for the `--lib --tests` loop with full debug info (library test 4.7 GB, plain library 3.7 GB, two checks 0.8 GB each), and 2.8 GB plus 0.4 GB for the `--lib` loop with line tables.
- Use `--lib` for focused tests. `--tests` also builds the plain library (1,060 to 1,216 s of CPU in parallel) and 6 integration binaries.

### BLD3: faster CI images

- Add `# syntax=docker/dockerfile:1` to the root `Dockerfile`. Copy only `server/Cargo.toml`, `server/Cargo.lock`, `server/kv`, `server/src`, `server/tests` and `server/examples` (Cargo checks that declared `[[test]]` and `[[example]]` paths exist), so docs and Events changes no longer rebuild the image.
- Add a dependency-only layer: copy the manifests, create stub `src/lib.rs`, `src/main.rs`, `kv/src/lib.rs` and the declared test and example files, run `cargo build --release --locked` for the selected features, then copy the real sources. Build with `docker/build-push-action` and `cache-from`/`cache-to: type=gha,mode=max,scope=<image>`, which keeps that layer between runs. On Actions this saves the 58 s of dependencies. The 366 s for the server crate remain until BLD6 or BLD9.
- Add BuildKit cache mounts for `/usr/local/cargo/registry` and `/src/server/target`, as the Events Dockerfile already has. They help only persistent builders, because `type=gha` does not export cache mounts.
- Base the `oltp` target on `debian:bookworm-slim`, like `cache`, if nothing in it needs Python. Check first: the runtime stage installs the Python ORM, and O01 acceptance may run it inside the image.
- Keep compiling inside `rust:1.94.1-bookworm`. A binary built directly on `ubuntu-24.04` (glibc 2.39) does not run on the bookworm runtime image (glibc 2.36).

### BLD4: in-tree S3 client

Scope, from the code at `b2196d2`:

| Use | Operations | Headers and behaviour | Credentials |
| --- | --- | --- | --- |
| OLAP `storage/s3.rs` | `GetObject`, `PutObject` | `If-None-Match: *`, `If-Match: <etag>`; returns the ETag; maps 401/403, 404 (but not `NoSuchBucket`), 409/412; 5 s connect, 30 s read and operation timeouts; retries off; endpoint override cleared | default chain with an optional region; explicit per-request static credentials (`from_context`) |
| OLTP backup `oltp_backup_repository/s3.rs` | `GetObject`, `PutObject` | `If-None-Match: *`, `x-amz-meta-nabedra-sha256`, `Content-Length`; checksums only when required; a localhost HTTP endpoint only in tests | default chain, or explicit static credentials |
| Events | none | Events backups go peer to peer | |

Requirements:

- Virtual-hosted-style HTTPS over the existing `hyper-util` + `hyper-rustls` client. Verify certificates against webpki or native roots. No proxy, no redirects.
- SigV4 with a signed `x-amz-content-sha256` (a full payload hash, not `UNSIGNED-PAYLOAD`), HMAC and SHA-256 from `ring`. Tests use AWS's published SigV4 test suite.
- Parse the XML error body for `Code` only (`NoSuchKey`, `NoSuchBucket`, `AccessDenied`, ...) with a bounded body size.
- Credential sources to decide (open decision 3): static and environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) are simple. A shared profile with static keys is simple. IMDSv2, ECS container credentials and web identity (`AssumeRoleWithWebIdentity`) each need an HTTP call and refresh before expiry. Profile `role_arn`, SSO and `credential_process` are the largest part of `aws-config`. Recommended: support everything except SSO and `credential_process`, and document `aws configure export-credentials` for those two.
- Never log secrets, signatures or session tokens. Refuse clock skew over 5 min with a clear error.

### BLD5: `ring` only

`ring` is already a direct dependency of every engine. `aws-lc-rs` comes from `rustls`, `tokio-rustls` and `hyper-rustls` features in `server/Cargo.toml`, from `axum-server`'s `tls-rustls` feature, from the AWS SDK's `rustls-aws-lc` client (which BLD4 removes), and in Events from `reqwest`'s `rustls` feature and `rustls`' `aws_lc_rs` feature. Switch to `rustls/ring`, `tokio-rustls/ring`, `hyper-rustls/ring`, `axum-server/tls-rustls-ring` and `reqwest/rustls-no-provider`, then change the 9 call sites (`main.rs`, `runtime.rs`, `cluster/startup_material/files.rs`, `cache/cluster/transport.rs`, `cache/config.rs`, `oltp_runtime/{api_process.rs,mod.rs}`, `storage/http.rs`, `oltp_replication/tls.rs`) to `rustls::crypto::ring::default_provider()`. Land it after BLD4, or set the SDK's client to `rustls-ring`. Events gains `ring` (a 28 s build script) and loses `aws-lc-sys`.

### BLD8: Events dependency trims

| Crate | Use | Change |
| --- | --- | --- |
| `fs2` | 1 call, `available_space` | `libc::statvfs`, as in `server/src/oltp/pool/mount.rs` |
| `hmac`, `sha2` 0.10, `subtle` | peer token MAC in `http/auth.rs`, backup digests | `ring::hmac` (`verify` is constant time) and `ring::digest`, after BLD5 |
| `reqwest` | Raft peer client with mTLS, 8 lines in 2 files | `hyper-util` client + `hyper-rustls`, as the server already does |
| `time` | RFC 3339 in 2 files | keep unless a small formatter is shorter |
| `anyhow`, `thiserror`, `tempfile`, `base64` | 70, 2, 13 and 2 uses | keep |
| `openraft` 0.9.25 | Raft | moving to 0.10 matches the server's API. Separate workspaces share no build, so it has no build-time benefit. |

### BLD6 to BLD10: measured results

Phase 8 split the OLTP code into six crates (`nabedra-protocol`, `nabedra-resources`, `nabedra-oltp-types`,
`nabedra-transport`, `nabedra-oltp-storage` and `nabedra-oltp-replication`), with `oltp_runtime` left in the
server crate. `scripts/testing/build_times.py` (the `build-times` check) measured the pre-split base `53a5716f`
against each extraction head on one GitHub Actions runner: 4 vCPU, rustc 1.94.1, `CARGO_BUILD_JOBS=4`. It ran 3
alternating rounds per step and reports the median in seconds. Cold steps build with warm dependencies and cold
workspace crates, incremental off and debug info 0. Edit steps append a constant to one file and rebuild with
incremental on and line-table debug info. Times are recorded, not gated. The rebuild sets are deterministic, and the
check fails if one breaks a phase 8 rule.

| Record | Head | Runner CPU |
| --- | --- | --- |
| P8-G (storage extracted) | `e42f87d3`, [job 138263860899](https://github.com/zPy52/nabedra-database/actions/runs/45340789755/job/138263860899) | AMD EPYC 7763 |
| P8-H (replication extracted, first head) | `f6817e08`, [job 138309942107](https://github.com/zPy52/nabedra-database/actions/runs/45354787718/job/138309942107) | Intel Xeon 6975P-C |
| P8-H (replication extracted, final review head) | `ee387c2b`, [job 138399122333](https://github.com/zPy52/nabedra-database/actions/runs/45380577146/job/138399122333) | Intel Xeon 6975P-C |
| P8-I (candidate head after #106) | `16ced08f`, [job 138511374032](https://github.com/zPy52/nabedra-database/actions/runs/45412853815/job/138511374032) | AMD EPYC 7763 |

The `build-times` record on the final head of PR #109 is linked in that PR, because a commit cannot cite its own run.

Medians, base then head, from the `ee387c2b` and `16ced08f` records. Compare each head with its own base only:
the two records ran on different CPUs.

| Step | Base (Xeon) | `ee387c2b` | Ratio | Base (EPYC) | `16ced08f` | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold OLTP library (`--features oltp --lib`) | 70.6 | 58.5 | 0.83 | 93.8 | 77.7 | 0.83 |
| Cold OLAP library (`olap,s3`) | 14.9 | 14.7 | 0.99 | 19.0 | 19.1 | 1.01 |
| Cold Cache library | 4.8 | 5.2 | 1.09 | 6.4 | 7.1 | 1.10 |
| Cold Events library | 8.6 | 8.6 | 1.00 | 11.0 | 11.5 | 1.05 |
| Cold OLTP test build (server and every OLTP crate, `--lib --tests --no-run`) | 109.3 | 103.3 | 0.94 | 147.3 | 141.4 | 0.96 |
| Edit `oltp_runtime`, then `check --lib` of the server | 7.7 | 1.6 | 0.20 | 9.8 | 2.0 | 0.21 |
| Edit `oltp_runtime`, then `test --lib --no-run` of the server | 18.8 | 5.4 | 0.29 | 21.0 | 6.9 | 0.33 |
| Edit replication (`typed_log.rs`), then check its crate | 7.6 | 3.8 | 0.49 | 9.8 | 4.8 | 0.49 |
| Edit replication, then test build of its crate | 18.5 | 7.6 | 0.41 | 21.2 | 9.5 | 0.44 |
| Edit storage (`pool.rs`), then check its crate | 7.5 | 1.4 | 0.18 | 10.3 | 1.8 | 0.17 |
| Edit storage, then test build of its crate | 18.9 | 2.6 | 0.14 | 21.9 | 3.4 | 0.16 |
| Edit transport (`session.rs`), then check its crate | 7.9 | 0.27 | 0.03 | 9.8 | 0.35 | 0.04 |
| Edit transport, then test build of its crate | 17.8 | 0.65 | 0.04 | 21.0 | 0.86 | 0.04 |
| Edit resources (`memory.rs`), then check its crate | 7.8 | 0.20 | 0.03 | 9.8 | 0.26 | 0.03 |
| Edit resources, then test build of its crate | 18.7 | 0.36 | 0.02 | 21.1 | 0.50 | 0.02 |

A crate edit followed by a check of the whole OLTP server graph took 7.2 s (storage), 5.6 s (replication), 5.8 s
(transport) and 8.3 s (resources) on the `ee387c2b` head, against 7.5 to 7.9 s for any edit on the base. On `16ced08f`
those steps took 9.4, 7.1, 7.2 and 10.6 s against 9.8 to 10.3 s. The `f6817e08` record agrees with `ee387c2b` within
0.9 s on every edit step and 3.2 s on every cold or test-build step. The `e42f87d3` record ran on the EPYC runner class (cold OLTP
library 91.2 s base and 85.2 s head) and predates the replication extraction.

Rebuild sets on the head (the gated result; `nabedra-rebuild-rule: runtime, transport, storage pass` in all four
records):

| Edited file | Rebuilt workspace crates |
| --- | --- |
| `server/src/oltp_runtime/http.rs` | `nabedra-database` |
| `server/transport/src/transport/session.rs` | `nabedra-transport`, `nabedra-oltp-replication`, `nabedra-database` |
| `server/oltp-storage/src/oltp/pool.rs` | `nabedra-oltp-storage`, `nabedra-oltp-replication`, `nabedra-database` |
| `server/oltp-replication/src/oltp_replication/typed_log.rs` | `nabedra-oltp-replication`, `nabedra-database` |
| `server/resources/src/memory.rs` | every OLTP crate and `nabedra-database` |

On the base every one of these edits rebuilt the single server crate.

Per-unit times from the cold OLTP library build (`ee387c2b` medians): the base server crate took 69.3 s. On the head
the server crate takes 27.6 s, replication 32.1 s, storage 6.8 s, OLTP types 2.1 s, `nabedra-kv` 1.6 s, and
transport, protocol, resources and `nabedra-rt` 1.0 s or less each. In the cold test build the server's library test
unit fell from 104.5 s to 39.2 s, and the replication crate's library test unit takes 67.1 s.

Findings:

- BLD9's estimate held for the runtime: an `oltp_runtime` edit no longer recompiles storage or replication.
  Replication is now the largest unit, so a replication edit gains least (about 2x).
- An edit in a lower crate rebuilds the crates above it, as Cargo requires. A `resources` edit rebuilds the whole
  OLTP graph, slightly slower than the base's single crate (8.3 s against 7.8 s).
- The duplicate lists (BLD10) and the OLAP test-target count (BLD7) are the same before and after.
- The Cache library is about 0.4 s slower (0.6 s on the EPYC runner). `nabedra-rt` takes 0.5 s to build on the head
  and took 0.03 s on the base. Its source grew from 1,787 lines to 14,185 over phases 8 to 10, so the comparison
  against `53a5716f` includes more than the split.

---

## Test tiers

| Tier | Command | Time now | Target |
| --- | --- | --- | --- |
| T0 | `cargo check --features oltp --lib` after an edit | 221 s incremental off, 43 s incremental on (`--lib --tests`, local) | under 45 s local |
| T0 | `cargo test -p nabedra-kv` | 67 s CI | under 90 s local |
| T1 | `cargo test --features oltp --lib <filter>` after an edit | build 24 min today, 87 to 96 s with BLD2 (local) | build under 2 min, plus the filtered tests |
| T1 | Events tests | 20 s warm CI | under 1 min local |
| T2 | OLTP build + lib suite + integration | 3m15s + 91 s + integration (CI, cold) | under 8 min |
| T3 | Images + NS27 | 7m24s (OLTP), 2m00s + acceptance (Events) | OLTP under 7 min after BLD3 |
| T3 | Release crash campaign, Linux OLTP acceptance | on request | |
