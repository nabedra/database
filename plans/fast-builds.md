# Fast builds and test feedback

A one-line change in `server/src/oltp` takes 24 minutes to reach a runnable test binary with the settings agents use today on the shared development Mac, and 44 minutes to reach a local Docker image. Incremental builds with line-table debug info cut the first number to about 1.5 minutes, and a GitHub Actions runner builds the image in 7.4 minutes. This plan moves heavy builds and full suites to Actions, makes the local edit loop incremental, and removes the dependencies that cost the most.

Status: measured 2026-11-20 at `b2196d2` (local) and in Actions runs `44707640355` (`7f1e328`) and `44711723599` (`fb8187f`). Nothing here is implemented. RocksDB is already gone. Every number below says where it comes from and the load it ran under. Lines marked "estimate" give their method.

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
| BLD4 | In-tree S3 client to replace `aws-sdk-s3` and `aws-config` (details below) | 103 crates and 31% of release CPU time. The critical path shortens by about 250 s locally (estimate: the library could start once `openraft` and `clap` finish, about 480 s instead of 729 s). No effect on the default OLTP image, which builds without S3. | `server/src/storage/s3.rs`, `server/src/oltp_backup_repository/s3.rs`, new `server/src/s3/` | Medium: security-critical | `cargo tree` for `oltp-backup-s3` and `olap,s3` has no `aws-*` crate. The SigV4 test vectors pass. The `oltp_backup_repository` S3 contract tests and the live-context probe pass against real S3 with explicit authorization. |
| BLD5 | `ring` as the only TLS provider (details below) | 6 crates, 356 s of release CPU time, and the `cmake` build tool. Cold builds only. | `server/Cargo.toml`, `server/events/Cargo.toml`, the 9 `aws_lc_rs::default_provider()` call sites | Low code risk; needs the user's decision | No `aws-lc-*` or `cmake` crate in any engine or in Events (add both to `FORBIDDEN_NATIVE` in `scripts/check_engine_features.py`). The mTLS, gateway and Events TLS tests pass. |
| BLD6 | Find out why the server crate is slow to compile | Decides whether BLD9 is worth doing | report only | None | A report that maps the module cycles, ranks codegen by function (`cargo llvm-lines` on a CI runner, not a project dependency), and explains why the release binary takes 437 s |
| BLD7 | One OLAP integration-test binary instead of 33 | About 32 links of 5 to 22 s each, so 1.5 to 2 min wall at 4 jobs (estimate from the OLTP integration binaries' link times) | `server/tests/*.rs`, `server/Cargo.toml` `[[test]]` | Low | `cargo test --no-run` for the default features produces 1 OLAP integration binary. The test counts match before and after. |
| BLD8 | Events dependency trims | Small: Events builds in 67 s debug and 97 s release on CI | `server/events/**` | Low | Fewer crates in Events' `cargo tree`. Events tests and NS27 pass. |
| BLD9 | Split the server crate (only if BLD6 justifies it) | Estimate: an edit in `oltp_runtime` recompiles 47k lines plus its dependents instead of 172k | `server/src/**`, new workspace members | High: wide refactor | Follows from BLD6 |
| BLD10 | Small dependency cleanup | `tracing-attributes` (7 s of release CPU time). `syn` 2 stays, because `openraft`'s `derive_more` needs it. | `server/Cargo.toml` | Low | `tracing` with `default-features = false, features = ["std"]` builds. Duplicate list shrinks. |

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
