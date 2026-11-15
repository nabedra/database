# Runtime integration milestone

The local HTTP-store, supervised API and two manual-worker path is implemented.
This is a core runnable milestone, not completion of the distributed-node plan.
Existing component work and its independent certificates remain separate from
process evidence.

## Implementation

`cluster/runtime.rs` retains startup material and the connected HTTP backend,
initializes/attaches catalog and control state, acquires controller ownership
before announcing ready, renews the lease, verifies enrollment and signed
readiness, observes worker heartbeats, and signs operation forwarding. A new API
needs advancing heartbeat observations before routing becomes available.

`node/runtime.rs`, `node/http.rs` and `node/coordinator.rs` connect manual worker
bootstrap, retained signing keys, configured initialization, finite payload owners,
authenticated HTTP dispatch and captured partition execution. Self and peer jobs
use the same real TCP protocol. The original checked CONTROL, advanced reader,
source bytes and delivery ownership remain registered through EOF and settlement.
Parsing/signature checks run outside the receiver mutex. Parquet, spool and
storage work run on retained worker pools. Result receipt and publication recheck
the original accepted authority against fresh control state.

Mutations stage verified prepared files, encode the final decision and catalog,
upload immutable objects and publish one captured v3 root. CAS losers recompute
against the newer catalog. Durable decisions retain exact request identity and
terminal response. Transient mutation failures and exhausted contention report
unknown because a different same-ID attempt might still apply. Public routing
failure also reports unknown for a valid mutation, including after API restart.
The acceptance scenario caught a previous incorrect `not_applied` response here.

HTTP ingress bounds connections, header readers, header bytes and execution
admission. The client uses the operation deadline for response headers and a
separate five-second connection timeout. Worker heartbeats and API renewal use
the configured intervals. Worker scratch creation now sits inside the partition
pool cleanup boundary. Signal shutdown stops admission, waits for listener and
executors, stops heartbeat work, drains payload ownership, joins partition/CPU
threads and removes owned scratch. This does not issue an idle certificate.

## Personally executed evidence

All Cargo commands below used existing `server/target` and this environment:

```sh
export CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

- `cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --no-run`
  initially failed on four formerly irrefutable CLI patterns. They now explicitly
  require `Serve`; subsequent Clippy compiled all test targets successfully.
- `cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings`
  passed after fixing the runtime lints and boxing the retained checked-reply
  message. It passed again after the public unknown-outcome correction.
- `cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload -- --test-threads=2`
  passed 104 tests, including checked TCP continuation, malformed EOF, original
  ownership, captured handlers and registered result streaming.
- `cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test cli --test publication --test history --test runtime -- --test-threads=2`
  passed 23 tests. These cover exact history/replay, shared-base races, pending
  original fencing, ambiguous publication, CLI compatibility and detached drain.
- `cargo build --manifest-path server/Cargo.toml --locked --offline --no-default-features --bin meshdb-server`
  passed on the final Rust code in 31.83 seconds.
- `rustfmt --check --edition 2024 --config skip_children=true` on the owned
  runtime/integration source files and affected CLI tests passed.
- `python3 scripts/acceptance_nodes.py` passed on the final Rust binary. It
  checks create/batch/get/collect/count and invalid bearer rejection, actual peer
  and self TCP jobs at both workers, two distinct coordinators capturing the
  same catalog before publication, both writes preserved, a complete successful
  response suppressed at the API TCP front door, exact terminal replay before
  and after API restart, conditional unknown-routing assertions, global filter/order/limit
  and projection, and clean API/worker exit with incomplete worker headers open.
  All fixture processes are waited and TCP handler threads explicitly joined,
  including on Python 3.6 where `ThreadingMixIn.server_close` does not join them.

The earlier reported parent build/process pass was not counted as a personally
executed check. During this implementation turn an expanded process scenario
passed before restart was added. Restart then failed first on missing routing
availability and subsequently exposed the incorrect definite failure described
above. Those failures cleaned up their processes before assertions returned.

## Remaining limits

Only external provisioning, fixed policy and HTTP storage are composed here.
S3 runtime composition, managed callbacks, scaling/admin routes, autoscaling,
worker replacement, simultaneous API replicas, TLS process acceptance and full
ORM supervisor acceptance are unproved or unfinished. There is no aggregate
cloud/distributed completion claim.

The process scenario checks competing mutations on different worker coordinators
with a shared catalog response barrier, complete global results, a complete
response suppressed at a TCP front door, exact same-ID replay before/after API
restart, and clean signal shutdown. It does not exhaust crash timing, malformed
peer traffic, all operations, capacity saturation or long-running computation.
Component tests cover broader ownership and framing cases independently.

The focused GPT-6.1 Sol/high source review passed without critical or important
findings. It confirmed the core process-test claims but noted that the
unknown-routing assertion is conditional: a passing run does not establish that
branch executed. The reviewer did not rerun builds or tests.

A previously reported intermittent worker shutdown failure was not diagnosed in
its original run. Keep it an open limitation unless reproduced with a causal
failure and corrected. A green process exit alone does not resolve its cause.
Positive idle/drain acknowledgment and safe provider termination are unfinished;
the runtime drain endpoint refuses rather than inventing that evidence. Stale
control observations cannot make publication atomic with a separate control-root
CAS, and this milestone does not claim that stronger transaction.

No dependencies, downloads, commits, branches, worktrees, cloud requests or real
credentials were used. Only owned synthetic fixture files and loopback services
were accessed. See the runnable instructions in `server/README.md`.

The minimal plan marks only T09 and T14 complete on this evidence. Larger
acceptance boxes remain open. All initiated Cargo/test/fixture processes and
owned tasks have exited. Rust implementation and Cargo/build ownership are
RELEASED.
