# DN-R01b1: bounded frame-v1 codec

Implemented only the low-level codec from the Execution transport and validation section of `plans/distributed-contracts.md`. Read the shared contract, `node/job.rs` constants/APIs and DN-R01a before implementation. No applicable AGENTS.md was found in the repository, its parent project tree or filesystem ancestors. Applied the test-driven-development and unslop skills. Existing substantial uncommitted work was preserved.

Owned changes are new `server/src/node/frame.rs`, new `server/tests/node_frame.rs`, this note, and a single `pub mod frame;` addition to `server/src/node/mod.rs`. No job, dependency, plan, runtime, configuration, CLI, storage, engine, ORM or script edits.

## API and memory

`FrameKind` is the exact v1 discriminant enum: Control=1, Data=2, PartMeta=3, PayloadEnd=4, StreamEnd=5. Headers are exactly five bytes: kind followed by unsigned u32 big-endian payload length. There is no additional magic or version; the initial CONTROL DTO carries the existing version.

`FrameReader<R: AsyncRead + Unpin>::new(R)` owns the reader. `next_frame(&mut self)` returns `Result<Option<Frame>, FrameError>`. A Frame contains a kind and one owned bounded payload. CONTROL and PART_META must contain 1..=MAX_CONTROL_BYTES bytes, reusing the 16 MiB DTO constant. DATA must contain 1..=MAX_DATA_BYTES bytes, where MAX_DATA_BYTES is 64 KiB. Terminators must have length zero. Header kind/length checks precede any payload allocation or payload read. There is no aggregate buffer, retained previous payload, JSON processing, row/file collection or runtime task in the codec. Callers must consume/drop returned frames instead of collecting a result in memory.

PAYLOAD_END is returned as a frame. STREAM_END is consumed internally; `None` is returned only after a subsequent one-byte read proves actual body EOF. A trailing byte fails immediately. EOF before STREAM_END, including at a clean frame boundary, or in a header/payload is Truncated. The caller must drain through `None` before accepting framing completion. This says nothing about semantic result validity.

`FrameWriter<W: AsyncWrite + Unpin>::new(W)` owns the writer. `write_frame(kind, &[u8])` validates before emitting bytes, then writes a fixed bounded header and the borrowed payload without copying it. Invalid caller arguments emit nothing and leave the writer ready. STREAM_END must use `finish(&mut self)`, which writes that zero-length terminator and flushes. Successful finish is local writer completion only; it cannot prove peer receipt or EOF. Neither finish nor drop calls shutdown. After successful finish, `into_inner(self)` transfers the stream back so the owner can supply actual EOF via write-half shutdown, end of HTTP body or an appropriate transport close. Calling into_inner early returns UseFinish or Poisoned and consumes/drops the writer. No implicit finish occurs on drop.

## State, cancellation and errors

Each codec has explicit Ready, Poisoned and Completed states. Reader next_frame sets Poisoned before its first I/O await. It restores Ready only after a whole non-final frame succeeds, or changes to Completed only after STREAM_END and actual EOF. Writer write_frame sets Poisoned after argument validation and before I/O; it restores Ready only after header and payload succeed. Writer finish sets Poisoned before terminator I/O and changes to Completed only after terminator and flush succeed.

Thus dropping a polled operation while pending, including while waiting for EOF after STREAM_END, leaves Poisoned. I/O/protocol failure on the reader and I/O failure on the writer also leave Poisoned. Further operations reject. Dropping an unpolled future has not started an operation. A cancelled/failed attempt must be abandoned, not resumed. Completed readers and writers return Completed on all subsequent frame/finish operations. Reader EOF confirmation never yields repeated successful None results.

FrameError is a small fixed enum implementing Display/Error with fixed text: UnknownKind, InvalidLength, Truncated, TrailingBytes, Io, Poisoned, Completed and UseFinish. Underlying I/O details and peer text are discarded; no source error or payload is retained. Reader UnexpectedEof maps to Truncated, other I/O failures to Io.

## Focused evidence

Tests were written before the production module. Initial failing command:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_frame
```

Exit 101: E0432, unresolved import `meshdb_server::node::frame`, because the new module/API did not yet exist. Rust also emitted cascading unsized inference diagnostics. This was a compile-time red, not a runtime assertion failure. After implementation the same command passed with no warnings: 5 passed, 0 failed, 0 ignored, 0 measured, 0 filtered out, 0.97s test execution.

Final checks:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test node_frame -- -D warnings
```

Exit 0, no warnings, 5.30s. Every Cargo invocation was sequential, used the existing `server/target`, and had exactly the environment prefix shown above.

```text
rustfmt --edition 2024 --config skip_children=true --check server/src/node/frame.rs server/src/node/mod.rs server/tests/node_frame.rs
git diff --check -- server/src/node/frame.rs server/src/node/mod.rs server/tests/node_frame.rs server/notes/DN-R01b1.md
```

Both exit 0. A separate content whitespace check covers the untracked owned files, which git diff does not inspect.

Five grouped Tokio duplex tests cover binary roundtrip including non-UTF8/NUL, one-byte fragmented headers/payloads, exact inclusive 16 MiB and 64 KiB edges, invalid declarations with the peer still open and no payload, unknown kinds, nonzero terminators, zero data/control/meta, truncated header/payload, missing STREAM_END, trailing bytes, writer validation before output, I/O failure poisoning, explicit finish/shutdown, and rejection after completion. Cancellation tests poll actual duplex operations to Pending after partial header I/O, and after STREAM_END while waiting for EOF, then drop the futures and verify permanent poisoning. No sleep guesses are used; watchdog timeouts bound failures only. Owned concurrent writer tasks are awaited. No giant invalid u32 allocation, aggregate row buffer, DTO matrix duplication or claim of network/multi-process evidence.

## Remaining work and handoff

Bounded I/O does not validate result semantics. R01b2/R03 must enforce request/result sequence and identity, Get/Collect JSON arrays, metadata preparation completion, actual payload lengths/digests, spool/disk budgets, authentication, HTTP adapters, deadlines and transport shutdown/body EOF. This codec deliberately accepts any sequence of individually valid non-final kinds before STREAM_END. The caller must validate that sequence and all payload semantics before accepting an attempt. Independent review remains parent-owned.

Independent specification and code-quality reviews passed without actionable
important findings. Both reused the recorded checks without redundant builds or
tests. This completes the low-level codec slice, not network/process acceptance.

No full/default-AWS suite, cloud calls, .env reads, delegates, branches/worktrees, commits or pushes were used. Cargo ownership is released on delivery.
