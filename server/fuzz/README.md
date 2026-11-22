# Decoder fuzzing

This workspace has its own lockfile. Its default features enable OLAP and Events. Cache uses a
separate build because the server selects exactly one engine. The stable test builds skip the
`libfuzzer` feature and every fuzz binary.

## Replay on stable

Run both commands from `server/fuzz`.

```sh
cargo test --locked -j 2
cargo test --locked -j 2 --no-default-features --features cache
```

Use the main checkout's `server/fuzz/target` as `CARGO_TARGET_DIR` when working in a separate
worktree. Both commands together replay every seed and regression. The `fuzz-replay` check
runs both.

## Run a target on nightly

The `fuzz-nightly` check runs `sh nightly.sh` from this directory. The script runs each target
for 300 seconds by default. The Actions fuzz job can set `RUST_NIGHTLY` and `FUZZ_SECONDS`.
CI installs the nightly toolchain and cargo-fuzz. On a machine with those tools already
installed, use the following command from `server`, with a target and features from the table.

```sh
cargo +nightly fuzz run "$target" --no-default-features --features "$features" -- -max_total_time=300
```

| Target | Features | Decoder and assertion |
| --- | --- | --- |
| `cache_resp` | `cache,libfuzzer` | RESP2 stream decoder. Whole and fragmented input agree, and accepted frames survive a round trip. |
| `cache_command` | `cache,libfuzzer` | Command parser. Input is RESP or a command selector byte followed by NUL-separated arguments. Command name case preserves the result, and `COMMAND GETKEYS` agrees with direct parsing. |
| `events_publish` | `events,olap,libfuzzer` | Single and batch publish JSON. An accepted single event also decodes in a batch. |
| `events_poll` | `events,olap,libfuzzer` | GET query string. Adding an unknown query parameter to an accepted query is rejected. |
| `events_ack` | `events,olap,libfuzzer` | Checkpoint JSON and canonical offset decoding. Appending trailing garbage to accepted JSON is rejected. |
| `events_lease` | `events,olap,libfuzzer` | Claim, renew and release bodies. Appending trailing garbage to an accepted body is rejected. |
| `olap_operation` | `events,olap,libfuzzer` | `/v1/operations` JSON envelope and operation. Accepted requests round-trip when the encoded body fits the size limit. |
| `olap_predicate` | `events,olap,libfuzzer` | Query predicate JSON. Accepted predicates round-trip, including nested nodes and numeric scalar types. |
| `olap_catalog` | `events,olap,libfuzzer` | Cluster catalog JSON. Accepted v3, v4 and v5 records round-trip when the encoded catalog fits the size limit. |

Events uses the actual HTTP router and test grants. Its in-process backend returns 404 at route
resolution, after the wire decoder has accepted the request. The harness checks parsing up to
that point, including body limits. Lease credentials, delivery tokens and database behavior
are outside these decoder checks.

## Keep a regression

cargo-fuzz writes crash inputs under `server/fuzz/artifacts/<target>/`. Minimize a crash from
`server` with the same feature flags as the run.

```sh
cargo +nightly fuzz tmin "$target" "$crash" --no-default-features --features "$features"
```

Copy the minimized bytes to `server/fuzz/regressions/<target>/<name>` and rerun stable replay.
For a crash that still reproduces, give the file a dedicated `#[ignore = "reason and issue"]`
test in `tests/replay.rs` and list that exact path as an exception in the normal replay.
Remove the exception and the ignore attribute when the fix lands.

Keep manually chosen valid and malformed examples in `corpus/<target>/`. Preserve raw bytes,
including RESP line endings. Add generated corpus inputs only when they cover a distinct case.
For command syntax and crash minimization, see the [Rust Fuzz Book](https://rust-fuzz.github.io/book/cargo-fuzz.html).
