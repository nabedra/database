# OLAP streaming Docker evidence

The runs below cover the stream acceptance scenarios and the scheduler assertions named in each result. The checks used Linux x86-64 containers with Rust and Cargo 1.94.1, Python 3.11, and loopback APIs and storage. Process tests ran real API and worker processes against the local conditional object store. [Streaming plan](../../../../plans/olap-streaming.md) · [Scheduler contract](../../scheduler-contract.md)

## Build pins

| Binary or test source | Commit | Binary SHA-256 |
| --- | --- | --- |
| Candidate OLAP build and targeted Rust tests | `b30fc1ff4527b5c83a13b80e9b1e228c89d7d397` | `6ed815f988132840877f55bc9a31f9c2c6fd99d3bb81d8139c1065fed369e4d8` |
| Final modern and mixed process fixture | `10b7b7e9a41815868ab9e2471ed1e90850784880` | Same candidate binary |
| Earlier OLAP build for SDK and ten process assertions | `1678220bbcba49553686d743d00b3e13553e7ed7` | `5ceaedc72bca3d1c49cc27b3b55cec873b88952a6d97390ee806ab193b690eeb` |
| Intermediate OLAP build for mixed versions and recovery | `e94a4f10b99be98de0eb5ed4b7968158bea56586` | `c9b021e673464e508d48f5e54ff20b320e5a90ac97685016c107435f1a9af36d` |
| OLAP and Python regression source, OLTP build and tests | `9c37b68249e033ac4fa4676daecbe77fe124a7ad` | OLTP `315c7e0d0b8ec9de5f398b1b03b5b1f4f3a6b3a56e731d9c6b808a46e0b01169` |
| Older OLAP control binary | `6dfc7134c5da37b6f926b55b7609cb43fb7f490b` | `279a3c2cb6df742b2273523cc2161a8bbd52367f71420dba9e077253cbd8f59c` |

The Docker allocation stayed within the 4 CPU and 8 GB testing budget. The full OLAP regression used 2 CPUs and 4 GiB and peaked at 3.11 GiB. Later build and test runs used 1.5 CPUs and 4 GiB while other Docker jobs ran. The candidate build peaked at 1.34 GiB and the targeted Rust run peaked at 2.13 GiB. The earlier full process run peaked at 83.6 MiB. The final modern and mixed-version processes ran sequentially in one container at 1.5 CPUs and 1.5 GiB with a shared peak of 83.8 MiB. The CPU PyTorch run used 1.5 CPUs and 2 GiB and peaked at 1.38 GiB. These cgroups reported zero OOM events. The numbers are container memory peaks. Worker charged-byte assertions come from the stream tests.

## Stream assertions

| Scenario | Executed evidence |
| --- | --- |
| ST01 | Candidate Rust tests matched partition and row-group replay to serial query results, checked projection integrity, and asserted one-connection local serve (U01). The live SDK run consumed seven pinned rows across a drop and recreate, with the remaining batches holding three and one rows. |
| ST02 | Candidate Rust tests checked the oversize and dropped-tail paths for charge release and cursor stability. Python with PyArrow rejected malformed IPC schema and payload and closed invalid row streams without acknowledging them. |
| ST03 | Candidate Rust tests checked prefetch and delivery windows, close during prefetch, replica byte limits, low control reserve, bounded pinned query decoding, expiry, socket deadlines, and reuse after a successful flush. The candidate process trace recorded 16 paused consumers exhausting admission while accepted streams and an ordinary read progressed (Q02). An ordinary `table.get` completed before the held stream checkpoint acknowledgement was released (C07). |
| ST04 | The final process runs verified same-run replay across APIs (Q05/Q06), a held replica acknowledgement that fenced a concurrent delivery (Q03/Q05), malformed request rejection (U02), and byte-identical recovery from lost replies and a truncated body (Q04/Q06/U02). They distinguished signed old-term requests from altered signatures (R05/U02), resumed recovery from the acknowledged page (R01/Q03), recovered after owner loss before and after replica installation (R01/R06), and abandoned handles after cold startup while reopening committed table data (R04). Candidate Rust tests covered invalid sequences during delivery, stale replica writes after a newer page (R05/Q03), an expired join seal releasing a waiter (F03/R03), recovery of a surviving record, and managed object reauthorization including access revocation and a retryable probe outage. |
| ST05 | Candidate Rust tests verified physical shard seeks and legacy ordering. The earlier live OLAP SDK run with PyArrow 20.0.0 and PyTorch 2.7.1+cpu used two Gloo ranks. Disjoint shards contained 13 and 44 rows, then 0 and 26 with the stricter filter. Their ordered union matched serial results. The fixture checked stable repeats, `drop_last`, and rejection of multi-shard limits. The unchanged ORM exercised all 13 operations at 1, 100, and 1,000 rows against OLAP. The pinned OLTP and older OLAP binaries returned `StreamUnsupportedError` for both client mode preferences. |
| ST06 | The CPU benchmark is in progress. The first attempt timed out while seeding the fixture and produced zero trials. Live GPU timings remain outstanding. |

All 18 distinct targeted Rust tests passed on the candidate. Three of them ran under two selectors and passed both times. The final combined process run passed 13 assertion groups in 263.1 seconds. The mixed-version run passed both groups in 43.0 seconds. A modern coordinator enrolled a worker while a legacy peer stayed ready and ordinary reads continued. The mixed cluster refused a stream, then returned the exact batch on the same run after replacing the legacy worker (ST04/R03).

An earlier combined run of the same candidate passed seven groups in 130.9 seconds before the former owner's replacement missed the 40-second readiness deadline. That fixture supplied only the original enrollment address, which pointed to a standby API after controller takeover. The final fixture supplied both live API URLs. At initial startup, the primary returned HTTP 200. After takeover, the primary returned HTTP 503 `storage_unavailable` and the elected alternate returned HTTP 200. The durable membership then showed the replacement and both surviving workers ready. The final fixture also stopped both APIs before authorizing a cold run. The earlier candidate mixed-version run independently passed in 39.8 seconds.

The earlier OLAP process run completed ten assertion groups in 125.9 seconds. Besides admission, sequence, and reply-loss cases, it observed API-only restart preserving an exact batch (Q04), scan-owner loss before and after replica checkpoint installation (R01/R06), and full worker shutdown abandoning old handles and an unanswered open while committed table data reopened in a new run (R04). On the intermediate binary, the same mixed-version assertions passed in 37.2 seconds. Isolated stale-owner and recovery-deadline runs also passed. The recovery fixture observed saved page-cursor reuse and ordinary read completion while a peer page was held (ST04/R01/Q03). The intermediate combined run stopped after six recorded groups when a restarting worker missed the 40-second readiness deadline. Its isolated restart scenario passed.

## Engine regression and remaining checks

- At `9c37b68` the full OLAP Rust run passed 765 tests. Python passed 183 tests and skipped one Arrow case in the base environment. A separate five-test Python stream run at `3696d63` passed with PyArrow installed, including IPC validation. The default, OLAP-only, OLTP-only, and OLTP with S3 dependency graph checks passed (O02). The OLAP-only dependency graph included Arrow IPC 60.1.0. The OLTP-only graph excluded it. The invalid-feature compile guards were interrupted for the test handoff.
- The OLTP build at `9c37b68` linked a cached native RocksDB archive after checking the exact `librocksdb-sys` lock entry, target and compiler settings, and byte-for-byte matches for six newly compiled object files. The build compiled the Rust crate, wrappers and tests from the pinned source. The OLTP library suite passed 146 of 147 tests. `c07_m02_native_authority_read_progresses_while_query_owns_all_unreserved_memory` panicked on a subtraction underflow in `src/oltp/pool/tests.rs`. Fifteen integration tests passed in four test executables before handoff. The exact older-baseline rerun remains pending.
- The pinned OLTP single-node probe required four workers in its local fixture. All 13 unchanged ORM operations passed at 1 and 100 rows. The 1,000-row `insert_many` exceeded the 20-second SDK deadline and returned `UnknownOutcomeError`. The separate retry measured 20,049 ms and the owned process was reaped after its 15-second graceful shutdown watchdog. The trace leaves the mutation outcome unknown.
- GPU performance, live S3 permissions, autoscaling and provider tests, final feature guards, and verification against the published OLTP branch remain open. The cgroup evidence covers local Docker execution.

## Reproduce

Run the relevant commands from the indicated source pin inside a Linux Docker container with the matching cgroup limits. Use separate paths for the built engine binaries because Cargo writes each selection to the same binary name.

```sh
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 RUST_TEST_THREADS=1

cargo build --locked --manifest-path server/Cargo.toml --no-default-features --features olap --bin nabedra-db
cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features olap --lib --tests
cargo build --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --bin nabedra-db
cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --lib --tests
python3.11 scripts/check_engine_features.py --dependencies-only

export PYTHONPATH=orm/python/src
python3.11 -m unittest discover -s orm/python/tests
python3.11 -m unittest discover -s orm/python/tests -p test_streaming.py
python3.11 scripts/acceptance_olap_streaming.py --binary "$OLAP_BINARY" --evidence process.json
python3.11 scripts/acceptance_olap_streaming.py --binary "$OLAP_BINARY" --legacy-binary "$OLD_OLAP_BINARY" --scenario legacy-join --evidence mixed.json
python3.11 scripts/acceptance_olap_stream_pytorch.py --olap-binary "$OLAP_BINARY" --old-olap-binary "$OLD_OLAP_BINARY"
```

The PyTorch fixture uses CPU PyTorch 2.7.1+cpu, PyArrow 20.0.0, and NumPy 2.2.6. For the narrowed OLTP probe, start the pinned binary with four workers, call `unsupported` against its URL, then call `exercise` from `server/tests/oltp_orm_contract.py` separately with `scales=(1, 100)` and `scales=(1000,)`. [Stream process fixture](../../../../scripts/acceptance_olap_streaming.py) · [PyTorch fixture](../../../../scripts/acceptance_olap_stream_pytorch.py)
