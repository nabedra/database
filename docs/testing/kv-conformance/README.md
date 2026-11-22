# Cache conformance against Valkey

K01 compares Nabedra's standalone cache replies with Valkey 8.1.11. The
reference uses the official `valkey/valkey:8.1.11` image index digest
`sha256:8f836008e0986c36adf50da714c7060cf689dbb335f61fe077939d90fa650809`.
The Tcl tests and command descriptions come from the release commit
`5fb5bc4b13b22084b54c7db2a88d14ce2f6fa867`. The vendored JSON files are
unmodified and include Valkey's BSD-3 license notices.
[Valkey 8.1.11](https://github.com/valkey-io/valkey/releases/tag/8.1.11)

## Differential

Run on two disposable standalone instances, or start Nabedra and the pinned
Valkey container with the second command.

```sh
python3 scripts/cache-acceptance/differential.py \
  --a 127.0.0.1:6380 --b 127.0.0.1:6379 --seed 42 --shrink

python3 scripts/cache-acceptance/differential.py \
  --nabedra-bin server/target/debug/nabedra-db --spawn-valkey --seed 42 --shrink
```

The same seed and program dimensions reproduce the logical command sequences.
Each execution uses a fresh key prefix on both instances, including during
shrinking. The servers retain the test keys until expiry, deletion or shutdown.
The default JSONL log in `artifacts/cache-valkey-diff.jsonl` records invocations
and replies from both engines. The `value` field holds the exact hex-encoded
RESP2 frame. The `attempt` field distinguishes the initial programs from
shrink replays. The log retains the original TTL values.

The check uses the supported data commands and SET options. It covers signed
64-bit integer boundaries, wrong arity, negative expiry, absent and
duplicate keys, empty strings, and binary payloads. It exercises string values
that `INCRBY` rejects as non-integers. Nabedra stores strings, so the data
command set cannot create a list or another value that triggers `WRONGTYPE`.
The direct RESP2 comparison sends raw frames and runs no client library, so it
is not evidence for CACHE-G12. The existing K01 client checks exercise those
clients.

The comparator accepts live PTTL values within 50 ms by default. TTL replies
round to whole seconds, so the same tolerance allows adjacent live TTL values
at a rounding boundary. The expiry and missing-key sentinels must match
exactly. The default program pauses between a write and a read, then checks
PTTL so a read that restarts the expiry fails within the narrower tolerance.

The validator uses the success schemas for both engines. RESP error replies
have no success schema, so the validator checks their error format and the
differential compares their full wire bytes. This follows the success-schema
scope of Redis's request/reply validator.
[Redis request/reply validator](https://github.com/redis/redis/blob/unstable/utils/req-res-log-validator.py)

## Tcl canary

```sh
python3 scripts/cache-acceptance/valkey_tcl.py --nabedra-bin server/target/debug/nabedra-db
```

With `--host` and `--port`, point the canary at a disposable cache. Upstream
external mode attempts to flush it. Docker needs host networking for loopback
targets. The script runs each allowed suite separately and reports the number
of `[ok]`, `[err]` and `[exception]` lines. The assertion count uses completed
test bodies. Upstream emits `[exception]` for suite-level failures and
`--durable` emits `[err]` for exceptions inside test bodies.

External setup requires `FLUSHALL` and `FUNCTION FLUSH` even with
`--singledb`. Those commands are outside Nabedra's current command table.
Some test bodies also require `CONFIG`, `SELECT` or `FLUSHDB`. The canary
reports failures in its log and declares itself non-authoritative below 100
executed assertions. Tcl assertion failures are informational.
The registered canary does not qualify a Cache guarantee.
[Valkey external mode setup](https://github.com/valkey-io/valkey/blob/5fb5bc4b13b22084b54c7db2a88d14ce2f6fa867/tests/support/server.tcl#L373-L396)

## Benchmark fuzz capability

```sh
python3 scripts/cache-acceptance/benchmark_fuzz.py --nabedra-bin server/target/debug/nabedra-db
```

Valkey 8.1.11's `valkey-benchmark` has no `--fuzz --fuzz-mode malformed-commands`
option. The script probes the installed command in the pinned image and reports
`not-run` without starting Nabedra when the mode is missing. It requires PING
before and after a run when the mode is available.
[Valkey 8.1.11 benchmark source](https://github.com/valkey-io/valkey/blob/5fb5bc4b13b22084b54c7db2a88d14ce2f6fa867/src/valkey-benchmark.c)
