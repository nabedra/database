# Deterministic OLTP cluster harness

The smoke workload starts three registered workers and two API gateways. Each server process
runs on its own mounted FaultFs volume. The bootstrap client provisions the workers, enrolls
the gateways through public HTTP, and drives each gateway's supervisor handshake. Two client
nodes then create a table, write and read a key, and check a retry through the second gateway.
The workload checks public direct-path counters and resource gauges, mounted device identity,
bounded readiness, and registry cleanup. The history checker checks single-key linearizability
and keyed deduplication together.

## Run and replay

Run from `server/` with the pinned toolchain.

```sh
export RUSTFLAGS='--cfg tokio_unstable'
export RUST_MIN_STACK=16777216
export CARGO_TARGET_DIR=target/sim
cargo run --locked -p nabedra-sim-oltp --bin nabedra-sim -- sweep
NABEDRA_SEED=1 NABEDRA_SEEDS=1 NABEDRA_SIM_VERIFY_REPLAY=1 \
  cargo run --locked -p nabedra-sim-oltp --bin nabedra-sim -- run
```

`NABEDRA_SEED` and `NABEDRA_SEEDS` select a contiguous seed range. A local sweep defaults to
20 seeds and one job. `NABEDRA_SIM_VERIFY_REPLAY` selects the fraction that repeats. Each replay
compares the trace hash, event count and history bytes. Worker frames on independent
connections at the same virtual instant are ordered by connection. The trace preserves their
per-connection order and history, probe, and fault boundaries. HTTP response flushes record
delivery metadata because public stats include host-dependent profiling counters. Set `NABEDRA_SIM_TRACE=full` to save
the whole chronological trace. `NABEDRA_SIM_ARTIFACTS` changes the artifact directory.
Use an absolute path to share a directory between CLI and libtest invocations.

```sh
NABEDRA_SEED=1 NABEDRA_SEEDS=1 NABEDRA_SIM_VERIFY_REPLAY=1 NABEDRA_SIM_TEST_DEFECTS=1 \
  cargo run --locked -p nabedra-sim-oltp --bin nabedra-sim -- sweep

NABEDRA_SEED=1 NABEDRA_SEEDS=1 NABEDRA_SIM_DEFECT=lost-write \
  cargo run --locked -p nabedra-sim-oltp --bin nabedra-sim -- shrink
```

The first command runs a clean seed, then runs both seeded defects and confirms the reduced
loss tape reproduces the checker anomaly. The second command reports the expected anomaly and
writes a reduced tape. Set `NABEDRA_SIM_TAPE` to that tape, keep the defect and seed, and use
`run` to replay it.

For the seeded lost write, the harness removes a row through HTTP after a real acknowledgement
and excludes the injected deletion from the client history. For the seeded stale read, it
records a previously observed row in place of the latest HTTP response. Both controls are
part of the development-only harness.

## Resources overlay

`NABEDRA_SIM_WORKLOAD=resources` runs `direct`, `cold` and `move` with the phase 11 `resources`
overlay, and `direct,resources` runs it on `direct` alone. Without `resources` in the list the
overlay draws nothing and changes nothing. With it, the run lowers some limits, cuts client
requests at a drawn poll, fills the admission limits, stalls and drains processes during work, and
ends with a quiet stage that must leave every resource ledger where it was.

```sh
NABEDRA_SIM_WORKLOAD=direct,resources NABEDRA_SEED=1 NABEDRA_SEEDS=1 NABEDRA_SIM_VERIFY_REPLAY=1 \
  cargo run --locked --release -p nabedra-sim-oltp --bin nabedra-sim -- run
```

Every run installs the probes, so their rules run too. `ExactlyOnceRelease` and
`ReleaseAfterWork` follow every native lease and Raft log write by name. A request that a client cuts still runs on the servers,
as the product intends. Only a deadline or a stop cancels work there, so the seeds with a short
deadline and many stalls are the ones that abandon native work.

## Topology workload

`NABEDRA_SIM_WORKLOAD=topology` runs splits, replica moves, leader transfers, index publication, a
table drop and gateways with old routes next to the phase 10 nemesis and clients. The rules and the
text each one quotes are in `src/invariants/topology.rs`. Rules that no public answer can judge have
fixtures there and are `not_run` in the sweep record until probes feed them.

A seed's summary says how often each rule was judged. A rule with a count of 0 did not run in that
seed, and the sweep record lists it as `not_run` with the seed. A seed in which no client write
committed, no client read was answered or no auditor scan was answered under load is red (`Progress`).

`NABEDRA_SIM_TOPOLOGY` forces parts of the drawn plan, as a comma list: `split-under-load` or
`split-before-load`, `move` or `no-move`, and `drop-race` or `no-drop-race`. A requested split that
does not publish within its bound after the heal is red. `NABEDRA_SIM_TOPOLOGY_DEFECT` seeds one
defect (`lose-row`, `duplicate-row`, `choose-building`, `fake-drop`, `refresh-twice`,
`lie-about-refusal`, `stuck-fence`, `lose-range`). `scripts/testing/sim_planted.py` runs each with its
clean twin. They test the rules and their wiring. Only `lose-row` and `fake-drop` change what the
cluster does.

## Evidence scope

The sweep report stores seed and configuration hashes, trace hashes, replay counts, virtual
duration, run wall time, CPU including the simulated blocking helper, and the process RSS high-water mark for each
measured run. Run with one job to calibrate the memory required by a single active seed. Report
and history evidence are labelled fault-model. The RT stack setting leaves room for the real
worker bootstrap in debug builds.

The public checks cover the smoke history, a bounded startup, device identity and visible
resource gauges. The first T1 shard also runs a focused power-cycle regression that
reopens the mounted workers and reads the acknowledged value through the original gateway.
Per-replica apply and vote state, all published key ranges, and the complete
ledger need the P10-PROBES stream. The direct workload will supply D04 and D05, and the cold
workload will supply O15. Those scenario claims require their own assertions and timing runs.

The fixture certificates and private keys are generated exclusively for this simulator. Their
fingerprints are stable across seeds. SimNet carries the same peer identity and negotiated
protocol to the production authorization code. The fixture keys are public test data.
