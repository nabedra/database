# Events simulator

The simulator runs a whole Events cluster in one process, on one thread, in virtual
time. A run is a function of its seed: the same seed gives the same schedule, the same
faults, the same client history and the same trace hash. It drives the real broker code
(`Broker`, `Group`, the publish batcher, the `partition::Machine` state machine and the
nabedra-kv store) and replaces only the clock, randomness, task spawning, blocking work,
the peer network and the disk.

It is compiled only with the `sim` cargo feature. A product build without the feature
runs exactly the code it ran before; `src/main.rs` refuses to build with the feature.

Since phase 10 (P10-EVENTS) the simulator is built on the shared core. The per-node
context (clocks, seeded ids, the task registry, inline blocking work) is the
`nabedra_rt::sim` world, which the product code reaches through `src/rt.rs`, now thin
re-exports of `nabedra_rt`. The network plan, the FNV trace hash and the Raft observer come
from `nabedra-sim` (`server/sim`). This directory keeps the Events harness: the brokers,
the workload, the nemesis, the history and its checker, the in-memory router and the
openraft 0.9 `SimRuntime`. The feature enables `nabedra-rt/sim` and the optional
`nabedra-sim` dependency; neither reaches a product build.

## Running

```sh
cd server/events
export CARGO_TARGET_DIR=<events target dir>/sim   # RUSTFLAGS differ from other builds
RUSTFLAGS="--cfg tokio_unstable" cargo test --locked -j 2 --features sim --lib sim:: -- --nocapture --test-threads=1
```

| Variable | Meaning | Default |
| --- | --- | --- |
| `NABEDRA_SEED` | first seed of the sweep | 1 |
| `NABEDRA_SEEDS` | number of consecutive seeds in the sweep | 20 |
| `NABEDRA_SIM_VERIFY_REPLAY` | run every sweep seed twice and compare its trace and history | enabled in the T1 check |
| `NABEDRA_PLANTED_SEED`, `NABEDRA_PLANTED_SEEDS` | first seed and budget of each planted-bug search | 1000, 200 (10,000 in `events-sim-planted`) |
| `NABEDRA_HANDOFF_SEED` | the seed `same_term_leader_handoff_preserves_safety` runs | 397 |
| `NABEDRA_SIM_HISTORY_DIR` | write every run's history there, not only failing ones | unset |

A failing seed prints its violations, the last trace lines, the path of its history file
(under the system temp directory, `nabedra-sim/`) and a replay command such as
`NABEDRA_SEED=42 NABEDRA_SEEDS=1 RUSTFLAGS="--cfg tokio_unstable" cargo test --features sim --lib sim::tests::seed_sweep -- --nocapture`.
Single-seed sweeps print the trace hash and event count. T1 runs 500 seeds twice
in debug. Nightly runs release seeds 100000 through 119999 in four jobs of 5,000
seeds each, with each job capped at five hours including the build.

`--cfg tokio_unstable` enables `Builder::rng_seed`, which makes `tokio::select!` branch
order part of the seed. Without it a run still works, but two runs of a seed may
diverge, and the determinism test can fail.

## Seams

| Seam | Product | Simulation |
| --- | --- | --- |
| Time (`rt::now`, `sleep`, `sleep_until`, `timeout`, `timeout_at`, `wall_now`, `unix_ms`) | tokio and `SystemTime` | `nabedra_rt::sim`: tokio paused time plus each node's monotonic clock and wall offset |
| Ids (`rt::uuid_v4`) | `Uuid::new_v4` | the node's seeded SplitMix64 stream in the `nabedra_rt::sim` world |
| Tasks (`rt::spawn`) | `tokio::spawn` | registered with the node in the world's task registry |
| Blocking storage work (`rt::blocking`) | `spawn_blocking` | `nabedra_rt`'s blocking helper completes each job while virtual time stands still |
| Raft runtime (`raft::Runtime`) | `openraft::TokioRuntime` | `sim::runtime::SimRuntime` |
| Peer network (`RaftNetwork for Network`) | reqwest to the axum peer router | `sim::net::call`, an in-memory router |
| Disk | the real filesystem | a `nabedra_kv::fs::FaultFs` volume per broker |

The node context is `nabedra_rt::sim`'s task-local holding the broker id. `rt::spawn` and
`SimRuntime::spawn` copy it into every new task and add the task's abort handle to the
node's registry. The router runs each request as a task of the target node. The product
hooks (apply, append, truncate and the planted-bug knobs) find the run's harness through
the world installed on the current thread, which `nabedra_rt` also installs on its
blocking helper.

### The openraft runtime

openraft 0.9's `AsyncRuntime` trait is implementable, so the simulator uses it rather
than seeding election timeouts through the config. `SimRuntime` stays in Events, because
only Events uses openraft 0.9. It delegates to `nabedra_rt`, which gives three changes:

- `Instant` is `SimInstant`, read on the current node's monotonic clock, so a clock jump
  moves that node's election, heartbeat and lease timers.
- `ThreadLocalRng` draws from the node's seeded stream. openraft uses it for the
  randomized election timeout, so elections are reproducible and differ per node.
- `spawn` registers the Raft core, replication, state machine and log tasks with the
  node, so a crash aborts them.

### Network

`sim::net::call` plans every message with `nabedra_sim::NetPlan` when it is sent, drawing
from the run's `net` stream:

- dropped: a black hole until the sender's RPC deadline;
- delayed by 1 ms plus up to the configured maximum;
- duplicated: a second copy is delivered later and its reply discarded.

A partition blocks directed links. A crashed target refuses at once, like a closed port.
A paused node holds what it sends and receives until it resumes. The reply crosses the
same filters, so a request can apply on the target while its reply is lost. The target
keeps the checks of the axum handler: an unknown group, or a vote while votes are
disabled, is refused.

### Disk, crash and restart

Each broker has a `FaultFs` volume (the kv store) and a real temporary directory (the
snapshot directory, which stays empty because runs stay below the snapshot threshold).

- A crash removes the node's broker (its network is cut), halts its `FaultFs` (its disk is
  cut), then crashes the node in the `nabedra_rt::sim` world, which aborts every
  registered task. Destructors that run afterwards can neither send nor
  write. The next start opens `FaultFs::crash()`, which holds only durable data.
- A restart opens a new broker on that image and reopens the partition group.
- A wipe replaces the volume with an empty one. It is in the nemesis only when
  `SimConfig::wipes` is set; no test enables it yet.

## A run

1. Start three brokers, initialize the group `p0` on broker 1, then commit the partition
   `Init` and open the subscription `s`.
2. For `chaos_ms` (12 virtual seconds) a producer publishes batches of one to three
   events, retries unknown outcomes with the same event ids, and sometimes re-publishes
   an acknowledged batch to hit dedup. A consumer claims a lease, polls, and acknowledges
   what it was delivered. The nemesis acts every 150 to 1,200 ms: crash (one node at a
   time, restarted 0.2 to 3 s later), isolate a node or the leader, block one link, heal,
   pause a node, jump a node's clocks, and change the drop, duplicate and delay filters.
3. Heal everything, restart crashed nodes, wait for the replicas to converge, take a new
   lease and drain the subscription, and converge again.
4. Read every replica's partition and check the end-of-run properties.

## Invariants

After every 10 ms step (and inside the apply hook), after Kafka's
`RaftEventSimulationTest`. They are `nabedra_sim::RaftObserver`'s, fed openraft 0.9 ids
through `sim::log_id` and `sim::vote`; each message names the group, as in
`MonotonicEpoch: p0 n2 ...`.

| Name | Check |
| --- | --- |
| `MonotonicHighWatermark` | a replica's applied index never decreases within one incarnation |
| `MonotonicCommit` | the apply hook never sees an index at or below the previous one |
| `MonotonicEpoch` | each replica's vote is monotonically ordered, including across restarts |
| `SingleLeader` | only the broker named by a committed vote can report leadership for that vote |
| `MajorityReachedHighWatermark` | each entry is in the durable log of a majority at first apply and the highest applied entry is rechecked every step. The append and truncate hooks track the logs, which are rebuilt from the volume at every start. |
| `AppliedPrefixesAgree` | every replica that applies index i applies the same Raft log ID and payload |
| `ElectionSafety` | at most one broker reports leadership under one vote |

Events uses [OpenRaft's default leader IDs](https://docs.rs/openraft/0.9.25/openraft/docs/data/leader_id/index.html).
They order elections by `(term, node ID)`, so a higher vote can replace a leader within
the same numeric term. The harness counts these handoffs (`Stats::same_term_handoffs`), and
`same_term_leader_handoff_preserves_safety` requires one in seed 397, the first of seeds 1
to 399 that has one.

At the end:

| Name | Check |
| --- | --- |
| `ConsistentCommittedData` | every replica holds the same retained events, tail and subscription checkpoint |
| `Liveness` | replicas converge, and the drain finishes, within 30 s of healing |
| history checks | below |

## History

Client operations are recorded in the `nabedra-check` JSON-lines format
(`{"index","type","process","f","key","value","request_id","node","time_ns"}`, with `type`
one of `invoke`, `ok`, `fail`, `info`). Timeouts and Group errors, including "not
leader", are `info` (unknown outcome), and the next operation uses a fresh process number.
Publish attempts for the same batch share a request ID. The receipt carries the offsets
and reports `dedup: true` when the broker inserted no events. Poll and ack events name
their subscription.

The simulator tests replay every seed's JSONL through the `nabedra-check` queue checker.
The final timestamped `heal` starts a 64.2-second virtual redelivery window, which allows
for convergence, lease expiry and the final drain. `history.rs` also checks the history
against the replicas' final log:

| Anomaly | Guarantee |
| --- | --- |
| `lost-write`: an acknowledged event is not in the final log at its offset | EVT-G01 |
| `duplicate-offset`, `gap`, `poll-order`, `phantom-read`, `invented-event` | EVT-G02 |
| `offset-reassigned`, `duplicate-event`: a retried publish got different offsets | EVT-G03 |
| `split-batch`, `incomplete-receipt`, `partial-batch`: a batch returns consecutive offsets for all invoked IDs and leaves all or none of its events on disk | EVT-G04 |
| `not-delivered`, `drain-incomplete`: at-least-once delivery up to the final checkpoint | EVT-G05 (partial) |
| a poll answered by a node that could not reach a quorum for the whole call | EVT-G06 |

## Planted bugs

Test-only knobs (`Knobs`), compiled only with the feature. Each test searches seeds from
`NABEDRA_PLANTED_SEED` until a matching violation appears and fails if none does within
`NABEDRA_PLANTED_SEEDS` seeds. It then runs the catching seed again and requires the same
trace hash, event count, history and violations. The phase 6 acceptance is that every bug
is caught within 10,000 seeds; the `events-sim-planted` check runs the searches with that
budget.

| Knob | Where | Expected detection |
| --- | --- | --- |
| `ack_unreachable_appends` | `raft/network.rs`: an unreachable follower's append counts as acknowledged | `MajorityReachedHighWatermark` |
| `reuse_offset_after_dedup` | `partition/events.rs`: a dedup hit rewinds the offset counter | Queue checker `duplicate-offset` or `duplicate-event` |
| `lose_fsynced_on_restart` | the harness restarts a broker from its volume as of its previous start | `MonotonicEpoch` and others |
| `delay_redelivery_after_heal` | the final consumer starts polling after the redelivery deadline | Queue checker `redelivery-missing` |

The searches were rerun after the move onto `nabedra_rt` and `nabedra_sim`, which changed
every trace hash. Debug build, `--cfg tokio_unstable`, window starting at seed 1000:

| Search | Before (phase 6) | After (phase 10) | Caught by, after |
| --- | --- | --- | --- |
| `commit-without-majority` | seed 1000 | seed 1000, trace `ce8e7273a245f05e` (3,249 lines) | `lost-write` |
| `offset-reuse-after-dedup` | seed 1000 | seed 1000, trace `11f9261a9a20929f` (1,589 lines) | queue checker `duplicate-offset` |
| `missing-redelivery-after-heal` | seed 1001 | seed 1001, trace `045418dc3b853c84` (3,112 lines) | queue checker `redelivery-missing` |
| `fsynced-data-loss` | seed 1000 | seed 1000, trace `ab723fdd02ef5272` (3,345 lines) | `MonotonicEpoch` |

The checkers also have direct tests on known-bad inputs (`sim::tests::checkers`): one
fixture per history anomaly, and synthetic observations for each Kafka invariant.

## Determinism

The trace is `nabedra_sim::Trace`: an FNV-1a hash over every message plan, reply, apply,
nemesis action and history event, each prefixed with the virtual time. `same_seed_same_trace`
runs seed 7 twice and compares the hashes, the event counts and the histories. Seed 7's
trace was `2a2434cce49a35e1` (2,391 lines) in phase 6 and is `f3563b3d61b41ddc` (1,684
lines) since phase 10. Every random choice now comes from a named stream of the seed
(`nemesis`, `net`, `client-<process>`) or from the node's `nabedra_rt` stream.

Sources of nondeterminism that were removed or ruled out:

- tokio's `select!` order: seeded by `rng_seed` (`tokio_unstable`).
- `spawn_blocking`: `nabedra_rt` runs each storage job to completion on its helper while
  the simulation thread waits.
- `Uuid::new_v4` on the Raft path: seeded.
- openraft's election timeout rng: seeded through `SimRuntime`.
- nabedra-kv's background flush and compaction threads: the default memtable is large
  enough that a run never flushes, so they stay idle.

## Limits and next steps

- Snapshot transfer is not simulated; `full_snapshot` reports the peer unreachable. Runs
  stay well below the 1,024-entry snapshot policy. Simulating it needs the snapshot
  directory on `FaultFs` instead of `tokio::fs`.
- One partition group, three voters, no membership change, no control plane.
- Pause holds a node's messages but its local timers keep running.
- Leases are not renewed; a lease loss leads to a new claim and redelivery.
- The router is called through `RaftNetwork`, not through the axum peer router and
  `tower::ServiceExt::oneshot`; the HTTP layer is not in the simulated path.
