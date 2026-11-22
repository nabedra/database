# Events simulator

The simulator runs a whole Events cluster in one process, on one thread, in virtual
time. A run is a function of its seed: the same seed gives the same schedule, the same
faults, the same client history and the same trace hash. It drives the real broker code
(`Broker`, `Group`, the publish batcher, the `partition::Machine` state machine and the
nabedra-kv store) and replaces only the clock, randomness, task spawning, blocking work,
the peer network and the disk.

It is compiled only with the `sim` cargo feature. A product build without the feature
runs exactly the code it ran before; `src/main.rs` refuses to build with the feature.

## Running

```sh
cd server/events
export CARGO_TARGET_DIR=<events target dir>/sim   # RUSTFLAGS differ from other builds
RUSTFLAGS="--cfg tokio_unstable" cargo test -j 2 --features sim --lib sim::
```

| Variable | Meaning | Default |
| --- | --- | --- |
| `NABEDRA_SEED` | first seed of the sweep, and the seed of the determinism test | 1 (sweep), 7 (determinism) |
| `NABEDRA_SEEDS` | number of consecutive seeds in the sweep | 20 |
| `NABEDRA_PLANTED_SEED`, `NABEDRA_PLANTED_SEEDS` | first seed and budget of each planted-bug search | 1000, 200 |
| `NABEDRA_SIM_HISTORY_DIR` | write every run's history there, not only failing ones | unset |

A failing seed prints its violations, the last trace lines, the path of its history file
(under the system temp directory, `nabedra-sim/`) and a replay command such as
`NABEDRA_SEED=42 NABEDRA_SEEDS=1 RUSTFLAGS="--cfg tokio_unstable" cargo test --features sim --lib sim::tests::seed_sweep -- --nocapture`.

`--cfg tokio_unstable` enables `Builder::rng_seed`, which makes `tokio::select!` branch
order part of the seed. Without it a run still works, but two runs of a seed may
diverge, and the determinism test can fail.

## Seams

| Seam | Product | Simulation |
| --- | --- | --- |
| Time (`rt::now`, `sleep_until`, `timeout_at`, `wall_now`) | tokio and `SystemTime` | tokio paused time plus a per-node monotonic and wall offset |
| Ids (`rt::uuid_v4`) | `Uuid::new_v4` | the node's seeded SplitMix64 stream |
| Tasks (`rt::spawn`) | `tokio::spawn` | registered with the node in the task-local context |
| Blocking storage work (`rt::blocking`) | `spawn_blocking` | inline on the simulation thread |
| Raft runtime (`raft::Runtime`) | `openraft::TokioRuntime` | `sim::runtime::SimRuntime` |
| Peer network (`RaftNetwork for Network`) | reqwest to the axum peer router | `sim::net::call`, an in-memory router |
| Disk | the real filesystem | a `nabedra_kv::fs::FaultFs` volume per broker |

The node context is a tokio task-local holding the broker id. `rt::spawn` and
`SimRuntime::spawn` copy it into every new task and add the task's abort handle to the
node's registry. The router runs each request as a task of the target node.

### The openraft runtime

openraft 0.9's `AsyncRuntime` trait is implementable, so the simulator uses it rather
than seeding election timeouts through the config. `SimRuntime` delegates to tokio, with
three changes:

- `Instant` is `SimInstant`, read on the current node's monotonic clock, so a clock jump
  moves that node's election, heartbeat and lease timers.
- `ThreadLocalRng` draws from the node's seeded stream. openraft uses it for the
  randomized election timeout, so elections are reproducible and differ per node.
- `spawn` registers the Raft core, replication, state machine and log tasks with the
  node, so a crash aborts them.

### Network

`sim::net::call` plans every message with the world's network rng when it is sent:

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

- A crash marks the node down (its network is cut), halts its `FaultFs` (its disk is cut),
  then aborts every registered task. Destructors that run afterwards can neither send nor
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
`RaftEventSimulationTest`:

| Name | Check |
| --- | --- |
| `MonotonicHighWatermark` | a replica's applied index never decreases within one incarnation |
| `MonotonicCommit` | the apply hook never sees an index at or below the previous one |
| `MonotonicEpoch` | a replica's term never decreases, across restarts too (the vote is durable) |
| `SingleLeader` | each term has at most one leader |
| `MajorityReachedHighWatermark` | the highest applied entry is in the durable log of a majority, by the append and truncate hooks, rebuilt from the volume at every start |
| `AppliedPrefixesAgree` | every replica that applies index i applies the same term and payload |

At the end:

| Name | Check |
| --- | --- |
| `ConsistentCommittedData` | every replica holds the same partition (offset to event id) |
| `Liveness` | replicas converge, and the drain finishes, within 30 s of healing |
| history checks | below |

## History

Client operations are recorded in the `nabedra-check` JSON-lines format
(`{"index","type","process","f","key","value","request_id","node","time_ns"}`, with `type`
one of `invoke`, `ok`, `fail`, `info`). Timeouts and Group errors, including "not
leader", are `info` (unknown outcome).

The `nabedra-check` queue checker was not on `main` when this was written, so
`history.rs` implements the assertions locally:

| Anomaly | Guarantee |
| --- | --- |
| `lost-write`: an acknowledged event is not in the final log at its offset | EVT-G01 |
| `duplicate-offset`, `gap`, `poll-order`, `phantom-read`, `invented-event` | EVT-G02 |
| `offset-reassigned`, `duplicate-event`: a retried publish got different offsets | EVT-G03 |
| `split-batch`: a batch's offsets are not consecutive | EVT-G04 |
| `not-delivered`, `drain-incomplete`: at-least-once delivery up to the final checkpoint | EVT-G05 (partial) |
| a poll answered by a node that could not reach a quorum for the whole call | EVT-G06 |

## Planted bugs

Test-only knobs (`Knobs`), compiled only with the feature. Each test searches seeds from
`NABEDRA_PLANTED_SEED` until a matching violation appears and fails if none does within
`NABEDRA_PLANTED_SEEDS` seeds.

| Knob | Where | Expected detection |
| --- | --- | --- |
| `ack_unreachable_appends` | `raft/network.rs`: an unreachable follower's append counts as acknowledged | `MajorityReachedHighWatermark` |
| `reuse_offset_after_dedup` | `partition/events.rs`: a dedup hit rewinds the offset counter | `duplicate-offset`, `offset-reassigned` |
| `lose_fsynced_on_restart` | the harness restarts a broker from its volume as of its previous start | `MonotonicEpoch` and others |

The checkers also have direct tests on known-bad inputs (`sim::tests::checkers`): one
fixture per history anomaly, and synthetic observations for each Kafka invariant.

## Determinism

The trace is an FNV-1a hash over every message plan, reply, apply, nemesis action and
history event, each prefixed with the virtual time. `same_seed_same_trace` runs a seed
twice and compares the hashes, the event counts and the histories.

Sources of nondeterminism that were removed or ruled out:

- tokio's `select!` order: seeded by `rng_seed` (`tokio_unstable`).
- `spawn_blocking`: storage work runs inline, so no OS thread decides an order.
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
