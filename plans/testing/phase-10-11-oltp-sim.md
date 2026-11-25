# Phases 10 and 11: OLTP simulation

Design for the testing plan's phase 10 (OLTP simulation v1 on the shared core) and phase 11
(queued, topology and membership workloads). Written 2026-11-23 against `origin/main` at
`a5182df` plus the open phase 7 branches `origin/nabedra/p7-{oltp,repl,runtime}` (pull
requests 55, 56 and 59). Nothing here is implemented. Line numbers are on `a5182df` unless a branch is
named. The phase 7 conversions and phase 8 moves shift them, so implementers re-grep.

## 1. Goal and acceptance

The plan's rows (revision 2, "Phased rollout, revised"):

| Phase | Deliverable | How it is checked | Agent-days |
| --- | --- | --- | --- |
| 10 | OLTP simulation v1 on the shared core | Five reverted fixes found within 10,000 seeds | 13 |
| 11 | OLTP queued, topology and membership workloads | 15 more reverted fixes | 15 |

START-HERE.md adds the scenario sets. Phase 10: D02 to D05, D07, D10, O06, O10, O29. Phase 11:
Q03 to Q06, O12, O14 to O17, O26, O28, D11, R01 to R05, O30 to O32, with at least one reverted
fix per bug class. Revision 1's phase 7 row names the five phase 10 fixes: BUG1, FIXA, the D04
lost responder, ACC3 O15 and AZ2. It also asks for the speed to be measured against the real
runs.

Guarantees proved, as fault-model simulation evidence (kinds S and H in
`docs/testing/guarantees/oltp.md`):

- Phase 10: OLTP-G01 (D04, O06), OLTP-G02 for single-key operations, OLTP-G03 (D02, D03, O10),
  OLTP-G04 (O10, O29, D05, D10), OLTP-G06 (D07), OLTP-G10 (D05). The two extra workloads that
  the ACC3 and AZ2 targets need also give first S evidence for OLTP-G07 (O15 cold start) and
  OLTP-G08 (O26 replica move).
- Phase 11: OLTP-G05, G06, G07 and G08 in full, plus OLTP-G09 through the resource invariants.

The phases pass when:

1. Every listed scenario has a workload whose seeds execute its assertions, and the T1 sweep
   passes on the head SHA with double-run determinism.
2. Each reverted fix, applied as a patch in a scratch worktree, makes a seed in `[1, 10000]`
   report the expected invariant or anomaly. The same seed passes on the unpatched build.
   Phase 10 needs all five. Phase 11 needs 15 more, at least one per class.
3. Product builds are unchanged: `rt-zero-cost` still passes and `cargo tree -e features` for
   every product feature set shows no `sim` feature and no dev-only crate.

## 2. Current state, measured

**Shared runtime (phase 7, merged).** `server/rt/src/sim.rs` (732 lines) already has the
`World`: per-node clocks with drift and wall jumps (`sim.rs:133-192`), seeded per-node
SplitMix64 id streams (`sim.rs:103-130`), a per-node task registry, `crash` and `restart`
(`sim.rs:386-401`), inline `spawn_blocking` on a helper thread (`sim.rs:635-679`) and
`spawn_service` threads that a crash cannot stop (`sim.rs:694-729`). `sim::runtime()`
(`sim.rs:455`) builds a paused current-thread runtime but does not set `Builder::rng_seed`.
`World::crash` only marks the node down and aborts its tasks. It does not cut the network or
the disk, which the module leaves to the harness. The World has no pause (freeze), no
per-run extension slot and no hooks. The server's `sim` feature is
`sim = ["nabedra-rt/sim"]` (`server/Cargo.toml:36`), and `server/src/main.rs:1-5` refuses to
build a binary with it, including through feature unification.

**Events simulator (phase 6).** `server/events/src/sim/` has 2,310 lines of code and 499 lines
of tests. It keeps its own copy of the World (`mod.rs:164-310`), the SplitMix64 and FNV trace
hash (`mod.rs:103-162`), the network planner (`net.rs:20-51`: drop, delay, duplicate, directed
blocks, pause), the Kafka invariant observer (`cluster.rs:115-258`), the history writer
(`history.rs:44-131`), the planted-bug search (`tests.rs:186`) and the openraft 0.9
`SimRuntime` (`runtime.rs`). Events is a separate workspace with its own lock file, openraft
0.9.25 and no dependency on `nabedra-rt`. The T1 check (`events-sim.toml`) runs 500 seeds
twice in about 12 minutes including the build, and the nightly runs 4 shards of 5,000.

**openraft 0.10 in OLTP.** `server/src/oltp_replication/authority/config.rs` declares five
configs at lines 12, 25, 37, 53 and 68 (Control, Timestamp, Status, Data, Catalog). None sets
`AsyncRuntime`, so all use the default `TokioRuntime` (feature `tokio-rt`,
`server/Cargo.toml:63`). openraft 0.10's trait (`openraft-rt-0.10.0/src/async_runtime.rs`) adds
`Mpsc`, `Watch`, `Oneshot`, `Mutex`, `new`, `block_on` and `spawn_blocking` to 0.9's set, and
`ThreadLocalRng: rand::Rng` is rand 0.10 (locked at 0.10.4 through openraft-rt). The only direct
use of openraft's clock in product code is `C::now()` at
`oltp_replication/manager/system_recovery.rs:129`, which P7-BANS left to this phase. The leader
lease behind direct-path lease reads also runs on `C::now()`, so per-node drift reaches it
only through a `SimRuntime`.

**Seam conversion state.** On `a5182df` the OLTP modules have no `nabedra_rt` calls yet. A
regex count of direct `tokio::time`, `tokio::spawn`, `Instant::now`, `SystemTime::now` and
`Uuid::new_v4` calls outside tests gives 60 in `oltp`, 319 in `oltp_replication` and 301 in
`oltp_runtime`. #55, #56 and #59 convert them (for example `direct/executor.rs:620` and `:1741`
become `nabedra_rt::time::sleep` on `p7-runtime`).

**Order-dependent hash maps.** There are 31 `HashMap` or `HashSet` lines in 11 non-test OLTP
files. These five are iterated, so their random order reaches the schedule:

| Site | Iteration | Change |
| --- | --- | --- |
| `oltp_runtime/direct/executor.rs:1710` (`expire_served`), and `served` at `:111` | `handles.into_values()` at `:1720` orders the dedup expiry proposals across groups | `BTreeMap<Uuid, _>` for both |
| `oltp_replication/service_rpc/grant_cache.rs:147` (`warm`) | `warm.iter()` at `:365` orders the serial warm refreshes (RD2) | `BTreeMap<GrantKey, Warm>`, so `GrantKey` (`:79`) derives `Ord` |
| `oltp_replication/multiplex/transport/session.rs:471` (`streams`) | `into_values()` at `:636` orders waiter wake-ups on session close | `BTreeMap<u64, _>` |
| `session.rs:977` (`slots`) | `values()` at `:1005` and `:1012` (`close_all`) | `BTreeMap<(Uuid, Uuid, usize), _>` |
| `oltp_runtime/application/direct.rs:313` (route LRU) | `min_by_key(tick)` at `:373-378`, and ticks are unique | none needed, add a comment |

The others are lookup-only: `direct/schema.rs:86,88,200`, `direct/batcher.rs:153`,
`mvcc/store/direct/clock.rs:90`, `executor.rs:115,1392`, `session.rs:1313` and
`transport.rs:1849,1886` (retain only).

**Process-wide statics.** There are 13 non-test statics in the OLTP modules. Since a simulation
puts every node in one process, three matter. `oltp/pool/disk.rs:108` (`BUDGETS`) keys the
filesystem budget by device id, so all simulated nodes would share one disk budget. The
pointer-keyed registries `mvcc/store/direct/clock.rs:145`,
`authority/timestamp/direct.rs:222`, `service_rpc/direct.rs:240,275`, `typed_log.rs:307` and
`pool/resources.rs:19` must be empty after each seed, or state leaks across seeds in a sweep.
`hosted.rs:916-920` (`proposal_clock`) is diagnostic only.

**Threads, blocking and other nondeterminism.** The `NativeExecutor` query and control pools
run on `spawn_service_with_stack` threads (`oltp/pool/resources.rs:1098-1130` on `p7-oltp`).
Every storage job crosses them, so OLTP is not deterministic in simulation until phase 9 runs
them as tasks. Other sites: a thread at `pool/damage.rs:140`, `spawn_blocking` at
`damage.rs:345`, `restore.rs:88` and `multiplex/transport/resolve.rs:141`, and
`block_in_place` at `oltp/pool.rs:2152`, which panics on a current-thread runtime. A rough
count finds 53 `std::fs` or `tokio::fs` sites in `oltp`, 26 in `oltp_replication` and 60 in
`oltp_runtime`. The kv store already takes `Arc<dyn nabedra_kv::fs::Fs>` (`oltp/pool.rs:249`,
`:289`), so `FaultFs` (`kv/src/fs/fault.rs:581`, `crash` at `:647`, `halt` at `:716`) can back
it today. There are 39 non-test `select!` sites, 5 of them biased. `ring` randomness stays,
for example the restart key at `manager/local/registered/restart.rs:57` on `p7-runtime`.

**Existing harnesses.** The direct tests build a three-voter Data group over real loopback mTLS
(`oltp_runtime/direct/tests.rs:227-359`: `TcpListener`, `WorkerTransport::new`, `RaftConsensus`,
a `StaticSchema` and a `Counter` timestamp source). Process-level tests drive the binary through
`server/tests/oltp_direct_cluster.py` (653 lines: `nabedra-db worker --bootstrap`, the API through
`nabedra.cli database serve`). The product entry points are `registered_process::run`,
`data_worker_process::run` (`data_worker_process.rs:128`) and `api_process::run_api_configured`
(`main.rs:195-218`). Splits go through the operator operation `storage.shard.split`
(`oltp_runtime/operator.rs:208`).

**Checkers available** (`server/check/src/checker/mod.rs`): `linearizable::check_kv` (`get`,
`put`, `cas` and `delete` per key), `outcome::check` (`failed-write-observed`, including dedup
hits), `txn::check` (Elle-style, G0 to G2, real time), `queue::check` and `relaxed_kv::check`.
`gen::Gen` already records a choice tape (`from_tape` at `gen.rs:31`), and `prop::shrink`
(`prop.rs:312`) shrinks it.

## 3. Target design

### 3.1 Crates and where the shared core goes

Decision: the shared core moves into an in-tree dev-only crate, and the OLTP harness gets a
second one. The World itself stays in `nabedra-rt::sim`.

| Crate | Path | Contents | Depends on |
| --- | --- | --- | --- |
| `nabedra-rt` (exists, feature `sim`) | `server/rt` | The World, plus four additions: `runtime_seeded(seed)` (paused runtime with `rng_seed` under `cfg(tokio_unstable)`), `World::freeze` and `World::thaw` (a frozen node's tasks are not polled, like SIGSTOP), `World::insert_ext` and `World::with_ext::<T>` (a type-keyed slot for the probe sink and per-run state), and `sim::openraft10::SimRuntime` behind a new feature `sim-openraft` | optional `openraft-rt =0.10.0` and `rand =0.10.4`, both already in the lock |
| `nabedra-sim` (new, `publish = false`) | `server/sim` | Generic core: FNV trace with tail, `NetPlan` (drop, delay, duplicate, directed blocks, partitions, pause, all drawn from a net stream), the nemesis scheduler, `RaftObserver` over `(group, node, term, index, payload hash)`, the sweep driver (seed ranges, `NABEDRA_SIM_JOBS` threads, double-run check, failure artifacts), the tape shrinker adapter, and the planted search loop | `nabedra-rt` (sim), `nabedra-check`, tokio, uuid, serde_json. No product crate and no openraft |
| `nabedra-sim-oltp` (new, `publish = false`) | `server/sim-oltp` | OLTP cluster harness, the SimNet frame and HTTP backends, SimDisk wiring, the probe sink and invariants, workloads, history mapping, the `nabedra-sim` CLI binary, and `bugs/` | `nabedra-sim` and the phase 8 OLTP crates with `features = ["oltp", "sim"]` |

Why two crates instead of the plan's single `nabedra-sim`: Events is its own workspace with
openraft 0.9 and its own lock. It can path-depend on a core with no product dependencies, but
not on a crate that pulls in the OLTP graph. Phase 12 adds `nabedra-sim-cache` the same way.
This is open question 1.

Both new crates join the server workspace `members`, but not `default-members`. Plain
`cargo build` or `cargo test` at the root therefore never unifies `sim` into the server bin,
which `main.rs` refuses to compile. Every sim command names `-p nabedra-sim-oltp`. The P7-BANS
clippy run must use `-p` lists or `--exclude nabedra-sim-oltp` (see section 7).

Events migration (task P10-EVENTS, which nothing blocks on): Events' `rt.rs` becomes thin
re-exports of `nabedra_rt` (same tokio 1.54.1 and uuid 1.29.2). Its `sim/mod.rs` World, its
SplitMix64 and FNV copies, `net.rs::plan` and the `Observer` move onto `nabedra-rt::sim` and
`nabedra-sim`. The openraft 0.9 `SimRuntime` stays in Events. Trace hashes change, so the four
planted-bug searches are rerun and their seed windows updated in the same PR.

### 3.2 Product-side changes, all zero cost

1. **openraft runtime alias (P10-RAFT).** In `authority/config.rs`:

   ```rust
   #[cfg(not(feature = "sim"))]
   pub(crate) type OltpRaftRuntime = openraft::impls::TokioRuntime; // same type as today
   #[cfg(feature = "sim")]
   pub(crate) type OltpRaftRuntime = nabedra_rt::sim::openraft10::SimRuntime;
   openraft::declare_raft_types!(
       pub(crate) ControlConfig:
           D=OwnedCommand<ControlCommand>, R=Applied<Option<ControlReply>>,
           NodeId=u64, Node=ReplicaMember, Entry=OwnedRaftEntry<ControlCommand>,
           AsyncRuntime=OltpRaftRuntime,
   );
   // ...the same AsyncRuntime line on the other four
   ```

   The product alias names exactly the default type, so `rt-zero-cost` must show no change.
   `impls::TokioRuntime` is deprecated in 0.10. Use `#[allow(deprecated)]` on the alias, or a
   direct `openraft-rt-tokio = "=0.10.0"` dependency, which is already in the graph. The
   server's `sim` feature becomes `["nabedra-rt/sim", "nabedra-rt/sim-openraft"]`.

   `SimRuntime` fills the 0.10 trait this way. `Instant = SimInstant(tokio::time::Instant)` with
   `now()` from `nabedra_rt::time::now()`, which makes `C::now()` at `system_recovery.rs:129`
   and the leader lease per-node and drift-aware. `sleep`, `sleep_until`, `timeout` and
   `timeout_at` go through `nabedra_rt::time`, which maps node time to runtime time. `spawn` and
   `spawn_blocking` use `nabedra_rt::task`, so Raft cores join the node registry. `ThreadLocalRng`
   is `SimRng`, implementing rand 0.10's core `Rng` over `nabedra_rt::ids::random_u64()`, which
   seeds election timeouts. `Mpsc`, `Watch`, `Oneshot` and `Mutex` are
   `<TokioRuntime as AsyncRuntime>::*`, and `new` and `block_on` use `runtime_seeded`.
2. **Determinism fixes (P10-DET).** The five map changes in the section 2 table. Key `BUDGETS`
   (`pool/disk.rs:108`) by `(sim node, device)` under `cfg(feature = "sim")`, or by the phase 9
   volume id. Replace `block_in_place` (`pool.rs:2152`) with `nabedra_rt::task::spawn_blocking`
   unless phase 9 already does. Add a `#[cfg(feature = "sim")] fn registries_empty() -> bool`
   that the sweep asserts between seeds.
3. **Probes (P10-PROBES).** A `#[cfg(feature = "sim")] pub mod sim_probe` in the lowest OLTP
   crate (`nabedra-oltp-types` after phase 8). It defines
   `pub enum Probe { Appended, Truncated, VoteSaved, Applied, LeaderSeen, DedupApplied,
   SnapshotServed, DirectProposed, RouteEpochPublished, LedgerSnapshot, GatewayAttempt,
   QueueCopy, Receipt, GuardAcquired, GuardReleased, WorkDone }` and
   `pub fn emit(p: impl FnOnce() -> Probe)`, which looks up the sink with
   `World::with_ext::<ProbeSink>` and does nothing outside a world. The call sites are typed_log
   append, truncate and vote, hosted apply, the direct dedup write in `mvcc/store/direct.rs`, the
   `RangeClock` read and register paths (`clock.rs:240`), Catalog route publication, gateway
   attempts (`application/direct.rs` send loop) and `WorkerSharedResources` counters. Without
   the feature the module and every call site compile away. This is the pattern Events uses
   (`events/src/sim/mod.rs:421-472`).
4. Test-only product hooks the sim needs move from `#[cfg(test)]` to
   `#[cfg(any(test, feature = "sim"))]`. For phase 11 that is
   `ControlMachine::known_features_for_test` (`authority/control/machine.rs:65`).

### 3.3 SimNet over the phase 8 transport seam

SimNet has two backends, both driven by `nabedra_sim::NetPlan`.

- **Frames, between workers.** This implements phase 8's `nabedra_transport::{Connector,
  Listener}` for whole `StreamFrame`s, with no TLS. The connect side hands the target's
  `PeerDirectory::verify` (`multiplex/transport.rs:98-101`) a peer identity value
  (`worker_id`, `incarnation`, `enrollment_revision` and a fixed fake certificate digest per
  identity), so trust and revocation logic still runs. Every frame is planned when it is sent,
  and partitions and pauses are checked again at delivery, as in `events/src/sim/net.rs:70-125`.
  - **Lost-reply cut.** When the last request frame of stream `s` has been delivered and the
    target endpoint has produced reply frames for `s`, the planner may drop those reply frames
    and either black-hole the stream or reset the session. The request has applied, and the
    caller sees `DirectSendError::Uncertain` or a session error. This needs the seam to expose
    the stream id and direction without decoding the payload (requirement R8-1).
  - **Session drop mid-stream (D10).** Reset a session after a random number of frames.
  - **Auth flap (BUG1).** For a window, the connector presents the caller's identity with a
    stale `enrollment_revision`, so the receiver's verify refuses it. Open question 4 covers the
    fallback lever.
- **Bytes, from clients to the API.** HTTP clients reach the API node's axum server through
  `tokio::io::duplex` pairs. axum 0.8.9's `serve` takes a `Listener` implementation (requirement
  R8-4), so hyper and the real handlers stay in the path. The cuts are: close after the request
  body is delivered and before the response (a lost reply), close mid-response, and refuse the
  connection.

Crash semantics on the network: a killed node's established sessions get a reset, the way
the kernel closes a dead process's sockets. A power-off or a partition black-holes them. New
connections to a down node are refused.

### 3.4 SimDisk and crash semantics

Each worker node gets one `FaultFs` volume for its pool root, used through the phase 9
`rt::fs` seam for pool, mount and manifest files and handed to the kv store through its existing
`fs` parameter. The volume survives restarts and can be wiped. Mount and device identity come
from the volume id (requirement R9-5).

| Fault | Disk image on restart | Network | Models |
| --- | --- | --- | --- |
| `kill` | every written byte, synced or not (`FaultFs::process_kill()`, new in phase 9) | sessions reset | SIGKILL; the page cache survives |
| `power` | durable data only (`FaultFs::crash()`, `fault.rs:647`) | black hole | power loss, NS15/NS16 shape |
| `wipe` | empty volume, same node identity offered | black hole | RV-REP #3, PVC wipe |
| `drain` | none; the product's stop channel is signalled | normal | graceful stop with work in flight (D04 lost responder) |
| `freeze` | none; tasks not polled, I/O held | frames held | SIGSTOP (D05, R05) |

Crash order, enforced in `nabedra_sim_oltp::node::crash`: (1) SimNet marks the node down,
drops its in-flight frames and resets or black-holes its sessions; (2) SimDisk halts the volume
(`FaultFs::halt`, `fault.rs:716`), so later writes and syncs fail and nothing more becomes
durable; (3) `World::crash(node)` aborts every registered task; (4) the harness drops its
handles to the process future, so destructors run with network and disk already cut; (5) it
asserts `World::services(node)` is empty, since a live service thread at crash time is a
harness error, not a pass. On restart it opens the chosen image, calls `World::restart` and
runs the process entry point again inside `in_node`.

### 3.5 The cluster harness

The harness is black-box by default: nodes run the real process entry points, and clients use
the public HTTP API.

- **Nodes.** Workers 1 to 3 run `registered_process::run(path)` (RF3) inside
  `nabedra_rt::sim::in_node`. Phase 11 adds 4 and 5 for replacement and growth, plus Data-only
  workers through `data_worker_process::run`. Nodes 101 and 102 are API gateways
  (`api_process::run_api_configured`); a second API covers D02 "retry through another gateway"
  and Q05. Client processes are nodes 1001 and up, each with its own clock and id stream.
- **Bootstrap.** A Rust port of the config generator in `server/tests/oltp_direct_cluster.py`
  (enrollments, mounts and auth files, about lines 330 to 430) writes each node's bootstrap JSON
  onto its SimDisk. The real bootstrap and cold-start code then runs. The default topology has
  one table, created through the API, and one range. D07 splits it through
  `storage.shard.split`.
- **Configuration randomizer.** `nabedra_check::config::sample` draws, from the seed, and
  records in the report and evidence: `direct_oltp` (on in phase 10, either in phase 11),
  `data.direct.batch` (FIXA needs it on), the request deadline (250 ms to 10 s), drift (plus or
  minus 200 ppm per node), wall jumps (up to 2 s, O09), executor `max_in_flight`, lane sizes
  and the Data byte budget (D10, BUG1-encode).
- **Probes as nemesis triggers.** The nemesis subscribes to the probe stream, so it can place
  faults at protocol points instead of by luck. Examples: crash the leader right after an
  `Applied` probe for a client's request (D04 after quorum), drain a leader between `Applied`
  and the reply (the lost responder), or cut the reply when a `GatewayAttempt` probe shows less
  than two `RETRY_PAUSE`s (2 ms, `executor.rs:57`) left before the deadline. The probability of
  each trigger is drawn from the tape.

### 3.6 Workloads and nemesis profiles

Workloads and nemesis decisions draw from one `nabedra_check::gen::Gen`, so they record a
tape. The schedule (tokio `rng_seed`, node streams, net stream) derives from the seed.

**Phase 10 workloads:**

- `direct`: 2 to 6 clients over 8 to 64 keys run `table.get`, `insert`, `upsert`, `update`,
  `delete` and single-range `insert_many`, with unique values and request IDs from the client
  stream. On an unknown outcome a client retries the same ID and body until its budget runs
  out. Some requests reuse an ID with another body on the same and on another key (D03), or
  retry through the other API (D02). The run is 20 s of virtual chaos, then heal, converge (up
  to 30 s) and a final read of every key through both APIs.
- `cold`: run `direct`, then stop every worker (`kill` or `power`), then restart them with
  start delays of 0 to 3 s each (O15, D09, ACC3). The cold start must finish within 120 s of
  virtual time after the last start.
- `move`: run `direct` while the operator moves one Data replica to worker 4, through the
  `storage.replica.*` operations (`operator.rs:222`). Delay is injected on the old voters, so
  the learner replays historical membership (O26, AZ2). The move must finish within 60 s
  after heal.

**Phase 10 nemesis profiles.** Profiles are generic: the regular sweep uses every one, and
none is written for a single bug. Each weights these actions: `crash-leader` (before or after
quorum), `kill-gateway` (after the leader proposes), `lost-reply` (frames and HTTP),
`deadline-edge`, `freeze-follower` (one, then two), `partition`, `auth-flap`, `drain-leader`,
`leader-move`, `split`, `session-drop`, `lane-exhaust`, `clock` (drift and wall jumps) and
`cold`. D10's "old one-shot peer" needs the transport to offer only the fallback protocol
(R8-5). Without it, D10 records that assertion as `not_run`.

**Phase 11 workloads:**

- `queued`: `POST /v1/requests`, receipts, cross-range batches and `direct_oltp=false` runs
  (Q03 to Q06, O14, R01 to R05, O30 to O32). The nemesis can freeze a replica before its
  installation acknowledgment, kill the submitting coordinator, close client sockets, restart
  APIs, run the same ID through two APIs at once, expire results, lose terminal responses,
  stop workers one by one down to one holder, and stop all workers (abandonment).
- `topology`: splits, moves and leader transfers under load, an index publish (including a
  unique index that fails validation), a table drop, and gateway route caches with random
  staleness (O12, O16, O28, D11, O21).
- `membership`: voter replacement, wipe and restart, add and remove sequences, a revocation
  storm past the 1,024 limit (nightly only), mixed feature advertisement through
  `known_features_for_test`, and rolling restarts with a joiner (O17, O26, R03).
- `resources`: an overlay on every workload. It cancels client futures at a random poll, drains
  workers during work, and runs with arenas and lanes at their limits (C07, F03 and the
  resource class).

### 3.7 Invariants

These are checked after every 10 ms step, and synchronously in the probe sink where an event
alone decides them. A violation records the virtual time, the invariant name and the trace
tail.

| Invariant | Rule | Source |
| --- | --- | --- |
| `ElectionSafety` | at most one leader per `(group, vote)` | `VoteSaved`, `LeaderSeen` |
| `MonotonicVote`, `MonotonicCommit` | per replica, including across restarts; commit and apply only within one incarnation | `VoteSaved`, `Applied` |
| `LogMatching`, `AppliedPrefixesAgree` | the same `(group, index)` always applies the same log id and payload hash | `Applied` |
| `MajorityReached` | an applied entry is durable on a majority, rebuilt from the volume at each start | `Appended`, `Truncated` |
| `DedupUnique` | at most one dedup record per `(table, key, request_id)` per replica, equal across replicas | `DedupApplied` |
| `NotAppliedNeverCommitted` | a client answer of `not_applied` (or a definite refusal) for `(table, key, request_id)` has no committed Data entry carrying that identity on any replica, ever. This is the simulator form of the plan's proof-token rule for OLTP-G04 | history plus `Applied` |
| `RangeClockRule1` | no direct commit at `c` on a range where a snapshot read at `s >= c` completed before the commit was proposed | `SnapshotServed`, `DirectProposed` |
| `KeyspaceCoverage` | every published route epoch covers each table's key space with no gap or overlap | `RouteEpochPublished` |
| `ResourceBaseline` | at quiescence, every `WorkerSharedResources` ledger (charged bytes per class, native leases, pending guards, scratch leases, disk reservations) and every `RangeClock` pending set is back at baseline | `LedgerSnapshot` |
| `Liveness` | convergence within 30 s of the heal; the cold start, move or index build finishes within its bound | harness |

At the end of the run, `ConsistentCommittedData` compares each group's applied state across
replicas, and the history checks in section 3.8 run.

Phase 11 adds probe-driven invariants. `SchemaEpochAtCommit`: a commit's schema epoch is the
one in effect at its timestamp. `CoveringIndexComplete`: an acknowledged insert appears in
every covering index scan at a later snapshot (the workload runs the scans).
`RetiredLeaderSilent`: a retired range leader never answers success. `QueueRetention`: an
accepted request keeps a complete copy while any holder lives, and the new run is empty after
a full stop. `OneExecutionPerIdentity`: cluster-wide on the queued path and across the path
boundary. `QueueCopyStaleNotInvalid`: a well-formed copy from a newer authority is refused as
`Stale`. `FenceAfterValidation`: a Data fence epoch names a published Catalog lifecycle.
`WipedNeverVotes`: votes under a node identity come from the storage generation that first
voted under it. `RetryCertifiesNamedRemoval`. `CapabilityBeforeUse`: no entry needing feature
F commits while a voter lacks F. `ReleaseAfterWork`: every `GuardReleased` follows its
`WorkDone` on the trace. `ExactlyOnceRelease`: one release per acquire.

### 3.8 History recorder and checkers

Every client operation is written as a `nabedra_check::history::Event`. The `invoke` event
records `f`, `key`, the value, `request_id`, client and node. The completion maps a success to
`ok`, an error with `Outcome::NotApplied` to `fail` with `"not_applied": true` and its error
code, and `OutcomeUnknown`, a timeout or a cut reply to `info`. After an `info`, the client
continues as a new process. Values are rows `{"id": k, "value": u}`, and the checkers compare
`value`.

| Workload | Checkers |
| --- | --- |
| `direct`, `cold`, `move` | `linearizable::check_kv` per key; `outcome::check` (`failed-write-observed`, dedup hits); a serial oracle that checks result payloads, such as an insert conflict on an existing key, against the O04 pure evaluator extended to mutations |
| `queued` | `txn::check` for batches and queued transactions (G0 to G2 and their real-time forms); `outcome::check`; a receipt check (each accepted ID reaches one terminal state, and `abandoned` appears only after a full stop) |
| `topology`, `membership`, `resources` | the `direct` set plus the probe invariants |

Two checker extensions go to `server/check/src/checker/linearizable.rs` (P10-CHECK): `insert`,
which is a `cas` from absent and fails with `conflict` when the key is present, and `update`,
which writes only when the key is present. Without them the per-key check would have to drop
`insert` and `update`. Each extension gets two known-bad fixtures in
`server/check/tests/corpus/`.

### 3.9 Determinism, replay and shrinking

- The sweep asserts `cfg!(tokio_unstable)` and refuses to run unseeded. RUSTFLAGS
  `--cfg tokio_unstable` apply only to `CARGO_TARGET_DIR=target/sim`, and product builds never
  get them (decision 3).
- The trace hashes frame metadata (stream id, direction, size, plan), probes, nemesis actions
  and history events, each prefixed with virtual time. It never hashes payload bytes that may
  embed `ring` nonces.
- In T1 the first 10% of a shard's seeds run twice. The run fails if the trace hash, event
  count or history bytes differ, which is a determinism bug and never retried.
- Replay one seed:
  `NABEDRA_SEED=<s> NABEDRA_SEEDS=1 NABEDRA_SIM_WORKLOAD=direct RUSTFLAGS="--cfg tokio_unstable"
  CARGO_TARGET_DIR=target/sim cargo test --locked -p nabedra-sim-oltp --lib sweep -- --nocapture`.
  With `NABEDRA_SIM_TRACE=full` the full trace is also written. A failing seed prints this
  command, its violations, the trace tail, the sampled configuration and the history path.
- Shrink with `NABEDRA_SIM_SHRINK=1`. `prop::shrink` runs ddmin over the workload and nemesis
  tape, with a budget of 200 runs. A candidate is kept when any of the original anomaly kinds
  reproduces. The shrinker writes `<seed>.tape`, and `NABEDRA_SIM_TAPE=<file>` replays it. The
  schedule still comes from the seed, so shrinking is a search, not a guarantee.
- The CLI, `cargo run -p nabedra-sim-oltp --bin nabedra-sim -- run|sweep|shrink`, wraps the
  same entry points for local use: one thread and 20 seeds, per the plan's local limit.

### 3.10 How a reverted fix is applied

Each reverted fix is a patch, never a cfg knob. Product source never contains the buggy code.
Each bug has a patch file and a metadata file in `server/sim-oltp/bugs/` (the plan's
`server/sim/bugs/`):

- `<bug-id>.patch`: a diff against the phase 10 base from `git revert --no-commit` of the fix's
  product hunks, or, where that conflicts, a hand-ported revert of the same lines. No test
  hunks.
- `<bug-id>.toml`: `bug` (the id in `docs/testing/bugs/`), `fix_commits`, `class`,
  `scenarios`, `workload`, `profile`, `expect` (invariant or anomaly kinds), `seeds` (default
  `[1, 10000]`) and `found_seed` (from the last passing evidence).

`scripts/testing/sim_bugs.py` (stdlib only) has two commands:

- `check` validates every pair: the bug id exists, the SHAs resolve in history, and
  `git apply --check` succeeds on HEAD. A product change that breaks a patch therefore fails in
  T0 and T1, and the author refreshes the patch.
- `run <ids>` builds the unpatched sim binary first. Then, per bug, it runs
  `git worktree add --detach $RUNNER_TEMP/bug-<id> HEAD` and `git apply`, builds in a shared
  `target/sim-bugs` (so consecutive patches rebuild incrementally), and sweeps with
  `NABEDRA_SIM_EXPECT=<kinds>` until the first matching seed within the budget. It replays
  that seed on the unpatched build and requires a pass. It writes one assertion per bug (bug
  id, patch SHA-256, detecting seed, seeds tried, observed kinds, clean replay) and removes the
  worktree.

The script refuses to run in the main checkout, and no build in the job produces a binary
artifact.

## 4. Reverted fixes

"Revert at a5182df" is the result of `git diff <fix>^ <fix> -- <product files> | git apply -R
--check` on `a5182df`. Patches are regenerated after #55, #56 and #59 and the phase 8 moves in
any case.

### 4.1 Phase 10: the five named fixes

| Target (bug record) | Fix commit | Class, scenarios | Revert (product lines) | Workload, profile | Expected detection |
| --- | --- | --- | --- | --- | --- |
| BUG1 (`2026-11-21-bug1-auth`) | `d7b9d63c3d548e7f694d077aec3d3d4743cdb1c2` hunk in `oltp_runtime/application/direct.rs` (with `259da02c`) | uncertain-outcome; D02, O10 | Conflicts as a whole. Hand-port one line: `application/direct.rs:1132` `return Err(if *uncertain { unknown() } else { error });` becomes `return Err(error);` | `direct` with `lost-reply`, then `auth-flap` on the retry | `NotAppliedNeverCommitted`, and `failed-write-observed` once a later get reads the value |
| FIXA (`2026-11-22-fixa-replay`) | `443322361d89d5f3de5dd06632f09e4a14dd8fec` | uncertain-outcome; D02 | Clean (1 file). Restores `executor.rs:1110-1144` to `Replayed(Some(result))` plus `Committed::Replayed(None) => return Ok(Attempt::Retry(RetryCause::Conflict))`, so a retry within one `RETRY_PAUSE` of the deadline answers `unavailable_not_applied` | `direct`, `data.direct.batch` on, concurrent same-range writers, `lost-reply` plus `deadline-edge` | `NotAppliedNeverCommitted`, `failed-write-observed` |
| D04 lost responder (`2026-11-21-bug1-d04`) | `259da02c2369527ebce3d292dac83dd5ffad2d71` (with `d7b9d63c`) | uncertain-outcome; D04, O10 | Conflicts. Hand-port: in `DirectExecutor::proposal_failed` (`executor.rs:657-667`), return `Ok(())` for every non-disk error, so a proposal of unknown outcome retries locally. A deadline exit then reports `unavailable_not_applied` (`executor.rs` loop), which brings back the pre-fix behaviour where only the leader held the uncertainty (`uncertain` flag, `deadline_expired(uncertain)`, `not_leader_or(handle, uncertain)`) | `direct` with `drain-leader` triggered between `Applied` and the reply, and `crash-leader` | `NotAppliedNeverCommitted` |
| ACC3 O15 (`2026-11-21-acc3-cold-fence`) | `6cf032792e7ed01c97e01c16a31f0adc8478ee95` | fence-lifecycle-order; O15, R04 | Clean (10 lines). Delete the match arm at `oltp_replication/service_rpc.rs:921-931` (`PeerRejected` for `ParticipantFence` with `placement_rejected_before_execution`, then `continue`) | `cold` with staggered reopen | `Liveness` (cold start does not finish within 120 s; a worker exits on the refusal) |
| AZ2 (`2026-11-21-az2`) | `2df6cd9aefeeeed22d56a2c5cf87cebded9f54df` | membership-replacement-trust; O26 | Conflicts (5 files). Hand-port one line: in `raft_sender` (`oltp_replication/typed_network.rs:709-711`) drop `replaying_history \|\|`, so a learner refuses bootstrap voters missing from a historical membership | `move` with delay on the old voters | `Liveness` (move stalls, "sender is outside enrolled replica membership" in the trace) |

Spares, if a target proves unreachable through the black-box harness (the PR states why):
FIXA-unproposed `6ea31de7e9ac775cb8d6e91a61297fdb693e8716` (clean, `direct/batcher.rs`, D04,
seen as `Liveness` or `ResourceBaseline` because a range stays busy); BUG1-encode-direct
(`d7b9d63c`, the `encode_in` `map_err` in `service_rpc/direct.rs`, D10 with a low Data byte
budget, seen as `NotAppliedNeverCommitted`); BUG1-pre-append (`d7b9d63c`, `hosted.rs:1225`
back to `unavailable()`, a precision regression that needs a proof-aware rule).

### 4.2 Phase 11: 22 candidates, at least 15 to be found

| # | Bug record | Fix commit(s) | Class | Scenarios | Revert at a5182df | Workload | Expected detection |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | ACC3-receipt | `d6eb811a9846485713937fb9bedff51f22b2bce1` | uncertain-outcome | O13, O14, Q04 | clean (`remote_api.rs`, `observation_deadline`) | queued, receipt lookup faults, API restart | `outcome::check`, receipt check |
| 2 | BUG1-encode-submit | `d7b9d63c` (`application.rs` hunks: `submission_reply_unavailable`, `possibly_accepted`) | uncertain-outcome | Q05, O13 | port | queued with a low Data byte budget | `failed-write-observed`, `QueueRetention` |
| 3 | BUG1-stream | `c9d7c5b86280e174f613ab6ddacc747c7aafda0c` | uncertain-outcome | D03, Q05 | conflict, port (`http.rs`) | direct on; retries without Content-Length | `OneExecutionPerIdentity` |
| 4 | TOPO1-child-schema | `40e0c7f76d27c67d3f1deda91bc585b1781811bf` | stale-route-topology-schema | O28, D11 | clean (`direct/schema.rs`, `CacheKey`) | topology: index publish, then split | `CoveringIndexComplete` |
| 5 | BUG2 | `fd5aa4a3cef443fc1635ff5667e232b071b6c007` | stale-route-topology-schema | O16 | clean (`replica.rs:180-202`) | queued plus view change | `QueueCopyStaleNotInvalid`, `QueueRetention` |
| 6 | RD1 | `f2e21999dfb5801dcaee413b4c4c524254fa4511` | stale-route-topology-schema | O12, D11 | conflict, port (descriptor and snapshot epoch recheck in `executor.rs`) | topology with DDL fences | `SchemaEpochAtCommit` |
| 7 | IDXHOT | `4fda542014c4d3aa129124503122b75c405a6084` | stale-route-topology-schema | D11, O12 | conflict, port (`mvcc/index/build.rs`) | topology with a hot key during backfill | `Liveness` (index build) |
| 8 | O21CAP | `c4fad97e65892eddf3f96cfe26287a8b04175300`, `c7761409efaad8613c3fa0991730a14862d46879` | stale-route-topology-schema | O21 | first clean, second conflict | queued `table.create` during view install | `Liveness`, receipt check |
| 9 | ACC2 | `c25875663dddcad60275ff34b92cbd62d160ffef` | fence-lifecycle-order | O12 | clean (`planner/index_build.rs`: fence moves back before validation) | topology with a unique index over duplicates | `FenceAfterValidation` |
| 10 | ACC3-ready-fence | `afbc048bb192cdb86b86bb279e80d911864a90ce` | fence-lifecycle-order | O15 | clean (`bootstrap/registered.rs:618-626`) | cold with a Timestamp election | `Liveness` (cold start) |
| 11 | ACC3-delegation | `6ad8a99c2561ba48c0bcb4a2b8252d9c1e2ad30b` | fence-lifecycle-order | O14, R03 | clean (`control/forwarding.rs:516-540`) | membership with rolling restarts | `Liveness`, `outcome::check` |
| 12 | FIXC-owner-fence | `c7b93782a1e9bf4802d3e0dd046ee6725d5616b1`, `4287df8234a38854edf7e54e1e42cd460f1c551a` | fence-lifecycle-order | O08, R01 | first clean, second conflict | queued with owner kills | `outcome::check` (conflict for recoverable work) |
| 13 | FIXB-cold-publish | `c7249204deb25eff3c49afc677a2f0a4a7afc2e2`, `d509f66bbc1e01b8687eec8429a236a0a1016ef5` | fence-lifecycle-order | O17, R04 | first clean | membership: full stop between Mount and Publish | `Liveness` (replacement) |
| 14 | REP2 | `116fb51dd377611f0a53642fe68e1bb4f742da7d` | membership-replacement-trust | O17, NS14 | conflict, port (reconcile by a stable voter set instead of the exact plan) | membership with lost replies on replacement retries | `RetryCertifiesNamedRemoval` |
| 15 | RV-REP-3 | `96d892ea0c3de91af4c7c14a272c66d15ec1e5db` (+ `2def358f`) | membership-replacement-trust | O17, NS23 | conflict, port (skip verification of retained storage on resume) | membership with `wipe` | `WipedNeverVotes` |
| 16 | FIXB-voter-refresh | `780db860fffae8e5a0d1cb0838cc6c146eb06850` | membership-replacement-trust | O17 | clean, one line (`control/endpoint.rs:781`, `64 << 10` back to `HEADER_LIMIT`) | two sequential Control voter replacements | `Liveness`, revocation probe |
| 17 | CAP1 | `9be40820c5d2385d24e8bc4d5b3b441820567f04`, `414dcf79c8f29eb01a4b1a5961b078eacb477232`, `2920a442d269135de2a7471001d6af9ea4c45853` | membership-replacement-trust | O02, O12 | first clean | membership with mixed feature advertisement | `CapabilityBeforeUse` |
| 18 | RV-REP-2 | `8b3c44b2c55b51c4aae414567924ace705fdf048` | membership-replacement-trust | O17 | clean (`system_trust.rs`, `MAX_RETAINED_REVOCATIONS`) | revocation storm past 1,024, nightly only | `Liveness` (Control) |
| 19 | FLAKE3-epoch | `38fc414df670cdfae981184e4721c0bbd79ff0e9` | resource-release-order | O06, O11 | clean (`typed_log.rs` `take(epoch)`) | any, with elections under append load | `LogMatching`, `MajorityReached` |
| 20 | RD2 | `116f03f811997edbff331d619f9baee328856948` | resource-release-order | C07, O15 | clean (`grant_cache.rs` serial warm reads) | cold with a slow Control | `Liveness`; a probe rule allows at most one background Control lane slot |
| 21 | QLANE | `b179b5840b06d2ec7bf0bf001325101b06629dad`, `7ddad3bbf4c3871c2db88a579312e81dc33fbd2a` | resource-release-order | O07, O08 | first clean | queued batches with small lanes | `QueueRetention`, `outcome::check` |
| 22 | GC1-double-guard | `b694f75b3a2198c6a02aff222a2f62dd1161adde` | resource-release-order | C07, F03 | conflict, port (a second `PendingGuard` per `AppendJob`) | resources overlay with cancellation | `ExactlyOnceRelease` |

Stretch entries: FLAKE2-drain `541b37587b46888095186b458ef070b0906b1fcd` (`ReleaseAfterWork`),
REP5-cold-intent `ec8d06c5b80a012b42faf0ea891e459cc934a4f4` (`Liveness`), RES1
`c5d4a26e...`/`60100811...` (`ResourceBaseline` on a shared filesystem, which needs per-node
budgets first).

The commitment is 15 of these 22: four per class where the table has four or more, and at
least one in every class. Rows 1, 4, 5, 9, 10, 11, 16, 18, 19 and 20 revert cleanly today and
come first.

## 5. Checks and evidence

New `docs/testing/checks/*.toml`. All use `module = "oltp"`, `cwd = "server"` and
`env.RUSTFLAGS = "--cfg tokio_unstable"`.

| Id | Kinds | Tiers | Command | Seeds and budget | Passes when |
| --- | --- | --- | --- | --- | --- |
| `sim-core` | U | T0, T1 | `cargo test --locked -j 2 -p nabedra-sim --lib` | unit tests, known-bad observations for each invariant, crash-order test | all pass |
| `oltp-sim`, `oltp-sim-2` to `-4` | S H | T1 | `cargo test --locked -j 2 -p nabedra-sim-oltp --release --lib sweep -- --nocapture --test-threads=1` with `NABEDRA_SIM_JOBS=4`, `NABEDRA_SEED` 1, 501, 1001 and 1501, `NABEDRA_SEEDS=500`, `NABEDRA_SIM_VERIFY_REPLAY=0.1` | 2,000 seeds per SHA, all workloads round-robin; 15 min wall per shard after the shared build (target; see below) | no violation, no anomaly, replay hashes equal |
| `oltp-sim-nightly-1` to `-4` | S H | T2, T3 | the same with `NABEDRA_SEEDS=5000` and a base derived from the date: `1000000 + 20000 * day` | 20,000 new seeds per night | as above; a failing seed opens a known-open entry |
| `oltp-sim-bugs-check` | U | T0, T1 | `python3 scripts/testing/sim_bugs.py check` | seconds | every patch applies and its metadata validates |
| `oltp-sim-bugs-direct` | S H | T2 weekly, T3, dispatch | `python3 scripts/testing/sim_bugs.py run --set direct` | 5 patches, at most 10,000 seeds each, `timeout_s = 18000` | each patch detected within budget, and the clean replay passes |
| `oltp-sim-bugs-{queued,topology,membership,resources}` | S H | T2 weekly, T3, dispatch | `sim_bugs.py run --set <name>` | the phase 11 rows | at least 15 detected across the four sets, one per class |

`oltp-sim` covers OLTP-G01, G02, G03, G04, G06 and G10 in phase 10, and adds G05, G07, G08
and G09 with the phase 11 workloads. Its `scenarios` list grows with each workload PR, and the
`-bugs-` checks cite their bugs' guarantees. Each command writes its record through
`nabedra-check`'s evidence writer: `seeds{range, count, failing}`, `sim_core_hours`,
`virtual_hours`, `mode.direct_oltp`, the sampled configuration hash, and `assertions` (one per
invariant family and one per bug), with histories and traces in `artifacts[]`. Every record is
labelled fault-model evidence, and the real-environment rows in
`docs/testing/guarantees/oltp.md` stay required.

**Budget and where it runs.** Every heavy check runs on GitHub Actions, dispatched through the
`checks` input and pinned to the SHA. The nightly runs through `nabedra-nightly.yml`. Per-seed
cost is unknown until the harness exists: three full workers host Control, Timestamp, Status,
Catalog and Data groups, about 15 Raft replicas. Events costs about 0.5 s per debug seed. The
target is at most 3 CPU seconds per release seed of 60 s virtual time. At that rate 10,000
seeds are about 8.3 core-hours (about 2 h on one 4-vCPU runner with 4 sim threads), a T1
shard fits about 500 seeds in 15 minutes, and the nightly's 20,000 seeds are about 17
core-hours, well inside OLTP's 60% of the 160 core-hour nightly.

P10-HARNESS measures the real cost and the memory per thread, and sets `NABEDRA_SEEDS` per shard
so T1 stays at or under 15 minutes. It also records the virtual and wall time of D04, D05 and O15
against the real runs (16 to 153 s and 55 to 100 s in the plan's table), as the acceptance asks.
The nightly launcher's cap of 8 jobs is already half used by `events-sim-nightly-*`. See open
question 2.

**What counts as pass.** Simulation failures never rerun. A failing T1 or nightly seed is a bug.
It goes on the known-open list with an owner and an expiry and stays red. A seed that does not
reproduce on replay is a determinism bug. The acceptance evidence for phase 10 is one passing
`oltp-sim-bugs-direct` record on the merged phase 10 SHA, plus passing `oltp-sim` shards on the
same SHA. For phase 11 it is the four `-bugs-` records and the shards on its final SHA.

## 6. PR split, order and dependencies

### 6.1 Dependency on phases 8 and 9, precisely

| Need | From | Requirement |
| --- | --- | --- |
| R8-1 | phase 8 | a `nabedra-transport` `Connector`/`Listener` pair that moves whole `StreamFrame`s, with stream id and direction visible without decoding the payload |
| R8-2 | phase 8 | the peer identity reaches `PeerDirectory::verify` as a value (the plan's seam 4) |
| R8-3 | phase 8 | one injection point for the worker and API entry points, for example `nabedra_transport::backend::current()`: TCP and mTLS in product, the World-installed SimNet in sim |
| R8-4 | phase 8 | the axum API server and internal HTTP clients run over the same `Listener` and `Connector` |
| R8-5 | phase 8 | D10 only: a way to offer only the one-shot fallback protocol |
| R8-6 | phase 8 | the split crates exist (`nabedra-oltp-types` holds `sim_probe`) |
| R9-1 | phase 9 | `rt::fs` covers the pool, mount and manifest sites (about 139), so a node's disk is one `FaultFs` volume |
| R9-2 | phase 9 | the kv journal, flush and compaction are steppable, with no service thread doing work |
| R9-3 | phase 9 | `NativeExecutor` (`pool/resources.rs:1098-1130`) runs jobs as sim tasks with the same slot count |
| R9-4 | phase 9 | `FaultFs` offers `process_kill()` (keep unsynced writes), the existing `crash()`, and wipe |
| R9-5 | phase 9 | mount and device identity come from the volume, which also fixes `BUDGETS` |
| R9-6 | phase 9 | the damage thread (`damage.rs:140`), the `spawn_blocking` sites and `block_in_place` (`pool.rs:2152`) are sim-safe |

Without R9-2, R9-3 and R9-6 a run is not deterministic. Without R9-1 and R9-4 the fault model
loses power loss and wipes.

**What can start now**, once phase 7's open PRs merge: P10-CORE, P10-EVENTS, P10-RAFT (after
PR #56), P10-DET (after PRs #55, #56 and #59), P10-CHECK, and the draft patches with
`sim_bugs.py` (P10-BUGS). None of them needs phase 8 or 9.

**What waits.** P10-PROBES waits for the phase 8 move PRs, to avoid editing files that are
about to move. P10-NET waits for R8-1 to R8-4. P10-HARNESS waits for P10-NET, R9-1 to R9-6,
P10-RAFT and P10-PROBES, and P10-WORKLOADS waits for P10-HARNESS. Phase 11 waits for phase 10
to merge (harness, invariant framework, `cold` workload), and its `membership` workload also
needs `wipe` (R9-4).

### 6.2 Phase 10 wave

| Task | Owned paths | Depends on | Size | Start |
| --- | --- | --- | --- | --- |
| P10-CORE | `server/sim/**` (new crate) | phase 7 merged | M (about 900 lines: about 450 moved in from Events, about 450 new for sweep, shrink and nemesis) | now |
| P10-EVENTS | `server/events/src/sim/**`, `server/events/src/rt.rs`, `[dependencies]` and `[features]` of `server/events/Cargo.toml`, `server/events/Cargo.lock` | P10-CORE | M (net removal of about 500 lines) | after P10-CORE |
| P10-RAFT | `server/rt/src/sim.rs`, `server/rt/src/sim/**`, `server/rt/Cargo.toml`, `oltp_replication/authority/config.rs` | #56 | S (about 350 lines) | now |
| P10-DET | `direct/executor.rs` (maps only), `service_rpc/grant_cache.rs`, `multiplex/transport/session.rs`, `oltp/pool/disk.rs`, `oltp/pool.rs:2152` | #55, #56, #59 | S (about 100 lines) | now |
| P10-CHECK | `server/check/src/checker/linearizable.rs`, `server/check/tests/corpus/**` | none | S | now |
| P10-BUGS | `server/sim-oltp/bugs/**`, `scripts/testing/sim_bugs.py`, `scripts/testing/test_sim_bugs.py`, `docs/testing/checks/oltp-sim-bugs-*.toml` | none (patches regenerated before merge) | S to M | now |
| P10-PROBES | `nabedra-oltp-types` `sim_probe` module, plus probe call sites (one line each) in typed_log, hosted, `mvcc/store/direct*.rs` and the gateway send loop | phase 8 moves | M (about 400 lines) | after phase 8 |
| P10-NET | `server/sim-oltp/src/net/**` | R8-1 to R8-4 | M | after phase 8 |
| P10-HARNESS | `server/sim-oltp/src/{node,disk,bootstrap,invariants,history,sweep}.rs`, `server/sim-oltp/Cargo.toml`, `docs/testing/checks/oltp-sim*.toml`, `server/sim-oltp/README.md` | P10-NET, P10-RAFT, P10-PROBES, phase 9 | L (about 2,500 lines) | after phase 9 |
| P10-WORKLOADS | `server/sim-oltp/src/workloads/{direct,cold,moves}.rs`, `server/sim-oltp/src/nemesis/**` | P10-HARNESS | M (about 1,200 lines) | last |

**Shared files and owners:**

| Shared file | Owner | Changes |
| --- | --- | --- |
| `server/Cargo.toml` (members, `default-members`, the `sim` feature line) and `server/Cargo.lock` | the phase 8 crate-split agent | P10-CORE and P10-RAFT send their lines as patches |
| `server/rt/**` | P10-RAFT, for the wave | |
| `.github/workflows/nabedra-nightly.yml`, `docs/testing/modules.toml` (map `server/sim/**` and `server/sim-oltp/**` to module `oltp`, and `server/sim/**` also to `events`), `docs/testing/catalog.json`, `docs/testing/coverage-matrix.*` | the orchestrator | regenerated after each merge |
| `server/src/lib.rs` or the split crates' `lib.rs` | the phase 8 owner | the `sim_probe` declaration goes through them |

Each `checks/<id>.toml` belongs to the task that adds the check.

### 6.3 Phase 11 wave

| Task | Owned paths | Size |
| --- | --- | --- |
| P11-QUEUED | `sim-oltp/src/workloads/queued.rs`; probes in `oltp_runtime/{coordinator,replica,remote_api}.rs` and `worker_endpoint/**` | L |
| P11-TOPO | `sim-oltp/src/workloads/topology.rs`; probes in `mvcc/planner/**`, `mvcc/index/**` and `direct/schema.rs` | M |
| P11-MEMBER | `sim-oltp/src/workloads/membership.rs`; probes in `authority/control/**` and `manager/**`; the `known_features_for_test` cfg | L |
| P11-RES | `sim-oltp/src/workloads/resources.rs`; guard probes in `oltp/pool/resources.rs` and `typed_log.rs` | M |
| P11-BUGS | the phase 11 entries in `sim-oltp/bugs/**` and `checks/oltp-sim-bugs-{queued,topology,membership,resources}.toml` | M |

Each workload task owns its probe call sites. A file that two tasks need, such as `typed_log.rs`
(P11-RES), goes to one owner, and the other sends a patch. The four workload tasks run in
parallel. P11-BUGS adds each patch only after its workload merges.

## 7. Risks, open questions, and what phase 7 must still add

**Risks.**

- Per-seed cost: three full workers with five group kinds may cost well over 3 s per seed, and
  an in-process fixture would skip the bootstrap code ACC3 and AZ2 need. Measure in
  P10-HARNESS and lower `NABEDRA_SEEDS`, never the topology.
- Determinism leaks: library time, `ring` nonces reaching ordering, the process-wide
  registries, `tokio::fs` (its own blocking pool), threads left after phase 9. The double-run
  check finds them, and each is fixed at its source.
- Hand-ported reverts (BUG1, D04, AZ2; `revert_patch_ok = false` or conflicts at `a5182df`) may
  differ from the original bug. Each port cites the original hunk, and the reviewer confirms
  it makes the original regression test fail before it counts (for example
  `d04_o10_bug1_leader_shutdown_after_apply_preserves_proposal_uncertainty`,
  `direct/tests.rs:1792`).
- Patch rot is caught by `oltp-sim-bugs-check` on the PR that causes it. Overfitting is limited
  by shared profiles, and `found_seed` must stay inside the budget on every weekly run.
- Liveness bounds: too tight gives false failures, too loose misses ACC3, AZ2 and IDXHOT. Each
  bound is a constant per workload, set from the clean sweep's 99.9th percentile with 2x
  headroom.

**Open questions for the lead.**

1. Split the plan's `nabedra-sim` into `nabedra-sim` (core, also used by Events) and
   `nabedra-sim-oltp` (later `nabedra-sim-cache`)? The alternative, one crate with an optional
   `oltp` feature, makes the Events lock and feature resolution harder to reason about.
2. The nightly cap is 8 jobs. Recommend 4 OLTP and 2 Events shards (the plan's 60/15 split),
   with the `-bugs-` sets on a separate weekly schedule.
3. Approve black-box workers (real entry points, HTTP clients), with probes as the only
   white-box access.
4. BUG1's `auth-flap` lever is a stale `enrollment_revision` at the transport seam. If the
   real stack does not then answer `Unauthorized` with `NotApplied`, the fallback revokes and
   restores the API worker's Data role through Control, which needs a sim-only admin call.
5. Migrate Events onto `nabedra-rt` (P10-EVENTS)? It adds `nabedra-rt` to Events' product
   graph at zero cost and changes its trace hashes.
6. D10's old one-shot peer stays `not_run` until phase 8 offers R8-5.

**Phase 7 and P7-BANS must still add:** merge PRs #55, #56 and #59, which convert the OLTP
sites phase 10 relies on. Add the `Instant::elapsed` ban with `nabedra_rt::time::elapsed`,
route `JoinSet::spawn` through the registry (`multiplex/transport.rs:1014`), ban
`Handle::spawn_blocking`, and add the `assert_no_sim` cases to `check_engine_features.py`. The
P7-BANS workspace clippy must run with `--exclude nabedra-sim --exclude nabedra-sim-oltp` or
explicit `-p` lists, because a workspace-wide build with `sim` unified hits `main.rs`'s
`compile_error!`. `nabedra_rt::sim::runtime()` should gain `rng_seed`; P10-RAFT adds it if
phase 7 does not.
