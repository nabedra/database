# Simulator fault model (`specs/sim`)

Module `sim`, level L1 (`docs/testing/modules.toml`). These specs model the deterministic simulator itself: the
environment that every module spec and every S cell assumes. They are written from the shipped simulator code, because
the simulator has no plan in `plans/` beyond the phase 10 and 11 copy in `plans/testing/`. Its designs live outside the
repository (`designs/phase-9-disk-seam.md`, `phase-10-11-oltp-sim.md`, `phase-12-kv-cluster-sim.md`,
`lead-decisions.md`). This file carries the design review section the skill asks of a plan and a short conformance
matrix. The full fault matrix, the trace vocabulary and the mapping to code are in the review wave's
`sim-mapping/sim.md`.

First written by dr-sim at `7dadd41d` and `d4963667` (2026-11-24). Reworked by rv-sim on `e6b4040b` (2026-11-26):
every switch is now a case of the one `Mutant` constant (the `Variant` and `Router` constants are gone), each mutant
runs at the constants of a named baseline, the clock bound is sampled by a step, and the findings fixed on main since
2026-11-24 became reverted-fix mutants.

| Spec | Models | Code |
| --- | --- | --- |
| `disk/SimDisk.tla` | FaultFs and the volume layer: write, set_len, fsync (and its EIO), create, rename, remove, directory fsync; crash, abort and late destructors; restart with a kill, clean-cut or power-loss image (512-byte and 4 KiB modes); disk loss and restart without the disk, with or without mkfs inode numbering | `server/rt/src/fs/fault.rs`, `server/rt/src/sim/volumes.rs`, `World::crash` in `server/rt/src/sim.rs` |
| `net/SimNet.tla` | The stream seam: per-connection pipes, delay, frame order, loss as a stall, reset, partition as a hold, heal, crash, freeze | `server/rt/src/sim/net.rs`, `server/sim/src/net.rs`, `server/sim-oltp/src/net/mod.rs`, `server/src/cache/cluster/sim/harness.rs` |
| `net-rpc/SimNetRpc.tla` | The Events message router: drop until the deadline, delay, duplicate copy, lost reply after apply, link checked at arrival, router pause, target crash | `server/events/src/sim/net.rs`, `server/sim/src/net.rs` |
| `clock/SimClock.tla` | Node clocks (rate drift, reanchoring, wall jumps, monotonic stalls, stop-the-world pauses), timers mapped to the runtime clock, `SimClockBound`, and the lease check's sample and serve | `server/rt/src/sim.rs`, `server/rt/src/time.rs`, `server/src/cache/cluster/sim/clock_bound.rs`, `hooks.rs` |

Each spec has its own folder, because `tla_check.py` runs every file in a folder's `mutants/` against one spec. Each
holds `MC<Name>.tla`, the T1 baselines `<Name>.cfg` and `<Name>.<part>.cfg`, `mutants/<mutant>.cfg`, and
`witnesses/<witness>.cfg`.

None of this has been compared with a running simulator. No trace of the simulator has been replayed through these
specs.

## Kinds of configuration

Every configuration in `mutants/` fails on the property its first line names, and its third line names the baseline
whose constants it uses (only `Mutant` differs, so no control run is needed). The second line says which kind it is:

- **mutant**: a one-line change to the simulator that a property must reject.
- **shipped (finding)**: the shipped behaviour, where it differs from the baseline. The baseline models the smallest
  fix, and the configuration keeps the counterexample until the code is fixed (the `prefix_<slug>` rule of the design
  bug loop). Open bug record in `docs/testing/bugs/`.
- **reverted fix**: a finding fixed on main since 2026-11-24. The baseline is the shipped code; the configuration is
  the code before the fix.
- **gap**: what a real environment does and the shipped simulator does not. The property states the simulator's
  restriction, and the gap configuration breaks it. A gap shows where the simulator is weaker than reality.
- **expected failure**: phase 12's E1 and E2, beyond-bound clock modes that the checker must catch.

`witnesses/` holds reachability probes: the baseline with a property that states the absence of a fault, whose
violation shows the simulator can inject the fault. No baseline checks those properties, so `tla_check.py` does not
run them (it refuses a mutant whose property no baseline checks). Run them by hand; their results are
`local, unattested`:

```sh
cd specs/sim/disk && ../../tools/tlc --tla-out /tmp/w -workers 2 -config witnesses/reach_torn_sector.cfg MCSimDisk.tla
```

## Design review

### Guarantees

The simulator has no register. Its fault model is the environment of every S and H cell. The guarantees below name a
disk, network or clock fault in their failure model, and their S evidence depends on the simulator injecting it
faithfully. No register row should gain T from these checks: they check the fault model, not the product.

| Guarantee (scenarios) | Seam | Properties | Not modelled, and why |
| --- | --- | --- | --- |
| LSM-G01 (LSM01, NS04, NS05) | disk | `SyncedSurvives`, `SyncedSectorsSurvive`, `CrashImageAllowed`, `OsIsDurablePlusPending` | |
| LSM-G02, LSM-G03 (NS04, NS05) | disk | `CrashImageAllowed`, `NoDoubleLink`; witnesses `reach_torn_sector`, `reach_lost_dir_op`, `reach_4k_destroys_synced` | the exact-prefix rules are LSM's (`specs/lsm`) |
| OLTP-G01 (O11, NS04, NS05, NS13, NS14) | disk | as LSM-G01, plus `NoWriteAfterCut`, `IncarnationSeesOneVolume` | job atomicity (finding SIM-1): a concurrency property of the runtime, not of the disk |
| OLTP-G08 (O17 precondition, RV-REP #3) | disk | `UuidAnchorRejectsWipe`; witness `reach_inode_reuse` | two mounts per node: the model has one volume |
| OLTP-G03, OLTP-G04 (D02, D10, O10) | network | `ReceivedIsPrefix`, `ConnectionFifo`, `NoDuplicateDelivery`, `NoDeliveryAfterReset`; witness `reach_lost_message` | HTTP framing cuts (byte level) |
| CACHE-G05, CACHE-G06, CACHE-G07 (K03) | network, clock | `HoleHoldsInFlight`, `HeldNotLost`, `ClockAssumption`, `SampleFreshAtServe`, `MonoNeverBackward`, `TimerNeverEarly`; expected failures `e1_beyond_bound`, `e2_local_docker` | lease rules themselves are `specs/cache` |
| OLTP-G02 (O09) | clock | `MonoNeverBackward` (wall jumps leave the monotonic clock alone), `TimerNeverEarly` | OLTP lease drift bound: not stated anywhere (finding SIM-6) |
| EVT-G01, EVT-G03, EVT-G06 (EV01, EV02, EV07) | network (RPC) | `NothingCrossesBlockedLink`, `PausedRunsNothing`; witnesses `reach_rpc_duplicate`, `reach_lost_reply_applied`, `reach_rpc_reorder` | |
| EVT-G05, EVT-G08 | clock | `MonoNeverBackward` | the Events spec models wall jumps itself |
| OLAP-G02..G05 (T03..T06) | object store | none | `SimObjectStore` is modelled directly by `specs/olap` |

### Topology and failure model

```text
 SimDisk:  node ──calls──> FaultFs volume { OS layer, durable layer, pending dir ops }
                 World::crash (halt volumes, net cut, services dead, abort tasks)
                 World::restart_with(cut) | World::wipe(down node) + World::restart
 SimNet:   client node C ──stream (two pipes)──> server node S
                 planner (OLTP Router | Cache Network) decides each flush's fate
                 World::black_hole / heal / reset_connection / crash / freeze
 SimNetRpc: caller C ──call(plan)──> target T (duplicate copy, reply plan, router pause)
 SimClock: true time E (incl. pauses) ──> per-node local L = anchor + (E - anchor) * rate
                 wall = L + offset; timers mapped to the runtime clock; SimClockBound per node
                 lease check: Sample (reading, error, truth) ... Serve, in one poll
```

The skill's fault classes, and whether the simulator can inject each. "Seeded" means a default sweep draws it; "opt-in"
means a workload, overlay or check enables it; "API" means no seeded run uses it. O = OLTP (`server/sim-oltp`), C =
Cache (`server/src/cache/cluster/sim`), E = Events (`server/events/src/sim`), L = the LSM campaign
(`server/kv/tests/campaign`), P = the pool sim tests. Read on main `e6b4040b`, not run.

| Fault class | Injectable, and where | Workloads that use it | Spec |
| --- | --- | --- | --- |
| crash | `World::crash` (cut network, volumes, services, then tasks, one synchronous call). Lands between polls and, without storage pauses, between blocking jobs | seeded in O, C, E, L, P | `SimDisk!Crash`, `SimNet!CrashClient`, `SimNetRpc!CrashTarget` |
| restart with the disk | `restart_with(Kill)` keeps unsynced data unsynced; `Cut::Crash` keeps synced data | O kill and clean cut, E clean cut only, L all, P kill and crash; C workers keep no disk, C controllers a real `/dev/shm` directory | `RestartKill`, `RestartCrash` |
| restart without the disk, same identity | `World::wipe` (down node only) + `restart`; O `restart_same_identity` with a surviving keys volume; E `Cluster::wipe` | O membership (half of its seeds, `oltp-sim-membership-wipe`, 24 seeds T2); E off by default, no test enables it; C none | `Wipe`, `RestartFresh` |
| restart with a stale disk, same identity | O `disk::snapshot` + `DataVolume::Stale` | opt-in membership step, in no `draw()` and no check | not modelled |
| partition | `World::black_hole` and `NetPlan::block` hold messages, heal delivers them in order; dials hang if either direction is held | seeded symmetric in O, C, E; one-way seeded in E only | `SimNet!Partition`, `SimNetRpc!Block` |
| message loss | streams: the direction stalls until a reset (TCP-like); RPC: a lone drop | C filters (at most 5%, as a stall), E lone drops; O beyond-TCP drops need an environment flag no check sets | `SimNet!Send` drop, `SimNetRpc!Send` drop |
| duplication | streams absorb duplicates (TCP); RPC runs the handler twice | E only | `SimNet` duplicate (absorbed), `SimNetRpc!HandleDuplicate` |
| reordering | across connections, and across streams of one session (stronger than TCP, finding SIM-7) | O under DeadlineEdge and Slow delays; E RPC delays | `SimNet!BecomeReady`, mutant `prefix_frames_overtake` |
| delay | fixed per message, seeded rules | O fixed, C jitter up to 15 ms, E seeded | `SimNet!BecomeReady` |
| clock drift within a bound | per-node rate, mid-run changes; `SimClockBound` interval for Cache | O ±200 ppm per seed and mid-run (`Action::Clock`); C workers mid-run, never controllers; E none (wall jumps and monotonic stalls instead) | `SetDrift`, `Resync`, `ClockAssumption` |
| torn unsynced writes | `Cut::PowerLoss`, `Cut::PowerLoss4k` | L only. O maps its power cut to the clean cut, E uses the clean cut, C has no disk cut | `RestartPowerLoss` |
| pause between a clock read and its use | #123 clock freezes on `time::now` and `wall_now`, off by default; the Cache ClockBound sample cannot stop | O `resources` overlay only | gap `gap_pause_after_sample` |
| storage job stopped at a sync | #123 storage pauses before and after write, sync and rename, off by default | O membership and `resources` only | `SimDisk!Crash` may land at any call boundary (stronger than the default sweeps) |

Out of scope in the specs: write and rename EIO and ENOSPC (refusals with no image effect), bit flips (media, not cut
semantics), nested directories (ancestor durability is unit-tested in `fault.rs`), modes and owners, locks, dials,
power off, byte-level partial reads, buffer limits, caller crash in the RPC model, and the millisecond rounding in
Cache `validate` (product code, `specs/cache`).

### Spec

| Baseline | Bounds | Why they are enough |
| --- | --- | --- |
| `disk/SimDisk.cfg` | 3 names in one directory, 2 inodes, 2 writes or truncations, 3 directory ops, 1 fsync EIO, 2 crashes; kill, clean cut and 512-byte power loss | 3 names and 3 ops are the shortest broken rename chain; 2 writes give a synced sector next to a dirty one and an intermediate version; 2 crashes give kill then power loss |
| `disk/SimDisk.p4k.cfg` | 2 names, 2 inodes, 2 writes, 2 ops, 1 EIO, 2 crashes; kill and 4 KiB power loss | a synced sector shares its page with a dirty one, alone and after a kill |
| `disk/SimDisk.wipe.cfg` | 2 names, 1 inode, 1 write, 1 op, 2 crashes, 2 wipes, 3 inode bases, every cut | a wipe after a wipe, with and without a restart between, with and without mkfs numbering |
| `disk/SimDisk.full.cfg` | the union: 3 names, 2 writes, 3 ops, 1 EIO, 2 crashes, 1 wipe, every cut | the combinations of the three above |
| `net/SimNet.cfg` | framed or plain, 2 streams, 3 messages, 1 each of drop, duplicate, partition, reset, crash, freeze | 3 messages on 2 streams are the smallest overtaking and the smallest gap |
| `net-rpc/SimNetRpc.cfg` | 3 calls, 1 each of drop, duplicate, block, pause, crash | a duplicate, a reorder and a lost reply need two calls |
| `clock/SimClock.cfg` | 1 ClockBound node, drift in {-1, 0, 1} per 4 ticks, base error 1, 9 ticks, 5 drift changes, 1 resync, 1 pause | 5 changes are the fewest that make the truncation budget matter (mutant `no_reanchor_budget`) |
| `clock/SimClock.age.cfg` | 1 ClockBound node, declared drift 2 per 4 ticks, 16 ticks, 1 drift change | a slow clock's understated age exceeds the base error only after a long run (mutant `error_ages_on_local`) |
| `clock/SimClock.timers.cfg` | 1 node, 2 timers, 8 ticks, 2 drift changes, 1 pause, 1 wall jump, 1 monotonic stall | a timer armed before a drift change |
| `clock/SimClock.joint.cfg` | ClockBound faults and one timer on one node, 3 drift changes | the combination |

Two things are still not combined: 2 wipes run only in `SimDisk.wipe.cfg`, and wall jumps and monotonic stalls only
with timers. The clock baselines have one node each: no action reads two nodes' clocks, so every property is per node.

The model draws a default wipe's inode base from bases the volume never used. The shipped guard compares only with the
previous base, so a repeat of an older one is possible there, with a chance of about 2^-20 per wipe.

No fairness: liveness is optional at L1 and is not modelled. `MaxTime` and the budgets are the only bounds; no
configuration has a `CONSTRAINT`.

T checks: `sim-sim-disk`, `sim-sim-net`, `sim-sim-net-rpc`, `sim-sim-clock` (tier T1).

### Results

The attested runs are named in the pull request body and in the review wave's report `reports/rv-sim.md`, with the
SHA they tested. Local counts there are not evidence.

**What a pass means.** The baselines model the fix where the shipped code differs (findings SIM-5 early timer, SIM-7
frame overtaking, SIM-8 Events duplicate): the `prefix_` configurations show the shipped behaviour failing. A green T
check does not say the shipped simulator satisfies those three properties. `SampleFreshAtServe` holds because the
simulator is weaker than reality there (finding SIM-4).

### Mutants

| Spec | Configuration | Kind | Breaks | Baseline |
| --- | --- | --- | --- | --- |
| disk | `sync_skips_dirty` | mutant | `SyncedSurvives` | `SimDisk.cfg` |
| disk | `syncdir_noop` | mutant | `SyncedSurvives` | `SimDisk.cfg` |
| disk | `tear_clean_sectors` | mutant | `SyncedSectorsSurvive` | `SimDisk.cfg` |
| disk | `garbage_in_512` | mutant | `CrashImageAllowed` | `SimDisk.cfg` |
| disk | `no_chain_dependency` | mutant | `NoDoubleLink` | `SimDisk.cfg` |
| disk | `rename_not_atomic` | mutant | `NoDoubleLink` | `SimDisk.cfg` |
| disk | `rename_skips_pending` | mutant | `OsIsDurablePlusPending` | `SimDisk.cfg` |
| disk | `abort_before_halt` (phase 9 P2) | mutant | `NoWriteAfterCut` | `SimDisk.cfg` |
| disk | `no_incarnation_fence` | mutant | `NoWriteAfterCut` | `SimDisk.cfg` |
| disk | `gap_writeback` | gap | `OnlyLatestOrOld` | `SimDisk.cfg` |
| disk | `wipe_keeps_uuid` | mutant | `UuidAnchorRejectsWipe` | `SimDisk.wipe.cfg` |
| disk | `prefix_wipe_while_up` | reverted fix (e75ec244) | `IncarnationSeesOneVolume` | `SimDisk.wipe.cfg` |
| net | `drop_without_stall` | mutant | `ReceivedIsPrefix` | `SimNet.cfg` |
| net | `frames_ignore_stream` | mutant | `ReceivedIsPrefix` | `SimNet.cfg` |
| net | `deliver_duplicates` | mutant | `NoDuplicateDelivery` | `SimNet.cfg` |
| net | `hole_ignores_in_flight` | mutant | `HoleHoldsInFlight` | `SimNet.cfg` |
| net | `reset_keeps_queue` | mutant | `NoDeliveryAfterReset` | `SimNet.cfg` |
| net | `prefix_frames_overtake` | shipped (SIM-7) | `ConnectionFifo` | `SimNet.cfg` |
| net | `prefix_cache_partition_stall` | reverted fix (Cache `Partitions::Late`) | `HeldNotLost` | `SimNet.cfg` |
| net | `gap_crash_sends_fin` | gap | `NoStaleFromCrashed` | `SimNet.cfg` |
| net-rpc | `link_checked_at_send_only` | mutant | `NothingCrossesBlockedLink` | `SimNetRpc.cfg` |
| net-rpc | `pause_ignored` | mutant | `PausedRunsNothing` | `SimNetRpc.cfg` |
| net-rpc | `prefix_dup_crosses_block` | shipped (SIM-8) | `NothingCrossesBlockedLink` | `SimNetRpc.cfg` |
| clock | `error_ages_on_local` | mutant | `ClockAssumption` | `SimClock.age.cfg` |
| clock | `no_reanchor_budget` | mutant | `ClockAssumption` | `SimClock.cfg` |
| clock | `reanchor_drops_local` | mutant | `MonoNeverBackward` | `SimClock.cfg` |
| clock | `e1_beyond_bound` (E1) | expected failure | `ClockAssumption` | `SimClock.cfg` |
| clock | `e2_local_docker` (E2) | expected failure | `ClockAssumption` | `SimClock.cfg` |
| clock | `prefix_early_timer` | shipped (SIM-5) | `TimerNeverEarly` | `SimClock.timers.cfg` |
| clock | `gap_pause_after_sample` | gap (SIM-4) | `SampleFreshAtServe` | `SimClock.cfg` |

Witnesses (by hand): disk `reach_torn_sector`, `reach_lost_dir_op`, `reach_4k_destroys_synced`, `reach_fsyncgate`,
`reach_kill_keeps_unsynced`, `reach_inode_reuse`; net `reach_lost_message`; net-rpc `reach_rpc_duplicate`,
`reach_lost_reply_applied`, `reach_rpc_reorder`. Counterexample lengths of all of them are in `reports/rv-sim.md`.
`TypeOK` has no mutant, by the runner's exemption.

### Counterexamples

Each is a finding about the shipped simulator. None changes a product protocol.

| Finding | Shortest trace | Record |
| --- | --- | --- |
| SIM-5 early timer | arm `sleep_until(now + 2)` at drift 0; set drift -1; tick; tick; the timer fires at node time 1, deadline 2 | `docs/testing/bugs/2026-11-26-design-sim-early-timer.toml` |
| SIM-7 frame overtaking | send a frame on stream 1, then one on stream 2; the second becomes ready and is read first | `docs/testing/bugs/2026-11-26-design-sim-frames-overtake.toml` |
| SIM-8 Events duplicate | send a call planned `duplicate`; it arrives; block the link; the duplicate copy runs the handler | `docs/testing/bugs/2026-11-26-design-sim-events-dup-crosses-partition.toml` |
| SIM-4 pause after sample (gap) | sample the bound; a tick passes; serve at a later true time than the sample | none: a fidelity gap, no seed |

### Cost

The simulator's cost per fault, read off the specs' actions:

| Fault | Work in the simulator | Trace lines (vocabulary in `sim-mapping/sim.md`) |
| --- | --- | --- |
| crash | halt every volume, reset or black-hole every connection of the node, kill its services, abort its tasks | 1 |
| restart with a power-loss cut | one pass over the pending directory ops (one coin each) and over the dirty units of every inode (one draw each) | 2 per volume |
| dropped message | one stall flag; later sends on the direction are no-ops | 1 |
| partition, heal | one hole entry; held messages are kept in their pipes | 1 |
| drift change | one reanchor (one tick of truncation, budgeted in `SimClockBound`) | 1 |

### Upgrade and rollout

Not applicable: the simulator is dev-only and never runs mixed versions. Every proposed change is sim-only and must
keep product builds unchanged (O02, NS26, `rt-zero-cost`).

### Independent read

The reads are the review wave's `reads/sim.md` files (2026-11-24, 28 scenarios R-sim-01 to R-sim-28, and the
2026-11-26 read). The encoding of every scenario is in `reports/rv-sim.md`, section "Independent read".
