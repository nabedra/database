# Simulator fault model (`specs/sim`)

Module `sim`, level L1 (`docs/testing/modules.toml`). These specs model the deterministic simulator itself: the
environment that every module spec and every S cell assumes. They are faithful to the shipped simulator code
(read at `7dadd41d`, and again at `d4963667` after #106 merged), not to a plan, because the simulator has no plan in
`plans/`. Its designs live outside the repository (`designs/phase-9-disk-seam.md`, `phase-10-11-oltp-sim.md`,
`phase-12-kv-cluster-sim.md`, `lead-decisions.md`). This file carries the design review section the skill asks of a
plan, the fault-model conformance matrix, and the vocabulary both waves use for fault steps in traces.

| Spec | Models | Code |
| --- | --- | --- |
| `SimDisk.tla` | FaultFs and the volume layer: write, set_len, fsync (and its EIO), create, rename, remove, directory fsync; crash, abort and late destructors; restart with a kill, clean-cut or power-loss image (512-byte and 4 KiB modes); disk loss and restart without the disk | `server/rt/src/fs/fault.rs`, `server/rt/src/sim/volumes.rs`, `World::crash` in `server/rt/src/sim.rs` |
| `SimNet.tla` | The TCP-like stream seam: per-connection pipes, delay and per-stream frame order, loss as a stall, reset, partition as a hold, heal, crash, freeze, with the OLTP or the Cache planner | `server/rt/src/sim/net.rs`, `server/sim/src/net.rs`, `server/sim-oltp/src/net/mod.rs`, `server/src/cache/cluster/sim/harness.rs` |
| `SimNetRpc.tla` | The Events message router: drop until the deadline, delay, duplicate copy, lost reply after apply, link checked at arrival, router pause, target crash | `server/events/src/sim/net.rs`, `server/sim/src/net.rs` |
| `SimClock.tla` | Node clocks (rate drift, reanchoring, wall jumps, monotonic stalls, stop-the-world pauses), timers mapped to the runtime clock, and `SimClockBound` | `server/rt/src/sim.rs`, `server/rt/src/time.rs`, `server/src/cache/cluster/sim/clock_bound.rs` |

Each spec has its own folder (`disk`, `net`, `net-rpc`, `clock`), because `tla_check.py` runs every file in a folder's
`mutants/` against one spec. Each holds `MC<Name>.tla`, the T1 baselines `<Name>.cfg` and `<Name>.<part>.cfg`, and
`mutants/<mutant>.cfg`.

None of this has been compared with a running simulator. The specs were written from the code, and no trace of the
simulator has been replayed through them.

## Kinds of mutant configuration

Every configuration in `mutants/` fails on the property its first line names. There are four kinds, written on the
second line:

- **mutant**: a one-line change to the simulator that a property must reject.
- **finding**: the shipped behaviour, where it differs from the baseline. The baseline then models the smallest
  fix, and the finding configuration keeps the counterexample until the simulator wave fixes the code (the
  `prefix_<slug>` rule of the design bug loop).
- **witness**: the baseline itself, with a property that states the absence of a fault. Its violation proves the
  simulator can inject the fault (the simulator is at least that strong).
- **gap**: a variant that models what a real environment does and the shipped simulator does not. Its violation
  shows the gap (the simulator is weaker there).

## Design review

### Guarantees

The simulator has no register. Its fault model is the environment of every S and H cell. The guarantees below name a
disk, network or clock fault in their failure model, and their S evidence depends on the simulator injecting it
faithfully.

| Guarantee (scenarios) | Seam | Properties | Not modelled, and why |
| --- | --- | --- | --- |
| LSM-G01 (LSM01, NS04, NS05) | disk | `SyncedSurvives`, `SyncedSectorsSurvive`, `CrashImageAllowed`, `OsIsDurablePlusPending` | |
| LSM-G02, LSM-G03 (NS04, NS05) | disk | `CrashImageAllowed`, `NoDoubleLink`; witnesses `reach_torn_sector`, `reach_lost_dir_op`, `reach_4k_destroys_synced` | the exact-prefix rules are LSM's (`specs/lsm`) |
| OLTP-G01 (O11, NS04, NS05, NS13, NS14) | disk | as LSM-G01, plus `NoWriteAfterCut`, `IncarnationSeesOneVolume` | |
| OLTP-G08 (O17 precondition, RV-REP #3) | disk | `UuidAnchorRejectsWipe`, gap `DevInoAnchorRejectsWipe` | two mounts per node: the model has one volume |
| OLTP-G03, OLTP-G04 (D02, D10, O10) | network | `ReceivedIsPrefix`, `NoDuplicateDelivery`, `NoDeliveryAfterReset`; witness `reach_lost_message` | HTTP framing cuts (byte level) |
| CACHE-G05, CACHE-G06, CACHE-G07 (K03) | network, clock | `HoleHoldsInFlight`, `HeldNotLost`, `ClockAssumption`, `MonoNeverBackward`, `TimerNeverEarly`; expected failures `e1_beyond_bound`, `e2_local_docker` | lease rules themselves are `specs/cache` |
| CACHE-G06 expected failure (E1, E2) | clock | `ClockAssumption` | |
| OLTP-G02 (O09) | clock | `MonoNeverBackward` (wall jumps leave the monotonic clock alone) | |
| EVT-G01, EVT-G03, EVT-G06 (EV01, EV02, EV07) | network (RPC) | `NothingCrossesBlockedLink`, `PausedRunsNothing`; witnesses `reach_rpc_duplicate`, `reach_lost_reply_applied`, `reach_rpc_reorder` | |
| EVT-G05, EVT-G08 | clock | `MonoNeverBackward` | the Events spec must model wall jumps (matrix G3) |
| OLAP-G02..G05 (T03..T06) | object store | none | `SimObjectStore` is modelled directly by `specs/olap` |

### Topology and failure model

```text
 SimDisk:  node ──calls──> FaultFs volume { OS layer, durable layer, pending dir ops }
                 World::crash (halt volumes, net cut, services dead, abort tasks)
                 World::restart_with(cut) | World::wipe + World::restart
 SimNet:   client node C ──stream (two pipes, per-stream barriers)──> server node S
                 planner (OLTP Router | Cache Network) decides each flush's fate
                 World::black_hole / heal / reset_connection / crash / freeze
 SimNetRpc: caller C ──call(plan)──> target T (duplicate copy, reply plan, router pause)
 SimClock: true time E (incl. pauses) ──> per-node local L = anchor + (E - anchor) * rate
                 wall = L + offset; timers mapped to the runtime clock; SimClockBound per node
```

| Channel or store | Faults modelled | Out of scope, and why |
| --- | --- | --- |
| Volume | crash (halt), kill, clean cut, 512-byte power loss, 4 KiB destroying power loss, fsync EIO, disk loss (reformat, replace), late destructors | write/rename EIO and ENOSPC (refusals with no image effect), bit flips (media, not cut semantics), nested directories (one directory; ancestor durability is unit-tested in `fault.rs`), modes and owners, locks |
| Stream connection | delay, per-stream reordering of frames, loss with stall, reset, partition (hold), heal, crash of either end, freeze of the reader, duplicates (absorbed) | dials, power off (`#106`), byte-level partial reads, buffer limits |
| RPC call | drop, delay, duplicate, lost reply, partition at arrival, router pause, target crash and restart | caller crash (the reply wait is aborted; no effect on the target) |
| Clocks | rate drift and drift changes, reanchoring truncation, wall jumps, monotonic stalls, stop-the-world pauses, sync loss, resync within the base error, drift beyond the bound (E1), zero width (E2) | node crash (the clock continues across restarts), ms rounding in Cache `validate` (product code, `specs/cache`) |

### Spec

| Baseline | Bounds | Why they are enough |
| --- | --- | --- |
| `disk/SimDisk.cfg` | 3 names in one directory, 2 inodes, 2 writes or truncations, 3 directory ops, 1 fsync EIO, 2 crashes; kill, clean cut and 512-byte power loss | 3 names and 3 ops are the shortest broken rename chain; 2 writes give a synced sector next to a dirty one and an intermediate version; 2 crashes give kill then power loss |
| `disk/SimDisk.p4k.cfg` | 2 names, 2 inodes, 2 writes, 2 ops, 1 EIO, 2 crashes; kill and 4 KiB power loss | a synced sector shares its page with a dirty one, alone and after a kill |
| `disk/SimDisk.wipe.cfg` | 2 names, 1 inode, 1 write, 1 op, 2 crashes, 2 wipes, 3 inode bases, every cut | a wipe after a wipe, with and without a restart between |
| `net/SimNet.cfg` | framed or plain, 2 streams, 3 messages, 1 each of drop, duplicate, partition, reset, crash, freeze | 3 messages on 2 streams are the smallest overtaking and the smallest gap |
| `net-rpc/SimNetRpc.cfg` | 3 calls, 1 each of drop, duplicate, block, pause, crash | a duplicate, a reorder and a lost reply need two calls |
| `clock/SimClock.cfg` | 1 ClockBound node, drift in {-1, 0, 1} per 4 ticks, base error 1, 9 ticks, 5 drift changes, 1 resync, 1 pause | 5 changes are the fewest that make the truncation budget matter (mutant `no_reanchor_budget`) |
| `clock/SimClock.timers.cfg` | 1 node, 2 timers, 8 ticks, 2 drift changes, 1 pause, 1 wall jump, 1 monotonic stall | a timer armed before a drift change |

`SimDisk.full.cfg` takes the union of the disk bounds (3 names, 2 writes, 3 ops, 1 EIO, 2 crashes, 1 wipe, every cut),
and `SimClock.joint.cfg` runs the ClockBound faults with a timer (3 drift changes). The smaller baselines were written
first, because the full ones did not finish on a shared laptop. They stay because they are fast to run locally. Two
things are still not combined: 2 wipes run only in `SimDisk.wipe.cfg`, and wall jumps and monotonic stalls only with
timers. The clock baselines have one node each: no action reads two nodes' clocks, so every property is per node.

The model draws a wipe's inode base from bases the volume never used. The shipped guard compares only with the
previous base, so a repeat of an older one is possible there, with a chance of about 2^-20 per wipe.

No fairness: liveness is optional at L1 and is not modelled. `MaxTime` and the budgets are the only state bounds.

T checks: `sim-sim-disk`, `sim-sim-net`, `sim-sim-net-rpc`, `sim-sim-clock` (tier T1).

### Results

The attested runs are named in the pull request body, with the SHA they tested. The counts below are from local runs
(TLC v1.8.0, `-workers 2`), which are not evidence.

| Baseline | Distinct states | Depth | Verdict (local, unattested) |
| --- | --- | --- | --- |
| `SimDisk.cfg` | 283,511 | 17 | pass, exhaustive |
| `SimDisk.p4k.cfg` | 36,577 | 16 | pass, exhaustive |
| `SimDisk.wipe.cfg` | 6,307 | 14 | pass, exhaustive |
| `SimDisk.full.cfg` | 1,078,645 | 19 | pass, exhaustive (Actions) |
| `SimNet.cfg` | 19,080 | 16 | pass, exhaustive |
| `SimNetRpc.cfg` | 210,456 | 21 | pass, exhaustive |
| `SimClock.cfg` | 784,050 | 17 | pass, exhaustive |
| `SimClock.timers.cfg` | 1,491,366 | 16 | pass, exhaustive |
| `SimClock.joint.cfg` | 4,448,286 | 17 | pass, exhaustive (Actions) |

Minimum action coverage (generated states): disk 270 over 17 actions, net 2,364 over 11, net-rpc 10,668 over 13, clock
32,940 over 11. A three-node `SimClock.cfg` stopped at the local time
budget. It is `not-run`. The depth varies by one between runs with several workers.

**What the pass means.** The baselines of `SimClock`, `SimNetRpc` and `SimDisk` model the proposed fix for findings 1,
4 and 5, and `SimNet` models the OLTP planner, not the Cache one (finding 3). On the shipped behaviour those four
properties fail: that is what the `prefix_` configurations show. A green T check here therefore does not say the
shipped simulator satisfies them.

### Mutants

Counterexample lengths are in states. TLC with two workers may report a slightly longer trace than the shortest.

| Spec | Configuration | Kind | Breaks | Length |
| --- | --- | --- | --- | --- |
| disk | `sync_skips_dirty` | mutant | `SyncedSurvives` | 8 |
| disk | `syncdir_noop` | mutant | `SyncedSurvives` | 7 |
| disk | `tear_clean_sectors` | mutant | `SyncedSectorsSurvive` | 8 |
| disk | `garbage_in_512` | mutant | `CrashImageAllowed` | 7 |
| disk | `no_chain_dependency` | mutant | `NoDoubleLink` | 7 |
| disk | `rename_not_atomic` | mutant | `NoDoubleLink` | 6 |
| disk | `rename_skips_pending` | mutant | `OsIsDurablePlusPending` | 3 |
| disk | `abort_before_halt` (P2) | mutant | `NoWriteAfterCut` | 4 |
| disk | `no_incarnation_fence` | mutant | `NoWriteAfterCut` | 6 |
| disk | `wipe_keeps_uuid` | mutant | `UuidAnchorRejectsWipe` | 4 |
| disk | `prefix_wipe_while_up` | finding 5 | `IncarnationSeesOneVolume` | 2 |
| disk | `gap_realistic_reformat` (P5) | gap, finding 2 | `DevInoAnchorRejectsWipe` | 4 |
| disk | `gap_writeback` | gap | `OnlyLatestOrOld` | 8 |
| disk | `reach_4k_destroys_synced` | witness | `SyncedSectorsSurviveAnyCut` | 9 |
| disk | `reach_fsyncgate` | witness | `CleanMeansDurable` | 5 |
| disk | `reach_kill_keeps_unsynced` | witness | `KillImageDurable` | 7 |
| disk | `reach_lost_dir_op` | witness | `UnsyncedOpsSurvive` | 5 |
| disk | `reach_torn_sector` | witness | `NoTornSector` | 6 |
| net | `drop_without_stall` | mutant | `ReceivedIsPrefix` | 5 |
| net | `frames_ignore_stream` | mutant | `ReceivedIsPrefix` | 5 |
| net | `deliver_duplicates` | mutant | `NoDuplicateDelivery` | 6 |
| net | `hole_ignores_in_flight` | mutant | `HoleHoldsInFlight` | 4 |
| net | `reset_keeps_queue` | mutant | `NoDeliveryAfterReset` | 5 |
| net | `prefix_cache_partition_stall` | finding 3 | `HeldNotLost` | 3 |
| net | `gap_crash_sends_fin` | gap | `NoStaleFromCrashed` | 5 |
| net | `reach_frame_overtake` | witness, finding 6 | `ConnectionFifo` | 5 |
| net | `reach_lost_message` | witness | `NothingLost` | 2 |
| net-rpc | `link_checked_at_send_only` | mutant | `NothingCrossesBlockedLink` | 5 |
| net-rpc | `pause_ignored` | mutant | `PausedRunsNothing` | 5 |
| net-rpc | `prefix_dup_crosses_block` | finding 4 | `NothingCrossesBlockedLink` | 5 |
| net-rpc | `reach_lost_reply_applied` | witness | `NoneMeansNotHandled` | 5 |
| net-rpc | `reach_rpc_duplicate` | witness | `AtMostOnceHandling` | 5 |
| net-rpc | `reach_rpc_reorder` | witness | `HandledInOrder` | 7 |
| clock | `error_ages_on_local` | mutant | `ClockAssumption` | 11 |
| clock | `no_reanchor_budget` | mutant | `ClockAssumption` | 9 |
| clock | `reanchor_drops_local` | mutant | `MonoNeverBackward` | 4 |
| clock | `e1_beyond_bound` (E1) | expected failure | `ClockAssumption` | 5 |
| clock | `e2_local_docker` (E2) | expected failure | `ClockAssumption` | 2 |
| clock | `prefix_early_timer` | finding 1 | `TimerNeverEarly` | 6 |

Three of these needed repair before they detected anything: `frames_ignore_stream` first strengthened the rule it
should have dropped, `no_reanchor_budget` needed five drift changes, and two properties were reported by TLC under an
inner operator's name. `TypeOK` has no mutant, by the runner's exemption.

### Counterexamples

Each is a finding about the shipped simulator, recorded in the dr-sim report with its trace. None changes a product
protocol. The simulator wave owns every fix.

| Finding | Shortest trace |
| --- | --- |
| 1 early timer | arm `sleep_until(now + 2)` at drift 0; set drift -1; tick; tick; the timer fires at node time 1, deadline 2 |
| 2 reformat | crash; abort; wipe (reformat): the root's `(dev, ino)` equals the old one |
| 3 Cache partition stall | partition the link; send one message: its plan is `drop`, so the direction stalls for good |
| 4 Events duplicate | send a call planned `duplicate`; it arrives; block the link; the duplicate copy runs the handler |
| 5 wipe while up | wipe a live node: its incarnation now sees another volume |
| 6 frame overtaking | send a frame on stream 1, then one on stream 2; the second is read first |

Findings 1, 3 and 4 are code-level fidelity bugs in the simulator, 2 is a design gap, 5 an API hazard, and 6 a
deliberate choice that is stronger than TCP. All come from reading the code and from the specs. None was reproduced in
a running simulator. Draft bug records for 1 to 4 are in `bugs/`: `scripts/testing/bugs.py` rejects an open record
(`fix_commits = []`), so they stay out of `docs/testing/bugs/` until a fix exists.

### Cost

The simulator's cost per fault, read off the specs' actions:

| Fault | Work in the simulator | Trace lines (`trace-emission.md`) |
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

The independent read is `wave-dr/reads/sim.md` (28 scenarios R-sim-01 to R-sim-28, 40 environment properties, 21
hiding risks). Each scenario is encoded below, or the reason it does not apply is given. "Code" answers an ambiguity
the reader could not resolve from the designs.

| Scenario | Encoding |
| --- | --- |
| R-sim-01 torn writes in cluster runs | `SimDisk!RestartPowerLoss` (both modes); witnesses `reach_torn_sector`, `reach_lost_dir_op`. Harness gap: sim-oltp `DiskCut::Power` uses `Cut::Crash` (matrix S9) |
| R-sim-02 ack while the fsync is in flight | not encodable in a disk model: it is cross-thread concurrency. Code: blocking jobs are atomic (`sim.rs:46-53`), so it is unreachable. Matrix S11 |
| R-sim-03 rename without a directory fsync | `SimDisk!Rename`, `SyncDir`, `RestartCrash`; property `CrashImageAllowed`, witness `reach_lost_dir_op`. Code: `FaultFs::crash()` keeps only the durable namespace, so it drops pending directory ops (ambiguity 1 answered) |
| R-sim-04 `create_dir_all`, `remove_dir_all` under power loss | code: each created directory and each removed entry is its own pending op and its own counted mutation (`fault.rs:1347-1380`, `:1685-1705`), so the fake is not kinder. Nested directories are outside the one-directory model |
| R-sim-05 crash inside a synchronous sequence | `SimDisk!Crash` may follow any call (every `crash_after` boundary). Harness gap: no cluster nemesis uses `crash_volume_after` (matrix S10) |
| R-sim-06 disk lost under a live process | `SimDisk!SyncFails` (EIO while running, fsyncgate) and witness `reach_fsyncgate`. Code: `crash_after` halts the volume and the node keeps running; calls fail with a generic error, not EIO (`fault.rs:1049`) until the harness crashes the node (ambiguity 2 answered) |
| R-sim-07 kill image stays non-durable | `SimDisk!RestartKill` keeps `dirty` and `pending`; witness `reach_kill_keeps_unsynced`; `MaxCrashes = 2` reaches kill then power loss. Code: `FaultFs::kill` keeps both layers (`fault.rs:1136`; ambiguity 1, `process_kill` is `kill`) |
| R-sim-08 reformat reuses inode numbers | gap variant `realistic_reformat`, property `DevInoAnchorRejectsWipe`; `UuidAnchorRejectsWipe` holds. Finding 2. Step 3 does not hold in the code: `verify_directory_anchor` compares with the mount just inspected, not a stored record. One volume per sim-oltp node (matrix S12) |
| R-sim-09 cold start with a replaced device | `SimDisk!Wipe("replace")` then `RestartFresh`. Harness gap: `cold_start` asserts "cold start requires retained volumes" (#106 `node.rs:363`) |
| R-sim-10 full disk | not modelled: ENOSPC is a refusal with no image effect. Code: `CAPACITY_BYTES` is fixed at 64 GiB and nothing enforces it; L injects ENOSPC by rule. Matrix G2 |
| R-sim-11 zombie I/O from a dead incarnation | `SimDisk!ZombieWrite`, `AbortTasks`; property `NoWriteAfterCut`, mutants `no_incarnation_fence`, `abort_before_halt`. Code: paths resolve only for the current incarnation (`volumes.rs:110-119`, `:386-395`) |
| R-sim-12 minority leader with a gateway | product protocol (`specs/oltp`). Clock half: matrix S4 (no OLTP bound stated) |
| R-sim-13 asymmetric partitions | `SimNet!Partition` holds one direction. Code: a dial hangs if either direction is held (`net.rs:767`); an established connection keeps streaming the open direction without a window (matrix section 5) |
| R-sim-14 lost reply then auth flap | `SimNet!Send` drop on the reply direction; auth flap is `World::override_certificate` (API, not modelled) |
| R-sim-15 same ID through two gateways, dedup horizon | product and checker (`specs/oltp`); no environment property |
| R-sim-16 stale frame after a split | `SimNet!BecomeReady` delays and `HoleHoldsInFlight`; held frames arrive after the heal (OLTP router) |
| R-sim-17 reset while the handler runs | `SimNet!ResetConnection` clears the pipes only; the server task is product code and keeps running (`net.rs:342`) |
| R-sim-18 kill versus power off | `SimNet!CrashClient` (reset), gap `NoStaleFromCrashed` with variant `crash_sends_fin`. Code (#106): power off black-holes sessions, refuses new dials at once, and never resets old sessions after the restart (matrix G7) |
| R-sim-19 pause between clock sample and effect | not injectable: freeze acts at `.await` points (matrix S6). `SimClock` has no such action because the sim has none |
| R-sim-20 adversarial ±e placement | `SimClock!Resync` with `r = ±BaseError` on two bound nodes; `ClockAssumption` holds |
| R-sim-21 drift beyond the bound, controller drift | mutants `e1_beyond_bound`, `e2_local_docker`. Controller drift is not injected (matrix S5) |
| R-sim-22 backward resync within e, loss of sync | `SimClock!Resync` with negative residual, `SetSynchronized`; `MonoNeverBackward` holds (the wall clock moves, the monotonic clock does not) |
| R-sim-23 wall jumps (O09) | `SimClock!JumpWall`; `MonoNeverBackward` |
| R-sim-24 OLTP lease beyond the bound | matrix S4 (no OLTP twin) |
| R-sim-25 stop-the-world sleep | `SimClock!PauseAll`; timers armed before it fire late; matrix G4 |
| R-sim-26 crash from a probe | not checked beyond #106, which has no probe stream; `SimDisk!Crash` is atomic, which matches a probe crash only if it takes effect at the next poll. Recorded for P10-PROBES |
| R-sim-27 statics across incarnations | not an environment fault. Code (#106): `World::track_registry`, `live_registries` check between seeds only |
| R-sim-28 blocking jobs never overlap | as R-sim-02: structural, not encodable here (matrix S11) |

Environment properties from the read: DISK-1 `SyncedSurvives`, `SyncedSectorsSurvive`; DISK-2 `CrashImageAllowed`
(plus gap `OnlyLatestOrOld`); DISK-3 `RestartPowerLoss`; DISK-4 `NoDoubleLink`, `CrashImageAllowed`; DISK-5 the
closure rule `Closed` (FaultFs keeps a dependency-closed set, stronger than a journal prefix; matrix section 5);
DISK-6 `PowerBytes` keeps data and names apart; DISK-7 witness `reach_kill_keeps_unsynced`; DISK-8 `Crash` and
`SyncFails`; DISK-9 `NoWriteAfterCut`; DISK-10 not modelled (locks); DISK-11 gap `DevInoAnchorRejectsWipe`;
DISK-12 not modelled (matrix G2); DISK-13 not modelled (`list` returns `BTreeMap` order, sorted: a code-level
weakness the read predicted); DISK-14 the README states the target: POSIX plus a dependency-closed namespace, which
is stronger than ext4 `data=ordered`; DISK-15..18 not environment faults. NET-1 `ReceivedIsPrefix` (per stream;
witness `reach_frame_overtake` shows the sim is stronger than NET-1 across streams); NET-2 `NoDuplicateDelivery`;
NET-3 finding 6; NET-4 dial rule (code); NET-5 not modelled (no transport timeout; callers' deadlines reset); NET-6
`HoleHoldsInFlight`, `NothingCrossesBlockedLink` (finding 4); NET-7 matrix G7; NET-8 `CrashClient` resets; NET-9
not modelled; NET-10 `Send` drop stalls; NET-11 not modelled (HTTP byte cuts); NET-12 `Freeze`, `PausedRunsNothing`;
NET-13 determinism (not a TLA+ property). CLK-1 `MonoNeverBackward`; CLK-2 `TimerNeverEarly` (finding 1); CLK-3
`ClockAssumption`; CLK-4 `E` is the one truth; CLK-5 not modelled; CLK-6 matrix S6 and G4; CLK-7 E1, E2 mutants;
CLK-8 `SetSynchronized`, `ErrorBound`; CLK-9 not modelled.

## Fault-model conformance matrix

This is the same matrix as the design review wave's `sim-mapping/fault-model.md`. The trace-emission design is in
`sim-mapping/trace-emission.md`. Findings are numbered as in section 6 below.

### 1. What the simulator can inject

"Harness" says which shipped harness uses the fault: O = OLTP (`server/sim-oltp`, PR #106), C = Cache
(`server/src/cache/cluster/sim`), E = Events (`server/events/src/sim`), L = the LSM campaign
(`server/kv/tests/campaign`), P = the OLTP pool component tests (`server/oltp-storage/src/oltp/pool/sim_tests.rs`),
S = OLAP stream (`server/src/node/stream/sim_tests.rs`), A = OLAP commit (`server/src/storage/commit_protocol_tests.rs`).
"API" means the seam or harness offers it and no workload drives it yet.

**Corrected 16:40.** The first version marked many OLTP rows "today" from the harness API. The merged OLTP
workload drives only: a full stop and cold start with the clean cut (`DiskCut::Power`, one focused test), process
kill at teardown, per-seed drift, and wall jumps. Its network profiles are exercised by component tests
(`sim-oltp/src/net/tests.rs`: frame cut, one-stream delay, one-shot fallback, HTTP over freeze and crash). Nothing
drives partitions, filters, freeze, gateway kill or wipe in a cluster seed yet (P10-WORKLOADS). Events, the LSM
campaign and the Cache controllers do not use World volumes: Events and LSM own their `FaultFs`, and Cache
controllers write a real directory on `/dev/shm`.

#### Disk (`SimDisk`)

| Fault | Seam entry point | Harness | Spec action |
| --- | --- | --- | --- |
| Process kill: every completed write survives, unsynced data stays unsynced | `World::restart_with(Cut::Kill)`, `FaultFs::kill` | L, P; O only at teardown, never reopened | `RestartKill` |
| Clean cut: only synced data and synced directory entries survive | `Cut::Crash`, `FaultFs::crash` | O (one cold-start test), E, L, P | `RestartCrash` |
| Power loss, 512-byte sectors: each unsynced sector new, old or torn; each unsynced directory op kept or lost, closed under entry dependencies; unsynced length kept or not | `Cut::PowerLoss`, `FaultFs::power_loss` | L only (P, O, E never use it) | `RestartPowerLoss("power512")` |
| Power loss, 4 KiB pages destroyed whole (also new, old, torn, zeros, garbage); takes synced neighbours of a dirty sector with it | `Cut::PowerLoss4k`, `power_loss_with(destroy_4k)` | L | `RestartPowerLoss("power4k")` |
| Crash at the n-th mutating call (NS04 enumeration) | `FaultFs::crash_after`, `World::crash_volume_after` | L, P | `Crash` at any call boundary |
| fsync EIO (fsyncgate: dirty pages dropped, a later success does not persist them) | `Fault::on(SyncData/SyncAll/SyncDir)` | L | `SyncFails` |
| Write, read, rename EIO; ENOSPC (one call or repeated) | `Fault::on(op).error(..)` | L | not modelled (no data effect beyond refusal) |
| Bit flip in both layers | `FaultFs::flip_bit` | L | not modelled |
| Disk loss, restart without the disk (reformat: same device, new uuid, new inode base; replace: new device) | `World::wipe`, then `World::restart` | P; O has `DiskCut::Wipe` and no caller; E has its own `wipe` behind `SimConfig::wipes`, never enabled | `Wipe`, `RestartFresh` |
| A wiped data mount next to a surviving key mount (RV-REP #3) | two `World::mount`s, `World::wipe` of one | P only; O mounts one volume per node | (one volume in `SimDisk`) |
| Crash order: network, volumes, services, then tasks, in one synchronous call; late destructors fail by incarnation | `World::crash` / `cut_node`, `World::resolve_node_path` | O, P, Cache workers (network and tasks). E halts its own volume first, then calls `World::crash` | `Crash`, `AbortTasks`, `ZombieWrite` |

#### Network (`SimNet`, `SimNetRpc`)

| Fault | Seam entry point | Harness | Spec action |
| --- | --- | --- | --- |
| Delay per message; FIFO per connection for plain connections | `Planner::message` -> `Fate::Deliver{delay}` | O, C | `Send`, `BecomeReady` |
| Frames of different streams overtake each other on one session ("one-stream delay") | `Fate::Frame{stream, delay}` | O: every worker frame takes this path, with the `NetPlan` delay (filters are clear in the current workload) | `BecomeReady` (barrier per stream) |
| Loss = the direction stalls until reset (TCP never delivers past a lost segment) | `Fate::Drop` | C (filters up to 5%); O component tests only | `Send` with plan `drop` |
| Connection reset by the planner, a profile, the World, or a crash | `Fate::Reset`, `World::reset_connection`, `net::crash` | C and O (crash); the planner and World resets are API | `ResetConnection`, `CrashClient`, `CrashServer` |
| Partition (directed hold: in-flight and later messages wait, then arrive in order) | `World::black_hole`, `heal`, `NetPlan::block` | C (stalls the connection, see finding 3); O API | `Partition`, `Heal` |
| Dial refused or hung | `Planner::dial` | C (hang while blocked or paused); O API | not modelled (R-sim-18) |
| Duplicate | `NetPlan` plans it; the stream routers absorb it (TCP) | none on streams | `Send` with plan `duplicate`, absorbed |
| Process pause (SIGSTOP): tasks not polled, clocks run | `World::freeze`, `thaw` | C (`pause`), O (API only, `#[allow(dead_code)]`) | `Freeze`, `Thaw` |
| Authentication flap / impersonation | `World::override_certificate` | API only | not modelled |
| Power off: sessions black-holed forever, new dials refused at once | `World::power_off` | O (the cold-start test) | not modelled |
| Message-level RPC: drop until the deadline, delay, duplicate (handler runs twice), lost reply after apply, link checked at arrival, router pause | `events/src/sim/net.rs` `call` over `NetPlan` | E | `SimNetRpc` |

#### Clock (`SimClock`)

| Fault | Seam entry point | Harness | Spec action |
| --- | --- | --- | --- |
| Per-node rate drift (fixed per seed) | `NodeClock::drift_ppm` | O (sampled per node), C (gradual drift) | `SetDrift` at time 0 |
| Drift change mid-run | `World::set_drift_ppm`, `SimClockBound::set_drift_ppm` | C (nemesis `drift`) | `SetDrift` |
| Wall-clock jump forward or back (O09) | `World::jump_wall_*` | O (config), E (nemesis) | `JumpWall` |
| Monotonic stall forward | `World::advance_mono` | E | `AdvanceMono` |
| Stop-the-world pause (every node's clocks jump, timers fire late) | `World::pause_all`, `thread_sleep` | all (implicitly, from synchronous sleeps) | `PauseAll` |
| ClockBound interval: base error, declared drift, resync within base, loss of sync | `SimClockBound` | C | `Resync`, `SetSynchronized`, `ClockAssumption` |
| Drift beyond the bound (E1), LocalDocker zero width (E2) | harness config | C (mutants.rs) | mutants `e1_beyond_bound`, `e2_local_docker` |

#### Object store (OLAP, not a World seam)

`SimObjectStore` (`server/src/storage/sim.rs`): status faults (412, 409, 404, 503), `LostResponse` (applied, reply
lost), `DelayApply` (applies after the caller's deadline), `Hold` (applies when released), `EmbeddedError` (a 200 that
did not apply). Harness A, and S for GET 503s. It has no TLA+ model in `specs/sim/`; `specs/olap/OlapCommit.tla`
models the store directly.

### 2. Per module

Status: **today** = a shipped harness injects it; **API** = the seam can, no harness does; **planned** = a phase 10
to 12 design commits to it; **no** = not injectable.

#### oltp: `OltpDirect` (dr-oltp-direct) and `OltpRanges` (dr-oltp-ranges), L3

| Spec fault | Simulator | Gap |
| --- | --- | --- |
| `CrashNode` / `Crash`, `Restart` with the disk | API for one node (`Cluster::crash`, `disk::reopen`); today only for all workers at once (`cold_start`) | **harness gap**: no single-node crash and restart under load (P10-WORKLOADS `crash-leader`) |
| `RestartWithoutDisk` / `Wipe` | API (`DiskCut::Wipe`, no caller); P at component level | **harness gap**; one volume per node, so data wiped with keys kept is P only |
| `Partition` (one node isolated), `Heal` | API (`update_plan(isolate)` gives held links) | **harness gap** until P10-WORKLOADS. Asymmetric partitions stream without a window (section 5) |
| `LoseMessage` / `LoseMsg` (one message lost from a bag) | component tests, as a connection stall: every later message on that direction is lost until a reset | **spec stronger than the sim and than TCP** for two messages on one connection. A spec trace that loses one message and delivers a later one maps to two connections |
| `DuplicateMessage` / `Redeliver` (a message delivered twice, later) | **no** on streams: duplicates are absorbed. Only application retries on a new stream, session or gateway duplicate | **sim gap** (deliberate, TCP-faithful). Spec duplicates map to retries; a network-level duplicate of a Raft or direct frame is not injectable |
| reordering (bag or set channel) | API across sessions; across streams of one session in component tests | reordering within one stream is not injectable (spec stronger) |
| `KillGateway` | API (`Cluster::crash` of a gateway node) | **harness gap** |
| `FullStop` | today (O `cold_start_reopens_mounted_workers`, clean cut) | torn images never occur: `DiskCut::Power` is `Cut::Crash` (R-sim-01, H-02) |
| `MoveLeader`, `StepDown`, `RaftElect` | product behaviour under faults; leader transfer through operator ops is planned (P10 `leader-move`) | planned |
| `FlipDirectOltp`, `UpgradeNode` (mixed versions, flag flip mid-run) | **no**: `direct_oltp` is sampled per seed; one binary | **sim gap**: the rollout properties are checked only in the spec |
| `DdlFence`, splits | planned (P11-TOPO) | planned |
| `Drift(n, d)` (OltpRanges: a bounded offset, steps of ±1 tick, may step a local clock backward) | partly: per-node rate drift fixed per seed, wall jumps. No mid-run drift change in O, no stated OLTP bound, no beyond-bound twin | **mismatch**: the sim's offset grows with run length (rate model) while the spec bounds the offset; the spec lets a monotonic clock step back, which neither the sim nor a kernel does. State the OLTP lease drift bound (R-sim-12, R-sim-24, H-17) |
| torn or lost unsynced log tail on restart (rubric) | API (`Cut::PowerLoss*`); O does not use it | **harness gap** (R-sim-01) |
| fsync EIO, ENOSPC, bit flip (OLTP-G01 failure model) | API only (L uses them) | **harness gap**: no OLTP seed sees a sync failure |

#### queue: `OltpQueue` (dr-queue), L3

| Spec fault | Simulator | Gap |
| --- | --- | --- |
| `Crash(w)` (RAM lost), `Restart(w)` | API | P11-QUEUED not started |
| `Partition(w)`, `Heal(w)` (a live worker cut off from Control and peers) | API | P11-QUEUED not started |
| `StopAll`, `ColdStart` (full stop, abandonment) | today for a full stop with no queued work outstanding | the abandonment case (pending requests) has no workload |
| lost reply to the client (`MaxLostReply`) | API (HTTP profile cut) | P11-QUEUED not started |
| lost install acknowledgment (`MaxLostAck`) | API (frame profile cut on the reply direction) | the cut stalls the whole session direction, not one acknowledgment |
| transient lane refusal (`MaxLane`) | partly: lane and slot limits are per-seed knobs (`SimLimits`); no injected one-off refusal | **sim gap**: a refusal at a chosen step needs a probe-triggered limit |
| receipt lookup failure (`MaxReceiptFault`) | API (HTTP cut, API restart) | P11-QUEUED not started |
| view changes, rejoin with a stale copy (O14, #103) | planned (P11-QUEUED, MEMBER) | planned |

#### lsm: `Lsm` (dr-lsm), L3

| Spec fault | Simulator | Gap |
| --- | --- | --- |
| `Kill` | today (L) | none |
| `PowerLoss`: unsynced slots new, old or torn; a page mixing synced and unsynced bytes lost; each pending directory entry kept or lost **independently** | today (L: `power_loss_with`, 512 and 4 KiB) | **mismatch**: FaultFs keeps pending directory ops only as a dependency-closed set (`fault.rs:1185-1200`), so a lost rename never leaves a file under two names. The Lsm spec's independent choice is stronger than FaultFs (and than any journalled filesystem); a counterexample that needs a broken rename chain is not reproducible |
| a crash at every step, including recovery | today (L: `crash_after` enumeration, NS04) | none |
| media damage of a synced record (`MaxMedia`: bit flip or EIO) | today (L: `flip_bit`, read EIO by range) | none |
| disk loss (`MaxDiskLoss`) | not driven: the campaign has no wipe step (`CutKind` is Crash, Kill, PowerLoss, PowerLoss4k) | **harness gap** |
| fsync EIO | today (L: `EioSyncData`, `EioSyncAll`, `EioSyncDir`) | **spec gap**: `Lsm.tla` states "no ENOSPC or fsync errors" |
| ENOSPC, write and rename EIO | today (L) | **spec gap** (same note in `Lsm.tla`) |

#### cache: `CacheCluster` (dr-cache), L2

| Spec fault | Simulator | Gap |
| --- | --- | --- |
| `Crash(w)`, `Restart(w)` (new incarnation, empty RAM), `Replace(w)` | today for crash and restart (C `CrashWorker`, `RestartWorker`). `Replace` is not in the nemesis | **harness gap** for `Replace` |
| `CutCtrl(w)`, `CutPeer(w)`, `HealCtrl`, `HealPeer` | today (C `link`, `isolate_controller_links`) | the Cache planner turns every message sent across a blocked link into a stalled connection (finding 3): a renewal sent during the cut is lost for good, not delivered after the heal |
| `DropPacket(w, s)` | today (C filters: drop -> stall) | none (the spec's per-packet drop is stronger) |
| `HoldReply`, `DeliverHeld` | today (delay up to 60 ms, black hole, freeze) | none |
| `LeaderChange` | today (crash the controller leader) | none |
| `DriftWorker(w, d)` | today (C `drift`, in bound) | the drift change makes earlier timers fire early on the node clock (finding 1) |
| `DriftController(d)` | partly: controllers carry a `SimClockBound`, but the nemesis drifts workers only | **sim gap**: no controller drift, in or beyond the bound (R-sim-21) |
| drift beyond `MaxDrift` (E1), LocalDocker (E2) | today (C mutants) | none |
| process pause between the clock sample and the effect | **no**: freeze stops a node only at `.await` points | **sim gap** (R-sim-19, H-04) |
| controller disk loss | deferred (lead decision 4: controller log on `/dev/shm`) | `not-run` until the controller uses a modelled volume |

#### events: `EventsLog` and `EventsDelivery` (dr-events), L2

| Spec fault | Simulator | Gap |
| --- | --- | --- |
| `Crash(n)`, `Restart(n)` (EventsLog) | today (E: broker removed, `FaultFs::crash()` image) | E never uses a kill or a power-loss image: unsynced data never survives a crash, and never tears |
| `RestartWithoutDisk(n)` (EventsLog, `MaxWipes`; mutant `prefix_empty_disk_rejoins_voter`) | API (`SimConfig::wipes`, no test enables it) | **harness gap**, and it blocks a seeded check of events R-04 |
| `MaxStopAll` (full stop) | API (`Cluster::stop_all`); not checked whether a test drives it mid-run | confirm with the simulator wave |
| partition, `Isolate`, `Heal` | today (E isolate, block) | none |
| `DropRequest`, `DropReply`, duplicates | today (E: drop until the deadline, duplicate copy, delays) | the duplicate copy crosses a link blocked after the original arrived (finding 4) |
| `ClockBack(n)` (EventsDelivery) | today (E wall jumps) | none for a backward step. Monotonic stalls (`advance_mono`) have no spec action |
| `LoseLease`, consumer crash, failover | not checked | confirm with dr-events |
| process pause | **no**: router pause only, the broker keeps running | **sim gap** if a spec adds it |

#### olap: `OlapCommit` (dr-olap), L2

| Spec fault | Simulator | Gap |
| --- | --- | --- |
| `CrashWriter` | partly (A drops the coordinator once, in the planned-readback T04 case) | no seeded writer crash at every step |
| `LoseReply` (applied, reply lost) | today (`Fault::LostResponse`) | none |
| `PutDelayTimeout` (applies after the deadline) | today (`Fault::DelayApply`) | none |
| `SchedulerTakeover`, `ColdStart` | `ColdStart` fence today (A T05). I found no same-run takeover in `commit_protocol_tests.rs` | **sim gap** for `SchedulerTakeover` |
| 5xx, 412, 409, 404, embedded error | today (`Fault::Status`, `EmbeddedError`) | OlapCommit models an ambiguous root PUT, which covers 5xx and the embedded error |
| object-store network partition, process pause | **no** (in-process store) | not needed by the store semantics |

#### olap-stream: `OlapStream` (dr-olap-stream), L2

| Spec fault | Simulator | Gap |
| --- | --- | --- |
| `Crash(n)`, `Takeover`, `ColdStart`, `Restart` | today (S: drop `Local`, fenced install, cold-start transition) | none |
| `DropReply` | today (S: lost reply) | none |
| `DropRequest`, `DropInstall`, duplicate install (`MaxDup`), `Partition`, `Heal` | **no**: S calls the coordinator in process; peer transfer runs only in process tests | **sim gap**: replica-transfer loss, duplication and partitions have no seeded executor |
| `SealExpire` | today (S cold start abandons old handles) | none |
| GET 503 from the object store | today (S) | **spec gap**: I found no failed read in `OlapStream.tla` |

### 3. Gaps in the simulator (a spec models it, the sim cannot inject it)

| # | Fault | Specs that model it | Where it would go | Status |
| --- | --- | --- | --- | --- |
| S1 | Network duplicate of one frame on a stream | OltpDirect, OltpRanges, OlapStream | not on a TCP stream (reality cannot either); map spec duplicates to retries in the mapping files | by design |
| S2 | Lone message loss with later messages delivered on the same connection | OltpDirect, OltpRanges, CacheCluster | same as S1 | by design |
| S3 | Mixed binaries, `direct_oltp` flip mid-run | OltpDirect | a per-node feature switch through `known_features_for_test` (P11-MEMBER) and a runtime flag reload | **no** |
| S4 | OLTP drift within a stated bound, mid-run drift change, beyond-bound twin | OltpRanges | `World::set_drift_ppm` from the O nemesis (`clock` profile, P10-WORKLOADS) | planned, bound unstated |
| S5 | Controller drift (Cache) | CacheCluster | `SimClockBound::set_drift_ppm` on controller nodes in the C nemesis | **no** |
| S6 | Pause between a clock sample and its effect | CacheCluster (lease), OltpRanges (lease reads) | a seeded advance of true time after `hooks::clock_bound` and `rt::time::now` return (R-sim-19) | **no** |
| S7 | A one-off lane or slot refusal at a chosen step | OltpQueue | a probe-triggered resource limit | **no** |
| S8 | Replica-transfer loss, duplication and partitions for OLAP stream | OlapStream | a World-based stream workload | **no** |
| S9 | Torn and partially kept unsynced writes in cluster runs | OltpDirect (rubric), OltpRanges (`Wipe`), Events | `DiskCut::Power` -> `Cut::PowerLoss{seed}` (`sim-oltp/src/disk.rs`); Events `volume.fs.crash()` -> `power_loss(seed)` | API, unused |
| S10 | A crash inside a synchronous multi-file sequence in cluster runs | Lsm (component), OltpDirect (install) | `crash_volume_after` from the O nemesis, or probe-triggered | API, unused (R-sim-05) |
| S11 | An ack sent while the fsync is still in flight (cross-thread) | OltpDirect (`MajorityReached` shape) | a suspension point at every `sync_*` in simulated blocking jobs (R-sim-02, H-01) | **no**: blocking jobs are atomic |
| S12 | Data wipe with surviving keys in the cluster | OltpRanges (`Wipe`, `Revoke`) | two mounts per O node (`/sim/nX/keys`, `/sim/nX/data`) | P only |

### 4. Gaps in the specs (the sim injects it, no spec models it)

| # | Fault the sim injects | Harness | Specs that should model it, or say why not |
| --- | --- | --- | --- |
| G1 | fsync EIO with fsyncgate semantics | L | Lsm (LSM-G01 failure model names "sync failure"); OLTP-G01 names it too |
| G2 | ENOSPC, write and rename EIO | L | Lsm |
| G3 | Wall-clock jumps and monotonic stalls | O (wall jumps), E (both) | Events (EVT-G05, EVT-G08), OltpDirect (O09) |
| G4 | Stop-the-world pauses from synchronous sleeps (every node's timers fire late at once) | all | none model it; it is a simulator artifact (H-14). Specs may ignore it, but liveness bounds must not be set from runs full of it |
| G5 | Frames of different streams overtaking each other inside one session | O | OltpDirect, OltpRanges model a bag, so they allow it; but no real TCP session does it (finding 6). Specs are fine; the sim is stronger than reality |
| G6 | Session stall after one drop (all later frames on the direction lost until a reset) | O, C | specs model independent loss; a stall is a sequence of losses, so it is covered, but the specs' liveness must allow a reset-and-reconnect cycle |
| G7 | Power off: sessions black-holed, never reset after the node restarts; dials refused at once | O (#106) | OltpDirect, OltpRanges, OltpQueue liveness assumes timely failure detection; the refusal is faster than reality, the half-open session slower (R-sim-18, H-13) |
| G8 | 4 KiB page destruction that takes synced bytes | L | Lsm models it (`DestroyablePages`). OLTP and Events specs do not, and their harnesses do not inject it |
| G9 | Duplicate RPC handler execution (message level) | E | the Events spec must model duplicates (planned) |
| G10 | `World::wipe` on a live node (the API allows it) | no cluster harness; `sim/volumes/tests.rs:464` does it in a unit test | `SimDisk` mutant `prefix_wipe_while_up` shows the hazard (finding 5) |

### 5. Where the simulator is weaker or stronger than reality

"Weaker" means a real bug can pass in simulation. "Stronger" means the simulator can produce a behaviour no real
system can, which risks false failures or product changes for impossible cases.

| Seam | Weaker than reality | Stronger than reality |
| --- | --- | --- |
| Disk | Never persists an intermediate unsynced version of a sector (only old or latest; `SimDisk` gap `OnlyLatestOrOld`, variant `writeback`). Never repeats inode numbers after a reformat (finding 2). Newly allocated extents read back as zeros, never stale data (ext4 `data=writeback`). An in-flight fsync never overlaps other work (H-01). A `crash_after` halt reports a generic error, not EIO | Pending directory ops survive as any dependency-closed subset, not a journal prefix (ext4, XFS). A rename into a directory whose creation was lost makes the file vanish from both names (the closure tracks entry keys, not the parent's link). 4 KiB destroy mode loses synced neighbours (a device-level fault, beyond POSIX) |
| Network | A crash discards what the node already sent (a killed process's kernel sends it, then FIN; `SimNet` gap `NoStaleFromCrashed`). Power-off sessions never turn into resets after the restart. Dials to a down node are refused at once, even after a power off (fast failure detection) | Frames of one session overtake each other across streams (finding 6). A one-way partition lets the open direction stream without a window (TCP stops after one window without ACKs). Events duplicates whole HTTP requests at the network level. The Cache planner turns a partition into a permanent connection stall (finding 3) |
| Clock | Freeze acts only at `.await` points, so there is no pause between a clock sample and its effect (R-sim-19). OLTP drift is fixed per seed | A timer armed before a drift change fires before its node clock reaches the deadline (finding 1). Stop-the-world pauses delay every node at once (H-14) |

### 6. Findings that affect the matrix

Numbered as in `specs/sim/README.md` and the dr-sim report:

1. **Early timers after a drift change** (SimClock, code-level fidelity bug in `rt::sim`): `sleep_until` maps the
   node deadline to the runtime clock once, at arming; a later slowdown (`World::set_drift_ppm`) leaves the old
   mapping, so the timer fires while the node clock still reads before the deadline. The module doc claims "a timer
   never fires before the node clock reaches its deadline". Reachable in Cache sweeps (`tests.rs:217` drifts
   workers mid-run). Small: at 200 ppm declared drift a change of up to 400 ppm moves a 3 s timer by about 1.2 ms. Mutant `prefix_early_timer` holds the counterexample.
2. **Reformat never repeats inode numbers** (SimDisk, fidelity gap in the phase 9 design and `World::wipe`): a real
   `mkfs` gives its root a fixed inode number (2 on ext4), so a reformat of the same device repeats the root's
   `(dev, ino)`. The sim always draws a new base, the phase 9 design lists the realistic case as planted bug P5, and
   `oltp-pool-sim` asserts `assert_ne!(root_inode, old_inode)`. **Product impact: none found.** The stored-inode
   comparison (`pool/mount.rs:585`, reopen permit) runs after a filesystem-uuid equality check (`:575`), and
   `verify_directory_anchor` (`registered.rs:881`) compares the locked directory with the mount inspected moments
   earlier, not with a stored record. So step 3 of R-sim-08 does not hold in the code. I read these paths only; it is
   not an audit of every inode comparison. The sim should still inject the realistic case, so a future check that
   relies on the inode alone fails in simulation.
3. **Cache planner stalls connections on a partition** (SimNet, code-level fidelity bug in the Cache harness):
   `harness.rs:115` maps every `Plan::Drop`, including the drop `NetPlan` plans for a blocked link, to `Fate::Drop`,
   which stalls the direction for good. The OLTP Router maps it to `Pass` and lets the World hole hold the message.
   A Cache renewal sent during a partition is therefore lost, never delivered late after the heal; the stale
   delivery after heal that K03's lease rules must survive is reachable only for messages already in flight. Mutant
   `prefix_cache_partition_stall`.
4. **Events duplicate crosses a partition** (SimNetRpc, code-level fidelity bug): the duplicate copy runs 1 ms
   after the original arrived and never checks the link again (`events/src/sim/net.rs:56-64`). A request duplicate
   can be handled across a partition that opened after the original arrived. Mutant `prefix_dup_crosses_block`.
5. **`World::wipe` accepts a live node** (SimDisk, API hazard): the live incarnation silently continues on the empty
   replacement volume, because `NodeFs` resolves every call for the current incarnation. No cluster harness does this
   today. One rt unit test does (`sim/volumes/tests.rs:464`), so an added assertion must update it. Mutant
   `prefix_wipe_while_up`.
6. **Frames overtake inside one session** (SimNet, stronger than TCP): `Fate::Frame` lets one stream's frames be
   delayed while other streams pass on the same connection (#101 "one-stream delay"). Witness
   `reach_frame_overtake`.

### 7. Restart under the same stable identity (lead request, 15:50)

Two independent reads point at one fault class: a node restarts with an empty disk, or with an older disk, under the
same stable identity, then votes again (events R-04; oltp-ranges R-06, R-07, R-14). The simulator's answer per
module:

| Fault | OLTP Data and Control voters | Events brokers | Cache controllers | Cache workers |
| --- | --- | --- | --- | --- |
| **Restart, empty disk, same identity** | **API, not driven.** `disk::reopen(DiskCut::Wipe)` (`sim-oltp/src/disk.rs`) wipes the one pool volume (`Wipe::Replace`) and restarts; `start_workers` rewrites the same worker bootstrap (`config.write_worker(index)`) and the World keeps the node's certificate digest, so it should come back under its old identity with no log (read from the code, not run). No workload calls it (`cold_start` asserts "cold start requires retained volumes"). Because the keys live on the same volume, the RV-REP #3 shape (data wiped, keys kept) needs two mounts, which only the pool component tests have | **API, disabled.** `Cluster::wipe` (`events/src/sim/cluster.rs:277`) installs a fresh volume under the same broker id; the nemesis draws it only when `SimConfig::wipes` is set, and no test sets it | **No.** The controller log and snapshots use a real temporary directory (`/dev/shm`); lead decision 4 defers controller disk loss, so this stays `not-run` | Not applicable: workers keep no disk; every restart is empty RAM under a new incarnation by design |
| **Restart, stale disk, same identity** (an older image of the node's own volume) | **No.** No World API reinstalls an earlier image. `FaultFs` can produce one (`kill()` or `crash()` copies kept by the harness), but `World` has no "install image" call; `restart_with` only cuts the current image | **No.** Same: the harness could keep an old `FaultFs` copy in `volume.at_start` but never restores an older one | **No** (deferred as above) | Not applicable |

Proposed seam: `World::restore(node, path, image: FaultFs)` that installs a saved image while the node is down,
keeping the device identity (a snapshot-restore or a stale PVC), plus a harness action that keeps an image taken at a
cut and restores it after later commits. In `SimDisk` terms this is a `Restore` action whose image is an earlier
durable state of the same volume (not modelled yet: the shipped simulator cannot do it).

### 8. Verdicts on the early findings posted for the simulator wave

| Lead | Verdict | Evidence in the shipped code |
| --- | --- | --- |
| H-01: storage jobs are atomic; no crash point between a write, its fsync and the acknowledgement | **Confirmed.** `task::spawn_blocking` runs the closure to completion on the helper while the simulation thread waits (`rt/src/sim.rs:46-53`, `run_blocking`); `World::crash` runs on the simulation thread, so it can only land between jobs (read from the code, not run). Inside a job only a pre-armed `crash_after(n)` halts the volume at an operation, and the node keeps running until the harness crashes it. A cross-thread ack-before-fsync bug cannot fail. Not modelled in `SimDisk` (concurrency, not disk semantics); matrix S11 |
| H-04: freeze or pause cannot land between the clock read and the serve | **Confirmed.** `World::freeze` gates polls (`Gate`, `sim.rs:740`): a frozen node stops at its next `.await`. A sample-then-serve path with no await in between has a zero pause window. The Cache `pause` is `freeze` plus `NetPlan::pause` (`harness.rs:390`); Events' pause is a router pause only. Matrix S6 |
| H-03: P10 per-frame network faults are stronger than TCP | **Partly confirmed.** Refuted for loss and duplication: a planned drop or a profile `Cut` stalls the whole direction until a reset (`Fate::Drop`, `net.rs:1066`), and the OLTP Router absorbs duplicates (`sim-oltp/src/net/mod.rs:209`). Confirmed for reordering: `Fate::Frame{stream, delay}` lets frames of other streams overtake within one session ("one-stream delay", #101), which TCP cannot do (finding 6, witness `reach_frame_overtake`) |
| P10 has one volume per worker | **Confirmed.** #106 `sim-oltp/src/disk.rs` mounts one `ROOT` per server process; keys, bootstrap and pool share it. The two-mount RV-REP #3 shape exists only in `oltp-storage/src/oltp/pool/sim_tests.rs` |
| P2 is misframed | **Refuted for P2; confirmed for P5.** P2 (`World::crash` aborts tasks before halting volumes) is a real simulator-level defect with a detector (`sim/volumes/tests.rs:292` `no_mutating_call_reaches_a_volume_after_a_crash`) and a spec mutant (`disk/mutants/abort_before_halt.cfg`, `NoWriteAfterCut`). The independent read's "misframed" verdict is about **P5** ("`Wipe::Reformat` keeps the old inode base"), which is the realistic behaviour of `mkfs`: finding 2. Note: #106 changes the crash order to network first, then volumes (`cut_node`); both happen in one synchronous call, so no task runs in between and the order does not matter for P2 |

### 9. Planted bugs and reverted fixes of phases 9 to 12: design level or code level

**Design level** means a guard or an order is removed from the protocol, so a spec mutant should reject it. **Code
level** means the rule is right and an implementation detail is wrong, so only the simulator or a test can see it.
**Simulator level** plants a defect in the fault model or the harness. I classified from the designs, the bug records'
titles and the independent read, and did not read the fix diffs. Rows marked "?" need them. The last column is a spec
mutant that exists in another agent's worktree now and matches by name. I did not run those mutants.

| Phase | Design level | Code level | Simulator level | Matching spec mutants seen |
| --- | --- | --- | --- | --- |
| 9 | R1, R2?, P1 | R3, R4 | P2, P3, P4, P6. P5 is the real behaviour of `mkfs`, not a defect | R1 `lsm/no_open_dir_sync`; P1 `lsm/flush_no_journal_sync`; R2 `lsm/retire_before_edit`?; P2 `sim/disk/abort_before_halt` |
| 10 | BUG1, FIXA, D04, ACC3 O15 (liveness), AZ2 (liveness) | spares BUG1-encode-direct, BUG1-pre-append, FIXA-unproposed? | none | not compared (dr-oltp-direct's mutants were not in its folder yet) |
| 11 uncertain outcome | 1 ACC3-receipt | 2 BUG1-encode-submit, 3 BUG1-stream | | not compared |
| 11 stale route, topology, schema | 4 TOPO1, 5 BUG2, 6 RD1 | 7 IDXHOT?, 8 O21CAP? | | TOPO1 `oltp/child_stale_schema` |
| 11 fence, lifecycle | 9 ACC2, 10 ACC3-ready-fence, 11 ACC3-delegation, 12 FIXC, 13 FIXB-cold-publish | | | not compared |
| 11 membership, trust | 14 REP2, 15 RV-REP-3, 17 CAP1 | 16 FIXB-voter-refresh, 18 RV-REP-2 | | REP2 `oltp/rep2_any_removal`; RV-REP-3 `oltp/wiped_resumes` |
| 11 resource release | 20 RD2? | 19 FLAKE3-epoch, 21 QLANE, 22 GC1-double-guard | | none |
| 12 | P1, P2 (a coarse stand-in: it drops every delete, not the SET-after-DEL race the plan names), P3, P4, both lead decision 6 defects | | E1, E2 are expected failures of the fault model | P1 `cache/no_uncertainty`; P2 `cache/replica_keeps_deleted`; P3 `cache/owner_change_keeps_epoch`; P4 `cache/cold_reset_ignores_live_lease`; E1 `cache/drift_beyond_bound`, `sim/clock/e1_beyond_bound`; LD 6 `cache/staged_ignores_fence`, `cache/staged_ignores_lag` |

The code-level rows depend most on interleaving and fault granularity. H-01 (atomic storage jobs) is the main risk for
FLAKE3-epoch and GC1-double-guard, whose windows may not exist in the simulator.

#### Simulator invariants with no spec property yet

Compared by name with the specs on disk at 17:00. A "no" means I found no property with that meaning.

| Simulator invariant | Spec property |
| --- | --- |
| `SlotOwnership`, `LeaseExclusive`, `ServedEpochOrder`, `ReplicaPrefix`, `ColdStartEmpty`, `ClockAssumption` | yes (`CacheCluster`; `ClockAssumption` also here) |
| `ReplicaNotAhead`, `Convergence` (Cache) | no (`G05_ReplicaPrefix` may imply the first; ask dr-cache) |
| `MemoryCap`, `RedirectAgrees` (Cache) | no. Reasons to leave them out: a capacity count, and client routing |
| `SingleLeader`, `MonotonicCommit`, `AppliedPrefixesAgree` (Cache controllers) | no: the controllers are abstracted as one log |
| `ElectionSafety`, `AppliedPrefixesAgree` (Events) | yes (`EventsLog`) |
| `ElectionSafety`, `MonotonicVote` (OLTP) | yes (`OltpRanges` one leader and one vote per term) |
| `LogMatching`, `MajorityReached` (OLTP) | partly (`G01_CommittedNeverLost`) |
| `KeyspaceCoverage` (OLTP) | no property with that name in `OltpRanges` |
| `WipedNeverVotes`, `RetryCertifiesNamedRemoval` | yes in effect (`OltpRanges` mutants `wiped_resumes`, `rep2_any_removal`) |
| `SchemaEpochAtCommit` | partly (`OltpRanges` mutants `child_stale_schema`, `ddl_publish_first`) |
| `CoveringIndexComplete`, `FenceAfterValidation` (index), `CapabilityBeforeUse` | no |
| `DedupUnique`, `NotAppliedNeverCommitted`, `RangeClockRule1`, `QueueRetention`, `OneExecutionPerIdentity`, `QueueCopyStaleNotInvalid` | `OltpQueue` had `QueueRetention`, `G03_OnceExecution`, `G07_NoStaleRecord` and `G04_NotAppliedHasProof` at 15:30. I did not compare `OltpDirect` |
| `ResourceBaseline`, `ExactlyOnceRelease`, `ReleaseAfterWork` | `OltpQueue` had `G09_SettleOnce`; nothing models the ledgers or native guards |
| `CrashOrder`, `MountedDeviceIdentity` (harness) | `NoWriteAfterCut`, `UuidAnchorRejectsWipe` here |
