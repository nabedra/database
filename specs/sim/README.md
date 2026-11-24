# Simulator fault model (`specs/sim`)

Module `sim`, level L1 (`docs/testing/modules.toml`). These specs model the deterministic simulator itself: the
environment that every module spec and every S cell assumes. They are faithful to the shipped simulator code
(`origin/main` `7dadd41d`, plus open PR #106 where noted), not to a plan, because the simulator has no plan in
`plans/`. Its designs live outside the repository (`designs/phase-9-disk-seam.md`, `phase-10-11-oltp-sim.md`,
`phase-12-kv-cluster-sim.md`, `lead-decisions.md`). This file carries the design review section the skill asks of a
plan, the fault-model conformance matrix, and the vocabulary both waves use for fault steps in traces.

| Spec | Models | Code |
| --- | --- | --- |
| `SimDisk.tla` | FaultFs and the volume layer: write, set_len, fsync (and its EIO), create, rename, remove, directory fsync; crash, abort and late destructors; restart with a kill, clean-cut or power-loss image (512-byte and 4 KiB modes); disk loss and restart without the disk | `server/rt/src/fs/fault.rs`, `server/rt/src/sim/volumes.rs`, `World::crash` in `server/rt/src/sim.rs` |
| `SimNet.tla` | The TCP-like stream seam: per-connection pipes, delay and per-stream frame order, loss as a stall, reset, partition as a hold, heal, crash, freeze, with the OLTP or the Cache planner | `server/rt/src/sim/net.rs`, `server/sim/src/net.rs`, `server/sim-oltp/src/net/mod.rs`, `server/src/cache/cluster/sim/harness.rs` |
| `SimNetRpc.tla` | The Events message router: drop until the deadline, delay, duplicate copy, lost reply after apply, link checked at arrival, router pause, target crash | `server/events/src/sim/net.rs`, `server/sim/src/net.rs` |
| `SimClock.tla` | Node clocks (rate drift, reanchoring, wall jumps, monotonic stalls, stop-the-world pauses), timers mapped to the runtime clock, and `SimClockBound` | `server/rt/src/sim.rs`, `server/rt/src/time.rs`, `server/src/cache/cluster/sim/clock_bound.rs` |

Each spec has `MC<Name>.tla`, a T1 baseline `<Name>.cfg` (SimDisk also `SimDisk.wipe.cfg`), and mutants in
`mutants/<Name>_<mutant>.cfg`. Mutant files carry the spec name as a prefix because four specs share this folder;
`tla_check.py` must select `mutants/<Name>_*.cfg` for `<Name>` (see the dr-sim report).

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
| OLTP-G08 (O17 precondition, RV-REP #3) | disk | `UuidAnchorRejectsWipe`, gap `DevInoAnchorRejectsWipe` | two mounts per node: one volume in the model (README "bounds") |
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

| Spec | Bounds (T1, local) | Why they are enough | Fairness |
| --- | --- | --- | --- |
| `SimDisk.cfg` | 3 names in one directory, 2 inodes, 2 sector writes or truncations, 3 directory ops, 1 fsync EIO, 2 crashes, every cut, no wipe | 3 names and 3 ops are the shortest broken rename chain (`a` created, `a`→`b`, `b`→`c`); 2 writes give a synced sector next to a dirty one, an intermediate version, and a dropped page plus rewrite; 2 crashes give kill then power loss (R-sim-07) | none (safety only) |
| `SimDisk.wipe.cfg` | 2 names, 1 inode, 1 write, 1 op, 2 crashes, 2 wipes, 3 bases | two wipes of one volume give a reformat after a reformat, and a wipe with and without an earlier crash | none |
| `SimNet.cfg` | framed and plain (chosen at Init), 2 streams, 3 messages, 1 random drop, 1 duplicate, 1 partition, 1 reset, 1 crash, 1 freeze | 3 messages on 2 streams are the smallest overtaking and the smallest gap after a drop; one of each fault | none |
| `SimNetRpc.cfg` | 3 calls, 1 drop, 1 duplicate, 1 block, 1 pause, 1 crash | a duplicate, a reorder and a lost reply each need two calls; one of each fault | none |
| `SimClock.cfg` | 3 nodes (2 with a ClockBound), drift in {-1, 0, 1} per 4 ticks, base error 1, 9 ticks, 2 drift changes, 1 resync, 1 pause, 1 wall jump, 1 monotonic stall, 2 timers | 2 drift changes give a timer armed across a change and the per-reanchor truncation; 9 ticks let the error grow past the base | none |

The time bound (`MaxTime`) and the budgets are the only state constraints. Liveness is optional at L1 and not
modelled: the simulator's own progress (a run finishes, a stalled connection is reset by the caller's deadline) is
harness behaviour, checked by the sweep's liveness bounds.

Planned T checks, once `dr-tooling` lands kind T (do not add the files before that):

| Check | Command | Tier |
| --- | --- | --- |
| `sim-sim-disk` | `python3 scripts/testing/tla_check.py specs/sim SimDisk` | T1 |
| `sim-sim-net` | `python3 scripts/testing/tla_check.py specs/sim SimNet` | T1 |
| `sim-sim-net-rpc` | `python3 scripts/testing/tla_check.py specs/sim SimNetRpc` | T1 |
| `sim-sim-clock` | `python3 scripts/testing/tla_check.py specs/sim SimClock` | T1 |

### Results

Local, unattested: `/home/dev/bin/tlc-local` (TLC v1.8.0, CommunityModules 202611060952), `-workers 2`, on a machine
shared with eight other TLC runs. See the dr-sim report for the logs. No attested run exists yet (`not-run:
tooling-not-on-main`).

RESULTS_TABLE

### Mutants

MUTANTS_TABLE

### Counterexamples

Each is a finding about the shipped simulator, recorded in the dr-sim report with its trace. None changes a product
protocol. The simulator wave owns every fix.

COUNTEREXAMPLES

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

RECONCILE

## Fault-model conformance matrix

The matrix, the vocabulary of fault steps and the trace-emission design are in the design review wave's
`sim-mapping/fault-model.md` and `sim-mapping/trace-emission.md`. The summary:

MATRIX
