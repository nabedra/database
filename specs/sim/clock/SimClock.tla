------------------------------ MODULE SimClock ------------------------------
(***************************************************************************)
(* The simulator's clocks: per-node monotonic and wall clocks with drift,  *)
(* stop-the-world pauses, wall jumps, timers mapped to the runtime clock,  *)
(* and the Cache harness's SimClockBound. Written from main 7dadd41d and   *)
(* checked again on main e6b4040b (rv-sim, 2026-11-26).                    *)
(*                                                                         *)
(*   server/rt/src/sim.rs      Clock::{local, virtual_at, reanchor},       *)
(*                             World::{set_drift_ppm, advance_mono,        *)
(*                             jump_wall_*, pause_all}, to_runtime_*       *)
(*   server/rt/src/time.rs     sleep, sleep_until (the timer mapping)      *)
(*   server/src/cache/cluster/sim/clock_bound.rs  SimClockBound            *)
(*                                                                         *)
(* Design: phase-12-kv-cluster-sim.md section 3.3; phase-10-11 section 2. *)
(* Scenarios: K03 clock items, E1, E2, O09. Guarantees that assume it:     *)
(* CACHE-G06 (the 10 ms bound), OLTP-G02 (O09), EVT-G05, EVT-G08.          *)
(*                                                                         *)
(* Time is an integer number of ticks. A drift d means the node clock runs *)
(* at (Scale + d) / Scale of true time, the ppm arithmetic of Clock with    *)
(* Scale standing for 1,000,000. Integer division truncates as the code's  *)
(* i128 arithmetic does.                                                   *)
(*                                                                         *)
(* The Cache harness samples the bound and serves in one poll: nothing     *)
(* else runs and virtual time does not move between the sample and the     *)
(* serve (Sample, Serve). Mutant gap_pause_after_sample is the real        *)
(* environment, where a process can stop between the two (R-sim-19, H-04).*)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Nodes,           \* simulated nodes
    BoundNodes,      \* nodes whose wall clock carries a SimClockBound (Cache workers)
    Scale,           \* stands for PPM
    DeclaredDrift,   \* BoundSpec::declared_drift_ppm, in units of 1/Scale
    Drifts,          \* drifts a harness may set; |d| <= DeclaredDrift in bound
    BaseError,       \* BoundSpec::base_error_ns
    MaxTime,         \* true-time horizon (a budget: Tick and PauseAll stop there)
    MaxDriftChanges, MaxResyncs, MaxPauses, MaxWallJumps, MaxMonoSteps, MaxTimers,
    Mutant           \* "none", or a mutant name (see mutants/ and the README)

VARIABLES
    E,        \* World::elapsed: true virtual time, pauses included
    paused,   \* World::paused: stop-the-world time the runtime clock never ran
    clk,      \* node -> [av, al, d, w]: Clock anchors, drift, wall offset
    bound,    \* node -> [syncedAt, syncedLocal, reanchors, synced]
    timers,   \* armed timers: [node, deadline (node clock), fireRT (runtime clock)]
    sample,   \* node -> the ClockBound sample a poll holds: [held, reading, err, truth]
    budget

vars == <<E, paused, clk, bound, timers, sample, budget>>

Abs(x) == IF x < 0 THEN -x ELSE x
Max(a, b) == IF a > b THEN a ELSE b
CeilDiv(a, b) == IF a <= 0 THEN -((-a) \div b) ELSE (a + b - 1) \div b   \* b > 0

\* Drifts a harness can set. Mutant e1_beyond_bound is phase 12's expected failure E1:
\* the actual drift is three times the declared one.
DriftChoices ==
    IF Mutant = "e1_beyond_bound" THEN Drifts \cup {-3 * DeclaredDrift, 3 * DeclaredDrift}
    ELSE Drifts

\* Clock::local (sim.rs): the node's monotonic elapsed time at true time e.
LocalAt(c, e) == c.al + ((Max(e - c.av, 0)) * (Scale + c.d)) \div Scale
Local(n) == LocalAt(clk[n], E)
\* wall_now, with the epoch at 0
Reading(n) == Local(n) + clk[n].w
\* Clock::virtual_at: the earliest true time with local >= l.
VirtualAt(c, l) ==
    IF l - c.al <= 0 THEN c.av
    ELSE c.av + CeilDiv((l - c.al) * Scale, Scale + c.d)
Runtime == E - paused

\* SimClockBound::sample (clock_bound.rs): the error bound now. Mutant
\* error_ages_on_local ages it on the node clock, which a slow clock understates;
\* no_reanchor_budget drops the one tick per reanchor; e2_local_docker is phase 12's
\* expected failure E2 (LocalDocker reports a zero-width interval).
Age(n) == IF Mutant = "error_ages_on_local" THEN Local(n) - bound[n].syncedLocal
          ELSE E - bound[n].syncedAt
ErrorBound(n) ==
    IF Mutant = "e2_local_docker" THEN 0
    ELSE BaseError + CeilDiv(Age(n) * DeclaredDrift, Scale)
         + (IF Mutant = "no_reanchor_budget" THEN 0 ELSE bound[n].reanchors)

NoSample == [held |-> FALSE, reading |-> 0, err |-> 0, truth |-> 0]

TypeOK ==
    /\ E \in Nat /\ paused \in Nat /\ paused <= E
    /\ clk \in [Nodes -> [av : Nat, al : Nat, d : -3 * DeclaredDrift..3 * DeclaredDrift, w : Int]]
    /\ bound \in [Nodes -> [syncedAt : Nat, syncedLocal : Nat, reanchors : Nat, synced : BOOLEAN]]
    /\ timers \subseteq [node : Nodes, deadline : Nat, fireRT : Int]
    /\ sample \in [Nodes -> [held : BOOLEAN, reading : Int, err : Int, truth : Nat]]
    /\ budget \in [drift : Nat, resync : Nat, pause : Nat, jump : Nat, mono : Nat, timers : Nat]

(* World::add_node with NodeClock::default, then the harness's boot resync *)
(* (harness.rs): residual 0 at true time 0.                                *)
Init ==
    /\ E = 0
    /\ paused = 0
    /\ clk = [n \in Nodes |-> [av |-> 0, al |-> 0, d |-> 0, w |-> 0]]
    /\ bound = [n \in Nodes |-> [syncedAt |-> 0, syncedLocal |-> 0, reanchors |-> 0, synced |-> n \in BoundNodes]]
    /\ timers = {}
    /\ sample = [n \in Nodes |-> NoSample]
    /\ budget = [drift |-> 0, resync |-> 0, pause |-> 0, jump |-> 0, mono |-> 0, timers |-> 0]

Due(t) == Runtime >= t.fireRT

\* A poll that holds a sample runs to its serve before anything else happens: the
\* simulation thread polls one task at a time, and World::freeze acts only at the next
\* .await (the Gate in sim.rs). Mutant gap_pause_after_sample lifts this, as reality does.
InPoll == \E n \in Nodes : sample[n].held
Between == ~InPoll \/ Mutant = "gap_pause_after_sample"

\* The paused runtime advances one tick. tokio fires a due timer before time moves on.
Tick ==
    /\ Between
    /\ E < MaxTime
    /\ ~\E t \in timers : Due(t)
    /\ E' = E + 1
    /\ UNCHANGED <<paused, clk, bound, timers, sample, budget>>

\* World::pause_all: every node clock moves, the runtime clock does not.
PauseAll(p) ==
    /\ Between
    /\ budget.pause < MaxPauses
    /\ E + p <= MaxTime
    /\ E' = E + p
    /\ paused' = paused + p
    /\ budget' = [budget EXCEPT !.pause = @ + 1]
    /\ UNCHANGED <<clk, bound, timers, sample>>

\* SimClockBound::set_drift_ppm -> World::set_drift_ppm: reanchor, then the new rate.
\* The bound budgets one tick per reanchor.
SetDrift(n, d) ==
    /\ Between
    /\ budget.drift < MaxDriftChanges
    /\ d # clk[n].d
    /\ clk' = [clk EXCEPT ![n] = [av |-> Max(@.av, E),
                                  al |-> IF Mutant = "reanchor_drops_local" THEN @.al ELSE Local(n),
                                  d |-> d, w |-> @.w]]
    /\ bound' = [bound EXCEPT ![n].reanchors = @ + 1]
    /\ budget' = [budget EXCEPT !.drift = @ + 1]
    /\ UNCHANGED <<E, paused, timers, sample>>

\* SimClockBound::resync: step the wall clock to truth plus a residual within the base
\* error, restart the error.
Resync(n, r) ==
    /\ Between
    /\ n \in BoundNodes
    /\ budget.resync < MaxResyncs
    /\ Abs(r) <= BaseError
    /\ clk' = [clk EXCEPT ![n].w = E + r - Local(n)]
    /\ bound' = [bound EXCEPT ![n] = [syncedAt |-> E, syncedLocal |-> Local(n), reanchors |-> 0, synced |-> TRUE]]
    /\ budget' = [budget EXCEPT !.resync = @ + 1]
    /\ UNCHANGED <<E, paused, timers, sample>>

\* SimClockBound::set_synchronized: a node that loses sync stops serving (sample errs).
SetSynchronized(n, s) ==
    /\ Between
    /\ n \in BoundNodes
    /\ bound[n].synced # s
    /\ bound' = [bound EXCEPT ![n].synced = s]
    /\ UNCHANGED <<E, paused, clk, timers, sample, budget>>

\* World::jump_wall_forward and jump_wall_backward (O09). Used on OLTP and Events nodes;
\* the Cache harness steps a ClockBound node's wall clock only through resync.
JumpWall(n, j) ==
    /\ Between
    /\ n \notin BoundNodes
    /\ budget.jump < MaxWallJumps
    /\ clk' = [clk EXCEPT ![n].w = @ + j]
    /\ budget' = [budget EXCEPT !.jump = @ + 1]
    /\ UNCHANGED <<E, paused, bound, timers, sample>>

\* World::advance_mono: a stall moves the monotonic clock forward (Events).
AdvanceMono(n, by) ==
    /\ Between
    /\ n \notin BoundNodes
    /\ budget.mono < MaxMonoSteps
    /\ clk' = [clk EXCEPT ![n] = [av |-> Max(@.av, E), al |-> Local(n) + by, d |-> @.d, w |-> @.w]]
    /\ budget' = [budget EXCEPT !.mono = @ + 1]
    /\ UNCHANGED <<E, paused, bound, timers, sample>>

\* hooks::clock_bound -> SimClockBound::sample on a synchronized Cache worker: the lease
\* check reads (reading, error), and the poll keeps the answer until it serves.
Sample(n) ==
    /\ Between
    /\ n \in BoundNodes
    /\ bound[n].synced
    /\ ~sample[n].held
    /\ sample' = [sample EXCEPT ![n] = [held |-> TRUE, reading |-> Reading(n),
                                        err |-> ErrorBound(n), truth |-> E]]
    /\ UNCHANGED <<E, paused, clk, bound, timers, budget>>

\* The same poll applies the command and replies: the effect of the lease check.
Serve(n) ==
    /\ sample[n].held
    /\ sample' = [sample EXCEPT ![n] = NoSample]
    /\ UNCHANGED <<E, paused, clk, bound, timers, budget>>

\* time::sleep_until(now + delta) on node n (time.rs, sim.rs): the deadline is read on
\* the node clock and mapped to the runtime clock.
ArmSleepUntil(n, delta) ==
    /\ Between
    /\ budget.timers < MaxTimers
    /\ LET deadline == Local(n) + delta
       IN timers' = timers \cup {[node |-> n, deadline |-> deadline,
                                  fireRT |-> VirtualAt(clk[n], deadline) - paused]}
    /\ budget' = [budget EXCEPT !.timers = @ + 1]
    /\ UNCHANGED <<E, paused, clk, bound, sample>>

\* time::sleep(delta): the duration is scaled by the rate now.
ArmSleep(n, delta) ==
    /\ Between
    /\ budget.timers < MaxTimers
    /\ timers' = timers \cup {[node |-> n, deadline |-> Local(n) + delta,
                               fireRT |-> Runtime + IF clk[n].d = 0 THEN delta
                                                   ELSE CeilDiv(delta * Scale, Scale + clk[n].d)]}
    /\ budget' = [budget EXCEPT !.timers = @ + 1]
    /\ UNCHANGED <<E, paused, clk, bound, sample>>

\* tokio fires the timer when the runtime clock reaches it. A wake before the node clock
\* reaches the deadline maps it again (the fix of SIM-early-timer). Mutant
\* prefix_early_timer keeps the first mapping, as main 7dadd41d did.
Fire(t) ==
    /\ Between
    /\ t \in timers
    /\ Due(t)
    /\ IF Mutant # "prefix_early_timer" /\ Local(t.node) < t.deadline
       THEN timers' = (timers \ {t}) \cup
                {[t EXCEPT !.fireRT = VirtualAt(clk[t.node], t.deadline) - paused]}
       ELSE timers' = timers \ {t}
    /\ UNCHANGED <<E, paused, clk, bound, sample, budget>>

\* The horizon: time stops at MaxTime, so a timer due later never fires.
Done ==
    /\ E = MaxTime
    /\ ~InPoll
    /\ UNCHANGED vars

FireAny == \E t \in timers : Fire(t)

Next ==
    \/ Tick
    \/ \E p \in 1..2 : PauseAll(p)
    \/ \E n \in Nodes, d \in DriftChoices : SetDrift(n, d)
    \/ \E n \in Nodes, r \in -BaseError..BaseError : Resync(n, r)
    \/ \E n \in Nodes, s \in BOOLEAN : SetSynchronized(n, s)
    \/ \E n \in Nodes, j \in {-2, 2} : JumpWall(n, j)
    \/ \E n \in Nodes : AdvanceMono(n, 1)
    \/ \E n \in Nodes : Sample(n)
    \/ \E n \in Nodes : Serve(n)
    \/ \E n \in Nodes, delta \in 2..4 : ArmSleepUntil(n, delta)
    \/ \E n \in Nodes, delta \in 2..4 : ArmSleep(n, delta)
    \/ FireAny
    \/ Done

Spec == Init /\ [][Next]_vars

(* ----------------------------------------------------------------------- *)
(* Properties                                                              *)
(* ----------------------------------------------------------------------- *)

\* time::now never goes backward (time.rs).
MonoNeverBackward ==
    [][\A n \in Nodes : LocalAt(clk'[n], E') >= LocalAt(clk[n], E)]_vars

\* CACHE-G06 precondition, the harness's ClockAssumption (phase 12 section 3.11): every
\* interval a synchronized node samples contains true time while drift stays in bound.
ClockAssumption ==
    \A n \in Nodes : sample[n].held => Abs(sample[n].reading - sample[n].truth) <= sample[n].err

\* The shipped simulator serves at the true time it sampled: no pause, freeze or tick
\* lands between a lease check and its effect. True of the simulator and false of a real
\* process (GC, SIGSTOP, VM steal): mutant gap_pause_after_sample.
SampleFreshAtServe ==
    [][\A n \in Nodes : (sample[n].held /\ ~sample'[n].held) => E = sample[n].truth]_vars

\* A timer never fires before its node's clock reaches the deadline, as on a real
\* kernel (the sim.rs module doc claims it).
TimerNeverEarly ==
    [][\A t \in timers :
         LET completed == ~\E u \in timers' : u.node = t.node /\ u.deadline = t.deadline
         IN completed => Local(t.node) >= t.deadline]_vars

=============================================================================
