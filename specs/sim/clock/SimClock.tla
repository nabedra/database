------------------------------ MODULE SimClock ------------------------------
(***************************************************************************)
(* The simulator's clocks: per-node monotonic and wall clocks with drift,  *)
(* stop-the-world pauses, wall jumps, timers mapped to the runtime clock,  *)
(* and the Cache harness's SimClockBound, as shipped on main 7dadd41d.     *)
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
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Nodes,           \* simulated nodes
    BoundNodes,      \* nodes whose wall clock carries a SimClockBound (Cache workers)
    Scale,           \* stands for PPM
    DeclaredDrift,   \* BoundSpec::declared_drift_ppm, in units of 1/Scale
    Drifts,          \* drifts a harness may set; |d| <= DeclaredDrift in bound
    BaseError,       \* BoundSpec::base_error_ns
    MaxTime,         \* true-time horizon (a state constraint, see README)
    MaxDriftChanges, MaxResyncs, MaxPauses, MaxWallJumps, MaxMonoSteps, MaxTimers,
    Mutant,          \* "none", or a mutant name
    Variant          \* "shipped", "rearm_on_wake" (proposed fix), "local_docker" (E2)

VARIABLES
    E,        \* World::elapsed: true virtual time, pauses included
    paused,   \* World::paused: stop-the-world time the runtime clock never ran
    clk,      \* node -> [av, al, d, w]: Clock anchors, drift, wall offset
    bound,    \* node -> [syncedAt, syncedLocal, reanchors, synced]
    timers,   \* armed timers: [node, deadline (node clock), fireRT (runtime clock)]
    budget

vars == <<E, paused, clk, bound, timers, budget>>

Abs(x) == IF x < 0 THEN -x ELSE x
Max(a, b) == IF a > b THEN a ELSE b
CeilDiv(a, b) == IF a <= 0 THEN -((-a) \div b) ELSE (a + b - 1) \div b   \* b > 0

\* Clock::local (sim.rs:212): the node's monotonic elapsed time at true time e.
LocalAt(c, e) == c.al + ((Max(e - c.av, 0)) * (Scale + c.d)) \div Scale
Local(n) == LocalAt(clk[n], E)
\* wall_now (sim.rs:914), with the epoch at 0
Reading(n) == Local(n) + clk[n].w
\* Clock::virtual_at (sim.rs:222): the earliest true time with local >= l.
VirtualAt(c, l) ==
    IF l - c.al <= 0 THEN c.av
    ELSE c.av + CeilDiv((l - c.al) * Scale, Scale + c.d)
Runtime == E - paused

\* SimClockBound::sample (clock_bound.rs:33): the error bound now.
Age(n) == IF Mutant = "error_ages_on_local" THEN Local(n) - bound[n].syncedLocal
          ELSE E - bound[n].syncedAt
ErrorBound(n) ==
    IF Variant = "local_docker" THEN 0
    ELSE BaseError + CeilDiv(Age(n) * DeclaredDrift, Scale)
         + (IF Mutant = "no_reanchor_budget" THEN 0 ELSE bound[n].reanchors)

TypeOK ==
    /\ E \in Nat /\ paused \in Nat /\ paused <= E
    /\ clk \in [Nodes -> [av : Nat, al : Nat, d : Drifts, w : Int]]
    /\ bound \in [Nodes -> [syncedAt : Nat, syncedLocal : Nat, reanchors : Nat, synced : BOOLEAN]]
    /\ timers \subseteq [node : Nodes, deadline : Nat, fireRT : Int]
    /\ budget \in [drift : Nat, resync : Nat, pause : Nat, jump : Nat, mono : Nat, timers : Nat]

(* World::add_node with NodeClock::default, then the harness's boot resync *)
(* (harness.rs:211-238): residual 0 at true time 0.                        *)
Init ==
    /\ E = 0
    /\ paused = 0
    /\ clk = [n \in Nodes |-> [av |-> 0, al |-> 0, d |-> 0, w |-> 0]]
    /\ bound = [n \in Nodes |-> [syncedAt |-> 0, syncedLocal |-> 0, reanchors |-> 0, synced |-> n \in BoundNodes]]
    /\ timers = {}
    /\ budget = [drift |-> 0, resync |-> 0, pause |-> 0, jump |-> 0, mono |-> 0, timers |-> 0]

Due(t) == Runtime >= t.fireRT

\* The paused runtime advances one tick. tokio fires a due timer before time moves on.
Tick ==
    /\ E < MaxTime
    /\ ~\E t \in timers : Due(t)
    /\ E' = E + 1
    /\ UNCHANGED <<paused, clk, bound, timers, budget>>

\* World::pause_all (sim.rs:637): every node clock moves, the runtime clock does not.
PauseAll(p) ==
    /\ budget.pause < MaxPauses
    /\ E + p <= MaxTime
    /\ E' = E + p
    /\ paused' = paused + p
    /\ budget' = [budget EXCEPT !.pause = @ + 1]
    /\ UNCHANGED <<clk, bound, timers>>

\* SimClockBound::set_drift_ppm -> World::set_drift_ppm (sim.rs:395): reanchor, then
\* the new rate. The bound budgets one tick per reanchor.
SetDrift(n, d) ==
    /\ budget.drift < MaxDriftChanges
    /\ d # clk[n].d
    /\ clk' = [clk EXCEPT ![n] = [av |-> Max(@.av, E),
                                  al |-> IF Mutant = "reanchor_drops_local" THEN @.al ELSE Local(n),
                                  d |-> d, w |-> @.w]]
    /\ bound' = [bound EXCEPT ![n].reanchors = @ + 1]
    /\ budget' = [budget EXCEPT !.drift = @ + 1]
    /\ UNCHANGED <<E, paused, timers>>

\* SimClockBound::resync (clock_bound.rs:99): step the wall clock to truth plus a
\* residual within the base error, restart the error.
Resync(n, r) ==
    /\ n \in BoundNodes
    /\ budget.resync < MaxResyncs
    /\ Abs(r) <= BaseError
    /\ clk' = [clk EXCEPT ![n].w = E + r - Local(n)]
    /\ bound' = [bound EXCEPT ![n] = [syncedAt |-> E, syncedLocal |-> Local(n), reanchors |-> 0, synced |-> TRUE]]
    /\ budget' = [budget EXCEPT !.resync = @ + 1]
    /\ UNCHANGED <<E, paused, timers>>

\* SimClockBound::set_synchronized: a node that loses sync stops serving (sample errs).
SetSynchronized(n, s) ==
    /\ n \in BoundNodes
    /\ bound[n].synced # s
    /\ bound' = [bound EXCEPT ![n].synced = s]
    /\ UNCHANGED <<E, paused, clk, timers, budget>>

\* World::jump_wall_forward and jump_wall_backward (O09). Used on OLTP and Events nodes;
\* the Cache harness steps a ClockBound node's wall clock only through resync.
JumpWall(n, j) ==
    /\ n \notin BoundNodes
    /\ budget.jump < MaxWallJumps
    /\ clk' = [clk EXCEPT ![n].w = @ + j]
    /\ budget' = [budget EXCEPT !.jump = @ + 1]
    /\ UNCHANGED <<E, paused, bound, timers>>

\* World::advance_mono (sim.rs:405): a stall moves the monotonic clock forward (Events).
AdvanceMono(n, by) ==
    /\ n \notin BoundNodes
    /\ budget.mono < MaxMonoSteps
    /\ clk' = [clk EXCEPT ![n] = [av |-> Max(@.av, E), al |-> Local(n) + by, d |-> @.d, w |-> @.w]]
    /\ budget' = [budget EXCEPT !.mono = @ + 1]
    /\ UNCHANGED <<E, paused, bound, timers>>

\* time::sleep_until(now + delta) on node n (time.rs:148, sim.rs:930): the deadline is
\* read on the node clock and mapped once, at arming, to the runtime clock.
ArmSleepUntil(n, delta) ==
    /\ budget.timers < MaxTimers
    /\ LET deadline == Local(n) + delta
       IN timers' = timers \cup {[node |-> n, deadline |-> deadline,
                                  fireRT |-> VirtualAt(clk[n], deadline) - paused]}
    /\ budget' = [budget EXCEPT !.timers = @ + 1]
    /\ UNCHANGED <<E, paused, clk, bound>>

\* time::sleep(delta) (time.rs:134, sim.rs:954): the duration is scaled by the rate now.
ArmSleep(n, delta) ==
    /\ budget.timers < MaxTimers
    /\ timers' = timers \cup {[node |-> n, deadline |-> Local(n) + delta,
                               fireRT |-> Runtime + IF clk[n].d = 0 THEN delta
                                                   ELSE CeilDiv(delta * Scale, Scale + clk[n].d)]}
    /\ budget' = [budget EXCEPT !.timers = @ + 1]
    /\ UNCHANGED <<E, paused, clk, bound>>

\* tokio fires the timer when the runtime clock reaches it. Variant rearm_on_wake is the
\* proposed fix: a wake before the node clock reaches the deadline maps it again.
Fire(t) ==
    /\ t \in timers
    /\ Due(t)
    /\ IF Variant = "rearm_on_wake" /\ Local(t.node) < t.deadline
       THEN timers' = (timers \ {t}) \cup
                {[t EXCEPT !.fireRT = VirtualAt(clk[t.node], t.deadline) - paused]}
       ELSE timers' = timers \ {t}
    /\ UNCHANGED <<E, paused, clk, bound, budget>>

\* The horizon: time stops at MaxTime, so a timer due later never fires.
Done ==
    /\ E = MaxTime
    /\ UNCHANGED vars

FireAny == \E t \in timers : Fire(t)

Next ==
    \/ Tick
    \/ \E p \in 1..2 : PauseAll(p)
    \/ \E n \in Nodes, d \in Drifts : SetDrift(n, d)
    \/ \E n \in Nodes, r \in -BaseError..BaseError : Resync(n, r)
    \/ \E n \in Nodes, s \in BOOLEAN : SetSynchronized(n, s)
    \/ \E n \in Nodes, j \in {-2, 2} : JumpWall(n, j)
    \/ \E n \in Nodes : AdvanceMono(n, 1)
    \/ \E n \in Nodes, delta \in 2..4 : ArmSleepUntil(n, delta)
    \/ \E n \in Nodes, delta \in 2..4 : ArmSleep(n, delta)
    \/ FireAny
    \/ Done

Spec == Init /\ [][Next]_vars

(* ----------------------------------------------------------------------- *)
(* Properties                                                              *)
(* ----------------------------------------------------------------------- *)

\* time::now never goes backward (time.rs:3-6).
MonoNeverBackward ==
    [][\A n \in Nodes : LocalAt(clk'[n], E') >= LocalAt(clk[n], E)]_vars

\* CACHE-G06 precondition, the harness's ClockAssumption (phase 12 section 3.11): a
\* synchronized node's interval contains true time while drift stays within the bound.
ClockAssumption ==
    \A n \in BoundNodes :
        bound[n].synced => Abs(Reading(n) - E) <= ErrorBound(n)

\* A timer never fires before its node's clock reaches the deadline, as on a real
\* kernel (sim.rs:29-31 claims it).
TimerNeverEarly ==
    [][\A t \in timers :
         LET completed == ~\E u \in timers' : u.node = t.node /\ u.deadline = t.deadline
         IN completed => Local(t.node) >= t.deadline]_vars

=============================================================================
