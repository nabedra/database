---------------------------- MODULE CacheCluster ----------------------------
(***************************************************************************)
(* Design spec of the Cache cluster (module `cache`, level L2).            *)
(*                                                                         *)
(* Plan: plans/cache.md, "Replication, fencing and recovery" (topology     *)
(* authority, leases, replication, failover and restart, recovery during   *)
(* replica replacement, online move) and "The hot path and memory" (TTL).  *)
(* Lead decisions 5 and 6 (designs/lead-decisions.md), as shipped in PR    *)
(* #89.  Where the plan is silent the spec follows the shipped code on     *)
(* main e6b4040b:                                                          *)
(*   server/src/cache/cluster/controller/{state,placement}.rs              *)
(*   server/src/cache/cluster/controller.rs (heartbeat, reconcile_once)    *)
(*   server/src/cache/cluster/worker/actor.rs (install, reconcile,         *)
(*     execute, maintenance, transfer), worker/actor/copies.rs,            *)
(*   worker/sync.rs (renewals, replicate)                                  *)
(*                                                                         *)
(* Abstractions (each recorded in the plan's design review section):       *)
(*  - The three Raft voters are one linearizable state machine with a      *)
(*    leader.  A leadership change is a fault that loses the leader's      *)
(*    volatile observations and may change the controller clock.  Plan and *)
(*    apply of a reconcile turn are one step: the shipped apply rejects a  *)
(*    change when the slot record (lease included) moved since planning    *)
(*    (placement.rs reconcile, `old != expected`), and the planner's       *)
(*    lower_ms is only older (more conservative) at apply.                 *)
(*  - A heartbeat records the report on the leader, commits the renewal    *)
(*    and replies with the committed topology and grants in one step.  A   *)
(*    lost reply is a fault (the renewal stays committed).                 *)
(*  - An online move (rebalance or drain) is started by an operator order  *)
(*    and planned in the same step.  Freeze, frozen evidence and cutover   *)
(*    are separate reconcile steps.                                        *)
(*  - Zones: every worker is in its own zone.                              *)
(*  - Reads that return a value change nothing and are not actions.  A     *)
(*    read that finds a due key journals a delete (store.rs observe ->     *)
(*    stage_delete), as the sweep does; both are the Expire action.        *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Workers,        \* worker node ids (model values)
    Slots,          \* modelled slots
    Keys,           \* keys, all in each slot
    Clients,        \* client ids (model values)
    NoNode,         \* model value: "no worker"
    Sys,            \* model value: author of expiry deletes
    D,              \* ClockBound half width: the interval is reading +- D
    MaxDrift,       \* largest clock offset from true time (MaxDrift <= D)
    L,              \* lease duration (LEASE_MS = 3 s, scaled)
    RA,             \* progress report age (REPORT_AGE_MS); abstracted, see Live
    G,              \* staged cold-reset grace (STAGED_RESET_GRACE_MS = REPORT_AGE)
    TTL,            \* the TTL a client may set
    MaxLag,         \* MAX_CUTOVER_LAG (32, scaled)
    MaxTime,        \* last true instant
    GST,            \* no fault action after this true instant
    MaxEpoch,       \* epochs per slot
    MaxWrites,      \* client mutations in a behaviour
    MaxCrashes, MaxRestarts, MaxReplaces, MaxCuts, MaxLoss, MaxDelays,
    MaxLeaderChanges, MaxDriftSteps, MaxResets, MaxMoves,
    Mutant,         \* "none" or a mutant name, see mutants/*.cfg
    Variant         \* a set of candidate design fixes, {} for the shipped design

ASSUME D \in Nat /\ MaxDrift \in 0..D /\ L > 2 * D /\ RA > 2 * D /\ MaxEpoch >= 1
ASSUME Variant \subseteq {"follow_retired", "observe_window"}

\* Candidate fixes.  Each prefix_* mutant reverts one of them (design bug loop).
\* follow_retired: a copy keeps pulling from a retired source incarnation
\* (no member gate on the source), and a staged promotion commits only once
\* the controller's lower bound reaches the source's latest expiry.
FollowRetired == "follow_retired" \in Variant /\ Mutant # "prefix_retired_gate"
LateStagedCommit == "follow_retired" \in Variant /\ Mutant # "prefix_early_staged_commit"
\* observe_window: failover and replica replacement wait for the report
\* window of the term, as the cold reset already does.
ObserveWindow == "observe_window" \in Variant /\ Mutant # "prefix_no_observe_window"

-----------------------------------------------------------------------------
(* Values *)

NoW == <<NoNode, 0>>                       \* no worker incarnation
WIds == (Workers \X (1..(MaxRestarts + MaxReplaces + 1))) \cup {NoW}
NoDl == -1                                 \* no deadline
EmptyVal == [k \in Keys |-> 0]             \* value 0 means absent
EmptyDl == [k \in Keys |-> NoDl]
Epochs == 1..MaxEpoch

\* A worker's clock offset from true time.  In bound: |off| <= MaxDrift <= D,
\* so the reported interval [reading - D, reading + D] contains true time.
OffIn == -MaxDrift..MaxDrift
OffAll == IF Mutant = "drift_beyond_bound" THEN -(D + 1)..(D + 1) ELSE OffIn
\* Initial clock offsets.  0: every clock exact (drift actions skew them).
\* 1: extreme in-bound skews on every clock.  Drift past the bound (E1) is
\* the drift_beyond_bound mutant's DriftWorker step.
CONSTANT InitSkew
InitOff == IF InitSkew = 0 THEN {0} ELSE {-MaxDrift, MaxDrift}
InitCoff == IF InitSkew = 1 THEN InitOff ELSE {0}
\* Allowed initial <<primary, replica>> pairs.  Symmetric runs use all pairs;
\* liveness runs (no symmetry) use one pair, the others being permutations.
CONSTANT InitPlaces

\* Frozen evidence (protocol.rs Frozen): sequence, upper clock bound at the
\* freeze, and the expiry of the grant the actor held.
NoFro == [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]
\* Replica seed of a cutover (protocol.rs ReplicaSeed).
NoSeed == [on |-> FALSE, prev |-> NoW, ep |-> 0, seq |-> 0]
NoShard == [role |-> "none", src |-> NoW, dst |-> {}, ep |-> 0, seq |-> 0, base |-> FALSE,
            val |-> EmptyVal, dl |-> EmptyDl, gexp |-> 0, gnb |-> 0,
            frq |-> FALSE, fro |-> NoFro]
NoRptSlot == [ok |-> FALSE, ep |-> 0, seq |-> 0, base |-> FALSE, cur |-> FALSE, fro |-> NoFro]
NoRep == [inc |-> 0, sl |-> [s \in Slots |-> NoRptSlot]]
NoPkt == [kind |-> "none", src |-> NoW, gen |-> 0, ep |-> 0, seq |-> 0,
          eff |-> [k |-> CHOOSE k \in Keys : TRUE, v |-> 0, d |-> NoDl, c |-> Sys],
          val |-> EmptyVal, dl |-> EmptyDl]
Unset == [set |-> FALSE, val |-> EmptyVal, dl |-> EmptyDl, cold |-> FALSE, fep |-> 0, fseq |-> 0]
NoPromo == [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0]

-----------------------------------------------------------------------------
VARIABLES
    \* Controller, committed state machine (durable on the voters' disks).
    cs,         \* [Slots -> slot record]: epoch, primary, replica, staged target,
                \* move target, freeze request, frozen evidence, replica seed, cold
    lease,      \* [Slots -> [exp, nb]]: latest committed grant (0, 0 = none)
    stg,        \* [Slots -> [on, seq]]: staging in progress and source watermark
    member,     \* [Workers -> Nat]: enrolled incarnation (lower ones are retired)
    cur,        \* [Workers -> [Slots -> BOOLEAN]]: installed a topology at or after the
                \* slot's staging start (the staging-revision test of placement.rs staged_copy)
    gen,        \* cluster generation
    \* Controller leader, volatile.
    coff,       \* the leader's clock offset
    rep,        \* [Workers -> report]: last progress report per worker
    obsStart,   \* cold_reset_not_before_ms of the current term
    \* Workers.  Only inc and off survive a crash.
    up, inc, off,
    wv,         \* [Workers -> installed topology view]
    sh,         \* [Workers -> [Slots -> shard]]: role, epoch, seq, baseline, data, grant, freeze
    pk,         \* [Workers -> [Slots -> packet]]: replication reply in flight to the worker
    \* Environment.
    now,        \* true time
    ccut,       \* workers cut from the controllers (clients still reach them)
    pcut,       \* workers cut from their peers
    fb,         \* fault and operator budgets used
    nWrites,
    steady,     \* [Workers -> BOOLEAN]: connected since the current term started
    \* Ghost history.
    jr,         \* [Slots -> [Epochs -> Seq(effect)]]: the journal of each epoch
    base,       \* [Slots -> [Epochs -> base]]: the store an epoch starts from
    owners,     \* [Slots -> [Epochs -> SUBSET WIds]]: committed primaries per epoch
    served,     \* [Slots -> [Epochs -> SUBSET WIds]]: workers that served a write
    srvMax,     \* [Slots -> Nat]: highest epoch served so far
    srvBad,     \* a write was served at an epoch below srvMax
    promo,      \* [Slots -> [Epochs -> staged promotion record]]
    gone,       \* [Slots -> SUBSET Nat]: value versions a client saw expire
    lateAck     \* a write was acknowledged in an epoch a successor already took over

ctrlVars == <<cs, lease, stg, member, cur, gen>>
leaderVars == <<coff, rep, obsStart>>
workerVars == <<up, inc, off, wv, sh, pk>>
envVars == <<now, ccut, pcut, fb, nWrites, steady>>
ghostVars == <<jr, base, owners, served, srvMax, srvBad, promo, gone, lateAck>>
vars == <<ctrlVars, leaderVars, workerVars, envVars, ghostVars>>

-----------------------------------------------------------------------------
(* Clocks (plan: Leases; code: clock.rs ClockBounds, lease_authorizes) *)

WL(w) == now + off[w] - D       \* worker lower bound
WU(w) == now + off[w] + D       \* worker upper bound
CL == now + coff - D            \* controller lower bound
CU == now + coff + D

\* Shard::authorized: write_eligible (primary, baseline, no freeze request)
\* and lease_authorizes: lower >= not_before and upper < expires.
LeaseOK(w, nb, exp) ==
    IF Mutant = "no_uncertainty"
    THEN WU(w) >= nb /\ WL(w) < exp        \* P1, the sim knob's exact form
    ELSE WL(w) >= nb /\ WU(w) < exp

Authorized(w, s) ==
    /\ up[w]
    /\ sh[w][s].role = "pri" /\ sh[w][s].base
    /\ ~sh[w][s].frq \/ Mutant = "freeze_keeps_serving"
    /\ sh[w][s].gexp > 0
    /\ LeaseOK(w, sh[w][s].gnb, sh[w][s].gexp)

\* Expiry: a key is hidden once the upper bound reaches its deadline.
ExpClock(w) == IF Mutant = "expiry_lower_endpoint" THEN WL(w) ELSE WU(w)
Visible(w, x, k) == x.val[k] # 0 /\ (x.dl[k] = NoDl \/ ExpClock(w) < x.dl[k])

-----------------------------------------------------------------------------
(* Store effects (plan: Replication, canonical effects with absolute deadlines) *)

Step(st, e) == [val |-> [st.val EXCEPT ![e.k] = e.v],
                dl  |-> [st.dl EXCEPT ![e.k] = IF e.v = 0 THEN NoDl ELSE e.d]]

RECURSIVE ApplyN(_, _, _)
ApplyN(st, js, n) == IF n = 0 THEN st ELSE Step(ApplyN(st, js, n - 1), js[n])

StateOf(x) == [val |-> x.val, dl |-> x.dl]

-----------------------------------------------------------------------------
(* Controller observations (placement.rs Observations) *)

Me(w) == <<w, inc[w]>>
IsMember(id) == id # NoW /\ member[id[1]] = id[2]
Retired(id) == id # NoW /\ member[id[1]] > id[2]
\* A report is live until it ages out (ReportAges).  The leader's 3 s report
\* age is abstracted: a report ages only when its worker cannot heartbeat
\* (crashed, cut from the controllers, or retired).  A connected worker's
\* heartbeat (every 1 s) keeps it live.  Delayed heartbeats of a connected
\* worker are covered by the CutCtrl fault.
Live(id) == IsMember(id) /\ rep[id[1]].inc = id[2]
Rpt(id, s) == rep[id[1]].sl[s]
Baseline(id, s) ==
    /\ id # NoW /\ Live(id)
    /\ Rpt(id, s).ok /\ Rpt(id, s).ep = cs[s].ep /\ Rpt(id, s).base
LiveIds == {<<w, member[w]>> : w \in {x \in Workers : member[x] > 0}} \cap {id \in WIds : Live(id)}
MemberIds == {<<w, member[w]>> : w \in {x \in Workers : member[x] > 0}}
\* replica_for: any live worker in another zone, or none when there is none.
ReplicaChoices(p) ==
    LET c == {r \in LiveIds : r[1] # p[1]} IN IF c = {} THEN {NoW} ELSE c
Expired(s) == CL >= lease[s].exp
\* The candidate fix observe_window: the term's report window has passed.
Observed == ~ObserveWindow \/ CL >= obsStart

-----------------------------------------------------------------------------
(* Initial state: an initialized cluster.  Each slot is cold at epoch 1 with *)
(* an empty primary and a replica that still needs its baseline.            *)

SlotRec(ep, pri, rp, cold) ==
    [ep |-> ep, pri |-> pri, rep |-> rp, tgt |-> NoW, mtgt |-> NoW, mfrz |-> FALSE,
     mfro |-> NoFro, seed |-> NoSeed, cold |-> cold]

Init ==
    /\ \E pf \in [Slots -> Workers], rf \in [Slots -> Workers] :
        /\ \A s \in Slots : pf[s] # rf[s] /\ <<pf[s], rf[s]>> \in InitPlaces
        /\ cs = [s \in Slots |-> SlotRec(1, <<pf[s], 1>>, <<rf[s], 1>>, TRUE)]
        /\ sh = [w \in Workers |-> [s \in Slots |->
                   IF w = pf[s] THEN [NoShard EXCEPT !.role = "pri", !.ep = 1, !.base = TRUE,
                                                     !.dst = {<<rf[s], 1>>}]
                   ELSE IF w = rf[s] THEN [NoShard EXCEPT !.role = "copy", !.src = <<pf[s], 1>>, !.ep = 1]
                   ELSE NoShard]]
        /\ owners = [s \in Slots |-> [e \in Epochs |-> IF e = 1 THEN {<<pf[s], 1>>} ELSE {}]]
    /\ lease = [s \in Slots |-> [exp |-> 0, nb |-> 0]]
    /\ stg = [s \in Slots |-> [on |-> FALSE, seq |-> -1]]
    /\ member = [w \in Workers |-> 1]
    /\ cur = [w \in Workers |-> [s \in Slots |-> TRUE]]
    /\ gen = 1
    /\ coff \in InitCoff
    /\ rep = [w \in Workers |-> NoRep]
    /\ obsStart = coff - D + G
    /\ up = [w \in Workers |-> TRUE]
    /\ inc = [w \in Workers |-> 1]
    /\ off \in [Workers -> InitOff]
    /\ wv = [w \in Workers |-> [gen |-> 1, mem |-> member]]
    /\ pk = [w \in Workers |-> [s \in Slots |-> NoPkt]]
    /\ now = 0
    /\ ccut = {} /\ pcut = {}
    /\ fb = [crash |-> 0, restart |-> 0, replace |-> 0, cut |-> 0, loss |-> 0,
             delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0]
    /\ nWrites = 0
    /\ steady = [w \in Workers |-> TRUE]
    /\ jr = [s \in Slots |-> [e \in Epochs |-> <<>>]]
    /\ base = [s \in Slots |-> [e \in Epochs |->
                 IF e = 1 THEN [Unset EXCEPT !.set = TRUE, !.cold = TRUE] ELSE Unset]]
    /\ served = [s \in Slots |-> [e \in Epochs |-> {}]]
    /\ srvMax = [s \in Slots |-> 0]
    /\ srvBad = FALSE
    /\ promo = [s \in Slots |-> [e \in Epochs |-> NoPromo]]
    /\ gone = [s \in Slots |-> {}]
    /\ lateAck = FALSE

-----------------------------------------------------------------------------
(* Worker install of a committed topology and its grants                     *)
(* (actor.rs install and reconcile; sync.rs grant checks)                    *)

Fresh(ep, role, src, b) ==
    [NoShard EXCEPT !.role = role, !.src = src, !.ep = ep, !.base = b]

\* The shard that worker w keeps for slot s after installing placement c,
\* given its current shard x.  The code's retention rule for a promoted copy
\* also requires the previous view's primary to be the copy's source; a
\* copy's source is that primary by construction (a source change recreates
\* the shard), so the spec omits the redundant test.  A primary keeps the
\* destinations allowed to fetch from it (replica, staged and move targets).
\* A primary that is asked to freeze records the frozen evidence at install
\* (actor.rs reconcile: freeze_requested |= intent, then freeze()).
NewShard(w, x, c) ==
    LET me == Me(w)
        role == IF c.pri = me THEN "pri"
                ELSE IF c.rep = me \/ c.tgt = me \/ c.mtgt = me THEN "copy" ELSE "none"
        src == IF role = "copy" THEN c.pri ELSE NoW
        dst == IF role = "pri" THEN {c.rep, c.tgt, c.mtgt} \ {NoW} ELSE {}
        \* replica_seed: the copy keeps the frozen prefix after a cutover.
        seeded == /\ role = "copy" /\ c.rep = me /\ c.seed.on
                  /\ c.seed.ep = x.ep /\ c.seed.seq = x.seq
                  /\ \/ x.role = "copy" /\ x.src = c.seed.prev
                     \/ x.role = "pri" /\ me = c.seed.prev /\ x.fro.on
        retain == /\ (~c.cold \/ Mutant = "cold_reset_retains_data")
                  /\ x.base
                  /\ \/ x.role = "copy" /\ role = "pri"
                     \/ x.role = "pri" /\ role = "pri"
                     \/ seeded
        n == IF role = "none" THEN NoShard
             ELSE IF x.role = "none" THEN Fresh(c.ep, role, src, c.cold /\ role = "pri")
             ELSE IF x.ep # c.ep \/ x.role # role \/ x.src # src
                  THEN IF retain
                       THEN [Fresh(c.ep, role, src, TRUE) EXCEPT !.val = x.val, !.dl = x.dl]
                       ELSE Fresh(c.ep, role, src, c.cold /\ role = "pri")
                  ELSE x
        frq == n.frq \/ (role = "pri" /\ c.mfrz)
        fro == IF role = "pri" /\ ~n.fro.on /\ n.base /\ frq
               THEN [on |-> TRUE, seq |-> n.seq, up |-> WU(w), lexp |-> n.gexp]
               ELSE n.fro
    IN IF role = "none" THEN n ELSE [n EXCEPT !.dst = dst, !.frq = frq, !.fro = fro]

\* Ghost: the store a new primary, or a seeded copy, starts its epoch from.
BaseAfter(s, x, n, c) ==
    IF n.role # "none" /\ n.base /\ ~base[s][n.ep].set /\ (x.ep # n.ep \/ x.role # n.role)
    THEN [set |-> TRUE, val |-> n.val, dl |-> n.dl, cold |-> c.cold,
          fep |-> IF c.cold THEN 0 ELSE x.ep,
          fseq |-> IF c.cold THEN 0 ELSE x.seq]
    ELSE base[s][n.ep]

\* Epochs of slot s whose prefix a later epoch has taken over.
Sealed(s) == {base[s][e].fep : e \in {f \in Epochs : base[s][f].set /\ ~base[s][f].cold}}

-----------------------------------------------------------------------------
(* Worker heartbeat: report, renewal commit and reply (controller.rs         *)
(* heartbeat; state.rs renew; placement.rs Observations::record)             *)

Heartbeat(w, delivered) ==
    /\ up[w] /\ w \notin ccut
    /\ member[w] = inc[w]
    /\ delivered \/ (fb.loss < MaxLoss /\ now < GST)
    /\ LET me == Me(w)
           sameGen == wv[w].gen = gen
           rpt == [s \in Slots |->
                     IF /\ sh[w][s].role # "none" /\ sh[w][s].ep = cs[s].ep
                        /\ (cs[s].pri = me \/ cs[s].rep = me \/ cs[s].tgt = me \/ cs[s].mtgt = me)
                     THEN [ok |-> TRUE, ep |-> sh[w][s].ep, seq |-> sh[w][s].seq,
                           base |-> sh[w][s].base, cur |-> cur[w][s], fro |-> sh[w][s].fro]
                     ELSE NoRptSlot]
           newExp == CL + L
           \* state.rs renew: not for a slot whose move asked for a freeze.
           renew(s) == /\ sameGen /\ cs[s].pri = me /\ ~cs[s].mfrz
                       /\ newExp >= lease[s].exp /\ newExp > lease[s].nb
           lease2 == [s \in Slots |-> IF renew(s) THEN [exp |-> newExp, nb |-> lease[s].nb]
                                      ELSE lease[s]]
           stg2 == [s \in Slots |->
                      IF sameGen /\ cs[s].pri = me /\ cs[s].tgt # NoW /\ stg[s].on
                         /\ rpt[s].ok /\ rpt[s].base
                      THEN [stg[s] EXCEPT !.seq = IF @ < rpt[s].seq THEN rpt[s].seq ELSE @]
                      ELSE stg[s]]
           \* Install: a new generation clears every slot first.
           cleared == IF sameGen THEN sh[w] ELSE [s \in Slots |-> NoShard]
           n1 == [s \in Slots |-> NewShard(w, cleared[s], cs[s])]
           \* actor.rs install: a grant is taken only by an unfrozen primary.
           n2 == [s \in Slots |->
                    IF /\ renew(s) /\ n1[s].role = "pri" /\ ~n1[s].frq
                       /\ (n1[s].gexp = 0 \/ newExp >= n1[s].gexp)
                    THEN [n1[s] EXCEPT !.gexp = newExp, !.gnb = lease2[s].nb]
                    ELSE n1[s]]
       IN /\ rep' = IF sameGen
                    THEN [rep EXCEPT ![w] = [inc |-> inc[w], sl |-> rpt]]
                    ELSE rep
          /\ lease' = lease2
          /\ stg' = stg2
          /\ IF delivered
             THEN /\ wv' = [wv EXCEPT ![w] = [gen |-> gen, mem |-> member]]
                  /\ cur' = [cur EXCEPT ![w] = [s \in Slots |-> TRUE]]
                  /\ sh' = [sh EXCEPT ![w] = n2]
                  /\ base' = [s \in Slots |->
                                 IF n2[s].role = "none" THEN base[s]
                                 ELSE [base[s] EXCEPT ![n2[s].ep] = BaseAfter(s, cleared[s], n2[s], cs[s])]]
                  /\ fb' = fb
             ELSE /\ UNCHANGED <<wv, sh, base, cur>>
                  /\ fb' = [fb EXCEPT !.loss = @ + 1]
    /\ UNCHANGED <<cs, member, gen, coff, obsStart, up, inc, off, pk,
                   now, ccut, pcut, nWrites, steady, jr, owners, served, srvMax, srvBad,
                   promo, gone, lateAck>>

-----------------------------------------------------------------------------
(* Reconcile turns (placement.rs plan, recover_staged, plan_slot, plan_move,  *)
(* reconcile, valid_change, valid_staged).  Each commits one slot change.     *)

Commit(s, next) ==
    /\ cs' = [cs EXCEPT ![s] = next]
    /\ owners' = [owners EXCEPT ![s][next.ep] = @ \cup {next.pri}]

ClearStg(s) == stg' = [stg EXCEPT ![s] = [on |-> FALSE, seq |-> -1]]

ReconcileUnchangedButCur == UNCHANGED <<member, gen, coff, rep, obsStart, workerVars, envVars,
                                  jr, base, served, srvMax, srvBad, gone, lateAck>>
ReconcileUnchanged == ReconcileUnchangedButCur /\ UNCHANGED cur

NextEpoch(c, newPri) ==
    IF Mutant = "owner_change_keeps_epoch" /\ newPri # c.pri THEN c.ep ELSE c.ep + 1

\* Failover to a ready assigned replica (plan_slot; valid_change has_fence).
\* Also the move branch: the source is gone, or it was asked to freeze and
\* has not shown frozen evidence (an actor with an invalid clock).
Failover(s) ==
    LET c == cs[s] IN
    /\ Expired(s)
    /\ Observed
    /\ ~Baseline(c.pri, s) \/ (c.mfrz /\ ~c.mfro.on)
    /\ IsMember(c.rep) /\ Baseline(c.rep, s)
    /\ c.ep < MaxEpoch
    /\ \E r \in ReplicaChoices(c.rep) :
         Commit(s, SlotRec(NextEpoch(c, c.rep), c.rep, r, FALSE))
    /\ lease' = [lease EXCEPT ![s] = [exp |-> lease[s].exp, nb |-> lease[s].exp]]
    /\ ClearStg(s)
    /\ UNCHANGED promo
    /\ ReconcileUnchanged

\* recover_staged: the source is unavailable and the assigned replica is not ready.
StagedCase(s) ==
    LET c == cs[s] IN
    /\ c.tgt # NoW /\ c.pri # NoW
    /\ Retired(c.pri) \/ ~Baseline(c.pri, s) \/ Mutant = "staged_ignores_fence"
    /\ ~(IsMember(c.rep) /\ Baseline(c.rep, s))

FenceKind(s) == IF Retired(cs[s].pri) THEN "retired" ELSE "expired"
SourceFenced(s) ==
    \/ Mutant = "staged_ignores_fence"
    \/ FenceKind(s) = "retired"
    \/ FenceKind(s) = "expired" /\ CL >= lease[s].exp

StagedCopyOK(s) ==
    LET t == cs[s].tgt IN
    /\ IsMember(t) /\ Baseline(t, s)
    /\ Rpt(t, s).cur

WithinLag(s) ==
    Mutant = "staged_ignores_lag" \/ stg[s].seq - Rpt(cs[s].tgt, s).seq <= MaxLag

StagedCutoff(s) ==
    IF \/ Mutant = "staged_ignores_fence"
       \/ Mutant = "retire_is_fence" /\ FenceKind(s) = "retired"
    THEN CL ELSE lease[s].exp

CanPromote(s) ==
    /\ StagedCase(s) /\ SourceFenced(s) /\ StagedCopyOK(s)
    /\ stg[s].on /\ stg[s].seq >= 0 /\ WithinLag(s)
    /\ cs[s].ep < MaxEpoch
    /\ Mutant # "prefix_no_staged_recovery"
    \* Candidate fix: the transition commits only after the source's grant.
    /\ LateStagedCommit => CL >= StagedCutoff(s)

\* Decision 6: promote the verified staged target.  The target's grant starts
\* at the source's latest committed expiry (StagedPromotion::cutoff).
StagedPromote(s) ==
    LET c == cs[s]
        t == c.tgt
        cutoff == StagedCutoff(s)
    IN
    /\ CanPromote(s)
    /\ \E r \in ReplicaChoices(t) :
         Commit(s, SlotRec(NextEpoch(c, t), t, r, FALSE))
    /\ lease' = [lease EXCEPT ![s] = [exp |-> cutoff, nb |-> cutoff]]
    /\ promo' = [promo EXCEPT ![s][NextEpoch(c, t)] =
                   [on |-> TRUE, srcSeq |-> stg[s].seq, retSeq |-> Rpt(t, s).seq]]
    /\ ClearStg(s)
    /\ ReconcileUnchanged

\* No staged copy qualifies: scoped cold reset after the report window
\* (staged_reset_at and cold_reset_not_before_ms).
EligibleIds == LiveIds
StagedReset(s) ==
    LET c == cs[s] IN
    /\ StagedCase(s) /\ ~CanPromote(s)
    /\ Mutant # "prefix_no_staged_recovery"
    /\ CL >= lease[s].exp + G
    /\ CL >= obsStart
    /\ c.ep < MaxEpoch
    /\ \E p \in EligibleIds :
         /\ (<<c.pri[1], member[c.pri[1]]>> \in EligibleIds) => p[1] = c.pri[1]
         /\ \E r \in ReplicaChoices(p) :
              Commit(s, SlotRec(c.ep + 1, p, r, TRUE))
    /\ lease' = [lease EXCEPT ![s] = [exp |-> 0, nb |-> 0]]
    /\ ClearStg(s)
    /\ UNCHANGED promo
    /\ ReconcileUnchanged

\* Start a replica-only transfer: the replica is missing, retired or silent.
StageStart(s) ==
    LET c == cs[s] IN
    /\ c.tgt = NoW /\ c.mtgt = NoW
    /\ Observed
    /\ Baseline(c.pri, s)
    /\ c.rep = NoW \/ ~IsMember(c.rep) \/ ~Live(c.rep)
    /\ \E t \in LiveIds :
         /\ t[1] # c.pri[1] /\ t # c.rep
         /\ Commit(s, [c EXCEPT !.tgt = t])
         /\ stg' = [stg EXCEPT ![s] = [on |-> TRUE, seq |-> Rpt(c.pri, s).seq]]
         /\ cur' = [w \in Workers |-> [cur[w] EXCEPT ![s] = FALSE]]
    /\ UNCHANGED <<lease, promo>>
    /\ ReconcileUnchangedButCur

\* The staged copy is close enough: it becomes the assigned replica.
StageComplete(s) ==
    LET c == cs[s] IN
    /\ c.tgt # NoW
    /\ Baseline(c.pri, s)
    /\ StagedCopyOK(s)
    /\ Rpt(c.tgt, s).seq <= Rpt(c.pri, s).seq
    /\ Rpt(c.pri, s).seq - Rpt(c.tgt, s).seq <= MaxLag
    /\ Commit(s, [c EXCEPT !.rep = c.tgt, !.tgt = NoW])
    /\ ClearStg(s)
    /\ UNCHANGED <<lease, promo>>
    /\ ReconcileUnchanged

\* The target is not live while the source serves: release the transfer.
StageCancel(s) ==
    LET c == cs[s] IN
    /\ c.tgt # NoW
    /\ Baseline(c.pri, s)
    /\ ~Live(c.tgt)
    /\ Commit(s, [c EXCEPT !.tgt = NoW])
    /\ ClearStg(s)
    /\ UNCHANGED <<lease, promo>>
    /\ ReconcileUnchanged

-----------------------------------------------------------------------------
(* Online move with freeze (plan: Online move; placement.rs plan_move,       *)
(* valid_change; state.rs order).  An operator order, then freeze, frozen    *)
(* evidence and cutover, each a reconcile turn.                              *)

MoveUnchanged == UNCHANGED <<lease, stg, promo>> /\ ReconcileUnchanged

\* Admin::Rebalance or a drain names a target; plan_slot starts the move.
MoveStart(s) ==
    LET c == cs[s] IN
    /\ fb.moves < MaxMoves
    /\ c.tgt = NoW /\ c.mtgt = NoW
    /\ Baseline(c.pri, s)
    /\ \E t \in LiveIds :
         /\ t[1] # c.pri[1]
         /\ Commit(s, [c EXCEPT !.mtgt = t, !.mfrz = FALSE, !.mfro = NoFro, !.seed = NoSeed])
    /\ UNCHANGED <<lease, stg, promo, member, gen, coff, rep, obsStart, workerVars,
                   now, ccut, pcut, nWrites, steady, cur, jr, base, served, srvMax, srvBad,
                   gone, lateAck>>
    /\ fb' = [fb EXCEPT !.moves = @ + 1]

\* plan_move: the target is close enough, ask the source to freeze.
MoveFreeze(s) ==
    LET c == cs[s] IN
    /\ c.mtgt # NoW /\ ~c.mfrz
    /\ Baseline(c.pri, s) /\ Baseline(c.mtgt, s)
    /\ Rpt(c.mtgt, s).seq <= Rpt(c.pri, s).seq
    /\ Rpt(c.pri, s).seq - Rpt(c.mtgt, s).seq <= MaxLag
    /\ Commit(s, [c EXCEPT !.mfrz = TRUE])
    /\ MoveUnchanged

\* plan_move: record the source's frozen evidence for its latest grant.
MoveFrozen(s) ==
    LET c == cs[s]
        f == Rpt(c.pri, s).fro
    IN
    /\ c.mtgt # NoW /\ c.mfrz /\ ~c.mfro.on
    /\ Baseline(c.pri, s) /\ f.on
    /\ f.lexp = lease[s].exp \/ Expired(s)
    /\ f.up <= CU
    /\ Commit(s, [c EXCEPT !.mfro = f])
    /\ MoveUnchanged

\* The cutover's grant starts at the freeze's upper bound when the source
\* froze its latest grant, else at that grant's expiry.
MoveCutoff(s) ==
    IF cs[s].mfro.lexp = lease[s].exp THEN cs[s].mfro.up ELSE lease[s].exp

\* plan_move and valid_change: the target holds exactly the frozen prefix;
\* the source (if live) or a replica at that prefix becomes the new replica.
MoveCutover(s) ==
    LET c == cs[s]
        f == c.mfro
        cutoff == MoveCutoff(s)
        prior == IF Live(c.pri) THEN c.pri
                 ELSE IF Baseline(c.rep, s) /\ Rpt(c.rep, s).seq = f.seq /\ c.rep[1] # c.mtgt[1]
                 THEN c.rep ELSE NoW
    IN
    /\ c.mtgt # NoW /\ f.on
    /\ Baseline(c.mtgt, s) /\ Rpt(c.mtgt, s).seq = f.seq
    /\ CL >= cutoff
    /\ prior # NoW
    /\ c.ep < MaxEpoch
    /\ Commit(s, [SlotRec(NextEpoch(c, c.mtgt), c.mtgt, prior, FALSE) EXCEPT
                    !.seed = [on |-> TRUE, prev |-> c.pri, ep |-> c.ep, seq |-> f.seq]])
    /\ lease' = [lease EXCEPT ![s] = [exp |-> cutoff, nb |-> cutoff]]
    /\ ClearStg(s)
    /\ UNCHANGED promo
    /\ ReconcileUnchanged

\* plan_slot: a stalled frozen move resumes the same verified source under a
\* fresh epoch once its latest committed lease is fenced.
MoveResume(s) ==
    LET c == cs[s] IN
    /\ c.mtgt # NoW /\ c.mfro.on
    /\ Expired(s)
    /\ Baseline(c.pri, s)
    /\ c.ep < MaxEpoch
    /\ Commit(s, SlotRec(c.ep + 1, c.pri, c.rep, FALSE))
    /\ lease' = [lease EXCEPT ![s] = [exp |-> lease[s].exp, nb |-> lease[s].exp]]
    /\ ClearStg(s)
    /\ UNCHANGED promo
    /\ ReconcileUnchanged

\* plan_slot: an unfrozen move whose target is no longer eligible is released.
\* valid_change refuses to drop a move once a freeze was requested.
MoveRelease(s) ==
    LET c == cs[s] IN
    /\ c.mtgt # NoW /\ ~c.mfrz
    /\ ~Live(c.mtgt)
    /\ Baseline(c.pri, s)
    /\ Commit(s, [c EXCEPT !.mtgt = NoW])
    /\ MoveUnchanged

-----------------------------------------------------------------------------
(* Operator cold reset, scoped or fleet (state.rs reset).  The shipped code  *)
(* refuses while the controller's lower bound is below any scoped expiry.    *)
(* The new primary may be any enrolled worker (covers a drained owner).      *)

AdminColdReset(scope, fleet) ==
    /\ fb.reset < MaxResets
    /\ Mutant = "cold_reset_ignores_live_lease" \/ \A s \in scope : CL >= lease[s].exp
    /\ \A s \in scope : cs[s].ep < MaxEpoch
    /\ \E pf \in [scope -> MemberIds], rf \in [scope -> MemberIds \cup {NoW}] :
         /\ \A s \in scope : rf[s] = NoW \/ rf[s][1] # pf[s][1]
         /\ cs' = [s \in Slots |-> IF s \in scope
                     THEN SlotRec(cs[s].ep + 1, pf[s], rf[s], TRUE)
                     ELSE cs[s]]
         /\ owners' = [s \in Slots |-> IF s \in scope
                         THEN [owners[s] EXCEPT ![cs[s].ep + 1] = @ \cup {pf[s]}]
                         ELSE owners[s]]
    /\ gen' = IF fleet THEN gen + 1 ELSE gen
    /\ lease' = [s \in Slots |-> IF s \in scope THEN [exp |-> 0, nb |-> 0] ELSE lease[s]]
    /\ stg' = [s \in Slots |-> IF s \in scope THEN [on |-> FALSE, seq |-> -1] ELSE stg[s]]
    /\ fb' = [fb EXCEPT !.reset = @ + 1]
    /\ UNCHANGED <<member, cur, coff, rep, obsStart, workerVars, now, ccut, pcut, nWrites,
                   steady, jr, base, served, srvMax, srvBad, promo, gone, lateAck>>

-----------------------------------------------------------------------------
(* Worker data path (actor.rs execute, maintenance, publish)                 *)

\* A client mutation.  The final authority check and the journal append are
\* one actor turn: the linearization point.
Write(c, w, s, k, op) ==
    LET x == sh[w][s]
        v == nWrites + 1
        eff == CASE op = "set" -> [k |-> k, v |-> v, d |-> NoDl, c |-> c]
                 [] op = "ttl" -> [k |-> k, v |-> v, d |-> WU(w) + TTL, c |-> c]
                 [] op = "del" -> [k |-> k, v |-> 0, d |-> NoDl, c |-> c]
        st == Step(StateOf(x), eff)
    IN
    /\ Authorized(w, s)
    /\ nWrites < MaxWrites
    /\ op = "del" => x.val[k] # 0
    /\ jr' = [jr EXCEPT ![s][x.ep] = Append(@, eff)]
    /\ sh' = [sh EXCEPT ![w][s] = [x EXCEPT !.seq = @ + 1, !.val = st.val, !.dl = st.dl]]
    /\ nWrites' = nWrites + 1
    /\ served' = [served EXCEPT ![s][x.ep] = @ \cup {Me(w)}]
    /\ srvMax' = [srvMax EXCEPT ![s] = IF x.ep > @ THEN x.ep ELSE @]
    /\ srvBad' = (srvBad \/ x.ep < srvMax[s])
    /\ lateAck' = (lateAck \/ x.ep \in Sealed(s))
    /\ UNCHANGED <<ctrlVars, leaderVars, up, inc, off, wv, pk, now, ccut, pcut, fb, steady,
                   base, owners, promo, gone>>

\* A due key is deleted, only while authorized: the maintenance sweep, or a
\* read that finds it due.  The client saw that version expire.
Expire(w, s, k) ==
    LET x == sh[w][s]
        eff == [k |-> k, v |-> 0, d |-> NoDl, c |-> Sys]
        st == Step(StateOf(x), eff)
    IN
    /\ Authorized(w, s)
    /\ x.val[k] # 0 /\ x.dl[k] # NoDl /\ ExpClock(w) >= x.dl[k]
    /\ jr' = [jr EXCEPT ![s][x.ep] = Append(@, eff)]
    /\ sh' = [sh EXCEPT ![w][s] = [x EXCEPT !.seq = @ + 1, !.val = st.val, !.dl = st.dl]]
    /\ gone' = [gone EXCEPT ![s] = @ \cup {x.val[k]}]
    /\ UNCHANGED <<ctrlVars, leaderVars, up, inc, off, wv, pk, envVars,
                   base, owners, served, srvMax, srvBad, promo, lateAck>>

-----------------------------------------------------------------------------
(* Replication (worker/sync.rs replicate; actor.rs transfer, fetch, apply;  *)
(* copies.rs)                                                                *)

\* Pull replication.  The destination w probes its source; the source answers
\* with the next delta, or a snapshot when w has no baseline (fetch).
\* A lost request or reply changes nothing (the copy probes again), so
\* message loss on this channel is not a separate action.
\* sync.rs replicate skips a source that is not a current member.
CanFetch(w, s) ==
    LET x == sh[w][s]
        p == x.src
        y == sh[p[1]][s]
    IN
    /\ up[w] /\ w \notin pcut
    /\ x.role = "copy"
    /\ up[p[1]] /\ inc[p[1]] = p[2] /\ p[1] \notin pcut
    /\ FollowRetired \/ wv[w].mem[p[1]] = p[2]
    /\ y.role = "pri" /\ y.base /\ y.ep = x.ep
    /\ wv[p[1]].gen = wv[w].gen /\ wv[p[1]].mem[w] = inc[w]
    /\ Me(w) \in y.dst
    /\ x.seq <= y.seq
    /\ ~(x.base /\ x.seq = y.seq)

MkPkt(w, s) ==
    LET x == sh[w][s]
        p == x.src
        y == sh[p[1]][s]
    IN IF x.base
       THEN [NoPkt EXCEPT !.kind = "delta", !.src = p, !.gen = wv[p[1]].gen, !.ep = y.ep,
                          !.seq = x.seq + 1, !.eff = jr[s][y.ep][x.seq + 1]]
       ELSE [NoPkt EXCEPT !.kind = "snap", !.src = p, !.gen = wv[p[1]].gen, !.ep = y.ep,
                          !.seq = y.seq, !.val = y.val, !.dl = y.dl]

\* Copies::apply: exact next delta, or a snapshot at a cut not behind the copy.
\* Wrong generation, retired source incarnation (Transfer::Apply member
\* gate), other role or epoch: dropped.
ApplyDelta(x, e) ==
    IF Mutant = "replica_keeps_deleted" /\ e.v = 0
    THEN x                                         \* P2: drop delete effects
    ELSE LET st == Step(StateOf(x), e) IN [x EXCEPT !.val = st.val, !.dl = st.dl]

Recv(w, s, m) ==
    LET x == sh[w][s]
        ok == /\ wv[w].gen = m.gen
              /\ FollowRetired \/ wv[w].mem[m.src[1]] = m.src[2]
              /\ x.role = "copy" /\ x.src = m.src
              /\ x.ep = m.ep \/ Mutant = "accept_retired_epoch"
        \* P2's ordering race (phase 12): a late SET lands after the primary's
        \* synthesized expiry DEL of the same key.  The contiguity rule forbids it.
        lateSet == /\ Mutant = "replica_set_after_expiry_del"
                   /\ ok /\ m.kind = "delta" /\ x.base /\ m.seq < x.seq
                   /\ m.eff.v # 0
                   /\ LET last == jr[s][x.ep][x.seq] IN
                        last.k = m.eff.k /\ last.v = 0 /\ last.c = Sys
    IN IF lateSet
       THEN ApplyDelta(x, m.eff)
       ELSE IF ok /\ m.kind = "delta" /\ x.base /\ m.seq = x.seq + 1
       THEN [ApplyDelta(x, m.eff) EXCEPT !.seq = m.seq]
       ELSE IF ok /\ m.kind = "snap" /\ (~x.base \/ m.seq >= x.seq)
       THEN [x EXCEPT !.val = m.val, !.dl = m.dl, !.seq = m.seq, !.base = TRUE]
       ELSE x

Replicate(w, s) ==
    /\ CanFetch(w, s)
    /\ sh' = [sh EXCEPT ![w][s] = Recv(w, s, MkPkt(w, s))]
    /\ UNCHANGED <<ctrlVars, leaderVars, up, inc, off, wv, pk, envVars, ghostVars>>

\* Fault: a reply is delayed.  It is held and delivered later, possibly
\* after an epoch, role, membership or generation change (R-cache-10).
HoldReply(w, s) ==
    /\ now < GST /\ fb.delay < MaxDelays
    /\ CanFetch(w, s) /\ pk[w][s].kind = "none"
    /\ pk' = [pk EXCEPT ![w][s] = MkPkt(w, s)]
    /\ fb' = [fb EXCEPT !.delay = @ + 1]
    /\ UNCHANGED <<ctrlVars, leaderVars, up, inc, off, wv, sh, now, ccut, pcut, nWrites,
                   steady, ghostVars>>

DeliverHeld(w, s) ==
    /\ up[w] /\ pk[w][s].kind # "none"
    /\ sh' = [sh EXCEPT ![w][s] = Recv(w, s, pk[w][s])]
    /\ pk' = [pk EXCEPT ![w][s] = NoPkt]
    /\ UNCHANGED <<ctrlVars, leaderVars, up, inc, off, wv, envVars, ghostVars>>

-----------------------------------------------------------------------------
(* Faults, each with a budget and only before GST                           *)

FaultOK == now < GST

\* Worker crash: RAM, installed view, grants and inbound replies are lost.
Crash(w) ==
    /\ FaultOK /\ up[w] /\ fb.crash < MaxCrashes
    /\ up' = [up EXCEPT ![w] = FALSE]
    /\ sh' = [sh EXCEPT ![w] = [s \in Slots |-> NoShard]]
    /\ pk' = [pk EXCEPT ![w] = [s \in Slots |-> NoPkt]]
    /\ wv' = [wv EXCEPT ![w] = [gen |-> 0, mem |-> [x \in Workers |-> 0]]]
    /\ cur' = [cur EXCEPT ![w] = [s \in Slots |-> FALSE]]
    /\ fb' = [fb EXCEPT !.crash = @ + 1]
    /\ steady' = [steady EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<cs, lease, stg, member, gen, leaderVars, inc, off, now, ccut, pcut, nWrites, ghostVars>>

\* Restart with empty RAM and a fresh incarnation; enrollment retires the old one.
Restart(w) ==
    /\ FaultOK /\ ~up[w] /\ fb.restart < MaxRestarts
    /\ up' = [up EXCEPT ![w] = TRUE]
    /\ inc' = [inc EXCEPT ![w] = member[w] + 1]
    /\ member' = [member EXCEPT ![w] = member[w] + 1]
    /\ fb' = [fb EXCEPT !.restart = @ + 1]
    /\ UNCHANGED <<cs, lease, stg, gen, cur, leaderVars, off, wv, sh, pk, now, ccut, pcut, nWrites,
                   steady, ghostVars>>

\* Admin Replace / re-enrollment at the same node while the old process is
\* still running, for example isolated in its zone (R-cache-13).  The new
\* incarnation never runs in the model; the old one keeps its grants.
Replace(w) ==
    /\ FaultOK /\ up[w] /\ fb.replace < MaxReplaces
    /\ member' = [member EXCEPT ![w] = member[w] + 1]
    /\ fb' = [fb EXCEPT !.replace = @ + 1]
    /\ steady' = [steady EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<cs, lease, stg, gen, cur, leaderVars, workerVars, now, ccut, pcut, nWrites, ghostVars>>

CutCtrl(w) ==
    /\ FaultOK /\ w \notin ccut /\ fb.cut < MaxCuts
    /\ ccut' = ccut \cup {w}
    /\ fb' = [fb EXCEPT !.cut = @ + 1]
    /\ steady' = [steady EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<ctrlVars, leaderVars, workerVars, now, pcut, nWrites, ghostVars>>

CutPeer(w) ==
    /\ FaultOK /\ w \notin pcut /\ fb.cut < MaxCuts
    /\ pcut' = pcut \cup {w}
    /\ fb' = [fb EXCEPT !.cut = @ + 1]
    /\ UNCHANGED <<ctrlVars, leaderVars, workerVars, now, ccut, nWrites, steady, ghostVars>>

\* Healing is the environment, not a fault; it is fair after GST.
HealCtrl(w) ==
    /\ w \in ccut
    /\ ccut' = ccut \ {w}
    /\ UNCHANGED <<ctrlVars, leaderVars, workerVars, now, pcut, fb, nWrites, steady, ghostVars>>

HealPeer(w) ==
    /\ w \in pcut
    /\ pcut' = pcut \ {w}
    /\ UNCHANGED <<ctrlVars, leaderVars, workerVars, now, ccut, fb, nWrites, steady, ghostVars>>

Connected(w) == up[w] /\ w \notin ccut /\ member[w] = inc[w]

\* Leadership change: the new leader's observations start empty
\* (Observations::reset_if_changed) and it may have another clock.
LeaderChange ==
    /\ FaultOK /\ fb.lead < MaxLeaderChanges
    /\ \E o \in OffIn :
         /\ coff' = o
         /\ obsStart' = now + o - D + G
    /\ rep' = [w \in Workers |-> NoRep]
    /\ fb' = [fb EXCEPT !.lead = @ + 1]
    /\ steady' = [w \in Workers |-> Connected(w)]
    /\ UNCHANGED <<ctrlVars, workerVars, now, ccut, pcut, nWrites, ghostVars>>

\* Clock drift: an offset moves by one tick.  In bound unless the
\* drift_beyond_bound fault configuration (E1) is on.
DriftWorker(w, d) ==
    /\ FaultOK /\ fb.drift < MaxDriftSteps
    /\ off[w] + d \in OffAll
    /\ off' = [off EXCEPT ![w] = off[w] + d]
    /\ fb' = [fb EXCEPT !.drift = @ + 1]
    /\ UNCHANGED <<ctrlVars, leaderVars, up, inc, wv, sh, pk, now, ccut, pcut, nWrites, steady, ghostVars>>

DriftController(d) ==
    /\ FaultOK /\ fb.drift < MaxDriftSteps
    /\ coff + d \in OffAll
    /\ coff' = coff + d
    /\ fb' = [fb EXCEPT !.drift = @ + 1]
    /\ UNCHANGED <<ctrlVars, rep, obsStart, workerVars, now, ccut, pcut, nWrites, steady, ghostVars>>

\* The leader's report of a worker that cannot heartbeat ages out
\* (REPORT_AGE_MS).  Fair: the leader's clock passes the report age.
ReportAges(w) ==
    /\ rep[w].inc # 0 /\ ~Connected(w)
    /\ rep' = [rep EXCEPT ![w] = NoRep]
    /\ UNCHANGED <<ctrlVars, coff, obsStart, workerVars, envVars, ghostVars>>

\* Timing assumption (the worker's 1 s renewal timer is shorter than the 3 s
\* report window): a worker connected since the term started has reported
\* before the controller's lower bound passes the window.
WindowOK ==
    \A w \in Workers :
        (steady[w] /\ Connected(w) /\ rep[w].inc # inc[w]) => now + 1 + coff - D < obsStart

\* Time advances.  One normalization keeps the state space small without
\* changing the lease decisions: a grant whose expiry the worker's clock has
\* passed is dropped (the actor's clock endpoints are monotonic, clock.rs
\* now, so a passed expiry never authorizes again).
GrantPassed(w, exp, t) ==
    IF Mutant = "no_uncertainty" THEN t + off[w] - D >= exp ELSE t + off[w] + D >= exp

Tick ==
    /\ now < MaxTime
    /\ WindowOK
    /\ now' = now + 1
    /\ sh' = [w \in Workers |-> [s \in Slots |->
                 IF sh[w][s].gexp > 0 /\ GrantPassed(w, sh[w][s].gexp, now + 1)
                 THEN [sh[w][s] EXCEPT !.gexp = 0, !.gnb = 0] ELSE sh[w][s]]]
    /\ UNCHANGED <<ctrlVars, leaderVars, up, inc, off, wv, pk, ccut, pcut, fb, nWrites, steady, ghostVars>>

\* The run ends at MaxTime: an explicit terminal stuttering step.
Done ==
    /\ now = MaxTime
    /\ UNCHANGED vars

-----------------------------------------------------------------------------

Reconcile(s) ==
    \/ Failover(s) \/ StagedPromote(s) \/ StagedReset(s)
    \/ StageStart(s) \/ StageComplete(s) \/ StageCancel(s)
    \/ MoveFreeze(s) \/ MoveFrozen(s) \/ MoveCutover(s) \/ MoveResume(s) \/ MoveRelease(s)

Next ==
    \/ Tick \/ Done
    \/ \E w \in Workers : Heartbeat(w, TRUE) \/ Heartbeat(w, FALSE)
    \/ \E s \in Slots : Reconcile(s) \/ MoveStart(s)
    \/ \E c \in Clients, w \in Workers, s \in Slots, k \in Keys, op \in {"set", "ttl", "del"} :
         Write(c, w, s, k, op)
    \/ \E w \in Workers, s \in Slots, k \in Keys : Expire(w, s, k)
    \/ \E w \in Workers, s \in Slots : Replicate(w, s) \/ HoldReply(w, s) \/ DeliverHeld(w, s)
    \/ \E w \in Workers : Crash(w) \/ Restart(w) \/ Replace(w) \/ ReportAges(w)
    \/ \E w \in Workers : CutCtrl(w) \/ CutPeer(w) \/ HealCtrl(w) \/ HealPeer(w)
    \/ \E w \in Workers, d \in {-1, 1} : DriftWorker(w, d)
    \/ \E d \in {-1, 1} : DriftController(d)
    \/ LeaderChange
    \/ \E s \in Slots : AdminColdReset({s}, FALSE)
    \/ AdminColdReset(Slots, TRUE)

\* Fairness: each assumption names its mechanism.
\*  Tick: real time.  Heartbeat: the worker's 1 s renewal timer.  Reconcile:
\*  the controller's 250 ms reconcile turn.  Replicate: the 25 ms
\*  replication poll.  HealCtrl and HealPeer: partitions heal after GST (an
\*  environment assumption, the register's "reachable surviving workers").
\*  ReportAges: the leader's clock passes the 3 s report age.
\*  No fairness on faults, client writes, moves or operator actions.
Fairness ==
    /\ WF_vars(Tick)
    /\ \A w \in Workers : WF_vars(Heartbeat(w, TRUE))
    /\ \A s \in Slots : WF_vars(Reconcile(s))
    /\ \A w \in Workers, s \in Slots : WF_vars(Replicate(w, s))
    /\ \A w \in Workers : WF_vars(HealCtrl(w)) /\ WF_vars(HealPeer(w)) /\ WF_vars(ReportAges(w))

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties *)

TypeOK ==
    /\ \A s \in Slots :
         /\ cs[s].ep \in Epochs /\ cs[s].pri \in WIds /\ cs[s].rep \in WIds
         /\ cs[s].tgt \in WIds /\ cs[s].mtgt \in WIds /\ cs[s].cold \in BOOLEAN
         /\ cs[s].mfrz \in BOOLEAN /\ cs[s].mfro.on \in BOOLEAN /\ cs[s].seed.on \in BOOLEAN
         /\ ~(cs[s].tgt # NoW /\ cs[s].mtgt # NoW)
         /\ lease[s].exp \in Int /\ lease[s].nb \in Int
         /\ stg[s].on \in BOOLEAN
         /\ gone[s] \subseteq 1..MaxWrites
    /\ member \in [Workers -> Nat] /\ gen \in Nat
    /\ coff \in OffAll /\ off \in [Workers -> OffAll]
    /\ up \in [Workers -> BOOLEAN] /\ inc \in [Workers -> Nat]
    /\ steady \in [Workers -> BOOLEAN]
    /\ \A w \in Workers, s \in Slots :
         /\ sh[w][s].role \in {"none", "pri", "copy"}
         /\ sh[w][s].seq \in Nat /\ sh[w][s].base \in BOOLEAN
         /\ sh[w][s].frq \in BOOLEAN /\ sh[w][s].fro.on \in BOOLEAN
         /\ pk[w][s].kind \in {"none", "delta", "snap"}
    /\ now \in 0..MaxTime /\ ccut \subseteq Workers /\ pcut \subseteq Workers
    /\ nWrites \in 0..MaxWrites
    /\ srvBad \in BOOLEAN /\ lateAck \in BOOLEAN

\* Precondition of the lease argument: every reported interval covers true time.
ClockAssumption == coff \in -D..D /\ \A w \in Workers : off[w] \in -D..D

\* CACHE-G07: every committed state assigns each slot one primary, and one
\* (slot, epoch) never names two primaries across committed states.
G07_SlotOwnership ==
    \A s \in Slots :
        /\ cs[s].pri # NoW
        /\ \A e \in Epochs : Cardinality(owners[s][e]) <= 1

\* CACHE-G06: at every true instant at most one worker passes the authority
\* check for a slot (LeaseExclusive in the simulator).
G06_LeaseExclusive ==
    \A s \in Slots : Cardinality({w \in Workers : Authorized(w, s)}) <= 1

\* CACHE-G06: served epochs never decrease in true time, and no epoch is
\* served by two workers (ServedEpochOrder in the simulator).
G06_ServedEpochOrder ==
    /\ ~srvBad
    /\ \A s \in Slots, e \in Epochs : Cardinality(served[s][e]) <= 1

\* CACHE-G05: every verified copy equals its epoch's journal applied to the
\* epoch's base up to its sequence (ReplicaPrefix in the simulator).
G05_ReplicaPrefix ==
    \A w \in Workers, s \in Slots :
        LET x == sh[w][s] IN
        (up[w] /\ x.role # "none" /\ x.base) =>
            /\ base[s][x.ep].set
            /\ x.seq <= Len(jr[s][x.ep])
            /\ StateOf(x) = ApplyN([val |-> base[s][x.ep].val, dl |-> base[s][x.ep].dl],
                                   jr[s][x.ep], x.seq)

\* CACHE-G05 with decision 5's scope: an epoch that does not start cold starts
\* from a prefix of an earlier epoch's history.  Effects in that prefix
\* (deletes and expiries included) survive; effects after it are the allowed
\* asynchronous loss.  Covers failover, moves and decision 6 staged promotion.
G05_PromotedPrefix ==
    \A s \in Slots, e \in Epochs :
        (base[s][e].set /\ ~base[s][e].cold) =>
            LET f == base[s][e].fep IN
            /\ f \in Epochs /\ f < e
            /\ base[s][f].set
            /\ base[s][e].fseq <= Len(jr[s][f])
            /\ [val |-> base[s][e].val, dl |-> base[s][e].dl] =
                 ApplyN([val |-> base[s][f].val, dl |-> base[s][f].dl], jr[s][f], base[s][e].fseq)

\* CACHE-G02: a key an authorized primary would return has not expired in true time.
G02_ExpiredNeverReturns ==
    \A w \in Workers, s \in Slots :
        Authorized(w, s) =>
            \A k \in Keys : Visible(w, sh[w][s], k) => (sh[w][s].dl[k] = NoDl \/ now < sh[w][s].dl[k])

\* CACHE-G02 across owners ("Expired keys never return", R-cache-09): a value
\* version that a client saw expire is never visible on an authorized primary
\* again, after failover, a move or a staged promotion.
G02_NoResurrection ==
    \A w \in Workers, s \in Slots :
        Authorized(w, s) => \A k \in Keys : Visible(w, sh[w][s], k) => sh[w][s].val[k] \notin gone[s]

\* CACHE-G09: an epoch created by a cold reset starts empty.
G09_ColdStartEmpty ==
    \A s \in Slots, e \in Epochs : (base[s][e].set /\ base[s][e].cold) => base[s][e].val = EmptyVal

\* Decision 6: a staged promotion's retained prefix (as installed) trails the
\* committed source watermark by at most MaxLag.
D6_LagBound ==
    \A s \in Slots, e \in Epochs :
        (promo[s][e].on /\ base[s][e].set) => base[s][e].fseq >= promo[s][e].srcSeq - MaxLag

\* CACHE-G05 and decision 5, the lag window ends at the handover: no write
\* is acknowledged in an epoch after a later epoch took over its prefix.
\* Such a write is lost however fast replication is.
G05_NoAckAfterHandover == ~lateAck

\* CACHE-G05 and decision 5, loss comes only from replication lag: while a
\* source can still acknowledge writes for an epoch, a promotable copy that
\* follows it (enrolled, named by the slot, connected to it) can pull them.
Follows(w, s) ==
    LET x == sh[w][s]
        p == x.src
        y == sh[p[1]][s]
    IN
    /\ up[w] /\ x.role = "copy" /\ x.base /\ IsMember(Me(w))
    /\ Me(w) \in {cs[s].rep, cs[s].tgt, cs[s].mtgt}
    /\ p # NoW /\ up[p[1]] /\ inc[p[1]] = p[2]
    /\ Authorized(p[1], s) /\ y.ep = x.ep /\ x.seq < y.seq
    /\ w \notin pcut /\ p[1] \notin pcut
    /\ wv[w].gen = wv[p[1]].gen

G05_LossOnlyFromLag ==
    \A w \in Workers, s \in Slots :
        Follows(w, s) => (CanFetch(w, s) /\ Recv(w, s, MkPkt(w, s)).seq = sh[w][s].seq + 1)

\* CACHE-G05 ("A lost connection alone does not prove a slot's copies are
\* gone"), K03: no failover or replica replacement drops a worker that has
\* been connected since the current controller term started.
SteadyId(id) == id # NoW /\ steady[id[1]] /\ Connected(id[1]) /\ inc[id[1]] = id[2]
G05_NoRepairOfSteady ==
    [][\A s \in Slots :
         LET c == cs[s]
             n == cs'[s]
         IN /\ (c.tgt = NoW /\ c.mtgt = NoW /\ n.tgt # NoW /\ n.ep = c.ep) => ~SteadyId(c.rep)
            /\ (c.tgt = NoW /\ c.mtgt = NoW /\ n.ep > c.ep /\ n.pri # c.pri /\ ~n.cold)
                   => ~SteadyId(c.pri)
      ]_vars

\* Liveness.  A slot is available when some worker passes the authority check.
Available(s) == \E w \in Workers : Authorized(w, s)
Holds(id, s) ==
    /\ id # NoW /\ up[id[1]] /\ inc[id[1]] = id[2]
    /\ sh[id[1]][s].role # "none" /\ sh[id[1]][s].base /\ sh[id[1]][s].ep = cs[s].ep
\* The plan's explicit-operator regime: no staged move, and neither the
\* primary nor the assigned replica holds a baseline of the current epoch.
OperatorScope(s) ==
    /\ cs[s].tgt = NoW
    /\ ~Holds(cs[s].pri, s) /\ ~Holds(cs[s].rep, s)

\* Decision 6 as shipped: a slot leaves unavailability unless it is in the
\* operator regime (CACHE-G05 repair, scoped to staged and ready copies).
D6_StagedRepair == \A s \in Slots : []<>(Available(s) \/ OperatorScope(s))

\* Decision 6's invariant as written: no slot stays without an authorized
\* primary (after faults stop, with surviving workers).  Not in a baseline:
\* the shipped design fails it (finding CACHE-1 in the review).
D6_RepairBudget == \A s \in Slots : []<>Available(s)

=============================================================================
