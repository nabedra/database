---------------------------- MODULE OlapStreamJoin ----------------------------
(***************************************************************************)
(* Design spec of stream membership: plans/olap-streaming.md, "Worker      *)
(* execution, memory, and recovery" ("Reserve replica bytes and follow     *)
(* all-ready admission, join, drain and takeover rules"), and the          *)
(* scheduling plan's join rule ("copy before readiness", R03).             *)
(* Guarantee: OLAP-STR-G04, through the interface that OlapStream (A4)     *)
(* assumes: every ready worker holds every replied stream entry.           *)
(*                                                                         *)
(* A join is Command::Join (copy under lock_stream_changes, kept as a      *)
(* 30 s seal by retain_seal), the controller's readiness publication       *)
(* (cluster/runtime.rs publish) and Unseal. Stream commits wait for the    *)
(* lock, so they are excluded while a seal is held.                        *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*  J1. A stream entry is a version number per request ID: copy[n][k].     *)
(*      replied[k] is the highest version a client was told about. The     *)
(*      data protocol (OlapStream) checks content, fences and replay.      *)
(*  J2. A commit is one step and reaches every worker the scheduler fans   *)
(*      out to, or only some of them with no reply (CommitPartial).        *)
(*  J3. A lost worker leaves the ready set, the next scheduler is elected  *)
(*      and recovery by the owner happens in one step (Settle), as in      *)
(*      OlapStream A3. Recovery is lazy only after a join: the next stream *)
(*      request runs EnsureStreamRecovered.                                *)
(*                                                                         *)
(* The shipped readiness publication does not check that the seal is still *)
(* live (cluster/runtime.rs, then a failing Unseal). It is the mutant      *)
(* prefix_join_seal_expiry. Variant names the candidate designs:           *)
(*   "plan":          publication needs the live seal; an expired seal     *)
(*                    cancels the join (the scheduling plan's rule);       *)
(*   "fanout_joiner": commits also go to workers holding a Join copy;      *)
(*   "repair_publish": publication re-copies the joiner from the           *)
(*                    scheduler in the same step.                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Nodes,          \* workers
    Keys,           \* stream request IDs
    Nil,
    MaxVer,         \* commits per key (state constraint on copy versions)
    MaxCrashes, MaxPartitions, MaxExpiries, MaxPartial,
    Mutant,         \* "none", or "prefix_join_seal_expiry"
    Variant

ASSUME Variant \in {"plan", "fanout_joiner", "repair_publish"}

VARIABLES
    ready,      \* Ready and Verified workers
    sched,      \* the scheduler owner, or Nil
    term,
    recovered,  \* the scheduler finished stream recovery for this view
    alive,
    isolated,   \* a worker cut off from every other node, or Nil
    seal,       \* the joiner whose Join holds the lock, or Nil
    copied,     \* joiners holding a Join copy that await readiness
    copy,       \* copy[n][k]: version of the entry that worker n holds
    replied,    \* replied[k]: highest version a client was told about
    nCrash, nPart, nExpire, nPartial

vars == <<ready, sched, term, recovered, alive, isolated, seal, copied, copy, replied,
          nCrash, nPart, nExpire, nPartial>>

Zero == [k \in Keys |-> 0]
Max(S) == CHOOSE v \in S : \A w \in S : w <= v

\* The scheduler serves stream requests: recovered for the view, not holding
\* a join lock.
Serving(n) == alive[n] /\ sched = n /\ isolated # n /\ recovered /\ seal = Nil

Fanout(n) == (ready \cup (IF Variant = "fanout_joiner" THEN copied ELSE {})) \ {n}
Reach(a, b) == isolated \notin {a, b}

\* Recovery merges every ready copy and repairs every ready worker.
MaxOver(cp, S, k) == Max({cp[x][k] : x \in S})
Repaired(cp, S) ==
    [x \in Nodes |-> IF x \in S THEN [k \in Keys |-> MaxOver(cp, S, k)] ELSE cp[x]]

\* Worker n leaves the ready set (J3).
Settle(n, cp, s) ==
    LET R == ready \ {n}
        keep == sched \in R
    IN /\ ready' = R
       /\ CASE keep -> s = sched /\ sched' = sched /\ term' = term
            [] ~keep /\ R = {} -> s = Nil /\ sched' = Nil /\ term' = term
            [] OTHER -> s \in R /\ sched' = s /\ term' = term + 1
       /\ recovered' = (s # Nil)
       /\ copy' = IF s = Nil THEN cp ELSE Repaired(cp, R)
       \* The join, its copy and its seal die with the scheduler.
       /\ IF keep
          THEN /\ seal' = IF seal = n THEN Nil ELSE seal
               /\ copied' = copied \ {n}
          ELSE /\ seal' = Nil /\ copied' = {}

\* The version a commit writes: the coordinator's local entry if it is ahead
\* of what was replied (a retry after a partial commit re-commits it, sync
\* in stream.rs), otherwise the next one.
NextVer(n, k) == IF copy[n][k] > replied[k] THEN copy[n][k] ELSE copy[n][k] + 1

\* A replicated commit: the local install and every transfer, then the
\* reply (DriverCommit).
Commit(n, k) ==
    /\ Serving(n)
    /\ NextVer(n, k) <= MaxVer
    /\ \A p \in Fanout(n) : alive[p] /\ Reach(n, p)
    /\ LET v == NextVer(n, k) IN
       /\ copy' = [x \in Nodes |-> IF x = n \/ x \in Fanout(n)
                                   THEN [copy[x] EXCEPT ![k] = v] ELSE copy[x]]
       /\ replied' = [replied EXCEPT ![k] = v]
    /\ UNCHANGED <<ready, sched, term, recovered, alive, isolated, seal, copied,
                   nCrash, nPart, nExpire, nPartial>>

\* A commit that reaches only some workers and sends no reply.
CommitPartial(n, k, S) ==
    /\ nPartial < MaxPartial
    /\ Serving(n)
    /\ copy[n][k] < MaxVer
    /\ S \subseteq {p \in Fanout(n) : alive[p] /\ Reach(n, p)}
    /\ LET v == copy[n][k] + 1 IN
       copy' = [x \in Nodes |-> IF x = n \/ x \in S
                                THEN [copy[x] EXCEPT ![k] = v] ELSE copy[x]]
    /\ nPartial' = nPartial + 1
    /\ UNCHANGED <<ready, sched, term, recovered, alive, isolated, seal, copied, replied,
                   nCrash, nPart, nExpire>>

\* ensure_stream_recovered: merge the ready copies, repair them, before any
\* stream work in the new view (lazy after a join).
EnsureStreamRecovered(n) ==
    /\ alive[n] /\ sched = n /\ isolated # n /\ seal = Nil
    /\ ~recovered
    /\ copy' = Repaired(copy, ready \cup {n})
    /\ recovered' = TRUE
    /\ UNCHANGED <<ready, sched, term, alive, isolated, seal, copied, replied,
                   nCrash, nPart, nExpire, nPartial>>

\* Command::Join: after stream recovery, take lock_stream_changes, reset
\* the joiner and copy every entry (copy_stream_replicas), keep the lock as
\* a seal. A lost worker restarts empty and joins the same way.
JoinCopy(n) ==
    /\ n \notin ready /\ n \notin copied
    /\ seal = Nil /\ sched # Nil /\ Serving(sched)
    /\ Reach(n, sched)
    /\ alive' = [alive EXCEPT ![n] = TRUE]
    /\ copy' = [copy EXCEPT ![n] = copy[sched]]
    /\ seal' = n
    /\ copied' = copied \cup {n}
    /\ UNCHANGED <<ready, sched, term, recovered, isolated, replied,
                   nCrash, nPart, nExpire, nPartial>>

\* The controller publishes the joiner's readiness, then Unseal.
PublishReady(n) ==
    /\ n \in copied /\ alive[n] /\ isolated # n
    /\ \/ seal = n
       \/ Mutant = "prefix_join_seal_expiry"
       \/ Variant = "fanout_joiner"
       \/ Variant = "repair_publish" /\ sched # Nil /\ Serving(sched) /\ Reach(n, sched)
    /\ ready' = ready \cup {n}
    /\ copied' = copied \ {n}
    /\ seal' = IF seal = n THEN Nil ELSE seal
    /\ recovered' = FALSE
    \* repair_publish: the scheduler re-copies the joiner with the publish.
    /\ copy' = IF Variant = "repair_publish" THEN [copy EXCEPT ![n] = copy[sched]] ELSE copy
    /\ UNCHANGED <<sched, term, alive, isolated, replied, nCrash, nPart, nExpire, nPartial>>

\* retain_seal's deadline passes before readiness is published.
SealExpire ==
    /\ nExpire < MaxExpiries /\ seal # Nil
    /\ Mutant # "seal_never_released"
    /\ seal' = Nil
    /\ copied' = IF Variant = "plan" /\ Mutant # "prefix_join_seal_expiry"
                 THEN copied \ {seal} ELSE copied
    /\ nExpire' = nExpire + 1
    /\ UNCHANGED <<ready, sched, term, recovered, alive, isolated, copy, replied,
                   nCrash, nPart, nPartial>>

-----------------------------------------------------------------------------
(* Faults, each with a budget.                                             *)

Crash(n, s) ==
    /\ nCrash < MaxCrashes /\ alive[n] /\ (n \in ready \/ n \in copied)
    /\ alive' = [alive EXCEPT ![n] = FALSE]
    /\ IF n \in ready THEN Settle(n, [copy EXCEPT ![n] = Zero], s)
       ELSE /\ s = sched
            /\ copy' = [copy EXCEPT ![n] = Zero]
            \* retain_seal holds the lock until its deadline (SealExpire).
            /\ seal' = IF seal = n /\ Mutant # "seal_never_released" THEN Nil ELSE seal
            /\ copied' = copied \ {n}
            /\ UNCHANGED <<ready, sched, term, recovered>>
    /\ nCrash' = nCrash + 1
    /\ UNCHANGED <<isolated, replied, nPart, nExpire, nPartial>>

\* Cut off, not stopped: the worker keeps its copy.
Partition(n, s) ==
    /\ nPart < MaxPartitions /\ isolated = Nil /\ (n \in ready \/ n \in copied)
    /\ isolated' = n
    /\ IF n \in ready THEN Settle(n, copy, s)
       ELSE /\ s = sched
            /\ UNCHANGED <<ready, sched, term, recovered, copy, seal, copied>>
    /\ nPart' = nPart + 1
    /\ UNCHANGED <<alive, replied, nCrash, nExpire, nPartial>>

Heal ==
    /\ isolated # Nil
    /\ isolated' = Nil
    /\ UNCHANGED <<ready, sched, term, recovered, alive, seal, copied, copy, replied,
                   nCrash, nPart, nExpire, nPartial>>

\* Every holder stopped: an authorized cold start. The new run abandons the
\* old streams, so nothing replied survives into it.
ColdStart(s) ==
    /\ ready = {}
    /\ ready' = Nodes
    /\ sched' = s
    /\ term' = term + 1
    /\ recovered' = TRUE
    /\ alive' = [n \in Nodes |-> TRUE]
    /\ seal' = Nil /\ copied' = {}
    /\ copy' = [n \in Nodes |-> Zero]
    /\ replied' = Zero
    /\ UNCHANGED <<isolated, nCrash, nPart, nExpire, nPartial>>

Init ==
    /\ ready \in (SUBSET Nodes) \ {{}}
    /\ sched \in ready
    /\ term = 1
    /\ recovered = TRUE
    /\ alive = [n \in Nodes |-> TRUE]
    /\ isolated = Nil
    /\ seal = Nil /\ copied = {}
    /\ copy = [n \in Nodes |-> Zero]
    /\ replied = Zero
    /\ nCrash = 0 /\ nPart = 0 /\ nExpire = 0 /\ nPartial = 0

\* Named wrappers so TLC's coverage reports each action separately.
CommitAny == \E n \in Nodes, k \in Keys : Commit(n, k)
CommitPartialAny == \E n \in Nodes, k \in Keys, S \in SUBSET Nodes : CommitPartial(n, k, S)
EnsureStreamRecoveredAny == \E n \in Nodes : EnsureStreamRecovered(n)
JoinCopyAny == \E n \in Nodes : JoinCopy(n)
PublishReadyAny == \E n \in Nodes : PublishReady(n)
CrashAny == \E n \in Nodes, s \in Nodes \cup {Nil} : Crash(n, s)
PartitionAny == \E n \in Nodes, s \in Nodes \cup {Nil} : Partition(n, s)
ColdStartAny == \E s \in Nodes : ColdStart(s)

\* Every worker is ready and every commit has been taken: the explicit
\* terminal step.
Done ==
    /\ ready = Nodes /\ isolated = Nil
    /\ \A n \in Nodes, k \in Keys : copy[n][k] = MaxVer
    /\ UNCHANGED vars

Next ==
    \/ CommitAny \/ CommitPartialAny \/ EnsureStreamRecoveredAny
    \/ JoinCopyAny \/ PublishReadyAny \/ SealExpire
    \/ CrashAny \/ PartitionAny \/ Heal \/ ColdStartAny
    \/ Done

(***************************************************************************)
(* Fairness with the mechanism that provides it: the API's retry loops     *)
(* (Commit), the first stream request in a new view (EnsureRecovered), the *)
(* provider's join loop (JoinCopy, PublishReady), the controller lease and *)
(* heartbeat (Settle inside Crash and Partition), the operator's cold      *)
(* start, healing partitions, and the controller's readiness CAS racing   *)
(* the seal's 30 s deadline (SF on PublishReady: the provider retries the  *)
(* join, so a publication completes infinitely often). Liveness is         *)
(* checked after the faults stop; every fault has a finite budget. No      *)
(* fairness on a fault, on CommitPartial or on SealExpire.                 *)
(***************************************************************************)
Fairness ==
    /\ \A n \in Nodes : WF_vars(EnsureStreamRecovered(n))
    /\ \A n \in Nodes : WF_vars(JoinCopy(n))
    /\ \A n \in Nodes : SF_vars(PublishReady(n))
    /\ WF_vars(ColdStartAny)
    /\ WF_vars(Heal)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties.                                                             *)

TypeOK ==
    /\ ready \subseteq Nodes
    /\ sched \in Nodes \cup {Nil}
    /\ term \in Nat
    /\ recovered \in BOOLEAN
    /\ alive \in [Nodes -> BOOLEAN]
    /\ isolated \in Nodes \cup {Nil}
    /\ seal \in Nodes \cup {Nil} /\ copied \subseteq Nodes
    /\ copy \in [Nodes -> [Keys -> 0..MaxVer]]
    /\ replied \in [Keys -> 0..MaxVer]

\* OLAP-STR-G04, the interface OlapStream assumes (A4): every live ready
\* worker holds every replied entry.
G04_ReadyHoldsReplied ==
    \A n \in ready, k \in Keys : alive[n] => copy[n][k] >= replied[k]

\* OLAP-STR-G04: no worker ever serves from a replied version it lacks: a
\* serving scheduler is at or past every replied version.
G04_ServingIsCurrent ==
    \A n \in Nodes, k \in Keys : Serving(n) => copy[n][k] >= replied[k]

\* Liveness, OLAP-STR-G04 (recovery): after the faults stop, every live,
\* reachable worker becomes ready and stays ready.
JoinProgress ==
    <>[](ready = {n \in Nodes : alive[n] /\ isolated # n})

=============================================================================
