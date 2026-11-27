------------------------------ MODULE QueueReady ------------------------------
(***************************************************************************)
(* Readiness and queue copies of the OLAP cluster's request queue:         *)
(* plans/resource-aware-scheduling.md, "a new worker copies outstanding   *)
(* queue records before it becomes ready" (R03) and "replicate ... to all *)
(* ready workers before acknowledging". Guarantee OLAP-G07 (scheduler      *)
(* queue, recovery). A small model for finding SCHED-1 of the rv-olap      *)
(* review; the queue spec of the OLTP review does not cover this runtime.  *)
(*                                                                         *)
(* Code: server/src/cluster/runtime.rs probe (425-485) publishes a joiner  *)
(* Ready after Command::Join copies the owner's queue, but skips the Join  *)
(* when scheduler(&root) finds no Ready coordinator (queue_runtime.rs      *)
(* 480-490). The replacement coordinator is a Ready node (runtime.rs       *)
(* 207-225, the first by id; any Ready node here), and its takeover keeps  *)
(* its own copy (refresh_queue, queue_runtime.rs 917-941).                 *)
(*                                                                         *)
(* Queue copies live in RAM: a lost node loses its copy. The join copy and *)
(* the publication are one step here (the seal; its expiry is OLAPS-1,     *)
(* modelled in specs/olap-stream/join).                                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, Initial, Records, MaxLosses, Mutant, None

VARIABLES state, owner, held, accepted, losses
vars == <<state, owner, held, accepted, losses>>

Ready == {n \in Nodes : state[n] = "ready"}

Init ==
    /\ state = [n \in Nodes |-> IF n \in Initial THEN "ready" ELSE "starting"]
    /\ owner = CHOOSE n \in Initial : TRUE
    /\ held = [n \in Nodes |-> {}]
    /\ accepted = {}
    /\ losses = 0

OwnerReady == owner # None /\ state[owner] = "ready"

\* queue_runtime.rs Submit: replicate to every ready worker, then acknowledge.
Accept(r) ==
    /\ OwnerReady /\ r \notin accepted
    /\ held' = [n \in Nodes |-> IF n \in Ready THEN held[n] \cup {r} ELSE held[n]]
    /\ accepted' = accepted \cup {r}
    /\ UNCHANGED <<state, owner, losses>>

\* probe with a Ready scheduler: Command::Join copies the owner's queue under
\* the seal, then the controller publishes the joiner Ready.
JoinAndPublish(d) ==
    /\ state[d] = "starting" /\ OwnerReady
    /\ held' = [held EXCEPT ![d] = held[owner]]
    /\ state' = [state EXCEPT ![d] = "ready"]
    /\ UNCHANGED <<owner, accepted, losses>>

\* probe when scheduler(&root) fails: no Join, readiness is published anyway.
\* Fix (baseline, runtime.rs join_requirement): publish without a copy only
\* when no Ready worker exists, the first enrollment of a run or after every
\* holder was lost. While any Ready worker may hold accepted requests, the
\* probe waits for a reassigned scheduler and joins through it.
PublishWithoutScheduler(d) ==
    /\ state[d] = "starting" /\ ~OwnerReady
    /\ Mutant # "prefix_ready_without_join" => Ready = {}
    /\ state' = [state EXCEPT ![d] = "ready"]
    /\ UNCHANGED <<owner, held, accepted, losses>>

\* observe_losses: a ready node is lost with its RAM copy. When the last
\* holder goes, the plan abandons its accepted requests ("stopping every
\* worker abandons it"); they leave the scope of G07.
Lose(n) ==
    /\ losses < MaxLosses /\ losses' = losses + 1
    /\ state[n] = "ready"
    /\ state' = [state EXCEPT ![n] = "lost"]
    /\ held' = [held EXCEPT ![n] = {}]
    /\ accepted' = IF Ready = {n} THEN {} ELSE accepted
    /\ UNCHANGED owner

\* runtime.rs coordinator choice: a Ready node replaces a lost owner and keeps
\* its own copy. In the code the controller publishes the loss
\* (observe_losses, runtime.rs:552-556) and then reassigns the coordinator in
\* ensure_catalog_fence (runtime.rs:196-260), two CAS steps under the
\* controller's `changes` lock. A probe can run between them only when the
\* second step fails (a store error or timeout) or the controller stops, so
\* Lose and Reassign are separate steps here.
Reassign(n) ==
    /\ ~OwnerReady /\ n \in Ready
    /\ owner' = n
    /\ UNCHANGED <<state, held, accepted, losses>>

Step ==
    \/ \E r \in Records : Accept(r)
    \/ \E n \in Nodes : JoinAndPublish(n) \/ PublishWithoutScheduler(n) \/ Lose(n) \/ Reassign(n)

\* The run ends when no step is enabled (every record accepted, every joiner
\* published, the loss budget spent, or nobody left).
Done == ~ENABLED Step /\ UNCHANGED vars

Next == Step \/ Done
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ state \in [Nodes -> {"starting", "ready", "lost"}]
    /\ owner \in Nodes \cup {None}
    /\ held \in [Nodes -> SUBSET Records]
    /\ accepted \subseteq Records
    /\ losses \in 0..MaxLosses

\* OLAP-G07 (R03): every ready worker holds every acknowledged request, so a
\* takeover by any of them keeps the request.
G07_ReadyHoldsAccepted == \A n \in Ready : accepted \subseteq held[n]
=============================================================================
