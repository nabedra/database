------------------------------- MODULE SimNet -------------------------------
(***************************************************************************)
(* The simulator's stream network: rt::sim::net, the TCP-like seam every   *)
(* OLTP and Cache peer, gateway and client connection uses, with the two   *)
(* shipped planners that sit on NetPlan. Main 7dadd41d plus open PR #106.  *)
(*                                                                         *)
(*   server/rt/src/sim/net.rs        Pipe::push, Half::{send, poll_recv},  *)
(*                                   World::{black_hole, heal,             *)
(*                                   reset_connection}, net::crash         *)
(*   server/sim/src/net.rs           NetPlan::plan (drop, delay, dup)      *)
(*   server/sim-oltp/src/net/mod.rs  Router (OLTP frame and HTTP faults)   *)
(*   server/src/cache/cluster/sim/harness.rs  Network (Cache planner)      *)
(*                                                                         *)
(* Design: phase-8 transport seam (lead decision 4: one message per        *)
(* flushed frame), phase-10-11 section 3.3, phase-12 section 3.5.          *)
(* Scenarios: D02, D10, O10 (lost replies, session drops), K03 (partitions,*)
(* lost renewals). Guarantees that assume it: OLTP-G03, OLTP-G04,          *)
(* CACHE-G05..G07.                                                         *)
(*                                                                         *)
(* One connection from client node C to server node S. Only the C to S     *)
(* direction is modelled; the reply direction has the same rules. A        *)
(* message is one flush: a whole frame on a session (Framed), or a byte    *)
(* flush on a plain connection.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Router,      \* "oltp" (sim-oltp Router) or "cache" (Cache harness Network)
    Streams,     \* stream ids of a session
    MaxSends,    \* messages C sends
    MaxDrops,    \* random NetPlan drops and profile cuts
    MaxDups,     \* NetPlan duplicates
    MaxHoles,    \* partitions (NetPlan block plus World::black_hole)
    MaxResets,   \* World::reset_connection and profile Reset
    MaxCrashes,  \* World::crash of C or S
    MaxFreezes,  \* World::freeze of S
    Mutant,      \* "none" or a mutant name
    Variant      \* "shipped", or "crash_sends_fin" (a proposed realism change)

VARIABLES
    framed,      \* a multiplexed session (frames carry a stream id) or a plain connection
    sent,        \* ids C sent, in order, with their stream
    queue,       \* Pipe::queue: in-flight messages [id, stream, ready]
    readyQ,      \* messages whose delivery time passed, in delivery order
    recvd,       \* ids S read, in order
    stalled,     \* Pipe::stalled: a message was dropped, later ones vanish
    stallCause,  \* ghost: "none", "drop" or "partition"
    reset,       \* Connection::reset
    holed,       \* the C to S link is held (World::black_hole) and NetPlan-blocked
    cUp, sUp,    \* node incarnations
    sFrozen,     \* World::freeze of S
    heldReady,   \* ghost: ids that became ready while the link was held
    budget

vars == <<framed, sent, queue, readyQ, recvd, stalled, stallCause, reset, holed, cUp, sUp,
          sFrozen, heldReady, budget>>

Ids == 1..MaxSends
Msg == [id : Ids, stream : Streams]

TypeOK ==
    /\ framed \in BOOLEAN
    /\ sent \in Seq(Msg)
    /\ queue \in Seq([id : Ids, stream : Streams, ready : BOOLEAN])
    /\ readyQ \in Seq(Msg)
    /\ recvd \in Seq(Ids)
    /\ stalled \in BOOLEAN
    /\ stallCause \in {"none", "drop", "partition"}
    /\ reset \in BOOLEAN
    /\ holed \in BOOLEAN
    /\ cUp \in BOOLEAN /\ sUp \in BOOLEAN /\ sFrozen \in BOOLEAN
    /\ heldReady \subseteq Ids
    /\ budget \in [drops : Nat, dups : Nat, holes : Nat, resets : Nat, crashes : Nat, freezes : Nat]

Init ==
    /\ framed \in BOOLEAN
    /\ sent = <<>>
    /\ queue = <<>>
    /\ readyQ = <<>>
    /\ recvd = <<>>
    /\ stalled = FALSE
    /\ stallCause = "none"
    /\ reset = FALSE
    /\ holed = FALSE
    /\ cUp = TRUE /\ sUp = TRUE /\ sFrozen = FALSE
    /\ heldReady = {}
    /\ budget = [drops |-> 0, dups |-> 0, holes |-> 0, resets |-> 0, crashes |-> 0, freezes |-> 0]

Open == ~reset /\ cUp
StreamOf(m) == IF framed THEN m.stream ELSE 1

(* ----------------------------------------------------------------------- *)
(* Send: Half::send (net.rs:1006). The planner draws a NetPlan::plan; each  *)
(* router maps it to a Fate. A blocked link always plans Drop.             *)
(*   OLTP Router (sim-oltp net/mod.rs:207-213): Drop on a held link is     *)
(*     Pass (the World hole holds it), other Drops are Cut (Fate::Drop),   *)
(*     a duplicate is absorbed.                                            *)
(*   Cache Network (harness.rs:97-125): every Drop is Fate::Drop; a        *)
(*     duplicate is absorbed.                                              *)
(* Fate::Drop stalls the direction (TCP never delivers past a lost         *)
(* segment); a send on a stalled direction succeeds and vanishes.          *)
(* ----------------------------------------------------------------------- *)
Enqueue(id, st, copies) ==
    queue' = queue \o [k \in 1..copies |-> [id |-> id, stream |-> st, ready |-> FALSE]]

SendPlanned(st, plan) ==
    LET id == Len(sent) + 1
        fate == CASE plan = "drop" /\ holed /\ Router = "oltp" -> "deliver"
                  [] plan = "drop"                              -> "drop"
                  [] OTHER                                      -> plan
    IN /\ sent' = Append(sent, [id |-> id, stream |-> st])
       /\ IF stalled
          THEN UNCHANGED <<queue, stalled, stallCause>>
          ELSE CASE fate = "drop" ->
                      /\ stalled' = (Mutant # "drop_without_stall")
                      /\ stallCause' = IF Mutant = "drop_without_stall" THEN stallCause
                                       ELSE IF holed THEN "partition" ELSE "drop"
                      /\ UNCHANGED queue
                 [] fate = "deliver" ->
                      /\ Enqueue(id, st, 1)
                      /\ UNCHANGED <<stalled, stallCause>>
                 [] fate = "duplicate" ->
                      /\ Enqueue(id, st, IF Mutant = "deliver_duplicates" THEN 2 ELSE 1)
                      /\ UNCHANGED <<stalled, stallCause>>

Send(st, plan) ==
    /\ Open
    /\ Len(sent) < MaxSends
    /\ st \in (IF framed THEN Streams ELSE {CHOOSE s \in Streams : TRUE})
    /\ CASE plan = "deliver"   -> ~holed
         [] plan = "drop"      -> holed \/ budget.drops < MaxDrops
         [] plan = "duplicate" -> ~holed /\ budget.dups < MaxDups
    /\ SendPlanned(st, plan)
    /\ budget' = CASE plan = "drop" /\ ~holed -> [budget EXCEPT !.drops = @ + 1]
                   [] plan = "duplicate"      -> [budget EXCEPT !.dups = @ + 1]
                   [] OTHER                   -> budget
    /\ UNCHANGED <<framed, readyQ, recvd, reset, holed, cUp, sUp, sFrozen, heldReady>>

(* ----------------------------------------------------------------------- *)
(* Delivery time passes for a message (Message::ready_at). Unframed        *)
(* messages and frames of one stream keep their order (Pipe::push takes    *)
(* the later of its delay and its barrier's ready_at); frames of different *)
(* streams may pass each other. poll_recv holds a message whose ready_at is*)
(* at or after the time its link was held, so nothing becomes deliverable  *)
(* while the link is held.                                                 *)
(* ----------------------------------------------------------------------- *)
Barrier(k) ==
    \A j \in 1..(k - 1) :
        (Mutant = "frames_ignore_stream" \/ StreamOf(queue[j]) = StreamOf(queue[k])) => queue[j].ready

BecomeReady(k) ==
    /\ k \in 1..Len(queue)
    /\ ~queue[k].ready
    /\ Barrier(k)
    /\ ~holed \/ Mutant = "hole_ignores_in_flight"
    /\ queue' = [queue EXCEPT ![k].ready = TRUE]
    /\ readyQ' = Append(readyQ, [id |-> queue[k].id, stream |-> queue[k].stream])
    /\ heldReady' = IF holed THEN heldReady \cup {queue[k].id} ELSE heldReady
    /\ UNCHANGED <<framed, sent, recvd, stalled, stallCause, reset, holed, cUp, sUp, sFrozen, budget>>

\* Half::poll_recv (net.rs:1077): S reads the next deliverable message.
Recv ==
    /\ sUp /\ ~sFrozen
    /\ ~reset \/ Mutant = "reset_keeps_queue"
    /\ readyQ # <<>>
    /\ LET m == Head(readyQ)
           k == CHOOSE j \in 1..Len(queue) : queue[j].ready /\ queue[j].id = m.id
       IN /\ recvd' = Append(recvd, m.id)
          /\ readyQ' = Tail(readyQ)
          /\ queue' = [j \in 1..(Len(queue) - 1) |-> IF j < k THEN queue[j] ELSE queue[j + 1]]
    /\ UNCHANGED <<framed, sent, stalled, stallCause, reset, holed, cUp, sUp, sFrozen, heldReady, budget>>

(* ----------------------------------------------------------------------- *)
(* Faults                                                                  *)
(* ----------------------------------------------------------------------- *)

\* A partition: NetPlan::block plus World::black_hole (sim-oltp SimNet::configure,
\* Cache Cluster::link). Held messages stay queued.
Partition ==
    /\ ~holed
    /\ budget.holes < MaxHoles
    /\ holed' = TRUE
    /\ budget' = [budget EXCEPT !.holes = @ + 1]
    /\ UNCHANGED <<framed, sent, queue, readyQ, recvd, stalled, stallCause, reset, cUp, sUp, sFrozen, heldReady>>

\* World::heal (net.rs:658): held messages arrive in order.
Heal ==
    /\ holed
    /\ holed' = FALSE
    /\ UNCHANGED <<framed, sent, queue, readyQ, recvd, stalled, stallCause, reset, cUp, sUp, sFrozen, heldReady, budget>>

ClearPipe ==
    IF Mutant = "reset_keeps_queue" THEN UNCHANGED <<queue, readyQ>>
    ELSE queue' = <<>> /\ readyQ' = <<>>

\* World::reset_connection, or a profile Reset (Fate::Reset): both sides read
\* ConnectionReset, unread data is lost (Connection::reset, net.rs:342).
ResetConnection ==
    /\ ~reset
    /\ budget.resets < MaxResets
    /\ reset' = TRUE
    /\ ClearPipe
    /\ budget' = [budget EXCEPT !.resets = @ + 1]
    /\ UNCHANGED <<framed, sent, recvd, stalled, stallCause, holed, cUp, sUp, sFrozen, heldReady>>

\* World::crash(C) (net::crash, net.rs:554): every connection of the node is reset and
\* its queued data discarded. PR #106's power_off black-holes instead, which also
\* discards the queue. Variant crash_sends_fin is the kernel's behaviour on process
\* death: what was sent still arrives, then EOF.
CrashClient ==
    /\ cUp
    /\ budget.crashes < MaxCrashes
    /\ cUp' = FALSE
    /\ IF Variant = "crash_sends_fin"
       THEN UNCHANGED <<queue, readyQ, reset>>
       ELSE reset' = TRUE /\ queue' = <<>> /\ readyQ' = <<>>
    /\ budget' = [budget EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<framed, sent, recvd, stalled, stallCause, holed, sUp, sFrozen, heldReady>>

CrashServer ==
    /\ sUp
    /\ budget.crashes < MaxCrashes
    /\ sUp' = FALSE
    /\ reset' = TRUE
    /\ queue' = <<>> /\ readyQ' = <<>>
    /\ budget' = [budget EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<framed, sent, recvd, stalled, stallCause, holed, cUp, sFrozen, heldReady>>

\* World::freeze / thaw of S (SIGSTOP): its tasks are not polled.
Freeze ==
    /\ sUp /\ ~sFrozen
    /\ budget.freezes < MaxFreezes
    /\ sFrozen' = TRUE
    /\ budget' = [budget EXCEPT !.freezes = @ + 1]
    /\ UNCHANGED <<framed, sent, queue, readyQ, recvd, stalled, stallCause, reset, holed, cUp, sUp, heldReady>>

Thaw ==
    /\ sFrozen
    /\ sFrozen' = FALSE
    /\ UNCHANGED <<framed, sent, queue, readyQ, recvd, stalled, stallCause, reset, holed, cUp, sUp, heldReady, budget>>

Quiet ==
    /\ readyQ = <<>>
    /\ \A k \in 1..Len(queue) : ~queue[k].ready
    /\ ~(\E k \in 1..Len(queue) : ENABLED BecomeReady(k))
    /\ UNCHANGED vars

Next ==
    \/ \E st \in Streams, plan \in {"deliver", "drop", "duplicate"} : Send(st, plan)
    \/ \E k \in 1..MaxSends * 2 : BecomeReady(k)
    \/ Recv
    \/ Partition
    \/ Heal
    \/ ResetConnection
    \/ CrashClient
    \/ CrashServer
    \/ Freeze
    \/ Thaw
    \/ Quiet

Spec == Init /\ [][Next]_vars

(* ----------------------------------------------------------------------- *)
(* Properties                                                              *)
(* ----------------------------------------------------------------------- *)
SentIdsOf(st) == SelectSeq([k \in 1..Len(sent) |-> sent[k]], LAMBDA m : StreamOf(m) = st)
RecvdOf(st) ==
    SelectSeq(recvd, LAMBDA id : \E k \in 1..Len(sent) : sent[k].id = id /\ StreamOf(sent[k]) = st)
IsPrefix(a, b) == Len(a) <= Len(b) /\ \A k \in 1..Len(a) : a[k] = b[k].id

\* TCP: what S reads on each stream (the whole connection when unframed) is a prefix
\* of what C sent on it: no gap, no reordering, no duplicate.
ReceivedIsPrefix ==
    \A st \in Streams : IsPrefix(RecvdOf(st), SentIdsOf(st))

\* Each message is read at most once.
NoDuplicateDelivery ==
    \A i, j \in 1..Len(recvd) : i # j => recvd[i] # recvd[j]

\* A held link delivers nothing that was still in flight when it was cut.
HoleHoldsInFlight == heldReady = {}

\* After a reset, S reads nothing more on the connection.
NoDeliveryAfterReset == [][reset => recvd' = recvd]_vars

\* A partition delays; it never loses data on a live connection (a TCP link outage:
\* the segments are retransmitted after the heal). Fails for the Cache planner.
HeldNotLost == stallCause # "partition"

\* Gap witness: nothing a crashed client sent is read after its crash. Holds in the
\* shipped sim (the crash resets and discards the queue); a real kernel closes a
\* killed process's sockets with FIN and delivers what was sent. Variant
\* crash_sends_fin breaks it.
NoStaleFromCrashed == [][~cUp => recvd' = recvd]_vars

\* Witness: a lost message is reachable (a stalled direction), fails on the baseline.
NothingLost == ~stalled

\* Witness: frames of different streams overtake each other, fails on a framed baseline.
ConnectionFifo == IsPrefix(recvd, [k \in 1..Len(sent) |-> sent[k]])

=============================================================================
