----------------------------- MODULE SimNetRpc -----------------------------
(***************************************************************************)
(* The simulator's message network: the Events peer router, one RPC per    *)
(* call planned by NetPlan, as shipped on main 7dadd41d.                   *)
(*                                                                         *)
(*   server/events/src/sim/net.rs  call (request, duplicate copy, reply)  *)
(*   server/sim/src/net.rs         NetPlan::{plan, block, pause}           *)
(*                                                                         *)
(* Design: phase-10-11 section 3.1 (P10-EVENTS), events/src/sim/README.md *)
(* "Network". Scenarios: EV01, EV02, EV07. Guarantees that assume it:      *)
(* EVT-G01, EVT-G03, EVT-G06.                                              *)
(*                                                                         *)
(* A caller node C sends RPCs to a target node T. Each RPC is planned when *)
(* it is sent: dropped (a black hole until the caller's deadline), or      *)
(* delivered after a delay, possibly duplicated. The target checks the     *)
(* link when the request arrives. The reply crosses the same filters.      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    MaxCalls,    \* RPCs C sends
    MaxDrops,    \* NetPlan drops (requests and replies)
    MaxDups,     \* NetPlan duplicates
    MaxBlocks,   \* NetPlan::block of the C to T link
    MaxPauses,   \* NetPlan::pause of T
    MaxCrashes,  \* crash of T
    Mutant       \* "none" or a mutant name (see mutants/ and the README)

VARIABLES
    call,        \* id -> state of the call at the caller
    dupPending,  \* ids whose duplicate copy is in flight
    handled,     \* id -> times T ran the handler
    order,       \* ids in the order T handled them
    blocked,     \* NetPlan::is_blocked(C, T)
    tPaused,     \* NetPlan::is_paused(T)
    tUp,         \* T's broker exists
    crossed,     \* ghost: handler runs that arrived through a blocked link
    pausedRuns,  \* ghost: handler runs while T was paused
    budget

vars == <<call, dupPending, handled, order, blocked, tPaused, tUp, crossed, pausedRuns, budget>>

Ids == 1..MaxCalls
States == {"idle", "flight", "lostRequest", "handling", "handled", "lostReply",
           "replied", "refused", "none"}

TypeOK ==
    /\ call \in [Ids -> States]
    /\ dupPending \subseteq Ids
    /\ handled \in [Ids -> Nat]
    /\ order \in Seq(Ids)
    /\ blocked \in BOOLEAN /\ tPaused \in BOOLEAN /\ tUp \in BOOLEAN
    /\ crossed \in Nat /\ pausedRuns \in Nat
    /\ budget \in [drops : Nat, dups : Nat, blocks : Nat, pauses : Nat, crashes : Nat]

Init ==
    /\ call = [i \in Ids |-> "idle"]
    /\ dupPending = {}
    /\ handled = [i \in Ids |-> 0]
    /\ order = <<>>
    /\ blocked = FALSE /\ tPaused = FALSE /\ tUp = TRUE
    /\ crossed = 0 /\ pausedRuns = 0
    /\ budget = [drops |-> 0, dups |-> 0, blocks |-> 0, pauses |-> 0, crashes |-> 0]

\* net.rs:37-48: NetPlan::plan(from, to). A blocked link drops without a draw.
Send(i, plan) ==
    /\ call[i] = "idle"
    /\ \A j \in 1..(i - 1) : call[j] # "idle"   \* C sends in id order
    /\ CASE plan = "drop"      -> blocked \/ budget.drops < MaxDrops
         [] plan = "deliver"   -> ~blocked
         [] plan = "duplicate" -> ~blocked /\ budget.dups < MaxDups
    /\ call' = [call EXCEPT ![i] = IF plan = "drop" THEN "lostRequest" ELSE "flight"]
    /\ dupPending' = IF plan = "duplicate" THEN dupPending \cup {i} ELSE dupPending
    /\ budget' = CASE plan = "drop" /\ ~blocked -> [budget EXCEPT !.drops = @ + 1]
                   [] plan = "duplicate"        -> [budget EXCEPT !.dups = @ + 1]
                   [] OTHER                     -> budget
    /\ UNCHANGED <<handled, order, blocked, tPaused, tUp, crossed, pausedRuns>>

\* net.rs:49-54: after the delay, a crashed target refuses; a link blocked now drops.
Arrive(i) ==
    /\ call[i] = "flight"
    /\ LET next == CASE ~tUp                                           -> "refused"
                      [] blocked /\ Mutant # "link_checked_at_send_only" -> "lostRequest"
                      [] OTHER                                          -> "handling"
       IN /\ call' = [call EXCEPT ![i] = next]
          \* the duplicate copy is spawned only after these checks pass
          /\ dupPending' = IF next = "handling" THEN dupPending ELSE dupPending \ {i}
    /\ crossed' = IF tUp /\ blocked /\ Mutant = "link_checked_at_send_only" THEN crossed + 1 ELSE crossed
    /\ UNCHANGED <<handled, order, blocked, tPaused, tUp, pausedRuns, budget>>

\* The serve task waits until T is unpaused (net.rs:66-72), then runs the handler.
Handle(i) ==
    /\ call[i] = "handling"
    /\ tUp
    /\ ~tPaused \/ Mutant = "pause_ignored"
    /\ call' = [call EXCEPT ![i] = "handled"]
    /\ handled' = [handled EXCEPT ![i] = @ + 1]
    /\ order' = Append(order, i)
    /\ pausedRuns' = IF tPaused THEN pausedRuns + 1 ELSE pausedRuns
    /\ UNCHANGED <<dupPending, blocked, tPaused, tUp, crossed, budget>>

\* The duplicate copy (net.rs:56-64) is spawned when the original arrives and runs the
\* handler 1 ms later on the broker it captured. It waits for T to be unpaused. The
\* baseline checks the link again before it runs (the fix); mutant
\* prefix_dup_crosses_block is the router that does not.
HandleDuplicate(i) ==
    /\ i \in dupPending
    /\ call[i] \notin {"idle", "flight"}
    /\ tUp
    /\ ~tPaused \/ Mutant = "pause_ignored"
    /\ dupPending' = dupPending \ {i}
    /\ IF blocked /\ Mutant # "prefix_dup_crosses_block"
       THEN UNCHANGED <<handled, order, crossed, pausedRuns>>
       ELSE /\ handled' = [handled EXCEPT ![i] = @ + 1]
            /\ order' = Append(order, i)
            /\ crossed' = IF blocked THEN crossed + 1 ELSE crossed
            /\ pausedRuns' = IF tPaused THEN pausedRuns + 1 ELSE pausedRuns
    /\ UNCHANGED <<call, blocked, tPaused, tUp, budget>>

\* net.rs:74-88: the reply is planned like a request.
Reply(i, plan) ==
    /\ call[i] = "handled"
    /\ CASE plan = "drop"    -> budget.drops < MaxDrops
         [] plan = "deliver" -> TRUE
    /\ call' = [call EXCEPT ![i] = IF plan = "drop" THEN "lostReply" ELSE "replied"]
    /\ budget' = IF plan = "drop" THEN [budget EXCEPT !.drops = @ + 1] ELSE budget
    /\ UNCHANGED <<dupPending, handled, order, blocked, tPaused, tUp, crossed, pausedRuns>>

\* nabedra_rt::time::timeout(ttl): the caller gives up and sees None.
Timeout(i) ==
    /\ call[i] \in {"lostRequest", "lostReply", "handling", "refused"}
    /\ call' = [call EXCEPT ![i] = "none"]
    /\ UNCHANGED <<dupPending, handled, order, blocked, tPaused, tUp, crossed, pausedRuns, budget>>

Block ==
    /\ ~blocked /\ budget.blocks < MaxBlocks
    /\ blocked' = TRUE
    /\ budget' = [budget EXCEPT !.blocks = @ + 1]
    /\ UNCHANGED <<call, dupPending, handled, order, tPaused, tUp, crossed, pausedRuns>>

Unblock ==
    /\ blocked
    /\ blocked' = FALSE
    /\ UNCHANGED <<call, dupPending, handled, order, tPaused, tUp, crossed, pausedRuns, budget>>

Pause ==
    /\ ~tPaused /\ budget.pauses < MaxPauses
    /\ tPaused' = TRUE
    /\ budget' = [budget EXCEPT !.pauses = @ + 1]
    /\ UNCHANGED <<call, dupPending, handled, order, blocked, tUp, crossed, pausedRuns>>

Resume ==
    /\ tPaused
    /\ tPaused' = FALSE
    /\ UNCHANGED <<call, dupPending, handled, order, blocked, tUp, crossed, pausedRuns, budget>>

\* A crash of T aborts its serve tasks and duplicate copies (World::crash); the caller
\* waits for its deadline. The restart opens a new broker.
CrashTarget ==
    /\ tUp /\ budget.crashes < MaxCrashes
    /\ tUp' = FALSE
    /\ call' = [i \in Ids |-> IF call[i] = "handling" THEN "lostRequest" ELSE call[i]]
    /\ dupPending' = {}
    /\ budget' = [budget EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<handled, order, blocked, tPaused, crossed, pausedRuns>>

RestartTarget ==
    /\ ~tUp
    /\ tUp' = TRUE
    /\ UNCHANGED <<call, dupPending, handled, order, blocked, tPaused, crossed, pausedRuns, budget>>

Done ==
    /\ \A i \in Ids : call[i] \in {"replied", "none"}
    /\ dupPending = {}
    /\ UNCHANGED vars

Next ==
    \/ \E i \in Ids, p \in {"drop", "deliver", "duplicate"} : Send(i, p)
    \/ \E i \in Ids : Arrive(i)
    \/ \E i \in Ids : Handle(i)
    \/ \E i \in Ids : HandleDuplicate(i)
    \/ \E i \in Ids : Timeout(i)
    \/ \E i \in Ids, p \in {"drop", "deliver"} : Reply(i, p)
    \/ Block
    \/ Unblock
    \/ Pause
    \/ Resume
    \/ CrashTarget
    \/ RestartTarget
    \/ Done

Spec == Init /\ [][Next]_vars

(* ----------------------------------------------------------------------- *)
(* Properties                                                              *)
(* ----------------------------------------------------------------------- *)

\* A blocked link carries nothing: no handler runs on a request that arrived while
\* the link was blocked. The shipped duplicate copy crosses a link blocked after the
\* original arrived: mutant prefix_dup_crosses_block.
NothingCrossesBlockedLink == crossed = 0

\* A paused target runs no handler until it resumes.
PausedRunsNothing == pausedRuns = 0

\* A refused or lost call ran no handler before the caller learned it, unless the
\* reply was lost: witness for "applied, reply lost" (fails on the baseline).
NoneMeansNotHandled == \A i \in Ids : call[i] = "none" => handled[i] = 0

\* Witness: a duplicate runs the handler twice (fails on the baseline).
AtMostOnceHandling == \A i \in Ids : handled[i] <= 1

\* Witness: requests are handled out of send order (fails on the baseline).
HandledInOrder == \A a, b \in 1..Len(order) : a < b => order[a] <= order[b]

=============================================================================
