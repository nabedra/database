----------------------------- MODULE OltpControl -----------------------------
(***************************************************************************)
(* Control's run owner and the topology steps only the owner may advance:  *)
(* a replica move and a pending ready-set removal. The owner's worker can  *)
(* be lost at any point; another worker then fences a new owner term.      *)
(*                                                                         *)
(* Plans: plans/oltp-engine.md "Queue, scheduler, and failure semantics"   *)
(* (run/term fencing; "Every decision transition checks its group's        *)
(* fence"), "Scale-up, scale-down" (one safe membership transition at a    *)
(* time; drain and fence before termination); plans/oltp-shard-refactor.md *)
(* "Ownership and memory" (fenced owner, monotonic term). Register         *)
(* OLTP-G07, OLTP-G08. Scenarios R05, O17. Independent read R-08, R-23.    *)
(*                                                                         *)
(* SHIPPED (e6b4040b): BeginOwnerFence is a compare-and-set on the ready  *)
(* view that computes owner_term + 1 from committed state                 *)
(* (machine.rs:925-949); the next owner's worker fences every group, and  *)
(* PublishFence makes it owner (authority_fence.rs:34-120, machine.rs     *)
(* 1107-1119). BeginOwnerFence and BeginColdFence answer Busy while a move *)
(* is active (machine.rs:315-321), and only the move's owner clears the   *)
(* move (machine/placement.rs:213, 409): open record                      *)
(* 2026-11-24-oltp-sim-move-owner-loss. While a ready-set transition is   *)
(* pending, supervise_membership advances it only on the owner's worker   *)
(* and begins no owner fence (worker_runtime/recovery.rs:109-123,         *)
(* 150-181): open record 2026-11-25-oltp-sim-removal-owner-lost.          *)
(*                                                                         *)
(* Control is one linearizable replicated state here; its Raft group is   *)
(* specs/oltp/ranges with the system-group identity rules. A worker that  *)
(* is lost never returns under its identity (a restart is a new           *)
(* incarnation, which is never the recorded owner).                       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Worker,      \* enrolled workers
    Mutant,
    MaxLoss,     \* workers lost for good
    MaxMoves,    \* replica moves the owner starts
    MaxRemoves   \* ready-set removals the owner starts

None == "none"

(***************************************************************************)
(* Design choices under test.                                              *)
(***************************************************************************)
\* Fixed design: BeginOwnerFence adopts an active move under the new owner
\* term (or aborts it), so a lost move owner cannot block owner change.
FenceAdoptsMove == Mutant # "prefix_move_blocks_owner_fence"
\* Fixed design: a pending ready-set transition does not stop the
\* supervisor from beginning an owner fence when the owner is lost.
FenceFromPending == Mutant # "prefix_remove_owner_lost"

VARIABLES
    up,          \* [Worker -> BOOLEAN]: the worker's current incarnation runs
    owner,       \* Control: the run owner
    term,        \* Control: owner_term
    fencing,     \* Control: FencePending, the candidate owner or None
    move,        \* Control: the active move's owner, or None
    remove,      \* Control: the pending removal's owner, or None
    stale,       \* ghost: a step ran under an owner term Control had retired
    bud

vars == <<up, owner, term, fencing, move, remove, stale, bud>>

Live == {w \in Worker : up[w]}

Init ==
    /\ up = [w \in Worker |-> TRUE]
    /\ \E w \in Worker : owner = w
    /\ term = 1
    /\ fencing = None
    /\ move = None
    /\ remove = None
    /\ stale = FALSE
    /\ bud = [loss |-> 0, moves |-> 0, removes |-> 0]

\* The worker of the current incarnation is lost (node loss, or a restart,
\* which comes back as a new incarnation). Two workers stay live, so a
\* Control quorum is assumed throughout.
LoseWorker(w) ==
    /\ up[w] /\ bud.loss < MaxLoss
    /\ Cardinality(Live) > 2
    /\ up' = [up EXCEPT ![w] = FALSE]
    /\ bud' = [bud EXCEPT !.loss = @ + 1]
    /\ UNCHANGED <<owner, term, fencing, move, remove, stale>>

\* The owner starts a replica move (Control begin; one move per cluster,
\* ct/replica-move-active).
BeginMove ==
    /\ up[owner] /\ move = None /\ fencing = None /\ bud.moves < MaxMoves
    /\ move' = owner
    /\ bud' = [bud EXCEPT !.moves = @ + 1]
    /\ UNCHANGED <<up, owner, term, fencing, remove, stale>>

\* The move's owner finishes it and clears the active key. A step of a
\* deposed owner is refused by the owner check (machine/placement.rs:100).
FinishMove ==
    /\ move # None /\ up[move] /\ move = owner
    /\ move' = None
    /\ UNCHANGED <<up, owner, term, fencing, remove, stale, bud>>

\* The owner starts a ready-set removal (Pending::Ready Remove).
BeginRemove ==
    /\ up[owner] /\ remove = None /\ fencing = None /\ bud.removes < MaxRemoves
    /\ remove' = owner
    /\ bud' = [bud EXCEPT !.removes = @ + 1]
    /\ UNCHANGED <<up, owner, term, fencing, move, stale>>

\* supervise_membership advances the pending removal on the owner's worker.
AdvanceRemove ==
    /\ remove # None /\ up[owner] /\ fencing = None
    /\ stale' = (stale \/ remove # owner)
    /\ remove' = None
    /\ UNCHANGED <<up, owner, term, fencing, move, bud>>

\* A live worker that sees the owner lost begins an owner fence. Shipped
\* refuses it while a move is active, and the supervisor does not try it
\* while a removal is pending.
BeginOwnerFence(w) ==
    /\ up[w] /\ ~up[owner] /\ fencing = None
    /\ move # None => FenceAdoptsMove
    /\ remove # None => FenceFromPending
    /\ fencing' = w
    /\ UNCHANGED <<up, owner, term, move, remove, stale, bud>>

\* The candidate fences every group and PublishFence makes it the owner.
\* In the fixed design it adopts the active move and the pending removal.
PublishFence ==
    /\ fencing # None /\ up[fencing]
    /\ owner' = fencing
    /\ term' = term + 1
    /\ fencing' = None
    /\ move' = IF move # None /\ Mutant # "adopt_keeps_old_owner" THEN fencing ELSE move
    /\ remove' = IF remove # None /\ Mutant # "adopt_keeps_old_owner" THEN fencing ELSE remove
    /\ UNCHANGED <<up, stale, bud>>

\* A fence whose candidate was lost before publishing is dropped, and the
\* supervisor of another worker may begin again.
DropFence ==
    /\ fencing # None /\ ~up[fencing]
    /\ fencing' = None
    /\ UNCHANGED <<up, owner, term, move, remove, stale, bud>>

LoseAny == \E w \in Worker : LoseWorker(w)
BeginFenceAny == \E w \in Worker : BeginOwnerFence(w)

\* Nothing left to do: the owner is live and no step is in flight. Or the
\* run waits for an owner that nobody can establish: the liveness
\* properties decide whether such a wait ends.
Terminal ==
    /\ \/ up[owner] /\ fencing = None /\ move = None /\ remove = None
       \/ ~up[owner]
    /\ UNCHANGED vars

Next ==
    \/ LoseAny \/ BeginMove \/ FinishMove \/ BeginRemove \/ AdvanceRemove
    \/ BeginFenceAny \/ PublishFence \/ DropFence
    \/ Terminal

Spec == Init /\ [][Next]_vars

\* Mechanisms: the owner's move workflow (FinishMove), supervise_membership
\* on each worker (AdvanceRemove, BeginOwnerFence, DropFence) and the fence
\* driver (PublishFence). Losses and new work get no fairness.
Fairness ==
    /\ WF_vars(FinishMove) /\ WF_vars(AdvanceRemove)
    /\ WF_vars(BeginFenceAny) /\ WF_vars(PublishFence) /\ WF_vars(DropFence)
LiveSpec == Spec /\ Fairness

TypeOK ==
    /\ up \in [Worker -> BOOLEAN]
    /\ owner \in Worker /\ term \in 1..(MaxLoss + 1)
    /\ fencing \in Worker \cup {None}
    /\ move \in Worker \cup {None} /\ remove \in Worker \cup {None}
    /\ stale \in BOOLEAN

\* OLTP-G07: no topology step runs under a retired owner term.
G07_NoStaleOwnerStep == ~stale

\* OLTP-G07 (R05): a lost owner is replaced, so the run keeps an owner.
G07_OwnerReplaced == (~up[owner]) ~> up[owner]
\* OLTP-G08 (O17): a started move and a pending removal end.
G08_MoveEnds == (move # None) ~> (move = None)
G08_RemoveEnds == (remove # None) ~> (remove = None)
=============================================================================
