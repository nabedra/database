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
(* view that computes owner_term + 1 from committed state; the next       *)
(* owner's worker fences every group, and PublishFence makes it owner.    *)
(* BeginOwnerFence and BeginColdFence answer Busy while a move is active  *)
(* (machine.rs:315-321), and only the move's owner clears the move        *)
(* (machine/placement.rs:213, 409): open record                           *)
(* 2026-11-24-oltp-sim-move-owner-loss. While a ready-set transition is   *)
(* pending, supervise_membership advances it only on the owner's worker   *)
(* and begins no owner fence (worker_runtime/recovery.rs): open record    *)
(* 2026-11-25-oltp-sim-removal-owner-lost.                                *)
(*                                                                         *)
(* CONTROL-1 (fx-control, 2026-11-27): the owner fence resolves an active *)
(* move by its phase, and carries the pending removal.                    *)
(*  - Claimed (begun, guard not armed, no replica added): the fence       *)
(*    cancels it, as CancelReplicaMove does.                              *)
(*  - Published (the new placement is in Control, the removed replica not *)
(*    yet retired): the fence releases the cluster's move slot and the    *)
(*    new owner retires the removed replica (variable retire).            *)
(*  - Moving (guard armed, the target being added to the group): the      *)
(*    fence turns it into a rollback owned by the new owner. The target   *)
(*    of a local-target move is the lost owner's worker itself, so the    *)
(*    move cannot be finished; a remote target may be lost too. The same  *)
(*    rollback ends a move whose target is lost while the owner lives.    *)
(*    Rollback (plan): restore the group's membership without the target  *)
(*    (or hand a committed dead target to damage replacement), release    *)
(*    the Catalog guard, then clear the slot.                             *)
(*  - The pending removal continues under the new owner (the OQ-2 rule of *)
(*    specs/oltp/queue settles its Timestamp fence first).                *)
(* Code: Claimed, Published and the removal are implemented; Moving still *)
(* answers Busy (mutant code_moving_busy), plan fixed, code pending.      *)
(*                                                                         *)
(* Control is one linearizable replicated state here; its Raft group is   *)
(* specs/oltp/ranges with the system-group identity rules. A worker that  *)
(* is lost never returns under its identity (a restart is a new           *)
(* incarnation, which is never the recorded owner). Retiring a removed    *)
(* replica whose worker is lost needs that worker's process exclusion,    *)
(* which this model does not show: a retirement here needs only a live    *)
(* owner.                                                                 *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Worker,      \* enrolled workers
    Mutant,
    MaxLoss,     \* workers lost for good
    MaxMoves,    \* replica moves the owner starts
    MaxRemoves   \* ready-set removals the owner starts

None == "none"
Phases == {"claimed", "moving", "published", "rollback"}
\* No active move (the record form keeps TLC comparisons between records).
NoMove == [own |-> None, phase |-> "idle", target |-> None]

(***************************************************************************)
(* Design choices under test.                                              *)
(***************************************************************************)
\* The shipped rule: BeginOwnerFence answers Busy while any move is active.
FenceBlockedByMove(m) ==
    \/ Mutant = "prefix_move_blocks_owner_fence"
    \/ Mutant = "code_moving_busy" /\ m.phase = "moving"
\* Fixed design: a pending ready-set transition does not stop the
\* supervisor from beginning an owner fence when the owner is lost.
FenceFromPending == Mutant # "prefix_remove_owner_lost"

VARIABLES
    up,          \* [Worker -> BOOLEAN]: the worker's current incarnation runs
    owner,       \* Control: the run owner
    term,        \* Control: owner_term
    fencing,     \* Control: FencePending, the candidate owner or None
    move,        \* Control: the active move (ct/replica-move-active), or None
    retire,      \* Control: the owner that retires a published move's removed
                 \* replica after the slot was released, or None
    remove,      \* Control: the pending removal's owner, or None
    stale,       \* ghost: a step ran under an owner term Control had retired
    bud

vars == <<up, owner, term, fencing, move, retire, remove, stale, bud>>

Live == {w \in Worker : up[w]}
MoveOf(w, p, t) == [own |-> w, phase |-> p, target |-> t]

Init ==
    /\ up = [w \in Worker |-> TRUE]
    /\ \E w \in Worker : owner = w
    /\ term = 1
    /\ fencing = None
    /\ move = NoMove
    /\ retire = None
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
    /\ UNCHANGED <<owner, term, fencing, move, retire, remove, stale>>

\* The owner claims a replica move (BeginReplicaMove; one per cluster). The
\* target worker t is the owner itself for a local-target move.
BeginMove(t) ==
    /\ up[owner] /\ up[t] /\ move = NoMove /\ fencing = None /\ bud.moves < MaxMoves
    /\ move' = MoveOf(owner, "claimed", t)
    /\ bud' = [bud EXCEPT !.moves = @ + 1]
    /\ UNCHANGED <<up, owner, term, fencing, retire, remove, stale>>

\* Each move step is the move owner's, and Control checks that it is still
\* the run owner (machine/placement.rs) and that no owner fence is pending.
OwnerStep(p) ==
    /\ move # NoMove /\ move.phase = p /\ move.own = owner /\ up[owner]
    /\ fencing = None

\* ArmReplicaMove: the Catalog guard is armed; the target is added next.
ArmMove ==
    /\ OwnerStep("claimed")
    /\ move' = [move EXCEPT !.phase = "moving"]
    /\ UNCHANGED <<up, owner, term, fencing, retire, remove, stale, bud>>

\* CancelReplicaMove: a claimed move ends without touching the group.
CancelMove ==
    /\ OwnerStep("claimed")
    /\ move' = NoMove
    /\ UNCHANGED <<up, owner, term, fencing, retire, remove, stale, bud>>

\* PublishReplicaMove: the target activated, the new placement is published.
PublishMove ==
    /\ OwnerStep("moving") /\ up[move.target]
    /\ move' = [move EXCEPT !.phase = "published"]
    /\ UNCHANGED <<up, owner, term, fencing, retire, remove, stale, bud>>

\* The target of a moving move was lost: its owner rolls it back.
AbortMove ==
    /\ OwnerStep("moving") /\ ~up[move.target]
    /\ move' = [move EXCEPT !.phase = "rollback"]
    /\ UNCHANGED <<up, owner, term, fencing, retire, remove, stale, bud>>

\* The rollback restores the group without the target and releases the
\* guard and the slot.
FinishRollback ==
    /\ OwnerStep("rollback")
    /\ move' = NoMove
    /\ UNCHANGED <<up, owner, term, fencing, retire, remove, stale, bud>>

\* ReplicaRetired: the removed replica retired; the slot is released.
RetireMove ==
    /\ OwnerStep("published")
    /\ move' = NoMove
    /\ UNCHANGED <<up, owner, term, fencing, retire, remove, stale, bud>>

\* The retirement of a published move whose slot a fence released. Mutant
\* retire_requires_slot keeps the shipped check that the move still holds
\* the cluster's slot (machine/placement.rs retire), which the fence released.
RetireAdopted ==
    /\ Mutant # "retire_requires_slot"
    /\ retire # None /\ up[owner] /\ fencing = None
    /\ stale' = (stale \/ retire # owner)
    /\ retire' = None
    /\ UNCHANGED <<up, owner, term, fencing, move, remove, bud>>

\* The owner starts a ready-set removal (Pending::Ready Remove).
BeginRemove ==
    /\ up[owner] /\ remove = None /\ fencing = None /\ bud.removes < MaxRemoves
    /\ remove' = owner
    /\ bud' = [bud EXCEPT !.removes = @ + 1]
    /\ UNCHANGED <<up, owner, term, fencing, move, retire, stale>>

\* supervise_membership advances the pending removal on the owner's worker.
AdvanceRemove ==
    /\ remove # None /\ up[owner] /\ fencing = None
    /\ stale' = (stale \/ remove # owner)
    /\ remove' = None
    /\ UNCHANGED <<up, owner, term, fencing, move, retire, bud>>

\* A live worker that sees the owner lost begins an owner fence. Shipped
\* refuses it while a move is active, and the supervisor does not try it
\* while a removal is pending.
BeginOwnerFence(w) ==
    /\ up[w] /\ ~up[owner] /\ fencing = None
    /\ move # NoMove => ~FenceBlockedByMove(move)
    /\ remove # None => FenceFromPending
    /\ fencing' = w
    /\ UNCHANGED <<up, owner, term, move, retire, remove, stale, bud>>

\* The candidate fences every group and PublishFence makes it the owner.
\* The active move is resolved by its phase; the pending removal and an
\* adopted retirement continue under the new owner. Mutant
\* adopt_keeps_old_owner keeps the steps under the old owner.
Resolve(m, w) ==
    CASE m.phase = "claimed" -> NoMove
      [] m.phase = "published" -> NoMove
      [] OTHER -> [m EXCEPT !.own = w, !.phase = "rollback"]
PublishFence ==
    /\ fencing # None /\ up[fencing]
    /\ owner' = fencing
    /\ term' = term + 1
    /\ fencing' = None
    /\ IF Mutant = "adopt_keeps_old_owner"
       THEN /\ move' = move /\ retire' = retire /\ remove' = remove
       ELSE /\ move' = IF move = NoMove THEN NoMove ELSE Resolve(move, fencing)
            /\ retire' = IF (move # NoMove /\ move.phase = "published") \/ retire # None
                         THEN fencing ELSE None
            /\ remove' = IF remove # None THEN fencing ELSE None
    /\ UNCHANGED <<up, stale, bud>>

\* A fence whose candidate was lost before publishing is dropped, and the
\* supervisor of another worker may begin again.
DropFence ==
    /\ fencing # None /\ ~up[fencing]
    /\ fencing' = None
    /\ UNCHANGED <<up, owner, term, move, retire, remove, stale, bud>>

LoseAny == \E w \in Worker : LoseWorker(w)
BeginFenceAny == \E w \in Worker : BeginOwnerFence(w)
BeginMoveAny == \E t \in Worker : BeginMove(t)

\* Nothing left to do: the owner is live and no step is in flight. Or the
\* run waits for an owner that nobody can establish: the liveness
\* properties decide whether such a wait ends.
Terminal ==
    /\ \/ up[owner] /\ fencing = None /\ move = NoMove /\ remove = None /\ retire = None
       \/ ~up[owner] \/ retire # None
    /\ UNCHANGED vars

Next ==
    \/ LoseAny \/ BeginMoveAny \/ ArmMove \/ CancelMove \/ PublishMove
    \/ AbortMove \/ FinishRollback \/ RetireMove \/ RetireAdopted
    \/ BeginRemove \/ AdvanceRemove
    \/ BeginFenceAny \/ PublishFence \/ DropFence
    \/ Terminal

Spec == Init /\ [][Next]_vars

\* Mechanisms: the owner's move workflow (ArmMove, PublishMove, AbortMove,
\* FinishRollback, RetireMove, RetireAdopted), supervise_membership on each
\* worker (AdvanceRemove, BeginOwnerFence, DropFence) and the fence driver
\* (PublishFence). Losses, new work and the operator's CancelMove get no
\* fairness.
Fairness ==
    /\ WF_vars(ArmMove) /\ WF_vars(PublishMove) /\ WF_vars(AbortMove)
    /\ WF_vars(FinishRollback) /\ WF_vars(RetireMove) /\ WF_vars(RetireAdopted)
    /\ WF_vars(AdvanceRemove)
    /\ WF_vars(BeginFenceAny) /\ WF_vars(PublishFence) /\ WF_vars(DropFence)
LiveSpec == Spec /\ Fairness

TypeOK ==
    /\ up \in [Worker -> BOOLEAN]
    /\ owner \in Worker /\ term \in 1..(MaxLoss + 1)
    /\ fencing \in Worker \cup {None}
    /\ move \in {NoMove} \cup [own : Worker, phase : Phases, target : Worker]
    /\ retire \in Worker \cup {None} /\ remove \in Worker \cup {None}
    /\ stale \in BOOLEAN

\* OLTP-G07: no topology step runs under a retired owner term.
G07_NoStaleOwnerStep == ~stale

\* OLTP-G07 (R05): a lost owner is replaced, so the run keeps an owner.
G07_OwnerReplaced == (~up[owner]) ~> up[owner]
\* OLTP-G08 (O17): a started move, an adopted retirement and a pending
\* removal end.
G08_MoveEnds == (move # NoMove) ~> (move = NoMove)
G08_RetireEnds == (retire # None) ~> (retire = None)
G08_RemoveEnds == (remove # None) ~> (remove = None)
=============================================================================
