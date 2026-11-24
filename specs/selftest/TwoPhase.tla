---------------------------- MODULE TwoPhase ----------------------------
(***************************************************************************)
(* Self-test spec for the kind T tooling (specs/README.md, "Self-test").  *)
(* It is a small but real protocol: two-phase commit with one transaction *)
(* manager (TM) and a set of resource managers (RMs), over an unordered   *)
(* channel that may lose messages, with a TM that may crash and restart.  *)
(* It exercises every part of the pipeline: TypeOK, a safety invariant, a *)
(* liveness property under named fairness, fault budgets, two mutants,   *)
(* coverage, and trace validation (TwoPhaseTrace.tla).                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    RM,          \* the resource managers
    MaxCrashes,  \* fault budget: TM crashes
    MaxDrops,    \* fault budget: lost messages
    Mutant       \* "none" in TwoPhase.cfg, a mutant name in mutants/*.cfg

VARIABLES
    rmState,     \* RM durable state: working, prepared, committed or aborted
    tmDecision,  \* TM durable state: the decision log, init, committed or aborted
    tmPrepared,  \* TM volatile state: RMs whose Prepared message it has read
    tmUp,        \* TM process is running
    msgs,        \* the channel: a set, so delivery is unordered and may repeat
    crashes,     \* faults used so far
    drops

vars == <<rmState, tmDecision, tmPrepared, tmUp, msgs, crashes, drops>>

Messages ==
    [type : {"Prepared"}, rm : RM] \cup [type : {"Commit", "Abort"}]

TypeOK ==
    /\ rmState \in [RM -> {"working", "prepared", "committed", "aborted"}]
    /\ tmDecision \in {"init", "committed", "aborted"}
    /\ tmPrepared \subseteq RM
    /\ tmUp \in BOOLEAN
    /\ msgs \subseteq Messages
    /\ crashes \in 0..MaxCrashes
    /\ drops \in 0..MaxDrops

Init ==
    /\ rmState = [r \in RM |-> "working"]
    /\ tmDecision = "init"
    /\ tmPrepared = {}
    /\ tmUp = TRUE
    /\ msgs = {}
    /\ crashes = 0
    /\ drops = 0

-----------------------------------------------------------------------------
(* Resource manager handlers. *)

\* The RM's client asks it to prepare. A client choice: no fairness.
RMPrepare(r) ==
    /\ rmState[r] = "working"
    /\ rmState' = [rmState EXCEPT ![r] = "prepared"]
    /\ msgs' = msgs \cup {[type |-> "Prepared", rm |-> r]}
    /\ UNCHANGED <<tmDecision, tmPrepared, tmUp, crashes, drops>>

\* The RM's client gives up before preparing. A client choice: no fairness.
RMChooseToAbort(r) ==
    /\ rmState[r] = "working"
    /\ rmState' = [rmState EXCEPT ![r] = "aborted"]
    /\ UNCHANGED <<tmDecision, tmPrepared, tmUp, msgs, crashes, drops>>

RMRcvCommit(r) ==
    /\ [type |-> "Commit"] \in msgs
    /\ rmState[r] = "prepared"
    /\ rmState' = [rmState EXCEPT ![r] = "committed"]
    /\ UNCHANGED <<tmDecision, tmPrepared, tmUp, msgs, crashes, drops>>

RMRcvAbort(r) ==
    /\ [type |-> "Abort"] \in msgs
    /\ rmState[r] \in {"working", "prepared"}
    /\ rmState' = [rmState EXCEPT ![r] = "aborted"]
    /\ UNCHANGED <<tmDecision, tmPrepared, tmUp, msgs, crashes, drops>>

-----------------------------------------------------------------------------
(* Transaction manager handlers. *)

TMRcvPrepared(r) ==
    /\ tmUp
    /\ tmDecision = "init"
    /\ [type |-> "Prepared", rm |-> r] \in msgs
    /\ r \notin tmPrepared
    /\ tmPrepared' = tmPrepared \cup {r}
    /\ UNCHANGED <<rmState, tmDecision, tmUp, msgs, crashes, drops>>

\* Mutant commit_one_prepared: commit once any RM is prepared.
CanCommit ==
    IF Mutant = "commit_one_prepared" THEN tmPrepared # {} ELSE tmPrepared = RM

TMCommit ==
    /\ tmUp
    /\ tmDecision = "init"
    /\ CanCommit
    /\ tmDecision' = "committed"
    /\ msgs' = msgs \cup {[type |-> "Commit"]}
    /\ UNCHANGED <<rmState, tmPrepared, tmUp, crashes, drops>>

\* The TM's decision timer fires before every RM prepared.
TMTimeout ==
    /\ tmUp
    /\ tmDecision = "init"
    /\ tmDecision' = "aborted"
    /\ msgs' = msgs \cup {[type |-> "Abort"]}
    /\ UNCHANGED <<rmState, tmPrepared, tmUp, crashes, drops>>

\* The TM's resend timer sends the logged decision again.
\* Mutant no_resend: the timer fires but sends nothing.
TMResend ==
    /\ tmUp
    /\ tmDecision # "init"
    /\ msgs' = IF Mutant = "no_resend"
               THEN msgs
               ELSE msgs \cup {[type |-> IF tmDecision = "committed" THEN "Commit" ELSE "Abort"]}
    /\ UNCHANGED <<rmState, tmDecision, tmPrepared, tmUp, crashes, drops>>

-----------------------------------------------------------------------------
(* Faults, each with a budget. A crash loses the volatile state only. *)

TMCrash ==
    /\ tmUp
    /\ crashes < MaxCrashes
    /\ tmUp' = FALSE
    /\ tmPrepared' = {}
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<rmState, tmDecision, msgs, drops>>

\* The process supervisor restarts the TM.
TMRestart ==
    /\ ~tmUp
    /\ tmUp' = TRUE
    /\ UNCHANGED <<rmState, tmDecision, tmPrepared, msgs, crashes, drops>>

DropMessage(m) ==
    /\ m \in msgs
    /\ drops < MaxDrops
    /\ msgs' = msgs \ {m}
    /\ drops' = drops + 1
    /\ UNCHANGED <<rmState, tmDecision, tmPrepared, tmUp, crashes>>

-----------------------------------------------------------------------------

Next ==
    \/ \E r \in RM :
        \/ RMPrepare(r) \/ RMChooseToAbort(r) \/ RMRcvCommit(r) \/ RMRcvAbort(r)
        \/ TMRcvPrepared(r)
    \/ TMCommit \/ TMTimeout \/ TMResend
    \/ TMCrash \/ TMRestart
    \/ \E m \in msgs : DropMessage(m)

\* Fairness, each with the mechanism that provides it:
\* - message handlers (RMRcvCommit, RMRcvAbort, TMRcvPrepared): the receive loop;
\* - TMTimeout and TMResend: the TM's decision and resend timers;
\* - TMRestart: the process supervisor.
\* Client choices (RMPrepare, RMChooseToAbort) and faults are not fair.
Fairness ==
    /\ \A r \in RM : WF_vars(RMRcvCommit(r)) /\ WF_vars(RMRcvAbort(r)) /\ WF_vars(TMRcvPrepared(r))
    /\ WF_vars(TMTimeout) /\ WF_vars(TMResend) /\ WF_vars(TMRestart)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Properties. *)

\* No RM commits while another aborts.
Consistent ==
    \A r1, r2 \in RM : ~(rmState[r1] = "committed" /\ rmState[r2] = "aborted")

\* Every RM reaches a decision. Faults are bounded by their budgets, so
\* they stop, and the fair timers and handlers finish the transaction.
Termination ==
    <>(\A r \in RM : rmState[r] \in {"committed", "aborted"})

=============================================================================
