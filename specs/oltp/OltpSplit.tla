------------------------------ MODULE OltpSplit ------------------------------
(***************************************************************************)
(* Split of one range into two under sustained direct writes: the write    *)
(* barrier, the cut, the source freeze, publication, abort and retry.      *)
(*                                                                         *)
(* Plan: plans/oltp-direct-path.md, addendum "Splits under write load"     *)
(* (a replicated barrier in the source before the cut), and                *)
(* plans/oltp-shard-refactor.md "Failure and movement". OLTP-G06, G01.     *)
(*                                                                         *)
(* Barrier = "written": the coordinator proposes DirectPause before Cut.   *)
(* Barrier = "shipped": nothing proposes DirectPause. At c8426fe1 the only *)
(* non-test mention is the type itself (mvcc/store/direct.rs:159).         *)
(*                                                                         *)
(* Refusal rule, from the code and not inferred: freeze_source rejects     *)
(* DirectCommitAboveCut when the highest applied direct commit timestamp   *)
(* is above the cut (lineage.rs:514). It has other refusals (an intent, a  *)
(* stale binding, another split's barrier, a receipt); of those only the   *)
(* barrier and the receipt are modelled. This spec therefore does not say  *)
(* which refusal any observed run hit.                                     *)
(*                                                                         *)
(* Raft safety of each range is assumed here and checked in OltpRanges: a  *)
(* committed entry survives, and only a connected leader with a quorum     *)
(* commits. Timestamps are reduced to one bit, "allocated after the cut of *)
(* the current attempt", which is all freeze_source and the image compare. *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Node,        \* three workers hosting all three ranges
    Client,      \* writers, each with its own key; they write forever
    Left,        \* the clients whose keys go to the left child
    Barrier,     \* "written" or "shipped"
    Fixes,       \* candidate fixes: "release_unbound", "pause_checks_receipt"
    Mutant,
    MaxAttempts, \* AUTOMATIC_ATTEMPTS = 3 per shard epoch (timestamp/split.rs:23)
    MaxCrash,    \* worker crashes; a worker that never restarts lost its disk
    MaxPart,     \* partitions of one worker
    MaxCoord,    \* coordinator crashes
    MaxGiveUp,   \* aborts after transient step failures (automatic_split.rs:447)
    MaxLead,     \* leadership changes while the old leader still works
    MaxDup       \* late copies of a request or of a DirectPause

Range == {"src", "left", "right"}
Attempts == 1..MaxAttempts
None == "none"

VARIABLES
    up, iso,     \* workers
    ldr,         \* [Range -> Node \cup {None}]
    pause,       \* attempt holding the barrier, 0 if none       (source log)
    frozen,      \* attempt that froze the source, 0 if none      (source log)
    pub,         \* the source recorded the publication           (source log)
    receipts,    \* attempts whose settlement the source recorded (source log)
    hi,          \* a direct commit above the current cut applied (source log)
    above,       \* clients whose applied write is above the cut  (source log)
    att, phase,  \* current attempt and its durable progress      (Timestamps)
    dec,         \* its durable decision: none, commit, abort     (Status)
    refused,     \* the freeze of this attempt was refused
    coord,       \* coordinator process is up
    pausedOk,    \* the coordinator has the barrier's reply       (volatile)
    route,       \* the Catalog routes to "src" or to "kids"
    gw,          \* [Client -> "src" / "kids"], cached route
    w,           \* [Client -> "idle" / "below" / "above"], write in flight
    lateReq,     \* clients with a late request copy addressed to the source
    latePause,   \* attempts with a late DirectPause
    missing,     \* clients whose committed write the child image lacks
    badOwner,    \* a write applied at the source after publication
    bud

hostV == <<up, iso>>
srcV == <<pause, frozen, pub, receipts, hi, above>>
attV == <<att, phase, dec, refused>>
coV == <<coord, pausedOk>>
cliV == <<gw, w>>
lateV == <<lateReq, latePause>>
ghostV == <<missing, badOwner>>
vars == <<hostV, ldr, srcV, attV, coV, route, cliV, lateV, ghostV, bud>>

Conn(n) == up[n] /\ iso # n
Can(r) == ldr[r] # None /\ Conn(ldr[r]) /\ 2 * Cardinality({n \in Node : Conn(n)}) > Cardinality(Node)
Child(c) == IF c \in Left THEN "left" ELSE "right"

Init ==
    /\ up = [n \in Node |-> TRUE] /\ iso = None
    /\ ldr = [r \in Range |-> None]
    /\ pause = 0 /\ frozen = 0 /\ pub = FALSE /\ receipts = {} /\ hi = FALSE /\ above = {}
    /\ att = 0 /\ phase = "none" /\ dec = None /\ refused = FALSE
    /\ coord = TRUE /\ pausedOk = FALSE
    /\ route = "src"
    /\ gw = [c \in Client |-> "src"] /\ w = [c \in Client |-> "idle"]
    /\ lateReq = {} /\ latePause = {}
    /\ missing = {} /\ badOwner = FALSE
    /\ bud = [crash |-> 0, part |-> 0, coord |-> 0, give |-> 0, lead |-> 0, dup |-> 0]

(***************************************************************************)
(* Workers and leadership.                                                 *)
(***************************************************************************)
\* A new leader of r. Proposals in flight at the source are kept or dropped.
\* The barrier is a log entry, so it survives; the mutant keeps it in the
\* leader's memory.
NewLeader(r, n) ==
    /\ Conn(n) /\ ldr[r] # n
    /\ 2 * Cardinality({m \in Node : Conn(m)}) > Cardinality(Node)
    /\ r \in {"left", "right"} => phase \in {"frozen", "staged", "published"} \/ route = "kids"
    /\ ldr' = [ldr EXCEPT ![r] = n]
    \* Replacing a working leader is budgeted; replacing a lost one is free.
    /\ IF Can(r) THEN bud.lead < MaxLead /\ bud' = [bud EXCEPT !.lead = @ + 1]
                 ELSE UNCHANGED bud
    /\ IF r # "src" THEN UNCHANGED <<w, pause>>
       ELSE /\ \E keep \in SUBSET Client :
                 w' = [c \in Client |-> IF c \in keep THEN w[c] ELSE "idle"]
            /\ pause' = IF Mutant = "pause_volatile" THEN 0 ELSE pause
    /\ UNCHANGED <<hostV, frozen, pub, receipts, hi, above, attV, coV, route, gw, lateV,
                   ghostV>>

Crash(n) ==
    /\ up[n] /\ bud.crash < MaxCrash
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ bud' = [bud EXCEPT !.crash = @ + 1]
    /\ UNCHANGED <<iso, ldr, srcV, attV, coV, route, cliV, lateV, ghostV>>

Restart(n) ==
    /\ ~up[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ ldr' = [r \in Range |-> IF ldr[r] = n THEN None ELSE ldr[r]]
    /\ UNCHANGED <<iso, srcV, attV, coV, route, cliV, lateV, ghostV, bud>>

Partition(n) ==
    /\ iso = None /\ bud.part < MaxPart
    /\ iso' = n
    /\ bud' = [bud EXCEPT !.part = @ + 1]
    /\ UNCHANGED <<up, ldr, srcV, attV, coV, route, cliV, lateV, ghostV>>

Heal ==
    /\ iso # None
    /\ iso' = None
    /\ UNCHANGED <<up, ldr, srcV, attV, coV, route, cliV, lateV, ghostV, bud>>

(***************************************************************************)
(* Direct writes. A cached route never authorizes a write: the apply       *)
(* checks the barrier and the freeze in log order (direct.rs:699-723).     *)
(***************************************************************************)
Held == pause # 0 \/ frozen # 0
CutTaken == phase \in {"cut", "bound", "frozen", "staged", "published"}

\* The source leader takes a commit timestamp and proposes.
Propose(c) ==
    /\ gw[c] = "src" /\ w[c] = "idle" /\ Can("src") /\ ~Held /\ ~pub
    /\ w' = [w EXCEPT ![c] = IF CutTaken THEN "above" ELSE "below"]
    /\ \/ UNCHANGED <<lateReq, bud>>
       \/ /\ bud.dup < MaxDup /\ bud' = [bud EXCEPT !.dup = @ + 1]
          /\ lateReq' = lateReq \cup {c}
    /\ UNCHANGED <<hostV, ldr, srcV, attV, coV, route, gw, latePause, ghostV>>

\* A late copy of a request reaches the source leader.
ProposeLate(c) ==
    /\ c \in lateReq /\ w[c] = "idle" /\ Can("src")
    /\ lateReq' = lateReq \ {c}
    /\ w' = IF Held \/ pub THEN w
            ELSE [w EXCEPT ![c] = IF CutTaken THEN "above" ELSE "below"]
    /\ UNCHANGED <<hostV, ldr, srcV, attV, coV, route, gw, latePause, ghostV, bud>>

\* The write commits and applies, or applies as a rejection.
ApplyWrite(c) ==
    LET refusedHere == /\ \/ pause # 0 /\ Mutant # "apply_ignores_pause"
                          \/ frozen # 0 /\ Mutant # "apply_ignores_freeze"
    IN
    /\ w[c] # "idle" /\ Can("src")
    /\ w' = [w EXCEPT ![c] = "idle"]
    /\ IF refusedHere THEN UNCHANGED <<hi, above, badOwner, missing>>
       ELSE /\ hi' = (hi \/ w[c] = "above")
            /\ above' = IF w[c] = "above" THEN above \cup {c} ELSE above
            /\ badOwner' = (badOwner \/ route = "kids")
            \* Applied after the children took their image.
            /\ missing' = IF phase \in {"staged", "published"} \/ route = "kids"
                          THEN missing \cup {c} ELSE missing
    /\ UNCHANGED <<hostV, ldr, pause, frozen, pub, receipts, attV, coV, route, gw, lateV, bud>>

\* StaleRoute: the published source answers it, and the gateway refreshes.
Refresh(c) ==
    /\ gw[c] = "src" /\ pub /\ Can("src") /\ Mutant # "no_refresh"
    /\ gw' = [gw EXCEPT ![c] = route]
    /\ UNCHANGED <<hostV, ldr, srcV, attV, coV, route, w, lateV, ghostV, bud>>

(***************************************************************************)
(* The coordinator (worker_runtime/split.rs, automatic_split.rs). Phase,   *)
(* decision and attempt number are durable; a crash loses pausedOk.        *)
(***************************************************************************)
Open ==
    /\ coord /\ phase = "none" /\ route = "src" /\ att < MaxAttempts
    /\ att' = att + 1 /\ phase' = "open" /\ dec' = None /\ refused' = FALSE
    /\ pausedOk' = FALSE
    /\ UNCHANGED <<hostV, ldr, srcV, coord, route, cliV, lateV, ghostV, bud>>

\* pause() in direct.rs:919-963: it does not read the split's receipt.
PauseApplies(a) ==
    /\ frozen = 0 /\ pause = 0
    /\ "pause_checks_receipt" \in Fixes => a \notin receipts

TakeBarrier ==
    /\ Barrier = "written" /\ coord /\ phase = "open" /\ dec = None /\ ~pausedOk /\ Can("src")
    /\ pause = att \/ PauseApplies(att)
    /\ pause' = att
    /\ \/ pausedOk' = TRUE /\ UNCHANGED <<latePause, bud>>
       \/ /\ bud.dup < MaxDup /\ bud' = [bud EXCEPT !.dup = @ + 1]
          /\ latePause' = latePause \cup {att} /\ pausedOk' = TRUE
    /\ UNCHANGED <<hostV, ldr, frozen, pub, receipts, hi, above, attV, coord, route, cliV,
                   lateReq, ghostV>>

\* Another split's barrier or freeze answers Conflict (direct.rs:950), which
\* ends the attempt like a refused freeze.
BarrierConflict ==
    /\ Barrier = "written" /\ coord /\ phase = "open" /\ dec = None /\ ~pausedOk /\ Can("src")
    /\ ~refused /\ pause # att /\ ~PauseApplies(att)
    /\ refused' = TRUE
    /\ UNCHANGED <<hostV, ldr, srcV, att, phase, dec, coV, route, cliV, lateV, ghostV, bud>>

\* A duplicated or delayed DirectPause of attempt a reaches the source.
LatePause(a) ==
    /\ a \in latePause /\ Can("src")
    /\ latePause' = latePause \ {a}
    /\ pause' = IF PauseApplies(a) THEN a ELSE pause
    /\ UNCHANGED <<hostV, ldr, frozen, pub, receipts, hi, above, attV, coV, route, cliV,
                   lateReq, ghostV, bud>>

\* Every timestamp issued so far is at or below the new cut.
Cut ==
    /\ coord /\ phase = "open" /\ dec = None
    /\ Barrier = "written" => pausedOk \/ Mutant = "cut_before_barrier"
    /\ phase' = "cut"
    /\ hi' = FALSE /\ above' = {}
    /\ w' = [c \in Client |-> IF w[c] = "idle" THEN "idle" ELSE "below"]
    /\ UNCHANGED <<hostV, ldr, pause, frozen, pub, receipts, att, dec, refused, coV, route, gw,
                   lateV, ghostV, bud>>

Bind ==
    /\ coord /\ phase = "cut" /\ dec = None
    /\ phase' = "bound"
    /\ UNCHANGED <<hostV, ldr, srcV, att, dec, refused, coV, route, cliV, lateV, ghostV, bud>>

\* freeze_source. A refusal other than an intent is a Conflict, which is
\* outside the retry set, so the attempt is cancelled (endpoint.rs Freeze
\* arm, automatic_split.rs:447). A resumed attempt keeps its cut
\* (split.rs: attempt.image), so it could not succeed later anyway.
Freeze ==
    LET refuse == \/ att \in receipts
                  \/ pause \notin {0, att}
                  \/ hi /\ Mutant # "freeze_ignores_cut"
    IN
    /\ coord /\ phase = "bound" /\ dec = None /\ ~refused /\ Can("src")
    /\ IF refuse THEN refused' = TRUE /\ UNCHANGED <<frozen, pause, phase>>
       ELSE frozen' = att /\ pause' = 0 /\ phase' = "frozen" /\ UNCHANGED refused
    /\ UNCHANGED <<hostV, ldr, pub, receipts, hi, above, att, dec, coV, route, cliV, lateV,
                   ghostV, bud>>

\* Both children import the image at the cut.
Stage ==
    /\ coord /\ dec = None /\ Can("left") /\ Can("right")
    /\ phase = "frozen"
    /\ phase' = "staged"
    /\ missing' = missing \cup above
    /\ UNCHANGED <<hostV, ldr, srcV, att, dec, refused, coV, route, cliV, lateV, badOwner, bud>>

\* The Catalog transaction: the commit decision and the routing page.
Publish ==
    /\ coord /\ dec = None
    /\ phase = "staged" \/ (Mutant = "publish_before_stage" /\ phase = "frozen")
    /\ phase' = "published" /\ dec' = "commit" /\ route' = "kids"
    /\ missing' = IF phase = "staged" THEN missing ELSE Client
    /\ UNCHANGED <<hostV, ldr, srcV, att, refused, coV, cliV, lateV, badOwner, bud>>

\* The abort decision, after a refusal or after repeated transient failures.
DecideAbort ==
    /\ coord /\ phase \notin {"none", "published"} /\ dec = None
    /\ \/ refused /\ UNCHANGED bud
       \/ bud.give < MaxGiveUp /\ bud' = [bud EXCEPT !.give = @ + 1]
    /\ dec' = "abort"
    /\ missing' = {}
    /\ UNCHANGED <<hostV, ldr, srcV, att, phase, refused, coV, route, cliV, lateV, badOwner>>

\* SplitFinishSource (lineage.rs finish_source). settle_split_proof sends it
\* only when the children were bound (split.rs:446), so an abort before Bind
\* never reaches the source unless the fix releases the barrier there.
Reaches == phase \notin {"open", "cut"} \/ "release_unbound" \in Fixes
Finish ==
    /\ coord /\ dec # None /\ att \notin receipts /\ Reaches /\ Can("src")
    /\ receipts' = receipts \cup {att}
    /\ IF dec = "commit" THEN pub' = TRUE /\ UNCHANGED <<frozen, pause>>
       ELSE /\ frozen' = IF frozen = att THEN 0 ELSE frozen
            /\ pause' = IF pause = att THEN 0 ELSE pause
            /\ UNCHANGED pub
    /\ UNCHANGED <<hostV, ldr, hi, above, attV, coV, route, cliV, lateV, ghostV, bud>>

\* Close in Timestamps; the next attempt can open.
Close ==
    /\ coord /\ dec # None /\ (att \in receipts \/ ~Reaches)
    /\ phase # "none"
    /\ phase' = "none" /\ pausedOk' = FALSE
    /\ UNCHANGED <<hostV, ldr, srcV, att, dec, refused, coord, route, cliV, lateV, ghostV, bud>>

CoordCrash ==
    /\ coord /\ bud.coord < MaxCoord
    /\ coord' = FALSE /\ pausedOk' = FALSE
    /\ bud' = [bud EXCEPT !.coord = @ + 1]
    /\ UNCHANGED <<hostV, ldr, srcV, attV, route, cliV, lateV, ghostV>>

\* Takeover by a live owner, which resumes the durable attempt. Run-owner
\* fencing of the old attempt is modelled in OltpRanges, not here.
CoordResume ==
    /\ ~coord
    /\ coord' = TRUE
    /\ UNCHANGED <<hostV, ldr, srcV, attV, pausedOk, route, cliV, lateV, ghostV, bud>>

(***************************************************************************)
(* Next state and fairness                                                 *)
(***************************************************************************)
Elect == \E r \in Range, n \in Node : NewLeader(r, n)
CrashAny == \E n \in Node : Crash(n)
RestartAny == \E n \in Node : Restart(n)
PartitionAny == \E n \in Node : Partition(n)
ProposeAny == \E c \in Client : Propose(c)
ProposeLateAny == \E c \in Client : ProposeLate(c)
ApplyAny == \E c \in Client : ApplyWrite(c)
RefreshAny == \E c \in Client : Refresh(c)
LatePauseAny == \E a \in Attempts : LatePause(a)

\* The split is published and settled and no write is left at the source.
Terminal ==
    /\ route = "kids" /\ pub /\ phase = "none" /\ \A c \in Client : w[c] = "idle" /\ gw[c] = "kids"
    /\ UNCHANGED vars

CoordStep == Open \/ TakeBarrier \/ BarrierConflict \/ Cut \/ Bind \/ Freeze \/ Stage \/ Publish \/ Finish \/ Close

Next ==
    \/ Elect \/ CrashAny \/ RestartAny \/ PartitionAny \/ Heal
    \/ ProposeAny \/ ProposeLateAny \/ ApplyAny \/ RefreshAny
    \/ Open \/ TakeBarrier \/ BarrierConflict \/ LatePauseAny \/ Cut \/ Bind \/ Freeze \/ Stage \/ Publish
    \/ DecideAbort \/ Finish \/ Close \/ CoordCrash \/ CoordResume
    \/ Terminal

Spec == Init /\ [][Next]_vars

\* An election only when the range has no working leader (election timer).
NeedLeader(r) == ~Can(r) /\ \E n \in Node : NewLeader(r, n)

\* Mechanisms: the coordinator's work loop (automatic_split.rs), its takeover
\* by a live owner, the election timer, the worker supervisor's restart and
\* the end of the partition after the faults stop, Raft replication for a
\* proposed write, the gateway refresh. Client writes get no fairness: they
\* are the load. A refused attempt is aborted by the work loop.
Fairness ==
    /\ WF_vars(CoordStep) /\ WF_vars(CoordResume)
    /\ WF_vars(refused /\ DecideAbort)
    /\ \A r \in Range : WF_vars(NeedLeader(r))
    /\ WF_vars(RestartAny) /\ WF_vars(Heal)
    /\ \A c \in Client : WF_vars(ApplyWrite(c)) /\ WF_vars(Refresh(c))
LiveSpec == Spec /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
    /\ up \in [Node -> BOOLEAN] /\ iso \in Node \cup {None}
    /\ ldr \in [Range -> Node \cup {None}]
    /\ pause \in 0..MaxAttempts /\ frozen \in 0..MaxAttempts /\ att \in 0..MaxAttempts
    /\ pub \in BOOLEAN /\ hi \in BOOLEAN /\ above \subseteq Client
    /\ receipts \subseteq Attempts /\ latePause \subseteq Attempts /\ lateReq \subseteq Client
    /\ phase \in {"none", "open", "cut", "bound", "frozen", "staged", "published"}
    /\ dec \in {None, "commit", "abort"} /\ refused \in BOOLEAN
    /\ coord \in BOOLEAN /\ pausedOk \in BOOLEAN /\ route \in {"src", "kids"}
    /\ gw \in [Client -> {"src", "kids"}] /\ w \in [Client -> {"idle", "below", "above"}]
    /\ missing \subseteq Client /\ badOwner \in BOOLEAN

\* OLTP-G01, G06: no committed write is missing from the published owner.
G06_NoWriteLostAcrossSplit == route = "kids" => missing = {}
\* OLTP-G06: no write applies at a range that no longer owns the key.
G06_NoWriteAtOldOwner == ~badOwner
\* Plan, addendum: the barrier comes first, so every direct commit that
\* applied is below the cut and the freeze is never refused for it. Checked
\* on the written design; the shipped design has no barrier.
G06_BarrierPreventsRefusal == Barrier = "written" => ~refused
\* A range with attempts left to make is not abandoned: the automatic splitter
\* never runs out of slots without publishing.
G06_SplitNotExhausted == ~(att = MaxAttempts /\ phase = "none" /\ route = "src")

\* OLTP-G06 liveness: the split eventually publishes under sustained writes.
G06_SplitPublishes == <>(route = "kids")
\* Every pause ends.
G06_PauseEnds == (pause # 0) ~> (pause = 0)
\* Routes converge once the split published.
G06_RoutesConverge == \A c \in Client : (route = "kids") ~> (gw[c] = "kids")
=============================================================================
