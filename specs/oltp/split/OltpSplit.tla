------------------------------ MODULE OltpSplit ------------------------------
(***************************************************************************)
(* Split of one range into two under sustained direct writes, across the   *)
(* source's Data log, the Timestamps split record, the Catalog route and    *)
(* the coordinator: the write barrier, the cut, the source freeze, the     *)
(* child image, publication, abort, retry, coordinator loss and a          *)
(* run-owner fence racing the split.                                       *)
(*                                                                         *)
(* Plans: plans/oltp-direct-path.md section 4 (apply checks), addendum     *)
(* "Splits under write load" (a replicated barrier in the source before    *)
(* the cut); plans/oltp-engine.md "Scale-up" ("Fence the old epoch and     *)
(* publish routing only when the new ownership is recoverable"), "Queue,   *)
(* scheduler, and failure semantics" (every decision checks its group's    *)
(* fence). Register OLTP-G06, OLTP-G01, OLTP-G07. Independent read R-01,   *)
(* R-04, R-05, R-24; scenarios O16, D07.                                   *)
(*                                                                         *)
(* SHIPPED sequence (server/src/oltp_runtime/worker_runtime/split.rs:83-366,*)
(* automatic_split.rs, authority/timestamp/split.rs, store/lineage.rs,     *)
(* manager/data_factory/split/endpoint.rs, planner/split.rs): reserve the  *)
(* split id, Open the attempt, take the Catalog Exclusive guard, Cut (the  *)
(* image pin), Bind the children, SplitFreeze in the source, host, stage   *)
(* and import both children, one Catalog transaction swaps the route,      *)
(* SplitFinishSource, release the guard, Close. Resume reuses the same     *)
(* attempt and cut. A refused freeze is a Conflict: an automatic split     *)
(* aborts and the next of AUTOMATIC_ATTEMPTS = 3 slots takes a new cut; a  *)
(* manual split is retried with the same id and the same cut.             *)
(*                                                                         *)
(* Apply checks run in log order (direct.rs:681-777): a write that races a *)
(* freeze applies as a rejection, so ownership is pinned to apply time.    *)
(* Timestamps are one bit, "taken after the cut of the current attempt",   *)
(* which is all freeze_source and the image compare. Raft safety of each   *)
(* range is assumed here and checked in specs/oltp/ranges.                 *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Node,        \* workers hosting all three ranges
    Client,      \* writers, each with its own key; they write forever
    Left,        \* the clients whose keys go to the left child
    Mutant,
    MaxAttempts, \* AUTOMATIC_ATTEMPTS = 3 per (source, schema, routing epoch)
    MaxCrash,    \* worker crashes; a crashed worker restarts with its disk
    MaxPart,     \* partitions of one worker
    MaxCoord,    \* coordinator crashes
    MaxGiveUp,   \* aborts after transient step failures (automatic_split.rs:447)
    MaxLead,     \* leadership changes while the old leader still works
    MaxDup,      \* late copies of a request or of a DirectPause
    MaxFence     \* run-owner fences during the split

Range == {"src", "left", "right"}
Attempts == 1..MaxAttempts
None == "none"

(***************************************************************************)
(* Design choices under test.                                              *)
(***************************************************************************)
\* The addendum's barrier. SHIPPED: no non-test code proposes DirectPause
\* (only store.rs:652 applies it); the prefix mutant restores that.
Barrier == Mutant # "prefix_no_pause"
\* The barrier is released by an abort that never bound its children.
\* SHIPPED settle_split_proof sends SplitFinishSource only after Bind
\* (split.rs:446); the fix sends it for every decided attempt.
ReleaseUnbound == Mutant # "pause_leak_unbound_abort"
\* A late copy of a settled attempt's DirectPause still applies: every
\* release of the store applies the same rule (direct.rs:919-963), so the
\* replicas of a group in a rolling upgrade agree. The owner's sweep then
\* releases a barrier whose attempt the source already settled
\* (automatic_split.rs, split `Release`). SHIPPED: nothing releases it.
SweepReleases == Mutant # "pause_leak_late"
\* Every decided or stuck attempt is resumed by a sweep. SHIPPED: only the
\* owner loop resumes automatic splits (automatic_split.rs:350-366); a
\* manual split is resumed only by an operator retry of the same id.
ResumeSweep == Mutant # "prefix_no_resume_sweep"

VARIABLES
    up, iso,     \* workers
    ldr,         \* [Range -> Node \cup {None}]
    auth,        \* source Data log: run-owner authority (owner term)
    pause,       \* source Data log: attempt holding the barrier, 0 if none
    frozen,      \* source Data log: attempt that froze the source, 0 if none
    pub,         \* source Data log: the source recorded the publication
    receipts,    \* source Data log: attempts whose settlement it recorded
    hi,          \* source Data log: a direct commit above the current cut applied
    above,       \* source Data log: clients whose applied write is above the cut
    att, kind,   \* Timestamps: current attempt and its kind, "auto" or "manual"
    phase,       \* Timestamps: the attempt's durable progress
    attAuth,     \* the authority the attempt pinned when it opened
    dec,         \* Status: the attempt's durable decision
    refused,     \* the freeze or the publication of this attempt was refused
    coord,       \* the coordinator process is up
    pausedOk,    \* the coordinator has the barrier's reply (volatile)
    route,       \* Catalog: "src" or "kids"
    gw,          \* [Client -> "src" / "kids"], the gateway's cached route
    w,           \* [Client -> "idle" / "below" / "above"], write in flight
    lateReq,     \* clients with a late copy of a request to the source
    latePause,   \* attempts with a late DirectPause in flight
    missing,     \* ghost: committed writes the child image lacks
    badOwner,    \* ghost: a write applied at the source after publication
    badPub,      \* ghost: a publication under a fenced authority
    loadRefused, \* ghost: a freeze was refused for a direct commit above the cut
    bud

hostV == <<up, iso>>
srcV == <<auth, pause, frozen, pub, receipts, hi, above>>
attV == <<att, kind, phase, attAuth, dec, refused>>
coV == <<coord, pausedOk>>
cliV == <<gw, w>>
lateV == <<lateReq, latePause>>
ghostV == <<missing, badOwner, badPub, loadRefused>>
vars == <<hostV, ldr, srcV, attV, coV, route, cliV, lateV, ghostV, bud>>

Conn(n) == up[n] /\ iso # n
QuorumUp == 2 * Cardinality({n \in Node : Conn(n)}) > Cardinality(Node)
Can(r) == ldr[r] # None /\ Conn(ldr[r]) /\ QuorumUp

Init ==
    /\ up = [n \in Node |-> TRUE] /\ iso = None
    /\ ldr = [r \in Range |-> None]
    /\ auth = 1 /\ pause = 0 /\ frozen = 0 /\ pub = FALSE /\ receipts = {}
    /\ hi = FALSE /\ above = {}
    /\ att = 0 /\ kind = "auto" /\ phase = "none" /\ attAuth = 0 /\ dec = None
    /\ refused = FALSE
    /\ coord = TRUE /\ pausedOk = FALSE
    /\ route = "src"
    /\ gw = [c \in Client |-> "src"] /\ w = [c \in Client |-> "idle"]
    /\ lateReq = {} /\ latePause = {}
    /\ missing = {} /\ badOwner = FALSE /\ badPub = FALSE /\ loadRefused = FALSE
    /\ bud = [crash |-> 0, part |-> 0, coord |-> 0, give |-> 0, lead |-> 0, dup |-> 0,
              fence |-> 0]

(***************************************************************************)
(* Workers and leadership.                                                 *)
(***************************************************************************)
\* A new leader of r. The barrier and the freeze are log entries, so they
\* survive. In-flight proposals at the old source leader may be lost.
NewLeader(r, n) ==
    /\ Conn(n) /\ ldr[r] # n /\ QuorumUp
    /\ r \in {"left", "right"} => phase \in {"frozen", "staged", "published"} \/ route = "kids"
    /\ ldr' = [ldr EXCEPT ![r] = n]
    \* Replacing a working leader is budgeted; replacing a lost one is free.
    /\ IF Can(r) THEN bud.lead < MaxLead /\ bud' = [bud EXCEPT !.lead = @ + 1]
                 ELSE UNCHANGED bud
    /\ IF r # "src" THEN UNCHANGED w
       ELSE \E keep \in SUBSET Client :
                w' = [c \in Client |-> IF c \in keep THEN w[c] ELSE "idle"]
    /\ UNCHANGED <<hostV, srcV, attV, coV, route, gw, lateV, ghostV>>

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
(* Direct writes. A cached route never authorizes a write: the apply checks *)
(* the barrier and the freeze in log order (direct.rs:703).                *)
(***************************************************************************)
Held == pause # 0 \/ frozen # 0
CutTaken == phase \in {"cut", "bound", "frozen", "staged", "published"}

\* The source leader latches the key, takes a commit timestamp and proposes
\* (leader execution steps 2 to 5). The leader's own split check answers
\* Fallback while the source is held.
ProposeDirect(c) ==
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

\* The entry commits and applies, or applies as a rejection
\* (SplitInProgress, DirectPaused).
ApplyDirectCommit(c) ==
    LET rejected == /\ Mutant # "apply_ignores_split"
                    /\ \/ pause # 0 /\ Mutant # "apply_ignores_pause"
                       \/ frozen # 0 /\ Mutant # "apply_ignores_freeze"
                       \/ pub
    IN
    /\ w[c] # "idle" /\ Can("src")
    /\ w' = [w EXCEPT ![c] = "idle"]
    /\ IF rejected THEN UNCHANGED <<hi, above, badOwner, missing>>
       ELSE /\ hi' = (hi \/ w[c] = "above")
            /\ above' = IF w[c] = "above" THEN above \cup {c} ELSE above
            /\ badOwner' = (badOwner \/ route = "kids")
            \* Applied after the children took their image (of an attempt that
            \* was not aborted: an aborted image is never published).
            /\ missing' = IF (phase \in {"staged", "published"} /\ dec # "abort") \/ route = "kids"
                          THEN missing \cup {c} ELSE missing
    /\ UNCHANGED <<hostV, ldr, auth, pause, frozen, pub, receipts, attV, coV, route, gw,
                   lateV, badPub, loadRefused, bud>>

\* StaleRoute: the published source answers it, and the gateway refreshes.
RefreshRoute(c) ==
    /\ gw[c] = "src" /\ pub /\ Can("src") /\ Mutant # "no_refresh"
    /\ gw' = [gw EXCEPT ![c] = route]
    /\ UNCHANGED <<hostV, ldr, srcV, attV, coV, route, w, lateV, ghostV, bud>>

(***************************************************************************)
(* The coordinator. Attempt, kind, phase and decision are durable; a crash *)
(* loses pausedOk.                                                         *)
(***************************************************************************)
\* Reserve the id, Open the attempt in Timestamps and take the Catalog
\* Exclusive guard. An automatic split opens the next slot; a manual split
\* is one operator request.
OpenSplit(k) ==
    /\ coord /\ phase = "none" /\ route = "src" /\ att < MaxAttempts
    /\ k = "manual" => att = 0
    /\ att' = att + 1 /\ kind' = k /\ phase' = "open" /\ attAuth' = auth
    /\ dec' = None /\ refused' = FALSE /\ pausedOk' = FALSE
    /\ UNCHANGED <<hostV, ldr, srcV, coord, route, cliV, lateV, ghostV, bud>>

\* DirectPause (direct.rs:919-963).
PauseApplies(a) ==
    /\ frozen = 0 /\ pause = 0
\* The coordinator's own pause: when the source answers Conflict because a
\* settled attempt's late pause holds it, the coordinator releases that one
\* first (split.rs `release_settled_barrier`) and pauses again.
CoordPauseApplies(a) ==
    \/ PauseApplies(a)
    \/ SweepReleases /\ frozen = 0 /\ pause \in receipts

\* The barrier applies, or another attempt's barrier or freeze answers
\* Conflict (direct.rs:950), which ends the attempt like a refused freeze.
\* The fixed design never reaches the Conflict branch.
ProposeDirectPause ==
    /\ Barrier /\ coord /\ phase = "open" /\ dec = None /\ ~pausedOk /\ Can("src")
    /\ ~refused
    /\ IF pause = att \/ CoordPauseApplies(att)
       THEN /\ pause' = att
            /\ \/ pausedOk' = TRUE /\ UNCHANGED <<latePause, bud>>
               \/ /\ bud.dup < MaxDup /\ bud' = [bud EXCEPT !.dup = @ + 1]
                  /\ latePause' = latePause \cup {att} /\ pausedOk' = TRUE
            /\ UNCHANGED refused
       ELSE /\ refused' = TRUE
            /\ UNCHANGED <<pause, pausedOk, latePause, bud>>
    /\ UNCHANGED <<hostV, ldr, auth, frozen, pub, receipts, hi, above, att, kind, phase,
                   attAuth, dec, coord, route, cliV, lateReq, ghostV>>

\* A duplicated or delayed DirectPause of attempt a reaches the source.
LatePause(a) ==
    /\ a \in latePause /\ Can("src")
    /\ latePause' = latePause \ {a}
    /\ pause' = IF PauseApplies(a) THEN a ELSE pause
    /\ UNCHANGED <<hostV, ldr, auth, frozen, pub, receipts, hi, above, attV, coV, route,
                   cliV, lateReq, ghostV, bud>>

\* The owner's sweep finds the source held by a settled attempt and sends
\* DirectPause { paused: false } for that split id.
SweepRelease ==
    /\ SweepReleases /\ pause # 0 /\ pause \in receipts /\ Can("src")
    /\ pause' = 0
    /\ UNCHANGED <<hostV, ldr, auth, frozen, pub, receipts, hi, above, attV, coV, route,
                   cliV, lateV, ghostV, bud>>

\* Cut: the image pin is high_watermark + 1 (timestamp/split.rs:431-443), so
\* every timestamp issued so far is at or below it. A resumed attempt keeps
\* its cut.
TakeCut ==
    /\ coord /\ phase = "open" /\ dec = None
    /\ Barrier => pausedOk \/ Mutant = "cut_before_barrier"
    /\ phase' = "cut"
    /\ hi' = FALSE /\ above' = {}
    /\ w' = [c \in Client |-> IF w[c] = "idle" THEN "idle" ELSE "below"]
    /\ UNCHANGED <<hostV, ldr, auth, pause, frozen, pub, receipts, att, kind, attAuth, dec,
                   refused, coV, route, gw, lateV, ghostV, bud>>

BindChildren ==
    /\ coord /\ phase = "cut" /\ dec = None
    /\ phase' = "bound"
    /\ UNCHANGED <<hostV, ldr, srcV, att, kind, attAuth, dec, refused, coV, route, cliV,
                   lateV, ghostV, bud>>

\* SplitFreeze (lineage.rs:442-530): refused when the attempt was settled,
\* another attempt holds the barrier, a direct commit above the cut applied
\* (DirectCommitAboveCut, :514), or the authority changed (store.rs:724).
\* A refusal is a Conflict, outside the retry set.
ProposeSplitFreeze ==
    LET refuse == \/ att \in receipts
                  \/ pause \notin {0, att}
                  \/ hi /\ Mutant # "freeze_ignores_cut"
                  \/ attAuth # auth
    IN
    /\ coord /\ phase = "bound" /\ dec = None /\ ~refused /\ Can("src")
    /\ IF refuse THEN refused' = TRUE /\ UNCHANGED <<frozen, pause, phase>>
       ELSE frozen' = att /\ pause' = 0 /\ phase' = "frozen" /\ UNCHANGED refused
    /\ loadRefused' = (loadRefused \/ (refuse /\ hi))
    /\ UNCHANGED <<hostV, ldr, auth, pub, receipts, hi, above, att, kind, attAuth, dec, coV,
                   route, cliV, lateV, missing, badOwner, badPub, bud>>

\* Host, fence, stage and import both children at the cut (endpoint.rs
\* 756-985). The copy is paged; the source stays frozen throughout.
StageChildren ==
    /\ coord /\ dec = None /\ Can("left") /\ Can("right")
    /\ phase = "frozen"
    /\ phase' = "staged"
    /\ missing' = missing \cup above
    /\ UNCHANGED <<hostV, ldr, srcV, att, kind, attAuth, dec, refused, coV, route, cliV,
                   lateV, badOwner, badPub, loadRefused, bud>>

\* The Catalog transaction: the commit decision and the route swap, with
\* the pinned authority (planner/split.rs:176, 258-373).
PublishSplit ==
    /\ coord /\ dec = None /\ ~refused
    /\ phase = "staged" \/ (Mutant = "publish_before_stage" /\ phase = "frozen")
    /\ IF attAuth # auth /\ Mutant # "publish_ignores_authority"
       THEN refused' = TRUE /\ UNCHANGED <<phase, dec, route, missing, badPub>>
       ELSE /\ phase' = "published" /\ dec' = "commit" /\ route' = "kids"
            /\ missing' = IF phase = "staged" THEN missing ELSE Client
            /\ badPub' = (badPub \/ attAuth # auth)
            /\ UNCHANGED refused
    /\ UNCHANGED <<hostV, ldr, srcV, att, kind, attAuth, coV, cliV, lateV, badOwner, loadRefused,
                   bud>>

\* The abort decision, after a refusal or after repeated transient failures
\* of an automatic split.
DecideAbort ==
    /\ coord /\ phase \notin {"none", "published"} /\ dec = None
    /\ \/ refused /\ UNCHANGED bud
       \/ kind = "auto" /\ bud.give < MaxGiveUp /\ bud' = [bud EXCEPT !.give = @ + 1]
    /\ dec' = "abort"
    /\ missing' = {}
    /\ UNCHANGED <<hostV, ldr, srcV, att, kind, phase, attAuth, refused, coV, route, cliV,
                   lateV, badOwner, badPub, loadRefused>>

\* SplitFinishSource (lineage.rs finish_source). Without the fix it is sent
\* only when the children were bound.
Reaches == phase \notin {"open", "cut"} \/ ReleaseUnbound
FinishSource ==
    /\ coord /\ dec # None /\ att \notin receipts /\ Reaches /\ Can("src")
    /\ receipts' = receipts \cup {att}
    /\ IF dec = "commit" THEN pub' = TRUE /\ UNCHANGED <<frozen, pause>>
       ELSE /\ frozen' = IF frozen = att THEN 0 ELSE frozen
            /\ pause' = IF pause = att THEN 0 ELSE pause
            /\ UNCHANGED pub
    /\ UNCHANGED <<hostV, ldr, auth, hi, above, attV, coV, route, cliV, lateV, ghostV, bud>>

\* Release the guard and Close in Timestamps; the next attempt can open.
CloseSplit ==
    /\ coord /\ dec # None /\ (att \in receipts \/ ~Reaches)
    /\ phase # "none"
    /\ phase' = "none" /\ pausedOk' = FALSE
    /\ UNCHANGED <<hostV, ldr, srcV, att, kind, attAuth, dec, refused, coord, route, cliV,
                   lateV, ghostV, bud>>

CoordCrash ==
    /\ coord /\ bud.coord < MaxCoord
    /\ coord' = FALSE /\ pausedOk' = FALSE
    /\ bud' = [bud EXCEPT !.coord = @ + 1]
    /\ UNCHANGED <<hostV, ldr, srcV, attV, route, cliV, lateV, ghostV>>

\* Takeover by a live owner, which resumes the durable attempt: the owner
\* loop for an automatic split, and in the fixed design a sweep for every
\* attempt.
ResumeSplit ==
    /\ ~coord /\ (kind = "auto" \/ ResumeSweep)
    /\ coord' = TRUE
    /\ UNCHANGED <<hostV, ldr, srcV, attV, pausedOk, route, cliV, lateV, ghostV, bud>>

\* An operator retries a manual split with the same id.
OperatorRetry ==
    /\ ~coord /\ kind = "manual"
    /\ coord' = TRUE
    /\ UNCHANGED <<hostV, ldr, srcV, attV, pausedOk, route, cliV, lateV, ghostV, bud>>

\* A new run owner fences the source (authority_fence.rs:34-124,
\* store.rs:576-585: exactly owner_term + 1). The attempt pinned the old
\* authority, so its freeze and its publication are refused.
FenceSource ==
    /\ bud.fence < MaxFence /\ Can("src")
    /\ auth' = auth + 1
    /\ bud' = [bud EXCEPT !.fence = @ + 1]
    /\ UNCHANGED <<hostV, ldr, pause, frozen, pub, receipts, hi, above, attV, coV, route,
                   cliV, lateV, ghostV>>

(***************************************************************************)
(* Next state and fairness                                                 *)
(***************************************************************************)
ElectAny == \E r \in Range, n \in Node : NewLeader(r, n)
CrashAny == \E n \in Node : Crash(n)
RestartAny == \E n \in Node : Restart(n)
PartitionAny == \E n \in Node : Partition(n)
ProposeAny == \E c \in Client : ProposeDirect(c)
ProposeLateAny == \E c \in Client : ProposeLate(c)
ApplyAny == \E c \in Client : ApplyDirectCommit(c)
RefreshAny == \E c \in Client : RefreshRoute(c)
LatePauseAny == \E a \in Attempts : LatePause(a)
OpenAny == \E k \in {"auto", "manual"} : OpenSplit(k)

\* The split is published and settled and no write is left at the source.
\* Or the writers wait: the source is held, or a gateway holds a route the
\* published source no longer serves. The liveness properties decide
\* whether such a wait ends.
Terminal ==
    /\ \/ /\ route = "kids" /\ pub /\ phase = "none"
          /\ \A c \in Client : w[c] = "idle" /\ gw[c] = "kids"
       \/ pause # 0 \/ frozen # 0
       \/ pub /\ \E c \in Client : gw[c] = "src"
    /\ UNCHANGED vars

CoordStep ==
    \/ ProposeDirectPause \/ TakeCut \/ BindChildren \/ ProposeSplitFreeze
    \/ StageChildren \/ PublishSplit \/ FinishSource \/ CloseSplit

Next ==
    \/ ElectAny \/ CrashAny \/ RestartAny \/ PartitionAny \/ Heal
    \/ ProposeAny \/ ProposeLateAny \/ ApplyAny \/ RefreshAny
    \/ OpenAny \/ CoordStep \/ LatePauseAny \/ SweepRelease \/ DecideAbort
    \/ CoordCrash \/ ResumeSplit \/ OperatorRetry \/ FenceSource
    \/ Terminal

Spec == Init /\ [][Next]_vars

\* An election only when the range has no working leader (election timer).
NeedLeader(r) == ~Can(r) /\ \E n \in Node : NewLeader(r, n)

\* Mechanisms: the coordinator's work loop, which opens the next automatic
\* slot (automatic_split.rs) and settles a refused attempt; its takeover by
\* a live owner (ResumeSplit); the election timer; the worker supervisor
\* and the end of the partition; Raft replication of a proposed write; the
\* gateway refresh. Client writes get no fairness: they are the load. An
\* operator retry is not a mechanism.
Fairness ==
    /\ WF_vars(CoordStep) /\ WF_vars(ResumeSplit) /\ WF_vars(SweepRelease)
    /\ WF_vars(OpenSplit("auto"))
    /\ WF_vars(refused /\ DecideAbort)
    /\ \A r \in Range : WF_vars(NeedLeader(r))
    /\ WF_vars(RestartAny) /\ WF_vars(Heal)
    /\ \A c \in Client : WF_vars(ApplyDirectCommit(c)) /\ WF_vars(RefreshRoute(c))
LiveSpec == Spec /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
    /\ up \in [Node -> BOOLEAN] /\ iso \in Node \cup {None}
    /\ ldr \in [Range -> Node \cup {None}]
    /\ auth \in 1..(MaxFence + 1)
    /\ pause \in 0..MaxAttempts /\ frozen \in 0..MaxAttempts /\ att \in 0..MaxAttempts
    /\ pub \in BOOLEAN /\ hi \in BOOLEAN /\ above \subseteq Client
    /\ receipts \subseteq Attempts /\ latePause \subseteq Attempts /\ lateReq \subseteq Client
    /\ kind \in {"auto", "manual"}
    /\ phase \in {"none", "open", "cut", "bound", "frozen", "staged", "published"}
    /\ dec \in {None, "commit", "abort"} /\ refused \in BOOLEAN
    /\ coord \in BOOLEAN /\ pausedOk \in BOOLEAN /\ route \in {"src", "kids"}
    /\ gw \in [Client -> {"src", "kids"}] /\ w \in [Client -> {"idle", "below", "above"}]
    /\ missing \subseteq Client /\ badOwner \in BOOLEAN /\ badPub \in BOOLEAN
    /\ loadRefused \in BOOLEAN

\* OLTP-G01, G06: no committed write is missing from the published owner.
G06_NoWriteLostAcrossSplit == route = "kids" => missing = {}
\* OLTP-G06 (R-01): no write applies at a range that no longer owns the key.
G06_NoWriteAtOldOwner == ~badOwner
\* OLTP-G06, G07 (R-04): routing is published only under the authority the
\* attempt pinned, which a new run owner's fence retires.
G06_PublishUnderOwner == ~badPub
\* The addendum: the barrier comes first, so every direct commit that
\* applied is below the cut and the freeze is never refused for it.
G06_NoRefusalForLoad == ~loadRefused
\* A range with attempts left is not abandoned: the automatic splitter does
\* not use up its slots without publishing.
G06_SplitNotExhausted == ~(att = MaxAttempts /\ phase = "none" /\ route = "src")

\* OLTP-G06 liveness: the split eventually publishes under sustained writes.
G06_SplitPublishes == <>(route = "kids")
\* A held source is released: every pause and freeze ends.
G06_PauseEnds == (pause # 0) ~> (pause = 0)
G06_FreezeEnds == (frozen # 0) ~> (frozen = 0 \/ route = "kids")
\* Routes converge once the split published.
G06_RoutesConverge == \A c \in Client : (route = "kids") ~> (gw[c] = "kids")
=============================================================================
