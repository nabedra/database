----------------------------- MODULE OltpQueue -----------------------------
\* The replicated RAM request queue on the OLTP queued path, all-ready policy.
\*
\* Plan: plans/resource-aware-scheduling.md (admission, retention, takeover,
\* cold start, settlement) and docs/testing/scheduler-contract.md.
\* The queued path still carries OLAP, POST /v1/requests and receipts, schema
\* changes and cross-range batches (plans/oltp-direct-path.md). Where the plan
\* is silent, the spec follows the shipped code on main e6b4040b:
\* server/src/oltp_runtime/{coordinator,replica,worker_runtime,admission}.rs,
\* worker_runtime/{recovery,retirement}.rs, server/src/request_queue.rs and
\* bootstrap/registered.rs.
\*
\* Design fix in force (finding OQ-1, plan fixed, code pending): a same-run
\* mutation receipt answers from the durable Status decision whenever one
\* exists. The shipped receipt is the mutant prefix_f1_receipt_regress.
\*
\* Guarantees: OLTP-G03, OLTP-G04, OLTP-G07, OLTP-G08 (queue copies),
\* OLTP-G09 (attempt settlement), SCHED-G04.
\* Scenarios: Q03-Q06, O14-O17, R01-R05, O32 (all-ready subset), T04, T05.
\*
\* Abstractions (each is listed in the plan's design review section):
\* A1 Control (ready view, owner, run) and Status (identity reservation and
\*    decision) are linearizable, so each update is one step.
\* A2 A queue copy is one step: the owner sends its current version and the
\*    holder installs it. A lost install is the step not being taken; a lost
\*    acknowledgment is a budgeted branch. A lost reply to a client is
\*    ReplyClient not being taken, which changes only ghost variables.
\* A3 The phase is the record version, and holders receive the payload,
\*    accepted and terminal versions. The code also copies a Running version
\*    before dispatch; the spec keeps execution state in the attempt, because
\*    every start and every takeover consults the Status decision first, so
\*    a holder's Running copy changes no outcome here.
\* A4 A ready-set removal (merge, re-copy, publish) and a join publication
\*    are single Control steps, because the owner freezes admission during
\*    both. Members that are live and current install the published view in
\*    that step. The owner may lag (the O14 race), and unreachable members
\*    lag until ApplyView.
\* A5 Only the relation between a worker's installed view and the published
\*    one matters ("none", "stale", "cur"), and an attempt only records
\*    whether its owner term is still current. Absolute revisions and terms
\*    are dropped.
\* A6 All modelled requests are valid mutations. A failed terminal record
\*    can only come from a transient fault, which the design forbids.
\* Out of scope: queries and pinned read grants, the scoped RF3 protocol
\* (O30, O31), byte and count limits (SCHED-G01), scheduling fairness.

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Workers,        \* worker processes, symmetric
    Reqs,           \* request identities (UUID with one body), symmetric
    NoW,            \* model value: no worker
    Mutant,         \* "none", a mutant name (mutants/*.cfg), or a fix
    MaxCrash,       \* worker process crashes (RAM lost)
    MaxPartition,   \* a live worker cut off from Control and peers
    MaxStop,        \* full stops of every worker (operator or power)
    MaxLane,        \* a transient local lane refusal during execution
    MaxReceiptFault, \* a transient receipt lookup failure
    MaxLostAck,     \* a copy installed by its holder whose ACK is lost
    MaxFaults       \* faults of any kind per behavior (they may overlap)

-----------------------------------------------------------------------------
\* Variables

VARIABLES
    \* Control (durable, Raft): the published ready view.
    run,        \* current run id
    ready,      \* ready members of the published view
    owner,      \* fenced queue owner, or NoW
    joining,    \* worker holding a join ticket, or NoW
    sealed,     \* the owner sealed its join export: no queue mutation
    jcopied,    \* Control's pending join is past Building: the owner copies
    \* Status (durable): identity reservation and decision, cold fence.
    ident,      \* [st |-> "none" | "reserved" | "committed", run |-> n]
    fenceRun,   \* run whose attempts Status still admits
    \* Worker volatile state, lost on crash.
    up,         \* process alive
    conn,       \* reachable; FALSE while partitioned
    view,       \* installed view: "none" (no queue), "stale" or "cur"
    q,          \* q[w][r]: phase of w's record for r
    acks,       \* the owner's copy barrier for its current version of r
    held,       \* held[w][r]: compute reservations of w's attempts for r
    \* Attempts. A submitted decision outlives its worker.
    atts,
    \* Fault budget counters.
    nCrash, nPart, nStop, nLane, nRcpt, nLostAck,
    \* Ghost variables for client-visible properties.
    applied,    \* applied[r]: Data publications of r
    commitRun,  \* run of the attempt that published r
    firstRun,   \* run of r's first durable reservation
    ackRun,     \* run in the client's 202; 0 before any 202
    obs,        \* the client's knowledge of r
    everAb,     \* a client was told "abandoned"
    everNA,     \* a client was told "not_applied"
    everAcc,    \* the accepted version once reached every ready member
    regress,    \* a client saw a non-terminal answer after a terminal one
    lostHeld,   \* a cold start abandoned work a live worker still held
    spurious    \* a same-run retry of accepted work was refused (503)

control == <<run, ready, owner, joining, sealed, jcopied>>
durable == <<ident, fenceRun>>
worker  == <<up, conn, view, q, acks, held>>
faults  == <<nCrash, nPart, nStop, nLane, nRcpt, nLostAck>>
ghost   == <<applied, commitRun, firstRun, ackRun, obs, everAb, everNA,
             everAcc, regress, lostHeld, spurious>>
ghostNoAcc == <<applied, commitRun, firstRun, ackRun, obs, everAb, everNA,
                regress, lostHeld, spurious>>
vars    == <<control, durable, worker, atts, faults, ghost>>

-----------------------------------------------------------------------------
\* Types and helpers

Phases  == {"none", "payload", "queued", "done", "failed"}
Rank(p) == CASE p = "none" -> 0 [] p = "payload" -> 1 [] p = "queued" -> 2
             [] OTHER -> 3
AccPh   == {"queued", "done", "failed"}
TermObs == {"succeeded", "failed", "abandoned", "notapplied"}
ObsVals == {"none", "accepted"} \cup TermObs
AttSt   == {"exec", "put", "res"}

Has(w, r)   == q[w][r] # "none"
Live(w)     == up[w] /\ conn[w]
\* A worker whose queue installed the published view.
Current(w)  == w \in Workers /\ Live(w) /\ w \in ready /\ view[w] = "cur"
\* The owner may mutate its queue (replica.rs mutating_owner).
OwnerOK     == owner # NoW /\ Current(owner) /\ ~sealed
Copied(r)   == acks[r] = ready
Majority(S) == 2 * Cardinality(S) > Cardinality(ready)
\* The all-ready barrier, or the ack_on_quorum mutant's majority.
Complete(r) == IF Mutant = "ack_on_quorum"
               THEN owner \in acks[r] /\ Majority(acks[r])
               ELSE Copied(r)
AttOf(r, w) == {a \in atts : a.r = r /\ a.w = w}
\* The total fault budget, shared by every kind.
FaultRoom   == nCrash + nPart + nStop + nLane + nRcpt + nLostAck < MaxFaults
\* The OQ-1 fix: receipts answer from the durable decision.
ReceiptFromStatus == Mutant # "prefix_f1_receipt_regress"
AllReadyDown == \A w \in ready : ~up[w] \/ view[w] = "none"

\* Client knowledge. Terminal knowledge never changes. A later non-terminal
\* answer only sets regress (finding F1, see Receipt_Monotone).
Observe(r, ans) ==
    /\ obs' = [obs EXCEPT ![r] = IF obs[r] \in TermObs \/ ans \in {"pending", "unknown"}
                                 THEN obs[r] ELSE ans]
    /\ regress' = (regress \/ (obs[r] \in TermObs /\ ans \notin {obs[r], "unknown"}))
    /\ everAb' = [everAb EXCEPT ![r] = @ \/ ans = "abandoned"]
    /\ everNA' = [everNA EXCEPT ![r] = @ \/ ans = "notapplied"]

\* Ghost: the accepted version of r reached every ready member.
AccSeen ==
    everAcc' = [r \in Reqs |-> everAcc[r] \/ (owner' # NoW /\ ready' # {}
                                 /\ q'[owner'][r] = "queued" /\ acks'[r] = ready')]

\* merge_removal_inventory: the most advanced record among the survivors.
Merge(S, r) ==
    LET P == {q[s][r] : s \in S}
    IN CHOOSE p \in P : \A x \in P : Rank(x) <= Rank(p)

\* Views after Control publishes ready set S with owner o. Live members that
\* held the previous view install it at once; the owner may lag (lag); the
\* others lag until ApplyView. A worker outside S keeps its local view.
Publish(S, o, lag) ==
    [w \in Workers |->
        IF w \in S /\ view[w] # "none"
        THEN IF Live(w) /\ view[w] = "cur" /\ ~(w = o /\ lag) THEN "cur" ELSE "stale"
        ELSE view[w]]
\* The new owner's barrier after publication: its own copy, if installed.
FreshAcks(o, applied_) == [r \in Reqs |-> IF applied_ /\ Has(o, r) THEN {o} ELSE {}]

TypeOK ==
    /\ run \in Nat
    /\ ready \subseteq Workers
    /\ owner \in Workers \cup {NoW}
    /\ joining \in Workers \cup {NoW}
    /\ sealed \in BOOLEAN /\ jcopied \in BOOLEAN
    /\ ident \in [Reqs -> [st : {"none", "reserved", "committed"}, run : Nat]]
    /\ fenceRun \in Nat
    /\ up \in [Workers -> BOOLEAN] /\ conn \in [Workers -> BOOLEAN]
    /\ view \in [Workers -> {"none", "stale", "cur"}]
    /\ q \in [Workers -> [Reqs -> Phases]]
    /\ acks \in [Reqs -> SUBSET Workers]
    /\ held \in [Workers -> [Reqs -> Int]]
    /\ \A a \in atts : a.r \in Reqs /\ a.w \in Workers \cup {NoW}
                       /\ a.run \in Nat /\ a.cur \in BOOLEAN /\ a.st \in AttSt
    /\ applied \in [Reqs -> Nat] /\ commitRun \in [Reqs -> Nat]
    /\ firstRun \in [Reqs -> Nat] /\ ackRun \in [Reqs -> Nat]
    /\ obs \in [Reqs -> ObsVals]
    /\ everAb \in [Reqs -> BOOLEAN] /\ everNA \in [Reqs -> BOOLEAN]
    /\ everAcc \in [Reqs -> BOOLEAN]
    /\ regress \in BOOLEAN /\ lostHeld \in BOOLEAN /\ spurious \in BOOLEAN

-----------------------------------------------------------------------------
\* Initial state: a running cluster, every worker ready with an empty queue.

Init ==
    /\ run = 1
    /\ ready = Workers
    /\ owner \in Workers
    /\ joining = NoW /\ sealed = FALSE /\ jcopied = FALSE
    /\ ident = [r \in Reqs |-> [st |-> "none", run |-> 0]]
    /\ fenceRun = 1
    /\ up = [w \in Workers |-> TRUE] /\ conn = [w \in Workers |-> TRUE]
    /\ view = [w \in Workers |-> "cur"]
    /\ q = [w \in Workers |-> [r \in Reqs |-> "none"]]
    /\ acks = [r \in Reqs |-> {}]
    /\ held = [w \in Workers |-> [r \in Reqs |-> 0]]
    /\ atts = {}
    /\ nCrash = 0 /\ nPart = 0 /\ nStop = 0 /\ nLane = 0 /\ nRcpt = 0
    /\ nLostAck = 0
    /\ applied = [r \in Reqs |-> 0] /\ commitRun = [r \in Reqs |-> 0]
    /\ firstRun = [r \in Reqs |-> 0] /\ ackRun = [r \in Reqs |-> 0]
    /\ obs = [r \in Reqs |-> "none"]
    /\ everAb = [r \in Reqs |-> FALSE] /\ everNA = [r \in Reqs |-> FALSE]
    /\ everAcc = [r \in Reqs |-> FALSE]
    /\ regress = FALSE /\ lostHeld = FALSE /\ spurious = FALSE

-----------------------------------------------------------------------------
\* Admission (plan "Workflow" and "Keep the queue ... in RAM";
\* worker_runtime.rs submit_original, coordinator.rs stage, accept_staged).
\* Guarantees OLTP-G03, OLTP-G07, SCHED-G04. Scenarios Q03, Q05, R04.

\* A client sends (id, body) through any API, and the owner handles it under
\* the UUID gate after the durable reservation. The first send and every
\* retry use this action; the two APIs of Q05 meet at the same owner. A
\* retry of a held identity attaches to it, which changes no state here.
SubmitRequest(r) ==
    /\ OwnerOK
    /\ LET o == owner IN
       \/ \* Fresh identity: Status reservation, then a local unaccepted copy.
          /\ \/ ident[r].st = "none"
             \/ /\ Mutant = "no_durable_reservation"
                /\ ident[r].st = "reserved" /\ ident[r].run < run
          /\ ident' = [ident EXCEPT ![r] = [st |-> "reserved", run |-> run]]
          /\ firstRun' = [firstRun EXCEPT ![r] = IF @ = 0 THEN run ELSE @]
          /\ q' = [q EXCEPT ![o][r] = "payload"]
          /\ acks' = [acks EXCEPT ![r] = {o}]
          /\ UNCHANGED <<obs, regress, everAb, everNA>>
       \/ \* Reserved in this run but no ready member holds a copy: it was
          \* never accepted, so admission starts again under the identity.
          /\ ident[r].st = "reserved" /\ ident[r].run = run /\ ~Has(o, r)
          /\ q' = [q EXCEPT ![o][r] = "payload"]
          /\ acks' = [acks EXCEPT ![r] = {o}]
          /\ UNCHANGED <<ident, firstRun, obs, regress, everAb, everNA>>
       \/ \* Reserved in an older run: a raw resubmit stays abandoned.
          /\ Mutant # "no_durable_reservation"
          /\ ident[r].st = "reserved" /\ ident[r].run < run
          /\ obs[r] # "abandoned"
          /\ Observe(r, "abandoned")
          /\ UNCHANGED <<ident, firstRun, q, acks>>
       \/ \* Status already holds the decision, also after retirement or a
          \* cold start: reserve answers the durable terminal record
          \* (admission.rs reserve, IdentityLookup::Terminal) and nothing
          \* runs again. Scenarios Q06, T04, R-queue-20.
          /\ ident[r].st = "committed"
          /\ obs[r] # "succeeded"
          /\ Observe(r, "succeeded")
          /\ UNCHANGED <<ident, firstRun, q, acks>>
    /\ UNCHANGED <<control, fenceRun, up, conn, view, held, atts, faults,
                   applied, commitRun, ackRun, lostHeld, spurious>>
    /\ AccSeen

\* O14, PR #103: a same-body retry reaches the owner after Control published
\* a newer ready view and before the owner's local membership actor installed
\* it. The fixed code waits up to 3 s for that exact view: the retry stays
\* pending (this step changes nothing) until ApplyView. The reverted fix
\* answered 503 to a same-run retry of accepted work.
RetryDuringViewLag(r) ==
    /\ owner # NoW /\ Live(owner) /\ owner \in ready /\ view[owner] = "stale"
    /\ q[owner][r] \in AccPh
    /\ ~spurious
    /\ spurious' = (Mutant = "prefix_o14_ready_view")
    /\ UNCHANGED <<control, durable, worker, atts, faults, applied, commitRun,
                   firstRun, ackRun, obs, everAb, everNA, everAcc, regress,
                   lostHeld>>

\* One copy of the owner's current version to a ready holder
\* (coordinator.rs ensure_phase; replica.rs install_prepared,
\* acknowledge_phase). The holder rejects an older version.
InstallCopy(w, r) ==
    /\ OwnerOK
    /\ Has(owner, r)
    /\ w # owner /\ Current(w)
    /\ w \notin acks[r]
    /\ Rank(q[w][r]) <= Rank(q[owner][r])
    /\ q' = [q EXCEPT ![w][r] = q[owner][r]]
    /\ \/ /\ acks' = [acks EXCEPT ![r] = @ \cup {w}]
          /\ UNCHANGED nLostAck
       \/ \* Fully transmitted but unacknowledged: the owner sends it again.
          /\ nLostAck < MaxLostAck /\ FaultRoom
          /\ nLostAck' = nLostAck + 1
          /\ UNCHANGED acks
    /\ UNCHANGED <<control, durable, up, conn, view, held, atts, nCrash, nPart,
                   nStop, nLane, nRcpt, ghostNoAcc>>
    /\ AccSeen

\* The last payload ACK lets the owner create the accepted version
\* (accept_staged, after bind_staged records the binding in Status).
AcceptStaged(r) ==
    /\ OwnerOK
    /\ q[owner][r] = "payload"
    /\ Complete(r)
    /\ q' = [q EXCEPT ![owner][r] = "queued"]
    /\ acks' = [acks EXCEPT ![r] = {owner}]
    /\ UNCHANGED <<control, durable, up, conn, view, held, atts, faults, ghostNoAcc>>
    /\ AccSeen

\* The 202, a same-run retry's answer, the synchronous reply, or a same-run
\* receipt (receipt_owned). Acceptance follows a complete barrier on the
\* accepted version. The shipped receipt (prefix_f1_receipt_regress)
\* answers a queued record from its local state, and a terminal record
\* "succeeded" only with its terminal copy confirmed in the owner's current
\* view, else "committing" (pending). The OQ-1 fix answers a mutation from
\* its durable Status decision whenever Status has one, whatever the local
\* record says, as the submit path already does.
ReplyClient(r) ==
    /\ OwnerOK
    /\ LET p == q[owner][r]
           decided == ReceiptFromStatus /\ ident[r].st = "committed"
           ans == CASE p \in {"queued", "done"} /\ decided -> "succeeded"
                    [] p = "queued" -> "accepted"
                    [] p = "done" /\ Copied(r) -> "succeeded"
                    [] p = "done" -> "pending"
                    [] OTHER -> "failed"
       IN /\ p \in AccPh
          /\ ans = "accepted" => Complete(r)
          /\ p = "failed" => Copied(r)
          /\ ans = "pending" => (obs[r] \in TermObs /\ ~regress)
          /\ ans # "pending" => obs[r] # ans
          /\ Observe(r, ans)
          /\ ackRun' = [ackRun EXCEPT ![r] = IF @ = 0 THEN run ELSE @]
    /\ UNCHANGED <<control, durable, worker, atts, faults, applied, commitRun,
                   firstRun, everAcc, lostHeld, spurious>>

-----------------------------------------------------------------------------
\* Execution, decision and settlement (plan: "Settlement owns the original
\* CPU, memory, and queue charges through cancellation and error delivery";
\* replica.rs start, worker_runtime.rs execute and Settlement).
\* Guarantees OLTP-G03, OLTP-G04, OLTP-G09. Scenarios R01, F03, T04, O07.

\* Accepted work starts only from a complete copy and only while Status has
\* no decision for it. The attempt reserves compute on the owner.
Start(r) ==
    /\ OwnerOK
    /\ q[owner][r] = "queued"
    /\ Complete(r) \/ Mutant = "dispatch_before_copies"
    /\ ident[r].st # "committed"
    /\ AttOf(r, owner) = {}
    /\ atts' = atts \cup {[r |-> r, w |-> owner, run |-> run, cur |-> TRUE, st |-> "exec"]}
    /\ held' = [held EXCEPT ![owner][r] = @ + (IF Mutant = "gc1_double_guard" THEN 2 ELSE 1)]
    /\ UNCHANGED <<control, durable, up, conn, view, q, acks, faults, ghost>>

\* Accepted work whose decision is already durable finishes from it
\* (recovery after a takeover: reconcile before any re-dispatch).
ResolveCommitted(r) ==
    /\ Mutant # "no_reconcile"
    /\ OwnerOK
    /\ q[owner][r] = "queued"
    /\ ident[r].st = "committed"
    /\ AttOf(r, owner) = {}
    /\ q' = [q EXCEPT ![owner][r] = "done"]
    /\ acks' = [acks EXCEPT ![r] = {owner}]
    /\ UNCHANGED <<control, durable, up, conn, view, held, atts, faults, ghost>>

\* The attempt submits its decision. From here the write can land after the
\* worker stops (plan: "An already submitted catalog PUT can succeed even if
\* every worker subsequently stops").
SubmitDecision(a) ==
    /\ a \in atts /\ a.st = "exec"
    /\ a.w # NoW /\ Live(a.w)
    /\ atts' = (atts \ {a}) \cup {[a EXCEPT !.st = "put"]}
    /\ UNCHANGED <<control, durable, worker, faults, ghost>>

\* O07 / QLANE / PR #108: a full local lane refuses accepted work's next
\* call. The design holds the same charged command and pin through a
\* bounded pre-send wait, so the attempt stays as it is. The mutant turns
\* the refusal into a terminal failure of accepted work.
LaneRefused(a) ==
    /\ a \in atts /\ a.st = "exec"
    /\ a.w # NoW /\ up[a.w]
    /\ nLane < MaxLane /\ FaultRoom
    /\ nLane' = nLane + 1
    /\ IF Mutant = "o07_lane_fails"
       THEN /\ atts' = atts \ {a}
            /\ held' = [held EXCEPT ![a.w][a.r] = @ - 1]
            /\ IF a.w = owner /\ OwnerOK /\ q[a.w][a.r] = "queued"
               THEN /\ q' = [q EXCEPT ![a.w][a.r] = "failed"]
                    /\ acks' = [acks EXCEPT ![a.r] = {a.w}]
               ELSE UNCHANGED <<q, acks>>
       ELSE UNCHANGED <<atts, held, q, acks>>
    /\ UNCHANGED <<control, durable, up, conn, view, nCrash, nPart, nStop, nRcpt, nLostAck, ghost>>

\* Status applies the decision once per identity (OLTP-G03), and only for
\* the run its fence admits (T05, O15). A fenced write applies nothing.
CommitDecision(a) ==
    /\ a \in atts /\ a.st = "put"
    /\ LET r == a.r
           admitted == a.run = fenceRun \/ Mutant = "no_cold_fence"
           fresh == ident[r].st # "committed" \/ Mutant = "no_identity_dedup"
       IN /\ IF admitted /\ fresh
             THEN /\ ident' = [ident EXCEPT ![r].st = "committed"]
                  /\ applied' = [applied EXCEPT ![r] = @ + 1]
                  /\ commitRun' = [commitRun EXCEPT ![r] = a.run]
             ELSE UNCHANGED <<ident, applied, commitRun>>
          /\ atts' = IF a.w = NoW THEN atts \ {a}
                     ELSE (atts \ {a}) \cup {[a EXCEPT !.st = "res"]}
    /\ UNCHANGED <<control, fenceRun, worker, faults, firstRun, ackRun, obs,
                   everAb, everNA, everAcc, regress, lostHeld, spurious>>

\* The worker learns the outcome and releases the reservation once. The
\* current owner records the terminal version; otherwise reconciliation
\* finishes the record from Status.
Settle(a) ==
    /\ a \in atts /\ a.st = "res"
    /\ a.w # NoW /\ Live(a.w)
    /\ Mutant = "no_settle_superseded" => a.cur
    /\ atts' = atts \ {a}
    /\ held' = [held EXCEPT ![a.w][a.r] = @ - 1]
    /\ IF /\ a.w = owner /\ OwnerOK
          /\ q[a.w][a.r] = "queued"
          /\ ident[a.r].st = "committed"
       THEN /\ q' = [q EXCEPT ![a.w][a.r] = "done"]
            /\ acks' = [acks EXCEPT ![a.r] = {a.w}]
       ELSE UNCHANGED <<q, acks>>
    /\ UNCHANGED <<control, durable, up, conn, view, faults, ghost>>

\* An attempt of a superseded owner term is cancelled and settles once. A
\* submitted decision cannot be recalled, so it settles when it resolves.
CancelAttempt(a) ==
    /\ a \in atts
    /\ a.w # NoW /\ Live(a.w)
    /\ ~a.cur
    /\ \/ /\ a.st = "exec"
          /\ atts' = atts \ {a}
       \/ /\ Mutant = "cancel_releases_put"
          /\ a.st = "put"
          /\ held[a.w][a.r] = Cardinality(AttOf(a.r, a.w))
          /\ UNCHANGED atts
    /\ held' = [held EXCEPT ![a.w][a.r] = @ - 1]
    /\ UNCHANGED <<control, durable, up, conn, view, q, acks, faults, ghost>>

\* Terminal retention expired: every ready holder drops the confirmed
\* terminal record. Status records the retirement first, and the decision
\* stays as the non-executable tombstone (retirement.rs retire_candidate,
\* status.retire_terminal; replica.rs retire). The mutant
\* retire_without_tombstone drops the durable identity with the RAM copy.
Retire(r) ==
    /\ OwnerOK
    /\ q[owner][r] = "done"
    /\ Copied(r)
    /\ ident[r].st = "committed"
    /\ q' = [w \in Workers |-> IF w \in ready THEN [q[w] EXCEPT ![r] = "none"] ELSE q[w]]
    /\ acks' = [acks EXCEPT ![r] = {}]
    /\ ident' = IF Mutant = "retire_without_tombstone"
                THEN [ident EXCEPT ![r] = [st |-> "none", run |-> 0]]
                ELSE ident
    /\ UNCHANGED <<control, fenceRun, up, conn, view, held, atts, faults, ghost>>

-----------------------------------------------------------------------------
\* Old-run receipts (plan: "An old-run lookup checks durable mutation
\* decisions before reporting abandonment"). Same-run receipts are
\* ReplyClient. Guarantees OLTP-G04, SCHED-G04. Scenarios Q06, R04, O32.

OldRunReceipt(r) ==
    /\ 0 < ackRun[r] /\ ackRun[r] < run
    /\ fenceRun = run \/ Mutant = "lookup_before_fence"
    /\ obs[r] \notin TermObs
    /\ Observe(r, IF ident[r].st = "committed" THEN "succeeded" ELSE "abandoned")
    /\ UNCHANGED <<control, durable, worker, atts, faults, applied, commitRun,
                   firstRun, ackRun, everAcc, lostHeld, spurious>>

\* ACC3-receipt: a transient lookup failure while the synchronous API waits.
\* The design keeps the original ID and reports uncertainty; the mutant ends
\* the observation as not_applied.
ReceiptFault(r) ==
    /\ ackRun[r] > 0 /\ obs[r] \notin TermObs
    /\ nRcpt < MaxReceiptFault /\ FaultRoom
    /\ nRcpt' = nRcpt + 1
    /\ Observe(r, IF Mutant = "acc3_receipt_early" THEN "notapplied" ELSE "unknown")
    /\ UNCHANGED <<control, durable, worker, atts, nCrash, nPart, nStop, nLane,
                   nLostAck, applied, commitRun, firstRun, ackRun, everAcc, lostHeld, spurious>>

-----------------------------------------------------------------------------
\* Membership: view installation, removal, rejoin and join (plan: "Recover
\* from a surviving replica; re-establish copies on all ready members"; "A
\* new worker copies outstanding queue records before it becomes ready";
\* "Transfer request copies before draining/removing the last holder").
\* Guarantees OLTP-G07, OLTP-G08. Scenarios R01-R03, R05, O14, O17.

\* A lagging member's membership actor installs the published view
\* (registered.rs spawn_membership, replica.rs fenced_takeover). An owner
\* that installs it must re-copy every record before using its barrier.
ApplyView(w) ==
    /\ Live(w) /\ w \in ready /\ view[w] = "stale"
    /\ view' = [view EXCEPT ![w] = "cur"]
    /\ acks' = IF w = owner THEN FreshAcks(w, TRUE) ELSE acks
    /\ UNCHANGED <<control, durable, up, conn, q, held, atts, faults, ghostNoAcc>>
    /\ AccSeen

\* Control removes a member that stopped answering: crashed, partitioned, or
\* restarted as a new incarnation. The owner (a new one when x owned the
\* queue) merges every survivor's inventory and copies it back before the
\* view is published (coordinator.rs copy_removal). Every survivor must
\* answer. A new owner fences the old term: older attempts become superseded.
\* Shipped Control (prefix_join_never_aborted) refuses every ready-set change
\* while a join is pending (machine.rs begin_ready and BeginOwnerFence answer
\* Busy). The OQ-2 fix lets the removal supersede the pending join: the join
\* is abandoned, the owner's seal ends with the view change, and the joiner
\* starts over.
RemoveMember(x, o) ==
    /\ x \in ready
    /\ ~Live(x) \/ view[x] = "none"
    /\ Mutant = "no_remove_crashed" => up[x]
    /\ o \in ready \ {x}
    /\ owner \in ready \ {x} => o = owner
    /\ Mutant = "prefix_join_never_aborted" => joining = NoW
    /\ \A s \in ready \ {x} : Current(s)
    /\ \E lag \in BOOLEAN :
       LET S == ready \ {x} IN
       /\ q' = [w \in Workers |->
                  CASE w \in S -> [r \in Reqs |-> Merge(S, r)]
                    [] w = joining -> [r \in Reqs |-> "none"]
                    [] OTHER -> q[w]]
       /\ ready' = S
       /\ view' = [w \in Workers |->
                    IF w = joining THEN "none" ELSE Publish(S, o, lag)[w]]
       /\ acks' = [r \in Reqs |-> IF ~lag /\ q'[o][r] # "none" THEN {o} ELSE {}]
    /\ joining' = NoW /\ jcopied' = FALSE /\ sealed' = FALSE
    /\ owner' = o
    /\ atts' = IF o # owner THEN {[a EXCEPT !.cur = FALSE] : a \in atts} ELSE atts
    /\ UNCHANGED <<run, durable, up, conn, held, faults, ghostNoAcc>>
    /\ AccSeen

\* OQ-2 fix: Control abandons a pending join whose joiner stopped answering
\* (the heartbeat timeout that drives removals), and the owner drops its
\* export and seal. Shipped Control has no such step: Pending::Ready is
\* cleared only by PublishReady or a cold fence (machine.rs publish_ready,
\* PublishFence).
AbortJoin ==
    /\ Mutant # "prefix_join_never_aborted"
    /\ joining # NoW /\ ~Live(joining)
    /\ joining' = NoW /\ jcopied' = FALSE /\ sealed' = FALSE
    /\ q' = [q EXCEPT ![joining] = [r \in Reqs |-> "none"]]
    /\ view' = [view EXCEPT ![joining] = "none"]
    /\ UNCHANGED <<run, ready, owner, durable, up, conn, acks, held, atts,
                   faults, ghost>>

\* A worker outside the ready set starts a join (Control BeginJoin): a live
\* one after a partition (LIVE1), or a stopped one that restarts. Its stale
\* RAM queue is replaced by an empty joining queue (replica.rs rejoin,
\* registered.rs Rejoin::begin, restart.rs begin_queue_join). A restarted
\* joiner adopts its own pending join only while Control is still in the
\* Building phase, before the owner's copy began (jcopied FALSE).
Rejoin(w) ==
    /\ w \notin ready /\ conn[w]
    /\ joining = NoW \/ (joining = w /\ ~jcopied /\ view[w] = "none")
    /\ OwnerOK
    /\ up' = [up EXCEPT ![w] = TRUE]
    /\ joining' = w
    /\ q' = IF Mutant = "rejoin_keeps_stale" THEN q
            ELSE [q EXCEPT ![w] = [r \in Reqs |-> "none"]]
    /\ view' = [view EXCEPT ![w] = IF Mutant = "ready_before_copy" THEN "cur" ELSE "none"]
    /\ UNCHANGED <<run, ready, owner, sealed, jcopied, durable, conn, acks, held,
                   atts, faults, ghost>>

\* The owner exports its queue (snapshot and delta pages) and seals it: no
\* queue mutation until the view changes (copy_join, seal_join; the seal is
\* export.final_cut, which mutating_owner checks and only fenced_takeover
\* clears). The seal has no timer, unlike the OLAP queue's (OLAPS-1).
JoinCopy ==
    /\ Mutant # "ready_before_copy"
    /\ joining # NoW /\ Live(joining) /\ view[joining] = "none" /\ ~jcopied
    /\ OwnerOK
    /\ q' = [q EXCEPT ![joining] =
                [r \in Reqs |-> IF Mutant = "rejoin_keeps_stale" /\ ~Has(owner, r)
                                THEN q[joining][r] ELSE q[owner][r]]]
    /\ view' = [view EXCEPT ![joining] = "cur"]
    /\ sealed' = (Mutant # "join_no_seal")
    /\ jcopied' = TRUE
    /\ UNCHANGED <<run, ready, owner, joining, durable, up, conn, acks, held,
                   atts, faults, ghost>>

\* Control publishes the larger ready view once the copy is verified.
PublishJoin ==
    /\ joining # NoW /\ Live(joining) /\ view[joining] = "cur"
    /\ owner # NoW /\ Current(owner)
    /\ sealed \/ Mutant \in {"join_no_seal", "ready_before_copy"}
    /\ \E lag \in BOOLEAN :
       /\ view' = Publish(ready \cup {joining}, owner, lag)
       /\ acks' = FreshAcks(owner, ~lag)
    /\ ready' = ready \cup {joining}
    /\ joining' = NoW /\ jcopied' = FALSE
    /\ sealed' = FALSE
    /\ UNCHANGED <<run, owner, durable, up, conn, q, held, atts, faults, ghostNoAcc>>
    /\ AccSeen

-----------------------------------------------------------------------------
\* Faults, each with a budget. Liveness holds after the budgets run out.

\* A worker process dies with its RAM queue, barrier and reservations. A
\* submitted decision stays in flight without an owner.
Crash(w) ==
    /\ up[w] /\ nCrash < MaxCrash /\ FaultRoom
    /\ nCrash' = nCrash + 1
    /\ up' = [up EXCEPT ![w] = FALSE]
    /\ view' = [view EXCEPT ![w] = "none"]
    /\ q' = [q EXCEPT ![w] = [r \in Reqs |-> "none"]]
    /\ held' = [held EXCEPT ![w] = [r \in Reqs |-> 0]]
    /\ atts' = {IF a.w = w THEN [a EXCEPT !.w = NoW] ELSE a :
                  a \in {b \in atts : b.w # w \/ b.st = "put"}}
    /\ acks' = IF w = owner THEN [r \in Reqs |-> {}] ELSE acks
    \* Control's pending join is durable: a crashed joiner keeps it, and
    \* only the owner's own crash ends the seal it holds in RAM.
    /\ sealed' = IF w = owner THEN FALSE ELSE sealed
    /\ UNCHANGED <<run, ready, owner, joining, jcopied, durable, conn, nPart, nStop, nLane, nRcpt, nLostAck, ghost>>

Partition(w) ==
    /\ up[w] /\ conn[w] /\ nPart < MaxPartition /\ FaultRoom
    /\ nPart' = nPart + 1
    /\ conn' = [conn EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<control, durable, up, view, q, acks, held, atts, nCrash,
                   nStop, nLane, nRcpt, nLostAck, ghost>>

Heal(w) ==
    /\ ~conn[w]
    /\ conn' = [conn EXCEPT ![w] = TRUE]
    \* A member that missed a publication while cut off is already "stale".
    /\ UNCHANGED <<control, durable, up, view, q, acks, held, atts, faults, ghost>>

\* Every worker stops (operator stop or power loss) with work outstanding.
StopAll ==
    /\ nStop < MaxStop /\ FaultRoom /\ \E w \in Workers : up[w]
    /\ nStop' = nStop + 1
    /\ up' = [w \in Workers |-> FALSE]
    /\ view' = [w \in Workers |-> "none"]
    /\ q' = [w \in Workers |-> [r \in Reqs |-> "none"]]
    /\ held' = [w \in Workers |-> [r \in Reqs |-> 0]]
    /\ atts' = {[a EXCEPT !.w = NoW] : a \in {b \in atts : b.st = "put"}}
    /\ acks' = [r \in Reqs |-> {}]
    /\ sealed' = FALSE
    /\ UNCHANGED <<run, ready, owner, joining, jcopied, durable, conn, nCrash, nPart, nLane, nRcpt, nLostAck, ghost>>

-----------------------------------------------------------------------------
\* Cold start (plan: "A deliberate cold start after all queue holders have
\* stopped creates a fresh run_id, abandons outstanding old-run work"; "A
\* missed heartbeat or network partition alone does not prove every worker
\* stopped"). Guarantees SCHED-G04, OLTP-G07. Scenarios R04, R05, O15, O32.

\* Control opens a new run with an empty ready set. The baseline requires
\* every worker to have stopped; the mutant trusts a missed owner heartbeat.
ColdStart ==
    /\ owner # NoW
    /\ IF Mutant = "cold_on_missed_heartbeat"
       THEN ~Live(owner)
       ELSE \A w \in Workers : ~up[w]
    /\ run' = run + 1
    /\ ready' = {} /\ owner' = NoW /\ joining' = NoW /\ sealed' = FALSE
    /\ jcopied' = FALSE
    /\ atts' = {[a EXCEPT !.cur = FALSE] : a \in atts}
    /\ lostHeld' = (lostHeld \/ \E w \in Workers, r \in Reqs :
                        up[w] /\ q[w][r] \in AccPh /\ ident[r].st # "committed")
    /\ UNCHANGED <<durable, worker, faults, applied, commitRun, firstRun,
                   ackRun, obs, everAb, everNA, everAcc, regress, spurious>>

\* The new run fences Status (and the allocator and Data groups) before any
\* old-run receipt is answered or new work is admitted.
FenceGroups ==
    /\ fenceRun < run
    /\ fenceRun' = run
    /\ UNCHANGED <<control, ident, worker, atts, faults, ghost>>

\* A stopped worker starts as the first member of the new run, with an
\* empty queue (replica.rs new_empty_bootstrap, activate_empty_bootstrap).
Bootstrap(w) ==
    /\ ready = {} /\ owner = NoW /\ fenceRun = run
    /\ ~up[w] /\ conn[w]
    /\ up' = [up EXCEPT ![w] = TRUE]
    /\ view' = [view EXCEPT ![w] = "cur"]
    /\ ready' = {w} /\ owner' = w
    /\ UNCHANGED <<run, joining, sealed, jcopied, durable, conn, q, acks, held, atts,
                   faults, ghost>>

-----------------------------------------------------------------------------
\* Terminal stuttering: no attempt is in flight and every request is decided,
\* abandoned or never sent; or every ready holder stopped, so an operator
\* must cold-start.

Quiescent ==
    /\ atts = {}
    /\ \/ \A r \in Reqs : ident[r].st # "reserved" \/ ident[r].run < run
       \/ AllReadyDown
    /\ UNCHANGED vars

-----------------------------------------------------------------------------
\* Next state. Each handler is a named disjunct so TLC coverage reports it.

SubmitDecisionAny == \E a \in atts : SubmitDecision(a)
LaneRefusedAny    == \E a \in atts : LaneRefused(a)
CommitDecisionAny == \E a \in atts : CommitDecision(a)
SettleAny         == \E a \in atts : Settle(a)
CancelAttemptAny  == \E a \in atts : CancelAttempt(a)

Next ==
    \/ \E r \in Reqs : SubmitRequest(r)
    \/ \E r \in Reqs : RetryDuringViewLag(r)
    \/ \E w \in Workers, r \in Reqs : InstallCopy(w, r)
    \/ \E r \in Reqs : AcceptStaged(r)
    \/ \E r \in Reqs : ReplyClient(r)
    \/ \E r \in Reqs : Start(r)
    \/ \E r \in Reqs : ResolveCommitted(r)
    \/ SubmitDecisionAny
    \/ LaneRefusedAny
    \/ CommitDecisionAny
    \/ SettleAny
    \/ CancelAttemptAny
    \/ \E r \in Reqs : Retire(r)
    \/ \E r \in Reqs : OldRunReceipt(r)
    \/ \E r \in Reqs : ReceiptFault(r)
    \/ \E w \in Workers : ApplyView(w)
    \/ \E x, o \in Workers : RemoveMember(x, o)
    \/ \E w \in Workers : Rejoin(w)
    \/ JoinCopy
    \/ PublishJoin
    \/ AbortJoin
    \/ \E w \in Workers : Crash(w)
    \/ \E w \in Workers : Partition(w)
    \/ \E w \in Workers : Heal(w)
    \/ StopAll
    \/ ColdStart
    \/ FenceGroups
    \/ \E w \in Workers : Bootstrap(w)
    \/ Quiescent

\* Fairness, each named for the mechanism that provides it:
\* - InstallCopy, AcceptStaged, Start, ResolveCommitted: the owner's
\*   admission future and its recovery loop (worker_runtime.rs
\*   recovery_ids, has_recovery_work), which re-drive copies after a view
\*   change;
\* - SubmitDecision, CommitDecision: the attempt task and Status Raft
\*   progress with a quorum;
\* - Settle, CancelAttempt: the Settlement guard and the term check;
\* - ApplyView, JoinCopy, PublishJoin: the membership actor's 250 ms poll
\*   (registered.rs spawn_membership);
\* - RemoveMember: Control's heartbeat timeout;
\* - FenceGroups: the cold-start procedure, which fences before serving.
\* - Heal: the fault-stop assumption. Liveness is claimed only after every
\*   partition ends; a crash or full stop has no matching recovery.
\* Faults, client sends and receipts, Rejoin, ColdStart and Bootstrap
\* (operator actions) get none.
Fairness ==
    /\ \A r \in Reqs : /\ WF_vars(AcceptStaged(r)) /\ WF_vars(Start(r))
                       /\ WF_vars(ResolveCommitted(r))
    /\ \A w \in Workers, r \in Reqs : WF_vars(InstallCopy(w, r))
    /\ WF_vars(SubmitDecisionAny) /\ WF_vars(CommitDecisionAny)
    /\ WF_vars(SettleAny) /\ WF_vars(CancelAttemptAny)
    /\ \A w \in Workers : WF_vars(ApplyView(w))
    /\ WF_vars(\E x, o \in Workers : RemoveMember(x, o))
    /\ WF_vars(JoinCopy) /\ WF_vars(PublishJoin) /\ WF_vars(AbortJoin)
    /\ WF_vars(FenceGroups)
    /\ \A w \in Workers : WF_vars(Heal(w))

Spec     == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
\* Safety properties. Each names its guarantee.

\* OLTP-G07, Q03, R02: an acknowledged request of this run is held by every
\* ready member that has a queue, or its decision is durable.
G07_ReadyHoldAccepted ==
    \A r \in Reqs : ackRun[r] = run =>
        \A w \in ready : (up[w] /\ view[w] # "none") =>
            (q[w][r] \in AccPh \/ ident[r].st = "committed")

\* OLTP-G07, OLTP-G08, R03, O14: no current member holds a record the owner
\* lacks, so a stale or retired copy never re-enters the live queue.
G07_NoStaleRecord ==
    (owner # NoW /\ Current(owner)) =>
        \A w \in ready, r \in Reqs : (Current(w) /\ Has(w, r)) => Has(owner, r)

\* OLTP-G07, SCHED-G04, R05, O32: a cold start never abandons accepted work
\* that a live worker still holds.
G07_NoAbandonWhileHeld == ~lostHeld

\* Q03: data comes only from a request whose accepted version reached every
\* ready member.
Q03_ExecuteOnlyAccepted == \A r \in Reqs : applied[r] > 0 => everAcc[r]

\* OLTP-G03, Q05, R01, T04: one publication per identity.
G03_OnceExecution == \A r \in Reqs : applied[r] <= 1

\* SCHED-G04, R04, O15: an identity reserved in one run never executes in a
\* later run.
R04_NoOldRunExecution ==
    \A r \in Reqs : applied[r] > 0 => commitRun[r] = firstRun[r]

\* OLTP-G04, R04, T05: abandoned and not_applied answers have proof.
G04_AbandonedNeverCommits ==
    \A r \in Reqs : everAb[r] => ident[r].st # "committed"
G04_NotAppliedHasProof ==
    \A r \in Reqs : everNA[r] => ident[r].st # "committed"

\* OLTP-G09, F03: each reservation has one live attempt as owner and is
\* released once.
G09_SettleOnce ==
    \A w \in Workers, r \in Reqs :
        /\ held[w][r] >= 0
        /\ held[w][r] = Cardinality(AttOf(r, w))

\* OLTP-G07, O07, QLANE: a transient local refusal never ends accepted work.
QueueRetention == \A w \in Workers, r \in Reqs : q[w][r] # "failed"

\* O14, PR #103: a same-run retry of accepted work is never refused while
\* its owner holds the record.
O14_RetryAttaches == ~spurious

\* Finding OQ-1 (old F1), OLTP-G04, Q04, Q06: once a client saw a terminal
\* answer, no later answer is non-terminal. The shipped receipt
\* (prefix_f1_receipt_regress) answers "running" or "committing" after the
\* submit path already returned the durable terminal record, and
\* "committing" after a view change until the terminal copy is confirmed
\* again.
Receipt_Monotone == ~regress

-----------------------------------------------------------------------------
\* Liveness, after the fault budgets run out. Checked without symmetry.

\* OLTP-G07, Q01: acknowledged work of a live run reaches a terminal record
\* or a durable decision, unless every ready holder stopped.
L_AcceptedTerminates ==
    \A r \in Reqs :
        (ackRun[r] = run /\ ackRun[r] > 0) ~>
            (ackRun[r] # run \/ AllReadyDown \/ ident[r].st = "committed")

\* OLTP-G07: a committed request of a live run gets a terminal record or is
\* retired, unless every ready holder stopped.
L_CommittedFinishes ==
    \A r \in Reqs :
        (ident[r].st = "committed" /\ ident[r].run = run /\ owner # NoW /\ Has(owner, r)) ~>
            (AllReadyDown \/ owner = NoW \/ q[owner][r] \in {"done", "none"})

\* OLTP-G09, F03: every reservation is eventually released.
L_ReservationsSettle ==
    \A w \in Workers, r \in Reqs : (held[w][r] > 0) ~> (held[w][r] = 0)

=============================================================================
