------------------------------ MODULE OlapStream ------------------------------
(***************************************************************************)
(* Design spec of the OLAP stream data protocol: plans/olap-streaming.md,  *)
(* "Query and delivery contract" and "Worker execution, memory, and        *)
(* recovery", plus the scheduling plan rules it inherits (all-ready        *)
(* admission, takeover, cold start). Register: OLAP-STR-G01 to G06 in      *)
(* docs/testing/guarantees/olap-stream.md.                                  *)
(*                                                                         *)
(* Processes: ORM clients (one stream each) and workers. The scheduler     *)
(* owner is the stream coordinator, and every ready worker holds a RAM     *)
(* copy of each stream entry. API nodes only forward, so they are part of  *)
(* the request and reply channels: an API restart is a lost request or     *)
(* reply.                                                                  *)
(* Stores: the control root and catalog fence (run, term, scheduler, ready *)
(* set) and the OLAP table catalog version are durable object-store state. *)
(* Worker registries are volatile. Batch bytes and prefetch are worker     *)
(* caches and are not modelled: a batch is a pure function of the pinned   *)
(* version and the cursor, so it is recomputed on demand.                  *)
(*                                                                         *)
(* Abstractions, recorded in the plan's design review section:             *)
(*  A1. A commit is one step. WorkerDriver::commit holds                   *)
(*      lock_stream_changes across the local install, every transfer and   *)
(*      the authority recheck. So the outcomes are: every ready peer       *)
(*      installs and the reply is sent (DriverCommit); or the commit stops *)
(*      after reaching a subset of the peers with no reply (CommitPartial, *)
(*      CrashMidCommit). Transfers that did not arrive stay in flight and  *)
(*      can land later (DeliverInstall).                                   *)
(*  A2. The reply channel is folded into the step that sends the reply.    *)
(*      Receiving it is a client-local step; its only effect on other      *)
(*      processes is the next request, which still waits in reqs and can   *)
(*      be delayed, reordered, duplicated or lost there. A reply to a      *)
(*      request the ORM no longer waits on is never read.                  *)
(*  A3. Losing a worker, electing the next scheduler and stream recovery   *)
(*      by the owner are one step (Settle). While a lost worker is still   *)
(*      listed as ready, no commit can collect its acknowledgement and     *)
(*      recovery cannot page it, so that window only adds unreplied        *)
(*      partial commits, which CommitPartial already produces.             *)
(*  A4. Membership only shrinks, except in a cold start. Joins, the join   *)
(*      seal and readiness are checked in OlapStreamJoin, whose invariant  *)
(*      G04_ReadyHoldsReplied is the interface: a joining worker becomes   *)
(*      ready only with a copy of every replied entry.                     *)
(*  A5. Streams are independent: they share only membership events, the    *)
(*      atomic commit and the handle counter. So one full stream, plus a   *)
(*      second client that only opens (OpenOnly), covers two streams.      *)
(*                                                                         *)
(* Action names follow the handlers in server/src/node/stream.rs and       *)
(* server/src/node/stream/distributed.rs. The mapping to code and to the   *)
(* simulator is in wave-dr/sim-mapping/olap-stream.md.                     *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,          \* workers
    Clients,        \* ORM clients; client c uses request ID c and one stream
    OpenOnly,       \* clients that open a stream and stop (A5)
    Nil,
    BatchRows,      \* batch_rows
    RowCount,       \* RowCount[v]: raw rows of the table at catalog version v
    DropLast,       \* drop_last
    MaxCrashes, MaxLoss, MaxDup, MaxDelay, MaxPartitions,
    MaxCommits,     \* concurrent OLAP table commits
    Mutant          \* "none" in the baselines; a mutant name in mutants/*.cfg

ASSUME BatchRows \in Nat \ {0}
ASSUME DropLast \in BOOLEAN
ASSUME OpenOnly \subseteq Clients

Versions == 1..(MaxCommits + 1)
ASSUME \A v \in Versions : RowCount[v] \in Nat

VARIABLES
    \* Durable: control root and catalog fence, and the table catalog.
    run,        \* run_id, bumped by a cold start
    term,       \* scheduler_term, bumped by a takeover
    sched,      \* the scheduler owner and stream coordinator, or Nil
    ready,      \* Ready and Verified workers: members(root)
    recovered,  \* the scheduler finished stream recovery for this view
    catVer,     \* OLAP table catalog version
    \* Workers. reg is volatile and lost on a crash.
    alive,
    reg,        \* reg[n][c]: the stream entry for request ID c, or Nil
    \* Channels: sets, so reordering is free. Delivery removes a message.
    reqs,       \* client, through an API, to the coordinator
    inst,       \* StreamInstall transfers still in flight
    isolated,   \* a worker cut off from every other node and client, or Nil
    \* Clients.
    cl,
    \* Ghosts for properties.
    nextH,      \* next handle ID
    hsnap,      \* hsnap[h]: pinned catalog version of handle h (0 if unused)
    hrun,       \* hrun[h]: run of handle h
    hcl,        \* hcl[h]: client of handle h
    ack,        \* ack[h]: highest sequence replied for h (-1 opened, -2 none)
    openedSent, \* <<c, run, h>> for every Opened reply sent
    last,       \* the commit taken in this step, or Nil after any other step
    \* Fault budgets used.
    nCrash, nLoss, nDup, nDelay, nPart,
    quiesced    \* set once by Done, so the terminal step has coverage

durable  == <<run, term, sched, ready, recovered, catVer>>
nodeVars == <<alive, reg>>
netVars  == <<reqs, inst, isolated>>
ghosts   == <<nextH, hsnap, hrun, hcl, ack, openedSent>>
budgets  == <<nCrash, nLoss, nDup, nDelay, nPart>>
vars == <<durable, nodeVars, netVars, cl, ghosts, last, budgets, quiesced>>

MaxHandles == 2 * Cardinality(Clients) + 2
Handles == 1..MaxHandles

-----------------------------------------------------------------------------
(* Scan semantics (OLAP-STR-G01): a pinned snapshot yields batches of      *)
(* exactly batch_rows rows in key order; the last may be short unless      *)
(* drop_last. A cursor is the raw row position. engine/stream/mod.rs.      *)

Min(a, b) == IF a < b THEN a ELSE b

\* Frame contents stand in for the frame's SHA-256 (Frame::Bytes).
EofOut == <<0, 0, 0>>
NoOut == <<-1, -1, -1>>

IsEof(v, first) ==
    first >= RowCount[v] \/ (DropLast /\ RowCount[v] - first < BatchRows)

\* scan_one: the checkpoint for sequence seq read from version v at cursor
\* first. out is the batch <<v, first, next>> or EofOut.
ScanCp(v, seq, first) ==
    IF IsEof(v, first)
    THEN [seq |-> seq, first |-> first, next |-> first, out |-> EofOut]
    ELSE [seq |-> seq, first |-> first,
          next |-> Min(first + BatchRows, RowCount[v]),
          out |-> <<v, first, Min(first + BatchRows, RowCount[v])>>]

\* The serial-scan oracle of a pinned version.
NumBatches(v) ==
    LET full == RowCount[v] \div BatchRows
        tail == IF RowCount[v] % BatchRows # 0 /\ ~DropLast THEN 1 ELSE 0
    IN full + tail
OracleOut(v, k) == ScanCp(v, k, k * BatchRows).out
Oracle(v) == [k \in 1..NumBatches(v) |-> OracleOut(v, k - 1)]

-----------------------------------------------------------------------------
(* Entries, versions and installs. node/stream/state.rs.                  *)

NoCp == [seq |-> -1, first |-> 0, next |-> 0, out |-> NoOut]
CpSeq(e) == e.cp.seq

VerLt(a, b) == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] < b[2])
\* Version::advance
Advance(prev, t) == IF prev[1] = t THEN <<t, prev[2] + 1>> ELSE <<t, 1>>
\* sync: re-version an entry into the current term before replicating it.
Sync(e) == IF e.ver[1] # term THEN [e EXCEPT !.ver = Advance(e.ver, term)] ELSE e

EmptyReg == [c \in Clients |-> Nil]

\* State::install, expect = None, as a holder applies StreamInstall or a
\* recovery page: a different handle for the request ID, or a different
\* entry at the same version, is a conflict. A lower version keeps the
\* newer copy and still answers Installed.
InstallConflict(old, e) ==
    /\ Mutant # "install_last_writer_wins"
    /\ old # Nil
    /\ (old.h # e.h \/ (e.ver = old.ver /\ e # old))
ReplicaInstall(old, e) ==
    IF Mutant = "install_last_writer_wins" \/ old = Nil THEN e
    ELSE IF InstallConflict(old, e) \/ VerLt(e.ver, old.ver) THEN old
    ELSE e

Reachable(a, b) == isolated \notin {a, b}

\* The commit fan-out: members(root) other than the coordinator.
Fanout(n) == IF Mutant = "reply_before_replicate" THEN {} ELSE ready \ {n}

-----------------------------------------------------------------------------
(* Requests and replies.                                                   *)

OpenReq(c, r) == [k |-> "open", c |-> c, run |-> r, h |-> 0, seq |-> 0]
DemandReq(c, r, h, s) == [k |-> "demand", c |-> c, run |-> r, h |-> h, seq |-> s]

\* The request the ORM waits on.
Req(c) ==
    IF cl[c].ph = "opening" THEN OpenReq(c, cl[c].run)
    ELSE DemandReq(c, cl[c].run, cl[c].h, cl[c].seq)

Waiting(m) == cl[m.c].ph \in {"opening", "streaming"} /\ m = Req(m.c)

Reply(m, kind, h, out, r) == [req |-> m, kind |-> kind, h |-> h, out |-> out, srun |-> r]

\* The ORM after a complete, validated reply: it yields a batch, then
\* demands the next sequence, which acknowledges the batch (A2).
ClAfter(r) ==
    LET c == r.req.c
        cur == cl[c]
    IN CASE r.kind = "opened" ->
              [cl EXCEPT ![c].ph = IF c \in OpenOnly THEN "done" ELSE "streaming",
                         ![c].h = r.h, ![c].seq = 0]
         [] r.kind = "batch" ->
              [cl EXCEPT ![c].got = Append(cur.got, r.out), ![c].seq = cur.seq + 1]
         [] r.kind = "eof" -> [cl EXCEPT ![c].ph = "done"]
         [] OTHER -> [cl EXCEPT ![c].ph = "failed", ![c].err = r.kind]

NextReqs(r) ==
    LET c == r.req.c
        cur == cl[c]
    IN CASE r.kind = "opened" /\ c \notin OpenOnly -> {DemandReq(c, cur.run, r.h, 0)}
         [] r.kind = "batch" -> {DemandReq(c, cur.run, cur.h, cur.seq + 1)}
         [] OTHER -> {}

LossChoices(r) == IF Waiting(r.req) /\ nLoss < MaxLoss THEN BOOLEAN ELSE {FALSE}

\* Send reply r. base is the request channel after this step's own change.
Respond(r, base) ==
    \E lost \in LossChoices(r) :
        /\ nLoss' = IF lost THEN nLoss + 1 ELSE nLoss
        /\ IF Waiting(r.req) /\ ~lost
           THEN /\ cl' = ClAfter(r)
                /\ reqs' = base \cup NextReqs(r)
           ELSE /\ cl' = cl
                /\ reqs' = base

\* Delivery of a request; keep = TRUE leaves a duplicate in the channel.
Consume(m, keep) == IF keep THEN reqs ELSE reqs \ {m}
KeepChoices == IF nDup < MaxDup THEN {FALSE, TRUE} ELSE {FALSE}
CountDup(keep) == nDup' = IF keep THEN nDup + 1 ELSE nDup

-----------------------------------------------------------------------------
(* Stream recovery, node/stream/distributed.rs: ensure_stream_recovered,   *)
(* repair_stream_replicas, and the controller's loss detection and lease   *)
(* (A3). The scheduler s merges every ready copy, keeping the highest      *)
(* version, then repairs every ready holder, before it serves anything.    *)

CandsOver(rg, R, s, c) == {rg[x][c] : x \in R \cup {s}} \ {Nil}
MaxEntry(S) == CHOOSE e \in S : \A x \in S : ~VerLt(e.ver, x.ver)
MergedOver(rg, R, s) ==
    [c \in Clients |->
        IF Mutant = "recover_own_copy" THEN rg[s][c]
        ELSE LET cs == CandsOver(rg, R, s, c) IN IF cs = {} THEN Nil ELSE MaxEntry(cs)]
MergeOver(rg, R, s) ==
    LET merged == MergedOver(rg, R, s) IN
    [x \in Nodes |->
        IF x \in R \cup {s}
        THEN [c \in Clients |->
                IF merged[c] = Nil THEN rg[x][c]
                ELSE IF x = s THEN merged[c]
                ELSE ReplicaInstall(rg[x][c], merged[c])]
        ELSE rg[x]]
ConflictFree(rg, R, s) ==
    \A c \in Clients : \A a, b \in CandsOver(rg, R, s, c) : ~InstallConflict(a, b)

\* Worker n leaves the ready set. The surviving scheduler recovers for the
\* new view, or the controller's lease elects scheduler s at the next term,
\* which recovers before serving. rg is the registry after the loss.
Settle(n, rg, s) ==
    LET R == ready \ {n}
        keepSched == sched \in R
        ok == s # Nil /\ ConflictFree(rg, R, s)
    IN /\ ready' = R
       /\ CASE keepSched -> s = sched /\ sched' = sched /\ term' = term
            [] ~keepSched /\ R = {} -> s = Nil /\ sched' = Nil /\ term' = term
            [] OTHER -> s \in R /\ sched' = s /\ term' = term + 1
       /\ recovered' = (R = {} \/ ok)
       /\ reg' = IF ok THEN MergeOver(rg, R, s) ELSE rg

-----------------------------------------------------------------------------
(* The coordinator's request handling, node/stream.rs: call, check_run,    *)
(* open_stream, reopen, demand, demand_position and scan_one. Prepare      *)
(* gives either a direct reply or the entry to commit with its reply.      *)

\* WorkerDriver::authority: the scheduler of the current term with stream
\* recovery finished for the current view.
Serving(n) == alive[n] /\ sched = n /\ isolated # n /\ recovered

Direct(m, kind) == [type |-> "direct", rep |-> Reply(m, kind, m.h, NoOut, run)]
ToCommit(c, e, rep, read, new) ==
    [type |-> "commit", c |-> c, e |-> e, rep |-> rep, read |-> read, new |-> new]

PrepareOpen(n, m) ==
    LET e == reg[n][m.c]
        v == IF Mutant = "stale_capture" THEN 1 ELSE catVer
    IN IF m.run # run /\ Mutant # "no_run_check" THEN Direct(m, "abandoned")
       ELSE IF e # Nil /\ Mutant # "reopen_new_handle"
       THEN \* reopen: the same ID and body replay the original handle
            ToCommit(m.c, Sync(e), Reply(m, "opened", e.h, NoOut, run), <<>>, FALSE)
       ELSE \* capture pins the current catalog version
            ToCommit(m.c, [h |-> nextH, ver |-> <<term, 1>>, snap |-> v, cp |-> NoCp],
                     Reply(m, "opened", nextH, NoOut, run), <<>>, TRUE)

PrepareDemand(n, m) ==
    LET e == reg[n][m.c]
        vOf == IF Mutant = "scan_latest_catalog" THEN catVer ELSE e.snap
        \* The rows the scan reads: from the cursor, or from the start.
        readOf(cp) == IF Mutant = "rescan_from_first" THEN <<0, cp.next>>
                      ELSE <<cp.first, cp.next>>
        tag(s) == IF Mutant = "checkpoint_prev_sequence" /\ s > 0 THEN s - 1 ELSE s
        kindOf(cp) == IF cp.out = EofOut THEN "eof" ELSE "batch"
        advanceTo(cp) ==
            ToCommit(m.c, [e EXCEPT !.cp = [cp EXCEPT !.seq = tag(cp.seq)],
                                    !.ver = Advance(e.ver, term)],
                     Reply(m, kindOf(cp), m.h, cp.out, run), readOf(cp), FALSE)
    IN IF m.run # run /\ Mutant # "no_run_check" THEN Direct(m, "abandoned")
       ELSE IF e = Nil \/ e.h # m.h THEN Direct(m, "expired")
       ELSE IF e.cp # NoCp /\ m.seq = CpSeq(e)
       THEN \* replay the demanded sequence from its checkpoint, verify, sync
            LET cp == ScanCp(vOf, m.seq, e.cp.first) IN
            IF cp # e.cp THEN Direct(m, "internal")  \* replay failed verification
            ELSE ToCommit(m.c, Sync(e), Reply(m, kindOf(cp), m.h, cp.out, run),
                          readOf(cp), FALSE)
       ELSE IF e.cp # NoCp /\ m.seq = CpSeq(e) + 1 /\ e.cp.out # EofOut
       THEN advanceTo(ScanCp(vOf, m.seq, e.cp.next))  \* acknowledges the checkpoint
       ELSE IF e.cp = NoCp /\ m.seq = 0
       THEN advanceTo(ScanCp(vOf, 0, 0))
       ELSE IF Mutant = "replay_acked_from_history" /\ e.cp # NoCp /\ m.seq < CpSeq(e)
       THEN \* recompute an acknowledged batch instead of conflicting
            LET cp == ScanCp(vOf, m.seq, m.seq * BatchRows) IN
            ToCommit(m.c, e, Reply(m, kindOf(cp), m.h, cp.out, run), <<>>, FALSE)
       ELSE Direct(m, "conflict")

Prepare(n, m) == IF m.k = "open" THEN PrepareOpen(n, m) ELSE PrepareDemand(n, m)

\* A reply without a commit: stream_abandoned, stream_expired, conflict, or
\* a replay that failed verification.
HandleRequest(n, m, keep) ==
    /\ Serving(n)
    /\ m \in reqs
    /\ Prepare(n, m).type = "direct"
    /\ CountDup(keep)
    /\ Respond(Prepare(n, m).rep, Consume(m, keep))
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, durable, nodeVars, inst, isolated, ghosts,
                   nCrash, nDelay, nPart>>

\* What a commit records for the content and fence properties.
LastOf(p, sent) ==
    [kind |-> p.rep.kind, h |-> p.rep.h, seq |-> p.rep.req.seq, out |-> p.rep.out,
     srun |-> p.rep.srun, rrun |-> p.rep.req.run, read |-> p.read,
     first |-> p.e.cp.first, sent |-> sent]

NewHandleGhosts(p) ==
    IF p.new
    THEN /\ p.e.h \in Handles
         /\ nextH' = nextH + 1
         /\ hsnap' = [hsnap EXCEPT ![p.e.h] = p.e.snap]
         /\ hrun' = [hrun EXCEPT ![p.e.h] = run]
         /\ hcl' = [hcl EXCEPT ![p.e.h] = p.c]
    ELSE UNCHANGED <<nextH, hsnap, hrun, hcl>>

\* The registry after installing e locally at n and at the peers in S.
RegWith(n, c, e, S) ==
    [x \in Nodes |-> IF x = n THEN [reg[n] EXCEPT ![c] = e]
                     ELSE IF x \in S THEN [reg[x] EXCEPT ![c] = ReplicaInstall(reg[x][c], e)]
                     ELSE reg[x]]

\* A peer that can take the transfer now.
Good(n, p, c, e) == alive[p] /\ Reachable(n, p) /\ ~InstallConflict(reg[p][c], e)

\* WorkerDriver::commit when every transfer is acknowledged and the
\* authority and view recheck passes: install everywhere, then reply.
DriverCommit(n, m, keep) ==
    /\ Serving(n)
    /\ m \in reqs
    /\ LET p == Prepare(n, m) IN
       /\ p.type = "commit"
       /\ \A q \in Fanout(n) : Good(n, q, p.c, p.e)
       /\ CountDup(keep)
       /\ reg' = RegWith(n, p.c, p.e, Fanout(n))
       /\ Respond(p.rep, Consume(m, keep))
       /\ NewHandleGhosts(p)
       /\ ack' = IF p.rep.kind = "opened"
                 THEN [ack EXCEPT ![p.rep.h] = IF @ < -1 THEN -1 ELSE @]
                 ELSE IF p.rep.kind \in {"batch", "eof"}
                 THEN [ack EXCEPT ![p.rep.h] = IF @ < p.rep.req.seq THEN p.rep.req.seq ELSE @]
                 ELSE ack
       /\ openedSent' = IF p.rep.kind = "opened"
                        THEN openedSent \cup {<<m.c, run, p.rep.h>>} ELSE openedSent
       /\ last' = LastOf(p, TRUE)
    /\ UNCHANGED <<quiesced, durable, alive, inst, isolated, nCrash, nDelay, nPart>>

\* The commit stops after the transfers to S. The local install stays and
\* the ORM gets no reply, so it retries the same request. The cause:
\*  "blocked": a fan-out peer rejects the entry (a conflict);
\*  "delay":   a transfer or its acknowledgement exceeds TRANSACTION_TIMEOUT
\*             (a fault: the message stays in flight and can land later).
\* Transfers to reachable peers outside S stay in flight.
CommitPartial(n, m, keep, S, why) ==
    /\ Serving(n)
    /\ m \in reqs
    /\ LET p == Prepare(n, m)
           F == Fanout(n)
           good == {q \in F : Good(n, q, p.c, p.e)}
           late == {q \in F \ S : alive[q] /\ ~InstallConflict(reg[q][p.c], p.e)}
       IN
       /\ p.type = "commit"
       /\ S \subseteq good
       /\ CASE why = "blocked" -> good # F
            [] why = "delay" -> nDelay < MaxDelay
       /\ CountDup(keep)
       /\ reqs' = Consume(m, keep)
       /\ inst' = inst \cup {[from |-> n, to |-> q, c |-> p.c, e |-> p.e, run |-> run]
                             : q \in late}
       /\ NewHandleGhosts(p)
       /\ last' = LastOf(p, FALSE)
       /\ reg' = RegWith(n, p.c, p.e, S)
       /\ nDelay' = IF why = "delay" THEN nDelay + 1 ELSE nDelay
    /\ UNCHANGED <<quiesced, durable, alive, isolated, cl, ack, openedSent,
                   nCrash, nLoss, nPart>>

\* A transfer that was in flight lands (distributed.rs accept): the sender
\* must be the current scheduler, the entry's run current and its term not
\* ahead of the fence; install_fenced then keeps the newer copy.
DeliverInstall(m, keep) ==
    /\ m \in inst
    /\ alive[m.to] /\ Reachable(m.from, m.to)
    /\ CountDup(keep)
    /\ inst' = IF keep THEN inst ELSE inst \ {m}
    /\ LET accept == \/ Mutant = "install_last_writer_wins"
                     \/ m.run = run /\ m.from = sched /\ m.e.ver[1] <= term
       IN reg' = IF accept
                 THEN [reg EXCEPT ![m.to][m.c] = ReplicaInstall(reg[m.to][m.c], m.e)]
                 ELSE reg
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, durable, alive, reqs, isolated, cl, ghosts,
                   nCrash, nLoss, nDelay, nPart>>

-----------------------------------------------------------------------------
(* Clients: the ORM BatchStream (orm/python/src/nabedra/stream.py).        *)

\* Discover the run (GET /v1/streams/capabilities) and open.
ClientStart(c) ==
    /\ cl[c].ph = "init"
    /\ cl' = [cl EXCEPT ![c] = [ph |-> "opening", run |-> run, h |-> 0, seq |-> 0,
                                 got |-> <<>>, err |-> "", floor |-> catVer]]
    /\ reqs' = reqs \cup {OpenReq(c, run)}
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, durable, nodeVars, inst, isolated, ghosts, budgets>>

\* The ORM's same-run retry timer re-sends the outstanding request once it
\* can no longer complete: the request or its reply was lost, or the commit
\* stopped. A retry racing its own reply is the duplicate fault.
ClientRetry(c) ==
    /\ cl[c].ph \in {"opening", "streaming"}
    /\ Req(c) \notin reqs
    /\ Mutant # "no_client_retry"
    /\ reqs' = reqs \cup {Req(c)}
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, durable, nodeVars, inst, isolated, cl, ghosts, budgets>>

-----------------------------------------------------------------------------
(* Control events.                                                         *)

\* The OLAP commit protocol publishes a new table version (catalog CAS).
PublishTableCommit ==
    /\ catVer <= MaxCommits
    /\ catVer' = catVer + 1
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, run, term, sched, ready, recovered, nodeVars, netVars, cl,
                   ghosts, budgets>>

\* An authorized cold start after every holder stopped: a new run, blank
\* registries, every worker ready again.
ColdStart(s) ==
    /\ ready = {}
    /\ run' = run + 1
    /\ term' = term + 1
    /\ sched' = s
    /\ ready' = Nodes
    /\ recovered' = TRUE
    /\ alive' = [n \in Nodes |-> TRUE]
    /\ reg' = [n \in Nodes |-> EmptyReg]
    /\ isolated' = Nil
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, catVer, reqs, inst, cl, ghosts, budgets>>

-----------------------------------------------------------------------------
(* Faults, each with a budget.                                             *)

Crash(n, s) ==
    /\ nCrash < MaxCrashes /\ alive[n] /\ n \in ready
    /\ alive' = [alive EXCEPT ![n] = FALSE]
    /\ Settle(n, [reg EXCEPT ![n] = EmptyReg], s)
    /\ nCrash' = nCrash + 1
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, run, catVer, netVars, cl, ghosts,
                   nLoss, nDup, nDelay, nPart>>

\* The coordinator crashes after the transfers to S (the cut between demand
\* and ack). The rest stays in flight, as in CommitPartial.
CrashMidCommit(n, m, S, s) ==
    /\ nCrash < MaxCrashes
    /\ Serving(n)
    /\ m \in reqs
    /\ LET p == Prepare(n, m)
           F == Fanout(n)
           good == {q \in F : Good(n, q, p.c, p.e)}
           late == {q \in F \ S : alive[q] /\ ~InstallConflict(reg[q][p.c], p.e)}
           rg == [RegWith(n, p.c, p.e, S) EXCEPT ![n] = EmptyReg]
       IN
       /\ p.type = "commit"
       /\ S \subseteq good
       /\ reqs' = reqs \ {m}
       /\ inst' = inst \cup {[from |-> n, to |-> q, c |-> p.c, e |-> p.e, run |-> run]
                             : q \in late}
       /\ NewHandleGhosts(p)
       /\ last' = LastOf(p, FALSE)
       /\ alive' = [alive EXCEPT ![n] = FALSE]
       /\ Settle(n, rg, s)
    /\ nCrash' = nCrash + 1
    /\ UNCHANGED <<quiesced, run, catVer, isolated, cl, ack, openedSent,
                   nLoss, nDup, nDelay, nPart>>

\* The worker is cut off from every node and client. The controller drops it
\* from the ready set; it returns only through a join (OlapStreamJoin).
Partition(n, s) ==
    /\ nPart < MaxPartitions /\ isolated = Nil /\ n \in ready
    /\ isolated' = n
    /\ nPart' = nPart + 1
    /\ last' = Nil
    /\ Settle(n, reg, s)
    /\ UNCHANGED <<quiesced, run, catVer, alive, reqs, inst, cl, ghosts,
                   nCrash, nLoss, nDup, nDelay>>

DropRequest(m) ==
    /\ nLoss < MaxLoss /\ m \in reqs
    /\ reqs' = reqs \ {m}
    /\ nLoss' = nLoss + 1
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, durable, nodeVars, inst, isolated, cl, ghosts,
                   nCrash, nDup, nDelay, nPart>>

DropInstall(m) ==
    /\ nLoss < MaxLoss /\ m \in inst
    /\ inst' = inst \ {m}
    /\ nLoss' = nLoss + 1
    /\ last' = Nil
    /\ UNCHANGED <<quiesced, durable, nodeVars, reqs, isolated, cl, ghosts,
                   nCrash, nDup, nDelay, nPart>>

\* Every client finished: the explicit terminal step. It sets quiesced once,
\* so coverage counts it, and then stutters.
Done ==
    /\ \A c \in Clients : cl[c].ph \in {"done", "failed"}
    /\ quiesced' = TRUE
    /\ last' = Nil
    /\ UNCHANGED <<durable, nodeVars, netVars, cl, ghosts, budgets>>

-----------------------------------------------------------------------------

Init ==
    /\ run = 1 /\ term = 1 /\ catVer = 1
    /\ ready = Nodes
    /\ sched \in Nodes
    /\ recovered = TRUE
    /\ alive = [n \in Nodes |-> TRUE]
    /\ reg = [n \in Nodes |-> EmptyReg]
    /\ reqs = {} /\ inst = {}
    /\ isolated = Nil
    /\ cl = [c \in Clients |-> [ph |-> "init", run |-> 0, h |-> 0, seq |-> 0,
                                got |-> <<>>, err |-> "", floor |-> 0]]
    /\ nextH = 1
    /\ hsnap = [h \in Handles |-> 0]
    /\ hrun = [h \in Handles |-> 0]
    /\ hcl = [h \in Handles |-> Nil]
    /\ ack = [h \in Handles |-> -2]
    /\ openedSent = {}
    /\ last = Nil
    /\ nCrash = 0 /\ nLoss = 0 /\ nDup = 0 /\ nDelay = 0 /\ nPart = 0
    /\ quiesced = FALSE

Whys == {"blocked", "delay"}

\* Named wrappers so TLC's coverage reports each action separately.
ClientStartAny == \E c \in Clients : ClientStart(c)
ClientRetryAny == \E c \in Clients : ClientRetry(c)
HandleRequestAny == \E n \in Nodes, m \in reqs, k \in KeepChoices : HandleRequest(n, m, k)
DriverCommitAny == \E n \in Nodes, m \in reqs, k \in KeepChoices : DriverCommit(n, m, k)
CommitPartialAny ==
    \E n \in Nodes, m \in reqs, k \in KeepChoices, S \in SUBSET Nodes, w \in Whys :
        CommitPartial(n, m, k, S, w)
CrashMidCommitAny ==
    \E n \in Nodes, m \in reqs, S \in SUBSET Nodes, s \in Nodes \cup {Nil} :
        CrashMidCommit(n, m, S, s)
DeliverInstallAny == \E m \in inst, k \in KeepChoices : DeliverInstall(m, k)
CrashAny == \E n \in Nodes, s \in Nodes \cup {Nil} : Crash(n, s)
PartitionAny == \E n \in Nodes, s \in Nodes \cup {Nil} : Partition(n, s)
ColdStartAny == \E s \in Nodes : ColdStart(s)
DropRequestAny == \E m \in reqs : DropRequest(m)
DropInstallAny == \E m \in inst : DropInstall(m)

Next ==
    \/ ClientStartAny \/ ClientRetryAny
    \/ HandleRequestAny \/ DriverCommitAny \/ CommitPartialAny \/ DeliverInstallAny
    \/ PublishTableCommit \/ ColdStartAny
    \/ CrashAny \/ CrashMidCommitAny \/ PartitionAny
    \/ DropRequestAny \/ DropInstallAny
    \/ Done

(***************************************************************************)
(* Fairness, each assumption with the mechanism that provides it:          *)
(*  - ClientRetry: the ORM's same-run retry timer with backoff.             *)
(*  - HandleRequest, DriverCommit: the coordinator's handlers, fed by the   *)
(*    API forwarders, and the commit's transfer loop.                       *)
(*  - DeliverInstall: a transfer in flight is delivered or times out.       *)
(*  - ColdStart: the operator's authorized restart once no worker is ready. *)
(* Liveness holds after the faults stop: every fault has a finite budget.   *)
(* No fairness on any fault, on CommitPartial, or on a client starting.     *)
(***************************************************************************)
Fairness ==
    /\ \A c \in Clients : WF_vars(ClientRetry(c))
    /\ \A n \in Nodes : WF_vars(\E m \in reqs : HandleRequest(n, m, FALSE))
    /\ \A n \in Nodes : WF_vars(\E m \in reqs : DriverCommit(n, m, FALSE))
    /\ WF_vars(\E m \in inst : DeliverInstall(m, FALSE))
    /\ WF_vars(ColdStartAny)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties.                                                             *)

TypeOK ==
    /\ run \in Nat /\ term \in Nat /\ catVer \in 1..(MaxCommits + 1)
    /\ sched \in Nodes \cup {Nil}
    /\ ready \subseteq Nodes
    /\ recovered \in BOOLEAN
    /\ alive \in [Nodes -> BOOLEAN]
    /\ \A n \in Nodes, c \in Clients :
         reg[n][c] = Nil \/ (reg[n][c].h \in Handles /\ reg[n][c].snap \in Versions)
    /\ isolated \in Nodes \cup {Nil}
    /\ \A c \in Clients : cl[c].ph \in {"init", "opening", "streaming", "done", "failed"}
    /\ nextH \in 1..(MaxHandles + 1)
    /\ \A h \in Handles : ack[h] \in -2..(NumBatches(MaxCommits + 1) + 1)
    /\ quiesced \in BOOLEAN

\* OLAP-STR-G01: deduplicated by sequence, the batches a client yields are
\* a prefix of the serial scan of its pinned snapshot, in key order, and EOF
\* comes only after the whole oracle.
G01_OracleOrder ==
    \A c \in Clients :
        cl[c].ph \in {"streaming", "done", "failed"} /\ cl[c].h # 0 =>
            LET o == Oracle(hsnap[cl[c].h]) IN
            /\ Len(cl[c].got) <= Len(o)
            /\ \A i \in 1..Len(cl[c].got) : cl[c].got[i] = o[i]
            /\ (cl[c].ph = "done" /\ c \notin OpenOnly) => Len(cl[c].got) = Len(o)

\* OLAP-STR-G01 (ConsistentRead) and G05: an open pins a version no older
\* than the catalog the client saw when it started.
G01_FreshPin ==
    \A h \in Handles : hcl[h] # Nil => hsnap[h] >= cl[hcl[h]].floor

\* OLAP-STR-G02: within a run, a request ID only ever gets one handle.
G02_OneHandle ==
    \A x, y \in openedSent : x[1] = y[1] /\ x[2] = y[2] => x[3] = y[3]

\* OLAP-STR-G03 and G01: every batch or EOF a commit produces for sequence
\* k is the oracle's batch k of the pinned snapshot, so replays are equal.
G03_ReplyMatchesOracle ==
    last # Nil =>
        /\ last.kind = "batch" => /\ last.seq < NumBatches(hsnap[last.h])
                                  /\ last.out = OracleOut(hsnap[last.h], last.seq)
        /\ last.kind = "eof" => last.seq = NumBatches(hsnap[last.h])

\* OLAP-STR-G03: demanding the next sequence acknowledges the prior one, so
\* no batch or EOF is sent for a sequence below one already replied.
G03_AckFence ==
    [][last' # Nil /\ last'.sent /\ last'.kind \in {"batch", "eof"} =>
           last'.seq >= ack[last'.h]]_vars

\* OLAP-STR-G04: whatever a client may have been told about a stream in the
\* current run is held by every live ready worker: the handle, and a
\* checkpoint at or past the highest replied sequence.
G04_ReplyImpliesReplicated ==
    \A h \in Handles :
        hrun[h] = run /\ ack[h] >= -1 =>
            \A n \in ready :
                alive[n] =>
                    /\ reg[n][hcl[h]] # Nil
                    /\ reg[n][hcl[h]].h = h
                    /\ CpSeq(reg[n][hcl[h]]) >= ack[h]

\* OLAP-STR-G04: a client's legitimate request never fails with conflict,
\* expiry or a replay error. Only a cold start ends a stream.
G04_NoSpuriousFailure ==
    \A c \in Clients : cl[c].ph = "failed" => cl[c].err = "abandoned"

\* OLAP-STR-G04: live ready holders agree on each request ID's handle and
\* never hold different entries at one version.
G04_HoldersAgree ==
    \A c \in Clients, a, b \in ready :
        alive[a] /\ alive[b] /\ reg[a][c] # Nil /\ reg[b][c] # Nil =>
            /\ reg[a][c].h = reg[b][c].h
            /\ reg[a][c].ver = reg[b][c].ver => reg[a][c] = reg[b][c]

\* OLAP-STR-G04 (read R-02): no live worker's entry for a handle ever moves
\* to an older version. A late transfer from a deposed coordinator or a
\* delayed duplicate cannot regress it.
G04_HolderMonotone ==
    [][\A n \in Nodes, c \in Clients :
          (/\ alive[n] /\ alive'[n]
           /\ reg[n][c] # Nil /\ reg'[n][c] # Nil
           /\ reg'[n][c].h = reg[n][c].h)
          => ~VerLt(reg'[n][c].ver, reg[n][c].ver)]_vars

\* OLAP-STR-G05: nothing is opened or served for a request from another
\* run, and no old-run handle is served in a new run.
G05_OldRunNeverServed ==
    last # Nil /\ last.kind \in {"opened", "batch", "eof"} =>
        last.rrun = last.srun /\ hrun[last.h] = last.srun

\* OLAP-STR-G06: a scan for a demanded sequence starts at its checkpoint
\* cursor. It never starts over and reads rows already acknowledged.
G06_NoRescan ==
    last # Nil /\ last.read # <<>> => last.read[1] = last.first

\* Liveness, OLAP-STR-G03 and G04: after the faults stop, every started
\* stream ends in EOF or, only after a cold start, stream_abandoned.
Termination ==
    \A c \in Clients : cl[c].ph = "opening" ~> cl[c].ph \in {"done", "failed"}

=============================================================================
