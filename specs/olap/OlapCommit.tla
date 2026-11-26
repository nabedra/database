------------------------------ MODULE OlapCommit ------------------------------
(***************************************************************************)
(* The OLAP commit protocol: plans/resource-aware-scheduling.md, mutation  *)
(* publication, fencing and reconciliation (S4, T01 to T06, R01, R04, R05),*)
(* and the catalog fence of plans/cli-and-serverless.md (SL03, SL05).      *)
(*                                                                         *)
(* Writers run mutation attempts for requests held in the replicated       *)
(* queue. One object store holds immutable Parquet files, immutable        *)
(* decision records and one mutable root (catalog.json), written If-Match. *)
(* An ETag is a function of the content, so a condition compares roots.    *)
(*                                                                         *)
(* Durable: root, commits, pending. Replicated RAM, lost only when every   *)
(* worker stops: q, pins. Volatile per writer: att. Ghosts: clientReplies, *)
(* claims and the counters.                                                *)
(*                                                                         *)
(* Exact representation of unreferenced objects. The plan forbids finding  *)
(* data by listing, so a file or decision is observable only through a     *)
(* root. An attempt's objects are therefore kept in its own record (have,  *)
(* rows), move into its root PUT, and enter `commits` when that PUT        *)
(* applies. Objects of a dead attempt are forgotten. A commit ID is a      *)
(* fresh UUID, modelled as the least ID nothing can still refer to.        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Writers, Requests, Readers,
    NumParts,       \* a mutation rewrites every partition (T01)
    MaxId,          \* commit IDs; large enough that allocation never blocks
    MaxCrashes,     \* writer crashes, or attempts lost to a store outage
    MaxLostReplies, \* replies lost after the PUT was evaluated (T04)
    MaxDelays,      \* root PUTs still pending at the writer's deadline (T04, T05)
    MaxTakeovers,   \* scheduler owner changes in a run (R01)
    MaxColdStarts,  \* stops of every worker, then a new run (R04, T05)
    MaxReplays,     \* client retries of a settled request (T06)
    HistoryBudget,  \* decision records one lookup may read
    Window,         \* Variant "window": decisions carried inline in the root
    RetryLimit,     \* prefix_retry_limit only: attempts before the owner stops
    Variant,        \* "chain" (walk the history) | "window" (candidate fix of F3)
    Gc,             \* "none" (shipped: nothing is deleted) | "safe" (candidate rule)
    Mutant,
    NoCid, NoObj, NotRead, Missing, Expired, Unknown

Parts == 1..NumParts
Cids  == 1..MaxId
Fences == [run : 1..(1 + MaxColdStarts), term : 1..(1 + MaxTakeovers)]
FenceInit == [run |-> 1, term |-> 1]
FenceLeq(a, b) == a.run < b.run \/ (a.run = b.run /\ a.term <= b.term)
RootInit == [rev |-> 0, head |-> NoCid, fence |-> FenceInit, recent |-> <<>>]

VARIABLES
    root,           \* catalog.json
    commits,        \* objects of every applied commit: [req, parent, rev, fence, rows, have]
    pending,        \* root PUTs the store accepted and has not evaluated
    replies,        \* store replies not yet received
    control,        \* fence_outside_root only: the fence kept in another object
    curFence,       \* authority of the live run and scheduler owner
    fenceBase,      \* the root the new owner read for its fence PUT
    att,            \* one attempt per writer, or NoObj
    q,              \* queue state. "queued" also stands for reconciling
    tries,          \* prefix_retry_limit only
    adm,            \* admission revision (Variant "window")
    pins, seen,     \* readers
    clientReplies, claims,
    crashes, lost, delays, takeovers, colds, replays

store  == <<root, commits, pending, replies, control>>
budget == <<crashes, lost, delays, takeovers, colds, replays>>
ghost  == <<clientReplies, claims>>
rds    == <<pins, seen>>
queue  == <<q, tries, adm>>
vars == <<store, curFence, fenceBase, att, queue, rds, ghost, budget>>

-----------------------------------------------------------------------------
RECURSIVE ChainSeq(_)
ChainSeq(c) == IF c = NoCid \/ commits[c] = NoObj THEN <<>> ELSE <<c>> \o ChainSeq(commits[c].parent)
Chain(rt) == LET s == ChainSeq(rt.head) IN {s[i] : i \in DOMAIN s}
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y

AuthFence == IF Mutant = "fence_outside_root" THEN control ELSE root.fence
Fenced == AuthFence = curFence

Rows(rt, p) == IF rt.head = NoCid THEN {}
               ELSE IF p \notin commits[rt.head].have THEN Missing ELSE commits[rt.head].rows

\* history.rs lookup_decision against one captured root. Absence is proved
\* only at the null terminus, and the walk stops at HistoryBudget records
\* (shipped, Variant "chain"). Variant "window" is the candidate fix of the
\* unbounded chain: the root carries the last Window decisions inline and a
\* request older than the window gets the one defined outcome Expired. The
\* mutant prefix_history_budget restores the shipped walk under that variant.
Lookup(rt, r) ==
    IF Variant = "window" /\ Mutant # "prefix_history_budget"
    THEN IF rt.rev - adm[r] > Window THEN [kind |-> "expired"]
         ELSE LET hit == {i \in DOMAIN rt.recent : commits[rt.recent[i]].req = r}
              IN IF hit = {} THEN [kind |-> "absent"]
                 ELSE [kind |-> "found", cid |-> rt.recent[MinOf(hit)]]
    ELSE LET ch == ChainSeq(rt.head)
             hit == {i \in DOMAIN ch : commits[ch[i]].req = r}
             depth == IF hit = {} THEN Len(ch) ELSE MinOf(hit)
         IN IF depth > HistoryBudget THEN [kind |-> "limit"]
            ELSE IF hit = {} THEN [kind |-> "absent"]
            ELSE [kind |-> "found", cid |-> ch[MinOf(hit)]]

NewRecent(rt, c) == IF Variant # "window" THEN <<>>
                    ELSE SubSeq(<<c>> \o rt.recent, 1, MinOf({Window, Len(rt.recent) + 1}))

\* Awaited by a live attempt. An unawaited PUT whose base is not the root can
\* never apply, because roots never repeat (G02_LinearHistory and
\* G04_FenceMonotonic), so forgetting it loses no behavior.
Awaited(c, a) == \E w \in Writers : a[w] # NoObj /\ a[w].pc = "await" /\ a[w].cid = c
Prune(ps, rt, a) == IF Mutant \in {"no_cas", "fence_outside_root", "cold_start_blind_overwrite"} THEN ps
                    ELSE {pt \in ps : pt.base = rt \/ (pt.kind = "commit" /\ Awaited(pt.cid, a))
                                      \/ (pt.kind = "fence" /\ pt.new.fence = curFence)}

InUse == {c \in Cids : commits[c] # NoObj}
         \cup {att[w].cid : w \in {x \in Writers : att[x] # NoObj}}
         \cup {pt.cid : pt \in pending} \cup {rp.cid : rp \in replies}
FreeCid == MinOf(Cids \ InUse)

Owns(a) == a.fence = curFence /\ q[a.req] = "running"

\* The attempt of w ends. `s` is the queue state if it still owns the request.
Finish(w, s) ==
    /\ q' = IF Owns(att[w]) THEN [q EXCEPT ![att[w].req] = s] ELSE q
    /\ att' = [att EXCEPT ![w] = NoObj]
    /\ pending' = Prune(pending, root, att')
Reply(r, v) == clientReplies' = [clientReplies EXCEPT ![r] = @ \cup {v}]

EndTerminal(w, c) == Finish(w, "done") /\ Reply(att[w].req, c) /\ UNCHANGED tries
EndExpired(w) == Finish(w, "done") /\ Reply(att[w].req, Expired) /\ UNCHANGED tries
\* OutcomeUnknown. The request stays in the queue and is retried.
EndUnknown(w) ==
    IF Mutant = "prefix_local_unknown"          \* F5: local serve settles it as terminal
    THEN Finish(w, "done") /\ Reply(att[w].req, Unknown) /\ UNCHANGED tries
    ELSE /\ Finish(w, "queued") /\ UNCHANGED clientReplies
         /\ tries' = IF Mutant = "prefix_retry_limit" /\ Owns(att[w])
                     THEN [tries EXCEPT ![att[w].req] = @ + 1] ELSE tries

-----------------------------------------------------------------------------
Init ==
    /\ root = RootInit /\ commits = [c \in Cids |-> NoObj] /\ pending = {} /\ replies = {}
    /\ control = FenceInit /\ curFence = FenceInit /\ fenceBase = RootInit
    /\ att = [w \in Writers |-> NoObj]
    /\ q = [r \in Requests |-> "queued"] /\ tries = [r \in Requests |-> 0]
    /\ adm = [r \in Requests |-> 0]
    /\ pins = [rd \in Readers |-> NoObj]
    /\ seen = [rd \in Readers |-> [p \in Parts |-> NotRead]]
    /\ clientReplies = [r \in Requests |-> {}] /\ claims = {}
    /\ crashes = 0 /\ lost = 0 /\ delays = 0 /\ takeovers = 0 /\ colds = 0 /\ replays = 0

-----------------------------------------------------------------------------
(* Queue and client (plan: replicated RAM admission, receipts; Q05, Q06).  *)

\* The same ID and body again, in the same run or after a cold start (T06).
ClientReplay(r) ==
    /\ replays < MaxReplays /\ replays' = replays + 1
    /\ q[r] \in {"done", "abandoned"} /\ Fenced
    /\ q' = [q EXCEPT ![r] = "queued"]
    /\ UNCHANGED <<store, curFence, fenceBase, att, tries, adm, rds, ghost,
                   crashes, lost, delays, takeovers, colds>>

\* queue_runtime.rs queue_pump and run_queued. The owner dispatches only after
\* its fence is on the root (owned_catalog).
ScheduleAttempt(w, r) ==
    /\ q[r] = "queued" /\ att[w] = NoObj
    /\ Fenced
    /\ Mutant = "prefix_retry_limit" => tries[r] < RetryLimit    \* F2
    /\ att' = [att EXCEPT ![w] = [req |-> r, fence |-> curFence, pc |-> "load", base |-> RootInit,
                                  cid |-> NoCid, have |-> {}, rows |-> {}, conflict |-> FALSE]]
    /\ q' = [q EXCEPT ![r] = "running"]
    /\ UNCHANGED <<store, curFence, fenceBase, tries, adm, rds, ghost, budget>>

\* Receipts. An old-run receipt is answered from durable decisions, and then
\* as abandoned. Candidate fixes: the root carries the current fence before
\* `abandoned` is reported (F1), and a process compares the fence of the root
\* it loads with its own before it answers for another run (F4).
LookupReceipt(r) ==
    LET l == Lookup(root, r) IN
    \/ /\ q[r] = "abandoned"
       /\ Fenced \/ Mutant = "prefix_unfenced_receipt"
       /\ \/ l.kind = "found" /\ Reply(r, l.cid) /\ UNCHANGED claims
          \/ l.kind = "absent" /\ UNCHANGED clientReplies
             /\ claims' = claims \cup {<<r, curFence.run - 1>>}
       /\ UNCHANGED <<store, curFence, fenceBase, att, queue, rds, budget>>
    \/ /\ Mutant = "prefix_stale_receipt"        \* F4: a process of an older run
       /\ colds > 0 /\ q[r] \in {"queued", "running"}
       /\ l.kind = "absent"
       /\ claims' = claims \cup {<<r, curFence.run>>}
       /\ UNCHANGED <<store, curFence, fenceBase, att, queue, rds, clientReplies, budget>>

-----------------------------------------------------------------------------
(* One attempt: coordinator.rs execute_inner, publication.rs publish.      *)

\* load_cluster_catalog, the fence check and lookup_decision. Also the top of
\* the loop after NeedsRecompute.
LoadAndLookup(w) ==
    /\ att[w] # NoObj /\ att[w].pc = "load"
    /\ LET a == att[w]
           l == IF Mutant = "skip_lookup" THEN [kind |-> "absent"] ELSE Lookup(root, a.req)
       IN IF AuthFence # a.fence /\ Mutant # "rebase_adopts_term" THEN EndUnknown(w)
          ELSE CASE l.kind = "found"   -> EndTerminal(w, l.cid)
                 [] l.kind = "expired" -> EndExpired(w)
                 [] l.kind = "limit"   -> EndUnknown(w)
                 [] OTHER -> /\ att' = [att EXCEPT ![w].pc = "upload", ![w].base = root, ![w].cid = FreeCid]
                             /\ UNCHANGED <<q, tries, pending, clientReplies>>
    /\ UNCHANGED <<root, commits, replies, control, curFence, fenceBase, adm, rds, claims, budget>>

\* Command::UploadStaged: one immutable Create per partition. A crash can fall
\* before, between or after the files (T02). A pruned base file fails the read.
UploadStaged(w) ==
    /\ att[w] # NoObj /\ att[w].pc = "upload"
    /\ LET a == att[w]
           p == Cardinality(a.have) + 1
           last == p = NumParts \/ Mutant = "root_before_uploads"
       IN IF Rows(a.base, p) = Missing THEN EndUnknown(w)
          ELSE /\ att' = [att EXCEPT ![w].have = @ \cup {p}, ![w].rows = Rows(a.base, p) \cup {a.req},
                                     ![w].pc = IF last THEN "publish" ELSE "upload"]
               /\ UNCHANGED <<q, tries, pending, clientReplies>>
    /\ UNCHANGED <<root, commits, replies, control, curFence, fenceBase, adm, rds, claims, budget>>

\* publish(): Create decisions/<id>.json, then PUT the root If-Match its base.
\* A decision that no root reaches is invisible, so one step stands for both.
Candidate(a) == [kind |-> "commit", cid |-> a.cid, base |-> a.base,
                 new |-> [rev |-> a.base.rev + 1, head |-> a.cid, fence |-> a.base.fence,
                          recent |-> NewRecent(a.base, a.cid)],
                 obj |-> [req |-> a.req, parent |-> a.base.head, rev |-> a.base.rev + 1,
                          fence |-> a.fence, rows |-> a.rows, have |-> a.have]]
PutRoot(w) ==
    /\ att[w] # NoObj /\ att[w].pc = "publish"
    /\ att' = [att EXCEPT ![w].pc = "await"]
    /\ pending' = pending \cup {Candidate(att[w])}
    /\ UNCHANGED <<root, commits, replies, control, curFence, fenceBase, queue, rds, ghost, budget>>

\* The store evaluates one PUT atomically, its condition at that moment.
StoreApplyPut(pt) ==
    LET ok == root = pt.base \/ (Mutant = "no_cas" /\ pt.kind = "commit")
                             \/ (Mutant = "cold_start_blind_overwrite" /\ pt.kind = "fence")
        outside == pt.kind = "fence" /\ Mutant = "fence_outside_root"
    IN /\ root' = IF ok /\ ~outside THEN pt.new ELSE root
       /\ control' = IF pt.kind = "fence" /\ (ok \/ outside) /\ FenceLeq(control, pt.new.fence)
                     THEN pt.new.fence ELSE control
       /\ commits' = IF ok /\ pt.kind = "commit" THEN [commits EXCEPT ![pt.cid] = pt.obj] ELSE commits
       /\ replies' = IF pt.kind = "commit" /\ Awaited(pt.cid, att)
                     THEN replies \cup {[cid |-> pt.cid, ok |-> ok]} ELSE replies
       /\ pending' = Prune(pending \ {pt}, root', att)
       /\ UNCHANGED <<curFence, fenceBase, att, queue, rds, ghost, budget>>
StoreApply == \E pt \in pending : StoreApplyPut(pt)

ReceivePutReply(w) ==
    /\ att[w] # NoObj /\ att[w].pc = "await"
    /\ \E rp \in replies :
         /\ rp.cid = att[w].cid
         /\ replies' = replies \ {rp}
         /\ IF rp.ok \/ Mutant = "ok_on_conflict" THEN EndTerminal(w, rp.cid)
            ELSE /\ att' = [att EXCEPT ![w].pc = "recover", ![w].conflict = TRUE]
                 /\ UNCHANGED <<q, tries, pending, clientReplies>>
    /\ UNCHANGED <<root, commits, control, curFence, fenceBase, adm, rds, claims, budget>>

\* Ambiguous: timeout_at fired. The PUT was evaluated and its reply lost, or
\* (a fault, with a budget) the store still holds it.
Ambiguous(w) ==
    IF Mutant = "timeout_not_applied"
    THEN /\ claims' = claims \cup {<<att[w].req, att[w].fence.run>>}
         /\ Finish(w, "done") /\ UNCHANGED <<tries, clientReplies>>
    ELSE /\ att' = [att EXCEPT ![w].pc = "recover", ![w].conflict = FALSE]
         /\ UNCHANGED <<q, tries, pending, clientReplies, claims>>
PutTimeout(w) ==
    /\ att[w] # NoObj /\ att[w].pc = "await"
    /\ ~\E rp \in replies : rp.cid = att[w].cid
    /\ ~\E pt \in pending : pt.cid = att[w].cid
    /\ Ambiguous(w)
    /\ UNCHANGED <<root, commits, replies, control, curFence, fenceBase, adm, rds, budget>>
PutDelayTimeout(w) ==
    /\ delays < MaxDelays /\ delays' = delays + 1
    /\ att[w] # NoObj /\ att[w].pc = "await"
    /\ \E pt \in pending : pt.cid = att[w].cid
    /\ Ambiguous(w)
    /\ UNCHANGED <<root, commits, replies, control, curFence, fenceBase, adm, rds,
                   crashes, lost, takeovers, colds, replays>>

\* publish(): reload the root and search its whole chain. Absent after a
\* Conflict is NeedsRecompute, and absent after Ambiguous is Unknown (T04).
RecoverPublication(w) ==
    /\ att[w] # NoObj /\ att[w].pc = "recover"
    /\ LET a == att[w]
           l == Lookup(root, a.req)
           again == l.kind = "absent" /\ a.conflict
       IN IF l.kind = "found" THEN EndTerminal(w, l.cid)
          ELSE IF again /\ Mutant = "loser_reput_stale"
          THEN /\ att' = [att EXCEPT ![w].pc = "await"]
               /\ pending' = pending \cup {[Candidate(a) EXCEPT !.base = root]}
               /\ UNCHANGED <<q, tries, clientReplies>>
          ELSE IF again /\ Mutant = "loser_keeps_files"
          THEN /\ att' = [att EXCEPT ![w].pc = "publish", ![w].base = root]
               /\ UNCHANGED <<q, tries, pending, clientReplies>>
          ELSE IF again
          THEN /\ att' = [att EXCEPT ![w].pc = "load", ![w].base = RootInit, ![w].cid = NoCid,
                                     ![w].have = {}, ![w].rows = {}, ![w].conflict = FALSE]
               /\ UNCHANGED <<q, tries, pending, clientReplies>>
          ELSE EndUnknown(w)
    /\ UNCHANGED <<root, commits, replies, control, curFence, fenceBase, adm, rds, claims, budget>>

-----------------------------------------------------------------------------
(* Faults.                                                                 *)

LoseReply ==
    /\ lost < MaxLostReplies /\ lost' = lost + 1
    /\ \E rp \in replies : replies' = replies \ {rp}
    /\ UNCHANGED <<root, commits, pending, control, curFence, fenceBase, att, queue, rds, ghost,
                   crashes, delays, takeovers, colds, replays>>

\* A writer stops at any step, or loses the store for the rest of its attempt.
\* The queue survives on the other replicas and reissues the same identity.
CrashWriter(w) ==
    /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ att[w] # NoObj
    /\ Finish(w, "queued")
    /\ replies' = {rp \in replies : rp.cid # att[w].cid}
    /\ tries' = IF Mutant = "prefix_retry_limit" /\ Owns(att[w])
                THEN [tries EXCEPT ![att[w].req] = @ + 1] ELSE tries
    /\ UNCHANGED <<root, commits, control, curFence, fenceBase, adm, rds, ghost,
                   lost, delays, takeovers, colds, replays>>

\* A new owner reads the root for its fence. Attempts of the old one keep
\* running, and the surviving queue reissues their requests (R01).
SchedulerTakeover ==
    /\ takeovers < MaxTakeovers /\ takeovers' = takeovers + 1
    /\ curFence' = [curFence EXCEPT !.term = @ + 1] /\ fenceBase' = root
    /\ q' = [r \in Requests |-> IF q[r] = "running" THEN "queued" ELSE q[r]]
    /\ tries' = [r \in Requests |-> 0]
    /\ UNCHANGED <<store, att, adm, rds, ghost, crashes, lost, delays, colds, replays>>

\* Every worker stops. Queue copies, attempts and pins are gone, and PUTs the
\* store accepted stay pending. An operator starts a new run (R04, T05).
ColdStart ==
    /\ colds < MaxColdStarts /\ colds' = colds + 1
    /\ curFence' = [run |-> curFence.run + 1, term |-> 1] /\ fenceBase' = root
    /\ att' = [w \in Writers |-> NoObj] /\ replies' = {}
    /\ pending' = IF Mutant \in {"no_cas", "fence_outside_root", "cold_start_blind_overwrite"}
                   THEN pending ELSE {pt \in pending : pt.base = root}
    /\ q' = [r \in Requests |-> IF q[r] \in {"queued", "running"} THEN "abandoned" ELSE q[r]]
    /\ tries' = [r \in Requests |-> 0]
    /\ pins' = [rd \in Readers |-> NoObj]
    /\ seen' = [rd \in Readers |-> [p \in Parts |-> NotRead]]
    /\ UNCHANGED <<root, commits, control, adm, ghost, crashes, lost, delays, takeovers, replays>>

\* history.rs fence_cluster_catalog: one conditional PUT that changes only the
\* fence. It can be delayed like any PUT. After a 412 the owner reads again.
FencePut == [kind |-> "fence", cid |-> NoCid, base |-> fenceBase,
             new |-> [fenceBase EXCEPT !.fence = curFence], obj |-> NoObj]
FenceClusterCatalog ==
    /\ ~Fenced
    /\ ~\E pt \in pending : pt.kind = "fence" /\ pt.new.fence = curFence
    /\ IF fenceBase = root \/ Mutant = "cold_start_blind_overwrite"
       THEN pending' = pending \cup {FencePut} /\ UNCHANGED fenceBase
       ELSE fenceBase' = root /\ UNCHANGED pending
    /\ UNCHANGED <<root, commits, replies, control, curFence, att, queue, rds, ghost, budget>>

-----------------------------------------------------------------------------
(* Readers pin one root (OLAP-G01). Each reader runs one query.            *)

ReaderPin(rd) ==
    /\ pins[rd] = NoObj /\ \A p \in Parts : seen[rd][p] = NotRead
    /\ pins' = [pins EXCEPT ![rd] = root]
    /\ UNCHANGED <<store, curFence, fenceBase, att, queue, seen, ghost, budget>>
ReaderGet(rd) ==
    /\ pins[rd] # NoObj
    /\ LET p == MinOf({x \in Parts : seen[rd][x] = NotRead})
           src == IF Mutant = "unpinned_read" THEN root ELSE pins[rd]
           now == [seen[rd] EXCEPT ![p] = Rows(src, p)]
           whole == p = NumParts /\ \A x \in Parts : now[x] = now[1] /\ now[x] # Missing
       \* A finished consistent read is only remembered as finished.
       IN /\ seen' = [seen EXCEPT ![rd] = IF whole THEN [x \in Parts |-> {}] ELSE now]
          /\ pins' = IF p = NumParts THEN [pins EXCEPT ![rd] = NoObj] ELSE pins
    /\ UNCHANGED <<store, curFence, fenceBase, att, queue, ghost, budget>>

-----------------------------------------------------------------------------
(* Garbage collection. The shipped design deletes nothing (Gc = "none").   *)
(* "safe" is a candidate rule: spare the current root, pinned snapshots,   *)
(* and files whose writer's base is still the root. The mutants            *)
(* gc_ignores_pins and gc_ignores_inflight drop one clause each.           *)

\* A step deletes all files of one commit or attempt. Every property and every
\* failed read sees a missing file the same way, so the order within the set
\* does not matter.
GcDelete ==
    /\ Gc # "none"
    /\ \/ \E c \in Cids :          \* a superseded commit
            /\ commits[c] # NoObj /\ commits[c].have # {} /\ c # root.head
            /\ Mutant # "gc_ignores_pins" => \A rd \in Readers : pins[rd] # NoObj => pins[rd].head # c
            /\ commits' = [commits EXCEPT ![c].have = {}]
            /\ UNCHANGED <<att, pending>>
       \/ \E w \in Writers :       \* files of an attempt that has not submitted its root
            /\ att[w] # NoObj /\ att[w].pc = "publish" /\ att[w].have # {}
            /\ Mutant # "gc_ignores_inflight" => att[w].base # root
            /\ att' = [att EXCEPT ![w].have = {}]
            /\ UNCHANGED <<commits, pending>>
       \/ \E pt \in pending :      \* files of a root PUT in flight
            /\ pt.kind = "commit" /\ pt.obj.have # {}
            /\ Mutant # "gc_ignores_inflight" => pt.base # root
            /\ pending' = (pending \ {pt}) \cup {[pt EXCEPT !.obj.have = {}]}
            /\ UNCHANGED <<commits, att>>
    /\ UNCHANGED <<root, replies, control, curFence, fenceBase, queue, rds, ghost, budget>>

-----------------------------------------------------------------------------
Quiescent ==
    /\ \A r \in Requests : q[r] \in {"done", "abandoned"}
    /\ \A w \in Writers : att[w] = NoObj
    /\ pending = {} /\ Fenced /\ UNCHANGED vars

Next ==
    \/ \E r \in Requests : ClientReplay(r) \/ LookupReceipt(r)
    \/ \E w \in Writers, r \in Requests : ScheduleAttempt(w, r)
    \/ \E w \in Writers : \/ LoadAndLookup(w) \/ UploadStaged(w) \/ PutRoot(w) \/ ReceivePutReply(w)
                          \/ PutTimeout(w) \/ PutDelayTimeout(w) \/ RecoverPublication(w) \/ CrashWriter(w)
    \/ StoreApply \/ LoseReply \/ SchedulerTakeover \/ ColdStart \/ FenceClusterCatalog
    \/ \E rd \in Readers : ReaderPin(rd) \/ ReaderGet(rd)
    \/ GcDelete
    \/ Quiescent

Spec == Init /\ [][Next]_vars

\* Fairness and the mechanism behind each assumption:
\*   ScheduleAttempt      queue_pump re-evaluates every 100 ms and on each settlement
\*   LoadAndLookup .. RecoverPublication   the coordinator task runs to its 60 s deadline
\*   StoreApply           the store evaluates or drops every accepted PUT
\*   PutTimeout           the publication deadline (timeout_at)
\*   FenceClusterCatalog  startup and takeover retry the fence before dispatch
\* No fairness on faults, clients, receipts, readers or GC.
AttemptStep(w) == LoadAndLookup(w) \/ UploadStaged(w) \/ PutRoot(w) \/ ReceivePutReply(w)
                  \/ PutTimeout(w) \/ RecoverPublication(w)
Fairness ==
    /\ \A r \in Requests : WF_vars(\E w \in Writers : ScheduleAttempt(w, r))
    /\ \A w \in Writers : WF_vars(AttemptStep(w))
    /\ WF_vars(StoreApply) /\ WF_vars(FenceClusterCatalog)
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
Roots == [rev : 0..MaxId, head : Cids \cup {NoCid}, fence : Fences, recent : Seq(Cids)]
Objs == [req : Requests, parent : Cids \cup {NoCid}, rev : 1..MaxId, fence : Fences,
         rows : SUBSET Requests, have : SUBSET Parts]
TypeOK ==
    /\ root \in Roots /\ fenceBase \in Roots /\ control \in Fences /\ curFence \in Fences
    /\ \A c \in Cids : commits[c] = NoObj \/ commits[c] \in Objs
    /\ \A pt \in pending : /\ pt.kind \in {"commit", "fence"} /\ pt.base \in Roots /\ pt.new \in Roots
                           /\ pt.kind = "commit" => pt.cid \in Cids /\ pt.obj \in Objs
    /\ replies \subseteq [cid : Cids, ok : BOOLEAN]
    /\ \A w \in Writers : att[w] = NoObj \/
         att[w] \in [req : Requests, fence : Fences, base : Roots, cid : Cids \cup {NoCid},
                     pc : {"load", "upload", "publish", "await", "recover"},
                     have : SUBSET Parts, rows : SUBSET Requests, conflict : BOOLEAN]
    /\ q \in [Requests -> {"queued", "running", "done", "abandoned"}]
    /\ tries \in [Requests -> Nat] /\ adm \in [Requests -> Nat]
    /\ \A rd \in Readers : pins[rd] = NoObj \/ pins[rd] \in Roots
    /\ \A rd \in Readers, p \in Parts : seen[rd][p] \in (SUBSET Requests) \cup {NotRead, Missing}
    /\ \A r \in Requests : clientReplies[r] \subseteq Cids \cup {Expired, Unknown}
    /\ claims \subseteq Requests \X (0..(1 + MaxColdStarts))

\* OLAP-G01 (T01, T02): the root references only complete, existing objects.
G01_NoTornCommit == root.head # NoCid => commits[root.head] # NoObj /\ commits[root.head].have = Parts
\* OLAP-G01: a reader sees one whole committed snapshot.
G01_ReaderSnapshot ==
    \A rd \in Readers : /\ \A p \in Parts : seen[rd][p] # Missing
                        /\ \A p1, p2 \in Parts : seen[rd][p1] # NotRead /\ seen[rd][p2] # NotRead
                                                    => seen[rd][p1] = seen[rd][p2]
\* OLAP-G02 (T03): the rows are exactly the requests in the history.
G02_NoLostUpdate ==
    root.head # NoCid => commits[root.head].rows = {commits[c].req : c \in Chain(root)}
\* OLAP-G02: a root change only moves the fence, or extends the history by one.
G02_LinearHistory ==
    [][\/ root'.head = root.head /\ root'.rev = root.rev
       \/ /\ root'.rev = root.rev + 1 /\ root'.head # NoCid /\ root'.fence = root.fence
          /\ commits'[root'.head].parent = root.head]_vars
\* OLAP-G03 (T04, T05): after `not_applied` or `abandoned` for a request in a
\* run, no attempt of that run commits it.
G03_NoFalseNotApplied ==
    \A cl \in claims : \A c \in Chain(root) : ~(commits[c].req = cl[1] /\ commits[c].fence.run = cl[2])
\* OLAP-G03: every reported commit is the request's reachable decision.
G03_TerminalIsCommitted ==
    \A r \in Requests : \A c \in clientReplies[r] \ {Expired, Unknown} :
        c \in Chain(root) /\ commits[c].req = r
\* OLAP-G04: the fence never goes back.
G04_FenceMonotonic == [][FenceLeq(AuthFence, AuthFence')]_vars
\* OLAP-G04 (T05): a commit applies only under the fence its attempt was issued with.
G04_NoStaleCommit == [][root'.head # root.head /\ root'.head # NoCid
                          => commits'[root'.head].fence = AuthFence]_vars
\* OLAP-G04: a committed decision stays reachable across fences and commits.
G04_CommitStaysVisible == [][Chain(root) \subseteq {c \in Cids : c \in Chain(root)'}]_vars
\* OLAP-G05 (T06): one decision per request identity, across runs.
G05_OncePerRequest == \A c, d \in Chain(root) : commits[c].req = commits[d].req => c = d
\* OLAP-G05 (T06): every terminal result of a request is the same. Expired is
\* the one defined result of a retry past the retention window.
G05_SameResult == \A r \in Requests : Cardinality(clientReplies[r] \ {Expired}) <= 1
\* Deletion spares everything a root, a pin or an applicable PUT refers to.
GC_NeverDeletesReferenced ==
    /\ root.head # NoCid => commits[root.head].have = Parts
    /\ \A rd \in Readers : pins[rd] # NoObj /\ pins[rd].head # NoCid => commits[pins[rd].head].have = Parts
    /\ \A pt \in pending : pt.kind = "commit" /\ pt.base = root => pt.obj.have = Parts
    /\ \A w \in Writers : att[w] # NoObj /\ att[w].pc = "publish" /\ att[w].base = root => att[w].have = Parts
\* Liveness (OLAP-G03, OLAP-G05, T04): after the faults stop, every accepted
\* request of the live run gets a terminal result.
G05_Progress == \A r \in Requests : (q[r] \in {"queued", "running"}) ~> (q[r] \in {"done", "abandoned"})
=============================================================================
