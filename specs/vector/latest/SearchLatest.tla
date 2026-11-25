-------------------------- MODULE SearchLatest --------------------------
(***************************************************************************)
(* `latest` search reads (plans/oltp-vector.md, sections 3 and 4).         *)
(*                                                                         *)
(* A range is a Raft group. Raft itself is not modelled again. A range is  *)
(* its committed log, at most one uncommitted entry of its leader, the     *)
(* leader's applied index, and possibly one deposed leader that has not    *)
(* noticed. A voter that feeds a search worker may have applied any prefix *)
(* of the committed log, so one nondeterministic prefix stands for any     *)
(* number of voters. Raft's log safety, and that a lease check passes only  *)
(* while no newer leader exists, are assumed: they are existing mechanisms  *)
(* of the OLTP module, used unchanged, and clock drift enters only there.   *)
(* The check and the answer are separate steps: a leader can be deposed     *)
(* between them.                                                            *)
(*                                                                         *)
(* Properties: VEC-G01 (LatestRealTime, MonotoneLatest), VEC-G03           *)
(* (NoInvisibleRow), VEC-G06 (CommitIndependent, QueryProgress).           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Ranges, Workers, Queries,
    Singles,        \* single-row writes; each may later be deleted
    Batches,        \* insert_many across ranges
    Parts,          \* [Singles \cup Batches -> SUBSET Ranges]
    Deletable,      \* the singles a client may delete
    MaxElections, MaxCrashes, MaxLost, MaxDups, MaxPurges, MaxBarriers,
    MaxLog,         \* at least the longest log of a run
    None,           \* a model value
    Mutant

Writes == Singles \cup Batches

VARIABLES
    started,    \* writes and deletes the client has sent; it retries them
    log,        \* log[r]: committed entries, durable
    tail,       \* tail[r]: the leader's uncommitted entry, or None
    lapp,       \* lapp[r]: the leader's applied index
    ready,      \* ready[r]: the leader applied the first entry of its term
    termAt,     \* termAt[r]: committed length when the leader was elected
    old,        \* old[r]: applied index of a deposed leader that has not noticed, or None
    prepared,   \* prepared[b]: ranges whose leader applied the prepare of b
    dec,        \* dec[b]: the Status decision, durable
    acked,      \* entries acknowledged to the client
    data,       \* data[s][r]: the worker's copy of the log
    wpos,       \* wpos[s][r]: the position the worker claims
    inst,       \* inst[s][r]: a range snapshot is being installed
    pages,      \* feed pages in flight
    purged,     \* purged[r]: the log before this index is gone
    wait, fly,  \* queries waiting for, and in, the barrier of (s, r)
    bid,        \* barrier RPC identity of (s, r)
    asked,      \* asked[s][r]: the in-flight RPC has not been answered
    cap,        \* cap[s][r]: what the leader captured when it got the RPC, or None:
                \* [ci, lp, old]: committed index, applied index, and that it was deposed since
    replies,    \* barrier replies in flight
    qst, qw, qneed, qpos, qres,
    qat,        \* qat[q][r]: the position a range was served at
    qafter,     \* qafter[q]: queries that were done when q began
    elections, crashes, lost, dups, purges

wvars == <<started, log, tail, lapp, ready, termAt, old, prepared, dec, acked>>
fvars == <<data, wpos, inst, pages, purged>>
bvars == <<wait, fly, bid, asked, cap, replies>>
qvars == <<qst, qw, qneed, qpos, qres, qat, qafter>>
cvars == <<elections, crashes, lost, dups, purges>>
vars == <<wvars, fvars, bvars, qvars, cvars>>

Entry(k, w) == [k |-> k, w |-> w]
Put(w) == Entry("put", w)
Del(w) == Entry("del", w)
Has(seq, e) == \E i \in 1..Len(seq) : seq[i] = e
IndexOf(seq, e) == CHOOSE i \in 1..Len(seq) : seq[i] = e
Prefix(seq, n) == SubSeq(seq, 1, n)

\* The writes a reader of this prefix sees.
Visible(seq) ==
    {w \in Singles : Has(seq, Put(w)) /\ ~Has(seq, Del(w))} \cup
    {b \in Batches : Has(seq, Entry("prep", b)) /\ Has(seq, Entry("res", b))}

Entries ==
    {Put(w) : w \in Singles} \cup {Del(w) : w \in Deletable} \cup
    {Entry(k, b) : k \in {"prep", "res", "abt"}, b \in Batches}

TypeOK ==
    /\ started \subseteq Entries
    /\ \A r \in Ranges :
        /\ \A i \in 1..Len(log[r]) : log[r][i] \in Entries
        /\ tail[r] = None \/ tail[r] \in Entries
        /\ lapp[r] \in 0..Len(log[r])
        /\ ready[r] \in BOOLEAN
        /\ termAt[r] \in 0..Len(log[r])
        /\ old[r] = None \/ old[r] \in 0..Len(log[r])
        /\ purged[r] \in 0..Len(log[r])
    /\ \A b \in Batches : prepared[b] \subseteq Ranges /\ dec[b] \in {"none", "commit", "abort"}
    /\ acked \subseteq Entries
    /\ \A s \in Workers, r \in Ranges :
        /\ wait[s][r] \subseteq Queries /\ fly[s][r] \subseteq Queries
        /\ bid[s][r] \in 0..MaxBarriers
        /\ inst[s][r] \in BOOLEAN /\ asked[s][r] \in BOOLEAN
    /\ qst \in [Queries -> {"idle", "run", "done"}]
    /\ elections \in 0..MaxElections /\ crashes \in 0..MaxCrashes
    /\ lost \in 0..MaxLost /\ dups \in 0..MaxDups /\ purges \in 0..MaxPurges

Init ==
    /\ started = {e \in Entries : e.k \in {"put", "prep"}}
    /\ log = [r \in Ranges |-> <<>>]
    /\ tail = [r \in Ranges |-> None]
    /\ lapp = [r \in Ranges |-> 0]
    /\ ready = [r \in Ranges |-> TRUE]
    /\ termAt = [r \in Ranges |-> 0]
    /\ old = [r \in Ranges |-> None]
    /\ prepared = [b \in Batches |-> {}]
    /\ dec = [b \in Batches |-> "none"]
    /\ acked = {}
    /\ data = [s \in Workers |-> [r \in Ranges |-> <<>>]]
    /\ wpos = [s \in Workers |-> [r \in Ranges |-> 0]]
    /\ inst = [s \in Workers |-> [r \in Ranges |-> FALSE]]
    /\ pages = {}
    /\ purged = [r \in Ranges |-> 0]
    /\ wait = [s \in Workers |-> [r \in Ranges |-> {}]]
    /\ fly = [s \in Workers |-> [r \in Ranges |-> {}]]
    /\ bid = [s \in Workers |-> [r \in Ranges |-> 0]]
    /\ asked = [s \in Workers |-> [r \in Ranges |-> FALSE]]
    /\ cap = [s \in Workers |-> [r \in Ranges |-> None]]
    /\ replies = {}
    /\ qst = [q \in Queries |-> "idle"]
    /\ qw = [q \in Queries |-> None]
    /\ qneed = [q \in Queries |-> {}]
    /\ qpos = [q \in Queries |-> [r \in Ranges |-> None]]
    /\ qres = [q \in Queries |-> [r \in Ranges |-> None]]
    /\ qat = [q \in Queries |-> [r \in Ranges |-> None]]
    /\ qafter = [q \in Queries |-> {}]
    /\ elections = 0 /\ crashes = 0 /\ lost = 0 /\ dups = 0 /\ purges = 0

-----------------------------------------------------------------------------
(* OLTP writes: the direct path and the cross-range transaction.           *)

Start(e) ==
    /\ e \notin started
    /\ e.k = "del" /\ Put(e.w) \in acked
    /\ started' = started \cup {e}
    /\ UNCHANGED <<log, tail, lapp, ready, termAt, old, prepared, dec, acked, fvars, bvars, qvars, cvars>>

Propose(r, e) ==
    /\ ready[r] /\ tail[r] = None /\ ~Has(log[r], e) /\ r \in Parts[e.w]
    /\ \/ e.k \in {"put", "del"} /\ e \in started
       \/ e.k = "prep" /\ e \in started /\ dec[e.w] = "none"
       \/ e.k = "res" /\ dec[e.w] = "commit" /\ Has(log[r], Entry("prep", e.w))
       \/ e.k = "abt" /\ dec[e.w] = "abort" /\ Has(log[r], Entry("prep", e.w))
    /\ tail' = [tail EXCEPT ![r] = e]
    /\ UNCHANGED <<started, log, lapp, ready, termAt, old, prepared, dec, acked, fvars, bvars, qvars, cvars>>

\* A quorum synced the entry.
Commit(r) ==
    /\ tail[r] # None
    /\ log' = [log EXCEPT ![r] = Append(@, tail[r])]
    /\ tail' = [tail EXCEPT ![r] = None]
    /\ UNCHANGED <<started, lapp, ready, termAt, old, prepared, dec, acked, fvars, bvars, qvars, cvars>>

\* The leader applies the next entry. A prepare is acknowledged to the coordinator after its apply.
ApplyLeader(r) ==
    /\ lapp[r] < Len(log[r])
    /\ lapp' = [lapp EXCEPT ![r] = @ + 1]
    /\ LET e == log[r][lapp[r] + 1] IN
       prepared' = IF e.k = "prep" THEN [prepared EXCEPT ![e.w] = @ \cup {r}] ELSE prepared
    /\ UNCHANGED <<started, log, tail, ready, termAt, old, dec, acked, fvars, bvars, qvars, cvars>>

BecomeReady(r) ==
    /\ ~ready[r] /\ lapp[r] >= termAt[r]
    /\ ready' = [ready EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<started, log, tail, lapp, termAt, old, prepared, dec, acked, fvars, bvars, qvars, cvars>>

Decide(b) ==
    /\ dec[b] = "none" /\ Entry("prep", b) \in started
    /\ \/ prepared[b] = Parts[b] /\ dec' = [dec EXCEPT ![b] = "commit"]
       \/ dec' = [dec EXCEPT ![b] = "abort"]
    /\ UNCHANGED <<started, log, tail, lapp, ready, termAt, old, prepared, acked, fvars, bvars, qvars, cvars>>

\* Mutant commit_waits_for_search: the leader waits for a search worker before it acknowledges.
SearchHasIt(e) ==
    \A r \in Parts[e.w] : \E s \in Workers : wpos[s][r] >= IndexOf(log[r], e)

AckSingle(e) ==
    /\ e \notin acked /\ e.k \in {"put", "del"}
    /\ \A r \in Parts[e.w] : Has(log[r], e) /\ IndexOf(log[r], e) <= lapp[r]
    /\ Mutant = "commit_waits_for_search" => SearchHasIt(e)
    /\ acked' = acked \cup {e}
    /\ UNCHANGED <<started, log, tail, lapp, ready, termAt, old, prepared, dec, fvars, bvars, qvars, cvars>>

\* The weakest rule: acknowledged as soon as the commit decision is durable.
AckBatch(b) ==
    /\ Entry("prep", b) \notin acked /\ dec[b] = "commit"
    /\ acked' = acked \cup {Entry("prep", b)}
    /\ UNCHANGED <<started, log, tail, lapp, ready, termAt, old, prepared, dec, fvars, bvars, qvars, cvars>>

\* A new leader is elected. The old leader's uncommitted entry survives or not. The new leader has
\* applied any prefix. The old one may keep believing it leads.
Elect(r) ==
    /\ elections < MaxElections
    /\ elections' = elections + 1
    /\ \E keep \in BOOLEAN :
        /\ keep => tail[r] # None
        /\ LET l2 == IF keep THEN Append(log[r], tail[r]) ELSE log[r] IN
            /\ log' = [log EXCEPT ![r] = l2]
            /\ termAt' = [termAt EXCEPT ![r] = Len(l2)]
            /\ \E n \in 0..Len(log[r]) : lapp' = [lapp EXCEPT ![r] = n]
    /\ tail' = [tail EXCEPT ![r] = None]
    /\ ready' = [ready EXCEPT ![r] = FALSE]
    /\ old' = [old EXCEPT ![r] = lapp[r]]
    /\ cap' = [s \in Workers |-> [cap[s] EXCEPT ![r] =
                IF @ = None \/ @.old THEN None ELSE [@ EXCEPT !.old = TRUE]]]
    /\ asked' = [s \in Workers |-> [asked[s] EXCEPT ![r] =
                  IF cap[s][r] # None /\ cap[s][r].old THEN FALSE ELSE @]]
    /\ UNCHANGED <<started, prepared, dec, acked, fvars, wait, fly, bid, replies, qvars, crashes, lost, dups, purges>>

-----------------------------------------------------------------------------
(* Search feed.                                                            *)

\* Mutant prefix_feed_omits_delete: the voter derives changes from its current rows, which no longer hold
\* the deleted one, so the entry arrives empty.
Sent(ents) ==
    [i \in 1..Len(ents) |->
        IF Mutant = "prefix_feed_omits_delete" /\ ents[i].k = "del" THEN Entry("empty", ents[i].w) ELSE ents[i]]

\* A voter answers a tail request with committed entries after the worker's position. The voter may
\* have applied any prefix. Mutant tail_unapplied: the leader also sends its uncommitted entry.
SendPage(s, r) ==
    /\ ~inst[s][r] /\ ~\E m \in pages : m.s = s /\ m.r = r
    /\ wpos[s][r] >= purged[r]
    /\ \E n \in wpos[s][r]..Len(log[r]) :
        LET extra == IF Mutant = "tail_unapplied" /\ n = Len(log[r]) /\ tail[r] # None
                     THEN <<tail[r]>> ELSE <<>>
            ents == Sent(SubSeq(log[r], wpos[s][r] + 1, n)) \o extra IN
        /\ ents # <<>>
        /\ pages' = pages \cup {[s |-> s, r |-> r, from |-> wpos[s][r], ents |-> ents]}
    /\ UNCHANGED <<wvars, data, wpos, inst, purged, bvars, qvars, cvars>>

\* Contiguous or overlapping pages extend the copy, a gap is refused. Mutant gap_in_tail accepts one.
TakePage(m) ==
    LET have == wpos[m.s][m.r]
        end == m.from + Len(m.ents)
        fits == m.from <= have \/ Mutant = "gap_in_tail"
        new == IF m.from > have THEN m.ents ELSE SubSeq(m.ents, have - m.from + 1, Len(m.ents)) IN
    IF fits /\ end > have /\ ~inst[m.s][m.r]
    THEN /\ data' = [data EXCEPT ![m.s][m.r] = @ \o new]
         /\ wpos' = [wpos EXCEPT ![m.s][m.r] = end]
    ELSE UNCHANGED <<data, wpos>>

RecvPage ==
    /\ \E m \in pages : TakePage(m) /\ pages' = pages \ {m}
    /\ UNCHANGED <<wvars, inst, purged, bvars, qvars, cvars>>

DupPage ==
    /\ dups < MaxDups /\ dups' = dups + 1
    /\ \E m \in pages : TakePage(m)
    /\ UNCHANGED <<wvars, inst, pages, purged, bvars, qvars, elections, crashes, lost, purges>>

LosePage ==
    /\ lost < MaxLost /\ lost' = lost + 1
    /\ \E m \in pages : pages' = pages \ {m}
    /\ UNCHANGED <<wvars, data, wpos, inst, purged, bvars, qvars, elections, crashes, dups, purges>>

Purge(r) ==
    /\ purges < MaxPurges /\ purges' = purges + 1
    /\ lapp[r] > purged[r]
    /\ purged' = [purged EXCEPT ![r] = lapp[r]]
    /\ UNCHANGED <<wvars, data, wpos, inst, pages, bvars, qvars, elections, crashes, lost, dups>>

\* SnapshotRequired: drop the range, then install a snapshot of any committed prefix that covers the
\* purge. Mutant position_before_install: the position moves before the data.
StartInstall(s, r) ==
    /\ ~inst[s][r] /\ wpos[s][r] < purged[r]
    /\ inst' = [inst EXCEPT ![s][r] = TRUE]
    /\ data' = [data EXCEPT ![s][r] = <<>>]
    /\ wpos' = [wpos EXCEPT ![s][r] = IF Mutant = "position_before_install" THEN purged[r] ELSE 0]
    /\ UNCHANGED <<wvars, pages, purged, bvars, qvars, cvars>>

FinishInstall(s, r) ==
    /\ inst[s][r]
    /\ \E n \in purged[r]..Len(log[r]) :
        /\ data' = [data EXCEPT ![s][r] = Prefix(log[r], n)]
        /\ wpos' = [wpos EXCEPT ![s][r] = n]
    /\ inst' = [inst EXCEPT ![s][r] = FALSE]
    /\ UNCHANGED <<wvars, pages, purged, bvars, qvars, cvars>>

\* A worker crashes. With its disk it keeps the prefix of its checkpoint, without it nothing. Its
\* queries lose their barriers and their scans and are asked again.
CrashWorker(s) ==
    /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ \E n \in [Ranges -> 0..MaxLog] :
        LET kept(r) == IF inst[s][r] \/ n[r] > Len(data[s][r]) THEN 0 ELSE n[r] IN
        /\ \A r \in Ranges : kept(r) = n[r]
        /\ data' = [data EXCEPT ![s] = [r \in Ranges |-> Prefix(data[s][r], kept(r))]]
        /\ wpos' = [wpos EXCEPT ![s] = [r \in Ranges |-> kept(r)]]
    /\ inst' = [inst EXCEPT ![s] = [r \in Ranges |-> FALSE]]
    /\ wait' = [wait EXCEPT ![s] = [r \in Ranges |-> {}]]
    /\ fly' = [fly EXCEPT ![s] = [r \in Ranges |-> {}]]
    /\ asked' = [asked EXCEPT ![s] = [r \in Ranges |-> FALSE]]
    /\ cap' = [cap EXCEPT ![s] = [r \in Ranges |-> None]]
    /\ LET hit(q) == qst[q] = "run" /\ qw[q] = s
           clear == [r \in Ranges |-> None] IN
       /\ qpos' = [q \in Queries |-> IF hit(q) THEN clear ELSE qpos[q]]
       /\ qres' = [q \in Queries |-> IF hit(q) THEN clear ELSE qres[q]]
       /\ qat' = [q \in Queries |-> IF hit(q) THEN clear ELSE qat[q]]
    /\ UNCHANGED <<wvars, pages, purged, bid, replies, qst, qw, qneed, qafter, elections, lost, dups, purges>>

-----------------------------------------------------------------------------
(* Queries and barriers.                                                   *)

StartQuery(q, s) ==
    /\ qst[q] = "idle"
    /\ qst' = [qst EXCEPT ![q] = "run"]
    /\ qw' = [qw EXCEPT ![q] = s]
    /\ qneed' = [qneed EXCEPT ![q] = acked]
    /\ qafter' = [qafter EXCEPT ![q] = {x \in Queries : qst[x] = "done"}]
    /\ UNCHANGED <<wvars, fvars, bvars, qpos, qres, qat, cvars>>

\* A query joins a barrier that has not been sent. Mutant join_inflight_barrier joins one that has.
JoinBarrier(q, r) ==
    LET s == qw[q] IN
    /\ qst[q] = "run" /\ qpos[q][r] = None
    /\ q \notin wait[s][r] /\ q \notin fly[s][r]
    /\ IF Mutant = "join_inflight_barrier" /\ fly[s][r] # {}
       THEN fly' = [fly EXCEPT ![s][r] = @ \cup {q}] /\ UNCHANGED wait
       ELSE wait' = [wait EXCEPT ![s][r] = @ \cup {q}] /\ UNCHANGED fly
    /\ UNCHANGED <<wvars, fvars, bid, asked, cap, replies, qvars, cvars>>

SendBarrier(s, r) ==
    /\ fly[s][r] = {} /\ wait[s][r] # {} /\ bid[s][r] < MaxBarriers
    /\ fly' = [fly EXCEPT ![s][r] = wait[s][r]]
    /\ wait' = [wait EXCEPT ![s][r] = {}]
    /\ bid' = [bid EXCEPT ![s][r] = @ + 1]
    /\ asked' = [asked EXCEPT ![s][r] = TRUE]
    /\ cap' = [cap EXCEPT ![s][r] = None]
    /\ UNCHANGED <<wvars, fvars, replies, qvars, cvars>>

ReplyInFlight(s, r) == \E p \in replies : p.s = s /\ p.r = r /\ p.id = bid[s][r]

\* The RPC deadline passes with no reply on its way: the worker sends the barrier again, under a new
\* identity. Mutant no_barrier_retry never resends.
RetryBarrier(s, r) ==
    /\ fly[s][r] # {} /\ ~asked[s][r] /\ bid[s][r] < MaxBarriers
    /\ ~ReplyInFlight(s, r)
    /\ Mutant # "no_barrier_retry"
    /\ bid' = [bid EXCEPT ![s][r] = @ + 1]
    /\ asked' = [asked EXCEPT ![s][r] = TRUE]
    /\ cap' = [cap EXCEPT ![s][r] = None]
    /\ UNCHANGED <<wvars, fvars, wait, fly, replies, qvars, cvars>>

\* The deadline passes although a reply is on its way. It may arrive after the resend, so it counts
\* as a lost reply.
SpuriousRetry(s, r) ==
    /\ fly[s][r] # {} /\ ReplyInFlight(s, r) /\ bid[s][r] < MaxBarriers
    /\ lost < MaxLost /\ lost' = lost + 1
    /\ bid' = [bid EXCEPT ![s][r] = @ + 1]
    /\ asked' = [asked EXCEPT ![s][r] = TRUE]
    /\ cap' = [cap EXCEPT ![s][r] = None]
    /\ UNCHANGED <<wvars, fvars, wait, fly, replies, qvars, elections, crashes, dups, purges>>

\* Committed batches whose resolve the leader has not applied. Mutant skip_intents ignores them.
OpenIntents(seq) ==
    {b \in Batches : Has(seq, Entry("prep", b)) /\ dec[b] = "commit" /\ ~Has(seq, Entry("res", b))}

\* A leader that is not ready does not know the committed index; the mutant takes what it applied.
\* The leader gets the RPC: it applied the first entry of its term, checks its lease, and captures the
\* committed index, which a follower that feeds a worker may have applied before it.
LeaderGetsBarrier(s, r) ==
    /\ asked[s][r] /\ cap[s][r] = None
    /\ ready[r] \/ Mutant = "new_leader_skips_term_entry"
    /\ cap' = [cap EXCEPT ![s][r] = [ci |-> IF ready[r] THEN Len(log[r]) ELSE lapp[r], lp |-> lapp[r], old |-> FALSE]]
    /\ UNCHANGED <<wvars, fvars, wait, fly, bid, asked, replies, qvars, cvars>>

Reply(s, r, pos) ==
    /\ asked' = [asked EXCEPT ![s][r] = FALSE]
    /\ cap' = [cap EXCEPT ![s][r] = None]
    /\ replies' = replies \cup {[s |-> s, r |-> r, id |-> bid[s][r], pos |-> pos]}

Enough(s, r, at) ==
    /\ at >= cap[s][r].ci \/ Mutant \in {"prefix_applied_not_committed", "position_before_resolve"}
    /\ Mutant = "skip_intents" \/ OpenIntents(Prefix(log[r], at)) = {}

\* It waits until it applied that index and resolved every committed intent, and then reads and
\* returns its applied position. Mutants: prefix_applied_not_committed does not wait,
\* position_before_resolve returns the position of the capture, skip_intents.
AnswerBarrier(s, r) ==
    /\ asked[s][r] /\ cap[s][r] # None /\ ~cap[s][r].old
    /\ Enough(s, r, lapp[r])
    /\ Reply(s, r, IF Mutant = "position_before_resolve" THEN cap[s][r].lp ELSE lapp[r])
    /\ UNCHANGED <<wvars, fvars, wait, fly, bid, qvars, cvars>>

\* A deposed leader. If its lease was good when it got the RPC, it may still answer from what it
\* applied. If it got the RPC afterwards, it refuses, and the worker asks again.
\* Mutant barrier_without_lease: it answers that too.
OldLeaderAnswers(s, r) ==
    /\ asked[s][r] /\ old[r] # None
    /\ \/ /\ cap[s][r] # None /\ cap[s][r].old /\ Enough(s, r, old[r])
          /\ Reply(s, r, old[r])
       \/ /\ IF cap[s][r] = None THEN TRUE ELSE cap[s][r].old
          /\ IF Mutant = "barrier_without_lease" THEN Reply(s, r, old[r])
             ELSE /\ asked' = [asked EXCEPT ![s][r] = FALSE]
                  /\ cap' = [cap EXCEPT ![s][r] = None]
                  /\ UNCHANGED replies
    /\ old' = [old EXCEPT ![r] = None]
    /\ UNCHANGED <<started, log, tail, lapp, ready, termAt, prepared, dec, acked,
                   fvars, wait, fly, bid, qvars, cvars>>

\* A deposed leader that noticed drops the RPC it held.
OldLeaderDrops(s, r) ==
    /\ asked[s][r] /\ old[r] = None /\ cap[s][r] # None /\ cap[s][r].old
    /\ asked' = [asked EXCEPT ![s][r] = FALSE]
    /\ cap' = [cap EXCEPT ![s][r] = None]
    /\ UNCHANGED <<wvars, fvars, wait, fly, bid, replies, qvars, cvars>>

\* A reply counts only for the RPC it answers. Mutant uncorrelated_reply takes any.
RecvReply ==
    /\ \E p \in replies :
        /\ replies' = replies \ {p}
        /\ IF (p.id = bid[p.s][p.r] \/ Mutant = "uncorrelated_reply") /\ fly[p.s][p.r] # {}
           THEN /\ qpos' = [q \in Queries |-> IF q \in fly[p.s][p.r]
                                              THEN [qpos[q] EXCEPT ![p.r] = p.pos] ELSE qpos[q]]
                /\ fly' = [fly EXCEPT ![p.s][p.r] = {}]
                /\ asked' = [asked EXCEPT ![p.s][p.r] = FALSE]
                /\ cap' = [cap EXCEPT ![p.s][p.r] = None]
           ELSE UNCHANGED <<qpos, fly, asked, cap>>
    /\ UNCHANGED <<wvars, fvars, wait, bid, qst, qw, qneed, qres, qat, qafter, cvars>>

LoseReply ==
    /\ lost < MaxLost /\ lost' = lost + 1
    /\ \E p \in replies : replies' = replies \ {p}
    /\ UNCHANGED <<wvars, fvars, wait, fly, bid, asked, cap, qvars, elections, crashes, dups, purges>>

\* The worker reached the barrier's position. Mutant serve_before_catchup does not wait.
\* Mutant expose_prepared shows rows of an undecided batch.
ServeRange(q, r) ==
    LET s == qw[q] IN
    /\ qst[q] = "run" /\ qpos[q][r] # None /\ qres[q][r] = None
    /\ ~inst[s][r] \/ Mutant = "position_before_install"
    /\ wpos[s][r] >= qpos[q][r] \/ Mutant = "serve_before_catchup"
    /\ qres' = [qres EXCEPT ![q][r] =
                 Visible(data[s][r]) \cup
                 (IF Mutant = "expose_prepared"
                  THEN {b \in Batches : Has(data[s][r], Entry("prep", b))} ELSE {})]
    /\ qat' = [qat EXCEPT ![q][r] = wpos[s][r]]
    /\ UNCHANGED <<wvars, fvars, bvars, qst, qw, qneed, qpos, qafter, cvars>>

FinishQuery(q) ==
    /\ qst[q] = "run" /\ \A r \in Ranges : qres[q][r] # None
    /\ qst' = [qst EXCEPT ![q] = "done"]
    /\ UNCHANGED <<wvars, fvars, bvars, qw, qneed, qpos, qres, qat, qafter, cvars>>

\* The run is over, or a worker used up the barrier RPCs it may send.
Terminated ==
    /\ \/ \A q \in Queries : qst[q] = "done"
       \/ \E s \in Workers, r \in Ranges : bid[s][r] = MaxBarriers
    /\ UNCHANGED vars

-----------------------------------------------------------------------------

Next ==
    \/ \E e \in Entries : Start(e) \/ AckSingle(e)
    \/ \E r \in Ranges :
        \/ \E e \in Entries : Propose(r, e)
        \/ Commit(r) \/ ApplyLeader(r) \/ BecomeReady(r) \/ Elect(r) \/ Purge(r)
    \/ \E b \in Batches : Decide(b) \/ AckBatch(b)
    \/ \E s \in Workers, r \in Ranges :
        \/ SendPage(s, r) \/ StartInstall(s, r) \/ FinishInstall(s, r)
        \/ SendBarrier(s, r) \/ RetryBarrier(s, r) \/ SpuriousRetry(s, r)
        \/ LeaderGetsBarrier(s, r) \/ AnswerBarrier(s, r)
        \/ OldLeaderAnswers(s, r) \/ OldLeaderDrops(s, r)
    \/ RecvPage \/ DupPage \/ LosePage
    \/ \E s \in Workers : CrashWorker(s)
    \/ \E q \in Queries :
        \/ \E s \in Workers : StartQuery(q, s)
        \/ \E r \in Ranges : JoinBarrier(q, r) \/ ServeRange(q, r)
        \/ FinishQuery(q)
    \/ RecvReply \/ LoseReply
    \/ Terminated

\* OLTP mechanisms: client retry, Raft replication and heartbeats, the apply loop, the transaction
\* coordinator and its resolver.
OltpFairness ==
    /\ \A r \in Ranges :
        /\ WF_vars(Commit(r)) /\ WF_vars(ApplyLeader(r)) /\ WF_vars(BecomeReady(r))
        /\ \A e \in Entries : SF_vars(Propose(r, e))
    /\ \A b \in Batches : WF_vars(Decide(b)) /\ WF_vars(AckBatch(b))
    /\ \A e \in Entries : WF_vars(AckSingle(e))

\* Search mechanisms: the tail loop, the snapshot installer, the barrier sender with its RPC
\* deadline, and the scan loop.
SearchFairness ==
    /\ \A s \in Workers, r \in Ranges :
        /\ WF_vars(SendPage(s, r)) /\ WF_vars(StartInstall(s, r)) /\ WF_vars(FinishInstall(s, r))
        /\ WF_vars(SendBarrier(s, r)) /\ WF_vars(RetryBarrier(s, r))
        /\ WF_vars(LeaderGetsBarrier(s, r)) /\ WF_vars(AnswerBarrier(s, r))
        /\ WF_vars(OldLeaderAnswers(s, r)) /\ WF_vars(OldLeaderDrops(s, r))
    /\ WF_vars(RecvPage) /\ WF_vars(RecvReply)
    /\ \A q \in Queries :
        /\ \A r \in Ranges : WF_vars(JoinBarrier(q, r)) /\ WF_vars(ServeRange(q, r))
        /\ WF_vars(FinishQuery(q))

Spec == Init /\ [][Next]_vars
CommitSpec == Spec /\ OltpFairness                       \* no fairness on any search action
LiveSpec == Spec /\ OltpFairness /\ SearchFairness

-----------------------------------------------------------------------------
(* Properties.                                                             *)

\* VEC-G01: a query sees every write acknowledged before it began, and no row whose delete was
\* acknowledged before it began. A row a client has asked to delete may be there or not.
LatestRealTime ==
    \A q \in Queries, r \in Ranges :
        qres[q][r] # None =>
            /\ \A e \in qneed[q] :
                e.k \in {"put", "prep"} /\ r \in Parts[e.w] /\ Del(e.w) \notin started
                    => e.w \in qres[q][r]
            /\ \A e \in qneed[q] : e.k = "del" => e.w \notin qres[q][r]

\* VEC-G01: a query that began after another one finished reads no earlier position of a range.
MonotoneLatest ==
    \A q \in Queries, r \in Ranges : \A x \in qafter[q] :
        qat[q][r] # None /\ qat[x][r] # None => qat[q][r] >= qat[x][r]

\* VEC-G03: what a range returned is the committed prefix at its read point.
NoInvisibleRow ==
    \A q \in Queries, r \in Ranges :
        qres[q][r] # None => qat[q][r] <= Len(log[r]) /\ qres[q][r] = Visible(Prefix(log[r], qat[q][r]))

\* VEC-G06: with no help from search, every single-row write is acknowledged.
CommitIndependent == \A w \in Singles : <>(Put(w) \in acked)

\* VEC-G06: once faults stop, every query finishes.
QueryProgress == \A q \in Queries : (qst[q] = "run") ~> (qst[q] = "done")

=============================================================================
