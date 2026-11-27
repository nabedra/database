------------------------------ MODULE EventsLog ------------------------------
(***************************************************************************)
(* Nabedra Events, one partition group: the replicated log and the publish *)
(* path, under broker crashes, restarts with and without the disk, a stop *)
(* of every broker, partitions and a lossy client channel.                *)
(*                                                                         *)
(* Plan: plans/events.md, "Brokers and storage" and the publish and       *)
(* deduplication rules of "Client and delivery". Register:                *)
(* docs/testing/guarantees/events.md, EVT-G01 to EVT-G04 and EVT-G06.     *)
(* Time, retention, clocks and subscriptions are in EventsDelivery, which *)
(* abstracts this log as one committed sequence with leader failover.     *)
(*                                                                         *)
(* OpenRaft is modelled at the level of atomic protocol steps: an election *)
(* by a quorum, one replication round to one follower, a commit and apply *)
(* on the leader, and a step-down on a higher term. The term (with the    *)
(* vote folded in) and the synced log prefix are durable. Role, commit    *)
(* index and the applied state are volatile.                              *)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Items,              \* [Producers -> Seq(Seq([id, body]))]: each producer's batches, in order
    Nodes,              \* brokers, the voters of the partition group
    Producers,          \* producer clients
    MaxTerm,            \* highest Raft term
    MaxCrashes,         \* broker crash, restart with the disk
    MaxStopAll,         \* every broker stops, restart on retained disks (EV07)
    MaxWipes,           \* broker restart without its disk
    MaxPartitions,      \* network partitions between brokers
    MaxDrops,           \* lost client request or reply
    MaxDups,            \* duplicated client request: a retry racing its original
    Mutant             \* "none" in EventsLog.cfg, a mutant name in mutants/*.cfg

ASSUME Cardinality(Nodes) >= 3

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
Range(s) == {s[i] : i \in DOMAIN s}

-----------------------------------------------------------------------------
(* Requests, commands and replies. Records of a kind share one field set.  *)

NoReq == [type |-> "none", p |-> "none", k |-> 0]
PubReq(p, k) == [type |-> "publish", p |-> p, k |-> k]
Cmd(type, rq) == [type |-> type, rq |-> rq]
Blank == Cmd("blank", NoReq)
Res(type) == [type |-> type, offs |-> <<>>]
Reply(rq, r) == [rq |-> rq, type |-> r.type, offs |-> r.offs]

-----------------------------------------------------------------------------
(* The publish side of the partition state machine (partition.rs apply,    *)
(* partition/events.rs publish): offsets from 1 in commit order,           *)
(* deduplication by id and bytes, whole-batch rejection. Subscriptions,    *)
(* leases and retention are in EventsDelivery. ins is a history field:     *)
(* every id and body ever inserted.                                        *)

Init0 == [next |-> 1, evs |-> {}, ins |-> {}]
TailOff(S) == S.next - 1

RECURSIVE PubLoop(_, _, _)
PubLoop(items, i, acc) ==
    IF i > Len(items) \/ ~acc.ok THEN acc
    ELSE LET it == items[i]
             hit == {e \in acc.evs : e.id = it.id}
         IN IF \E j \in 1..(i - 1) : items[j].id = it.id
            THEN [acc EXCEPT !.ok = FALSE]
            ELSE IF hit # {} /\ Mutant # "reexecute_on_retry"
            THEN LET e == CHOOSE x \in hit : TRUE
                 IN IF e.body # it.body /\ Mutant # "dedup_ignore_body"
                    THEN [acc EXCEPT !.ok = FALSE]
                    ELSE PubLoop(items, i + 1,
                            [acc EXCEPT !.rc = Append(@, e.off),
                                        \* planted bug reuse_offset_after_dedup
                                        !.next = IF Mutant = "reuse_offset_after_dedup"
                                                 THEN e.off ELSE @])
            ELSE LET o == acc.next
                     new == [off |-> o, id |-> it.id, body |-> it.body]
                 IN PubLoop(items, i + 1,
                        [acc EXCEPT !.evs = {x \in @ : x.off # o} \cup {new},
                                    !.next = o + 1,
                                    !.rc = Append(@, o),
                                    !.ins = @ \cup {<<it.id, it.body>>}])

ApplyPublish(S, cmd) ==
    LET items == Items[cmd.rq.p][cmd.rq.k]
        acc == PubLoop(items, 1, [ok |-> TRUE, next |-> S.next, evs |-> S.evs,
                                  rc |-> <<>>, ins |-> S.ins])
        stored == [next |-> acc.next, evs |-> acc.evs, ins |-> acc.ins]
    IN IF acc.ok
       THEN [s |-> stored, r |-> [Res("published") EXCEPT !.offs = acc.rc]]
       ELSE IF Mutant = "partial_batch"
       THEN [s |-> stored, r |-> Res("conflict")]
       ELSE IF Mutant = "conflict_consumes_offsets"
       THEN [s |-> [S EXCEPT !.next = @ + Len(items)], r |-> Res("conflict")]
       ELSE [s |-> S, r |-> Res("conflict")]

Step(S, cmd) ==
    IF cmd.type = "publish" THEN ApplyPublish(S, cmd) ELSE [s |-> S, r |-> Res("done")]

-----------------------------------------------------------------------------
Leader == "leader"
Follower == "follower"


Quorums == {Q \in SUBSET Nodes : 2 * Cardinality(Q) > Cardinality(Nodes)}
LastTerm(lg) == IF lg = <<>> THEN 0 ELSE lg[Len(lg)].t
UpToDate(a, b) ==
    \/ LastTerm(a) > LastTerm(b)
    \/ LastTerm(a) = LastTerm(b) /\ Len(a) >= Len(b)
IsPrefix(a, b) == Len(a) <= Len(b) /\ a = SubSeq(b, 1, Len(a))
RECURSIVE CommonLen(_, _, _)
CommonLen(a, b, i) ==
    IF i > Len(a) \/ i > Len(b) \/ a[i] # b[i] THEN i - 1 ELSE CommonLen(a, b, i + 1)

VARIABLES
    \* durable, per broker
    term,       \* current term; the vote is folded into it
    log,        \* Raft log, entries [t, c]
    synced,     \* length of the synced log prefix
    gone,       \* restarted without its disk: hosts no replica of the group
    \* volatile, per broker
    up, role,
    commit,     \* commit index known to a leader (followers keep 0)
    sm,         \* a leader's applied state, FoldLog(Init0, log[n], commit[n]), kept to avoid refolding
    \* environment
    part,       \* partition side of each broker
    \* client channel and producers
    req, rep, pst,
    used,       \* fault budget counters
    \* history, for properties only
    clog,       \* the longest committed prefix observed, entries [e, ct]: ct is the
                \* term of the leader that committed it
    csm,        \* the state machine at the end of clog
    receipts,   \* receipts [id, body, off] of every committed publish that published
    staleRead   \* some poll that passed the read barrier missed an acknowledged publish

brokerVars == <<term, log, synced, gone, up, role, commit, sm>>
envVars == <<part>>
clientVars == <<req, rep, pst>>
ghostVars == <<clog, csm, receipts, staleRead>>
vars == <<brokerVars, envVars, clientVars, used, ghostVars>>

Alive(n) == up[n] /\ ~gone[n]
Conn(a, b) == part[a] = part[b]
Serving(l) == Alive(l) /\ role[l] = Leader

\* The first entry of the leader's term: the blank entry its election appended.
NoopIdx(l) ==
    LET idx == {i \in 1..Len(log[l]) : log[l][i].t = term[l]}
    IN IF idx = {} THEN 0 ELSE MinOf(idx)

\* A request appended by a live leader in its term and not committed yet.
Pending(m) ==
    \E l \in Nodes :
        /\ Serving(l)
        /\ \E i \in (commit[l] + 1)..Len(log[l]) :
               log[l][i].t = term[l] /\ log[l][i].c.rq = m

Waiting(m) == m \notin req /\ ~(\E r \in rep : r.rq = m) /\ ~Pending(m)

Budget(k, max) == used[k] < max
Spend(k) == used' = [used EXCEPT ![k] = @ + 1]

-----------------------------------------------------------------------------
Init ==
    /\ \E l0 \in Nodes :
           role = [n \in Nodes |-> IF n = l0 THEN Leader ELSE Follower]
    /\ term = [n \in Nodes |-> 1]
    /\ log = [n \in Nodes |-> <<>>]
    /\ synced = [n \in Nodes |-> 0]
    /\ gone = [n \in Nodes |-> FALSE]
    /\ up = [n \in Nodes |-> TRUE]
    /\ commit = [n \in Nodes |-> 0]
    /\ sm = [n \in Nodes |-> Init0]
    /\ part = [n \in Nodes |-> 0]
    /\ req = {PubReq(p, 1) : p \in Producers}
    /\ rep = {}
    /\ pst = [p \in Producers |-> 1]
    /\ used = [crash |-> 0, stop |-> 0, wipe |-> 0, part |-> 0, drop |-> 0, dup |-> 0]
    /\ clog = <<>>
    /\ csm = Init0
    /\ receipts = {}
    /\ staleRead = FALSE

-----------------------------------------------------------------------------
(* Raft (plan: "Run a Raft group per partition"; "Persist votes and log    *)
(* entries with synchronous journal writes before acknowledging            *)
(* replication"; "Committed Raft membership and current leader authority  *)
(* govern writes").                                                        *)

\* The election timer of n fires: n has not heard from a leader of its term
\* or later. Every reachable broker that may vote grants its vote (one
\* round, so a vote request is never lost alone); with a quorum, n leads and
\* appends a blank entry. Voters that led an older term are deposed.
Voters(n, t) ==
    {v \in Nodes : Alive(v) /\ Conn(n, v) /\ term[v] < t /\ UpToDate(log[n], log[v])}

\* n campaigns above every reachable term: a candidacy that a higher term
\* rejects only teaches n that term (OpenRaft, vote rejection), and those
\* failed rounds are merged into this step.
Election(n) ==
    LET t == 1 + MaxOf({term[v] : v \in {u \in Nodes : Alive(u) /\ Conn(n, u)}})
        Q == Voters(n, t)
    IN
    /\ Alive(n)
    /\ t <= MaxTerm
    /\ ~\E l \in Nodes : Serving(l) /\ Conn(n, l) /\ term[l] >= term[n]
    /\ Q \in Quorums
    /\ term' = [v \in Nodes |-> IF v \in Q THEN t ELSE term[v]]
    /\ role' = [v \in Nodes |-> IF v = n THEN Leader
                               ELSE IF v \in Q THEN Follower ELSE role[v]]
    /\ commit' = [v \in Nodes |-> IF v \in Q \ {n} THEN 0 ELSE commit[v]]
    /\ sm' = [v \in Nodes |-> IF v \in Q \ {n} THEN Init0 ELSE sm[v]]
    /\ log' = [log EXCEPT ![n] = Append(@, [t |-> t, c |-> Blank])]
    /\ synced' = [synced EXCEPT ![n] = Len(log[n]) + 1]
    /\ UNCHANGED <<gone, up, envVars, clientVars, used, ghostVars>>

\* The leader commits the longest prefix ending in its term that a quorum
\* holds durably. A follower's acknowledgement is the entry in its log
\* (logs agree on a prefix wherever they agree on an entry of one term).
\* extra: brokers counted as acknowledging without holding the entry.
Holds(lgs, v, N, e) == Len(lgs[v]) >= N /\ lgs[v][N] = e

CommitTo(lgs, l, extra) ==
    LET C == {N \in (commit[l] + 1)..Len(log[l]) :
                 /\ log[l][N].t = term[l]
                 /\ 2 * Cardinality({v \in Nodes : v \in extra \/ Holds(lgs, v, N, log[l][N])})
                        > Cardinality(Nodes)}
    IN IF C = {} THEN commit[l] ELSE MaxOf(C)

\* Apply entries i..to, collecting each entry's result.
RECURSIVE ApplyRange(_, _, _, _)
ApplyRange(S, lg, i, to) ==
    IF i > to THEN [s |-> S, out |-> <<>>]
    ELSE LET st == Step(S, lg[i].c)
             rest == ApplyRange(st.s, lg, i + 1, to)
         IN [s |-> rest.s, out |-> <<[e |-> lg[i], r |-> st.r]>> \o rest.out]

\* Commit through N on leader l, apply (AppliedState::apply) and reply to the
\* requests l appended in its term: "Reply to publishes and acks after quorum
\* commit and local apply". cm and smv carry the step's other changes. A reply
\* goes only to a producer still waiting for that request: a client that moved
\* on is not listening.
CommitThrough(l, N, cm, smv) ==
    IF N <= commit[l]
    THEN /\ commit' = cm /\ sm' = smv
         /\ UNCHANGED <<rep, ghostVars>>
    ELSE LET lg == log[l]
             ap == ApplyRange(sm[l], lg, commit[l] + 1, N)
             pubs == {j \in DOMAIN ap.out : ap.out[j].e.c.type = "publish"
                                            /\ ap.out[j].r.type = "published"}
             Bat(j) == Items[ap.out[j].e.c.rq.p][ap.out[j].e.c.rq.k]
             rcpts == UNION {{[id |-> Bat(j)[x].id, body |-> Bat(j)[x].body,
                               off |-> ap.out[j].r.offs[x]] : x \in DOMAIN Bat(j)} : j \in pubs}
             mine == {j \in DOMAIN ap.out : ap.out[j].e.t = term[l]
                                            /\ ap.out[j].e.c.type = "publish"
                                            /\ pst[ap.out[j].e.c.rq.p] = ap.out[j].e.c.rq.k}
         IN /\ commit' = [cm EXCEPT ![l] = N]
            /\ sm' = [smv EXCEPT ![l] = ap.s]
            /\ rep' = rep \cup {Reply(ap.out[j].e.c.rq, ap.out[j].r) : j \in mine}
            /\ IF N > Len(clog)
               THEN /\ clog' = clog \o [k \in 1..(N - Len(clog)) |->
                                          [e |-> lg[Len(clog) + k], ct |-> term[l]]]
                    /\ csm' = ap.s
                    /\ receipts' = receipts \cup rcpts
               ELSE UNCHANGED <<clog, csm, receipts>>
            /\ UNCHANGED staleRead

\* RaftNetwork::append_entries and LogStore::append: one replication round to
\* f. The follower syncs the entries before it acknowledges (log.rs append);
\* the leader then advances its commit index and applies.
\*
\* Mutant ack_unreachable_appends (planted bug, raft/network.rs): an
\* unreachable follower's append counts as acknowledged.
\* Mutant stop_replication_after_commit: the leader stops replicating to a
\* follower once its entries have a quorum.
Reachable(l, f) == Alive(f) /\ Conn(l, f)

HandleAppendEntries(l, f) ==
    LET ok == Reachable(l, f)
        lgs == IF ok THEN [log EXCEPT ![f] = log[l]] ELSE log
        N == CommitTo(lgs, l, IF ok THEN {} ELSE {f})
    IN
    /\ Serving(l) /\ f # l
    /\ ok \/ Mutant = "ack_unreachable_appends"
    /\ IF ok THEN /\ term[f] <= term[l]
                  /\ log[f] # log[l] \/ term[f] # term[l] \/ role[f] # Follower
       ELSE N > commit[l]
    /\ Mutant = "stop_replication_after_commit" => (role[f] = Leader \/ N > commit[l])
    /\ log' = lgs
    /\ CommitThrough(l, N, [commit EXCEPT ![f] = IF ok THEN 0 ELSE @],
                     [sm EXCEPT ![f] = IF ok THEN Init0 ELSE @])
    /\ term' = [term EXCEPT ![f] = IF ok THEN term[l] ELSE @]
    /\ role' = [role EXCEPT ![f] = IF ok THEN Follower ELSE @]
    /\ synced' = [synced EXCEPT ![f] =
                     IF ~ok THEN @
                     ELSE IF Mutant = "ack_before_fsync"
                     THEN Min(synced[f], CommonLen(log[f], log[l], 1))
                     ELSE Len(log[l])]
    /\ UNCHANGED <<gone, up, envVars, req, pst, used>>

\* A heartbeat reply carries a higher term: the leader steps down.
StepDown(l, v) ==
    /\ Serving(l) /\ Alive(v) /\ Conn(l, v) /\ term[v] > term[l]
    /\ term' = [term EXCEPT ![l] = term[v]]
    /\ role' = [role EXCEPT ![l] = Follower]
    /\ commit' = [commit EXCEPT ![l] = 0]
    /\ sm' = [sm EXCEPT ![l] = Init0]
    /\ UNCHANGED <<log, synced, gone, up, envVars, clientVars, used, ghostVars>>

-----------------------------------------------------------------------------
(* Request handling on the leader: http::publish, PublishQueue::drain,     *)
(* Group::write_coalesced. The endpoint forwards to the cached leader     *)
(* route, so a deposed leader that still believes it leads can take a     *)
(* request.                                                               *)

AppendCmd(l, cmd) ==
    /\ log' = [log EXCEPT ![l] = Append(@, [t |-> term[l], c |-> cmd])]
    /\ synced' = [synced EXCEPT ![l] = Len(log[l]) + 1]

\* The network may duplicate the request (budgeted): it is served and kept.
HandlePublish(l, m, keep) ==
    /\ Serving(l) /\ m \in req
    /\ keep => Budget("dup", MaxDups)
    /\ AppendCmd(l, Cmd("publish", m))
    /\ IF keep THEN req' = req /\ Spend("dup") ELSE req' = req \ {m} /\ UNCHANGED used
    /\ UNCHANGED <<term, gone, up, role, commit, sm, envVars, rep, pst, ghostVars>>

\* Group::read's barrier (ensure_linearizable): a quorum confirms the leader,
\* and the read waits until the blank entry of its term is applied.
ReadReady(l) ==
    IF Mutant = "poll_local_read" THEN TRUE
    ELSE /\ commit[l] >= NoopIdx(l)
         /\ \E Q \in Quorums :
                /\ l \in Q
                /\ \A v \in Q : Alive(v) /\ Conn(l, v) /\ term[v] <= term[l]

\* http::poll on leader l: Group::read passes the barrier, then reads the
\* applied state (subscriptions::read, events::page). The page's tail is what
\* the consumer sees; a read that misses an acknowledged publish is recorded.
HandlePoll(l) ==
    /\ Serving(l) /\ ReadReady(l)
    /\ staleRead' = (staleRead \/ TailOff(sm[l]) < TailOff(csm))
    /\ UNCHANGED <<brokerVars, envVars, clientVars, used, clog, csm, receipts>>

-----------------------------------------------------------------------------
(* Producers retry with the same batch only when stuck: no copy in flight, *)
(* no reply in flight, nothing pending at a live leader. A retry racing    *)
(* its original is the duplication fault.                                  *)

Sending(p) == pst[p] <= Len(Items[p])

ProducerRetry(p) ==
    /\ Mutant # "no_retry"
    /\ Sending(p) /\ Waiting(PubReq(p, pst[p]))
    /\ req' = req \cup {PubReq(p, pst[p])}
    /\ UNCHANGED <<brokerVars, envVars, rep, pst, used, ghostVars>>

\* A definitive reply (receipts or conflict) moves to the next batch.
ProducerReceive(p, r) ==
    /\ Sending(p) /\ r \in rep /\ r.rq = PubReq(p, pst[p])
    /\ rep' = rep \ {r}
    /\ pst' = [pst EXCEPT ![p] = @ + 1]
    /\ req' = IF pst[p] + 1 <= Len(Items[p]) THEN req \cup {PubReq(p, pst[p] + 1)} ELSE req
    /\ UNCHANGED <<brokerVars, envVars, used, ghostVars>>

-----------------------------------------------------------------------------
(* Faults, each with a budget.                                             *)

Crashed(n) ==
    /\ role' = [role EXCEPT ![n] = Follower]
    /\ commit' = [commit EXCEPT ![n] = 0]
    /\ sm' = [sm EXCEPT ![n] = Init0]

\* Crash: volatile state resets and only the synced log survives. Mutant
\* volatile_vote: the vote is not durable.
Crash(n) ==
    /\ Alive(n) /\ Budget("crash", MaxCrashes)
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ Crashed(n)
    /\ log' = [log EXCEPT ![n] = SubSeq(@, 1, synced[n])]
    /\ term' = IF Mutant = "volatile_vote"
               THEN [term EXCEPT ![n] = LastTerm(SubSeq(log[n], 1, synced[n]))]
               ELSE term
    /\ Spend("crash")
    /\ UNCHANGED <<synced, gone, envVars, clientVars, ghostVars>>

\* Every broker stops and restarts on its retained disk ("Stop every Events
\* broker and recover committed state"), as one step. Restarts that leave a
\* broker down are the Crash fault.
StopAll ==
    /\ Budget("stop", MaxStopAll)
    /\ \E n \in Nodes : Alive(n)
    /\ role' = [n \in Nodes |-> Follower]
    /\ commit' = [n \in Nodes |-> 0]
    /\ sm' = [n \in Nodes |-> Init0]
    /\ log' = [n \in Nodes |-> SubSeq(log[n], 1, synced[n])]
    /\ term' = IF Mutant = "volatile_vote"
               THEN [n \in Nodes |-> LastTerm(SubSeq(log[n], 1, synced[n]))]
               ELSE term
    /\ up' = [n \in Nodes |-> TRUE]   \* a broker without its disk restarts empty too
    /\ Spend("stop")
    /\ UNCHANGED <<synced, gone, envVars, clientVars, ghostVars>>

\* The supervisor restarts a broker on its disk; Control::reopen opens every
\* stored group.
Restart(n) ==
    /\ ~up[n] /\ ~gone[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<term, log, synced, gone, role, commit, sm, envVars, clientVars,
                   used, ghostVars>>

\* Restart without the disk (a crash and a restart on an empty volume under the
\* same stable broker ID, as one step). The group is a partition group or the
\* metadata group: both run this Raft protocol, and only their reopen rule
\* differs. Plan rule (design review, EVENTS-1): a broker never votes or counts
\* toward a commit in a group from an empty volume under its old membership.
\* So the broker stays out of the group (gone) until ReAdd. Partition groups do
\* this in shipped code (reopen opens only stored groups; peers' appends find no
\* group).
\* Mutant prefix_empty_disk_rejoins_voter: it rejoins as the old voter with no
\* log and no vote, which is what Control::reopen does for the metadata group
\* (control/operate.rs:795).
RestartWithoutDisk(n) ==
    /\ Alive(n) /\ Budget("wipe", MaxWipes)
    /\ log' = [log EXCEPT ![n] = <<>>]
    /\ synced' = [synced EXCEPT ![n] = 0]
    /\ term' = [term EXCEPT ![n] = 0]
    /\ role' = [role EXCEPT ![n] = Follower]
    /\ commit' = [commit EXCEPT ![n] = 0]
    /\ sm' = [sm EXCEPT ![n] = Init0]
    /\ gone' = IF Mutant = "prefix_empty_disk_rejoins_voter" THEN gone
               ELSE [gone EXCEPT ![n] = TRUE]
    /\ Spend("wipe")
    /\ UNCHANGED <<up, envVars, clientVars, ghostVars>>

\* The way back for a broker that lost its disk (EVENTS-1 fix, the operator's
\* move_metadata or move_partition): a membership change first removes its old
\* identity, it joins as a learner and catches up from leader l, and a second
\* membership change promotes it. Removing the old identity commits only with a
\* majority of the other voters, so the step needs such a majority Q that has
\* accepted l's term and log (a promotion that commits implies Q holds it).
\* After the step the broker is a voter that holds l's log. When a majority of
\* the voters lost their disks no such Q exists: the group stays unavailable and
\* recovery is the manual backup restore into a fresh cluster (plan, "Cloud and
\* restore"). The operator drives it, so it carries no fairness.
ReAdd(l, n) ==
    /\ gone[n] /\ up[n] /\ Serving(l) /\ Conn(l, n)
    \* Mutant readd_without_removal: the broker is promoted from whatever leader
    \* it reaches, without first removing its old identity.
    /\ Mutant = "readd_without_removal" \/ \E Q \in SUBSET (Nodes \ {n}) :
           /\ 2 * Cardinality(Q) > Cardinality(Nodes) - 1
           /\ l \in Q
           /\ \A v \in Q : Alive(v) /\ Conn(l, v) /\ term[v] = term[l] /\ log[v] = log[l]
    /\ commit[l] >= NoopIdx(l)
    /\ gone' = [gone EXCEPT ![n] = FALSE]
    /\ log' = [log EXCEPT ![n] = log[l]]
    /\ synced' = [synced EXCEPT ![n] = Len(log[l])]
    /\ term' = [term EXCEPT ![n] = term[l]]
    /\ UNCHANGED <<up, role, commit, sm, envVars, clientVars, used, ghostVars>>

Partition(S) ==
    /\ Budget("part", MaxPartitions)
    /\ Cardinality(S) = 1   \* isolating one broker; its complement is the same cut
    /\ part' = [n \in Nodes |-> IF n \in S THEN 1 ELSE 0]
    /\ Spend("part")
    /\ UNCHANGED <<brokerVars, clientVars, ghostVars>>

Heal ==
    /\ \E n \in Nodes : part[n] # 0
    /\ part' = [n \in Nodes |-> 0]
    /\ UNCHANGED <<brokerVars, clientVars, used, ghostVars>>

DropRequest(m) ==
    /\ m \in req /\ Budget("drop", MaxDrops)
    /\ req' = req \ {m}
    /\ Spend("drop")
    /\ UNCHANGED <<brokerVars, envVars, rep, pst, ghostVars>>

DropReply(r) ==
    /\ r \in rep /\ Budget("drop", MaxDrops)
    /\ rep' = rep \ {r}
    /\ Spend("drop")
    /\ UNCHANGED <<brokerVars, envVars, req, pst, ghostVars>>

\* Every producer has its answers and the cluster has converged: the run is
\* over (explicit terminal stuttering, so deadlock checking stays on).
\* A broker at MaxTerm is the end of the bounded run too: no later election
\* is in the model, so a state with no leader there would otherwise read as a
\* deadlock of the protocol rather than of the bound.
Done ==
    /\ \/ \A p \in Producers : ~Sending(p)
       \/ \E n \in Nodes : term[n] = MaxTerm
    /\ UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ \E n \in Nodes : Election(n)
    \/ \E l, f \in Nodes : HandleAppendEntries(l, f)
    \/ \E l, v \in Nodes : StepDown(l, v)
    \/ \E l \in Nodes : HandlePoll(l)
    \/ \E l \in Nodes, m \in req, keep \in BOOLEAN : HandlePublish(l, m, keep)
    \/ \E p \in Producers : ProducerRetry(p) \/ \E r \in rep : ProducerReceive(p, r)
    \/ \E r \in rep : DropReply(r)
    \/ \E m \in req : DropRequest(m)
    \/ \E n \in Nodes : Crash(n) \/ Restart(n) \/ RestartWithoutDisk(n)
    \/ \E l, n \in Nodes : ReAdd(l, n)
    \/ StopAll
    \/ \E S \in SUBSET Nodes : Partition(S)
    \/ Heal
    \/ Done

\* Each fairness assumption names its mechanism. Faults and the duplicate
\* (keep) branch of a handler carry no fairness.
Fairness ==
    \* OpenRaft election timer (election_timeout 450 to 850 ms)
    /\ \A n \in Nodes : WF_vars(Election(n))
    \* OpenRaft replication and heartbeats (heartbeat_interval 120 ms)
    /\ \A l, f \in Nodes : WF_vars(HandleAppendEntries(l, f)) /\ WF_vars(StepDown(l, f))
    \* HTTP publish handler on the leader, reached through route refresh
    \* (per producer: each HTTP request has its own handler task)
    /\ \A l \in Nodes, p \in Producers :
           WF_vars(\E m \in req : m.p = p /\ HandlePublish(l, m, FALSE))
    \* SDK retry timer and response handling
    /\ \A p \in Producers : WF_vars(ProducerRetry(p))
                            /\ WF_vars(\E r \in rep : ProducerReceive(p, r))
    \* Supervisor restarts, the partition heals
    /\ \A n \in Nodes : WF_vars(Restart(n))
    /\ WF_vars(Heal)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
TypeOK ==
    /\ term \in [Nodes -> 0..MaxTerm]
    /\ \A n \in Nodes : \A i \in 1..Len(log[n]) :
           /\ log[n][i].t \in 1..MaxTerm
           /\ log[n][i].c.type \in {"blank", "publish"}
    /\ synced \in [Nodes -> Nat] /\ \A n \in Nodes : synced[n] <= Len(log[n])
    /\ gone \in [Nodes -> BOOLEAN] /\ up \in [Nodes -> BOOLEAN]
    /\ role \in [Nodes -> {Leader, Follower}]
    /\ commit \in [Nodes -> Nat] /\ \A n \in Nodes : commit[n] <= Len(log[n])
    /\ part \in [Nodes -> {0, 1}]
    /\ req \subseteq {PubReq(p, k) : p \in Producers, k \in 1..3}
    /\ \A r \in rep : r.rq \in req \cup {PubReq(p, k) : p \in Producers, k \in 1..3}
                      /\ r.type \in {"published", "conflict"}
    /\ pst \in [Producers -> 1..4]
    /\ \A k \in DOMAIN used : used[k] \in Nat

-----------------------------------------------------------------------------
(* Safety properties, named after the register rows they check.           *)

\* EVT-G01: a majority-acknowledged publish survives. Every leader of the
\* acknowledging term or a later one holds the acknowledged entry.
G01_AckedPublishSurvives ==
    \A i \in DOMAIN clog : \A l \in Nodes :
        (Serving(l) /\ term[l] >= clog[i].ct) => (Len(log[l]) >= i /\ log[l][i] = clog[i].e)

\* EVT-G01, EVT-G02: each broker's committed prefix is a prefix of the one
\* committed log; no committed entry is lost or replaced.
G01_CommittedPrefixesAgree ==
    \A n \in Nodes :
        LET c == SubSeq(log[n], 1, commit[n]) IN
        Len(c) <= Len(clog) /\ \A i \in DOMAIN c : c[i] = clog[i].e

\* EVT-G02: offsets start at 1, stay contiguous and are never reassigned.
G02_OffsetsStable ==
    \A a, b \in receipts : a.off = b.off => (a.id = b.id /\ a.body = b.body)

G02_Contiguous ==
    /\ {e.off : e \in csm.evs} = 1..(csm.next - 1)
    /\ \A e, f \in csm.evs : e.id = f.id => e.off = f.off

\* EVT-G03: a retried publish with the same UUID and body returns its original
\* offsets while the original is retained (nothing expires in this spec).
G03_RetryReturnsOriginal ==
    \A a, b \in receipts :
        (a.id = b.id /\ a.body = b.body) => a.off = b.off

\* EVT-G04: a batch applies whole or not at all.
\* Constant-level, so TLC evaluates it once.
OwnItems ==
    [p \in Producers |->
        {<<it.id, it.body>> : it \in UNION {Range(b) : b \in Range(Items[p])}}
        \ UNION {{<<it.id, it.body>> : it \in UNION {Range(b) : b \in Range(Items[q])}} :
                    q \in Producers \ {p}}]

G04_BatchWhole ==
    \A p \in Producers : OwnItems[p] \subseteq csm.ins \/ OwnItems[p] \cap csm.ins = {}

\* EVT-G06: a leader that passes the read barrier reads every acknowledged
\* publish (a quorum-confirmed, linearizable poll); a minority cannot.
G06_ReadIndexSafe == ~staleRead

Safety ==
    /\ G01_AckedPublishSurvives /\ G01_CommittedPrefixesAgree
    /\ G02_OffsetsStable /\ G02_Contiguous /\ G03_RetryReturnsOriginal
    /\ G04_BatchWhole /\ G06_ReadIndexSafe

-----------------------------------------------------------------------------
(* Liveness after the faults stop (fault budgets are finite).              *)

\* EVT-G01, EVT-G03: every producer gets a definitive reply to every batch,
\* and every acknowledged publish ends in the committed log of every live
\* replica.
L_PublishCompletes == <>(\A p \in Producers : ~Sending(p))

L_Replicated ==
    <>[](\A i \in DOMAIN clog : \A n \in Nodes :
            Alive(n) => (Len(log[n]) >= i /\ log[n][i] = clog[i].e))

=============================================================================
