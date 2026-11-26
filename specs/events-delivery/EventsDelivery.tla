---------------------------- MODULE EventsDelivery ----------------------------
(***************************************************************************)
(* Nabedra Events, one partition: delivery, ordering, acknowledgement,     *)
(* retention and replay for one subscription with a producer and two      *)
(* consumers (a holder and a standby).                                     *)
(*                                                                         *)
(* Plan: plans/events.md, "Client and delivery" (claims, leases, poll,     *)
(* delivery tokens, prefix acks, 410 checkpoint_expired) and the retention *)
(* rules of "Brokers and storage". Register: EVT-G02, EVT-G03, EVT-G05,   *)
(* EVT-G06 and EVT-G08.                                                    *)
(*                                                                         *)
(* The partition's Raft group is abstracted to what EventsLog shows about  *)
(* it: one committed command sequence that survives the modelled faults,  *)
(* applied in order. The current leader holds an ordered tail of appended *)
(* but uncommitted commands. A leader failover (a crash with or without   *)
(* the disk, a stop of every broker, or a partition that isolates the     *)
(* leader) commits a prefix of that tail without replies (it had reached  *)
(* a quorum) and loses the rest. Each command is stamped with the clock   *)
(* of the broker that took the HTTP request (http::now_ms).               *)
(***************************************************************************)
EXTENDS EventsPartition, TLC

CONSTANTS
    Nodes,              \* brokers: clock domains and leader candidates
    Producers,
    Consumers,          \* consumers of the one subscription, which starts at earliest
    MaxTime,            \* last tick of true time
    MaxClaims,          \* claim ids per consumer
    MaxFailovers,       \* leader crash, with or without its disk, or every broker stops
    MaxPartitions,      \* the leader is isolated in a minority and a new leader takes over
    MaxDrops,           \* lost client request or reply
    MaxDups,            \* duplicated client request: a retry racing its original
    MaxClockJumps,      \* a broker clock jumps one tick forward or back, within MaxDrift
    MaxDrift,           \* the assumed bound on a broker clock's error, in ticks
    MaxConsumerCrashes, \* a consumer dies holding its lease
    Variant             \* "none", or a candidate design fix under review

ASSUME Cardinality(Nodes) >= 3 /\ PageLimit >= 1 /\ LeaseTtl >= 1

VARIABLES
    sm,         \* the state machine after the committed log (durable, replicated)
    ldr,        \* the current leader
    pend,       \* commands the leader appended and has not committed, in log order
    stale,      \* an isolated former leader and its applied state (poll_local_read only)
    now,        \* true time
    skew,       \* broker n's clock reads now - skew[n]; negative is ahead
    req, rep,   \* client channel
    pst,        \* producer: index of the batch it is sending
    cst,        \* consumer state
    used,       \* fault budget counters
    \* history, for properties only
    receipts,   \* publish receipts sent: [id, body, off]
    seen,       \* every (offset, id, body) a client was told
    delivered,  \* offsets a consumer received in a page
    expiredIds, \* ids removed by retention
    maxAckCkpt, \* highest checkpoint in an ack reply sent
    maxRcpt,    \* highest offset in a publish receipt sent
    lastRead,   \* the last poll's read, with the bounds it must respect
    fence,      \* a poll for a superseded generation got a page
    saw410      \* some consumer received checkpoint_expired

logVars == <<sm, ldr, pend, stale>>
envVars == <<now, skew>>
clientVars == <<req, rep, pst, cst>>
ghostVars == <<receipts, seen, delivered, expiredIds, maxAckCkpt, maxRcpt, lastRead,
               fence, saw410>>
vars == <<logVars, envVars, clientVars, used, ghostVars>>

\* Broker n's clock: true time with an error of at most MaxDrift (the plan
\* monitors skew; it states no bound, so the spec fixes one tick).
Clock(n) == IF now >= skew[n] THEN now - skew[n] ELSE 0
Stamps == {Clock(n) : n \in Nodes}
NoStale == [on |-> FALSE, b |-> "none", sm |-> Init0]

Pending(m) == \E i \in 1..Len(pend) : pend[i].rq = m
Waiting(m) == m \notin req /\ ~(\E r \in rep : r.rq = m) /\ ~Pending(m)
Budget(k, max) == used[k] < max
Spend(k) == used' = [used EXCEPT ![k] = @ + 1]

Init ==
    /\ sm = Init0
    /\ ldr \in Nodes
    /\ pend = <<>>
    /\ stale = NoStale
    /\ now = 0
    /\ skew = [n \in Nodes |-> 0]
    /\ req = {PubReq(p, 1) : p \in Producers}
    /\ rep = {}
    /\ pst = [p \in Producers |-> 1]
    /\ cst = [c \in Consumers |-> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> NoReq]]
    /\ used = [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 0, ccrash |-> 0]
    /\ receipts = {}
    /\ seen = {}
    /\ delivered = {}
    /\ expiredIds = {}
    /\ maxAckCkpt = 0
    /\ maxRcpt = 0
    /\ lastRead = [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0]
    /\ fence = FALSE
    /\ saw410 = FALSE

-----------------------------------------------------------------------------
(* The log                                                                  *)

\* Ghost updates for one applied command and its reply.
Observe(cmd, r, S1) ==
    LET rq == cmd.rq
        bat == IF cmd.type = "publish" THEN Items[rq.p][rq.k] ELSE <<>>
        rc == IF cmd.type = "publish" /\ r.type = "published"
              THEN {[id |-> bat[x].id, body |-> bat[x].body, off |-> r.offs[x]] :
                       x \in DOMAIN bat}
              ELSE {}
    IN /\ receipts' = receipts \cup rc
       /\ seen' = seen \cup rc
       /\ maxRcpt' = MaxOf({maxRcpt} \cup {x.off : x \in rc})
       /\ maxAckCkpt' = IF r.type = "ckpt" THEN Max(maxAckCkpt, r.off) ELSE maxAckCkpt
       /\ expiredIds' = expiredIds \cup {x.id : x \in S1.removed}

\* The leader applies the next committed command (AppliedState::apply) and
\* replies: "Reply to publishes and acks after quorum commit and local apply".
CommitNext ==
    /\ pend # <<>>
    /\ LET cmd == Head(pend)
           st == Step(sm, cmd)
       IN /\ sm' = st.s
          /\ pend' = Tail(pend)
          /\ rep' = IF cmd.rq # NoReq THEN rep \cup {Reply(cmd.rq, st.r)} ELSE rep
          /\ Observe(cmd, st.r, st.s)
    /\ UNCHANGED <<ldr, stale, envVars, req, pst, cst, used, delivered, lastRead, fence,
                   saw410>>

\* Apply a prefix of the old leader's tail without replies (the new leader
\* commits entries that reached a quorum; nobody waits for their replies).
RECURSIVE ApplyAll(_, _)
ApplyAll(S, cmds) == IF cmds = <<>> THEN S ELSE ApplyAll(Step(S, Head(cmds)).s, Tail(cmds))

FailoverTo(k, b) ==
    LET S1 == ApplyAll(sm, SubSeq(pend, 1, k)) IN
    /\ sm' = S1
    /\ pend' = <<>>
    /\ ldr' = b
    /\ expiredIds' = expiredIds \cup {x.id : x \in S1.removed}
    /\ UNCHANGED <<receipts, seen, maxRcpt, maxAckCkpt>>

\* A leader crash (with or without its disk) or a stop of every broker.
LeaderFails(k, b) ==
    /\ Budget("fail", MaxFailovers) /\ k \in 0..Len(pend) /\ b \in Nodes
    /\ FailoverTo(k, b)
    /\ Spend("fail")
    /\ UNCHANGED <<stale, envVars, clientVars, delivered, lastRead, fence, saw410>>

\* A partition isolates the leader; the majority elects b. The old leader keeps
\* its applied state and still believes it leads until the partition heals.
LeaderIsolated(k, b) ==
    /\ Budget("part", MaxPartitions) /\ k \in 0..Len(pend) /\ b \in Nodes /\ b # ldr
    /\ FailoverTo(k, b)
    /\ stale' = IF Mutant = "poll_local_read" THEN [on |-> TRUE, b |-> ldr, sm |-> sm]
                ELSE stale
    /\ Spend("part")
    /\ UNCHANGED <<envVars, clientVars, delivered, lastRead, fence, saw410>>

Heal ==
    /\ stale.on
    /\ stale' = NoStale
    /\ UNCHANGED <<sm, ldr, pend, envVars, clientVars, used, ghostVars>>

-----------------------------------------------------------------------------
(* Request handling on the leader (http.rs, forward.rs, Group::write).     *)

Push(cmd) == pend' = Append(pend, cmd)

Consume(m, keep) ==
    IF keep THEN req' = req /\ Spend("dup") ELSE req' = req \ {m} /\ UNCHANGED used

\* Publish, claim and ack: stamp now_ms on the broker that took the request and
\* append through the leader.
HandleWrite(m, keep, t) ==
    /\ m \in req /\ m.type \in {"publish", "claim", "ack"} /\ t \in Stamps
    /\ keep => Budget("dup", MaxDups)
    /\ Push(Cmd(m.type, m, t, 0))
    /\ Consume(m, keep)
    /\ UNCHANGED <<sm, ldr, stale, envVars, rep, pst, cst, ghostVars>>

\* Control::retain on the leader: append a retention floor from its clock when
\* the oldest retained event is older than the cutoff.
RetainTick ==
    LET cut == Clock(ldr) - RetainAfter IN
    /\ \E e \in EventAt(sm, sm.earliest) : e.pt < cut
    /\ ~\E i \in 1..Len(pend) : pend[i].type = "retain"
    /\ Push(Cmd("retain", NoReq, 0, cut))
    /\ UNCHANGED <<sm, ldr, stale, envVars, clientVars, used, ghostVars>>

\* The operator answers 410 checkpoint_expired with ResetExpired, an explicit
\* metadata intent the controller runs on the partition leader.
OperatorReset ==
    /\ saw410
    /\ sm.ckpt < FirstCkpt(sm)
    /\ ~\E i \in 1..Len(pend) : pend[i].type = "reset"
    /\ Push(Cmd("reset", NoReq, 0, 0))
    /\ UNCHANGED <<sm, ldr, stale, envVars, clientVars, used, ghostVars>>

-----------------------------------------------------------------------------
(* Clients. A client retries only when stuck: no copy in flight, no reply  *)
(* in flight, nothing pending at the leader. A retry racing its original  *)
(* is the duplication fault.                                               *)

Sending(p) == pst[p] <= Len(Items[p])

ProducerRetry(p) ==
    /\ Sending(p) /\ Waiting(PubReq(p, pst[p]))
    /\ req' = req \cup {PubReq(p, pst[p])}
    /\ UNCHANGED <<logVars, envVars, rep, pst, cst, used, ghostVars>>

ProducerReceive(p) ==
    \E r \in rep :
        /\ Sending(p) /\ r.rq = PubReq(p, pst[p])
        /\ rep' = rep \ {r}
        /\ pst' = [pst EXCEPT ![p] = @ + 1]
        /\ req' = IF pst[p] < Len(Items[p]) THEN req \cup {PubReq(p, pst[p] + 1)} ELSE req
        /\ UNCHANGED <<logVars, envVars, cst, used, ghostVars>>

\* The consumer loop of the SDK: claim, poll, process, ack a prefix.
SendClaim(c) ==
    /\ cst[c].ph = "idle"
    /\ cst' = [cst EXCEPT ![c].ph = "wait", ![c].cur = ClaimReq(c, cst[c].cid)]
    /\ req' = req \cup {ClaimReq(c, cst[c].cid)}
    /\ UNCHANGED <<logVars, envVars, rep, pst, used, ghostVars>>

\* A lost lease: claim again with a new claim id, or stop once ids run out.
LoseLease(c) ==
    IF cst[c].cid < MaxClaims
    THEN [ph |-> "idle", cid |-> cst[c].cid + 1, gen |-> 0, cur |-> NoReq]
    ELSE [ph |-> "retired", cid |-> cst[c].cid, gen |-> 0, cur |-> NoReq]

Ready(c, g) == [ph |-> "ready", cid |-> cst[c].cid, gen |-> g, cur |-> NoReq]

\* http::poll (Group::read confirms leadership with a quorum, waits for the
\* applied index, then reads the committed state) as one step: the consumer
\* blocks on its own response, so a lost or reordered poll reply only repeats
\* the poll. Mutant poll_local_read: an isolated former leader answers from its
\* own applied state. Then process the page and ack a prefix of it
\* (subscriptions::read, http::poll and http::ack).
Poll(c) ==
    /\ cst[c].ph = "ready"
    /\ \E src \in {"leader"} \cup (IF stale.on THEN {"stale"} ELSE {}) :
       LET S == IF src = "leader" THEN sm ELSE stale.sm
           clock == IF src = "leader" THEN Clock(ldr) ELSE Clock(stale.b)
           m == PollReq(c, cst[c].gen)
           res == PollResult(S, m, clock)
       IN /\ seen' = seen \cup res.evs
          /\ lastRead' = [ckpt |-> S.ckpt, tail |-> TailOff(S),
                          minCkpt |-> maxAckCkpt, minTail |-> maxRcpt]
          /\ fence' = (fence \/ (res.type # "lost" /\ m.gen < sm.lease.gen))
          /\ CASE res.type = "lost" ->
                    /\ cst' = [cst EXCEPT ![c] = LoseLease(c)]
                    /\ UNCHANGED <<req, delivered, saw410>>
               [] res.type = "expired" ->
                    /\ saw410' = TRUE
                    /\ UNCHANGED <<cst, req, delivered>>
               [] res.type = "events" /\ res.evs = {} ->
                    /\ UNCHANGED <<cst, req, delivered, saw410>>
               [] res.type = "events" /\ res.evs # {} ->
                    /\ delivered' = delivered \cup {e.off : e \in res.evs}
                    /\ \E th \in (res.after + 1)..res.last :
                          LET a == AckReq(c, cst[c].gen, res.after, res.last, th) IN
                          /\ cst' = [cst EXCEPT ![c].ph = "wait", ![c].cur = a]
                          /\ req' = req \cup {a}
                    /\ UNCHANGED saw410
    /\ UNCHANGED <<logVars, envVars, rep, pst, used, receipts, expiredIds, maxAckCkpt,
                   maxRcpt>>

ConsumerRetry(c) ==
    /\ cst[c].ph = "wait" /\ Waiting(cst[c].cur)
    /\ req' = req \cup {cst[c].cur}
    /\ UNCHANGED <<logVars, envVars, rep, pst, cst, used, ghostVars>>

ConsumerReceive(c) ==
    \E r \in rep :
    /\ cst[c].ph = "wait" /\ r.rq = cst[c].cur
    /\ rep' = rep \ {r}
    /\ CASE r.type = "lease" ->
              /\ cst' = [cst EXCEPT ![c] = Ready(c, r.gen)]
              /\ UNCHANGED <<req, delivered, saw410>>
         [] r.type = "busy" ->
              /\ cst' = [cst EXCEPT ![c].ph = "idle", ![c].cur = NoReq]
              /\ UNCHANGED <<req, delivered, saw410>>
         [] r.type = "lost" ->
              /\ cst' = [cst EXCEPT ![c] = LoseLease(c)]
              /\ UNCHANGED <<req, delivered, saw410>>
         [] r.type = "expired" ->
              /\ cst' = [cst EXCEPT ![c] = Ready(c, cst[c].gen)]
              /\ saw410' = TRUE
              /\ UNCHANGED <<req, delivered>>
         [] OTHER ->  \* ckpt, invalid
              /\ cst' = [cst EXCEPT ![c] = Ready(c, cst[c].gen)]
              /\ UNCHANGED <<req, delivered, saw410>>
    /\ UNCHANGED <<logVars, envVars, pst, used, receipts, seen, expiredIds, maxAckCkpt,
                   maxRcpt, lastRead, fence>>

\* A reply nobody waits for any more.
DiscardReply ==
    \E r \in rep :
        /\ IF r.rq.type = "publish" THEN r.rq # PubReq(r.rq.p, pst[r.rq.p])
           ELSE cst[r.rq.cl].cur # r.rq
        /\ rep' = rep \ {r}
        /\ UNCHANGED <<logVars, envVars, req, pst, cst, used, ghostVars>>

-----------------------------------------------------------------------------
(* Faults                                                                  *)

DropRequest ==
    \E m \in req :
        /\ Budget("drop", MaxDrops)
        /\ req' = req \ {m}
        /\ Spend("drop")
        /\ UNCHANGED <<logVars, envVars, rep, pst, cst, ghostVars>>

DropReply ==
    \E r \in rep :
        /\ Budget("drop", MaxDrops)
        /\ rep' = rep \ {r}
        /\ Spend("drop")
        /\ UNCHANGED <<logVars, envVars, req, pst, cst, ghostVars>>

ClockJump(n) ==
    /\ Budget("clock", MaxClockJumps) /\ skew[n] = 0
    /\ \E d \in {-MaxDrift, MaxDrift} :
           /\ d <= now
           /\ skew' = [skew EXCEPT ![n] = d]
    /\ Spend("clock")
    /\ UNCHANGED <<logVars, now, clientVars, ghostVars>>

\* A consumer dies; its replacement claims with a new claim id. Allowed only
\* while a lease it holds expires within the time horizon, so the standby can
\* take over in the model.
ConsumerCrash(c) ==
    /\ Budget("ccrash", MaxConsumerCrashes)
    /\ cst[c].ph # "retired" /\ cst[c].cid < MaxClaims
    /\ now + LeaseTtl <= MaxTime
    /\ cst' = [cst EXCEPT ![c] = [ph |-> "idle", cid |-> cst[c].cid + 1, gen |-> 0,
                                  cur |-> NoReq]]
    /\ Spend("ccrash")
    /\ UNCHANGED <<logVars, envVars, req, rep, pst, ghostVars>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<logVars, skew, clientVars, used, ghostVars>>

\* The producer has its answers and every consumer has stopped: the run is
\* over (explicit terminal stuttering, so deadlock checking stays on).
Done ==
    /\ \A p \in Producers : ~Sending(p)
    /\ \A c \in Consumers : cst[c].ph = "retired"
    /\ UNCHANGED vars

From(x, m) == IF m.type = "publish" THEN m.p = x ELSE m.cl = x

HandleAnyWrite ==\E m \in req, keep \in BOOLEAN, t \in Stamps : HandleWrite(m, keep, t)
Failover == \E k \in 0..Len(pend), b \in Nodes : LeaderFails(k, b)
Isolate == \E k \in 0..Len(pend), b \in Nodes : LeaderIsolated(k, b)

Next ==
    \/ CommitNext \/ HandleAnyWrite \/ RetainTick \/ OperatorReset
    \/ \E p \in Producers : ProducerRetry(p) \/ ProducerReceive(p)
    \/ \E c \in Consumers : SendClaim(c) \/ Poll(c) \/ ConsumerRetry(c)
                            \/ ConsumerReceive(c) \/ ConsumerCrash(c)
    \/ DiscardReply \/ DropRequest \/ DropReply
    \/ Failover \/ Isolate \/ Heal
    \/ \E n \in Nodes : ClockJump(n)
    \/ Tick
    \/ Done

\* Each fairness assumption names its mechanism; faults and the duplicate
\* branch of a handler carry none.
Fairness ==
    \* OpenRaft replication, commit and apply (EventsLog L_PublishCompletes)
    /\ WF_vars(CommitNext)
    \* HTTP handlers on the leader, reached through route refresh
    \* Per client: each HTTP request has its own handler task, so one client's
    \* requests cannot starve another's (a single WF over every request could).
    /\ \A x \in Producers \cup Consumers :
           WF_vars(\E m \in req, t \in Stamps : From(x, m) /\ HandleWrite(m, FALSE, t))
    \* Control::retain loop and the 410 runbook
    /\ WF_vars(RetainTick) /\ WF_vars(OperatorReset)
    \* SDK retry timers and response handling
    /\ \A p \in Producers : WF_vars(ProducerRetry(p)) /\ WF_vars(ProducerReceive(p))
    /\ \A c \in Consumers : WF_vars(ConsumerRetry(c)) /\ WF_vars(ConsumerReceive(c))
    \* The consumer pool keeps claiming and polling (sub.claim(), lease.poll())
    /\ \A c \in Consumers : WF_vars(SendClaim(c)) /\ WF_vars(Poll(c))
    /\ WF_vars(DiscardReply)
    \* The partition heals; time passes
    /\ WF_vars(Heal) /\ WF_vars(Tick)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
TypeOK ==
    /\ ldr \in Nodes
    /\ \A i \in 1..Len(pend) :
           pend[i].type \in {"publish", "claim", "ack", "retain", "reset"}
    /\ stale.on \in BOOLEAN
    /\ now \in 0..MaxTime /\ skew \in [Nodes -> (-MaxDrift)..MaxDrift]
    /\ \A m \in req : m.type \in {"publish", "claim", "poll", "ack"}
    /\ \A r \in rep : r.rq.type \in {"publish", "claim", "poll", "ack"}
    /\ \A c \in Consumers :
           /\ cst[c].ph \in {"idle", "wait", "ready", "retired"}
           /\ cst[c].cid \in 1..MaxClaims
    /\ \A k \in DOMAIN used : used[k] \in Nat
    /\ fence \in BOOLEAN /\ saw410 \in BOOLEAN

-----------------------------------------------------------------------------
(* Safety properties, named after the register rows they check.           *)

\* EVT-G02: no offset is ever reported with two different events.
G02_OffsetsStable ==
    \A a, b \in seen : a.off = b.off => (a.id = b.id /\ a.body = b.body)

\* EVT-G02: retained offsets are contiguous; offsets start at 1.
G02_Contiguous ==
    /\ 1 <= sm.earliest /\ sm.earliest <= sm.next
    /\ {e.off : e \in sm.evs} = sm.earliest..(sm.next - 1)
    /\ \A e, f \in sm.evs : e.id = f.id => e.off = f.off

\* EVT-G03: a retried publish returns its original offsets while retained.
G03_RetryReturnsOriginal ==
    \A a, b \in receipts :
        (a.id = b.id /\ a.body = b.body /\ a.off # b.off) => a.id \in expiredIds

\* EVT-G05: every event after the checkpoint is redelivered until acknowledged:
\* the checkpoint passes only offsets some consumer received in a page, or
\* offsets an operator reset skipped after they expired.
G05_AckOnlyDelivered ==
    \A o \in 1..sm.ckpt : o \in delivered \/ o <= sm.skip

\* EVT-G05: a stale lease generation gets lease_lost, never a page or an ack.
G05_StaleGenerationFenced == ~fence /\ ~sm.stale

\* EVT-G05: the lease clock never moves backward, so a grant that an applied
\* command saw expire is never revived.
G05_NoRevivedLease == ~sm.revived

\* EVT-G06, EVT-G05: a poll reads at least every acknowledged checkpoint and
\* every acknowledged publish (acks advance a prefix, polls are linearizable).
G06_PollLinearizable ==
    lastRead.ckpt >= lastRead.minCkpt /\ lastRead.tail >= lastRead.minTail

\* EVT-G08: retention removes events by publish time only, never before their
\* retention period, and publish time is nondecreasing in offset order. The
\* register names no clock tolerance, so the property is strict. Variant
\* drift_bound_stated is the candidate fix: the plan states a clock bound and
\* the guarantee allows early removal by at most that bound.
G08_RetentionByPublishTime ==
    \A x \in sm.removed :
        x.pt + RetainAfter < now + (IF Variant = "drift_bound_stated" THEN MaxDrift ELSE 0)

G08_PublishTimeMonotone ==
    \A e, f \in sm.evs : e.off < f.off => e.pt <= f.pt

Safety ==
    /\ G02_OffsetsStable /\ G02_Contiguous /\ G03_RetryReturnsOriginal
    /\ G05_AckOnlyDelivered /\ G05_StaleGenerationFenced /\ G05_NoRevivedLease
    /\ G06_PollLinearizable
    /\ G08_RetentionByPublishTime /\ G08_PublishTimeMonotone

-----------------------------------------------------------------------------
(* Liveness after the faults stop (fault budgets are finite).              *)

\* EVT-G05: delivery makes progress. Eventually every published event has
\* been delivered or has expired, and the checkpoint reaches the tail.
Drained ==
    /\ \A p \in Producers : ~Sending(p)
    /\ \A r \in receipts : r.off \in delivered \/ r.id \in expiredIds
    /\ sm.ckpt = TailOff(sm)

L_Drain == <>Drained

=============================================================================
