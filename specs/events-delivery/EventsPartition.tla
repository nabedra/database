--------------------------- MODULE EventsPartition ---------------------------
(***************************************************************************)
(* The partition state machine of Nabedra Events, used by EventsDelivery.  *)
(* EventsLog holds its own copy of the publish rules. It follows          *)
(* server/events/src/partition.rs,                                        *)
(* partition/events.rs and partition/subscriptions.rs, which implement    *)
(* plans/events.md "Brokers and storage" (retained events, next offset,   *)
(* deduplication, retention floor, checkpoints, lease generations) and    *)
(* "Client and delivery".                                                  *)
(*                                                                         *)
(* A state is a record. Besides the replicated fields it carries history   *)
(* fields that only properties read: ins (every id and body ever           *)
(* inserted), removed (events removed by retention), expGens (lease       *)
(* generations an applied command saw expire), revived (an expired grant  *)
(* passed a lease check), stale (a superseded generation acked) and skip  *)
(* (offsets passed by an operator reset).                                 *)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, FiniteSets

CONSTANTS
    Items,          \* [Producers -> Seq(Seq([id, body]))]: each producer's batches, in order
    PageLimit,      \* poll page size (MAX_PAGE_EVENTS, scaled down)
    LeaseTtl,       \* lease lifetime in ticks
    RetainAfter,    \* retention period in ticks
    Mutant          \* "none" in the T1 configurations, a mutant name in mutants/*.cfg

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
Range(s) == {s[i] : i \in DOMAIN s}

-----------------------------------------------------------------------------
(* Requests and replies. Records of a kind share one set of fields.        *)

NoReq == [type |-> "none", p |-> "none", k |-> 0, cl |-> "none", cid |-> 0, gen |-> 0,
          after |-> 0, last |-> 0, th |-> 0]
PubReq(p, k) == [NoReq EXCEPT !.type = "publish", !.p = p, !.k = k]
ClaimReq(c, n) == [NoReq EXCEPT !.type = "claim", !.cl = c, !.cid = n]
PollReq(c, g) == [NoReq EXCEPT !.type = "poll", !.cl = c, !.gen = g]
AckReq(c, g, a, l, th) ==
    [NoReq EXCEPT !.type = "ack", !.cl = c, !.gen = g, !.after = a, !.last = l, !.th = th]

\* A command carries the request it serves, the clock of the broker that
\* stamped it (http::now_ms) and, for Retain, the cutoff.
Cmd(type, rq, t, cut) == [type |-> type, rq |-> rq, now |-> t, cut |-> cut]
Blank == Cmd("blank", NoReq, 0, 0)

Res(type) == [type |-> type, offs |-> <<>>, gen |-> 0, off |-> 0,
              after |-> 0, last |-> 0, evs |-> {}]
Reply(rq, r) == [rq |-> rq, type |-> r.type, offs |-> r.offs, gen |-> r.gen, off |-> r.off,
                 after |-> r.after, last |-> r.last, evs |-> r.evs]

NoLease == [gen |-> 0, owner |-> "none", cid |-> 0, exp |-> 0, rel |-> TRUE]

Init0 == [next |-> 1, earliest |-> 1, evs |-> {}, lastPub |-> 0, cut |-> 0,
          lclock |-> 0, ckpt |-> 0, lease |-> NoLease, skip |-> 0,
          ins |-> {}, removed |-> {}, expGens |-> {},
          revived |-> FALSE, stale |-> FALSE]

TailOff(S) == S.next - 1
FirstCkpt(S) == S.earliest - 1
EventAt(S, o) == {e \in S.evs : e.off = o}

-----------------------------------------------------------------------------
(* EVT-G02, EVT-G03, EVT-G04: events::publish. Items are checked in input  *)
(* order; a repeated id or a retained id with different bytes rejects the  *)
(* whole batch, and a rejected batch consumes no offsets.                  *)

RECURSIVE PubLoop(_, _, _, _)
PubLoop(items, i, acc, pt) ==
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
                                                 THEN e.off ELSE @], pt)
            ELSE LET o == acc.next
                     new == [off |-> o, id |-> it.id, body |-> it.body, pt |-> pt]
                 IN PubLoop(items, i + 1,
                        [acc EXCEPT !.evs = {x \in @ : x.off # o} \cup {new},
                                    !.next = o + 1,
                                    !.rc = Append(@, o),
                                    !.n = @ + 1,
                                    !.ins = @ \cup {<<it.id, it.body>>}], pt)

ApplyPublish(S, cmd) ==
    LET items == Items[cmd.rq.p][cmd.rq.k]
        \* published_ms = max(now, last_published, retention_cutoff)
        pt == IF Mutant = "raw_publish_time" THEN cmd.now
              ELSE Max(Max(cmd.now, S.lastPub), S.cut)
        acc == PubLoop(items, 1, [ok |-> TRUE, next |-> S.next, evs |-> S.evs,
                                  rc |-> <<>>, n |-> 0, ins |-> S.ins], pt)
        stored == [S EXCEPT !.next = acc.next, !.evs = acc.evs, !.ins = acc.ins,
                            !.lastPub = IF acc.n > 0 THEN pt ELSE @]
    IN IF acc.ok
       THEN [s |-> stored, r |-> [Res("published") EXCEPT !.offs = acc.rc]]
       ELSE IF Mutant = "partial_batch"
       THEN [s |-> stored, r |-> Res("conflict")]
       ELSE IF Mutant = "conflict_consumes_offsets"
       THEN [s |-> [S EXCEPT !.next = @ + Len(items)], r |-> Res("conflict")]
       ELSE [s |-> S, r |-> Res("conflict")]

-----------------------------------------------------------------------------
(* EVT-G05: leases and checkpoints, subscriptions.rs. change_lease keeps a *)
(* nondecreasing replicated lease clock and checks expiry against it.      *)

LeaseClock(S, cmd) ==
    IF Mutant = "lease_clock_follows_command" THEN cmd.now ELSE Max(S.lclock, cmd.now)

SawExpiry(S, lc) ==
    IF S.lease.gen > 0 /\ lc >= S.lease.exp THEN S.expGens \cup {S.lease.gen}
    ELSE S.expGens

\* Subscription::claim. A retry with the same claim id under the same live
\* grant returns it; an expired or released grant gets lease_lost.
ApplyClaim(S, cmd) ==
    LET lc == LeaseClock(S, cmd)
        L == S.lease
        live == L.gen > 0 /\ ~L.rel /\ (lc < L.exp \/ Mutant = "claim_ignores_expiry")
        S1 == [S EXCEPT !.lclock = lc, !.expGens = SawExpiry(S, lc)]
    IN IF L.gen > 0 /\ L.owner = cmd.rq.cl /\ L.cid = cmd.rq.cid
       THEN IF live
            THEN [s |-> [S1 EXCEPT !.revived = @ \/ L.gen \in S.expGens],
                  r |-> [Res("lease") EXCEPT !.gen = L.gen]]
            ELSE [s |-> S1, r |-> Res("lost")]
       ELSE IF live
       THEN [s |-> S1, r |-> Res("busy")]
       ELSE LET g == L.gen + 1
            IN [s |-> [S1 EXCEPT !.lease = [gen |-> g, owner |-> cmd.rq.cl,
                                            cid |-> cmd.rq.cid,
                                            exp |-> lc + LeaseTtl, rel |-> FALSE]],
                r |-> [Res("lease") EXCEPT !.gen = g]]

\* Command::Ack: check the live lease, the delivered range and the retention
\* gap, then advance the checkpoint monotonically.
ApplyAck(S, cmd) ==
    LET lc == LeaseClock(S, cmd)
        L == S.lease
        m == cmd.rq
        live == IF Mutant = "ack_skip_lease_check" THEN TRUE
                ELSE L.gen > 0 /\ ~L.rel /\ L.owner = m.cl /\ L.gen = m.gen /\ lc < L.exp
        S1 == [S EXCEPT !.lclock = lc, !.expGens = SawExpiry(S, lc)]
        okS(ck) == [S1 EXCEPT !.ckpt = ck,
                              !.revived = @ \/ m.gen \in S.expGens,
                              !.stale = @ \/ m.gen # L.gen]
    IN IF ~live THEN [s |-> S1, r |-> Res("lost")]
       ELSE IF m.after >= m.th \/ m.th > m.last \/ m.last > TailOff(S) \/ m.after > S.ckpt
       THEN [s |-> S1, r |-> Res("invalid")]
       ELSE IF m.th > S.ckpt
       THEN IF S.ckpt < FirstCkpt(S) /\ Mutant # "ack_across_gap"
            THEN [s |-> S1, r |-> Res("expired")]
            ELSE LET ck == IF Mutant = "ack_to_tail" THEN TailOff(S) ELSE m.th
                 IN [s |-> okS(ck), r |-> [Res("ckpt") EXCEPT !.off = ck]]
       ELSE LET ck == IF Mutant = "ack_regress" THEN m.th ELSE S.ckpt
            IN [s |-> okS(ck), r |-> [Res("ckpt") EXCEPT !.off = ck]]

-----------------------------------------------------------------------------
(* EVT-G08: events::retain removes a prefix by publish time only.          *)

RECURSIVE RetLoop(_, _)
RetLoop(S, cut) ==
    LET first == EventAt(S, S.earliest)
    IN IF S.earliest >= S.next \/ first = {} THEN S
       ELSE LET e == CHOOSE x \in first : TRUE
            IN IF e.pt < cut \/ (Mutant = "retain_by_checkpoint" /\ e.off <= S.ckpt)
               THEN RetLoop([S EXCEPT !.evs = @ \ {e},
                                      !.earliest = IF Mutant = "earliest_not_advanced" THEN @ ELSE @ + 1,
                                      !.removed = @ \cup {[id |-> e.id, off |-> e.off,
                                                           pt |-> e.pt]}], cut)
               ELSE S

ApplyRetain(S, cmd) ==
    LET cut == Max(cmd.cut, S.cut)
    IN [s |-> [RetLoop(S, cut) EXCEPT !.cut = cut], r |-> Res("done")]

\* subscriptions::reset_expired: the operator's explicit choice after 410.
ApplyReset(S) ==
    IF S.ckpt < FirstCkpt(S)
    THEN [s |-> [S EXCEPT !.ckpt = FirstCkpt(S), !.skip = FirstCkpt(S),
                          !.lease.rel = TRUE],
          r |-> Res("done")]
    ELSE [s |-> S, r |-> Res("done")]

Step(S, cmd) ==
    CASE cmd.type = "publish" -> ApplyPublish(S, cmd)
      [] cmd.type = "claim"   -> ApplyClaim(S, cmd)
      [] cmd.type = "ack"     -> ApplyAck(S, cmd)
      [] cmd.type = "retain"  -> ApplyRetain(S, cmd)
      [] cmd.type = "reset"   -> ApplyReset(S)
      [] OTHER                -> [s |-> S, r |-> Res("done")]

RECURSIVE FoldLog(_, _, _)
\* The state after applying entries from+1..to of lg to S.
FoldLog(S, lg, to) ==
    IF to = 0 THEN S ELSE Step(FoldLog(S, lg, to - 1), lg[to].c).s

\* Query::Poll on a state read after the barrier (subscriptions::read and
\* events::page). The lease is checked against max(now, lease clock).
PollResult(S, m, clock) ==
    LET lc == Max(clock, S.lclock)
        L == S.lease
        live == L.gen > 0 /\ ~L.rel /\ L.owner = m.cl /\ L.gen = m.gen /\ lc < L.exp
        hi == Min(S.ckpt + PageLimit, TailOff(S))
    IN IF ~live THEN Res("lost")
       ELSE IF S.ckpt < FirstCkpt(S) THEN Res("expired")
       ELSE [Res("events") EXCEPT
                !.after = S.ckpt, !.last = hi, !.gen = m.gen,
                !.evs = {[off |-> e.off, id |-> e.id, body |-> e.body] :
                            e \in {x \in S.evs : x.off > S.ckpt /\ x.off <= hi}}]

=============================================================================
