---- MODULE MCEventsDelivery_TTrace_1795535027 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, MCEventsDelivery

_expression ==
    LET MCEventsDelivery_TEExpression == INSTANCE MCEventsDelivery_TEExpression
    IN MCEventsDelivery_TEExpression!expression
----

_trace ==
    LET MCEventsDelivery_TETrace == INSTANCE MCEventsDelivery_TETrace
    IN MCEventsDelivery_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        saw410 = (FALSE)
        /\
        cst = ((c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]))
        /\
        delivered = ({})
        /\
        used = ([fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 1, ccrash |-> 0])
        /\
        lastRead = ([ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0])
        /\
        seen = ({[id |-> "e1", body |-> 1, off |-> 1]})
        /\
        pst = ((p1 :> 1))
        /\
        receipts = ({[id |-> "e1", body |-> 1, off |-> 1]})
        /\
        stale = ([sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"])
        /\
        maxRcpt = (1)
        /\
        expiredIds = ({"e1"})
        /\
        now = (1)
        /\
        maxAckCkpt = (0)
        /\
        sm = ([stale |-> FALSE, ckpt |-> 0, removed |-> {[id |-> "e1", off |-> 1, pt |-> 0]}, cut |-> 1, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 2, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {<<"e1", 1>>}, expGens |-> {}])
        /\
        ldr = (n1)
        /\
        rep = ({[rq |-> [p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], gen |-> 0, type |-> "published", off |-> 0, offs |-> <<1>>, evs |-> {}, after |-> 0, last |-> 0]})
        /\
        fence = (FALSE)
        /\
        skew = ((n1 :> -1 @@ n2 :> 0 @@ n3 :> 0))
        /\
        pend = (<<>>)
        /\
        req = ({})
    )
----

_init ==
    /\ cst = _TETrace[1].cst
    /\ skew = _TETrace[1].skew
    /\ fence = _TETrace[1].fence
    /\ delivered = _TETrace[1].delivered
    /\ now = _TETrace[1].now
    /\ maxRcpt = _TETrace[1].maxRcpt
    /\ pend = _TETrace[1].pend
    /\ ldr = _TETrace[1].ldr
    /\ saw410 = _TETrace[1].saw410
    /\ rep = _TETrace[1].rep
    /\ req = _TETrace[1].req
    /\ expiredIds = _TETrace[1].expiredIds
    /\ lastRead = _TETrace[1].lastRead
    /\ sm = _TETrace[1].sm
    /\ used = _TETrace[1].used
    /\ pst = _TETrace[1].pst
    /\ stale = _TETrace[1].stale
    /\ receipts = _TETrace[1].receipts
    /\ seen = _TETrace[1].seen
    /\ maxAckCkpt = _TETrace[1].maxAckCkpt
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ cst  = _TETrace[i].cst
        /\ cst' = _TETrace[j].cst
        /\ skew  = _TETrace[i].skew
        /\ skew' = _TETrace[j].skew
        /\ fence  = _TETrace[i].fence
        /\ fence' = _TETrace[j].fence
        /\ delivered  = _TETrace[i].delivered
        /\ delivered' = _TETrace[j].delivered
        /\ now  = _TETrace[i].now
        /\ now' = _TETrace[j].now
        /\ maxRcpt  = _TETrace[i].maxRcpt
        /\ maxRcpt' = _TETrace[j].maxRcpt
        /\ pend  = _TETrace[i].pend
        /\ pend' = _TETrace[j].pend
        /\ ldr  = _TETrace[i].ldr
        /\ ldr' = _TETrace[j].ldr
        /\ saw410  = _TETrace[i].saw410
        /\ saw410' = _TETrace[j].saw410
        /\ rep  = _TETrace[i].rep
        /\ rep' = _TETrace[j].rep
        /\ req  = _TETrace[i].req
        /\ req' = _TETrace[j].req
        /\ expiredIds  = _TETrace[i].expiredIds
        /\ expiredIds' = _TETrace[j].expiredIds
        /\ lastRead  = _TETrace[i].lastRead
        /\ lastRead' = _TETrace[j].lastRead
        /\ sm  = _TETrace[i].sm
        /\ sm' = _TETrace[j].sm
        /\ used  = _TETrace[i].used
        /\ used' = _TETrace[j].used
        /\ pst  = _TETrace[i].pst
        /\ pst' = _TETrace[j].pst
        /\ stale  = _TETrace[i].stale
        /\ stale' = _TETrace[j].stale
        /\ receipts  = _TETrace[i].receipts
        /\ receipts' = _TETrace[j].receipts
        /\ seen  = _TETrace[i].seen
        /\ seen' = _TETrace[j].seen
        /\ maxAckCkpt  = _TETrace[i].maxAckCkpt
        /\ maxAckCkpt' = _TETrace[j].maxAckCkpt

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("MCEventsDelivery_TTrace_1795535027.json", _TETrace)

=============================================================================

 Note that you can extract this module `MCEventsDelivery_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `MCEventsDelivery_TEExpression.tla` file takes precedence 
  over the module `MCEventsDelivery_TEExpression` below).

---- MODULE MCEventsDelivery_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, MCEventsDelivery

expression == 
    [
        \* To hide variables of the `MCEventsDelivery` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        cst |-> cst
        ,skew |-> skew
        ,fence |-> fence
        ,delivered |-> delivered
        ,now |-> now
        ,maxRcpt |-> maxRcpt
        ,pend |-> pend
        ,ldr |-> ldr
        ,saw410 |-> saw410
        ,rep |-> rep
        ,req |-> req
        ,expiredIds |-> expiredIds
        ,lastRead |-> lastRead
        ,sm |-> sm
        ,used |-> used
        ,pst |-> pst
        ,stale |-> stale
        ,receipts |-> receipts
        ,seen |-> seen
        ,maxAckCkpt |-> maxAckCkpt
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_cstUnchanged |-> cst = cst'
        
        \* Format the `cst` variable as Json value.
        \* ,_cstJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(cst)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_cstModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].cst # _TETrace[s-1].cst
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE MCEventsDelivery_TETrace ----
\*EXTENDS IOUtils, TLC, MCEventsDelivery
\*
\*trace == IODeserialize("MCEventsDelivery_TTrace_1795535027.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE MCEventsDelivery_TETrace ----
EXTENDS TLC, MCEventsDelivery

trace == 
    <<
    ([saw410 |-> FALSE,cst |-> (c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]),delivered |-> {},used |-> [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 0, ccrash |-> 0],lastRead |-> [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0],seen |-> {},pst |-> (p1 :> 1),receipts |-> {},stale |-> [sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"],maxRcpt |-> 0,expiredIds |-> {},now |-> 0,maxAckCkpt |-> 0,sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}],ldr |-> n1,rep |-> {},fence |-> FALSE,skew |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),pend |-> <<>>,req |-> {[p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]}]),
    ([saw410 |-> FALSE,cst |-> (c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]),delivered |-> {},used |-> [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 0, ccrash |-> 0],lastRead |-> [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0],seen |-> {},pst |-> (p1 :> 1),receipts |-> {},stale |-> [sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"],maxRcpt |-> 0,expiredIds |-> {},now |-> 0,maxAckCkpt |-> 0,sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}],ldr |-> n1,rep |-> {},fence |-> FALSE,skew |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),pend |-> <<[now |-> 0, rq |-> [p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], type |-> "publish", cut |-> 0]>>,req |-> {}]),
    ([saw410 |-> FALSE,cst |-> (c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]),delivered |-> {},used |-> [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 0, ccrash |-> 0],lastRead |-> [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0],seen |-> {[id |-> "e1", body |-> 1, off |-> 1]},pst |-> (p1 :> 1),receipts |-> {[id |-> "e1", body |-> 1, off |-> 1]},stale |-> [sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"],maxRcpt |-> 1,expiredIds |-> {},now |-> 0,maxAckCkpt |-> 0,sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {[id |-> "e1", body |-> 1, off |-> 1, pt |-> 0]}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 2, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {<<"e1", 1>>}, expGens |-> {}],ldr |-> n1,rep |-> {[rq |-> [p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], gen |-> 0, type |-> "published", off |-> 0, offs |-> <<1>>, evs |-> {}, after |-> 0, last |-> 0]},fence |-> FALSE,skew |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),pend |-> <<>>,req |-> {}]),
    ([saw410 |-> FALSE,cst |-> (c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]),delivered |-> {},used |-> [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 1, ccrash |-> 0],lastRead |-> [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0],seen |-> {[id |-> "e1", body |-> 1, off |-> 1]},pst |-> (p1 :> 1),receipts |-> {[id |-> "e1", body |-> 1, off |-> 1]},stale |-> [sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"],maxRcpt |-> 1,expiredIds |-> {},now |-> 0,maxAckCkpt |-> 0,sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {[id |-> "e1", body |-> 1, off |-> 1, pt |-> 0]}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 2, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {<<"e1", 1>>}, expGens |-> {}],ldr |-> n1,rep |-> {[rq |-> [p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], gen |-> 0, type |-> "published", off |-> 0, offs |-> <<1>>, evs |-> {}, after |-> 0, last |-> 0]},fence |-> FALSE,skew |-> (n1 :> -1 @@ n2 :> 0 @@ n3 :> 0),pend |-> <<>>,req |-> {}]),
    ([saw410 |-> FALSE,cst |-> (c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]),delivered |-> {},used |-> [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 1, ccrash |-> 0],lastRead |-> [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0],seen |-> {[id |-> "e1", body |-> 1, off |-> 1]},pst |-> (p1 :> 1),receipts |-> {[id |-> "e1", body |-> 1, off |-> 1]},stale |-> [sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"],maxRcpt |-> 1,expiredIds |-> {},now |-> 1,maxAckCkpt |-> 0,sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {[id |-> "e1", body |-> 1, off |-> 1, pt |-> 0]}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 2, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {<<"e1", 1>>}, expGens |-> {}],ldr |-> n1,rep |-> {[rq |-> [p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], gen |-> 0, type |-> "published", off |-> 0, offs |-> <<1>>, evs |-> {}, after |-> 0, last |-> 0]},fence |-> FALSE,skew |-> (n1 :> -1 @@ n2 :> 0 @@ n3 :> 0),pend |-> <<>>,req |-> {}]),
    ([saw410 |-> FALSE,cst |-> (c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]),delivered |-> {},used |-> [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 1, ccrash |-> 0],lastRead |-> [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0],seen |-> {[id |-> "e1", body |-> 1, off |-> 1]},pst |-> (p1 :> 1),receipts |-> {[id |-> "e1", body |-> 1, off |-> 1]},stale |-> [sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"],maxRcpt |-> 1,expiredIds |-> {},now |-> 1,maxAckCkpt |-> 0,sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {[id |-> "e1", body |-> 1, off |-> 1, pt |-> 0]}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 2, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {<<"e1", 1>>}, expGens |-> {}],ldr |-> n1,rep |-> {[rq |-> [p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], gen |-> 0, type |-> "published", off |-> 0, offs |-> <<1>>, evs |-> {}, after |-> 0, last |-> 0]},fence |-> FALSE,skew |-> (n1 :> -1 @@ n2 :> 0 @@ n3 :> 0),pend |-> <<[now |-> 0, rq |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], type |-> "retain", cut |-> 1]>>,req |-> {}]),
    ([saw410 |-> FALSE,cst |-> (c1 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]] @@ c2 :> [ph |-> "idle", cid |-> 1, gen |-> 0, cur |-> [p |-> "none", k |-> 0, cid |-> 0, gen |-> 0, type |-> "none", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"]]),delivered |-> {},used |-> [fail |-> 0, part |-> 0, drop |-> 0, dup |-> 0, clock |-> 1, ccrash |-> 0],lastRead |-> [ckpt |-> 0, tail |-> 0, minCkpt |-> 0, minTail |-> 0],seen |-> {[id |-> "e1", body |-> 1, off |-> 1]},pst |-> (p1 :> 1),receipts |-> {[id |-> "e1", body |-> 1, off |-> 1]},stale |-> [sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {}, cut |-> 0, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 1, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {}, expGens |-> {}], on |-> FALSE, b |-> "none"],maxRcpt |-> 1,expiredIds |-> {"e1"},now |-> 1,maxAckCkpt |-> 0,sm |-> [stale |-> FALSE, ckpt |-> 0, removed |-> {[id |-> "e1", off |-> 1, pt |-> 0]}, cut |-> 1, earliest |-> 1, evs |-> {}, lease |-> [cid |-> 0, gen |-> 0, owner |-> "none", exp |-> 0, rel |-> TRUE], next |-> 2, skip |-> 0, revived |-> FALSE, lastPub |-> 0, lclock |-> 0, ins |-> {<<"e1", 1>>}, expGens |-> {}],ldr |-> n1,rep |-> {[rq |-> [p |-> p1, k |-> 1, cid |-> 0, gen |-> 0, type |-> "publish", th |-> 0, after |-> 0, last |-> 0, cl |-> "none"], gen |-> 0, type |-> "published", off |-> 0, offs |-> <<1>>, evs |-> {}, after |-> 0, last |-> 0]},fence |-> FALSE,skew |-> (n1 :> -1 @@ n2 :> 0 @@ n3 :> 0),pend |-> <<>>,req |-> {}])
    >>
----


=============================================================================

---- CONFIG MCEventsDelivery_TTrace_1795535027 ----
CONSTANTS
    n1 = n1
    n2 = n2
    n3 = n3
    p1 = p1
    c1 = c1
    c2 = c2
    Nodes = { n1 , n2 , n3 }
    Producers = { p1 }
    Consumers = { c1 , c2 }
    PageLimit = 1
    LeaseTtl = 1
    RetainAfter = 1
    MaxTime = 3
    MaxClaims = 3
    MaxFailovers = 1
    MaxPartitions = 1
    MaxDrops = 1
    MaxDups = 1
    MaxClockJumps = 1
    MaxConsumerCrashes = 1
    MaxDrift = 1
    MaxFaults = 6
    Variant = "none"
    Mutant = "earliest_not_advanced"
    Items <- MCItems
    n2 = n2
    n1 = n1
    p1 = p1
    c1 = c1
    c2 = c2
    n3 = n3

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Tue Nov 24 15:43:56 UTC 2026