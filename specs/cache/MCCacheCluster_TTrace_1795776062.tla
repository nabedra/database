---- MODULE MCCacheCluster_TTrace_1795776062 ----
EXTENDS Sequences, TLCExt, Toolbox, MCCacheCluster_TEConstants, Naturals, TLC, MCCacheCluster

_expression ==
    LET MCCacheCluster_TEExpression == INSTANCE MCCacheCluster_TEExpression
    IN MCCacheCluster_TEExpression!expression
----

_trace ==
    LET MCCacheCluster_TETrace == INSTANCE MCCacheCluster_TETrace
    IN MCCacheCluster_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        cur = ((w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)))
        /\
        coff = (0)
        /\
        obsStart = (2)
        /\
        jr = ((s1 :> <<<<[k |-> k1, v |-> 1, d |-> -1, c |-> "client"]>>, <<>>>>))
        /\
        owners = ((s1 :> <<{<<w2, 1>>}, {<<w3, 1>>}>>))
        /\
        promo = ((s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> TRUE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>))
        /\
        ccut = ({})
        /\
        gen = (1)
        /\
        sh = ((w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 1, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 1), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 2, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])))
        /\
        now = (1)
        /\
        srvMax = ((s1 :> 1))
        /\
        member = ((w1 :> 1 @@ w2 :> 2 @@ w3 :> 1))
        /\
        nWrites = (1)
        /\
        up = ((w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE))
        /\
        rep = ((w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]))
        /\
        steady = ((w1 :> TRUE @@ w2 :> FALSE @@ w3 :> TRUE))
        /\
        inc = ((w1 :> 1 @@ w2 :> 1 @@ w3 :> 1))
        /\
        wv = ((w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 2 @@ w3 :> 1)]))
        /\
        pcut = ({})
        /\
        stg = ((s1 :> [on |-> FALSE, seq |-> -1]))
        /\
        off = ((w1 :> 0 @@ w2 :> 0 @@ w3 :> 0))
        /\
        gone = ((s1 :> {}))
        /\
        cs = ((s1 :> [ep |-> 2, cold |-> FALSE, rep |-> <<NoNode, 0>>, pri |-> <<w3, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]))
        /\
        srvBad = (FALSE)
        /\
        served = ((s1 :> <<{<<w2, 1>>}, {}>>))
        /\
        lateAck = (TRUE)
        /\
        pk = ((w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])))
        /\
        lease = ((s1 :> [nb |-> 3, exp |-> 3]))
        /\
        fb = ([ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 1, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0])
        /\
        base = ((s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 1, fseq |-> 0]>>))
    )
----

_init ==
    /\ rep = _TETrace[1].rep
    /\ obsStart = _TETrace[1].obsStart
    /\ pcut = _TETrace[1].pcut
    /\ inc = _TETrace[1].inc
    /\ cs = _TETrace[1].cs
    /\ ccut = _TETrace[1].ccut
    /\ promo = _TETrace[1].promo
    /\ fb = _TETrace[1].fb
    /\ gone = _TETrace[1].gone
    /\ gen = _TETrace[1].gen
    /\ off = _TETrace[1].off
    /\ jr = _TETrace[1].jr
    /\ now = _TETrace[1].now
    /\ lateAck = _TETrace[1].lateAck
    /\ owners = _TETrace[1].owners
    /\ coff = _TETrace[1].coff
    /\ pk = _TETrace[1].pk
    /\ srvMax = _TETrace[1].srvMax
    /\ lease = _TETrace[1].lease
    /\ steady = _TETrace[1].steady
    /\ base = _TETrace[1].base
    /\ sh = _TETrace[1].sh
    /\ nWrites = _TETrace[1].nWrites
    /\ member = _TETrace[1].member
    /\ up = _TETrace[1].up
    /\ srvBad = _TETrace[1].srvBad
    /\ stg = _TETrace[1].stg
    /\ cur = _TETrace[1].cur
    /\ served = _TETrace[1].served
    /\ wv = _TETrace[1].wv
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ rep  = _TETrace[i].rep
        /\ rep' = _TETrace[j].rep
        /\ obsStart  = _TETrace[i].obsStart
        /\ obsStart' = _TETrace[j].obsStart
        /\ pcut  = _TETrace[i].pcut
        /\ pcut' = _TETrace[j].pcut
        /\ inc  = _TETrace[i].inc
        /\ inc' = _TETrace[j].inc
        /\ cs  = _TETrace[i].cs
        /\ cs' = _TETrace[j].cs
        /\ ccut  = _TETrace[i].ccut
        /\ ccut' = _TETrace[j].ccut
        /\ promo  = _TETrace[i].promo
        /\ promo' = _TETrace[j].promo
        /\ fb  = _TETrace[i].fb
        /\ fb' = _TETrace[j].fb
        /\ gone  = _TETrace[i].gone
        /\ gone' = _TETrace[j].gone
        /\ gen  = _TETrace[i].gen
        /\ gen' = _TETrace[j].gen
        /\ off  = _TETrace[i].off
        /\ off' = _TETrace[j].off
        /\ jr  = _TETrace[i].jr
        /\ jr' = _TETrace[j].jr
        /\ now  = _TETrace[i].now
        /\ now' = _TETrace[j].now
        /\ lateAck  = _TETrace[i].lateAck
        /\ lateAck' = _TETrace[j].lateAck
        /\ owners  = _TETrace[i].owners
        /\ owners' = _TETrace[j].owners
        /\ coff  = _TETrace[i].coff
        /\ coff' = _TETrace[j].coff
        /\ pk  = _TETrace[i].pk
        /\ pk' = _TETrace[j].pk
        /\ srvMax  = _TETrace[i].srvMax
        /\ srvMax' = _TETrace[j].srvMax
        /\ lease  = _TETrace[i].lease
        /\ lease' = _TETrace[j].lease
        /\ steady  = _TETrace[i].steady
        /\ steady' = _TETrace[j].steady
        /\ base  = _TETrace[i].base
        /\ base' = _TETrace[j].base
        /\ sh  = _TETrace[i].sh
        /\ sh' = _TETrace[j].sh
        /\ nWrites  = _TETrace[i].nWrites
        /\ nWrites' = _TETrace[j].nWrites
        /\ member  = _TETrace[i].member
        /\ member' = _TETrace[j].member
        /\ up  = _TETrace[i].up
        /\ up' = _TETrace[j].up
        /\ srvBad  = _TETrace[i].srvBad
        /\ srvBad' = _TETrace[j].srvBad
        /\ stg  = _TETrace[i].stg
        /\ stg' = _TETrace[j].stg
        /\ cur  = _TETrace[i].cur
        /\ cur' = _TETrace[j].cur
        /\ served  = _TETrace[i].served
        /\ served' = _TETrace[j].served
        /\ wv  = _TETrace[i].wv
        /\ wv' = _TETrace[j].wv

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("MCCacheCluster_TTrace_1795776062.json", _TETrace)

=============================================================================

 Note that you can extract this module `MCCacheCluster_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `MCCacheCluster_TEExpression.tla` file takes precedence 
  over the module `MCCacheCluster_TEExpression` below).

---- MODULE MCCacheCluster_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, MCCacheCluster_TEConstants, Naturals, TLC, MCCacheCluster

expression == 
    [
        \* To hide variables of the `MCCacheCluster` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        rep |-> rep
        ,obsStart |-> obsStart
        ,pcut |-> pcut
        ,inc |-> inc
        ,cs |-> cs
        ,ccut |-> ccut
        ,promo |-> promo
        ,fb |-> fb
        ,gone |-> gone
        ,gen |-> gen
        ,off |-> off
        ,jr |-> jr
        ,now |-> now
        ,lateAck |-> lateAck
        ,owners |-> owners
        ,coff |-> coff
        ,pk |-> pk
        ,srvMax |-> srvMax
        ,lease |-> lease
        ,steady |-> steady
        ,base |-> base
        ,sh |-> sh
        ,nWrites |-> nWrites
        ,member |-> member
        ,up |-> up
        ,srvBad |-> srvBad
        ,stg |-> stg
        ,cur |-> cur
        ,served |-> served
        ,wv |-> wv
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_repUnchanged |-> rep = rep'
        
        \* Format the `rep` variable as Json value.
        \* ,_repJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(rep)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_repModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].rep # _TETrace[s-1].rep
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE MCCacheCluster_TETrace ----
\*EXTENDS IOUtils, MCCacheCluster_TEConstants, TLC, MCCacheCluster
\*
\*trace == IODeserialize("MCCacheCluster_TTrace_1795776062.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE MCCacheCluster_TETrace ----
EXTENDS MCCacheCluster_TEConstants, TLC, MCCacheCluster

trace == 
    <<
    ([cur |-> (w1 :> (s1 :> TRUE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, role |-> "none", src |-> <<NoNode, 0>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 0,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w3 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> FALSE, seq |-> -1]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 0]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> TRUE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, role |-> "none", src |-> <<NoNode, 0>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w3 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> FALSE, seq |-> -1]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 0]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> TRUE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, role |-> "none", src |-> <<NoNode, 0>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> TRUE])] @@ w3 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> FALSE, seq |-> -1]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> TRUE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, role |-> "none", src |-> <<NoNode, 0>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> TRUE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> FALSE, seq |-> -1]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> FALSE) @@ w3 :> (s1 :> FALSE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, role |-> "none", src |-> <<NoNode, 0>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> TRUE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> TRUE, seq |-> 0]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<w3, 1>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> FALSE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, role |-> "none", src |-> <<NoNode, 0>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> TRUE, seq |-> 0]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<w3, 1>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> TRUE, seq |-> 0]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<w3, 1>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> TRUE, seq |-> 0]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<w3, 1>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> TRUE])]),steady |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> TRUE, seq |-> 0]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<w3, 1>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 0, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 2 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 1, base |-> TRUE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> TRUE, cur |-> TRUE])]),steady |-> (w1 :> TRUE @@ w2 :> FALSE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> TRUE, seq |-> 0]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 1, cold |-> TRUE, rep |-> <<w1, 1>>, pri |-> <<w2, 1>>, tgt |-> <<w3, 1>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 0, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 1, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {<<w3, 1>>}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> TRUE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 2 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> FALSE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> FALSE, seq |-> -1]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 2, cold |-> FALSE, rep |-> <<NoNode, 0>>, pri |-> <<w3, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 3, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 1, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 0, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {<<w3, 1>>}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> TRUE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 2, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 0),member |-> (w1 :> 1 @@ w2 :> 2 @@ w3 :> 1),nWrites |-> 0,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> FALSE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 2 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> FALSE, seq |-> -1]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 2, cold |-> FALSE, rep |-> <<NoNode, 0>>, pri |-> <<w3, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{}, {}>>),lateAck |-> FALSE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 3, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 1, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 1, fseq |-> 0]>>)]),
    ([cur |-> (w1 :> (s1 :> FALSE) @@ w2 :> (s1 :> TRUE) @@ w3 :> (s1 :> TRUE)),coff |-> 0,obsStart |-> 2,jr |-> (s1 :> <<<<[k |-> k1, v |-> 1, d |-> -1, c |-> "client"]>>, <<>>>>),owners |-> (s1 :> <<{<<w2, 1>>}, {<<w3, 1>>}>>),promo |-> (s1 :> <<[on |-> FALSE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE], [on |-> TRUE, srcSeq |-> 0, retSeq |-> 0, lastOnly |-> FALSE]>>),ccut |-> {},gen |-> 1,sh |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 1, role |-> "copy", src |-> <<w2, 1>>, dst |-> {}, base |-> FALSE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w2 :> (s1 :> [seq |-> 1, ep |-> 1, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {<<w1, 1>>, <<w3, 1>>}, base |-> TRUE, val |-> (k1 :> 1), dl |-> (k1 :> -1), gexp |-> 3, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 2, role |-> "pri", src |-> <<NoNode, 0>>, dst |-> {}, base |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), gexp |-> 0, gnb |-> 0, frq |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0]])),now |-> 1,srvMax |-> (s1 :> 1),member |-> (w1 :> 1 @@ w2 :> 2 @@ w3 :> 1),nWrites |-> 1,up |-> (w1 :> TRUE @@ w2 :> TRUE @@ w3 :> TRUE),rep |-> (w1 :> [inc |-> 0, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w2 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])] @@ w3 :> [inc |-> 1, sl |-> (s1 :> [seq |-> 0, ep |-> 0, base |-> FALSE, fro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], ok |-> FALSE, cur |-> FALSE])]),steady |-> (w1 :> TRUE @@ w2 :> FALSE @@ w3 :> TRUE),inc |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1),wv |-> (w1 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w2 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 1 @@ w3 :> 1)] @@ w3 :> [gen |-> 1, mem |-> (w1 :> 1 @@ w2 :> 2 @@ w3 :> 1)]),pcut |-> {},stg |-> (s1 :> [on |-> FALSE, seq |-> -1]),off |-> (w1 :> 0 @@ w2 :> 0 @@ w3 :> 0),gone |-> (s1 :> {}),cs |-> (s1 :> [ep |-> 2, cold |-> FALSE, rep |-> <<NoNode, 0>>, pri |-> <<w3, 1>>, tgt |-> <<NoNode, 0>>, mtgt |-> <<NoNode, 0>>, mfrz |-> FALSE, mfro |-> [on |-> FALSE, seq |-> 0, up |-> 0, lexp |-> 0], seed |-> [on |-> FALSE, seq |-> 0, prev |-> <<NoNode, 0>>, ep |-> 0]]),srvBad |-> FALSE,served |-> (s1 :> <<{<<w2, 1>>}, {}>>),lateAck |-> TRUE,pk |-> (w1 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w2 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]]) @@ w3 :> (s1 :> [seq |-> 0, ep |-> 0, src |-> <<NoNode, 0>>, val |-> (k1 :> 0), dl |-> (k1 :> -1), kind |-> "none", gen |-> 0, eff |-> [k |-> k1, v |-> 0, d |-> -1, c |-> Sys]])),lease |-> (s1 :> [nb |-> 3, exp |-> 3]),fb |-> [ccut |-> 0, pcut |-> 0, crash |-> 0, restart |-> 0, replace |-> 1, loss |-> 0, delay |-> 0, lead |-> 0, drift |-> 0, reset |-> 0, moves |-> 0],base |-> (s1 :> <<[set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> TRUE, fep |-> 0, fseq |-> 0], [set |-> TRUE, val |-> (k1 :> 0), dl |-> (k1 :> -1), cold |-> FALSE, fep |-> 1, fseq |-> 0]>>)])
    >>
----


=============================================================================

---- MODULE MCCacheCluster_TEConstants ----
EXTENDS MCCacheCluster

CONSTANTS s1, k1

=============================================================================

---- CONFIG MCCacheCluster_TTrace_1795776062 ----
CONSTANTS
    w1 = w1
    w2 = w2
    w3 = w3
    c1 = c1
    c2 = c2
    Workers = { w1 , w2 , w3 }
    Clients = { c1 , c2 }
    Slots = { s1 }
    Keys = { k1 }
    NoNode = NoNode
    Sys = Sys
    InitPlaces <- AllPlaces
    D = 1
    MaxDrift = 1
    L = 3
    RA = 3
    G = 3
    TTL = 1
    MaxLag = 0
    MaxTime = 4
    GST = 4
    MaxEpoch = 2
    MaxWrites = 1
    MaxCrashes = 0
    MaxRestarts = 0
    MaxReplaces = 1
    MaxCtrlCuts = 0
    MaxPeerCuts = 0
    MaxLoss = 0
    MaxDelays = 0
    MaxLeaderChanges = 0
    MaxDriftSteps = 0
    MaxResets = 0
    MaxMoves = 0
    InitSkew = 0
    Ops = { "set" , "ttl" , "del" }
    Mutant = "prefix_early_staged_commit"
    Variant = { "follow_retired" }
    w1 = w1
    s1 = s1
    NoNode = NoNode
    w3 = w3
    c1 = c1
    w2 = w2
    c2 = c2
    k1 = k1
    Sys = Sys

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
\* Generated on Fri Nov 27 10:41:29 UTC 2026