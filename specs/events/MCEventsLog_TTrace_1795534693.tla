---- MODULE MCEventsLog_TTrace_1795534693 ----
EXTENDS Sequences, TLCExt, MCEventsLog, Toolbox, Naturals, TLC

_expression ==
    LET MCEventsLog_TEExpression == INSTANCE MCEventsLog_TEExpression
    IN MCEventsLog_TEExpression!expression
----

_trace ==
    LET MCEventsLog_TETrace == INSTANCE MCEventsLog_TETrace
    IN MCEventsLog_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        synced = ((n1 :> 2 @@ n2 :> 2 @@ n3 :> 0))
        /\
        role = ((n1 :> "leader" @@ n2 :> "follower" @@ n3 :> "follower"))
        /\
        log = ((n1 :> <<[t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]], [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p2, type |-> "publish", k |-> 1]]]>> @@ n2 :> <<[t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]], [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p2, type |-> "publish", k |-> 1]]]>> @@ n3 :> <<>>))
        /\
        part = ((n1 :> 0 @@ n2 :> 0 @@ n3 :> 0))
        /\
        commit = ((n1 :> 2 @@ n2 :> 0 @@ n3 :> 0))
        /\
        used = ([part |-> 0, crash |-> 0, stop |-> 0, wipe |-> 0, drop |-> 0, dup |-> 0])
        /\
        gone = ((n1 :> FALSE @@ n2 :> FALSE @@ n3 :> FALSE))
        /\
        clog = (<<[e |-> [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]], ct |-> 1], [e |-> [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p2, type |-> "publish", k |-> 1]]], ct |-> 1]>>)
        /\
        pst = ((p1 :> 1 @@ p2 :> 1))
        /\
        receipts = ({[id |-> "e1", body |-> 1, off |-> 1]})
        /\
        sm = ((n1 :> [next |-> 4, evs |-> {[id |-> "e1", body |-> 1, off |-> 1]}, ins |-> {<<"e1", 1>>}] @@ n2 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n3 :> [next |-> 1, evs |-> {}, ins |-> {}]))
        /\
        term = ((n1 :> 1 @@ n2 :> 1 @@ n3 :> 1))
        /\
        up = ((n1 :> TRUE @@ n2 :> TRUE @@ n3 :> TRUE))
        /\
        rep = ({[type |-> "published", rq |-> [p |-> p1, type |-> "publish", k |-> 1], offs |-> <<1>>], [type |-> "conflict", rq |-> [p |-> p2, type |-> "publish", k |-> 1], offs |-> <<>>]})
        /\
        csm = ([next |-> 4, evs |-> {[id |-> "e1", body |-> 1, off |-> 1]}, ins |-> {<<"e1", 1>>}])
        /\
        req = ({})
    )
----

_init ==
    /\ log = _TETrace[1].log
    /\ gone = _TETrace[1].gone
    /\ commit = _TETrace[1].commit
    /\ synced = _TETrace[1].synced
    /\ rep = _TETrace[1].rep
    /\ req = _TETrace[1].req
    /\ sm = _TETrace[1].sm
    /\ used = _TETrace[1].used
    /\ pst = _TETrace[1].pst
    /\ part = _TETrace[1].part
    /\ up = _TETrace[1].up
    /\ clog = _TETrace[1].clog
    /\ term = _TETrace[1].term
    /\ role = _TETrace[1].role
    /\ receipts = _TETrace[1].receipts
    /\ csm = _TETrace[1].csm
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ log  = _TETrace[i].log
        /\ log' = _TETrace[j].log
        /\ gone  = _TETrace[i].gone
        /\ gone' = _TETrace[j].gone
        /\ commit  = _TETrace[i].commit
        /\ commit' = _TETrace[j].commit
        /\ synced  = _TETrace[i].synced
        /\ synced' = _TETrace[j].synced
        /\ rep  = _TETrace[i].rep
        /\ rep' = _TETrace[j].rep
        /\ req  = _TETrace[i].req
        /\ req' = _TETrace[j].req
        /\ sm  = _TETrace[i].sm
        /\ sm' = _TETrace[j].sm
        /\ used  = _TETrace[i].used
        /\ used' = _TETrace[j].used
        /\ pst  = _TETrace[i].pst
        /\ pst' = _TETrace[j].pst
        /\ part  = _TETrace[i].part
        /\ part' = _TETrace[j].part
        /\ up  = _TETrace[i].up
        /\ up' = _TETrace[j].up
        /\ clog  = _TETrace[i].clog
        /\ clog' = _TETrace[j].clog
        /\ term  = _TETrace[i].term
        /\ term' = _TETrace[j].term
        /\ role  = _TETrace[i].role
        /\ role' = _TETrace[j].role
        /\ receipts  = _TETrace[i].receipts
        /\ receipts' = _TETrace[j].receipts
        /\ csm  = _TETrace[i].csm
        /\ csm' = _TETrace[j].csm

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("MCEventsLog_TTrace_1795534693.json", _TETrace)

=============================================================================

 Note that you can extract this module `MCEventsLog_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `MCEventsLog_TEExpression.tla` file takes precedence 
  over the module `MCEventsLog_TEExpression` below).

---- MODULE MCEventsLog_TEExpression ----
EXTENDS Sequences, TLCExt, MCEventsLog, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `MCEventsLog` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        log |-> log
        ,gone |-> gone
        ,commit |-> commit
        ,synced |-> synced
        ,rep |-> rep
        ,req |-> req
        ,sm |-> sm
        ,used |-> used
        ,pst |-> pst
        ,part |-> part
        ,up |-> up
        ,clog |-> clog
        ,term |-> term
        ,role |-> role
        ,receipts |-> receipts
        ,csm |-> csm
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_logUnchanged |-> log = log'
        
        \* Format the `log` variable as Json value.
        \* ,_logJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(log)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_logModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].log # _TETrace[s-1].log
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE MCEventsLog_TETrace ----
\*EXTENDS IOUtils, MCEventsLog, TLC
\*
\*trace == IODeserialize("MCEventsLog_TTrace_1795534693.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE MCEventsLog_TETrace ----
EXTENDS MCEventsLog, TLC

trace == 
    <<
    ([synced |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),role |-> (n1 :> "leader" @@ n2 :> "follower" @@ n3 :> "follower"),log |-> (n1 :> <<>> @@ n2 :> <<>> @@ n3 :> <<>>),part |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),commit |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),used |-> [part |-> 0, crash |-> 0, stop |-> 0, wipe |-> 0, drop |-> 0, dup |-> 0],gone |-> (n1 :> FALSE @@ n2 :> FALSE @@ n3 :> FALSE),clog |-> <<>>,pst |-> (p1 :> 1 @@ p2 :> 1),receipts |-> {},sm |-> (n1 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n2 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n3 :> [next |-> 1, evs |-> {}, ins |-> {}]),term |-> (n1 :> 1 @@ n2 :> 1 @@ n3 :> 1),up |-> (n1 :> TRUE @@ n2 :> TRUE @@ n3 :> TRUE),rep |-> {},csm |-> [next |-> 1, evs |-> {}, ins |-> {}],req |-> {[p |-> p1, type |-> "publish", k |-> 1], [p |-> p2, type |-> "publish", k |-> 1]}]),
    ([synced |-> (n1 :> 1 @@ n2 :> 0 @@ n3 :> 0),role |-> (n1 :> "leader" @@ n2 :> "follower" @@ n3 :> "follower"),log |-> (n1 :> <<[t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]]>> @@ n2 :> <<>> @@ n3 :> <<>>),part |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),commit |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),used |-> [part |-> 0, crash |-> 0, stop |-> 0, wipe |-> 0, drop |-> 0, dup |-> 0],gone |-> (n1 :> FALSE @@ n2 :> FALSE @@ n3 :> FALSE),clog |-> <<>>,pst |-> (p1 :> 1 @@ p2 :> 1),receipts |-> {},sm |-> (n1 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n2 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n3 :> [next |-> 1, evs |-> {}, ins |-> {}]),term |-> (n1 :> 1 @@ n2 :> 1 @@ n3 :> 1),up |-> (n1 :> TRUE @@ n2 :> TRUE @@ n3 :> TRUE),rep |-> {},csm |-> [next |-> 1, evs |-> {}, ins |-> {}],req |-> {[p |-> p2, type |-> "publish", k |-> 1]}]),
    ([synced |-> (n1 :> 2 @@ n2 :> 0 @@ n3 :> 0),role |-> (n1 :> "leader" @@ n2 :> "follower" @@ n3 :> "follower"),log |-> (n1 :> <<[t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]], [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p2, type |-> "publish", k |-> 1]]]>> @@ n2 :> <<>> @@ n3 :> <<>>),part |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),commit |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),used |-> [part |-> 0, crash |-> 0, stop |-> 0, wipe |-> 0, drop |-> 0, dup |-> 0],gone |-> (n1 :> FALSE @@ n2 :> FALSE @@ n3 :> FALSE),clog |-> <<>>,pst |-> (p1 :> 1 @@ p2 :> 1),receipts |-> {},sm |-> (n1 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n2 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n3 :> [next |-> 1, evs |-> {}, ins |-> {}]),term |-> (n1 :> 1 @@ n2 :> 1 @@ n3 :> 1),up |-> (n1 :> TRUE @@ n2 :> TRUE @@ n3 :> TRUE),rep |-> {},csm |-> [next |-> 1, evs |-> {}, ins |-> {}],req |-> {}]),
    ([synced |-> (n1 :> 2 @@ n2 :> 2 @@ n3 :> 0),role |-> (n1 :> "leader" @@ n2 :> "follower" @@ n3 :> "follower"),log |-> (n1 :> <<[t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]], [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p2, type |-> "publish", k |-> 1]]]>> @@ n2 :> <<[t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]], [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p2, type |-> "publish", k |-> 1]]]>> @@ n3 :> <<>>),part |-> (n1 :> 0 @@ n2 :> 0 @@ n3 :> 0),commit |-> (n1 :> 2 @@ n2 :> 0 @@ n3 :> 0),used |-> [part |-> 0, crash |-> 0, stop |-> 0, wipe |-> 0, drop |-> 0, dup |-> 0],gone |-> (n1 :> FALSE @@ n2 :> FALSE @@ n3 :> FALSE),clog |-> <<[e |-> [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p1, type |-> "publish", k |-> 1]]], ct |-> 1], [e |-> [t |-> 1, c |-> [type |-> "publish", rq |-> [p |-> p2, type |-> "publish", k |-> 1]]], ct |-> 1]>>,pst |-> (p1 :> 1 @@ p2 :> 1),receipts |-> {[id |-> "e1", body |-> 1, off |-> 1]},sm |-> (n1 :> [next |-> 4, evs |-> {[id |-> "e1", body |-> 1, off |-> 1]}, ins |-> {<<"e1", 1>>}] @@ n2 :> [next |-> 1, evs |-> {}, ins |-> {}] @@ n3 :> [next |-> 1, evs |-> {}, ins |-> {}]),term |-> (n1 :> 1 @@ n2 :> 1 @@ n3 :> 1),up |-> (n1 :> TRUE @@ n2 :> TRUE @@ n3 :> TRUE),rep |-> {[type |-> "published", rq |-> [p |-> p1, type |-> "publish", k |-> 1], offs |-> <<1>>], [type |-> "conflict", rq |-> [p |-> p2, type |-> "publish", k |-> 1], offs |-> <<>>]},csm |-> [next |-> 4, evs |-> {[id |-> "e1", body |-> 1, off |-> 1]}, ins |-> {<<"e1", 1>>}],req |-> {}])
    >>
----


=============================================================================

---- CONFIG MCEventsLog_TTrace_1795534693 ----
CONSTANTS
    n1 = n1
    n2 = n2
    n3 = n3
    p1 = p1
    p2 = p2
    Nodes = { n1 , n2 , n3 }
    Producers = { p1 , p2 }
    MaxTerm = 4
    MaxCrashes = 1
    MaxStopAll = 1
    MaxWipes = 1
    MaxPartitions = 1
    MaxDrops = 1
    MaxDups = 1
    MaxFaults = 6
    Mutant = "conflict_consumes_offsets"
    Items <- MCItems
    n1 = n1
    p1 = p1
    p2 = p2
    n3 = n3
    n2 = n2

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
\* Generated on Tue Nov 24 15:38:15 UTC 2026