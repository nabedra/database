--------------------------- MODULE OlapStreamTrace ---------------------------
(***************************************************************************)
(* Trace validation for OlapStream. Each nabedra.trace/1 step must be an   *)
(* OlapStream action, with the logged values as constraints on the next    *)
(* state. Node IDs and client IDs are strings from the header; node "orm"  *)
(* logs the ORM's own steps and the request in "msg" names the client.     *)
(*                                                                         *)
(* A line is written where the step takes effect: a commit after the       *)
(* authority recheck (the reply), a crash or partition when the            *)
(* controller's loss step completes (A3), a delivery after the holder's    *)
(* install. "msg" carries the request record {k, c, run, h, seq} or the    *)
(* transfer record {from, to, c, e, run}.                                  *)
(*                                                                         *)
(* Variables a trace does not constrain (each can take any value the       *)
(* action allows):                                                         *)
(* - last, quiesced and the ghosts nextH, hsnap, hrun, hcl, openedSent:    *)
(*   history the code does not hold;                                       *)
(* - ack: a ghost of replies sent;                                         *)
(* - nCrash, nLoss, nDup, nDelay, nPart: fault counters, which the harness *)
(*   does not log;                                                         *)
(* - recovered: the code logs only the recovered view (term and ready);    *)
(* - alive: implied by the node's crash line;                              *)
(* - reg of other workers than the line's node (a commit logs the          *)
(*   coordinator's entry; holders log their own installs);                 *)
(* - cl.got, cl.floor, cl.err: the ORM logs ph, seq and h, not its rows;   *)
(* - the duplicate flag of a request delivery (keep) and the loss choice   *)
(*   of a reply: the next line shows what followed.                        *)
(***************************************************************************)
EXTENDS OlapStream, NabedraTrace

ASSUME TraceHeaderOK("OlapStream")

TraceNodes == ToSet(TraceParams.Nodes)
TraceClients == ToSet(TraceParams.Clients)
TraceOpenOnly == ToSet(TraceParams.OpenOnly)
TraceBatchRows == TraceParams.BatchRows
TraceRowCount == TraceParams.RowCount
TraceDropLast == TraceParams.DropLast
TraceMaxCrashes == TraceParams.MaxCrashes
TraceMaxLoss == TraceParams.MaxLoss
TraceMaxDup == TraceParams.MaxDup
TraceMaxDelay == TraceParams.MaxDelay
TraceMaxPartitions == TraceParams.MaxPartitions
TraceMaxCommits == TraceParams.MaxCommits

-----------------------------------------------------------------------------
(* Constraints from the logged variables.                                  *)

\* The client's own state after the step.
MatchCl(c) ==
    /\ MatchVar("ph", cl'[c].ph)
    /\ MatchVar("seq", cl'[c].seq)
    /\ MatchVar("h", cl'[c].h)

\* The stream entry node n holds for request ID c after the step.
MatchEntry(n, c) == MatchVar("entry", reg'[n][c])

\* The control root after a loss, takeover or cold start.
MatchRoot ==
    /\ MatchVar("term", term')
    /\ MatchVar("run", run')
    /\ MatchVar("sched", sched')
    /\ MatchSet("ready", ready')

\* A request line's message, and the client it names.
Req2 == LineMsg
ReqC == LineMsg.c

-----------------------------------------------------------------------------

TraceNext ==
    \/ /\ IsEvent("ClientStart") /\ LineNode = "orm"
       /\ ClientStart(ReqC) /\ MatchCl(ReqC)
    \/ /\ IsEvent("ClientRetry") /\ LineNode = "orm"
       /\ ClientRetry(ReqC) /\ MatchCl(ReqC)
    \/ /\ IsEvent("HandleRequest")
       /\ \E keep \in BOOLEAN : HandleRequest(LineNode, Req2, keep)
       /\ MatchEntry(LineNode, ReqC) /\ MatchCl(ReqC)
    \/ /\ IsEvent("DriverCommit")
       /\ \E keep \in BOOLEAN : DriverCommit(LineNode, Req2, keep)
       /\ MatchEntry(LineNode, ReqC) /\ MatchCl(ReqC)
    \/ /\ IsEvent("CommitPartial")
       /\ \E keep \in BOOLEAN, S \in SUBSET Nodes, w \in Whys :
             CommitPartial(LineNode, Req2, keep, S, w)
       /\ MatchEntry(LineNode, ReqC)
    \/ /\ IsEvent("DeliverInstall")
       /\ \E keep \in BOOLEAN : DeliverInstall(LineMsg, keep)
       /\ MatchEntry(LineNode, ReqC)
    \/ /\ IsEvent("Crash")
       /\ \E s \in Nodes \cup {Nil} : Crash(LineNode, s)
       /\ MatchRoot
    \/ /\ IsEvent("CrashMidCommit")
       /\ \E S \in SUBSET Nodes, s \in Nodes \cup {Nil} :
             CrashMidCommit(LineNode, Req2, S, s)
       /\ MatchRoot
    \/ /\ IsEvent("Partition")
       /\ \E s \in Nodes \cup {Nil} : Partition(LineNode, s)
       /\ MatchRoot
    \/ /\ IsEvent("PublishTableCommit")
       /\ PublishTableCommit
       /\ MatchVar("catVer", catVer')
    \/ /\ IsEvent("ColdStart")
       /\ \E s \in Nodes : ColdStart(s)
       /\ MatchRoot
    \/ /\ IsEvent("DropRequest")
       /\ DropRequest(LineMsg)

TraceInitState == Init /\ TraceInit

=============================================================================
