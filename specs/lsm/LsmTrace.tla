------------------------------ MODULE LsmTrace ------------------------------
(***************************************************************************)
(* Trace validation of the journal's group commit against Lsm.            *)
(* The workload is server/kv/tests/lsm_trace.rs: writers submit one-key    *)
(* batches, synced or not, each waiting for its commit before the next,    *)
(* and the journal writer reports its steps through nabedra_kv::sim_trace.*)
(* Every line is one Lsm action:                                           *)
(*                                                                         *)
(*   Submit             Store::submit returned, by the writer that logs it *)
(*   JournalWriteGroup  Writer::process wrote the group (flush_buf or      *)
(*                      write_aligned)                                     *)
(*   JournalPublish     visible_seq stored without a sync                  *)
(*   JournalSync        sync_written, then the rest of the group published *)
(*                                                                         *)
(* The run has no segment roll, no memtable freeze, no flush, compaction,  *)
(* snapshot or fault, so these are all the steps the store takes in it.   *)
(* A run with any of them is outside what this spec explains: their lines  *)
(* (JournalRoll, FreezeMemtable, FlushWriteRun, Kill, PowerLoss and the    *)
(* Open steps of the sim mapping) are not logged yet.                      *)
(*                                                                         *)
(* Variables a line carries: lastSub (Submit), lastW (JournalWriteGroup),  *)
(* vis and durPub (JournalPublish), synced, vis and durPub (JournalSync). *)
(* Not carried, so not constrained by a line but evolved by the actions:  *)
(*  - queue, grp, wst, nextSeq, mem, hist, ack, quiet: process and ghost   *)
(*    state the store does not expose; each line's checks pin them through *)
(*    the next lines (a group can be written only after its submissions); *)
(*  - jos, jdur, padNext, wpos: the journal's slots and pages, which the   *)
(*    code does not report per record;                                    *)
(*  - every other variable, which no action in the run changes.           *)
(***************************************************************************)
EXTENDS Lsm, NabedraTrace

ASSUME TraceHeaderOK("Lsm")

TraceKeys == ToSet(TraceParams.Keys)
TraceWriters == ToSet(TraceParams.Writers)
TraceMaxSeq == TraceParams.MaxSeq
TraceSegCap == TraceParams.SegCap
TraceMaxSegs == TraceParams.MaxSegs
TraceMaxFile == TraceParams.MaxFile
TraceMaxImm == TraceParams.MaxImm
TraceNoKey == "nokey"

\* A writer's submission: the line names the key, the operation and whether the
\* writer asked for a sync, and the sequence the submission took.
Submits ==
    /\ LineNode \in Writers
    /\ HasMsg
    /\ LineMsg.k \in Keys
    /\ LineMsg.op \in Ops
    /\ LineMsg.sync \in BOOLEAN
    /\ RequireMsg([k |-> LineMsg.k, op |-> LineMsg.op, sync |-> LineMsg.sync], {})
    /\ Submit(LineNode, LineMsg.k, LineMsg.op, LineMsg.sync)

Journal(name, A, checks) == TraceStep(name, LineNode = "journal" /\ ~HasMsg /\ A, checks)

TraceNext ==
    \/ TraceStep("Submit", Submits, [lastSub |-> RequireVar(lastSub')])
    \/ Journal("JournalWriteGroup", JournalWriteGroup, [lastW |-> RequireVar(lastW')])
    \/ Journal("JournalPublish", JournalPublish,
               [vis |-> RequireVar(vis'), durPub |-> RequireVar(durPub')])
    \/ Journal("JournalSync", JournalSync,
               [synced |-> RequireVar(synced'), vis |-> RequireVar(vis'),
                durPub |-> RequireVar(durPub')])

TraceInitState == Init /\ TraceInit

=============================================================================
