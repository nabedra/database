---------------------------- MODULE MCOltpDirect ----------------------------
(* Model values and scenarios for OltpDirect at the L3 minimum bounds:     *)
(* 3 nodes with 2 ranges, 2 concurrent clients.                            *)
EXTENDS OltpDirect, TLC

CONSTANTS n1, n2, n3    \* model values

MCNodes == {n1, n2, n3}
MCRanges == {"r1", "r2"}
MCKeys == {"k1", "k2"}
MCRangeOf == [k \in MCKeys |-> IF k = "k1" THEN "r1" ELSE "r2"]
MCClients == {"c1", "c2"}
\* n1 leads both ranges at first, so n2 and n3 are interchangeable (safety only).
MCInitLeader == [r \in MCRanges |-> n1]
Sym == Permutations({n2, n3})

MCAllFaults == {"crash", "part", "move", "loss", "refuse", "gw"}


\* c1 is the client under test: two attempts per call, one new call after
\* outcome_unknown. c2 is the concurrent writer: one attempt, no retry.
MCAtt == [c \in MCClients |-> IF c = "c1" THEN 2 ELSE 1]
MCRetry == [c \in MCClients |-> IF c = "c1" THEN 1 ELSE 0]
MCZero == [c \in MCClients |-> 0]
MCRepeat == [c \in MCClients |-> IF c = "c1" THEN 1 ELSE 0]

Rq(id, op, key, val) == [id |-> id, op |-> op, key |-> key, val |-> val]
Rows(v1) == [k \in MCKeys |-> IF k = "k1" THEN v1 ELSE "none"]
Sc(q1, q2, rows, qkey) ==
    [req |-> [c \in MCClients |-> IF c = "c1" THEN q1 ELSE q2], rows |-> rows, qkey |-> qkey]

\* S1 (O08, D02, DR-1): an update races an insert of its row.
S1 == Sc(Rq("i1", "upd", "k1", "a"), Rq("i2", "ins", "k1", "b"), Rows("none"), "none")
\* S1D (O08, D02, DR-1 as the vector session traced it): a delete of a missing row races an insert of it.
S1D == Sc(Rq("i1", "del", "k1", "x"), Rq("i2", "ins", "k1", "b"), Rows("none"), "none")
\* S2 (D03): one id with two bodies on one key.
S2 == Sc(Rq("i1", "upd", "k1", "a"), Rq("i1", "upd", "k1", "b"), Rows("z"), "none")
\* S3 (D03): one id on two keys of two ranges: both execute.
S3 == Sc(Rq("i1", "ins", "k1", "a"), Rq("i1", "ins", "k2", "b"), Rows("none"), "none")
\* S5 (O10, DR-1): an insert refused on a present row races the row's delete.
S5 == Sc(Rq("i1", "ins", "k1", "a"), Rq("i2", "del", "k1", "x"), Rows("z"), "none")
\* SQ (O08, D08, R-01): an update races a queued insert of its row; c2 writes the other range.
SQ == Sc(Rq("i1", "upd", "k1", "a"), Rq("i2", "ins", "k2", "b"), Rows("none"), "k1")

MC1 == {S1}
MC1D == {S1D}
MC2 == {S2}
MC3 == {S3}
MC5 == {S5}
MCQ == {SQ}
=============================================================================
