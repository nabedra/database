---------------------------- MODULE MCOltpRanges ----------------------------
(***************************************************************************)
(* Model values, symmetry and state constraints for TLC.                   *)
(***************************************************************************)
EXTENDS OltpRanges

CONSTANTS n1, n2, n3, n4, k1, k2, c1, c2, g0, g1, g2

MCNode == {n1, n2, n3, n4}
MCKey == {k1, k2}
MCClient == {c1, c2}
MCWKey == (c1 :> k1) @@ (c2 :> k2)
MCChildren == {g1, g2}
MCChildSpan == (g1 :> {k1}) @@ (g2 :> {k2})
\* Three voters in three zones; the spare shares n1's zone.
MCZone == (n1 :> "a") @@ (n2 :> "b") @@ (n3 :> "c") @@ (n4 :> "a")

\* Safety runs only. The three initial voters are interchangeable in the
\* profiles without replica moves (their zones are distinct and the spare
\* takes no part). Swapping both clients together with their keys and the
\* child ranges that own those keys leaves WKey and ChildSpan unchanged.
SwapPairs ==
    { [x \in {c1, c2, k1, k2, g1, g2} |-> x],
      (c1 :> c2) @@ (c2 :> c1) @@ (k1 :> k2) @@ (k2 :> k1) @@ (g1 :> g2) @@ (g2 :> g1) }
MCSymmetry == { p @@ q : p \in Permutations({n1, n2, n3}), q \in SwapPairs }

\* Constants that bound counters. Each is recorded in the plan's design
\* review section because it limits what a run covers.
CONSTANTS MaxTs, MaxLog

MCConstraint ==
    /\ tso <= MaxTs
    /\ \A g \in Groups : Len(hst[g]) <= MaxLog

\* Local iteration only, never a T check: at most two faults in a run. The
\* T1 baselines use MCConstraint, so every modelled fault kind keeps its own
\* budget and all of them can occur in one run.
MCIterConstraint == MCConstraint /\ FaultCount(budget) <= 2
=============================================================================
