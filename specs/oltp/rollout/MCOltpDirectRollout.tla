----------------------- MODULE MCOltpDirectRollout -----------------------
EXTENDS OltpDirectRollout, TLC

CONSTANTS n1, n2, n3
MCClientRange == ("c1" :> "r1") @@ ("c2" :> "r2")
MCClientResult == ("c1" :> "no-op") @@ ("c2" :> "row-conflict")
MCSymmetry == Permutations(Node)
========================================================================
