-------------------------- MODULE MCOlapStreamJoin --------------------------
(* TLC model for OlapStreamJoin: model values and symmetry. *)
EXTENDS OlapStreamJoin

CONSTANTS n1, n2, n3, k1, k2

MCNodes == {n1, n2, n3}
MCKeys == {k1, k2}

\* Safety runs only. Liveness runs use no symmetry.
MCSymmetry == Permutations(MCNodes) \cup Permutations(MCKeys)
=============================================================================
