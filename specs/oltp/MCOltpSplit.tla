----------------------------- MODULE MCOltpSplit -----------------------------
(* Model values and symmetry for TLC. No state constraint. *)
EXTENDS OltpSplit
CONSTANTS n1, n2, n3, c1, c2
MCSymmetry == Permutations({n1, n2, n3})
=============================================================================
