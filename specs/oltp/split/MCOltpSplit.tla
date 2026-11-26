----------------------------- MODULE MCOltpSplit -----------------------------
(* Model values and symmetry for TLC. No state constraint. *)
EXTENDS OltpSplit, TLC
CONSTANTS n1, n2, n3
MCSymmetry == Permutations({n1, n2, n3})
=============================================================================
