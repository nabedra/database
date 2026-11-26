---------------------------- MODULE MCOltpControl ----------------------------
(* Model values and symmetry for TLC. No state constraint. *)
EXTENDS OltpControl, TLC
CONSTANTS w1, w2, w3, w4
MCSymmetry == Permutations({w1, w2, w3, w4})
=============================================================================
