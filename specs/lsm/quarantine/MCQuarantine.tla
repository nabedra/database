---------------------------- MODULE MCQuarantine ----------------------------
(* Model values and the symmetry set for TLC. The two groups are           *)
(* interchangeable, so safety runs use their permutations. Liveness runs   *)
(* use a configuration without SYMMETRY.                                   *)
EXTENDS Quarantine, TLC

CONSTANTS g1, g2

MCGroups == {g1, g2}
Sym == Permutations(MCGroups)
=============================================================================
