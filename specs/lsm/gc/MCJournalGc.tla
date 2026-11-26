----------------------------- MODULE MCJournalGc ----------------------------
(* Model values and the symmetry set for TLC. Keys and writers are        *)
(* interchangeable, so safety runs use both permutation groups. Liveness   *)
(* runs use a configuration without SYMMETRY.                              *)
EXTENDS JournalGc, TLC

CONSTANTS k1, k2, w1, w2

MCKeys == {k1, k2}
MCWriters == {w1, w2}
Sym == Permutations(MCKeys) \cup Permutations(MCWriters)
=============================================================================
