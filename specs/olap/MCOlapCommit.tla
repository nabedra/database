---------------------------- MODULE MCOlapCommit ----------------------------
(* TLC harness for OlapCommit. Symmetry is used only by safety runs.        *)
EXTENDS OlapCommit
Sym == Permutations(Writers) \cup Permutations(Requests)
=============================================================================
