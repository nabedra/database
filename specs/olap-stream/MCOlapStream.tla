---------------------------- MODULE MCOlapStream ----------------------------
(* TLC model for OlapStream: model values, symmetry and the row counts. *)
EXTENDS OlapStream

CONSTANTS n1, n2, n3, c1, c2

MCNodes == {n1, n2, n3}
MCClients == {c1, c2}

\* Version 1 has 2 rows (one full batch, then EOF). The concurrent table
\* commit gives version 2 three rows (a full batch and a short tail).
MCRowCount == <<2, 3>>

\* Safety runs only. Liveness runs use no symmetry.
MCSymmetry == Permutations(MCNodes)
=============================================================================
