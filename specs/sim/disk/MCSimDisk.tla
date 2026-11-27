----------------------------- MODULE MCSimDisk -----------------------------
(* TLC model of SimDisk. Names are interchangeable directory entries, so    *)
(* safety runs use them as a symmetry set.                                  *)
EXTENDS SimDisk, TLC

Symmetry == Permutations(Names)
=============================================================================
