---------------------------- MODULE MCOltpRanges ----------------------------
(* Model values and symmetry for TLC. n1, n2 and n3 are the initial      *)
(* voters in zones a, b and c; the spare n4 is in zone a, so the only    *)
(* zone-safe replacement is of n1, and any other is the zone mutant's    *)
(* counterexample. Without a replacement the zones play no part and the  *)
(* three voters are interchangeable; with one, only n2 and n3 are.       *)
(* Clients are interchangeable in every run.                             *)
EXTENDS OltpRanges, TLC
CONSTANTS n1, n2, n3, n4, c1, c2
MCZone == (n1 :> "a") @@ (n2 :> "b") @@ (n3 :> "c") @@ (n4 :> "a")
MCSymVoters == { p @@ q : p \in Permutations({n1, n2, n3}), q \in Permutations({c1, c2}) }
MCSymMove == { p @@ q : p \in Permutations({n2, n3}), q \in Permutations({c1, c2}) }
=============================================================================
