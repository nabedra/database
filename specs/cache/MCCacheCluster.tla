--------------------------- MODULE MCCacheCluster ---------------------------
(* TLC model values and symmetry for CacheCluster.  Symmetry is used only  *)
(* in safety runs; liveness configurations do not name Sym.                *)
EXTENDS CacheCluster

CONSTANTS w1, w2, w3, c1, c2

Sym == Permutations({w1, w2, w3}) \cup Permutations({c1, c2})
AllPlaces == Workers \X Workers
OnePlace == {<<w1, w2>>}
=============================================================================
