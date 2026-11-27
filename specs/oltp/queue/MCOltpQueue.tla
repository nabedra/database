---------------------------- MODULE MCOltpQueue ----------------------------
\* Model values and symmetry for TLC. Workers and requests are
\* interchangeable, so safety runs use both as symmetry sets. Liveness
\* configurations use OltpQueue.live.cfg, which names no symmetry.
EXTENDS OltpQueue

CONSTANTS w1, w2, w3, w4, r1, r2

MCWorkers4 == {w1, w2, w3, w4}
MCWorkers3 == {w1, w2, w3}
MCReqs2    == {r1, r2}
MCReqs1    == {r1}

Symmetry4  == Permutations(MCWorkers4) \cup Permutations(MCReqs2)
Symmetry3  == Permutations(MCWorkers3) \cup Permutations(MCReqs2)
=============================================================================
