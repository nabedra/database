------------------------- MODULE MCSearchLatest -------------------------
EXTENDS SearchLatest, TLC

CONSTANTS q1, q2

MCRanges == {"r1", "r2"}
MCOneRange == {"r1"}
MCWorkers == {"s1"}
MCWorkers2 == {"s1", "s2"}
MCQueries == {q1, q2}
MCOneQuery == {q1}
MCSingles == {"w1"}
MCSingles2 == {"w1", "w2"}
MCNone == {}
MCBatches == {"b1"}
MCParts == [w \in {"w1", "w2", "b1"} |-> {"r1"}]
MCPartsLarge == [w \in {"w1", "b1"} |-> IF w = "w1" THEN {"r1"} ELSE MCRanges]
Symm == Permutations(MCQueries)
=============================================================================
