-------------------------- MODULE MCLocalIndex --------------------------
EXTENDS LocalIndex, TLC
CONSTANTS a, b, p1, p2, p3, p4, q1, q2
MCSlots == {a, b}
MCOneSlot == {a}
MCParts == {p1, p2, p3, p4}
MCParts3 == {p1, p2, p3}
MCParts2 == {p1, p2}
MCFirst1 == {p1}
MCFirst2 == {p1, p2}
MCQueries == {q1}
MCQueries2 == {q1, q2}
Symm == Permutations({a, b}) \cup Permutations({p2, p3}) \cup Permutations({q1, q2})
MCNone == {}
=============================================================================
