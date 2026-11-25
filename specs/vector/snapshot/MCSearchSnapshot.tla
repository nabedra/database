------------------------ MODULE MCSearchSnapshot ------------------------
EXTENDS SearchSnapshot

MCRanges == {"r1", "r2"}
MCOneRange == {"r1"}
MCDirects == {"d1"}
MCDirects2 == {"d1", "d2"}
MCHome == [d \in {"d1", "d2"} |-> "r1"]
MCBatch == {"b"}
MCNone == {}
MCQueries == {"q1"}
=============================================================================
