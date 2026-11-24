----------------------------- MODULE MCEventsLog -----------------------------
(* Model values, the workload and the symmetry set for EventsLog.          *)
EXTENDS EventsLog

CONSTANTS n1, n2, n3, p1, p2

\* p1 publishes e1. p2 publishes the batch [e2, e1 with other bytes]: it
\* conflicts with p1's e1 while e1 is retained (EVT-G03 changed body, EVT-G04
\* whole batch), and applies whole if it commits first or after e1 expires.
MCItems ==
    [p \in {p1, p2} |->
        IF p = p1 THEN << <<[id |-> "e1", body |-> 1]>> >>
        ELSE << <<[id |-> "e2", body |-> 1], [id |-> "e1", body |-> 2]>> >>]

\* Brokers are interchangeable. Safety runs only.
MCSymmetry == Permutations({n1, n2, n3})

=============================================================================
