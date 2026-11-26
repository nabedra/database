--------------------------- MODULE MCEventsDelivery ---------------------------
(* Model values, the workload and the symmetry set for EventsDelivery.     *)
EXTENDS EventsDelivery

CONSTANTS n1, n2, n3, p1, c1, c2

\* p1 publishes e1, then e2, so polls and acks interleave with publishes and
\* retention can remove e1 before or after it is delivered.
MCItems ==
    [p \in {p1} |-> << <<[id |-> "e1", body |-> 1]>>, <<[id |-> "e2", body |-> 1]>> >>]

\* Brokers and consumers are interchangeable. Safety runs only.
MCSymmetry == Permutations({n1, n2, n3}) \cup Permutations({c1, c2})

=============================================================================
