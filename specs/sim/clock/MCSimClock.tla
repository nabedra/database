----------------------------- MODULE MCSimClock -----------------------------
(* TLC model of SimClock. MaxTime and the budgets bound every counter. A    *)
(* configuration file cannot write a negative literal, so the drift sets    *)
(* are defined here.                                                        *)
EXTENDS SimClock

Drifts1 == {-1, 0, 1}
Drifts2 == {-2, 0, 2}
Drifts3 == {-3, 0, 3}
=============================================================================
