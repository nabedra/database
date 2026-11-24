---------------------------- MODULE MCTwoPhase ----------------------------
(* Model values for TLC. RM IDs are strings, so that traces can name them. *)
EXTENDS TwoPhase

MCRM == {"r1", "r2", "r3"}
MCRMLarge == {"r1", "r2", "r3", "r4"}
=============================================================================
