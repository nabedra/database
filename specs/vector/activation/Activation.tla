--------------------------- MODULE Activation ---------------------------
(***************************************************************************)
(* Activation of `data.vector.v1` in a rolling upgrade                     *)
(* (plans/oltp-vector.md, section 7, "Compatibility").                     *)
(*                                                                         *)
(* A worker advertises under its exact identity, which changes with every  *)
(* process start. Control enables the feature once every enrolled worker   *)
(* advertised. It never reverts. Afterwards a worker that did not          *)
(* advertise cannot join. A Data leader proposes vector entries only once  *)
(* its applied Control state has the feature.                              *)
(*                                                                         *)
(* Property: VEC-G14 (EveryMemberDecodes).                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Workers, MaxRestarts, Mutant

VARIABLES
    build,      \* build[w]: "old" or "new"
    id,         \* id[w]: identity, bumped by each start
    joined,     \* joined[w]: enrolled in the cluster under its current identity
    advert,     \* advertised [w, id]
    active,     \* Control: the feature is enabled
    sees,       \* sees[w]: the worker applied the enablement
    entries,    \* a vector entry was committed
    restarts

vars == <<build, id, joined, advert, active, sees, entries, restarts>>

TypeOK ==
    /\ build \in [Workers -> {"old", "new"}] /\ id \in [Workers -> 1..(MaxRestarts + 1)]
    /\ joined \in [Workers -> BOOLEAN] /\ active \in BOOLEAN /\ entries \in BOOLEAN

Init ==
    /\ build = [w \in Workers |-> "old"] /\ id = [w \in Workers |-> 1]
    /\ joined = [w \in Workers |-> TRUE] /\ advert = {} /\ active = FALSE
    /\ sees = [w \in Workers |-> FALSE] /\ entries = FALSE /\ restarts = 0

\* Upgrade, downgrade or replacement: the process starts again with either build and a new identity.
\* The old identity leaves the cluster. Mutant advert_survives_restart keeps the identity.
Restart(w, b) ==
    /\ restarts < MaxRestarts /\ restarts' = restarts + 1
    /\ build' = [build EXCEPT ![w] = b]
    /\ id' = IF Mutant = "advert_survives_restart" THEN id ELSE [id EXCEPT ![w] = @ + 1]
    /\ joined' = [joined EXCEPT ![w] = FALSE]
    /\ sees' = [sees EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<advert, active, entries>>

Advertised(w) == [w |-> w, id |-> id[w]] \in advert

\* Enrolment and the advert of a new build are one Control decision order: an advert may follow later
\* only before activation. Mutant join_without_advert ignores the rule.
Join(w) ==
    /\ ~joined[w]
    /\ active => Advertised(w) \/ Mutant = "join_without_advert"
    /\ joined' = [joined EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<build, id, advert, active, sees, entries, restarts>>

Advertise(w) ==
    /\ build[w] = "new" /\ ~Advertised(w)
    /\ advert' = advert \cup {[w |-> w, id |-> id[w]]}
    /\ UNCHANGED <<build, id, joined, active, sees, entries, restarts>>

\* Mutant activate_on_quorum needs only a majority.
Activate ==
    /\ ~active
    /\ IF Mutant = "activate_on_quorum"
       THEN 2 * Cardinality({w \in Workers : Advertised(w)}) > Cardinality(Workers)
       ELSE \A w \in Workers : joined[w] => Advertised(w)
    /\ \E w \in Workers : joined[w]
    /\ active' = TRUE
    /\ UNCHANGED <<build, id, joined, advert, sees, entries, restarts>>

Learn(w) ==
    /\ active /\ joined[w] /\ ~sees[w]
    /\ sees' = [sees EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<build, id, joined, advert, active, entries, restarts>>

\* Any joined worker may lead. Mutant propose_before_active proposes when its own build is new.
Propose(w) ==
    /\ joined[w] /\ build[w] = "new" /\ ~entries
    /\ sees[w] \/ Mutant = "propose_before_active"
    /\ entries' = TRUE
    /\ UNCHANGED <<build, id, joined, advert, active, sees, restarts>>

Terminated == restarts = MaxRestarts /\ UNCHANGED vars

Next ==
  \/ Terminated
  \/
    \E w \in Workers :
        \/ \E b \in {"old", "new"} : Restart(w, b)
        \/ Join(w) \/ Advertise(w) \/ Learn(w) \/ Propose(w) \/ Activate

Spec == Init /\ [][Next]_vars

\* VEC-G14: once a vector entry exists, every member can decode it.
EveryMemberDecodes == entries => \A w \in Workers : joined[w] => build[w] = "new"

=============================================================================
