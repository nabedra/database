--------------------------- MODULE EventsTime ---------------------------
EXTENDS Integers, FiniteSets, TLC

CONSTANTS Nodes, Producers, Consumers, MaxTime, Age, Error, Retention, Ttl,
          Uncertainty, Mutant
VARIABLES now, proof, lastPub, events, lease, earlyRemoval, earlyExpiry, proofOptions

vars == <<now, proof, lastPub, events, lease, earlyRemoval, earlyExpiry, proofOptions>>
NoProof == [at |-> -1, lower |-> 0, upper |-> 0]
NoLease == [holder |-> {}, at |-> 0, expiry |-> 0]
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

\* Honest clock samples came from a bounded round ending at t. At most one
\* voter can return an arbitrarily inaccurate reading. Extremes beyond this
\* finite projection cannot enter an accepted interval without an honest bound.
RECURSIVE SampleClocks(_, _)
SampleClocks(voters, bad) ==
    IF voters = {} THEN {<<>>}
    ELSE LET n == CHOOSE v \in voters : TRUE
             tail == SampleClocks(voters \ {n}, bad)
             values == IF n \in bad THEN (-Age - 4 * Error)..(4 * Error)
                       ELSE (-Age - Error)..Error
         IN UNION {{[v \in voters |-> IF v = n THEN t ELSE rest[v]] :
                    t \in values} : rest \in tail}

\* Use the second-largest lower endpoint and the second-smallest upper endpoint.
\* Two voters give the union envelope. Three voters discard a single outlier.
Bounds(voters, clocks) ==
    LET low == [n \in voters |-> clocks[n] - Error]
        high == [n \in voters |-> clocks[n] + Error + Age]
        secondLow == {low[n] : n \in {x \in voters :
                      Cardinality({v \in voters : low[v] >= low[x]}) >= 2}}
        secondHigh == {high[n] : n \in {x \in voters :
                       Cardinality({v \in voters : high[v] <= high[x]}) >= 2}}
    IN [lower |-> IF Mutant = "one_fast_lower" THEN Max({low[n] : n \in voters})
                  ELSE Max(secondLow),
        upper |-> IF Mutant = "unguarded_upper" THEN Min({high[n] : n \in voters})
                  ELSE Min(secondHigh)]

Intervals ==
    LET options == UNION {UNION {{Bounds(voters, clocks) :
                             clocks \in SampleClocks(voters, bad)} :
                          bad \in {s \in SUBSET voters : Cardinality(s) <= 1}} :
                         voters \in {s \in SUBSET Nodes : Cardinality(s) >= 2}}
    IN {b \in options : b.lower <= b.upper /\ b.upper - b.lower <= Uncertainty}

Init ==
    /\ now = 0
    /\ proof = NoProof
    /\ lastPub = 0
    /\ events = {}
    /\ lease = NoLease
    /\ earlyRemoval = FALSE
    /\ earlyExpiry = FALSE
    /\ proofOptions = Intervals

Collect ==
    /\ proof.at # now
    /\ \E b \in proofOptions :
        proof' = [at |-> now, lower |-> now + b.lower, upper |-> now + b.upper]
    /\ UNCHANGED <<now, lastPub, events, lease, earlyRemoval, earlyExpiry, proofOptions>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<proof, lastPub, events, lease, earlyRemoval, earlyExpiry, proofOptions>>

Fresh == proof.at = now \/ (Mutant = "reuse_expired_proof" /\ proof.at >= 0)
Active == {e \in events : ~e.removed}

Publish(p) ==
    /\ Fresh
    /\ \A e \in events : e.by # p
    /\ lastPub' = Max({lastPub, proof.upper})
    /\ events' = events \cup {[by |-> p, stamp |-> lastPub', at |-> now, removed |-> FALSE]}
    /\ UNCHANGED <<now, proof, lease, earlyRemoval, earlyExpiry, proofOptions>>

Retain ==
    LET cutoff == IF Mutant = "upper_cutoff" THEN proof.upper ELSE proof.lower
        expired == {e \in Active : e.stamp + Retention < cutoff}
    IN /\ Fresh /\ expired # {}
       /\ earlyRemoval' = (earlyRemoval \/ \E e \in expired : now < e.at + Retention)
       /\ events' = (events \ expired) \cup {[e EXCEPT !.removed = TRUE] : e \in expired}
       /\ UNCHANGED <<now, proof, lastPub, lease, earlyExpiry, proofOptions>>

Claim(c) ==
    /\ Fresh
    /\ c \notin lease.holder
    /\ lease.holder = {} \/
       (IF Mutant = "upper_expiry" THEN proof.upper ELSE proof.lower) >= lease.expiry
    /\ lease' = [holder |-> {c}, at |-> now, expiry |-> proof.upper + Ttl]
    /\ earlyExpiry' = (earlyExpiry \/ (lease.holder # {} /\ now < lease.at + Ttl))
    /\ UNCHANGED <<now, proof, lastPub, events, earlyRemoval, proofOptions>>

Done == /\ now = MaxTime /\ UNCHANGED vars

Next ==
    \/ Collect
    \/ Tick
    \/ \E p \in Producers : Publish(p)
    \/ Retain
    \/ \E c \in Consumers : Claim(c)
    \/ Done

G08_RetentionAge == ~earlyRemoval
G05_LeaseAge == ~earlyExpiry
TrustedProof == proof = NoProof \/
    (proof.lower <= proof.at /\ proof.at <= proof.upper)
FreshPublishTime == \A e \in events : e.at <= e.stamp
=============================================================================
