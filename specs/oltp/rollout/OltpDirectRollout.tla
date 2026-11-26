------------------------ MODULE OltpDirectRollout ------------------------
(* OLTP direct-path plan, "Retry results" and "Cluster features".          *)
(* All builds know data.direct.v1. Only the new build understands a       *)
(* DirectCommit member that records a result without writing a row.        *)
(* Control and both Data ranges have the same three replica slots.         *)
EXTENDS Naturals, FiniteSets

CONSTANTS Node, Range, Client, ClientRange, ClientResult, MaxResets, Mutant

ASSUME /\ Cardinality(Node) = 3
       /\ Cardinality(Range) = 2
       /\ Cardinality(Client) = 2
       /\ ClientRange \in [Client -> Range]
       /\ \A r \in Range : \E c \in Client : ClientRange[c] = r
       /\ ClientResult \in [Client -> {"no-op", "row-conflict"}]

VARIABLES
    incarnation, advert, capable, admitted, resets,
    enabled, everEnabled, direct,
    phase, lastReply

members == <<incarnation, advert, capable, admitted, resets>>
feature == <<enabled, everEnabled>>
requests == <<phase, lastReply>>
vars == <<members, feature, direct, requests>>

CurrentAdvert(n) == advert[n] = incarnation[n]
Quorum == Cardinality(admitted) >= 2
AllAdmittedAdvertised == \A n \in admitted : CurrentAdvert(n)
CanActivate == Quorum /\ ~enabled /\ AllAdmittedAdvertised

Init ==
    /\ incarnation = [n \in Node |-> 1]
    /\ advert = [n \in Node |-> 0]
    /\ capable = {}
    /\ admitted = Node
    /\ resets = 0
    /\ enabled = FALSE /\ everEnabled = FALSE
    /\ direct = Node
    /\ phase = [r \in Range |-> "idle"]
    /\ lastReply = [r \in Range |-> "none"]

\* A rolling binary upgrade with the same disk and identity.
Upgrade(n) ==
    /\ n \notin capable
    /\ capable' = capable \cup {n}
    /\ UNCHANGED <<incarnation, advert, admitted, resets, feature, direct, requests>>

\* A down-level replacement starts with a fresh incarnation and no rights
\* to vote or serve. Keep its old receipt to exercise identity matching.
\* Other nodes may reset before it has rejoined.
Reset(n) ==
    /\ n \in admitted /\ resets < MaxResets
    /\ incarnation' = [incarnation EXCEPT ![n] = @ + 1]
    /\ admitted' = admitted \ {n}
    /\ capable' = capable \ {n}
    /\ resets' = resets + 1
    /\ UNCHANGED <<advert, feature, direct, requests>>

\* Control records the authenticated capable build's current identity.
\* A candidate can advertise before becoming a Control/Data replica.
Advertise(n) ==
    /\ Quorum /\ n \in capable /\ ~CurrentAdvert(n)
    /\ advert' = [advert EXCEPT ![n] = incarnation[n]]
    /\ UNCHANGED <<incarnation, capable, admitted, resets, feature, direct, requests>>

\* Abstracts the atomic Control activation check on the advert path.
EnableFeature ==
    /\ Quorum /\ ~enabled /\ Mutant # "skip_activation"
    /\ AllAdmittedAdvertised
       \/ (Mutant = "enable_early" /\ \E n \in admitted : CurrentAdvert(n))
    /\ enabled' = TRUE /\ everEnabled' = TRUE
    /\ UNCHANGED <<members, direct, requests>>

\* Model the capability gate before the new identity hosts any replica.
Admit(n) ==
    /\ Quorum /\ n \notin admitted
    /\ ~enabled \/ CurrentAdvert(n) \/ (Mutant = "allow_stale_advert" /\ advert[n] > 0)
    /\ admitted' = admitted \cup {n}
    /\ UNCHANGED <<incarnation, advert, capable, resets, feature, direct, requests>>

\* The operational assumption is necessary from the moment this
\* incarnation advertises. A durable receipt remains after a binary
\* restart. One mutant permits this before enablement, another permits a
\* downgrade after a new-format result is already durable.
DowngradeInPlace(n) ==
    /\ n \in capable
    /\ \/ ~enabled /\ (~CurrentAdvert(n) \/ Mutant = "downgrade_before_enable")
       \/ /\ Mutant = "downgrade_after_enable" /\ enabled /\ n \in admitted
          /\ \E r \in Range : phase[r] \in {"recorded", "replayed"}
    /\ capable' = capable \ {n}
    /\ UNCHANGED <<incarnation, advert, admitted, resets, feature, direct, requests>>

\* direct_oltp controls routing per worker. It never clears the durable
\* capability or prevents an already proposed record from applying.
FlipDirect(n) ==
    /\ n \in admitted
    /\ direct' = IF n \in direct THEN direct \ {n} ELSE direct \cup {n}
    /\ enabled' =
        IF Mutant = "flag_clears_feature" /\ n \in direct THEN FALSE ELSE enabled
    /\ UNCHANGED <<members, everEnabled, requests>>

\* One identity per client and range. The general retry and row-version
\* rules are in OltpDirect. The focus here is the new entry shape and its
\* stable result through binary rollout and a route change.
ProposeOutcome(c, n) ==
    LET r == ClientRange[c] IN
    /\ Quorum /\ n \in (admitted \cap capable \cap direct)
    /\ enabled \/ Mutant = "propose_before_gate"
    /\ phase[r] = "idle"
    /\ phase' = [phase EXCEPT ![r] = "proposed"]
    /\ UNCHANGED <<members, feature, direct, lastReply>>

CommitOutcome(c) ==
    LET r == ClientRange[c] IN
    /\ Quorum /\ phase[r] = "proposed"
    /\ phase' = [phase EXCEPT ![r] = "recorded"]
    /\ lastReply' = [lastReply EXCEPT ![r] = ClientResult[c]]
    /\ UNCHANGED <<members, feature, direct>>

RetryOutcome(c, n) ==
    LET r == ClientRange[c] IN
    /\ Quorum /\ n \in admitted /\ phase[r] = "recorded"
    /\ phase' = [phase EXCEPT ![r] = "replayed"]
    /\ lastReply' = [lastReply EXCEPT ![r] =
         IF Mutant = "recompute_on_rollback" /\ n \notin direct THEN "recomputed"
         ELSE lastReply[r]]
    /\ UNCHANGED <<members, feature, direct>>

Terminal ==
    /\ \A r \in Range : phase[r] = "replayed"
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Node : Upgrade(n)
    \/ \E n \in Node : Reset(n)
    \/ \E n \in Node : Advertise(n)
    \/ EnableFeature
    \/ \E n \in Node : Admit(n)
    \/ \E n \in Node : DowngradeInPlace(n)
    \/ \E n \in Node : FlipDirect(n)
    \/ \E c \in Client, n \in Node : ProposeOutcome(c, n)
    \/ \E c \in Client : CommitOutcome(c)
    \/ \E c \in Client, n \in Node : RetryOutcome(c, n)
    \/ Terminal

TypeOK ==
    /\ incarnation \in [Node -> 1..(MaxResets + 1)]
    /\ advert \in [Node -> 0..(MaxResets + 1)]
    /\ \A n \in Node : advert[n] <= incarnation[n]
    /\ capable \subseteq Node /\ admitted \subseteq Node /\ direct \subseteq Node
    /\ resets \in 0..MaxResets
    /\ enabled \in BOOLEAN /\ everEnabled \in BOOLEAN
    /\ phase \in [Range -> {"idle", "proposed", "recorded", "replayed"}]
    /\ lastReply \in [Range -> {"none", "no-op", "row-conflict", "recomputed"}]

\* OLTP-G03. Every active voter or learner after activation has a
\* current advert and a binary that understands recorded outcomes.
G03_EnabledMembersUnderstand ==
    enabled => \A n \in admitted : n \in capable /\ CurrentAdvert(n)

\* OLTP-G03. The encoded proposal itself is forbidden before activation.
G03_ProposalHasFeature ==
    (\E r \in Range : phase[r] # "idle") => enabled

\* OLTP-G03. A runtime routing flag flip cannot change format support.
G03_FeatureStaysEnabled == everEnabled => enabled

\* OLTP-G03. Replay at a gateway with its flag off still returns the
\* original durable result of the client identity.
G03_RecordedOutcomeStable ==
    \A c \in Client : phase[ClientRange[c]] \in {"recorded", "replayed"}
        => lastReply[ClientRange[c]] = ClientResult[c]

\* While Control can apply a capability command and every admitted
\* member remains eligible, processing advertisements drives activation.
LiveSpec == Init /\ [][Next]_vars /\ WF_vars(EnableFeature)
G10_EligibleClusterEnables == CanActivate ~> (enabled \/ ~CanActivate)
========================================================================
