------------------------- MODULE EventsAckGap -------------------------
EXTENDS EventsPartition

CONSTANTS Producer, Holder, Standby
VARIABLE verdict

AckItems ==
    [p \in {Producer} |->
        << <<[id |-> "first", body |-> "one"],
             [id |-> "second", body |-> "two"],
             [id |-> "third", body |-> "three"]>> >>]

GapState ==
    LET published == Step(Init0, Cmd("publish", PubReq(Producer, 1), 1, 0)).s
        claimed == Step(published, Cmd("claim", ClaimReq(Holder, 1), 1, 0)).s
        firstAck == Step(claimed, Cmd("ack", AckReq(Holder, 1, 0, 2, 1), 1, 0)).s
    IN Step(firstAck, Cmd("retain", NoReq, 2, 2)).s

Cases ==
    [
        delivered |-> [request |-> AckReq(Holder, 1, 1, 2, 2), expected |-> "ckpt", after |-> 2],
        old_token |-> [request |-> AckReq(Holder, 1, 0, 2, 2), expected |-> "expired", after |-> 1],
        old_generation |-> [request |-> AckReq(Holder, 2, 1, 2, 2), expected |-> "lost", after |-> 1],
        other_holder |-> [request |-> AckReq(Standby, 1, 1, 2, 2), expected |-> "lost", after |-> 1],
        beyond_delivered |-> [request |-> AckReq(Holder, 1, 1, 2, 3), expected |-> "invalid", after |-> 1]
    ]

Init == verdict = "unchecked"
Check ==
    /\ verdict = "unchecked"
    /\ \E name \in DOMAIN Cases :
        LET c == Cases[name]
            r == Step(GapState, Cmd("ack", c.request, 2, 0))
        IN verdict' = IF r.r.type = c.expected /\ r.s.ckpt = c.after THEN "correct" ELSE "wrong"
Done == verdict # "unchecked" /\ UNCHANGED verdict
Next == Check \/ Done

G05_AckAcrossGap ==
    /\ GapState.ckpt = 1
    /\ FirstCkpt(GapState) = 3
    /\ verdict # "wrong"
=========================================================================
