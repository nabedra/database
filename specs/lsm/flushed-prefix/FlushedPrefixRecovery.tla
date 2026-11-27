------------------------ MODULE FlushedPrefixRecovery ------------------------
(* LSM-2 refinement: a synced manifest covers the recovery prefix and     *)
(* retains a locator into a recorded journal segment. An earlier record's *)
(* damaged header need not damage the value bytes at that locator.         *)
(*                                                                         *)
(* Start with a flushed value. The crash leaves its manifest durable.      *)
(* Each case adds at most one media fault or an uncertified torn tail.     *)
(* Multiple scopes, other durability cuts and the reopened store are      *)
(* covered by Lsm and Quarantine. This model separates a read-only scan,   *)
(* validation of the underlying and projected segment identities, and     *)
(* destructive repair. It tracks bytes by an intact flag and a segment    *)
(* by its epoch. It does not encode offsets or journal record bytes.       *)
EXTENDS Naturals

CONSTANT Mutant

ASSUME Mutant \in {"none", "prefix_u1_flushed_values", "ignore_flushed_prefix",
                  "preflight_after_cleanup", "skip_certified_gap"}

Cases == {"clean", "flushed_header", "flushed_payload", "torn_tail",
          "missing_segment", "certified_gap"}
PrefixDamage == {"flushed_header", "flushed_payload"}
FlushedSeq == 2
ManifestEpoch == 1

\* For an unknown header, the first valid resync point can sit past
\* flushed_seq + 1. A later durable marker can certify an unflushed loss.
FirstValid(c) == IF c = "certified_gap" THEN 4 ELSE FlushedSeq + 1
LaterDurable(c) == IF c = "certified_gap" THEN 3 ELSE 0
CertifiedUnflushedGap(c) ==
    FirstValid(c) > FlushedSeq + 1 /\ LaterDurable(c) > FlushedSeq

CrashDisk(c) ==
    [epoch |-> IF c = "missing_segment" THEN 0 ELSE ManifestEpoch,
     valueIntact |-> c # "missing_segment",
     orphan |-> TRUE,
     tornTail |-> c = "torn_tail"]

RecordedSegmentLive(d) == d.epoch = ManifestEpoch
LocatorReadable(d) == RecordedSegmentLive(d) /\ d.valueIntact

\* One conservative recovery plan for an unknown header in the prefix:
\* leave the referenced segment untouched and roll the writable tail.
\* For known-length damage wholly in the prefix, advance by its length.
ChooseRepair(c) ==
    CASE c \in PrefixDamage /\ Mutant \in {"prefix_u1_flushed_values", "ignore_flushed_prefix"} ->
             "reepoch"
      [] CertifiedUnflushedGap(c) /\ Mutant # "skip_certified_gap" ->
             "refuse_gap"
      [] c \in {"flushed_header", "certified_gap"} -> "roll"
      [] c = "flushed_payload" -> "skip_covered"
      [] c = "torn_tail" -> "trim_tail"
      [] OTHER -> "cleanup"

ProjectedDisk(d, p) ==
    CASE p = "reepoch" -> [d EXCEPT !.epoch = ManifestEpoch + 1, !.valueIntact = FALSE]
      [] p = "trim_tail" -> [d EXCEPT !.tornTail = FALSE, !.orphan = FALSE]
      [] OTHER -> [d EXCEPT !.orphan = FALSE]

RefusedBeforeWrites(d, p) ==
    \/ ~RecordedSegmentLive(d)
    \/ ~RecordedSegmentLive(ProjectedDisk(d, p))
    \/ p = "refuse_gap"

CheckBeforeRepair == Mutant \notin {"prefix_u1_flushed_values", "preflight_after_cleanup"}

VARIABLES scenario, phase, disk, repair, nextSeq

vars == <<scenario, phase, disk, repair, nextSeq>>

Init ==
    /\ phase = "live" /\ scenario = "clean"
    /\ disk = CrashDisk("clean")
    /\ repair = "none" /\ nextSeq = 0

Crash(c) ==
    /\ phase = "live"
    /\ scenario' = c /\ disk' = CrashDisk(c) /\ phase' = "down"
    /\ UNCHANGED <<repair, nextSeq>>

ReadOnlyScan ==
    /\ phase = "down"
    /\ repair' = ChooseRepair(scenario) /\ phase' = "planned"
    /\ UNCHANGED <<scenario, disk, nextSeq>>

Preflight ==
    /\ phase = "planned"
    /\ phase' = IF CheckBeforeRepair /\ RefusedBeforeWrites(disk, repair)
                THEN "refused" ELSE "repair"
    /\ UNCHANGED <<scenario, disk, repair, nextSeq>>

ApplyRepair ==
    /\ phase = "repair"
    /\ disk' = ProjectedDisk(disk, repair)
    /\ nextSeq' = FlushedSeq + 1
    /\ phase' = "repair_applied"
    /\ UNCHANGED <<scenario, repair>>

FinishOpen ==
    /\ phase = "repair_applied"
    /\ phase' = IF RecordedSegmentLive(disk) /\ repair # "refuse_gap"
                THEN "open" ELSE "refused"
    /\ UNCHANGED <<scenario, disk, repair, nextSeq>>

Done ==
    /\ phase \in {"open", "refused"}
    /\ UNCHANGED vars

Next == (\E c \in Cases : Crash(c)) \/ ReadOnlyScan \/ Preflight
        \/ ApplyRepair \/ FinishOpen \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ scenario \in Cases
    /\ phase \in {"live", "down", "planned", "repair", "repair_applied", "open", "refused"}
    /\ disk.epoch \in 0..(ManifestEpoch + 1)
    /\ {disk.valueIntact, disk.orphan, disk.tornTail} \subseteq BOOLEAN
    /\ repair \in {"none", "reepoch", "refuse_gap", "roll", "skip_covered", "trim_tail", "cleanup"}
    /\ nextSeq \in {0, FlushedSeq + 1}

\* The original locator retains its epoch and surviving bytes throughout open.
G03_RecordedLocatorPreserved == scenario # "missing_segment" => LocatorReadable(disk)

\* Damage wholly covered by the flush can still open when the intact value
\* is present. A preflight that merely refuses invalid truncation is safe
\* for the bytes but leaves the availability bug.
G03_CoveredDamageCanOpen == scenario \in PrefixDamage => phase # "refused"

\* The validity check precedes even otherwise safe orphan cleanup.
G03_RefusalBeforeRepair == scenario = "missing_segment" => disk = CrashDisk(scenario)

\* Manifest coverage is finite. A later certificate of an unflushed gap
\* of unknown scope still takes the existing pool-refusal path.
G03_UnflushedCertificateHonored == CertifiedUnflushedGap(scenario) => phase # "open"

=============================================================================
