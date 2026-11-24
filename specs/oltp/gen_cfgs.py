#!/usr/bin/env python3
"""Generate the OltpRanges TLC configurations: the T1 baselines (one per
profile) and one configuration per mutant.

Run from specs/oltp: python3 gen_cfgs.py. The generated files are checked in;
edit this script, not the .cfg files.
"""
from pathlib import Path

HERE = Path(__file__).resolve().parent

MODEL = """    n1 = n1  n2 = n2  n3 = n3  n4 = n4
    k1 = k1  k2 = k2  c1 = c1  c2 = c2
    g0 = g0  g1 = g1  g2 = g2
    Node <- MCNode
    Spare = n4
    Zone <- MCZone
    Key <- MCKey
    Client <- MCClient
    WKey <- MCWKey
    G0 = g0
    Children <- MCChildren
    ChildSpan <- MCChildSpan"""

SAFETY = ("TypeOK G08_OneLeaderPerTerm G08_OneVotePerTerm G01_CommittedNeverLost "
          "G06_AckedAtOwner G06_NoStaleApply G02_LeaseReadFresh G08_PlacementMatchesData "
          "G08_ZonePlacement G08_SerialTopologyOps G08_EnrolledIdentity")

# Each profile is a T1 exhaustive run: at least 4 nodes or 3 nodes with 2
# ranges, 2 clients, every fault kind its mechanisms depend on at least once,
# and at least two faults that can overlap.
BASE = dict(MaxTerm=2, MaxCrash=1, MaxWipe=0, MaxStale=0, MaxPart=1, MaxLoss=1, MaxDup=1,
            MaxRefuse=0, MaxDrift=0, MaxDriftSteps=0, MaxTime=0, Lease=2,
            MaxTs=4, MaxLog=6, Fixes="{}", InitSplit="FALSE")
PROFILES = {
    # Split of one range into two: stale routes, the freeze, the Catalog
    # publication and the abort.
    "split": dict(BASE, Ops='{"split"}'),
    # Schema fences and run-owner fences over two ranges.
    "ddl": dict(BASE, Ops='{"ddl", "fence"}', InitSplit="TRUE", MaxTs=3),
    # Replica moves, zones, replacement after a wipe or restore, trust, enrollment.
    "member": dict(BASE, Ops='{"move", "enroll"}', MaxWipe=1, MaxStale=1, MaxRefuse=1,
                   MaxTs=3, MaxLog=7),
    # Leader leases and lease reads under clock drift.
    "lease": dict(BASE, Ops='{"lease"}', MaxDrift=1, MaxDriftSteps=2, MaxTime=3, Lease=1,
                  MaxDup=0, MaxTs=3, MaxLog=5),
}


def cfg(profile, mutant="none", expect=None, invariants=SAFETY, symmetry=True,
        overrides=None, comment=None, deadlock=True):
    p = dict(PROFILES[profile])
    p.update(overrides or {})
    lines = []
    if expect:
        lines.append(f"\\* expect: {expect}")
    if comment:
        lines.append(f"\\* {comment}")
    if not expect:
        lines.append("\\* no-mutant: TypeOK -- the type invariant; every run checks it.")
    lines.append("CONSTANTS")
    lines.append(MODEL)
    lines.append(f'    Mutant = "{mutant}"')
    for k, v in p.items():
        lines.append(f"    {k} = {v}")
    lines.append("INIT Init")
    lines.append("NEXT Next")
    lines.append("CONSTRAINT MCConstraint")
    lines.append(f"INVARIANTS {invariants}")
    if symmetry:
        lines.append("SYMMETRY MCSymmetry")
    if deadlock:
        lines.append("CHECK_DEADLOCK TRUE")
    return "\n".join(lines) + "\n"


# Mutants: (name, profile, expected property, overrides, note)
MUTANTS = [
    ("no_frozen_check", "split", "G06_AckedAtOwner", {}, "apply skips the frozen-source check"),
    ("no_cut_check", "split", "G06_AckedAtOwner", {}, "SplitFreeze ignores a direct commit above the cut"),
    ("image_after_cut", "split", "G06_AckedAtOwner", {}, "children import writes above the cut"),
    ("stage_before_freeze", "split", "G06_AckedAtOwner", {}, "children are created before the freeze commits"),
    ("publish_before_children", "split", "G06_AckedAtOwner", {}, "the Catalog publishes before the children exist"),
    ("abort_after_publish", "split", "G06_AckedAtOwner", {}, "the split aborts after the Catalog published it"),
    ("child_stale_schema", "ddl", "G06_NoStaleApply", {"Ops": '{"ddl", "fence", "split"}', "InitSplit": "FALSE", "MaxCrash": 0, "MaxPart": 0},
     "TOPO1 reverted: a child reuses a pre-fence schema epoch"),
    ("no_epoch_check", "ddl", "G06_NoStaleApply", {}, "apply skips the schema and routing epoch check"),
    ("no_authority_check", "ddl", "G06_NoStaleApply", {}, "apply skips the authority fence"),
    ("ddl_publish_first", "ddl", "G06_NoStaleApply", {}, "the Catalog publishes the schema epoch before fencing the Data groups"),
    ("wiped_resumes", "member", "G08_OneVotePerTerm", {}, "RV-REP-3 reverted: a wiped pool resumes its voter identity"),
    ("no_joint", "member", "G08_OneLeaderPerTerm", {}, "a voter replacement in one membership step"),
    ("rep2_any_removal", "member", "G08_PlacementMatchesData", {}, "REP2 reverted: publish certifies any stable voter set"),
    ("no_zone_check", "member", "G08_ZonePlacement", {}, "a move ignores the zone rule"),
    ("control_no_dedup", "member", "G08_EnrolledIdentity", {}, "Control applies a repeated operation ID again"),
    ("split_during_move", "member", "G08_SerialTopologyOps", {"Ops": '{"move", "split", "enroll"}'},
     "a move does not take the lifecycle guard"),
    ("restart_forgets_lease", "lease", "G02_LeaseReadFresh", {}, "a restarted follower votes inside the lease it gave"),
]


def main():
    out = {}
    for name in PROFILES:
        stem = "OltpRanges" if name == "split" else f"OltpRanges.{name}"
        out[f"{stem}.cfg"] = cfg(name, comment=f"T1 baseline, profile {name}")
    (HERE / "mutants").mkdir(exist_ok=True)
    for name, profile, expect, overrides, note in MUTANTS:
        out[f"mutants/{name}.cfg"] = cfg(profile, name, expect, overrides=overrides, comment=note)
    for rel, text in out.items():
        (HERE / rel).write_text(text)
    print("\n".join(sorted(out)))


if __name__ == "__main__":
    main()
