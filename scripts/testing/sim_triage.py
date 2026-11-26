#!/usr/bin/env python3
"""Sort the seeds of an OLTP simulation sweep into clean, red with a named cause, and red undiagnosed.

It reads sweep reports and the violations of their red seeds. A cause is named only from what a
diagnostic run of the same seed recorded, and only when that run had the same violation kinds and, byte for
byte, the same client history: instrumentation can change a schedule, and then it is another run. Every
violation of a seed needs its own evidence. Anything else is undiagnosed: an allocation site that this
file does not list, bytes that the evidence does not add up to, and a holder that is only inferred.

Nothing here is evidence for a guarantee. A named seed is as red as any other.
"""

import argparse
import collections
import csv
import glob
import gzip
import hashlib
import json
import pathlib
import re
import sys

# A leased record of the diagnostic build is this much larger than the product's.
INSTRUMENTATION = 8
SITES = {
    "control-route-cache": ("replica_routes", "ReplicaRouteCache"),
    "read-batch": ("read_batch", "ReadBatch"),
    "raft-log-batch": ("next_stored",),
}
# A Raft batch may belong to the stream to a removed replica, or to any other. The allocation site does
# not say which, so it names nothing.
INFERRED = {"raft-log-batch"}
SAMPLES = ["e0", "e1", "e2", "e3", "e4", "e5", "e6"]
FREEZE = "source refused the exact split freeze"
HELD = "captured schema or lifecycle changed"
# The survivors answer, and still list the lost worker. A survivor that does not answer, or a cold start
# that fails afterwards, has another reason.
NEVER_DROPPED = re.compile(r't=\d+ms Liveness: (?:n\d did not restart: |same-run restart failed after the heal: \["n\d: )'
                           r'survivors never dropped n\d: readiness deadline for [^(]*\[\(\d, \\?"200 ')
ACTIVE_MOVE = "Control refuses fences while a replica move is active"


# The charges of a group's Raft arenas and handle, of a pool, and of a cached route.
GROUP, POOL, ROUTE = (8 << 20) + (64 << 10), 64 << 20, 692
TOPOLOGY = "matches topology growth"


def topology(violation):
    """Whether every ledger that moved grew by whole groups, pools and cached routes. That is what a split
    or a move adds, and whether it should have been released is not shown, so it names no cause."""
    moved = re.findall(r"\w+(?: of group [0-9a-f-]{36})? (\d+) \(baseline (\d+)\)", violation)
    grown = [int(now) - int(before) for now, before in moved]
    large = [size for size in grown if size >= GROUP]

    def fits(size):
        return any((size - groups * GROUP - pools * POOL) in range(0, 17 * ROUTE, ROUTE)
                   for groups in range(9) for pools in range(3) if groups or pools)

    return bool(large) and all(fits(size) if size >= GROUP else size in range(0, 17 * ROUTE, ROUTE) for size in grown)


def digest(lines):
    return hashlib.sha256("\n".join(line.strip() for line in lines).encode()).hexdigest()


def kind(violation):
    found = re.match(r"t=\d+ms ([\w-]+):", violation)
    return found[1] if found else "unreadable"


def site(stack):
    head = stack.split(" ## ")[0]
    return next((name for name, marks in SITES.items() if any(mark in head for mark in marks)), None)


class Leases:
    """The scratch leases of one seed's diagnostic run, by node and sample."""

    def __init__(self):
        self.rows = collections.defaultdict(dict)

    def add(self, line):
        found = re.match(r"SCR (\w+) n(\d+) #(\d+) (\d+)B t(\d+) ?(.*)", line)
        if found:
            label, node, lease, size, _, stack = found.groups()
            self.rows[int(node), label][int(lease)] = (int(size), site(stack.strip()))
        return bool(found)

    def explain(self, node, growth):
        """The holders of `growth` bytes on a node, or (None, why not)."""
        base, after = self.rows.get((node, "base")), self.rows.get((node, "e0"))
        if base is None or after is None:
            return None, "no lease sample of this node"
        grown = collections.Counter()
        count = collections.Counter()
        for sign, rows in ((-1, base), (1, after)):
            for size, where in rows.values():
                grown[where] += sign * size
                count[where] += sign
        found = sum(grown.values()) - INSTRUMENTATION * count["control-route-cache"]
        if found != growth:
            return None, f"the sampled leases grew by {found} B and the violation says {growth} B"
        holders = sorted((where or "unknown site") for where, size in grown.items() if size)
        if "unknown site" in holders:
            return None, "an unknown allocation site holds part of it"
        inferred = INFERRED.intersection(holders)
        if inferred:
            return None, f"inferred only: {'+'.join(sorted(inferred))}"
        for where in holders:
            if grown[where] > 0 and where == "control-route-cache":
                kept = [{i for i, (_, at) in self.rows.get((node, label), {}).items() if at == where}
                        for label in SAMPLES]
                if not set(after).intersection(*kept):
                    return None, "the route cache is not held in every sample"
        return "+".join(holders), None


def usage(violation):
    """The node and the growth of `working_decode_bytes`, if nothing else grew. A scratch lease is charged to the
    scheduler and to the scratch gauge as well, so with the probes both ledgers may have grown by the same bytes."""
    found = (re.search(r"n(\d+) has (ResourceUsage .*) after quiescence, baseline (ResourceUsage .*)$", violation)
             or re.search(r"n(\d+) did not stay at its baseline .*, last off by (ResourceUsage .*) "
                          r"\(baseline (ResourceUsage .*)\)$", violation)
             or re.search(r"n(\d+) did not stay at its baseline .*, last off by SchedulerBytes \d+ \(baseline \d+\), "
                          r"ScratchBytes \d+ \(baseline \d+\), (ResourceUsage .*) \(baseline (ResourceUsage .*)\)$",
                          violation))
    if not found:
        return None
    node, now, before = int(found[1]), found[2], found[3]
    number = re.compile(r"working_decode_bytes: (\d+)")
    if number.sub("", now) != number.sub("", before):
        return None
    growth = int(number.search(now)[1]) - int(number.search(before)[1])
    ledgers = re.findall(r"(?:SchedulerBytes|ScratchBytes) (\d+) \(baseline (\d+)\)", violation)
    if any(int(after) - int(base) != growth for after, base in ledgers):
        return None
    return node, growth


class Diagnosis:
    """What the diagnostic run of one seed recorded."""

    def __init__(self, active_move_at=None):
        self.kinds = None
        # A digest of its client history.
        self.history = None
        # Where the diagnosed source refuses a command because a replica move is active, as file:line. A
        # `Busy` from anywhere else has another reason.
        self.active_move_at = active_move_at
        self.leases = Leases()
        self.busy_active_move = 0
        # Split plans refused at the freeze, with DirectCommitAboveCut, and for each plan refused before
        # it, the plan that held the table's lifecycle.
        self.above_cut = 0
        self.frozen = set()
        self.holders = []

    def add(self, line):
        if self.leases.add(line):
            return
        found = re.search(r"nabedra-sim-seed: (\{.*\})", line)
        if found:
            self.kinds = set(json.loads(found[1])["kinds"])
        elif "FREEZE DirectCommitAboveCut" in line:
            self.above_cut += 1
        elif "SPLIT-FREEZE-REFUSED" in line:
            self.frozen.update(re.findall(r"plan ([0-9a-f-]{36})", line))
        elif "SPLIT-PLAN-REFUSED" in line:
            held = re.search(r"holder Ok\(Some\(\(TxnId \{ request_id: ([0-9a-f-]{36})", line)
            self.holders.append(held and held[1])
        elif self.active_move_at and re.search(rf"CONTROL-FAULT Busy \S*{re.escape(self.active_move_at)}\b", line):
            self.busy_active_move += 1


def cause(violation, diagnosis):
    """(cause, None) from evidence, or (None, why not)."""
    name = kind(violation)
    if name == "ResourceBaseline":
        grown = usage(violation)
        if grown is None:
            return None, TOPOLOGY if topology(violation) else "another gauge than the decode bytes"
        holders, why = diagnosis.leases.explain(*grown)
        return (f"holder: {holders}", None) if holders else (None, why)
    if name == "Liveness" and "the split did not publish" in violation:
        start = violation.find('"last":')
        last = json.JSONDecoder().raw_decode(violation[start + 7:])[0] if start >= 0 else []
        if not last or FREEZE not in last[0] or not all(FREEZE in one or HELD in one for one in last):
            return None, "the plans did not all end with a refused freeze or a held lifecycle"
        if not diagnosis.above_cut or not diagnosis.frozen:
            return None, "no refused freeze was recorded"
        if any(HELD in one for one in last) and not (
                diagnosis.holders and all(holder in diagnosis.frozen for holder in diagnosis.holders)):
            return None, "another holder than the refused plan, or none, was recorded"
        return f"a direct commit above the cut refused the first plan, which then held the range against {len(last) - 1}", None
    if NEVER_DROPPED.match(violation):
        if diagnosis.busy_active_move > 100:
            return f"{ACTIVE_MOVE} ({diagnosis.busy_active_move} refusals)", None
        return None, "no refused fence was recorded"
    return None, "no rule names a cause"


def classify(row, violations, diagnosis, history=None):
    """(verdict, causes) of one seed of a sweep report."""
    if row["verdict"] != "red":
        return row["verdict"], []
    if not violations:
        return "undiagnosed", ["no violation text"]
    if diagnosis is None or diagnosis.kinds is None:
        return "undiagnosed", ["no diagnostic run"]
    if diagnosis.kinds != set(row["violations"]):
        return "undiagnosed", ["the diagnostic run had other violations"]
    if history is None or diagnosis.history != history:
        return "undiagnosed", ["the diagnostic run had another client history"]
    causes, named = [], True
    for violation in violations:
        found, why = cause(violation, diagnosis)
        named &= found is not None
        causes.append(f"{kind(violation)}: {found or 'OPEN, ' + why}")
    return ("named" if named else "undiagnosed"), causes


def verdict(row):
    """The sweep's verdict. Reports from before it had one: never clean without a replay, and
    progress is unknown, so `clean` needs a record of both."""
    if "verdict" in row:
        return row["verdict"]
    if row["violations"] or row.get("deterministic") is False:
        return "red"
    return "clean" if row["replayed"] and row.get("chaos_writes") and row.get("chaos_reads") else "unreplayed"


def load(roots):
    rows = {}
    for report in (path for root in roots for path in glob.glob(f"{root}/**/sweep-report.json", recursive=True)):
        base = pathlib.Path(report).parent
        for row in json.loads(pathlib.Path(report).read_text())["seeds"]:
            seed = row["seed"]
            found = glob.glob(f"{base}/*-seed-{seed}/summary.json")
            violations = json.loads(pathlib.Path(found[0]).read_text())["violations"] if found else []
            if seed in rows:
                raise SystemExit(f"seed {seed} is in two reports")
            path = pathlib.Path(found[0]).with_name("history.jsonl") if found else None
            history = digest(path.read_text().splitlines()) if path and path.exists() else None
            rows[seed] = (dict(row, verdict=verdict(row)), violations, history)
    return rows


def diagnoses(paths, active_move_at=None):
    out, seed = {}, None
    for path in paths:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", errors="replace") as lines:
            for line in lines:
                found = re.search(r"SCRSEED (\d+)", line)
                if found:
                    seed = int(found[1])
                    # The last run of a seed counts.
                    out[seed] = Diagnosis(active_move_at)
                elif seed is not None:
                    out[seed].add(line)
    return out


def add_histories(known, paths):
    """The client history of each diagnostic run, from its trace `s<seed>.trace[.gz]`."""
    for path in paths:
        seed = int(re.search(r"s(\d+)\.trace", path)[1])
        if seed in known:
            with (gzip.open if path.endswith(".gz") else open)(path, "rt", errors="replace") as lines:
                events = (re.match(r"\d+ history (\{.*\})$", line) for line in lines)
                known[seed].history = digest(event[1] for event in events if event)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", help="directories of downloaded sweep artifacts, one per shard")
    parser.add_argument("--diagnostics", nargs="*", default=[], help="logs of diagnostic runs")
    parser.add_argument("--traces", nargs="*", default=[], help="traces of the diagnostic runs, s<seed>.trace.gz")
    parser.add_argument("--active-move-at", help="file:line of the refusal while a replica move is active")
    parser.add_argument("--csv")
    args = parser.parse_args(argv)
    known = diagnoses(args.diagnostics, args.active_move_at)
    add_histories(known, args.traces)
    table, total, per = [], collections.Counter(), collections.defaultdict(collections.Counter)
    why = collections.Counter()
    for seed, (row, violations, history) in sorted(load(args.reports).items()):
        result, causes = classify(row, violations, known.get(seed), history)
        total[result] += 1
        per[row["workload"]][result] += 1
        why.update(re.sub(r"\d+", "#", text) for text in causes if result == "undiagnosed")
        table.append([seed, row["workload"], row["profile"], row["trace_hash"], result, "; ".join(causes)])
    if not table:
        raise SystemExit("no sweep report found")
    if args.csv:
        with open(args.csv, "w", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(["seed", "workload", "profile", "trace_hash", "status", "causes"])
            writer.writerows(table)
    print(len(table), "seeds", dict(total), {name: dict(counts) for name, counts in sorted(per.items())})
    for text, count in why.most_common(30):
        print(f"{count:5} {text}")


if __name__ == "__main__":
    sys.exit(main())
