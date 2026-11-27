"""Summarizes JSON lines of bench_local.rs or compete.py: median and spread over repetitions, and per
operation and filter case the cheapest setting that reaches recall@10 >= 0.95 (or the best recall
when none does).

    python -I summarize.py results.jsonl [--target 0.95]
"""

import argparse
import json
import statistics
from collections import defaultdict


def setting(p):
    """bench_local: stop=estimate target=r, stop=units units=n, stop=exact; compete: stop=<param>, <param>=v."""
    stop = p.get("stop", "?")
    for k in ("target", "units", stop):
        if k in p:
            return f"{stop}={p[k]}"
    return stop


def spread(values):
    if len(values) < 2:
        return ""
    return f" ±{(max(values) - min(values)) / 2:.3g}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--target", type=float, default=0.95)
    a = ap.parse_args()
    points, other = defaultdict(list), []
    with open(a.path) as f:
        for line in f:
            p = json.loads(line)
            if p["kind"] != "point":
                other.append(p)
                continue
            system = p.get("system", "nabedra")
            key = (system, p.get("recovered", False), p["op"], p["case"], setting(p), p.get("limit", 10))
            points[key].append(p)
    contended = any(p.get("meta", {}).get("contended") for ps in points.values() for p in ps)
    print(f"# {a.path}\n")
    if contended:
        print("**CONTENDED: other agents were building; these numbers are not results.**\n")
    for p in other:
        fields = {k: v for k, v in p.items() if k not in ("meta", "kind")}
        print(f"- {p['kind']}: {json.dumps(fields)}")
    print("\n| system | op | case | setting | limit | recall | units | p50 ms | p99 ms | QPS 1/4/8 |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    rows = []
    for key, ps in sorted(points.items()):
        med = lambda k: statistics.median(p[k] for p in ps)  # noqa: E731
        qps = "/".join(
            f"{statistics.median(p['qps'][t] for p in ps):.0f}" for t in sorted(ps[0]["qps"], key=int))
        rec = [p["recall"] for p in ps]
        system, rec_flag, op, case, s, limit = key
        name = system + (" (recovered)" if rec_flag else "")
        units = f"{med('mean_units'):.0f}" if "mean_units" in ps[0] else ""
        print(f"| {name} | {op} | {case} | {s} | {limit} | {statistics.median(rec):.4f}{spread(rec)} | {units} | "
              f"{med('p50_ms'):.3f} | {med('p99_ms'):.3f}{spread([p['p99_ms'] for p in ps])} | {qps} |")
        rows.append((key, statistics.median(rec), med("mean_ms"), med("p99_ms"), ps))
    print(f"\n## Where recall@10 >= {a.target} is reached\n")
    print("| system | op | case | cheapest setting | recall | p99 ms | QPS 1/4/8 | best recall if missed |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    groups = defaultdict(list)
    for key, rec, mean, p99, ps in rows:
        if "exact" in key[4]:
            continue
        groups[key[:4]].append((key, rec, mean, p99, ps))
    for g, items in sorted(groups.items()):
        ok = [i for i in items if i[1] >= a.target]
        name = g[0] + (" (recovered)" if g[1] else "")
        if ok:
            key, rec, mean, p99, ps = min(ok, key=lambda i: i[2])
            qps = "/".join(f"{statistics.median(p['qps'][t] for p in ps):.0f}" for t in sorted(ps[0]["qps"], key=int))
            print(f"| {name} | {g[2]} | {g[3]} | {key[4]} limit {key[5]} | {rec:.4f} | {p99:.3f} | {qps} | |")
        else:
            key, rec = max(((i[0], i[1]) for i in items), key=lambda i: i[1])
            print(f"| {name} | {g[2]} | {g[3]} | NOT REACHED | | | | {rec:.4f} at {key[4]} limit {key[5]} |")


if __name__ == "__main__":
    main()
