"""RD1: per-stage means (ms) and native jobs per op across benchmark results.json files: compare.py label=path ..."""
import json, sys, statistics
from collections import defaultdict
STAGES = [("api","http.dispatch.total"),("api","gw.direct.send"),("w1","rpc.direct.serve"),("w1","leader.mutation.total"),
 ("w1","leader.mutation.epochs"),("w1","leader.mutation.table"),("w1","leader.attempt.barrier"),("w1","raft.data.lease_index"),
 ("w1","leader.attempt.read"),("w1","leader.attempt.timestamp_fetch"),("w1","leader.attempt.timestamp"),("w1","ts.client.call"),
 ("w2","ts.serve.total"),("w2","ts.allocate.barrier"),("w1","leader.attempt.propose"),("w1","raft.data.client_write"),
 ("w1","native.query.run"),("w1","native.control.run")]
def load(path):
    r = json.load(open(path))
    ph = [p for p in r['phases'] if not str(p.get('label','')).endswith('warmup')]
    ops = sum(p['statuses'].get('succeeded',0) for p in ph)
    acc = defaultdict(lambda: [0,0])
    for p in ph:
        for proc, sec in ((p.get('stage_summary') or {}).get('direct') or {}).items():
            for n, v in (sec.get('stage_timing') or sec.get('api_stage_timing') or {}).items():
                acc[(proc,n)][0] += v.get('count',0); acc[(proc,n)][1] += v.get('sum_us',0)
    p50 = statistics.median(p['success_latency_ms']['p50'] for p in ph)
    p99 = statistics.median(p['success_latency_ms']['p99'] for p in ph)
    return ops, acc, p50, p99, r.get('correctness_verified')
cols = [(a.split('=')[0], load(a.split('=')[1])) for a in sys.argv[1:]]
print("| stage | " + " | ".join(c[0] for c in cols) + " |"); print("|---|" + "---:|"*len(cols))
print("| client p50 (median of phases) | " + " | ".join(f"{c[1][2]:.2f}" for c in cols) + " |")
print("| client p99 (median of phases) | " + " | ".join(f"{c[1][3]:.2f}" for c in cols) + " |")
for key in STAGES:
    cells=[]
    for _,(ops,acc,_,_,_) in cols:
        c,s = acc.get(key,[0,0])
        if key[1].startswith("native."):
            cells.append(f"{c/ops:.2f}/op × {s/c/1000:.3f}" if c else "–")
        else:
            cells.append(f"{s/c/1000:.3f}" if c else "–")
    print(f"| {key[0]} {key[1]} | " + " | ".join(cells) + " |")
print("| verified | " + " | ".join(str(c[1][4]) for c in cols) + " |")
