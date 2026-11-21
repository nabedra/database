#!/usr/bin/env python3
"""PERF1: per-stage breakdown from a benchmark results.json (sum of measured phases, warmup excluded)."""
import json, sys, math
from collections import defaultdict

def merge(acc, delta):
    for name, v in delta.items():
        a = acc[name]
        a['count'] += v.get('count', 0); a['sum_us'] += v.get('sum_us', 0)
        for b, c in (v.get('le_us') or {}).items():
            a['le'][b] = a['le'].get(b, 0) + c

def pct(le, count, q):
    if not count: return None
    rank = math.ceil(count * q)
    for b in sorted(le, key=lambda x: float('inf') if x == 'inf' else int(x)):
        rank -= le[b]
        if rank <= 0: return b
    return 'inf'

def main(path, per=None, prefixes=None):
    r = json.load(open(path))
    import os
    rates = os.environ.get('RATES')
    phases = [p for p in r['phases'] if not str(p.get('label','')).endswith('warmup') and (not rates or str(p['rate']) in rates.split(','))]
    ops = sum(p['statuses'].get('succeeded', 0) for p in phases)
    lat = [p['success_latency_ms']['p50'] for p in phases if p.get('success_latency_ms', {}).get('p50')]
    acc = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'sum_us': 0, 'le': {}}))
    for p in phases:
        for proc, sec in ((p.get('stage_summary') or {}).get('direct') or {}).items():
            merge(acc[proc], sec.get('stage_timing') or sec.get('api_stage_timing') or {})
    print(f'# {path}\n# phases={len(phases)} succeeded={ops} client p50 per phase (ms): '
          f'median {sorted(lat)[len(lat)//2] if lat else None}')
    per = per or ops or 1
    print(f"{'process':5} {'stage':42} {'count':>7} {'per_op':>6} {'mean_ms':>8} {'p50<=':>8} {'p99<=':>8} {'ms/op':>7}")
    for proc in sorted(acc):
        for name in sorted(acc[proc]):
            if prefixes and not any(name.startswith(x) for x in prefixes): continue
            a = acc[proc][name]
            if not a['count']: continue
            mean = a['sum_us'] / a['count'] / 1000
            p50 = pct(a['le'], a['count'], .5); p99 = pct(a['le'], a['count'], .99)
            fmt = lambda b: b if b in (None, 'inf') else f'{int(b)/1000:.3g}'
            print(f"{proc:5} {name:42} {a['count']:7} {a['count']/per:6.2f} {mean:8.3f} {fmt(p50):>8} {fmt(p99):>8} {a['sum_us']/1000/per:7.3f}")

if __name__ == '__main__':
    main(sys.argv[1], prefixes=sys.argv[2:] or None)
