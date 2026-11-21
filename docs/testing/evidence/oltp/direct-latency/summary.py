#!/usr/bin/env python3
"""PERF1: NS11-style per-rate cells (acknowledged/offered, median rate, median p50/p99) for result dirs."""
import json, sys, statistics
from collections import defaultdict
def cells(path):
    r = json.load(open(path if path.endswith('.json') else f'{path}/results.json'))
    by = defaultdict(list)
    for p in r['phases']:
        if str(p.get('label', '')).endswith('warmup'):
            continue
        by[p['rate']].append(p)
    out = {}
    for rate, ps in sorted(by.items()):
        ok = sum(p['statuses'].get('succeeded', 0) for p in ps)
        off = sum(p['offered'] for p in ps)
        rej = sum(p['statuses'].get('rejected', 0) for p in ps)
        cap = sum(p['statuses'].get('client_capacity', 0) for p in ps)
        un = sum(p['statuses'].get('unresolved', 0) for p in ps)
        med = lambda k: statistics.median([p['success_latency_ms'][k] for p in ps if p['success_latency_ms'].get(k) is not None] or [float('nan')])
        rps = statistics.median([p['succeeded_rps'] for p in ps])
        extra = ''.join(f', {n} {v}' for n, v in (('rej', rej), ('cap', cap), ('unres', un)) if v)
        out[rate] = f"{ok}/{off} · {rps:.2f}/s · p50 {med('p50'):,.1f} · p99 {med('p99'):,.1f}{extra}"
    return out, r.get('correctness_verified')
if __name__ == '__main__':
    cols = [(a.split('=')[0], a.split('=')[1]) for a in sys.argv[1:]]
    data = {name: cells(path) for name, path in cols}
    print('| Offered/s | ' + ' | '.join(n for n, _ in cols) + ' |')
    print('| ---: |' + ' --- |' * len(cols))
    rates = sorted({r for c, _ in data.values() for r in c})
    for rate in rates:
        print(f'| {rate:g} | ' + ' | '.join(data[n][0].get(rate, '–') for n, _ in cols) + ' |')
    print('verified: ' + ', '.join(f'{n}={data[n][1]}' for n, _ in cols))
