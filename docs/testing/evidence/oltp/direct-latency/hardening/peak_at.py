#!/usr/bin/env python3.11
"""BENCH3 G7 at equal load: each worker's memory.peak from the last cgroup sample at or before the drain end
of the last measured phase at a given offered rate (memory.peak is monotonic within a cgroup).
usage: peak_at.py <rate> <run dir>..."""
import json, sys
from pathlib import Path
rate = float(sys.argv[1])
for arg in sys.argv[2:]:
    run = Path(arg); d = json.loads((run / 'results.json').read_text())
    ends = [p['drain_end_unix_ns'] for p in d['phases'] if p['rate'] == rate]
    if not ends:
        print(run.name, 'no phase at', rate); continue
    end = max(ends); best = {}
    for line in (run / 'cgroup-samples.jsonl').read_text().splitlines():
        s = json.loads(line); f = s.get('files') or {}
        if s.get('worker') in ('w1', 'w2', 'w3') and s['unix_ns'] <= end and f.get('memory.peak', '').strip():
            best[s['worker']] = max(best.get(s['worker'], 0), int(f['memory.peak']))
    w = max(best, key=best.get)
    print(f'{run.name} peak through {rate:g}/s: {best[w] / 1e6:.1f} MB ({w})')
