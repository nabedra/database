"""Per-5s-interval CPU and throttling per container, inside the measured phases only."""
import json, sys, statistics
from pathlib import Path
for run in sys.argv[2:]:
    d = json.load(open(Path(run) / 'results.json'))
    phases = [p for p in d['phases'] if 'warmup' not in p['label']]
    windows = [(p['start_unix_ns'], p['drain_end_unix_ns']) for p in phases]
    rows = [json.loads(l) for l in (Path(run) / 'cgroup-samples.jsonl').read_text().splitlines() if l.strip()]
    print('==', run)
    for name in ('w1', 'w2', 'w3', 'api'):
        mine = [r for r in rows if r.get('worker') == name and 'cpu.stat' in r.get('files', {})]
        cpu, thr, per = [], 0, 0
        for a, b in zip(mine, mine[1:]):
            if not any(s <= a['unix_ns'] and b["unix_ns"] <= e + float(sys.argv[1])*1e9 for s, e in windows):
                continue
            sa = dict(l.split() for l in a['files']['cpu.stat'].splitlines() if l.strip())
            sb = dict(l.split() for l in b['files']['cpu.stat'].splitlines() if l.strip())
            secs = (b['unix_ns'] - a['unix_ns']) / 1e9
            cpu.append((int(sb['usage_usec']) - int(sa['usage_usec'])) / 1e6 / secs)
            thr += int(sb['nr_throttled']) - int(sa['nr_throttled']); per += int(sb['nr_periods']) - int(sa['nr_periods'])
        if cpu:
            print(f'  {name}: intervals {len(cpu)}, cpu median {statistics.median(cpu):.3f} (min {min(cpu):.3f} max {max(cpu):.3f}), throttled {thr}/{per} periods')
