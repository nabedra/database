#!/usr/bin/env python3
"""SNAP1 (hardening): stall windows, p99, refusals and bytes written per replica in NS11 soaks.

usage: analyze_snap1.py <soak-run-dir>... [--json out.json]

A stall window is a run of updates whose latency (settled - scheduled) exceeds 250 ms, with
scheduled times less than 1 s apart. Bytes written come from each worker's cgroup io.stat
(wbytes summed over devices) between the end of the seed and the last sample of the soak;
"per 5,000 entries" divides by the updates acknowledged in that window (one Data entry each).
"""
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path

STALL_MS = 250
GAP_S = 1.0


def p(values, q):
    values = sorted(values)
    return values[math.ceil(q / 100 * len(values)) - 1] if values else None


def wbytes(sample):
    text = sample.get('files', {}).get('io.stat', '')
    return sum(int(v) for v in re.findall(r'wbytes=(\d+)', text))


def run(path):
    results = json.loads((path / 'results.json').read_text())
    phases = results.get('phases', [])
    if not phases:
        return {'run': path.name, 'error': 'no phases'}
    origin = phases[0]['start_ns']
    samples = []
    for index in range(len(phases)):
        file = path / f'soak-{index}.jsonl'
        if file.is_file():
            samples += [json.loads(line) for line in file.read_text().splitlines() if line]
    statuses = Counter(s['status'] for s in samples)
    ok = [s for s in samples if s['status'] == 'succeeded']
    lat = [(s['settled_ns'] - s['scheduled_ns']) / 1e6 for s in ok]
    slow = sorted(((s['scheduled_ns'] - origin) / 1e9, (s['settled_ns'] - s['scheduled_ns']) / 1e6)
                  for s in ok if (s['settled_ns'] - s['scheduled_ns']) / 1e6 > STALL_MS)
    windows = []
    for t, ms in slow:
        if windows and t - windows[-1]['end_s'] < GAP_S:
            w = windows[-1]
            w['end_s'], w['requests'], w['max_ms'] = round(t, 1), w['requests'] + 1, round(max(w['max_ms'], ms), 1)
        else:
            windows.append({'start_s': round(t, 1), 'end_s': round(t, 1), 'requests': 1, 'max_ms': round(ms, 1)})
    phase_p99 = [ph['by_operation'].get('update', {}).get('success_latency_ms', {}).get('p99')
                 for ph in phases]
    start_unix = results.get('seed_end_unix_ns') or phases[0]['start_unix_ns']
    end_unix = phases[-1]['drain_end_unix_ns']
    io = {}
    cg = path / 'cgroup-samples.jsonl'
    if cg.is_file():
        rows = [json.loads(line) for line in cg.read_text().splitlines() if line]
        for worker in sorted({r['worker'] for r in rows if 'unix_ns' in r}):
            mine = [r for r in rows if r.get('worker') == worker and 'unix_ns' in r]
            before = [r for r in mine if r['unix_ns'] <= start_unix]
            # The 5 s sampler stops with the driver, a few seconds before the last drain:
            # the window ends at the last sample.
            inside = [r for r in mine if start_unix < r['unix_ns']]
            if not before or not inside:
                continue
            total = wbytes(inside[-1]) - wbytes(before[-1])
            series = [before[-1]] + inside
            bursts = [wbytes(b) - wbytes(a) for a, b in zip(series, series[1:])]
            io[worker] = {'wbytes_mb': round(total / 1e6, 1),
                          'per_5000_entries_mb': round(total / max(1, len(ok)) * 5000 / 1e6, 1),
                          'max_sample_burst_mb': round(max(bursts, default=0) / 1e6, 1)}
    return {
        'run': path.name,
        'offered': len(samples),
        'statuses': dict(sorted(statuses.items())),
        'refused_pct': round(100 * (len(samples) - len(ok)) / max(1, len(samples)), 2),
        'p50_ms': round(p(lat, 50), 2) if lat else None,
        'p99_ms': round(p(lat, 99), 2) if lat else None,
        'max_ms': round(max(lat), 1) if lat else None,
        'phase_p99_ms': [round(v, 1) if v is not None else None for v in phase_p99],
        'stall_windows': windows,
        'verification': results.get('verification', {}).get('mismatches'),
        'io': io,
    }


def main(argv):
    out = None
    if '--json' in argv:
        i = argv.index('--json')
        out = Path(argv[i + 1])
        argv = argv[:i] + argv[i + 2:]
    summary = [run(Path(a)) for a in argv if (Path(a) / 'results.json').is_file()]
    for r in summary:
        if 'error' in r:
            print(r['run'], r['error'])
            continue
        print(f"{r['run']}: offered {r['offered']} refused {r['refused_pct']}% {r['statuses']} "
              f"p50 {r['p50_ms']} p99 {r['p99_ms']} max {r['max_ms']} ms; phase p99 {r['phase_p99_ms']}; "
              f"verify mismatches {r['verification']}")
        print(f"  stall windows (>{STALL_MS} ms): {len(r['stall_windows'])} {r['stall_windows']}")
        for worker, io in r['io'].items():
            print(f"  {worker}: wrote {io['wbytes_mb']} MB, {io['per_5000_entries_mb']} MB per 5,000 entries, "
                  f"max 5 s burst {io['max_sample_burst_mb']} MB")
    if out:
        out.write_text(json.dumps(summary, indent=1) + '\n')


if __name__ == '__main__':
    main(sys.argv[1:])
