#!/usr/bin/env python3.11
"""BENCH2 G4/G5 per offered rate: fsync+fdatasync calls (strace) and cgroup wbytes per acknowledged write."""
import json, re, sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import analyze
LINE = re.compile(r'^\s*\d+\s+(\d+\.\d+)\s+(fsync|fdatasync)\(')
for arg in sys.argv[1:]:
    run = Path(arg); r = json.loads((run / 'results.json').read_text())
    times = []
    for log in run.glob('*.strace.log'):
        for line in log.read_text(errors='replace').splitlines():
            m = LINE.match(line)
            if m:
                times.append(float(m.group(1)) * 1e9)
    by = defaultdict(lambda: [None, None, 0])
    for p in r['phases']:
        if p['label'].endswith('warmup'):
            continue
        b = by[p['rate']]
        b[0] = p['start_unix_ns'] if b[0] is None else min(b[0], p['start_unix_ns'])
        b[1] = p['drain_end_unix_ns'] if b[1] is None else max(b[1], p['drain_end_unix_ns'])
        b[2] += p['statuses']['succeeded']
    out = {}
    for rate, (s, e, acked) in sorted(by.items()):
        calls = sum(1 for t in times if s <= t <= e)
        io = analyze.io_delta(run, s, e)
        wb = sum(v['wbytes'] for v in io.values() if v) if all(io.values()) else None
        out[rate] = {'acked': acked, 'fsync_fdatasync': calls, 'per_ack': round(calls / acked, 2) if acked else None,
                     'wbytes_per_ack': round(wb / acked) if wb is not None and acked else None,
                     'window_s': round((e - s) / 1e9, 1)}
    print(run.name, json.dumps(out))
