"""PERF2e: serial-pace summary of driver runs. usage: analyze.py <run dir>...

Per run: update latency over every measured sample, the API's new and reused
connections per measured update, TLS accept time, dispatch, and container CPU
inside the measured phase windows against the rest of the run.
"""
import json, statistics, sys
from pathlib import Path


def stage_sums(phase, who):
    d = phase['stage_summary']['direct'].get(who, {})
    return d.get('stage_timing') or d.get('api_stage_timing') or {}


def cgroup_split(run, windows):
    """CPU per container inside the measured windows and outside them."""
    path = Path(run) / 'cgroup-samples.jsonl'
    rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    out = {}
    for name in sorted({r['worker'] for r in rows if 'files' in r}):
        mine = [r for r in rows if r.get('worker') == name and 'cpu.stat' in r.get('files', {})]
        usage = [(r['unix_ns'], int(dict(l.split() for l in r['files']['cpu.stat'].splitlines() if l.strip())['usage_usec']))
                 for r in mine]
        inside = [0.0, 0.0]
        outside = [0.0, 0.0]
        for (t0, u0), (t1, u1) in zip(usage, usage[1:]):
            mid = (t0 + t1) // 2
            bucket = inside if any(a <= mid <= b for a, b in windows) else outside
            bucket[0] += (u1 - u0) / 1e6
            bucket[1] += (t1 - t0) / 1e9
        out[name] = (inside[0] / inside[1] if inside[1] else float('nan'),
                     outside[0] / outside[1] if outside[1] else float('nan'))
    return out


for run in sys.argv[1:]:
    d = json.load(open(Path(run) / 'results.json'))
    phases = [p for p in d['phases'] if 'warmup' not in p['label']]
    latencies = []
    for p in phases:
        for line in (Path(run) / p['samples']).read_text().splitlines():
            s = json.loads(line)
            if s['status'] == 'succeeded':
                latencies.append((s['settled_ns'] - s['scheduled_ns']) / 1e6)
    latencies.sort()
    agg = {}
    for p in phases:
        for who in ('w1', 'api'):
            for k, v in stage_sums(p, who).items():
                a = agg.setdefault((who, k), [0, 0])
                a[0] += v['count']
                a[1] += v.get('sum_us', 0)
    ops = len(latencies)
    count = lambda who, k: agg.get((who, k), [0, 0])[0]
    mean = lambda who, k: (agg[(who, k)][1] / agg[(who, k)][0] / 1000) if count(who, k) else float('nan')
    windows = [(p['start_unix_ns'], p['drain_end_unix_ns']) for p in phases]
    cpu = cgroup_split(run, windows)
    verification = d.get('verification')
    print(json.dumps({
        'run': Path(run).name, 'verified': d.get('correctness_verified'), 'updates': ops,
        'p50_ms': round(statistics.median(latencies), 2), 'p90_ms': round(latencies[int(0.9 * (ops - 1))], 2),
        'max_ms': round(latencies[-1], 2), 'mean_ms': round(statistics.fmean(latencies), 2),
        'api_new_connections_per_update': round(count('api', 'http.request.new_connection') / ops, 2),
        'api_reused_connections_per_update': round(count('api', 'http.request.reused_connection') / ops, 2),
        'api_tls_accept_ms': round(mean('api', 'http.accept.handshake'), 2),
        'api_dispatch_ms': round(mean('api', 'http.dispatch.total'), 2),
        'client_minus_dispatch_ms': round(statistics.fmean(latencies) - mean('api', 'http.dispatch.total'), 2),
        'driver_connections_opened': d.get('driver_cgroup', {}).get('connections_opened'),
        'cpu_in_measured_windows': {k: round(v[0], 3) for k, v in cpu.items()},
        'cpu_outside_measured_windows': {k: round(v[1], 3) for k, v in cpu.items()},
        'verification_seconds': verification and round(verification['seconds'], 1),
        'run_wall_seconds': round((phases[-1]['drain_end_unix_ns'] - d['phases'][0]['start_unix_ns']) / 1e9, 1),
    }))
