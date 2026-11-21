"""PERF2c: per-rate summary of driver runs. usage: analyze.py <run dir>..."""
import json, sys, statistics
from pathlib import Path

def stages(phase, who):
    d = phase['stage_summary']['direct'].get(who, {})
    return d.get('stage_timing') or d.get('api_stage_timing') or {}

def cgroup(run):
    path = Path(run) / 'cgroup-samples.jsonl'
    out = {}
    if not path.is_file():
        return out
    rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    for name in sorted({r['worker'] for r in rows if 'files' in r}):
        mine = [r for r in rows if r.get('worker') == name and 'cpu.stat' in r.get('files', {})]
        if len(mine) < 2:
            continue
        def stat(r):
            return dict(l.split() for l in r['files']['cpu.stat'].splitlines() if l.strip())
        a, b = stat(mine[0]), stat(mine[-1])
        secs = (mine[-1]['unix_ns'] - mine[0]['unix_ns']) / 1e9
        out[name] = dict(cpu=(int(b['usage_usec']) - int(a['usage_usec'])) / 1e6 / secs,
                         throttled_periods=int(b['nr_throttled']) - int(a['nr_throttled']),
                         periods=int(b['nr_periods']) - int(a['nr_periods']),
                         throttled_ms_per_s=(int(b['throttled_usec']) - int(a['throttled_usec'])) / 1e3 / secs,
                         seconds=round(secs))
    return out

for run in sys.argv[1:]:
    d = json.load(open(Path(run) / 'results.json'))
    phases = [p for p in d['phases'] if 'warmup' not in p['label']]
    print(f'== {run}  verified={d.get("correctness_verified")}')
    by_rate = {}
    for p in phases:
        by_rate.setdefault(p['rate'], []).append(p)
    for rate, ps in by_rate.items():
        p50 = statistics.median(p['success_latency_ms']['p50'] for p in ps if p['success_latency_ms']['count'])
        p99 = statistics.median(p['success_latency_ms']['p99'] for p in ps if p['success_latency_ms']['count'])
        secs = sum((p['drain_end_ns'] - p['start_ns']) / 1e9 for p in ps)
        ok = sum(p['statuses']['succeeded'] for p in ps)
        agg = {}
        for p in ps:
            for who in ('w1', 'api'):
                for k, v in stages(p, who).items():
                    a = agg.setdefault((who, k), [0, 0])
                    a[0] += v['count']; a[1] += v.get('sum_us', 0)
        def rate_of(who, k):
            return agg.get((who, k), [0, 0])[0] / secs
        def mean(who, k):
            c, s = agg.get((who, k), [0, 0])
            return s / c / 1000 if c else float('nan')
        cpu = sum(p.get('driver_cpu_seconds', 0) for p in ps)
        print(f'  rate {rate}: ok {ok}, p50 {p50:.1f} p99 {p99:.1f} ms; '
              f'api new conn {rate_of("api","http.request.new_connection"):.2f}/s reused {rate_of("api","http.request.reused_connection"):.2f}/s '
              f'handshake {mean("api","http.accept.handshake"):.2f} ms; dispatch {mean("api","http.dispatch.total"):.2f} ms; '
              f'w1 Control ReadIndex {rate_of("w1","raft.read_index_by_kind.Control"):.1f}/s; driver CPU {cpu/secs:.3f}')
    for name, c in cgroup(run).items():
        print(f'  cgroup {name}: cpu {c["cpu"]:.3f}, throttled {c["throttled_periods"]}/{c["periods"]} periods, {c["throttled_ms_per_s"]:.1f} ms/s over {c["seconds"]} s')
