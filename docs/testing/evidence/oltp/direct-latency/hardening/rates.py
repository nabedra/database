#!/usr/bin/env python3.11
"""BENCH3 per-rate resources for Nabedra runs (from RD3's and GC1's analyze.py).

Per offered rate, over the measured repetitions: acknowledged and refused requests, median rate, median p50/p99,
CPU ms per acknowledged request for w1/w2/w3/api (cgroup cpu.stat interpolated at the phase start and drain end),
journal syncs per acknowledged write summed over the three workers (`storage.journal_syncs` in /v1/runtime/stats),
cgroup io.stat write bytes per acknowledged write summed over the workers (nearest samples around the rate's window),
and the leader's direct counters: commits proposed, DirectBatch entries, commits inside them, commits per Data entry
and the batch member histogram.
usage: rates.py <run dir>... [--json]"""
import bisect, json, re, statistics as st, sys
from pathlib import Path

WORKERS = ('w1', 'w2', 'w3')


def kv(text):
    out = {}
    for line in text.splitlines():
        for field in line.split():
            if '=' in field:
                k, v = field.split('=', 1)
                if v.lstrip('-').isdigit():
                    out[k] = out.get(k, 0) + int(v)
    return out


def load(run):
    d = json.loads((run / 'results.json').read_text())
    cpu, io = {}, {}
    path = run / 'cgroup-samples.jsonl'
    if path.is_file():
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            s = json.loads(line); f = s.get('files') or {}
            if 'cpu.stat' in f:
                m = re.search(r'usage_usec (\d+)', f['cpu.stat'])
                if m:
                    cpu.setdefault(s['worker'], []).append((s['unix_ns'], int(m.group(1))))
            if 'io.stat' in f:
                io.setdefault(s['worker'], []).append((s['unix_ns'], kv(f['io.stat']).get('wbytes', 0)))
    return d, cpu, io


def interp(xs, t):
    ts = [x[0] for x in xs]; i = bisect.bisect_left(ts, t)
    if i <= 0 or i >= len(xs):
        return None
    (t0, u0), (t1, u1) = xs[i - 1], xs[i]
    return u0 + (u1 - u0) * (t - t0) / (t1 - t0)


def around(xs, start, end):
    before = [v for t, v in xs if t <= start]; after = [v for t, v in xs if t >= end]
    return after[0] - before[-1] if before and after else None


def counter(stats, w, section, key):
    return ((stats.get(w) or {}).get(section) or {}).get(key)


def delta(ps, w, section, key):
    total = 0
    for p in ps:
        a = counter(p.get('stage_stats_after') or {}, w, section, key)
        b = counter(p.get('stage_stats_before') or {}, w, section, key)
        if a is None or b is None:
            return None
        total += a - b
    return total


def writes(p):
    ops = p.get('by_operation') or {}
    if ops:
        return (ops.get('update') or {}).get('statuses', {}).get('succeeded', 0)
    return p['statuses']['succeeded'] if p.get('operation', 'update') == 'update' else None


def rows(run):
    d, cpu, io = load(run)
    operation = (d.get('settings') or {}).get('operation', 'update')
    rates = {}
    for p in d.get('phases', []):
        if 'warmup' in str(p.get('label', '')) or p.get('repetition', 1) == 0:
            continue
        rates.setdefault(p['rate'], []).append(p)
    out = []
    for rate, ps in sorted(rates.items()):
        ok = sum(p['statuses'].get('succeeded', 0) for p in ps)
        rej = sum(p['statuses'].get('rejected', 0) + p['statuses'].get('failed', 0) for p in ps)
        cap = sum(p['statuses'].get('client_capacity', 0) for p in ps)
        unres = sum(p['statuses'].get('unresolved', 0) for p in ps)
        lat = [p for p in ps if p['success_latency_ms'].get('count')]
        row = {'rate': rate, 'reps': len(ps), 'offered': sum(p['offered'] for p in ps), 'acked': ok,
               'refused': rej, 'driver_capped': cap, 'unresolved': unres,
               'median_rps': st.median(p['succeeded_rps'] for p in ps),
               'p50': st.median(p['success_latency_ms']['p50'] for p in lat) if lat else None,
               'p99': st.median(p['success_latency_ms']['p99'] for p in lat) if lat else None}
        if operation == 'update':
            w = ok
        elif operation == 'mix':
            w = sum((p.get('by_operation') or {}).get('update', {}).get('statuses', {}).get('succeeded', 0) for p in ps)
        else:
            w = 0
        row['acked_writes'] = w
        cpu_ms = {}
        for name in (*WORKERS, 'api'):
            tot = 0
            for p in ps:
                a, b = interp(cpu.get(name, []), p['start_unix_ns']), interp(cpu.get(name, []), p['drain_end_unix_ns'])
                if a is None or b is None:
                    tot = None; break
                tot += b - a
            cpu_ms[name] = round(tot / 1e3 / ok, 2) if tot is not None and ok else None
        row['cpu_ms_per_ack'] = cpu_ms
        syncs = [delta(ps, name, 'storage', 'journal_syncs') for name in WORKERS]
        row['journal_syncs_per_write'] = (round(sum(syncs) / w, 2) if w and all(s is not None for s in syncs) else None)
        row['leader_journal_syncs_per_write'] = round(syncs[0] / w, 2) if w and syncs[0] is not None else None
        wb = []
        for name in WORKERS:
            parts = [around(io.get(name, []), p['start_unix_ns'], p['drain_end_unix_ns']) for p in ps]
            wb.append(None if any(x is None for x in parts) else sum(parts))
        row['wbytes_per_write'] = round(sum(wb) / w) if w and all(x is not None for x in wb) else None
        prop = delta(ps, 'w1', 'direct', 'entries_proposed')
        be = delta(ps, 'w1', 'direct', 'batch_entries') or 0
        bc = delta(ps, 'w1', 'direct', 'batched_commits') or 0
        if prop is not None:
            entries = prop - bc + be
            mem = {}
            for p in ps:
                a = counter(p['stage_stats_after'], 'w1', 'direct', 'batch_members') or {}
                b = counter(p['stage_stats_before'], 'w1', 'direct', 'batch_members') or {}
                for k in a:
                    if a[k] - b.get(k, 0):
                        mem[k] = mem.get(k, 0) + a[k] - b.get(k, 0)
            row['direct'] = {'proposed': prop, 'batch_entries': be, 'batched_commits': bc,
                             'commits_per_entry': round(prop / entries, 2) if entries else None,
                             'batch_members': dict(sorted(mem.items(), key=lambda kv: int(re.sub(r'\D', '', kv[0]) or 0)))}
        out.append(row)
    return {'run': run.name, 'operation': operation, 'correctness_verified': d.get('correctness_verified'),
            'stopped_after_rate': d.get('stopped_after_rate'), 'stop_reason': d.get('stop_reason'), 'rates': out}


def fmt(x, spec='.2f'):
    return '–' if x is None else format(x, spec)


if __name__ == '__main__':
    as_json = '--json' in sys.argv
    result = [rows(Path(a)) for a in sys.argv[1:] if not a.startswith('--') and (Path(a) / 'results.json').is_file()]
    if as_json:
        print(json.dumps(result, indent=1))
        sys.exit()
    for r in result:
        print(f"## {r['run']} ({r['operation']}) correctness_verified={r['correctness_verified']} "
              f"stopped_after={r['stopped_after_rate']} {r['stop_reason'] or ''}")
        print('rate|acked/offered|refused|cap|unres|rps|p50|p99|CPU ms/ack w1/w2/w3/api|syncs/write (all, w1)|wbytes/write|commits/entry|batch members')
        for x in r['rates']:
            c = x['cpu_ms_per_ack']; dct = x.get('direct') or {}
            print(f"{x['rate']:g}|{x['acked']}/{x['offered']}|{x['refused']}|{x['driver_capped']}|{x['unresolved']}|"
                  f"{x['median_rps']:.2f}|{fmt(x['p50'], '.1f')}|{fmt(x['p99'], '.1f')}|"
                  f"{'/'.join(fmt(c[k]) for k in ('w1', 'w2', 'w3', 'api'))}|"
                  f"{fmt(x['journal_syncs_per_write'])}, {fmt(x['leader_journal_syncs_per_write'])}|{fmt(x['wbytes_per_write'], ',')}|"
                  f"{fmt(dct.get('commits_per_entry'))}|{' '.join(f'{k}:{v}' for k, v in (dct.get('batch_members') or {}).items()) or '-'}")
