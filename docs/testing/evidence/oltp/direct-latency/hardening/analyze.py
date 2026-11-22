#!/usr/bin/env python3.11
"""NS11 resource analysis: cgroup io.stat bytes per acknowledged write over the measured window (G5),
worker memory.peak and OOM events (G7), CPU throttling, and strace fsync counts per write (G4)."""
import json, re, statistics, sys
from pathlib import Path

WORKERS = ('w1', 'w2', 'w3')

def kv(text):
    out = {}
    for line in text.splitlines():
        for field in line.split():
            if '=' in field:
                k, v = field.split('=', 1)
                out[k] = out.get(k, 0) + int(v)
        parts = line.split()
        if len(parts) == 2 and parts[1].lstrip('-').isdigit():
            out[parts[0]] = int(parts[1])
    return out

def window(results):
    phases = [p for p in results.get('phases', []) if 'start_unix_ns' in p]
    if not phases:
        return None
    acked = sum(p['statuses']['succeeded'] for p in phases if p.get('by_operation', {}).get('get') is None
                or True) if False else None
    writes = 0
    for p in phases:
        ops = p.get('by_operation') or {}
        if ops:
            writes += (ops.get('update') or {}).get('statuses', {}).get('succeeded', 0)
        else:
            writes += p['statuses']['succeeded']
    return min(p['start_unix_ns'] for p in phases), max(p['drain_end_unix_ns'] for p in phases), writes

def io_delta(run, start, end):
    samples = [json.loads(l) for l in (run / 'cgroup-samples.jsonl').read_text().splitlines() if l.strip()]
    out = {}
    for w in WORKERS:
        mine = [s for s in samples if s.get('worker') == w and 'files' in s]
        before = [s for s in mine if s['unix_ns'] <= start]
        after = [s for s in mine if s['unix_ns'] >= end]
        if not before or not after:
            out[w] = None
            continue
        a, b = kv(before[-1]['files'].get('io.stat', '')), kv(after[0]['files'].get('io.stat', ''))
        ca, cb = kv(before[-1]['files'].get('cpu.stat', '')), kv(after[0]['files'].get('cpu.stat', ''))
        out[w] = {'wbytes': b.get('wbytes', 0) - a.get('wbytes', 0), 'wios': b.get('wios', 0) - a.get('wios', 0),
                  'cpu_usec': cb.get('usage_usec', 0) - ca.get('usage_usec', 0),
                  'throttled_periods': cb.get('nr_throttled', 0) - ca.get('nr_throttled', 0),
                  'periods': cb.get('nr_periods', 0) - ca.get('nr_periods', 0),
                  'window_s': (after[0]['unix_ns'] - before[-1]['unix_ns']) / 1e9}
    return out

def finals(run):
    rt = json.loads((run / 'container-runtime.json').read_text())
    out = {}
    for name, item in rt['containers'].items():
        role = name.rsplit('-', 1)[-1]
        text = item.get('cgroup_final', '')
        sections = {}
        current = None
        for line in text.splitlines():
            if line.startswith('/'):
                current = line.rsplit('/', 1)[-1]
                sections[current] = ''
            elif current:
                sections[current] += line + '\n'
        mem = kv(sections.get('memory.events', ''))
        cpu = kv(sections.get('cpu.stat', ''))
        out[role] = {'memory_peak': int(sections['memory.peak']) if sections.get('memory.peak', '').strip() else None,
                     'oom': mem.get('oom', 0), 'oom_kill': mem.get('oom_kill', 0), 'max': mem.get('max', 0),
                     'oom_killed_flag': item['state'].get('OOMKilled'),
                     'cpu_max': sections.get('cpu.max', '').strip(), 'nano_cpus': item['nano_cpus'],
                     'memory_limit': item['memory'], 'memory_swap': item['memory_swap'],
                     'throttled': f"{cpu.get('nr_throttled', 0)}/{cpu.get('nr_periods', 0)}"}
    return out, rt.get('budget_verified'), rt.get('errors')

STRACE = re.compile(r'^(\d+)\s+(\d+\.\d+)\s+(fsync|fdatasync|sync_file_range|syncfs)\((\d+).*?<(\d+\.\d+)>')

def strace(run, start, end):
    out = {}
    for w in WORKERS:
        files = list(run.glob(f'*-{w}.strace.log'))
        if not files:
            continue
        calls, durations = {}, []
        for line in files[0].read_text(errors='replace').splitlines():
            m = STRACE.match(line)
            if not m:
                continue
            t = float(m.group(2)) * 1e9
            if start <= t <= end:
                calls[m.group(3)] = calls.get(m.group(3), 0) + 1
                durations.append(float(m.group(5)) * 1000)
        durations.sort()
        out[w] = {'calls': calls, 'fsync_fdatasync': calls.get('fsync', 0) + calls.get('fdatasync', 0),
                  'p50_ms': durations[len(durations) // 2] if durations else None,
                  'p99_ms': durations[max(0, -(-len(durations) * 99 // 100) - 1)] if durations else None}
    return out

def analyze(run):
    run = Path(run)
    res = json.loads((run / 'results.json').read_text())
    w = window(res)
    report = {'run': run.name}
    if w:
        start, end, writes = w
        report['window_s'] = (end - start) / 1e9
        report['acknowledged_writes'] = writes
        io = io_delta(run, start, end)
        report['io'] = io
        total = sum(v['wbytes'] for v in io.values() if v)
        report['wbytes_total'] = total
        report['wbytes_per_ack'] = total / writes if writes else None
        st = strace(run, start, end)
        if st:
            report['strace'] = st
            calls = sum(v['fsync_fdatasync'] for v in st.values())
            report['fsync_fdatasync_total'] = calls
            report['fsyncs_per_ack'] = calls / writes if writes else None
    report['finals'], report['budget_verified'], report['errors'] = finals(run)
    return report

if __name__ == '__main__':
    for run in sys.argv[1:]:
        r = analyze(run)
        f = r['finals']
        peak = max((v['memory_peak'] or 0) for k, v in f.items() if k in WORKERS)
        ooms = sum(v['oom'] + v['oom_kill'] for v in f.values())
        line = f"{r['run']}: window={r.get('window_s', 0):.0f}s acks={r.get('acknowledged_writes')} wbytes={r.get('wbytes_total')} per_ack={r.get('wbytes_per_ack')}"
        if 'fsyncs_per_ack' in r:
            line += f" fsyncs={r['fsync_fdatasync_total']} per_ack={r['fsyncs_per_ack']}"
        line += f" worker_peak={peak} ooms={ooms} budget={r['budget_verified']} errors={r['errors']}"
        print(line)
        if '-v' in sys.argv[0:1]:
            pass
