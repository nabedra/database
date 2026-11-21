#!/usr/bin/env python3.11
"""BENCH2: per-run worker memory (G7), caps inspection, refusals and direct counters.

memory.peak is the maximum over the 5 s cgroup samples, the pre-kill sample of a cold restart and the
final cgroup read; a cold restart starts a new cgroup, so the final read alone would miss the peak.
"""
import json, sys
from pathlib import Path

WORKERS = ('w1', 'w2', 'w3')


def stat(text):
    return dict((l.split()[0], int(l.split()[1])) for l in text.splitlines() if len(l.split()) == 2)


def worker_memory(run):
    out = {w: {'peak': 0, 'anon': 0, 'file': 0, 'anon_thp': 0} for w in WORKERS}
    path = run / 'cgroup-samples.jsonl'
    if path.is_file():
        for line in path.read_text().splitlines():
            s = json.loads(line)
            w, f = s.get('worker'), s.get('files') or {}
            if w not in out:
                continue
            if f.get('memory.peak', '').strip():
                out[w]['peak'] = max(out[w]['peak'], int(f['memory.peak']))
            st = stat(f.get('memory.stat', ''))
            for k in ('anon', 'file', 'anon_thp'):
                out[w][k] = max(out[w][k], st.get(k, 0))
    cr = run / 'cold-restart.json'
    if cr.is_file():
        r = json.loads(cr.read_text())
        for key in ('before_kill', 'after_restart'):
            for w, s in (r.get(key) or {}).items():
                f = s.get('files', s) if isinstance(s, dict) else {}
                if isinstance(f, dict) and str(f.get('memory.peak', '')).strip():
                    out[w]['peak'] = max(out[w]['peak'], int(f['memory.peak']))
    return out


def main(compact_dir, runs_dir, names):
    rows = {}
    for name in names:
        run = Path(runs_dir) / name
        rec_path = Path(compact_dir) / f'{name}.json'
        rec = json.loads(rec_path.read_text()) if rec_path.is_file() else {}
        res = rec.get('resources') or {}
        mem = worker_memory(run)
        finals = res.get('finals') or {}
        for w in WORKERS:
            p = (finals.get(w) or {}).get('memory_peak')
            if p:
                mem[w]['peak'] = max(mem[w]['peak'], p)
        oom = {k: (v.get('oom', 0), v.get('oom_kill', 0), v.get('oom_killed_flag')) for k, v in finals.items()}
        caps = {k: (v.get('cpu_max'), v.get('nano_cpus'), v.get('memory_limit'), v.get('memory_swap'))
                for k, v in finals.items()}
        dc = rec.get('direct_counters') or {}
        rows[name] = {'memory': mem, 'largest_peak': max((m['peak'] for m in mem.values()), default=0),
                      'oom': oom, 'caps': caps, 'budget_verified': res.get('budget_verified'),
                      'refusals': rec.get('refusals'), 'fallbacks': dc.get('fallbacks'),
                      'retries': dc.get('retries'), 'proposal_failures': dc.get('proposal_failures'),
                      'batch_sizes': dc.get('batch_sizes'),
                      'wbytes_per_ack': res.get('wbytes_per_ack'), 'fsyncs_per_ack': res.get('fsyncs_per_ack'),
                      'fsync_fdatasync_total': res.get('fsync_fdatasync_total'),
                      'acknowledged_writes': res.get('acknowledged_writes')}
    return rows


if __name__ == '__main__':
    compact, runs, *names = sys.argv[1:]
    print(json.dumps(main(compact, runs, names), indent=1, default=str))
