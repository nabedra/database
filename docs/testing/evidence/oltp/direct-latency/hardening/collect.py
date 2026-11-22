#!/usr/bin/env python3.11
"""Write compact per-run evidence JSON for docs/testing/evidence/oltp/native-storage/runs/."""
import json, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import analyze, summarize

def direct_totals(run, d):
    total, refusals = {}, Counter()
    def add(acc, x):
        for k, v in x.items():
            if isinstance(v, dict):
                add(acc.setdefault(k, {}), v)
            elif isinstance(v, (int, float)) and not isinstance(v, bool) and k not in ('in_flight', 'max_in_flight'):
                acc[k] = acc.get(k, 0) + v
    for p in d.get('phases', []):
        for w, sec in ((p.get('stage_summary') or {}).get('direct') or {}).items():
            if w != 'api':
                add(total, sec.get('direct') or {})
        f = run / p.get('samples', f"{p['label']}.jsonl")
        if f.is_file():
            for line in f.read_text().splitlines():
                s = json.loads(line)
                if s['status'] in ('rejected', 'failed', 'unresolved'):
                    err = (s.get('response') or {}).get('error') or {}
                    refusals[' | '.join(str(x) for x in (s.get('op', 'update'), s['status'], s.get('http_status'),
                             err.get('message') or s.get('transport_error') or s.get('error')))] += 1
    return total, dict(refusals)

def collect(run, out_dir):
    run = Path(run)
    if not (run / 'results.json').is_file():
        return
    d = json.loads((run / 'results.json').read_text())
    meta = d.get('metadata', {})
    cm = json.loads((run / 'cluster-metadata.json').read_text()) if (run / 'cluster-metadata.json').is_file() else meta
    rec = {'run': run.name, 'settings': d.get('settings'), 'workload': d.get('workload'),
           'started_utc': d.get('started_utc'), 'error': d.get('error'),
           'correctness_verified': d.get('correctness_verified'),
           'metadata': {k: cm.get(k) for k in ('engine', 'engine_version', 'binary_sha256', 'server_version', 'image',
                        'direct_oltp', 'pool_format_version', 'strace', 'workspace_head', 'benchmark_source_sha256',
                        'docker_environment', 'allocations', 'consistency', 'commitlog_sync_by_node', 'measurement',
                        'driver_version', 'data_bytes') if cm.get(k) is not None},
           'summary': summarize.summarize(run)}
    if cm.get('direct_capability'):
        rec['metadata']['direct_activation_seconds_after_api_ready'] = cm['direct_capability'].get('seconds_after_api_ready')
    if d.get('phases') and 'stage_summary' in d['phases'][0]:
        rec['direct_counters'], rec['refusals'] = direct_totals(run, d)
    if (run / 'container-runtime.json').is_file():
        a = analyze.analyze(run)
        rec['resources'] = {k: a.get(k) for k in ('window_s', 'acknowledged_writes', 'io', 'wbytes_total',
                            'wbytes_per_ack', 'strace', 'fsync_fdatasync_total', 'fsyncs_per_ack', 'finals',
                            'budget_verified', 'errors')}
        # memory.stat anon/file at the last sample per worker, when sampled
        samples = [json.loads(l) for l in (run / 'cgroup-samples.jsonl').read_text().splitlines() if l.strip()]
        mem = {}
        for s in samples:
            if 'files' in s and s['files'].get('memory.stat'):
                st = dict(l.split() for l in s['files']['memory.stat'].splitlines() if len(l.split()) == 2)
                m = mem.setdefault(s['worker'], {'max_anon': 0, 'max_file': 0})
                m['max_anon'] = max(m['max_anon'], int(st.get('anon', 0)))
                m['max_file'] = max(m['max_file'], int(st.get('file', 0)))
        if mem:
            rec['resources']['memory_stat_max'] = mem
    for extra in ('recovery.json', 'cold-restart.json'):
        if (run / extra).is_file():
            r = json.loads((run / extra).read_text())
            r.pop('before_kill', None); r.pop('after_restart', None)
            rec[extra.split('.')[0]] = r
    if d.get('verification') and str((d.get('phases') or [{}])[0].get('label', '')).startswith('soak'):
        rec['soak'] = {k: d.get(k) for k in ('verification', 'summary', 'seed_details')}
        rec['soak']['seed_details'] = {'seed': d['seed_details'].get('seed')}
    for name in run.glob('*.console.log'):
        pass
    if d.get('verification'):
        rec['verification'] = d['verification']
    if d.get('phases') and str(d['phases'][0].get('label', '')).startswith('soak'):
        rec['soak_phases'] = [{k: p.get(k) for k in ('label', 'offered', 'statuses', 'succeeded_rps',
                               'success_latency_ms', 'unresolved_after_reconciliation')} for p in d['phases']]
    (out_dir / f'{run.name}.json').write_text(json.dumps(rec, indent=1, default=str) + '\n')

if __name__ == '__main__':
    out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
    for run in sys.argv[2:]:
        try:
            collect(run, out)
        except Exception as error:
            print(run, 'FAILED', type(error).__name__, error)
