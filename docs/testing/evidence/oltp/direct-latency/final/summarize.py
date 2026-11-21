#!/usr/bin/env python3.11
"""Summarize NS11 runs: per rate medians over measured repetitions."""
import json, statistics, sys
from pathlib import Path

def med(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None

def summarize(run):
    path = Path(run) / 'results.json'
    if not path.is_file():
        return {'run': Path(run).name, 'error': 'no results.json'}
    d = json.loads(path.read_text())
    out = {'run': Path(run).name, 'error': d.get('error'), 'correctness_verified': d.get('correctness_verified'),
           'stopped_after_rate': d.get('stopped_after_rate'), 'stop_reason': d.get('stop_reason'), 'rates': {}}
    phases = d.get('phases', [])
    for rate in dict.fromkeys(p['rate'] for p in phases):
        measured = [p for p in phases if p['rate'] == rate and p['repetition'] > 0]
        if not measured:
            continue
        statuses = {k: sum(p['statuses'][k] for p in measured) for k in measured[0]['statuses']}
        row = {
            'reps': len(measured), 'offered': sum(p['offered'] for p in measured),
            'median_succeeded_rps': med([p['succeeded_rps'] for p in measured]),
            'median_p50_ms': med([p['success_latency_ms']['p50'] for p in measured]),
            'median_p95_ms': med([p['success_latency_ms']['p95'] for p in measured]),
            'median_p99_ms': med([p['success_latency_ms']['p99'] for p in measured]),
            'statuses': statuses,
            'mismatches': sum(p['snapshot']['mismatches'] for p in measured if 'snapshot' in p),
            'unresolved_after_reconciliation': sum(p['unresolved_after_reconciliation'] for p in measured),
        }
        ops = {}
        for p in measured:
            for op, v in (p.get('by_operation') or {}).items():
                ops.setdefault(op, []).append(v)
        if len(ops) > 1:
            row['by_operation'] = {op: {'median_p50_ms': med([v['success_latency_ms']['p50'] for v in vs]),
                                        'median_p99_ms': med([v['success_latency_ms']['p99'] for v in vs]),
                                        'succeeded': sum(v['statuses']['succeeded'] for v in vs),
                                        'offered': sum(v['offered'] for v in vs)} for op, vs in ops.items()}
        out['rates'][str(rate)] = row
    return out

if __name__ == '__main__':
    for run in sys.argv[1:]:
        s = summarize(run)
        print(f"## {s['run']}  verified={s.get('correctness_verified')} stop={s.get('stopped_after_rate')} {s.get('stop_reason') or ''} err={s.get('error')}")
        for rate, r in s.get('rates', {}).items():
            st = r['statuses']
            line = (f"  {rate:>6}/s reps={r['reps']} ack/offered={st['succeeded']}/{r['offered']} rej={st['rejected']} "
                    f"cap={st['client_capacity']} fail={st['failed']} unres={st['unresolved']}->{r['unresolved_after_reconciliation']} "
                    f"rps={r['median_succeeded_rps']:.3f} p50={r['median_p50_ms']} p95={r['median_p95_ms']} p99={r['median_p99_ms']} mism={r['mismatches']}")
            print(line)
            for op, v in (r.get('by_operation') or {}).items():
                print(f"          {op}: {v}")
