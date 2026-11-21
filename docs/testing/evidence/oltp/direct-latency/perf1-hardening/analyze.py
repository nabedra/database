#!/usr/bin/env python3
"""PERF1 (hardening): per-phase summary of factor runs (benchmark_oltp_nabedra.py output).

usage: analyze.py <run-dir>... [--json out.json]

For each measured phase (rate sweep: repetitions pooled per rate; soak: per 60 s phase):
acknowledged/offered, refusals by message, client p50/p99 (median over repetitions), API and
leader CPU and CFS throttling from the 5 s cgroup samples, API and leader stage means from
/v1/runtime/stats deltas, and the leader's nabedra-kv counters (L0, write delay, compaction).
"""
import json
import re
import statistics
import sys
from collections import Counter
from pathlib import Path

API_STAGES = ('http.dispatch.spawn_wait', 'gw.route', 'gw.direct.lane_wait', 'gw.direct.send')
LEADER_STAGES = ('leader.mutation.total', 'raft.data.client_write', 'raft.data.append_write')
STORAGE = ('l0_runs', 'l0_runs_peak', 'write_delays', 'write_delay_us', 'compaction_bytes', 'flush_bytes',
           'stall_ms', 'journal_syncs', 'commits')


def stage_mean(after, before, name):
    a, b = after.get(name), before.get(name) or {'count': 0, 'sum_us': 0}
    if not a:
        return None
    count = a['count'] - b['count']
    return round((a['sum_us'] - b['sum_us']) / count / 1000, 2) if count > 0 else None


def cpu(samples, worker, start, end):
    rows = [s for s in samples if s['worker'] == worker]
    before = [s for s in rows if s['unix_ns'] <= start]
    after = [s for s in rows if s['unix_ns'] >= end]
    if not before or not after:
        return None
    s0, s1 = before[-1], after[0]

    def field(sample, key):
        return int(re.search(rf'{key} (\d+)', sample['files']['cpu.stat']).group(1))
    seconds = (s1['unix_ns'] - s0['unix_ns']) / 1e9
    return {'cpu': round((field(s1, 'usage_usec') - field(s0, 'usage_usec')) / 1e6 / seconds, 3),
            'throttled': f"{field(s1, 'nr_throttled') - field(s0, 'nr_throttled')}"
                         f"/{field(s1, 'nr_periods') - field(s0, 'nr_periods')}"}


def leader(phase):
    """The worker that served the direct mutations in this phase."""
    best, count = None, -1
    for name in ('w1', 'w2', 'w3'):
        after = phase['stage_stats_after'].get(name, {}).get('stage_timing', {})
        before = phase['stage_stats_before'].get(name, {}).get('stage_timing', {})
        n = after.get('leader.mutation.total', {}).get('count', 0) - before.get('leader.mutation.total', {}).get('count', 0)
        if n > count:
            best, count = name, n
    return best


def refusals(run, label):
    path = run / f'{label}.jsonl'
    messages = Counter()
    if path.exists():
        for line in path.open():
            sample = json.loads(line)
            if sample.get('status') in ('rejected', 'failed', 'unresolved'):
                error = (sample.get('response') or {}).get('error') or {}
                messages[f"{sample['status']}: {error.get('message', sample.get('error', '?'))}"] += 1
    return dict(messages)


def summarize(run):
    results = json.loads((run / 'results.json').read_text())
    # A sample that timed out under load carries only an error.
    samples = [s for s in map(json.loads, (run / 'cgroup-samples.jsonl').open()) if 'unix_ns' in s]
    soak = 'soak' in results.get('settings', {}) and results['settings'].get('soak') or \
        any(p.get('label', '').startswith('soak') for p in results['phases'])
    groups = {}
    for phase in results['phases']:
        label = phase.get('label', '')
        if 'warmup' in label:
            continue
        key = label if soak else phase.get('rate')
        groups.setdefault(key, []).append(phase)
    rows = []
    for key, phases in groups.items():
        offered = sum(p['offered'] for p in phases)
        statuses = Counter()
        for p in phases:
            statuses.update(p['statuses'])
        lat = [p['success_latency_ms'] for p in phases if p['success_latency_ms'].get('count')]
        row = {'phase': key, 'offered': offered, 'acked': statuses['succeeded'],
               'rejected': statuses['rejected'], 'cap': statuses['client_capacity'],
               'failed': statuses['failed'], 'unresolved': statuses['unresolved'],
               'ack_rate': round(statistics.median(p['succeeded_rps'] for p in phases), 2),
               'p50': round(statistics.median(x['p50'] for x in lat), 1) if lat else None,
               'p99': round(statistics.median(x['p99'] for x in lat), 1) if lat else None,
               'refusals': Counter(), 'api': [], 'leader_cpu': [], 'driver': []}
        for p in phases:
            row['refusals'].update(refusals(run, p['label']))
            start, end = p['start_unix_ns'], p['drain_end_unix_ns']
            row['api'].append(cpu(samples, 'api', start, end))
            lead = leader(p)
            row['leader'] = lead
            row['leader_cpu'].append(cpu(samples, lead, start, end))
            seconds = (end - start) / 1e9
            row['driver'].append(round(p['driver_cpu_seconds'] / seconds, 2) if seconds else None)
        last = phases[-1]
        first = phases[0]
        api_after = last['stage_stats_after']['api'].get('api_stage_timing', {})
        api_before = first['stage_stats_before']['api'].get('api_stage_timing', {})
        row['api_stages_ms'] = {s: stage_mean(api_after, api_before, s) for s in API_STAGES}
        lead = row['leader']
        w_after = last['stage_stats_after'][lead].get('stage_timing', {})
        w_before = first['stage_stats_before'][lead].get('stage_timing', {})
        row['leader_stages_ms'] = {s: stage_mean(w_after, w_before, s) for s in LEADER_STAGES}
        st_after = last['stage_stats_after'][lead].get('storage', {})
        st_before = first['stage_stats_before'][lead].get('storage', {})
        row['leader_storage'] = {}
        for field in STORAGE:
            if field in st_after:
                value = st_after[field] if field in ('l0_runs', 'l0_runs_peak') else st_after[field] - st_before.get(field, 0)
                row['leader_storage'][field] = value
        row['refusals'] = dict(row['refusals'])
        rows.append(row)
    meta = results.get('metadata', {})
    return {'run': run.name, 'binary_sha256': meta.get('binary_sha256'),
            'allocations': meta.get('allocations'), 'correctness_verified': results.get('correctness_verified'),
            'verification': {k: results.get('verification', {}).get('final_snapshot', {}).get(k)
                             for k in ('mismatches', 'unresolved_keys', 'rows')},
            'rows': rows}


def fmt(row):
    api = ' '.join(f"{a['cpu']}({a['throttled']})" for a in row['api'] if a)
    lead = ' '.join(f"{a['cpu']}({a['throttled']})" for a in row['leader_cpu'] if a)
    return (f"  {row['phase']}: {row['acked']}/{row['offered']} ack {row['ack_rate']}/s p50 {row['p50']} p99 {row['p99']}"
            f" rej {row['rejected']} cap {row['cap']} unres {row['unresolved']}\n"
            f"    refusals {row['refusals']}\n    api cpu {api} | {row['leader']} cpu {lead} | driver {row['driver']}\n"
            f"    api {row['api_stages_ms']}\n    leader {row['leader_stages_ms']}\n    kv {row['leader_storage']}")


def main(argv):
    out = None
    if '--json' in argv:
        i = argv.index('--json')
        out = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    summaries = []
    for path in argv:
        run = Path(path)
        try:
            summary = summarize(run)
        except (OSError, KeyError, ValueError) as error:
            print(f'{run.name}: {type(error).__name__}: {error}')
            continue
        summaries.append(summary)
        print(f"{summary['run']} verified={summary['correctness_verified']} {summary['verification']}"
              f" api_cpu={(summary['allocations'] or {}).get('api_cpu')}"
              f" hold={(summary['allocations'] or {}).get('hold_compaction')}")
        for row in summary['rows']:
            print(fmt(row))
    if out:
        Path(out).write_text(json.dumps(summaries, indent=1, sort_keys=True) + '\n')


if __name__ == '__main__':
    main(sys.argv[1:])
