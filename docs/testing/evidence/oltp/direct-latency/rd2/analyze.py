"""RD2 controlled-host summary. Per rate: median p50/p99, acked/s, refusals, CPU ms per ack
(cgroup cpu.stat over the phase), DirectBatch counters on the leader, and stage means.
usage: analyze.py <run dir>... [--stages]"""
import json, sys, re, bisect, statistics as st
WORKERS = ('w1', 'w2', 'w3', 'api')
STAGES = [  # (role, stage)
    ('api', 'http.dispatch.total'), ('api', 'gw.direct.send'), ('w1', 'rpc.direct.serve'),
    ('w1', 'leader.mutation.total'), ('w1', 'leader.attempt.read'), ('w1', 'leader.attempt.timestamp_fetch'),
    ('w1', 'ts.client.call'), ('w2', 'ts.serve.total'),
    ('w1', 'sess.c.call.ServiceAuthority'), ('w1', 'sess.c.queue.ServiceAuthority'), ('w1', 'sess.c.write.ServiceAuthority'),
    ('w1', 'sess.c.rtt.ServiceAuthority'), ('w1', 'sess.c.wake.ServiceAuthority'),
    ('w2', 'sess.s.receive.ServiceAuthority'), ('w2', 'sess.s.spawn.ServiceAuthority'), ('w2', 'sess.s.preflight.ServiceAuthority'),
    ('w2', 'sess.s.handle.ServiceAuthority'), ('w2', 'sess.s.queue.ServiceAuthority'), ('w2', 'sess.s.write.ServiceAuthority'),
    ('w2', 'sess.s.total.ServiceAuthority'),
    ('api', 'sess.c.call.ApiIngress'), ('api', 'sess.c.rtt.ApiIngress'), ('api', 'sess.c.wake.ApiIngress'),
    ('w1', 'sess.s.receive.ApiIngress'), ('w1', 'sess.s.spawn.ApiIngress'), ('w1', 'sess.s.preflight.ApiIngress'),
    ('w1', 'sess.s.handle.ApiIngress'), ('w1', 'sess.s.total.ApiIngress'),
    ('w1', 'grant.role.miss'), ('w1', 'grant.role.hit'), ('w2', 'grant.role.miss'), ('w2', 'grant.role.hit'),
    ('w1', 'grant.warm.refresh'), ('w2', 'grant.warm.refresh'),
    ('w1', 'raft.data.client_write'), ('w1', 'raft.data.propose_to_apply'), ('w1', 'raft.data.append'),
    ('w1', 'raft.data.append_encode'), ('w1', 'raft.data.append_write'), ('w1', 'raft.data.net.append'),
    ('w1', 'sess.c.rtt.DataConsensus'), ('w1', 'sess.c.call.DataConsensus'), ('w1', 'sess.c.queue.DataConsensus'),
    ('w2', 'raft.data.net.append_handle'), ('w2', 'raft.data.append_write'), ('w2', 'sess.s.total.DataConsensus'),
    ('w2', 'sess.s.spawn.DataConsensus'), ('w2', 'sess.s.receive.DataConsensus'),
    ('w1', 'raft.data.apply_entry'), ('w1', 'raft.data.apply_machine'), ('w1', 'raft.data.save_committed'),
    ('w1', 'raft.data.propose_to_append'), ('w1', 'raft.data.propose_to_send'), ('w1', 'raft.data.propose_to_flush'),
    ('w1', 'raft.data.propose_to_ack'),
    ('w1', 'pool.data_apply.order'), ('w1', 'pool.data_apply.prepare'), ('w1', 'pool.data_apply.execute'),
    ('w1', 'pool.data_apply.commit'), ('w1', 'native.query.admit'), ('w1', 'native.query.run'),
    ('w1', 'sess.c.call.ServiceMutation'), ('w1', 'sess.c.queue.ServiceMutation'), ('w1', 'sess.c.write.ServiceMutation'),
    ('w1', 'sess.c.rtt.ServiceMutation'), ('w1', 'sess.c.wake.ServiceMutation'), ('w2', 'sess.s.total.ServiceMutation'),
    ('w2', 'sess.s.preflight.ServiceMutation'), ('w2', 'sess.s.handle.ServiceMutation'), ('w2', 'sess.s.write.ServiceMutation'),
]
def stage(stats, role, name):
    r = stats.get(role, {})
    x = r.get('stage_timing', {}).get(name) or r.get('api_stage_timing', {}).get(name) or {}
    return x.get('count', 0), x.get('sum_us', 0)
def load(run):
    d = json.load(open(f'{run}/results.json'))
    S = {}
    try:
        for line in open(f'{run}/cgroup-samples.jsonl'):
            s = json.loads(line); f = s['files']
            if 'cpu.stat' in f:
                S.setdefault(s['worker'], []).append((s['unix_ns'], int(re.search(r'usage_usec (\d+)', f['cpu.stat']).group(1))))
    except FileNotFoundError:
        pass
    return d, S
def at(S, w, t):
    xs = S.get(w, []); ts = [x[0] for x in xs]; i = bisect.bisect_left(ts, t)
    if i <= 0 or i >= len(xs): return None
    (t0, u0), (t1, u1) = xs[i-1], xs[i]; return u0 + (u1 - u0) * (t - t0) / (t1 - t0)
def direct(stats, w, k):
    return ((stats.get(w) or {}).get('direct') or {}).get(k) or 0
show = '--stages' in sys.argv
for run in [a for a in sys.argv[1:] if not a.startswith('--')]:
    d, S = load(run)
    print(f'## {run.rstrip("/").split("/")[-1]}  correctness_verified={d.get("correctness_verified")} unresolved={d.get("verification", {}).get("unresolved") if isinstance(d.get("verification"), dict) else "?"}')
    rates = {}
    for p in d['phases']:
        if 'warmup' in p['label']: continue
        rates.setdefault(p['rate'], []).append(p)
    print('rate|reps|acked/s|refused|p50 med|p99 med|CPU ms/ack w1/w2/w3/api|proposed|batch_entries|batched_commits|commits/entry')
    for r, ps in sorted(rates.items()):
        ok = sum(p['statuses']['succeeded'] for p in ps)
        rej = sum(p['statuses'].get('rejected', 0) + p['statuses'].get('failed', 0) for p in ps)
        lat = [p for p in ps if p['success_latency_ms']['count']]
        p50 = st.median(p['success_latency_ms']['p50'] for p in lat) if lat else float('nan')
        p99 = st.median(p['success_latency_ms']['p99'] for p in lat) if lat else float('nan')
        rps = st.median(p['succeeded_rps'] for p in ps)
        cpu = []
        for w in WORKERS:
            tot = 0; good = True
            for p in ps:
                a, b = at(S, w, p['start_unix_ns']), at(S, w, p['drain_end_unix_ns'])
                if a is None or b is None: good = False; break
                tot += b - a
            cpu.append(f'{tot / 1e3 / ok:.2f}' if good and ok else 'nan')
        prop = sum(direct(p['stage_stats_after'], 'w1', 'entries_proposed') - direct(p['stage_stats_before'], 'w1', 'entries_proposed') for p in ps)
        be = sum(direct(p['stage_stats_after'], 'w1', 'batch_entries') - direct(p['stage_stats_before'], 'w1', 'batch_entries') for p in ps)
        bc = sum(direct(p['stage_stats_after'], 'w1', 'batched_commits') - direct(p['stage_stats_before'], 'w1', 'batched_commits') for p in ps)
        entries = prop - bc + be
        print(f"{r}|{len(ps)}|{rps:.2f}|{rej}|{p50:.1f}|{p99:.1f}|{'/'.join(cpu)}|{prop}|{be}|{bc}|{prop / entries if entries else float('nan'):.2f}")
        if show:
            for role, name in STAGES:
                n = s = 0
                for p in ps:
                    c1, s1 = stage(p['stage_stats_before'], role, name); c2, s2 = stage(p['stage_stats_after'], role, name)
                    n += c2 - c1; s += s2 - s1
                if n: print(f'    {role:3} {name:40} n={n:6d} mean={s / n / 1000:8.3f} ms')
