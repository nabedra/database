"""RD1: container CPU (cpu.stat usage) over the measured phases of a benchmark run directory."""
import json,sys
def usage(f):
    for l in f.split("\n"):
        if l.startswith("usage_usec"): return int(l.split()[1])
for path in sys.argv[1:]:
    r=json.load(open(path+"/results.json"))
    ph=[p for p in r['phases'] if not str(p.get('label','')).endswith('warmup')]
    ops=sum(p['statuses'].get('succeeded',0) for p in ph)
    # window: first measured phase start .. last end
    keys=[k for k in ph[0].keys()]
    samples=[json.loads(l) for l in open(path+"/cgroup-samples.jsonl")]
    t0=min(p['start_unix_ns'] for p in ph); t1=max(p['drain_end_unix_ns'] for p in ph)
    out=[]
    for w in ['w1','w2','w3','api']:
        s=[x for x in samples if x['worker']==w and 'cpu.stat' in x['files']]
        if not t0:
            print("phase keys", keys); break
        inside=[x for x in s if t0<=x['unix_ns']<=t1]
        if len(inside)<2: out.append(f"{w}:n/a"); continue
        du=usage(inside[-1]['files']['cpu.stat'])-usage(inside[0]['files']['cpu.stat']); dt=(inside[-1]['unix_ns']-inside[0]['unix_ns'])/1e3
        out.append(f"{w} {du/dt:.3f}cpu")
    print(path.rstrip('/').split('/')[-1], ops, " ".join(out))
