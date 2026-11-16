#!/usr/bin/env python3.11
"""GW01/GW02 and partial SL07 real loopback overhead evidence, no compute or AWS.

Run only on a quiet host. Sequential bounded clients use one connection at a time.
This measures forwarding, parsing, middleware and HTTP, not worker scheduling.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import resource
import subprocess
import sys
import time
import tracemalloc
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'orm' / 'src'))
import httpx
from meshdb.gateway import Gateway, S3, AwsCredentials
from gateway_host_fixture import endpoint


def percentile(values, q):
    return sorted(values)[max(0, math.ceil(len(values)*q)-1)]


def benchmark(args):
    rng = random.Random(args.seed)
    seen = []
    middleware_ms = []
    def upstream(method, path, headers, body):
        value = json.loads(body)
        seen.append((value['request_id'], hashlib.sha256(body).hexdigest()))
        raw = json.dumps({'request_id':value['request_id'],'data':[]}).encode()
        return 200, (), raw
    records = []
    tracemalloc.start()
    with endpoint(upstream) as compute:
        # The actual upstream address is allocated by the fixture, never a caller header.
        with Gateway(cluster_url=compute,token='fixture-compute',mount_path='/meshdb',max_concurrency=4) as gateway:
            @gateway.middleware
            def auth(ctx):
                started=time.perf_counter()
                ctx.principal='fixture-user'
                ctx.storage=S3('fixture-bucket',region='eu-west-1',credentials=AwsCredentials('fixture-access','fixture-secret'))
                middleware_ms.append((time.perf_counter()-started)*1000)
            def forward(method,path,headers,body):
                res=gateway.handle(method=method,path=path,headers=headers,body=body)
                return res.status,res.headers,res.body
            with endpoint(forward) as public, httpx.Client(trust_env=False,timeout=5) as client:
                for rep in range(-1,args.repetitions):
                    modes=['direct-http','gateway-http']; rng.shuffle(modes)
                    for mode in modes:
                        expected=[]; latencies=[]; before=len(seen); mid_before=len(middleware_ms)
                        url=compute+'/v1/operations' if mode=='direct-http' else public+'/meshdb/v1/operations'
                        count=args.warmup if rep==-1 else args.requests
                        started=time.perf_counter(); cpu=time.process_time()
                        for _ in range(count):
                            rid=str(uuid.UUID(int=rng.getrandbits(128),version=4))
                            raw=(' { "request_id" : "'+rid+'", "operation":"table.list", "payload": {} } ').encode()
                            expected.append((rid,hashlib.sha256(raw).hexdigest()))
                            t=time.perf_counter()
                            response=client.post(url,content=raw,headers={'Content-Type':'application/json','Authorization':'Bearer fixture-app'})
                            latencies.append((time.perf_counter()-t)*1000)
                            assert response.status_code==200 and response.json()=={'request_id':rid,'data':[]}
                        elapsed=time.perf_counter()-started
                        actual=seen[before:]
                        assert Counter(actual)==Counter(expected) and len(actual)==len(expected)
                        current,peak=tracemalloc.get_traced_memory()
                        if rep>=0:
                            mids=middleware_ms[mid_before:]
                            records.append(dict(mode=mode,repetition=rep+1,requests=count,p50_ms=percentile(latencies,.5),p95_ms=percentile(latencies,.95),p99_ms=percentile(latencies,.99),throughput_rps=count/elapsed,elapsed_s=elapsed,cpu_s=time.process_time()-cpu,latencies_ms=latencies,middleware_p95_ms=percentile(mids,.95) if mids else None,traced_current_bytes=current,traced_peak_bytes=peak,process_peak_rss_raw=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,lost_ids=0,duplicate_ids=0,body_digest_mismatches=0,request_body_bytes=sum(len((' { "request_id" : "'+rid+'", "operation":"table.list", "payload": {} } ').encode()) for rid,_ in expected)))
    tracemalloc.stop()
    return dict(scope='loopback HTTP forwarding overhead; no worker execution, AWS or tenant load',timestamp=datetime.now(timezone.utc).isoformat(),configuration={**vars(args), 'arrival_schedule':'sequential closed loop, one outstanding request', 'gateway_capacity':4, 'host_connection_policy':'close after each response', 'tracemalloc_enabled':True, 'comparison':'one HTTP hop versus two hops plus gateway'},python=sys.version,httpx=httpx.__version__,platform=platform.platform(),cpu_count=os.cpu_count(),cpu_quota='not measured',rss_units='bytes on macOS; KiB on Linux',memory_scope='combined client, host and upstream in one process; cumulative high water, not per-worker',commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),dirty_worktree=True,unmeasured=['queue latency','compute slot occupancy','charged bytes','S3/network costs','credential lookup I/O','partition sizes'],records=records)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='artifacts/cli-serverless/gateway-http.json')
    parser.add_argument('--requests',type=int,default=100)
    parser.add_argument('--warmup',type=int,default=20)
    parser.add_argument('--repetitions',type=int,default=3)
    parser.add_argument('--seed',type=int,default=20261116)
    args=parser.parse_args()
    if min(args.requests,args.warmup)<=0 or args.repetitions<3: parser.error('positive counts and at least three repetitions required')
    result=benchmark(args)
    path=Path(args.output); path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(result,indent=2)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,2,figsize=(9,3.5))
    for mode in ('direct-http','gateway-http'):
        rows=[r for r in result['records'] if r['mode']==mode]
        axes[0].plot([r['repetition'] for r in rows],[r['p95_ms'] for r in rows],marker='o',label=mode)
        axes[1].plot([r['repetition'] for r in rows],[r['throughput_rps'] for r in rows],marker='o',label=mode)
    for ax,title,y in zip(axes,('Completion latency','Sequential throughput'),('p95 milliseconds','requests / second')):
        ax.set(title=title,xlabel='Measured repetition',ylabel=y); ax.set_xticks(range(1,args.repetitions+1)); ax.legend()
    fig.suptitle('Loopback HTTP fixture, no compute execution'); fig.tight_layout()
    fig.savefig(path.with_suffix('.svg')); fig.savefig(path.with_suffix('.png'),dpi=160); plt.close(fig)
    print(str(path))

if __name__=='__main__': main()
