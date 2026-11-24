"""Scratch probe for ACC3-ready-fence (never shipped).

A retained worker rejoins. It is held after it sealed its ready revision and before its
Timestamp ready fence. Both other workers, so the Timestamp quorum, are then stopped for
longer than one service call, and resumed. The join must finish on the original run.
"""
import argparse
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from oltp_runtime_acceptance import *  # noqa: F401,F403
from oltp_runtime_acceptance import WORKERS, launch_retained, native_cluster, prepare_rolling_restart
from oltp_runtime_acceptance import release, same_run_ready, wait_reached, write_json

parser = argparse.ArgumentParser()
parser.add_argument('--binary', required=True)
parser.add_argument('--test-executable', required=True)
parser.add_argument('--hold', type=float, default=15)
args = parser.parse_args()

with native_cluster(args.binary, args.test_executable, 'ready-fence', mounts=True) as cluster:
    run = cluster.run_id
    joining, others = WORKERS[2], WORKERS[:2]
    cluster.workers[joining].kill()
    same_run_ready(cluster, run, others, 2)
    restart = prepare_rolling_restart(cluster, joining, others[0])
    join_dir = cluster.barrier_dir(joining)
    write_json(join_dir / 'gates.json', {restart['request_id']: 'probe_before_ready_fence'})
    launch_retained(cluster, joining)
    wait_reached([join_dir], restart, 'probe_before_ready_fence')
    for name in others:
        cluster.workers[name].pause()
    try:
        release(join_dir, restart)
        time.sleep(args.hold)
        alive = cluster.workers[joining].alive()
    finally:
        for name in others:
            cluster.workers[name].resume()
    print(f'PROBE ready fence: joining worker alive after the outage: {alive}', flush=True)
    assert alive, 'the joining worker exited when one ready-fence call failed'
    same_run_ready(cluster, run, WORKERS, 3)
    print('PROBE ready fence: three workers ready on the original run', flush=True)
