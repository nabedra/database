"""ACC3-ready-fence, independent review test (PR #121 review).

Three real worker processes start an RF3 cluster. The third one is held, by an
LD_PRELOAD shim, right after it printed `queue ready revision sealed`, which is
the last thing it does before the Timestamp ready fence. The other two workers
are then frozen (SIGSTOP) for FREEZE seconds, so no Timestamp leader answers for
longer than one service call, and thawed. A joining worker must outlive that and
the cluster must become ready with three workers. FREEZE=0 is the control.

usage: review121_fence.py <freeze seconds> <binary> <test executable, unused>
"""
import os
import pathlib
import subprocess
import sys
import tempfile
import time

TESTS = pathlib.Path(__file__).resolve().parent
FREEZE, BINARY = float(sys.argv[1]), sys.argv[2]
sys.path.insert(0, str(TESTS))
from oltp_direct_cluster import Rf3Cluster, WORKERS  # noqa: E402

SEALED, COMMITTED = 'queue ready revision sealed', 'timestamp ready fence committed'
HELD = 'w3'


def main():
    with tempfile.TemporaryDirectory(prefix='r121-fence-', dir=os.environ.get('R121_TMP')) as directory:
        SHIM = str(pathlib.Path(directory) / 'hold.so')
        subprocess.run(['cc', '-shared', '-fPIC', '-O1', '-o', SHIM, str(TESTS / 'review121_hold.c'), '-ldl'], check=True)
        cluster = Rf3Cluster(BINARY, directory, api_direct=())
        hold = pathlib.Path(directory) / 'hold'
        cluster.prepare_workers()
        frozen = []
        try:
            for name in WORKERS:
                cluster.worker_env = {} if name != HELD else dict(
                    LD_PRELOAD=SHIM, R121_HOLD=str(hold), R121_MARK=SEALED)
                cluster.launch_worker(name)
            deadline = time.monotonic() + 60
            while not pathlib.Path(f'{hold}.reached').exists():
                assert cluster.workers[HELD].alive(), 'setup: the joining worker exited early'
                assert time.monotonic() < deadline, 'setup: the joining worker never sealed'
                time.sleep(0.01)
            log = pathlib.Path(directory) / f'{HELD}.log'
            text = log.read_text(errors='replace')
            assert SEALED in text and COMMITTED not in text, 'setup: the hold is not before the fence'
            print(f'held {HELD} between the sealed revision and the fence', flush=True)
            if FREEZE:
                for name in WORKERS:
                    if name != HELD:
                        cluster.workers[name].pause()
                        frozen.append(name)
            pathlib.Path(f'{hold}.release').touch()
            started = time.monotonic()
            died = None
            while time.monotonic() - started < FREEZE:
                if died is None and not cluster.workers[HELD].alive():
                    died = time.monotonic() - started
                time.sleep(0.1)
            for name in frozen:
                cluster.workers[name].resume()
            frozen.clear()
            deadline = time.monotonic() + 90
            while died is None and time.monotonic() < deadline:
                if not cluster.workers[HELD].alive():
                    died = time.monotonic() - started
                elif COMMITTED in log.read_text(errors='replace'):
                    break
                time.sleep(0.1)
            text = log.read_text(errors='replace')
            print('--- the joining worker', *[line for line in text.splitlines() if 'nabedra-db' in line][-6:],
                  sep='\n', flush=True)
            assert died is None, (f'the joining worker exited {died:.1f} s into a {FREEZE:.0f} s Timestamp outage, '
                                  f'before the fence committed={COMMITTED not in text}')
            assert COMMITTED in text, 'the fence never committed after the outage'
            cluster.wait_workers(deadline_seconds=90)
            print(f'three workers ready, {time.monotonic() - started:.1f} s after the release', flush=True)
        finally:
            for name in frozen:
                cluster.workers[name].resume()
            cluster.kill_all()
            cluster.close()


main()
