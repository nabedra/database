#!/usr/bin/env python3
"""Run the LIVE3 OLAP and Cache assertions in one owned Docker lane session."""
import argparse
import hashlib
from pathlib import Path
import signal
import subprocess
import tempfile
import threading

import acceptance

REPO = Path(__file__).resolve().parents[2]
PYTHON = 'python:3.11-slim-bookworm'
BENCH = 'python:3.11-trixie'
VALKEY = 'valkey/valkey:8.1.11-trixie'


def remove(name, evidence):
    acceptance.run('docker', 'rm', '-f', '-v', name, check=False)
    remaining = acceptance.run('docker', 'ps', '-aq', '--filter', f'name=^/{name}$').strip()
    evidence.record('container-cleanup', name=name, remaining=bool(remaining))
    assert not remaining, remaining


def mount(source, target, *, writable=False):
    mapping = f'type=bind,source={source.resolve()},target={target}'
    return ['--mount', mapping if writable else mapping + ',readonly']


def container(scenario, args, evidence, extra, base, command, timeout):
    name = f'nabedra-live3-{scenario}-{acceptance.uid()[:8]}'
    log = args.output / f'{scenario}.log'
    stop = threading.Event()
    peak = []

    def memory():
        while not stop.wait(3):
            current = acceptance.run('docker', 'exec', name, 'cat', '/proc/meminfo', check=False, timeout=8)
            if current.returncode:
                continue
            values = {line.split(':')[0]: int(line.split()[1]) for line in current.stdout.splitlines()
                      if line.startswith(('MemTotal:', 'MemAvailable:'))}
            used = (values['MemTotal'] - values['MemAvailable']) * 1024
            peak.append(used)
            if used > 7 * (1 << 30):
                import os
                os.kill(os.getpid(), signal.SIGUSR1)
                return

    monitor = threading.Thread(target=memory)
    try:
        run = ['docker', 'run', '--rm', '--name', name, '--network', 'none',
               '--memory', '2560m', '--memory-swap', '2560m', '--cpus', '4',
               '-e', 'PYTHONDONTWRITEBYTECODE=1',
               *mount(REPO, '/source'), *mount(args.binary, '/nabedra-db'),
               *mount(args.output, '/result', writable=True), *extra, base,
               'sh', '-ec', command]
        evidence.record(scenario, status='started', image=base, container=name)
        monitor.start()
        with log.open('wb') as output:
            result = subprocess.run(run, stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
        print(log.read_text()[-10000:], flush=True)
        evidence.record(scenario, status='passed' if result.returncode == 0 else 'failed',
                        exit_code=result.returncode, vm_peak_used_bytes=max(peak) if peak else None)
        return result.returncode
    finally:
        stop.set()
        monitor.join(timeout=12)
        remove(name, evidence)


def cache(args, evidence):
    wheels = mount(args.redis_wheel, '/redis-6.4.0-py3-none-any.whl')
    lifecycle = container('cache-lifecycle', args, evidence,
                          [*wheels, '-e', 'PHASE=' + args.cache_phase], args.bench_image, '''
        export PYTHONPATH=/redis-6.4.0-py3-none-any.whl
        cp /nabedra-db /tmp/nabedra-db
        chmod 755 /tmp/nabedra-db
        exec python3 /source/scripts/local-kind/cache_lifecycle.py --binary /tmp/nabedra-db --output /result/history --phase "$PHASE"
    ''', 1200)
    if args.scenario == 'cache-lifecycle':
        return lifecycle
    return max(lifecycle, pair(args, evidence))


def pair(args, evidence):
    wheels = mount(args.redis_wheel, '/redis-6.4.0-py3-none-any.whl')
    with tempfile.TemporaryDirectory(prefix='nabedra-live3-valkey-') as scratch:
        reference = Path(scratch) / 'valkey-server'
        name = f'nabedra-live3-valkey-{acceptance.uid()[:8]}'
        try:
            acceptance.run('docker', 'create', '--name', name, VALKEY, timeout=150)
            acceptance.run('docker', 'cp', f'{name}:/usr/local/bin/valkey-server', str(reference))
            pin = acceptance.run('docker', 'image', 'inspect', '--format', '{{json .RepoDigests}}', VALKEY)
            evidence.record('valkey-reference', image=VALKEY, image_digests=pin.strip(),
                            sha256=hashlib.sha256(reference.read_bytes()).hexdigest())
        finally:
            remove(name, evidence)
        return container('cache-pair', args, evidence, [
            *wheels, *mount(reference, '/valkey-server'), *mount(args.hiredis_wheel, '/wheels/' + args.hiredis_wheel.name),
            '-e', 'REV=' + args.revision,
        ], args.bench_image, '''
            python3 -m pip install --no-index --no-deps --disable-pip-version-check /wheels/hiredis-*.whl
            export PYTHONPATH=/redis-6.4.0-py3-none-any.whl
            exec python3 /source/scripts/cache-benchmark/run.py --environment docker \
              --nabedra /nabedra-db --valkey /valkey-server --source-revision "$REV" \
              --output /result/pair --drivers stock --transports tls --sizes 65536 \
              --workloads get-hit,set-px --offered 650 --samples 10000 --repetitions 3 \
              --server-cpus 0 --client-cpus 1,2,3 --lanes 16
        ''', 480)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario', choices=('olap', 'cache', 'cache-lifecycle', 'cache-pair'))
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--lane', required=True, type=Path)
    parser.add_argument('--redis-wheel', type=Path)
    parser.add_argument('--hiredis-wheel', type=Path)
    parser.add_argument('--bench-image', default=BENCH)
    parser.add_argument('--cache-phase', choices=('all', 'zone', 'growth'), default='all')
    parser.add_argument('--load-archive', type=Path)
    args = parser.parse_args()
    if args.scenario.startswith('cache') and not args.redis_wheel:
        parser.error('Cache requires the redis 6.4.0 wheel')
    if args.scenario in ('cache', 'cache-pair') and not args.hiredis_wheel:
        parser.error('The paired run requires a Linux x86-64 hiredis 3.2.1 wheel')
    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGUSR1):
        signal.signal(signum, acceptance.interrupted)
    evidence = acceptance.Evidence(args.output)
    assert Path('/Volumes/ZX20/Docker/vms/0/data/Docker.raw').is_file(), '/Volumes/ZX20 Docker disk missing'
    assert acceptance.run('docker', 'context', 'show').strip() == 'desktop-linux'
    evidence.record('docker-checks', scenario=args.scenario, server_source=args.revision,
                    sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest())
    label = 'LIVE3 BENCH' if args.scenario in ('cache-pair', 'cache') else 'LIVE3 Docker'
    with acceptance.lane(args.lane, evidence, label):
        if args.load_archive:
            evidence.file('image-load.txt', acceptance.run(
                'docker', 'load', '-i', str(args.load_archive), timeout=300))
        if args.scenario == 'olap':
            return container('olap', args, evidence, [], PYTHON, '''
                cp /nabedra-db /tmp/nabedra-db
                chmod 755 /tmp/nabedra-db
                exec python3 /source/scripts/acceptance_providers.py --binary /tmp/nabedra-db --stream-drain
            ''', 360)
        if args.scenario == 'cache-pair':
            return pair(args, evidence)
        return cache(args, evidence)


if __name__ == '__main__':
    import time
    acceptance.START = time.monotonic()
    raise SystemExit(main())
