#!/usr/bin/env python3.11
"""Worker anon memory under point-update load, RF3 on real processes (FLAKE1, G7 follow-up).

Runs `Rf3Cluster` (three registered workers, one direct API gateway, mTLS on loopback), drives
`table.update` through the gateway in phases (idle, serial, open-loop rates with many clients,
idle), and samples every worker each second on Linux:

  /proc/<pid>/smaps_rollup   Rss, Anonymous, AnonHugePages
  /proc/<pid>/status         Threads
  /proc/<pid>/maps           glibc malloc heaps (64 MiB aligned anonymous reservations, one or more
                             per non-main arena)

The data group's leader is found by which worker's direct executor ran a probe update. Writes
`--output/memory.json` and prints one summary line per phase and worker. With `--heaptrack`, the
workers are profiled by `heaptrack` and the profiles are left in `--output` (memory samples then
include heaptrack's own overhead and are not comparable). heaptrack attaches to the running workers
(`heaptrack -p`, needs gdb and `kernel.yama.ptrace_scope=0`) once the table is loaded.

Linux only. Example:
  MALLOC_ARENA_MAX=2 python server/tests/oltp_worker_memory.py --binary server/target/release/nabedra-db \
      --output artifacts/memory-arena2
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import threading
import time

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1] / 'orm/python/src')]
from oltp_direct_cluster import WORKERS, Http, Rf3Cluster, operation  # noqa: E402
from nabedra import Database  # noqa: E402

HEAP = 64 << 20
# (name, offered rate per second or None for idle, seconds, clients)
PHASES = (('idle', None, 15, 0), ('serial', 0.2, 30, 1), ('rate-1', 1, 30, 32), ('rate-20', 20, 30, 32),
          ('rate-50', 50, 60, 32), ('rate-50b', 50, 60, 32),
          ('rate-50c', 50, 120, 32), ('idle-after', None, 30, 0))


def kib_fields(path, names):
    out = {}
    for line in Path(path).read_text().splitlines():
        key, _, rest = line.partition(':')
        if key in names:
            out[key] = int(rest.split()[0])
    return out


def malloc_heaps(pid):
    """Anonymous mappings that start on a 64 MiB boundary and, with the reservation right after
    them, span exactly 64 MiB: glibc's per-arena heaps (`HEAP_MAX_SIZE`)."""
    regions = []
    for line in Path(f'/proc/{pid}/maps').read_text().splitlines():
        fields = line.split()
        if len(fields) > 5:
            continue
        start, end = (int(x, 16) for x in fields[0].split('-'))
        regions.append((start, end, fields[1]))
    heaps, i = 0, 0
    while i < len(regions):
        start, end, _ = regions[i]
        if start % HEAP == 0:
            j = i
            while j + 1 < len(regions) and regions[j + 1][0] == regions[j][1] and regions[j + 1][1] - start <= HEAP:
                j += 1
            if regions[j][1] - start == HEAP:
                heaps += 1
                i = j + 1
                continue
        i += 1
    return heaps


def real_pid(pid, binary):
    """The process running `binary`: `pid` itself or a descendant (under a wrapper)."""
    queue = [pid]
    while queue:
        current = queue.pop(0)
        try:
            if os.path.realpath(f'/proc/{current}/exe') == binary:
                return current
            for task in os.listdir(f'/proc/{current}/task'):
                children = Path(f'/proc/{current}/task/{task}/children').read_text().split()
                queue.extend(int(child) for child in children)
        except OSError:
            continue
    return None


def sample(pid):
    rollup = kib_fields(f'/proc/{pid}/smaps_rollup', ('Rss', 'Anonymous', 'AnonHugePages'))
    threads = kib_fields(f'/proc/{pid}/status', ('Threads',))['Threads']
    return dict(rss_kib=rollup['Rss'], anon_kib=rollup['Anonymous'], anon_thp_kib=rollup['AnonHugePages'],
                threads=threads, malloc_heaps=malloc_heaps(pid))


def drive(url, token, ca, table_id, rows, rate, seconds, clients, results):
    """Open loop: request i is due at start + i / rate; `clients` connections take them in turn."""
    start = time.monotonic()
    total = max(1, int(rate * seconds))
    lock, next_index = threading.Lock(), [0]

    def client():
        connection = Http(url, token, 30, ca)
        try:
            while True:
                with lock:
                    index = next_index[0]
                    next_index[0] += 1
                if index >= total:
                    return
                delay = start + index / rate - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                body = operation('table.update', {'table_id': table_id, 'key': random.randrange(rows),
                                                  'values': {'value': index}})
                began = time.monotonic()
                try:
                    status, _ = connection.exchange('/v1/operations', body)
                except (OSError, http.client.HTTPException):
                    status = None
                    connection = Http(url, token, 30, ca)
                with lock:
                    results.append((status, time.monotonic() - began))
        finally:
            connection.close()

    with ThreadPoolExecutor(clients) as pool:
        for _ in range(clients):
            pool.submit(client)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--rows', type=int, default=2000)
    parser.add_argument('--heaptrack', action='store_true')
    parser.add_argument('--label', default='')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    binary = os.path.realpath(args.binary)
    settings = {}
    for name in ('enabled', 'defrag'):
        try:
            settings['thp_' + name] = Path(f'/sys/kernel/mm/transparent_hugepage/{name}').read_text().strip()
        except OSError:
            settings['thp_' + name] = None
    settings.update(malloc_arena_max=os.environ.get('MALLOC_ARENA_MAX'), nproc=os.cpu_count(),
                    glibc_tunables=os.environ.get('GLIBC_TUNABLES'), heaptrack=args.heaptrack, label=args.label)
    cluster = Rf3Cluster(binary, output / 'cluster', api_direct=(True,))
    tracers = {}
    samples, phases = [], []
    try:
        cluster.start()
        cluster.wait_direct_active()
        token, ca = cluster.auth['data_token'], cluster.ca
        url = cluster.api_urls['api1']
        db = Database(url=url, token=token, database='test', ca_file=str(ca), timeout=60)
        table = db.create_table('memory_kv', {'id': int, 'value': int}, key='id')
        api = Http(url, token, 60, ca)
        for key in range(args.rows):
            status, reply = api.exchange('/v1/operations', operation('table.insert', {
                'table_id': table.id, 'row': {'id': key, 'value': 0}}))
            assert status == 200, (status, reply)
        before = cluster.all_stats()
        status, reply = api.exchange('/v1/operations', operation('table.update', {
            'table_id': table.id, 'key': 0, 'values': {'value': 1}}))
        assert status == 200, (status, reply)
        after = cluster.all_stats()
        leaders = [name for name in after if (after[name]['direct'] or {}).get('mutations', 0)
                   > (before[name]['direct'] or {}).get('mutations', 0)]
        assert len(leaders) == 1, leaders
        leader = leaders[0]
        pids = {name: real_pid(cluster.workers[name].pid, binary) for name in WORKERS}
        assert all(pids.values()), pids
        if args.heaptrack:
            # Attached after startup (a worker started under heaptrack misses its bootstrap
            # deadline), so the profiles cover the load phases.
            for name, pid in pids.items():
                log = open(output / f'heaptrack.{name}.log', 'wb')
                tracers[name] = subprocess.Popen(['heaptrack', '-o', str(output / f'heaptrack.{name}'), '-p', str(pid)],
                                                 stdout=log, stderr=log)
            time.sleep(20)
            for name, tracer in tracers.items():
                assert tracer.poll() is None, (name, (output / f'heaptrack.{name}.log').read_text())
        stop = threading.Event()
        current = ['setup']

        def sampler():
            began = time.monotonic()
            while not stop.is_set():
                row = dict(t=round(time.monotonic() - began, 1), phase=current[0])
                for name, pid in pids.items():
                    try:
                        row[name] = sample(pid)
                    except OSError:
                        row[name] = None
                samples.append(row)
                stop.wait(1.0)

        thread = threading.Thread(target=sampler, daemon=True)
        thread.start()
        for name, rate, seconds, clients in PHASES:
            current[0] = name
            results = []
            began = time.monotonic()
            if rate is None:
                time.sleep(seconds)
            else:
                drive(url, token, ca, table.id, args.rows, rate, seconds, clients, results)
            ok = sorted(latency for status, latency in results if status == 200)
            phases.append(dict(phase=name, rate=rate, clients=clients, seconds=round(time.monotonic() - began, 1),
                               offered=len(results), ok=len(ok),
                               statuses={str(s): sum(1 for r in results if r[0] == s) for s in {r[0] for r in results}},
                               p50_ms=round(ok[len(ok) // 2] * 1000, 1) if ok else None,
                               p99_ms=round(ok[min(len(ok) - 1, len(ok) * 99 // 100)] * 1000, 1) if ok else None))
        stop.set()
        thread.join()
        final_stats = {name: cluster.stats(name) for name in WORKERS}
        if args.heaptrack:
            # Stop each worker alone, then let heaptrack finish its profile.
            for name, pid in pids.items():
                os.kill(pid, signal.SIGTERM)
            for name, tracer in tracers.items():
                tracer.wait(timeout=300)
    finally:
        cluster.close()
    summary = {}
    for phase in [p['phase'] for p in phases]:
        rows = [row for row in samples if row['phase'] == phase]
        summary[phase] = {}
        for name in WORKERS:
            values = [row[name] for row in rows if row.get(name)]
            if not values:
                continue
            summary[phase][name] = dict(
                role='leader' if name == leader else 'follower',
                anon_mib_max=round(max(v['anon_kib'] for v in values) / 1024, 1),
                anon_mib_last=round(values[-1]['anon_kib'] / 1024, 1),
                anon_thp_mib_max=round(max(v['anon_thp_kib'] for v in values) / 1024, 1),
                rss_mib_max=round(max(v['rss_kib'] for v in values) / 1024, 1),
                threads_max=max(v['threads'] for v in values),
                malloc_heaps_max=max(v['malloc_heaps'] for v in values))
            line = summary[phase][name]
            print(f"{args.label or 'run'} {phase:<11} {name} {line['role']:<8} anon max {line['anon_mib_max']:6.1f} "
                  f"last {line['anon_mib_last']:6.1f} thp {line['anon_thp_mib_max']:6.1f} rss {line['rss_mib_max']:6.1f} "
                  f"MiB threads {line['threads_max']:3} heaps {line['malloc_heaps_max']}", flush=True)
    for phase in phases:
        print(json.dumps(phase), flush=True)
    (output / 'memory.json').write_text(json.dumps(dict(settings=settings, leader=leader, phases=phases,
                                                        summary=summary, final_stats=final_stats, samples=samples), indent=1))


if __name__ == '__main__':
    main()
