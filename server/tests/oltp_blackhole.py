"""O06/O18: exercise a bidirectional packet drop against one live RF3 worker."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Lock
import time

from oltp_direct_cluster import DATABASE, WORKERS, Http, Rf3Cluster, call, operation, uid
from oltp_recovery_operations import initial_range

from nabedra import Database

PRINT_LOCK = Lock()


def privileged(*args):
    return subprocess.run(['sudo', '-n', *args], check=True, capture_output=True,
                          text=True, timeout=10).stdout


class IsolatedWorkers(Rf3Cluster):
    """The API stays on the host side of the bridge. Each worker has its own network stack."""

    def __init__(self, *args, **options):
        super().__init__(*args, **options)
        tag = uid()[:6]
        self.bridge = f'az{tag}'
        self.namespaces = {name: f'{self.bridge}-{name}' for name in WORKERS}
        self.addresses = {name: f'192.0.2.{index}' for index, name in enumerate(WORKERS, start=2)}
        self.host = '192.0.2.1'

    @contextmanager
    def network(self):
        try:
            privileged('ip', 'link', 'add', self.bridge, 'type', 'bridge')
        except subprocess.CalledProcessError as error:
            raise RuntimeError(error.stderr) from error
        created = []
        try:
            privileged('ip', 'addr', 'add', self.host + '/24', 'dev', self.bridge)
            privileged('ip', 'link', 'set', self.bridge, 'up')
            for name, namespace in self.namespaces.items():
                peer = namespace + 'p'
                privileged('ip', 'netns', 'add', namespace)
                created.append((namespace, peer))
                privileged('ip', 'link', 'add', peer, 'type', 'veth', 'peer', 'name', 'eth0',
                           'netns', namespace)
                privileged('ip', 'link', 'set', peer, 'master', self.bridge)
                privileged('ip', 'link', 'set', peer, 'up')
                privileged('ip', '-n', namespace, 'link', 'set', 'lo', 'up')
                privileged('ip', '-n', namespace, 'addr', 'add', self.addresses[name] + '/24', 'dev', 'eth0')
                privileged('ip', '-n', namespace, 'link', 'set', 'eth0', 'up')
            yield
        finally:
            self.close()
            for namespace, peer in reversed(created):
                try:
                    privileged('ip', 'link', 'delete', peer)
                except subprocess.CalledProcessError as error:
                    if 'Cannot find device' not in error.stderr:
                        raise
                privileged('ip', 'netns', 'delete', namespace)
            privileged('ip', 'link', 'delete', self.bridge)

    @contextmanager
    def blackhole(self, name):
        firewall = ['ip', 'netns', 'exec', self.namespaces[name], 'iptables', '-w', '5']
        try:
            privileged(*firewall, '-I', 'INPUT', '-i', 'eth0', '-j', 'DROP')
            try:
                privileged(*firewall, '-I', 'OUTPUT', '-o', 'eth0', '-j', 'DROP')
                try:
                    yield time.monotonic()
                    for chain in ('INPUT', 'OUTPUT'):
                        counters = privileged(*firewall, '-nvxL', chain)
                        assert any(line.split()[:1] and line.split()[0].isdigit()
                                   and int(line.split()[0]) > 0 and 'DROP' in line
                                   for line in counters.splitlines()), (name, chain, counters)
                finally:
                    privileged(*firewall, '-D', 'OUTPUT', '-o', 'eth0', '-j', 'DROP')
            finally:
                privileged(*firewall, '-D', 'INPUT', '-i', 'eth0', '-j', 'DROP')
        except subprocess.CalledProcessError as error:
            raise RuntimeError(error.stderr) from error

    def certificate(self, name, node_port=None):
        cert = super().certificate(name, node_port)
        address = self.addresses.get(name, self.host)
        cert['endpoint'] = address + ':' + cert['endpoint'].rsplit(':', 1)[1]
        return cert

    def prepare_workers(self):
        super().prepare_workers()
        for name, config in self.config.items():
            config['bind'] = self.addresses[name] + ':' + config['bind'].rsplit(':', 1)[1]
            config['node']['bind'] = self.enrollments[name]['endpoint']

    def launch_worker(self, name, **_):
        super().launch_worker(name, prefix=(
            'sudo', '-n', '-E', 'ip', 'netns', 'exec', self.namespaces[name],
            'setpriv', '--reuid', str(os.getuid()), '--regid', str(os.getgid()), '--init-groups',
        ))


def record(event, **values):
    with PRINT_LOCK:
        print(json.dumps(dict(event=event, **values)), flush=True)


def wait_leaders(cluster, groups, workers, *, down=None, started=None, limit=12):
    deadline = time.monotonic() + limit
    leaders = {}
    observed = {}
    ids = {cluster.enrollments[name]['worker_id']: name for name in workers}
    while time.monotonic() < deadline and len(leaders) < len(groups):
        for label, group in groups.items():
            if label in leaders:
                continue
            reports = []
            for worker in workers:
                probe = cluster.barrier_dir(worker) / 'probe-group-leader'
                probe.write_text(group)
                try:
                    status, stats = call(cluster.urls[worker], '/v1/runtime/stats',
                                         token=cluster.auth['data_token'], timeout=0.5)
                except (OSError, http.client.HTTPException):
                    continue
                finally:
                    probe.unlink(missing_ok=True)
                if status == 200:
                    reports.append(ids.get(stats.get('test_group_leader')))
            if len(reports) == len(workers) and reports[0] and len(set(reports)) == 1:
                leaders[label] = reports[0]
                observed[label] = round(time.monotonic() - started, 3) if started else None
        time.sleep(.05)
    record('raft leaders observed', blackhole=down, leaders=leaders, seconds=observed)
    return leaders


def traffic(cluster, table, write, started, window, first_key):
    label, table_id = table
    client = Http(cluster.api_urls['api1'], cluster.auth['data_token'], 7, cluster.ca)
    successes = []
    trace = []
    value = first_key
    first = True
    try:
        while first or time.monotonic() - started < window:
            first = False
            if write:
                value += 1
                request = operation('table.insert', {'table_id': table_id, 'row': {'id': value, 'value': value}})
            else:
                request = operation('table.get', {'table_id': table_id, 'key': 0})
            before = time.monotonic()
            try:
                status, reply = client.exchange('/v1/operations', request)
            except (OSError, http.client.HTTPException) as error:
                status, reply = None, repr(error)
            completed = round(time.monotonic() - started, 3)
            trace.append(dict(start=round(before - started, 3), end=completed, status=status,
                              error=None if status == 200 else reply))
            if status == 200:
                expected = None if write else {'id': 0, 'value': 100}
                assert reply['data'] == expected, (label, request, reply)
                successes.append((completed, request, reply))
            time.sleep(.1)
    finally:
        client.close()
        record('point attempts', table=label, write=write, attempts=trace)
    return successes, trace


def scenario(cluster, tables, groups, leaders, victim, bound, round_no):
    survivors = [name for name in WORKERS if name != victim]
    token = cluster.auth['data_token']
    gateway = cluster.api_urls['api1']
    for _, table_id in tables:
        assert call(gateway, '/v1/operations', operation('table.get', {'table_id': table_id, 'key': 0}),
                    token=token, ca=cluster.ca)[0] == 200
    record('before drop', victim=victim, workers=leaders)
    results = {}
    window = max(11, bound + 2)
    with cluster.blackhole(victim) as started, ThreadPoolExecutor(max_workers=len(tables) * 2 + 1) as pool:
        elections = pool.submit(wait_leaders, cluster, groups, survivors, down=victim, started=started)
        running = {(table[0], write): pool.submit(traffic, cluster, table, write, started, window, round_no * 10000)
                   for table in tables for write in (False, True)}
        for key, future in running.items():
            results[key] = future.result()
        new_leaders = elections.result()
        assert cluster.workers[victim].alive(), victim
        for worker in survivors:
            stats = cluster.stats(worker)
            record('worker timings', worker=worker, timings=stats['stage_timing'], transport=stats['transport'])
    status, stats = call(gateway, '/v1/runtime/stats', token=token, ca=cluster.ca)
    if status == 200:
        record('api timings', timings=stats['api_stage_timing'], routing=stats['direct_gateway'],
               transport=stats['transport'])
    failures = []
    for (table, write), (successes, trace) in results.items():
        first = successes[0][0] if successes else None
        if first is None or first > bound:
            failures.append((victim, table, 'write' if write else 'read', first, bound))
        if not write and leaders[table] != victim:
            if any(sample['status'] != 200 for sample in trace):
                failures.append((victim, table, 'surviving Data leader read failed'))
        if write:
            for _, request, original in successes:
                row = request['payload']['row']
                body = operation('table.get', {'table_id': request['payload']['table_id'], 'key': row['id']})
                status, reply = call(gateway, '/v1/operations', body, token=token, ca=cluster.ca)
                assert status == 200 and reply['data'] == row, (status, reply, row)
                assert call(gateway, '/v1/operations', request, token=token, ca=cluster.ca) == (200, original)
    assert set(new_leaders) == set(groups), (victim, new_leaders)
    record('drop result', victim=victim, failures=failures)
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--test-executable', required=True)
    parser.add_argument('--output', type=Path, required=True)
    # Up to 3 s of leader lease, 3 s of election delay, and 2 s to
    # renew the Control grant and reach Catalog and Timestamp on a cold leader.
    parser.add_argument('--bound', type=float, default=8)
    args = parser.parse_args()
    assert sys.platform.startswith('linux') and os.getuid() != 0, 'requires Linux with passwordless sudo'
    cluster = IsolatedWorkers(args.binary, args.output, test_executable=args.test_executable)
    with cluster.network():
        cluster.start()
        cluster.wait_direct_active()
        db = Database(url=cluster.api_urls['api1'], token=cluster.auth['data_token'], database=DATABASE,
                      ca_file=str(cluster.ca), timeout=30)
        tables = []
        placements = cluster.config['w1']['node']['placements']
        groups = {label: placements[label]['identity']['group_id'] for label in ('control', 'timestamps')}
        for i in range(2):
            label = f'az1_{i}'
            table = db.create_table(label, {'id': int, 'value': int}, key='id')
            table.insert({'id': 0, 'value': 100})
            tables.append((label, table.id))
            groups[label] = initial_range(cluster, table.id)['group_id']
        failures = []
        remaining = ['timestamps', 'control', *(label for label, _ in tables)]
        round_no = 0
        while remaining:
            leaders = wait_leaders(cluster, groups, WORKERS)
            assert set(leaders) == set(groups), leaders
            victim = leaders[remaining[0]]
            remaining = [label for label in remaining if leaders[label] != victim]
            round_no += 1
            failures.extend(scenario(cluster, tables, groups, leaders, victim, args.bound, round_no))
            cluster.wait_workers()
        leaders = wait_leaders(cluster, groups, WORKERS)
        assert set(leaders) == set(groups), leaders
        follower = min(WORKERS, key=lambda name: sum(leaders[label] == name for label, _ in tables))
        assert any(leaders[label] != follower for label, _ in tables), leaders
        failures.extend(scenario(cluster, tables, groups, leaders, follower, args.bound, round_no + 1))
        assert not failures, failures
        record('O06/O18 packet drop passed', groups=sorted(groups))


if __name__ == '__main__':
    main()
