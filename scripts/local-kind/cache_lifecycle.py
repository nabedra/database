"""Cache zone-labelled process loss and repeated local placement changes in Docker."""
import argparse
from contextlib import ExitStack
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import time
from uuid import UUID

from redis.cluster import key_slot
from redis.exceptions import MovedError

path = Path(__file__).resolve().parents[1] / 'cache-acceptance' / 'cluster.py'
spec = importlib.util.spec_from_file_location('cache_process_fixture', path)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def record(path, event, **details):
    with (path / 'lifecycle.jsonl').open('a') as evidence:
        print(json.dumps(dict(event=event, time=time.monotonic(), **details)), file=evidence)


def usage(fleet, names):
    samples = {}
    for name in names:
        proc = Path('/proc') / str(fleet.processes[name].pid)
        fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
        status = dict(line.split(':', 1) for line in (proc / 'status').read_text().splitlines())
        samples[name] = dict(cpu_ticks=int(fields[11]) + int(fields[12]),
                             rss_kib=int(status['VmRSS'].split()[0]),
                             rss_peak_kib=int(status['VmHWM'].split()[0]))
    return dict(processes=samples,
                net=(Path('/proc/net/dev').read_text()).splitlines(),
                cgroup_memory_current=Path('/sys/fs/cgroup/memory.current').read_text().strip(),
                cgroup_memory_limit=Path('/sys/fs/cgroup/memory.max').read_text().strip())


def validated(view, primaries=None):
    if not fixture.ready(view):
        return False
    if primaries and len({item['primary']['node'] for item in view['slots']}) != primaries:
        return False
    zones = {int(number): member['zone'] for number, member in view['workers'].items()}
    assert all(zones[slot['primary']['node']] != zones[slot['replica']['node']]
               for slot in view['slots']), 'primary and replica share a zone label'
    return True


def read(fleet, node, key, expected, stopped=()):
    ports = {port: member for member, (port, _) in fleet.workers.items()}
    while node not in stopped:
        stopped = (*stopped, node)
        try:
            with fleet.client(node) as client:
                value = client.get(key)
        except MovedError as error:
            node = ports[error.port]
            continue
        except fixture.TRANSPORT:
            return False
        assert value == expected, (node, key, value, expected)
        return True
    return False


def zone_loss(fleet, path, topology):
    survivor_names = ['c2', 'c3', 'w2', 'w3']
    keys = fixture.keys_for(topology)
    values = {node: f'zone-test-{node}'.encode() for node in keys}
    for node, key in keys.items():
        with fleet.client(node) as client:
            assert client.set(key, values[node], px=600_000)
        slot = key_slot(key)
        primary = fixture.wait('written sequence', lambda: (
            progress if (progress := fleet.progress(node, slot)) and progress['sequence'] > 0 else None))
        replica = topology['slots'][slot]['replica']['node']
        fixture.wait('replica has writing prefix', lambda: (
            (progress := fleet.progress(replica, slot)) and progress['baseline']
            and progress['epoch'] == primary['epoch'] and progress['sequence'] >= primary['sequence']))
    stop = threading.Event()
    times, errors = [], []

    def load():
        while not stop.is_set():
            for node, key in keys.items():
                started = time.monotonic()
                alternatives = [node, *sorted(set(keys) - {node})]
                ok = any(read(fleet, target, key, values[node]) for target in alternatives)
                ended = time.monotonic()
                record(path, 'zone read', owner=node, success=ok, started=started,
                       latency_ms=round((ended - started) * 1000, 3))
                if node == 1:
                    (times if ok else errors).append(ended)
                if stop.wait(.05):
                    return

    def guarded_load():
        try:
            load()
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=guarded_load)
    worker.start()
    try:
        fixture.wait('healthy traffic', lambda: len(times) >= 5)
        before = usage(fleet, survivor_names)
        start = time.monotonic()
        record(path, 'K04b zone pause issued', zone='az-1')
        fleet.stop('c1')
        fleet.stop('w1')
        try:
            slot = key_slot(keys[1])
            after = fleet.wait_placement(lambda view: (
                fixture.owner(view, slot) not in (None, 1)
                and view['slots'][slot]['epoch'] > topology['slots'][slot]['epoch']), seconds=60)
            winner = fixture.owner(after, slot)
            fixture.wait('verified lost-zone value from the new primary',
                         lambda: read(fleet, winner, keys[1], values[1], stopped=(1,)), seconds=30)
            recovered = time.monotonic()
            record(path, 'K04b survivor local observation', new_primary=winner, zone='az-1',
                   recovered_seconds=recovered - start, before=before, during=usage(fleet, survivor_names),
                   stopped=['c1', 'w1'])
        finally:
            record(path, 'K04b zone resume issued', zone='az-1')
            fleet.resume('c1')
            fleet.resume('w1')
        fleet.wait_placement(validated, seconds=60)
    finally:
        stop.set()
        worker.join(timeout=12)
    assert not worker.is_alive()
    for failure in errors:
        if isinstance(failure, BaseException):
            raise failure
    for node, key in keys.items():
        view = fleet.status()
        assert read(fleet, fixture.owner(view, key_slot(key)), key, values[node])
    return keys, values


def grow_and_drain(fleet, path, keys, values, count):
    start = time.monotonic()
    extra = range(4, count + 1)
    for node in extra:
        fleet.add_worker(node)
    fleet.wait_placement(lambda view: len(view['workers']) == count, seconds=70)
    view = fleet.status()
    protected = {key_slot(key) for key in keys.values()}
    slots = [slot for slot in range(fixture.SLOTS) if slot not in protected][:len(extra)]
    oracle = []
    for index, node in enumerate(extra):
        slot = slots[index]
        key = next(f'grow:{{{i}}}'.encode() for i in range(150_000)
                   if key_slot(f'grow:{{{i}}}'.encode()) == slot)
        value = f'{count}:{node}'.encode()
        previous = fixture.owner(view, slot)
        with fleet.client(previous) as client:
            assert client.set(key, value, px=600_000)
        fleet.apply('rebalance', '--first', str(slot), '--last', str(slot), '--target', str(node))
        fleet.wait_placement(lambda current: fixture.owner(current, slot) == node, seconds=60)
        fixture.wait('migrated row visible', lambda: read(fleet, node, key, value), seconds=30)
        oracle.append((slot, key, value))
    view = fleet.wait_placement(lambda current: validated(current, count), seconds=70)
    for node, key in keys.items():
        assert read(fleet, fixture.owner(view, key_slot(key)), key, values[node])
    record(path, 'K05 local growth', groups=count, moved_slots=slots, seconds=time.monotonic() - start)
    drain_start = time.monotonic()
    for node in reversed(extra):
        fleet.apply('drain', '--worker', str(node))
        view = fleet.wait_placement(lambda current: str(node) not in current['workers'], seconds=90)
        assert all((not slot['primary'] or slot['primary']['node'] != node)
                   and (not slot['replica'] or slot['replica']['node'] != node) for slot in view['slots'])
        fleet.reap(fleet.processes[f'w{node}'])
        for slot, key, value in oracle:
            fixture.wait('row remains after drain',
                         lambda: read(fleet, fixture.owner(view, slot), key, value), seconds=30)
    fleet.wait_placement(lambda current: validated(current, 3), seconds=60)
    record(path, 'K05 local drain', groups=3, stopped=list(extra), exact_rows=len(oracle),
           seconds=time.monotonic() - drain_start)


def repeated_growth_under_load(fleet, path, keys, values):
    stop = threading.Event()
    observed, failures = [], []

    def load():
        try:
            while not stop.is_set():
                for node, key in keys.items():
                    started = time.monotonic()
                    ok = read(fleet, node, key, values[node])
                    observed.append(ok)
                    record(path, 'K05 live read', owner=node, success=ok,
                           latency_ms=round((time.monotonic() - started) * 1000, 3))
                    if stop.wait(0.25):
                        return
        except BaseException as error:
            failures.append(error)

    worker = threading.Thread(target=load)
    worker.start()
    try:
        fixture.wait('reads before scale-out', lambda: len(observed) >= 3)
        for count in (6, 12):
            before = len(observed)
            grow_and_drain(fleet, path, keys, values, count)
            during = observed[before:]
            assert during and all(during), (count, len(during), failures)
            record(path, 'K05 exact live reads across growth and drain', groups=count, reads=len(during))
    finally:
        stop.set()
        worker.join(timeout=12)
    assert not worker.is_alive()
    if failures:
        raise failures[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp, ExitStack() as scope:
        fleet = fixture.Fleet(args.binary, Path(temp), args.output, scope)
        for node in range(1, 4):
            fleet.controller(node)
        fixture.wait('controller majority', fleet.status, seconds=45)
        for node in range(1, 4):
            fleet.add_worker(node)
        fleet.wait_placement(lambda view: len(view['workers']) == 3 and all(
            UUID(member['id']['incarnation']).int for member in view['workers'].values()))
        fleet.apply('init', '--groups', '3')
        topology = fleet.wait_placement(lambda view: validated(view, 3), seconds=70)
        keys, values = zone_loss(fleet, args.output, topology)
        repeated_growth_under_load(fleet, args.output, keys, values)


if __name__ == '__main__':
    main()
