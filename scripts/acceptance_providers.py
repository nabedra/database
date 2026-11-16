"""A01/A02 real API/worker callback transport and safe removal, no cloud calls."""
import argparse
import copy
import http.client
import json
import os
from pathlib import Path
import secrets
import select
import socket
import socketserver
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from acceptance_nodes import FrontDoor, announcement, call, receive, send, target, write

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'orm/src'))
from nabedra._callback_bridge import _CallbackBridge
from nabedra import ValidationError, Cluster, NabedraError
from nabedra._serve_args import _validate_serve_args


class GatedForward(socketserver.BaseRequestHandler):
    """Hold complete operation requests at a deterministic transport barrier."""
    def handle(self):
        try:
            self.request.settimeout(30)
            initial = bytearray()
            while b'\r\n\r\n' not in initial:
                chunk = self.request.recv(65536)
                if not chunk:
                    return
                initial.extend(chunk)
                assert len(initial) < 2 << 20
            path = initial.split(b' ', 2)[1].decode()
            with self.server.lock:
                self.server.requests.append((path, None, None))
            if path in ('/internal/v1/operations', '/internal/v1/jobs'):
                self.server.operation_entered.set()
                if not self.server.operation_release.wait(25):
                    raise AssertionError('operation barrier watchdog')
            with socket.create_connection(self.server.target, timeout=5) as upstream:
                upstream.sendall(initial)
                peers = {self.request: upstream, upstream: self.request}
                end = time.monotonic() + 30
                while time.monotonic() < end:
                    ready, _, _ = select.select(list(peers), [], [], .1)
                    for source in ready:
                        chunk = source.recv(65536)
                        if not chunk:
                            return
                        peers[source].sendall(chunk)
        except (OSError, AssertionError):
            return


def wait_for(predicate, label, seconds=30):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.03)
    raise AssertionError(label + ' watchdog')


def request(base, token, method, path, value=None):
    connection = http.client.HTTPConnection(*target(base), timeout=20)
    try:
        connection.request(method, path, None if value is None else json.dumps(value),
                           {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Connection': 'close'})
        response = connection.getresponse()
        return response.status, json.loads(response.read(8 << 20))
    finally:
        connection.close()


def run(binary, skip_zero=False):
    # A01 must fail before any callback or child resource exists.
    try:
        _validate_serve_args('Manual', (1, 2))
        raise AssertionError('A01 autoscaling without adapter accepted')
    except ValidationError:
        pass
    children, doors, channels = [], [], []
    bridge = None
    with tempfile.TemporaryDirectory(prefix='nabedra-provider-') as directory:
        root = Path(directory)
        diagnostics = (root / 'diagnostics').open('wb')
        env = {'PATH': os.defpath, 'NABEDRA_STORE_TOKEN': 'fixture-store-token'}
        release_stop = threading.Event()
        stop_entered = threading.Event()
        stopped = threading.Event()
        operations_release = threading.Event()
        operations_release.set()
        events, resources = [], {}
        provider_lock = threading.Lock()
        fail_inventory = threading.Event()
        try:
            store = subprocess.Popen([sys.executable, str(Path(__file__).with_name('local_store.py')),
                                      '--database', str(root / 'objects.sqlite'), '--port', '0'],
                                     env=env, stdout=subprocess.PIPE, stderr=diagnostics)
            children.append(store)
            address = announcement(store)
            backend = {'driver': 'http', 'options': {'base_url': 'http://%s:%d' % (address['host'], address['port']),
                       'namespace': 'providers/', 'token_file': 'token', 'ca_file': None}}
            (root / 'token').write_text('fixture-store-token\n')
            (root / 'token').chmod(0o600)
            auth = dict(data_token='d'*43, admin_token='a'*43, enrollment_token='e'*43)
            write(root / 'auth', auth)
            api_door = FrontDoor()
            doors.append(api_door)
            cluster = str(uuid.uuid4())
            worker = dict(listen='127.0.0.1:0', operation_slots=2, partition_slots=2,
                          queue_size=4, partition_size='4 KiB')
            write(root / 'api.json', dict(version=1, cluster_id=cluster, bind='127.0.0.1:0',
                  advertise_url=api_door.url, api_urls=[api_door.url], storage=backend,
                  auth_file='auth', tls=None, worker=worker,
                  lifecycle=dict(heartbeat_ms=100, expiry_ms=3000, readiness_ms=15000, drain_ms=15000,
                                 lease_ms=10000, renew_ms=2000, stabilization_ms=1500, cooldown_ms=500,
                                 retry_initial_ms=100, retry_max_ms=1000, max_attempts=8)))

            def control():
                try:
                    with sqlite3.connect(str(root / 'objects.sqlite'), timeout=2) as connection:
                        row = connection.execute('SELECT body FROM objects WHERE key=?', ('providers/cluster/control.json',)).fetchone()
                    return None if row is None else json.loads(row[0])
                except sqlite3.Error:
                    return None

            def start_node(payload):
                launch = payload['request_id']
                with provider_lock:
                    if launch in resources:
                        return resources[launch]['resource_id']
                door = FrontDoor()
                door.RequestHandlerClass = GatedForward
                door.operation_entered = threading.Event()
                door.operation_release = operations_release
                doors.append(door)
                bootstrap = copy.deepcopy(payload['bootstrap'])
                bootstrap['advertise_url'] = door.url
                path = root / ('worker-' + launch + '.json')
                write(path, bootstrap)
                child = subprocess.Popen([str(binary), 'worker', '--bootstrap', str(path)],
                                         env=env, stdout=subprocess.PIPE, stderr=diagnostics)
                children.append(child)
                door.target = target(announcement(child)['bound_url'])
                with provider_lock:
                    resources[launch] = dict(resource_id='fixture/' + launch, cluster_id=cluster,
                        node_id=payload['node_id'], request_id=launch, state='running', child=child, door=door)
                    events.append(('started', launch))
                return 'fixture/' + launch

            def stop_node(resource_id):
                snapshot = control()
                with provider_lock:
                    selected = next(r for r in resources.values() if r['resource_id'] == resource_id)
                member = next(n for n in snapshot['nodes'] if n['node_id'] == selected['node_id'])
                assert member['state'] == 'draining' and member['drain']['confirmed_idle'] is True
                assert member['drain']['ack_seq'] > 0
                assert snapshot['scheduler'].get('coordinator_node') != member['node_id']
                survivors = [n for n in snapshot['nodes'] if n['state'] == 'ready' and n['node_id'] != member['node_id']]
                if not survivors:
                    assert snapshot['policy']['mode'] == 'fixed' and snapshot['policy']['desired'] == 0
                    assert snapshot['scheduler'].get('coordinator_node') is None
                assert any(i['action'] == 'stop' and i['node_id'] == member['node_id']
                           and i['state'] == 'in_progress' and i['delivery'] for i in snapshot['intents'])
                events.append(('drain_authorized', selected['request_id']))
                stop_entered.set()
                assert release_stop.wait(15), 'stop effect barrier watchdog'
                selected['child'].terminate()
                assert selected['child'].wait(timeout=15) == 0
                with provider_lock:
                    selected['state'] = 'terminated'
                    events.append(('terminated', selected['request_id']))
                stopped.set()

            def list_nodes():
                if fail_inventory.is_set():
                    events.append(('inventory_failed', None))
                    raise RuntimeError('fixture inventory unavailable')
                with provider_lock:
                    return [{k: r[k] for k in ('resource_id','cluster_id','node_id','request_id','state')} for r in resources.values()]

            callback_token, authorize_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            bridge = _CallbackBridge((start_node, stop_node, list_nodes), callback_token, authorize_token)
            channel, peer = socket.socketpair()
            channel.settimeout(30)
            channels.append(channel)
            api = subprocess.Popen([str(binary), 'api', '--config', str(root / 'api.json')],
                                   env=dict(env, NABEDRA_SUPERVISOR_FD=str(peer.fileno())), pass_fds=(peer.fileno(),),
                                   stdout=diagnostics, stderr=diagnostics)
            children.append(api)
            peer.close()
            send(channel, dict(version=1, type='init', name='Providers', provisioning='managed',
                 initial_policy=dict(mode='autoscale', desired=1, min_nodes=1, max_nodes=2),
                 bridge=dict(url=bridge.url, callback_token=callback_token, authorize_token=authorize_token)))
            prepared = receive(channel)
            api_door.target = target(prepared['bound_url'])
            bridge.activate(cluster, prepared['authorize_url'])
            send(channel, dict(version=1, type='activate'))
            assert receive(channel)['type'] == 'ready'
            cluster_client = Cluster(api_door.url, auth['admin_token'])
            status = cluster_client.status()
            assert status['capabilities'] == dict(provision=True, terminate=True, autoscale=True)
            accepted_policy = cluster_client.scale(1)
            assert accepted_policy['mode'] == 'fixed' and accepted_policy['desired'] == 1
            policy_id = str(uuid.uuid4())
            policy_body = dict(request_id=policy_id, operation='cluster.scale', payload=dict(nodes=1))
            first_policy = request(api_door.url, auth['admin_token'], 'POST', '/v1/cluster/operations', policy_body)
            assert first_policy[0] == 200
            assert request(api_door.url, auth['admin_token'], 'POST', '/v1/cluster/operations', policy_body) == first_policy
            conflicting = dict(policy_body, payload=dict(nodes=2))
            assert request(api_door.url, auth['admin_token'], 'POST', '/v1/cluster/operations', conflicting)[0] == 409
            assert request(api_door.url, auth['data_token'], 'POST', '/v1/cluster/operations',
                           dict(request_id=str(uuid.uuid4()), operation='cluster.status', payload={}))[0] == 401
            accepted_policy = cluster_client.autoscale(min_nodes=1, max_nodes=2)
            assert accepted_policy['mode'] == 'autoscale' and accepted_policy['max_nodes'] == 2
            wait_for(lambda: (control() or {}).get('nodes') and any(n['state']=='ready' for n in control()['nodes']), 'initial provider worker')
            wait_for(lambda: call(api_door.url, 'table.list', {}, auth['data_token'])[0] == 200, 'initial queue service')
            status, created = call(api_door.url, 'table.create', dict(name='items', schema={'id':'int64'}, key='id', format='parquet'), auth['data_token'])
            assert status == 200, created
            table = created['data']['id']
            assert call(api_door.url, 'table.insert', dict(table_id=table, row={'id':1}), auth['data_token'])[0] == 200
            operations_release.clear()
            for resource in list(resources.values()):
                resource['door'].operation_entered.clear()
            accepted = []
            for _ in range(8):
                identity = str(uuid.uuid4())
                status, receipt = request(api_door.url, auth['data_token'], 'POST', '/v1/requests',
                    dict(request_id=identity, operation='table.get', payload=dict(table_id=table, key=1)))
                assert status == 202, receipt
                accepted.append((identity, receipt['run_id']))
            assert wait_for(lambda: any(r['door'].operation_entered.is_set() for r in resources.values()), 'CPU request barrier')
            wait_for(lambda: len(resources)==2 and len([n for n in (control() or {}).get('nodes',[]) if n['state']=='ready'])==2, 'provider scales up')
            # Provider outage cannot discard accepted IDs or create removals.
            fail_inventory.set()
            wait_for(lambda: any(e[0]=='inventory_failed' for e in events), 'inventory fault')
            for identity, run_id in accepted:
                status, receipt = request(api_door.url, auth['data_token'], 'GET', '/v1/requests/'+run_id+'/'+identity)
                assert status == 200 and receipt['state'] in ('queued','running','committing','reconciling'), receipt
            assert not stop_entered.is_set()
            fail_inventory.clear()
            operations_release.set()

            def terminal(pair):
                identity, run_id = pair
                status, receipt = request(api_door.url, auth['data_token'], 'GET', '/v1/requests/'+run_id+'/'+identity)
                if status == 503:
                    return None
                assert status == 200, receipt
                if receipt['state'] in ('succeeded','failed','abandoned'):
                    assert receipt['state']=='succeeded' and receipt['result']=={'id':1}, receipt
                    return receipt
                return None

            for pair in accepted:
                wait_for(lambda pair=pair: terminal(pair), 'accepted read completion')
            assert stop_entered.wait(30), 'safe scale down watchdog'
            # The provider callback has entered only after queue handoff and a
            # proof-bound idle barrier. Accept more work before removal takes effect.
            operations_release.clear()
            overlap = []
            for _ in range(4):
                identity = str(uuid.uuid4())
                status, receipt = request(api_door.url, auth['data_token'], 'POST', '/v1/requests',
                    dict(request_id=identity, operation='table.get', payload=dict(table_id=table, key=1)))
                assert status == 202, receipt
                overlap.append((identity, receipt['run_id']))
            release_stop.set()
            assert stopped.wait(15), 'provider worker exit watchdog'
            for identity, run_id in overlap:
                status, receipt = request(api_door.url, auth['data_token'], 'GET', '/v1/requests/'+run_id+'/'+identity)
                assert status == 200 and receipt['state'] in ('queued','running','committing','reconciling'), receipt
            operations_release.set()
            for pair in overlap:
                wait_for(lambda pair=pair: terminal(pair), 'drain-overlap accepted completion')
            assert call(api_door.url, 'table.get', dict(table_id=table,key=1), auth['data_token'])[1]['data']=={'id':1}
            public_status = cluster_client.status()
            assert public_status['counts']['ready'] == 1 and public_status['capabilities']['autoscale']
            assert all('resources' in node and 'available_threads' in node for node in public_status['nodes'])
            wait_for(lambda: (control() or {}).get('policy', {}).get('desired') == 1 and
                     len([n for n in control()['nodes'] if n['state'] == 'ready']) == 1 and
                     any(i['action'] == 'stop' and i['state'] == 'completed' and i['outcome'] == 'terminated'
                         for i in control()['intents']), 'provider termination reconciliation')
            assert len({identity for identity,_ in accepted+overlap})==12
            assert any(e[0]=='drain_authorized' for e in events) and any(e[0]=='terminated' for e in events)
            if not skip_zero:
                # An explicit fixed zero is an operator shutdown. It must drain
                # the final scheduler, prove provider termination, then fence a new
                # empty request run before the next launch while retaining data.
                cluster_client.scale(0, wait=True, timeout=30)
                assert cluster_client.status()['counts']['ready'] == 0
                cluster_client.scale(1, wait=True, timeout=30)
                status, restarted = request(api_door.url, auth['data_token'], 'POST', '/v1/requests',
                    dict(request_id=str(uuid.uuid4()), operation='table.get', payload=dict(table_id=table,key=1)))
                assert status == 202 and restarted['run_id'] != accepted[0][1]
                new_pair = (restarted['request_id'], restarted['run_id'])
                wait_for(lambda: terminal(new_pair), 'explicit zero restart retains committed row')
            # A01 also exercises the real public admin endpoint in a separate
            # fixed/manual namespace without constructing any provider bridge.
            manual_door = FrontDoor()
            doors.append(manual_door)
            config = json.loads((root / 'api.json').read_text())
            config.update(cluster_id=str(uuid.uuid4()), advertise_url=manual_door.url, api_urls=[manual_door.url])
            config['storage']['options']['namespace'] = 'manual-provider-test/'
            write(root / 'manual-api.json', config)
            manual_channel, manual_peer = socket.socketpair()
            manual_channel.settimeout(30)
            channels.append(manual_channel)
            manual = subprocess.Popen([str(binary), 'api', '--config', str(root / 'manual-api.json')],
                env=dict(env, NABEDRA_SUPERVISOR_FD=str(manual_peer.fileno())), pass_fds=(manual_peer.fileno(),),
                stdout=diagnostics, stderr=diagnostics)
            children.append(manual)
            manual_peer.close()
            send(manual_channel, dict(version=1,type='init',name='Manual',provisioning='external',bridge=None,
                initial_policy=dict(mode='fixed',desired=0,min_nodes=0,max_nodes=0)))
            manual_prepared = receive(manual_channel)
            assert manual_prepared['authorize_url'] is None
            manual_door.target = target(manual_prepared['bound_url'])
            send(manual_channel, dict(version=1,type='activate'))
            assert receive(manual_channel)['type']=='ready'
            manual_client = Cluster(manual_door.url, auth['admin_token'])
            assert manual_client.status()['capabilities'] == dict(provision=False,terminate=False,autoscale=False)
            try:
                manual_client.autoscale(min_nodes=1,max_nodes=2)
                raise AssertionError('A01 real manual API invented autoscaling')
            except NabedraError as error:
                assert error.code=='unsupported_capability' and error.outcome=='not_applied'
            assert manual_client.status()['counts']==dict(ready=0,starting=0,draining=0,lost=0)
            print('PASS A01/A02: public policies, configured callbacks, inventory failure retains 8 accepted IDs, 4 accepted IDs survive safe removal, committed row retained; explicit-zero=' + str(not skip_zero))
        finally:
            operations_release.set()
            release_stop.set()
            if bridge is not None:
                bridge.stop_admission()
            for channel in channels:
                try:
                    send(channel, dict(version=1,type='shutdown'))
                except OSError:
                    pass
                channel.close()
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=5)
                if child.stdout:
                    child.stdout.close()
            if bridge is not None:
                bridge.close()
                assert not bridge.alive, 'provider callbacks failed to settle'
            for door in reversed(doors):
                door.close()
            diagnostics.close()
            if sys.exc_info()[0] is not None:
                snapshot = control() or {}
                sys.stderr.write(json.dumps({'provider_events': events,
                    'policy': snapshot.get('policy'), 'scheduler': snapshot.get('scheduler'),
                    'nodes': [{k: n.get(k) for k in ('node_id','state','registration','operation_slots','partition_slots','drain')} for n in snapshot.get('nodes',[])],
                    'intents': [{k:i.get(k) for k in ('node_id','action','state','attempts','error_code')} for i in snapshot.get('intents',[])],
                    'worker_paths': [[p[0] for p in r['door'].requests[-12:]] for r in resources.values()]}) + '\n')
                sys.stderr.write((root / 'diagnostics').read_text()[-16384:])


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=Path,default=Path(__file__).resolve().parents[1]/'server/target/debug/nabedra-database-server')
    parser.add_argument('--skip-zero',action='store_true',help='Run the up/down provider contract without the explicit full-shutdown extension')
    args=parser.parse_args()
    run(args.binary.resolve(),skip_zero=args.skip_zero)
