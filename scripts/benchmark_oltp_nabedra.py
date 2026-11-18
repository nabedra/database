#!/usr/bin/env python3.11
"""Run the shared point-update workload against the production RF3 OLTP topology."""

import argparse
from decimal import Decimal
import hashlib
import http.client
from io import SEEK_END
import json
import math
from pathlib import Path
import re
import secrets
import ssl
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from benchmark_oltp_workload import TABLE, add_workload_args, run_workload, validate_args


ROOT = Path(__file__).resolve().parents[1]
DATABASE = 'test'
MIB = 1 << 20
TRACE_LOG_BYTES = 16 * MIB
NANO_CPUS = 1_000_000_000
WORKER_MEMORY = 2_150_000_000
API_CPU, API_MEMORY = '0.35', 550_000_000
DRIVER_CPU, DRIVER_MEMORY = '0.65', 900_000_000
WORKERS = ('w1', 'w2', 'w3')
DEFAULT_WORKER_CPUS = (NANO_CPUS,) * len(WORKERS)
STATUS_LEADER_HINT = re.compile(
    r'^(\S+)\s+WARN\s+nabedra_database_server::oltp_replication::hosted: '
    r'typed ReadIndex failed group=(\S+) kind=Status \{ bucket: 0 \} node=(\d+) '
    r'error=has to forward request to: Some\((\d+)\)',
    re.MULTILINE,
)
EXECUTION = {
    'max_workers_per_request': 1,
    'max_pending_requests': 128,
    'max_queue_bytes_per_replica': 8 * MIB,
    'max_retained_result_bytes_per_worker': 8 * MIB,
    'result_retention_ms': 600_000,
    'queue_replication': 'all_ready_workers',
    'queue_lifetime': 'live_cluster',
    'partition_size': '64 MiB',
}


def worker_cpu_quotas(value):
    quotas = [field.strip() for field in value.split(',')]
    if len(quotas) != len(WORKERS) or any(
        not re.fullmatch(r'(?:[0-9]+(?:\.[0-9]{1,9})?|\.[0-9]{1,9})', field) for field in quotas
    ):
        raise argparse.ArgumentTypeError('supply three decimal CPU quotas with up to nine fractional digits')
    nanocpus = tuple(int(Decimal(field) * NANO_CPUS) for field in quotas)
    if min(nanocpus) <= 0 or sum(nanocpus) != sum(DEFAULT_WORKER_CPUS):
        raise argparse.ArgumentTypeError('worker CPU quotas must be positive and sum to 3')
    return nanocpus


def uid():
    return str(uuid4())


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def resources(memory):
    return dict(max_threads=1, memory_limit_bytes=memory, task_memory_limit_bytes=128 * MIB,
                control_memory_reserve_bytes=96 * MIB)


def command(*args, timeout=30):
    completed = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if completed.returncode != 0:
        raise RuntimeError(f'{args[:3]} exited {completed.returncode}: {completed.stderr.strip()}')
    return completed.stdout.strip()


class Http:
    """One persistent, non-retrying connection to the production HTTP API."""

    def __init__(self, url, token, timeout, ca=None):
        target = urlsplit(url)
        if target.scheme == 'https':
            self.connection = http.client.HTTPSConnection(target.hostname, target.port, timeout=timeout, context=ca)
        else:
            self.connection = http.client.HTTPConnection(target.hostname, target.port, timeout=timeout)
        self.headers = {'Authorization': f'Bearer {token}', 'X-Nabedra-Database': DATABASE,
                        'Content-Type': 'application/json'}

    def exchange(self, path, body=None):
        payload = None if body is None else json.dumps(body, separators=(',', ':'), allow_nan=False).encode()
        self.sizes = {'request_body_bytes': len(payload) if payload else 0}
        try:
            self.connection.request('GET' if body is None else 'POST', path, payload, self.headers)
            response = self.connection.getresponse()
            raw = response.read(8 * MIB + 1)
            self.sizes['response_body_bytes'] = len(raw)
            if len(raw) > 8 * MIB:
                raise ValueError('HTTP response exceeds the operation body limit')
            if response.headers.get_content_type() != 'application/json':
                raise ValueError(f'HTTP {response.status} did not return JSON')
            return response.status, json.loads(raw)
        except (OSError, http.client.HTTPException, ValueError):
            self.close()
            raise

    def close(self):
        self.connection.close()


def require_data(status, response, request_id):
    if status != 200 or not isinstance(response, dict) or set(response) != {'request_id', 'data'} \
            or response['request_id'] != request_id:
        raise RuntimeError(f'expected a success envelope for {request_id}, got HTTP {status}: {response}')
    return response['data']


class Nabedra:
    def __init__(self, ca_file, auth, timeout, metadata):
        self.token, self.admin_token = auth['data_token'], auth['admin_token']
        self.timeout, self.metadata = timeout, metadata
        self.ca = ssl.create_default_context(cafile=str(ca_file))
        self.local = threading.local()
        self.connections = []
        self.lock = threading.Lock()
        self.table_id = None

    def client(self):
        client = getattr(self.local, 'client', None)
        if client is None:
            client = self.local.client = Http('https://api:8000', self.token, self.timeout, self.ca)
            with self.lock:
                self.connections.append(client)
        return client

    def operation(self, operation, payload):
        request_id = uid()
        response = self.client().exchange('/v1/operations', dict(request_id=request_id, operation=operation, payload=payload))
        return require_data(*response, request_id)

    def seed(self, rows):
        create = {'name': TABLE, 'schema': {'id': 'int64', 'value': 'int64'}, 'key': 'id', 'format': 'parquet'}
        descriptor = self.operation('table.create', create)
        self.table_id = descriptor['id']
        if descriptor != create | {'id': self.table_id}:
            raise RuntimeError(f'seed created an unexpected table descriptor: {descriptor}')
        for start in range(0, rows, 128):
            batch = [{'id': key, 'value': 0} for key in range(start, min(start + 128, rows))]
            count = self.operation('table.insert_many', {'table_id': self.table_id, 'rows': batch})
            if count != len(batch):
                raise RuntimeError(f'seed expected {len(batch)} inserted rows, got {count}')
        return {'descriptor': descriptor, 'data_placement': self.data_placement()}

    def data_placement(self):
        # The published genesis Data group IDs derive from the table and shard UUIDs.
        shard_bytes = hashlib.sha256(UUID(self.table_id).bytes + b'initial-shard').digest()[:16]
        group_bytes = hashlib.sha256(shard_bytes + b'data-group').digest()[:16]
        group = dict(cluster_id=self.metadata['cluster_id'], database=DATABASE, group_id=str(UUID(bytes=group_bytes)),
                     kind={'data': {'table_id': self.table_id, 'shard_id': str(UUID(bytes=shard_bytes))}},
                     group_format_version=1)
        client = Http('http://w1:8000', self.admin_token, self.timeout)
        try:
            request_id = uid()
            reply = client.exchange('/v1/cluster/operations', dict(request_id=request_id,
                operation='storage.replica.placement', payload={'group': group}))
            placement = require_data(*reply, request_id)
        finally:
            client.close()

        def members(voters):
            return {(voter['worker_id'], voter['pool_id']) for voter in voters}
        expected = self.metadata['placements']['control']['voters']
        if (placement['record']['identity'] != group or placement['record']['replica_count'] != len(WORKERS)
                or len(placement['members']) != len(WORKERS) or members(placement['members']) != members(expected)):
            raise RuntimeError(f'unexpected Data replica placement: {placement}')
        return placement

    def update(self, key, value, request_id):
        client = self.client()
        try:
            status, response = client.exchange('/v1/operations', {
                'request_id': request_id, 'operation': 'table.update',
                'payload': {'table_id': self.table_id, 'key': key, 'values': {'value': value}},
            })
        except (OSError, http.client.HTTPException) as error:
            return client.sizes | {'status': 'unresolved', 'transport_error': type(error).__name__, 'message': str(error)}
        except ValueError as error:
            return client.sizes | {'status': 'unresolved', 'protocol_error': str(error)}
        details = client.sizes | {'http_status': status, 'response': response}
        if status == 200:
            try:
                data = require_data(status, response, request_id)
            except RuntimeError:
                return details | {'status': 'unresolved', 'protocol_error': 'invalid success envelope'}
            if data is True:
                return details | {'status': 'succeeded', 'accepted': True}
            return details | {'status': 'failed', 'accepted': True, 'unexpected_result': data}
        if isinstance(response, dict) and set(response) == {'request_id', 'error'} \
                and response['request_id'] in (None, request_id) and isinstance(response['error'], dict):
            error = response['error']
            if error.get('outcome') == 'not_applied':
                reported = 'rejected' if status == 429 and error.get('code') == 'overloaded' else 'failed'
                return details | {'status': reported, 'outcome': 'not_applied'}
        return details | {'status': 'unresolved', 'outcome': 'unknown'}

    def reconcile(self, samples):
        # Look up each original identity at most once and leave timed outcomes intact.
        client = Http('https://api:8000', self.token, min(self.timeout, 0.5), self.ca)
        deadline = time.monotonic() + 3
        run_id = self.metadata['run_id']
        try:
            for sample in samples:
                if time.monotonic() >= deadline:
                    break
                request_id = sample['request_id']
                try:
                    status, receipt = client.exchange(f'/v1/requests/{run_id}/{request_id}')
                except (OSError, http.client.HTTPException, ValueError) as error:
                    sample['reconciliation'] = {'error': f'{type(error).__name__}: {error}'}
                    continue
                observed = {'http_status': status, 'reply': receipt, 'observed_ns': time.monotonic_ns()}
                sample['reconciliation'] = observed
                if status != 200 or not isinstance(receipt, dict):
                    continue
                if (receipt.get('request_id'), receipt.get('run_id')) != (request_id, run_id):
                    continue
                state, error = receipt.get('state'), receipt.get('error')
                if state == 'succeeded' and receipt.get('result') is True and error is None:
                    observed['resolution'] = 'succeeded'
                elif state == 'failed' and isinstance(error, dict) and error.get('outcome') == 'not_applied':
                    observed['resolution'] = 'not_applied'
        finally:
            client.close()

    def read(self, _keys):
        query = {'select': None, 'filter': None, 'order_by': None, 'limit': None}
        payload = {'table_id': self.table_id, 'query': query}
        rows = self.operation('query.collect', payload)
        count = self.operation('query.count', payload)
        values = {}
        for row in rows:
            if set(row) != {'id', 'value'} or type(row['id']) is not int or type(row['value']) is not int:
                raise RuntimeError(f'invalid snapshot row: {row}')
            if row['id'] in values:
                raise RuntimeError(f'duplicate snapshot key: {row["id"]}')
            values[row['id']] = row['value']
        if len(values) != count:
            raise RuntimeError(f'snapshot contains {len(values)} distinct keys, exact count is {count}')
        return values

    def stage_stats(self):
        cgroup = Path('/sys/fs/cgroup')
        driver = {name: (cgroup / name).read_text().strip() for name in
                  ('cpu.stat', 'io.stat', 'memory.current', 'memory.peak', 'memory.events') if (cgroup / name).is_file()}
        network = Path('/proc/net/dev')
        if network.is_file():
            driver['network'] = network.read_text()
        stats = {'driver_cgroup': driver}
        for worker in WORKERS:
            client = Http(f'http://{worker}:8000', self.token, min(self.timeout, 3))
            try:
                status, reply = client.exchange('/v1/runtime/stats')
                stats[worker] = reply if status == 200 else {'status': status, 'response': reply}
            except (OSError, http.client.HTTPException, ValueError) as error:
                stats[worker] = {'error': f'{type(error).__name__}: {error}'}
            finally:
                client.close()
        return stats

    def summarize_stage(self, before, after):
        for name in WORKERS:
            old = (before or {}).get(name, {}).get('accept_to_dispatch')
            new = (after or {}).get(name, {}).get('accept_to_dispatch')
            if not old or not new or old['worker'] != old['owner'] or new['worker'] != new['owner']:
                continue
            if old['view'] != new['view'] or old['upper_us'] != new['upper_us']:
                continue
            buckets = [end - start for start, end in zip(old['bucket_counts'], new['bucket_counts'], strict=True)]
            if min(buckets) < 0:
                continue
            bounds = new['upper_us'] + [None]
            count = sum(buckets)
            percentiles = {}
            for percentile in (50, 90, 95, 99):
                rank = math.ceil(count * percentile / 100)
                for bound, size in zip(bounds, buckets, strict=True):
                    rank -= size
                    if rank <= 0:
                        percentiles[f'p{percentile}'] = bound if count else None
                        break
            return {'source': name, 'accept_to_first_dispatch': {'count': count, 'bucket_counts': buckets,
                    'bucket_upper_us': bounds, 'percentile_upper_us': percentiles,
                    'waiting_before': old['waiting'], 'waiting_after': new['waiting'],
                    'discarded_before_dispatch': new['discarded_before_dispatch'] - old['discarded_before_dispatch']}}
        return {'accept_to_first_dispatch': None}

    def close(self):
        for client in self.connections:
            client.close()


class Cluster:
    def __init__(self, args, directory):
        self.args, self.directory = args, directory
        self.worker_nano_cpus = dict(zip(WORKERS, args.worker_cpus, strict=True))
        self.prefix = 'nabedra-oltp-bench-' + uuid4().hex[:10]
        self.cluster_id, self.run_id, self.bootstrap_id = uid(), uid(), uid()
        self.containers, self.volumes = [], []
        self.auth = {role + '_token': secrets.token_urlsafe(32) for role in ('admin', 'data', 'enrollment')}
        self.network_created = False
        self.binary = '/fixture-nabedra-db' if args.binary else '/usr/local/bin/nabedra-db'
        self.key_spec = {'rsa': 'rsa:2048', 'ed25519': 'ed25519'}[args.tls_key_type]
        image, = json.loads(command('docker', 'image', 'inspect', args.image))
        self.image = image['Id']
        self.metadata = {'engine': 'nabedra', 'cluster_id': self.cluster_id, 'run_id': self.run_id,
                         'replication_factor': 3, 'queue_replication': 'all_ready_workers',
                         'consistency': 'Nabedra production OLTP', 'protocol': 'synchronous HTTPS /v1/operations',
                         'image': self.image, 'image_os': image['Os'], 'image_architecture': image['Architecture'],
                         'contract_references': ['O24', 'Q01', 'B02'], 'rpc_trace': args.rpc_trace,
                         'tls_key_type': args.tls_key_type}
        if args.binary:
            with args.binary.open('rb') as binary:
                self.metadata['binary_sha256'] = hashlib.file_digest(binary, 'sha256').hexdigest()

    def docker(self, *args, **kwargs):
        return command('docker', *args, **kwargs)

    def openssl(self, *args):
        return command('openssl', *args)

    def certificate(self, name):
        private_key, csr, certificate, extension = (self.directory / f'{name}.{suffix}'
                                                      for suffix in ('key', 'csr', 'pem', 'ext'))
        extension.write_text(f'subjectAltName=DNS:{name},DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n')
        self.openssl('req', '-new', '-newkey', self.key_spec, '-nodes', '-keyout', private_key, '-out', csr,
                     '-subj', f'/CN={name}')
        self.openssl('x509', '-req', '-in', csr, '-CA', self.directory / 'ca.pem',
                     '-CAkey', self.directory / 'ca.key', '-set_serial', str(uuid4().int),
                     '-out', certificate, '-days', '1', '-extfile', extension)
        digest = hashlib.sha256(ssl.PEM_cert_to_DER_cert(certificate.read_text())).hexdigest()
        return dict(worker_id=uid(), incarnation=uid(), enrollment_revision=1, certificate_sha256=digest,
                    endpoint=f'{name}:9000', server_name=name)

    def launch(self, alias, cpu, memory, entry, arguments, *, options=()):
        name = f'{self.prefix}-{alias}'
        self.containers.append(name)
        mounts = ['--mount', f'type=bind,source={self.directory},target=/fixture,readonly',
                  '--mount', f'type=bind,source={ROOT},target=/source,readonly']
        if self.args.binary:
            mounts += ['--mount', f'type=bind,source={self.args.binary},target={self.binary},readonly']
        log_options = ['--log-opt', 'max-size=4m']
        if self.args.rpc_trace and alias != 'driver':
            log_options = ['--log-driver', 'json-file', '--log-opt', 'max-size=16m', '--log-opt', 'max-file=1',
                           '-e', 'NABEDRA_OLTP_RPC_TRACE=1']
        self.docker('run', '-d', '--pull=never', '--name', name, '--network', self.prefix, '--network-alias', alias,
                    '--cpus', cpu, '--memory', str(memory), '--memory-swap', str(memory), '--pids-limit', '96',
                    *log_options, '--user', '0:0', '-e', 'PYTHONDONTWRITEBYTECODE=1',
                    '-e', 'PYTHONPATH=/source/orm/python/src', *mounts, *options, '--entrypoint', entry,
                    self.image, *arguments)
        return name

    def host_url(self, alias, scheme='http'):
        alias_name = f'{self.prefix}-{alias}'
        address = self.docker('port', alias_name, '8000/tcp')
        host, port = address.rsplit(':', 1)
        if host != '127.0.0.1':
            raise RuntimeError(f'expected a loopback port for {alias}, got {address}')
        return f'{scheme}://localhost:{port}'

    def worker_ready(self, urls, ca=None, deadline_seconds=110):
        clients = {name: Http(url, self.auth['data_token'], 2, ca) for name, url in urls.items()}
        pending, ready = set(urls), {}
        deadline = time.monotonic() + deadline_seconds
        next_state_check = time.monotonic() + 2
        try:
            while pending and time.monotonic() < deadline:
                for name in sorted(pending):
                    try:
                        status, reply = clients[name].exchange('/health/ready')
                        ready[name] = reply
                        expected = {'engine': 'oltp', 'run_id': self.run_id, 'ready_workers': len(WORKERS)}
                        if name == 'api':
                            expected['topology'] = 'api-gateway'
                        else:
                            expected['replication_factor'] = len(WORKERS)
                        if status == 200 and all(reply.get(field) == value for field, value in expected.items()):
                            pending.remove(name)
                    except (OSError, http.client.HTTPException, ValueError) as error:
                        ready[name] = str(error)
                now = time.monotonic()
                if pending and now >= next_state_check:
                    next_state_check = now + 2
                    for name in sorted(pending):
                        state = json.loads(self.docker('inspect', '--format', '{{json .State}}', f'{self.prefix}-{name}'))
                        if not state['Running']:
                            raise RuntimeError(f'{name} exited during startup with code {state["ExitCode"]}')
                if pending:
                    time.sleep(0.25)
            if pending:
                raise RuntimeError(f'topology did not become ready ({sorted(pending)}): {ready}')
            return ready
        finally:
            for client in clients.values():
                client.close()

    def start(self):
        self.openssl('req', '-x509', '-newkey', self.key_spec, '-nodes', '-days', '1',
                     '-keyout', self.directory / 'ca.key', '-out', self.directory / 'ca.pem',
                     '-subj', '/CN=Nabedra OLTP benchmark')
        write_json(self.directory / 'auth.json', self.auth)
        (self.directory / 'auth.json').chmod(0o600)
        self.docker('network', 'create', '--driver', 'bridge', self.prefix)
        self.network_created = True
        enrollment = {name: self.certificate(name) for name in WORKERS}
        pool = {name: uid() for name in WORKERS}
        placements = {}
        for offset, (label, kind) in enumerate((('control', 'control'), ('timestamps', 'timestamps'),
                                                ('status', {'status': {'bucket': 0}}), ('catalog', 'catalog'))):
            order = WORKERS[offset % len(WORKERS):] + WORKERS[:offset % len(WORKERS)]
            if label == 'status' and self.args.status_seed_worker:
                seed = self.args.status_seed_worker
                order = (seed, *(worker for worker in order if worker != seed))
                self.metadata['status_seed_placement'] = {
                    'seed_worker': seed, 'workers_by_node_id': dict(enumerate(order, start=1)),
                }
            placements[label] = dict(identity=dict(cluster_id=self.cluster_id, database=DATABASE, group_id=uid(),
                                kind=kind, group_format_version=1), voters=[dict(node_id=index,
                                worker_id=enrollment[name]['worker_id'], pool_id=pool[name])
                                for index, name in enumerate(order, start=1)])
        if self.args.status_seed_worker:
            self.metadata['status_seed_placement']['group_id'] = placements['status']['identity']['group_id']
        enrollments = sorted(enrollment.values(), key=lambda record: record['worker_id'])
        operator_pins = sorted(record['certificate_sha256'] for record in enrollments)
        for index, name in enumerate(WORKERS, start=1):
            volume, marker = f'{self.prefix}-{name}', uid()
            self.docker('volume', 'create', '--label', f'io.nabedra.cluster={self.cluster_id}', volume)
            self.volumes.append(volume)
            inspected, = json.loads(self.docker('volume', 'inspect', volume))
            if inspected['Name'] != volume or inspected['Labels'].get('io.nabedra.cluster') != self.cluster_id:
                raise RuntimeError(f'Docker did not provision the requested worker volume: {volume}')
            write_json(self.directory / f'{name}-volume.json', dict(format_version=1, volume_name=volume, volume_marker=marker))
            worker = enrollment[name]
            identity = {field: worker[field] for field in ('worker_id', 'incarnation', 'enrollment_revision', 'certificate_sha256')}
            node = dict(format=1, directory='/data', pool_id=pool[name], scheduler_node_id=index,
                        enrollment=worker, bind='0.0.0.0:9000', certificate=f'/tmp/config/{name}.pem',
                        private_key=f'/tmp/config/{name}.key', authority_ca='/tmp/config/ca.pem',
                        bootstrap_id=self.bootstrap_id, initial_run=self.run_id, required_voters=len(WORKERS),
                        placements=placements, control_enrollments=enrollments, operator_pins=operator_pins,
                        mount=dict(profile={'docker': {'volume_marker': marker}}, worker=identity, generation=1,
                                   identity=dict(engine='oltp', pool_format_version=1, cluster_id=self.cluster_id,
                                                 worker_id=worker['worker_id'], pool_id=pool[name]),
                                   mount_path='/data', provider_volume_id='docker-volume:' + volume, filesystem_uuid=''))
            write_json(self.directory / f'{name}.json', dict(version=1, bind='0.0.0.0:8000',
                       auth_file='/tmp/config/auth.json', node=node, resources=resources(WORKER_MEMORY), execution=EXECUTION))
            script = '''set -eu
umask 077
mkdir /tmp/config
cp /fixture/auth.json /fixture/ca.pem "/fixture/$1.json" "/fixture/$1.pem" "/fixture/$1.key" /tmp/config/
cp "/fixture/$1-volume.json" /data/.nabedra-docker-volume.json
exec "$2" worker --bootstrap "/tmp/config/$1.json"
'''
            cpu = format(Decimal(self.worker_nano_cpus[name]).scaleb(-9).normalize(), 'f')
            self.launch(name, cpu, WORKER_MEMORY, 'sh', ['-c', script, 'worker', name, self.binary],
                        options=('-p', '127.0.0.1::8000', '--mount', f'type=volume,source={volume},target=/data'))
        worker_urls = {name: self.host_url(name) for name in WORKERS}
        print('Waiting for three OLTP workers', flush=True)
        self.metadata['ready'] = self.worker_ready(worker_urls)
        control = Http(worker_urls['w1'], self.auth['admin_token'], 15)
        try:
            def admin(operation, payload):
                request_id = uid()
                response = control.exchange('/v1/cluster/operations', dict(request_id=request_id, operation=operation, payload=payload))
                return require_data(*response, request_id)

            connection = admin('cluster.connection', {})
            if len(connection['control_peers']) != len(WORKERS) or connection['cluster_id'] != self.cluster_id:
                raise RuntimeError(f'worker returned inconsistent cluster connection metadata: {connection}')
            api_enrollment = {'database': DATABASE, 'enrollment': self.certificate('api')}
            admin('api.enroll', {'expected': None, 'next': api_enrollment})
        finally:
            control.close()
        write_json(self.directory / 'api.json', dict(version=1, engine='oltp', bind='0.0.0.0:8000', database=DATABASE,
                   auth_file='/tmp/config/auth.json', tls=dict(cert_file='/tmp/config/api.pem', key_file='/tmp/config/api.key'),
                   topology=dict(mode='remote', cluster_id=self.cluster_id, control_group_id=connection['control_group_id'],
                                 control_peers=connection['control_peers'], enrollment=api_enrollment,
                                 worker_tls=dict(cert_file='/tmp/config/api.pem', key_file='/tmp/config/api.key',
                                                 ca_file='/tmp/config/ca.pem')),
                   worker=dict(resources=resources(API_MEMORY), execution=EXECUTION, queue_size=128)))
        script = '''set -eu
umask 077
mkdir /tmp/config
cp /fixture/auth.json /fixture/ca.pem /fixture/api.json /fixture/api.pem /fixture/api.key /tmp/config/
export NABEDRA_SERVER_BINARY="$1"
if [ "${NABEDRA_OLTP_RPC_TRACE:-}" = 1 ]; then
    # The CLI drains its child's stderr. Send benchmark-only native traces to Docker.
    export NABEDRA_BENCHMARK_BINARY="$1"
    cat > /tmp/config/trace-server <<'TRACE_WRAPPER'
#!/bin/sh
exec "$NABEDRA_BENCHMARK_BINARY" "$@" 2>/proc/1/fd/2
TRACE_WRAPPER
    chmod 700 /tmp/config/trace-server
    export NABEDRA_SERVER_BINARY=/tmp/config/trace-server
fi
exec python3 -m nabedra.cli database serve --config /tmp/config/api.json
'''
        self.launch('api', API_CPU, API_MEMORY, 'sh', ['-c', script, 'api', self.binary], options=('-p', '127.0.0.1::8000'))
        print('Waiting for the remote OLTP API', flush=True)
        self.metadata['ready'].update(self.worker_ready({'api': self.host_url('api', 'https')},
                                     ca=ssl.create_default_context(cafile=str(self.directory / 'ca.pem')), deadline_seconds=40))
        worker = f'{self.prefix}-w1'
        version = self.docker('exec', worker, self.binary, '--version')
        if 'oltp' not in version.split():
            raise RuntimeError(f'expected an OLTP binary, got {version}')
        if 'binary_sha256' not in self.metadata:
            self.metadata['binary_sha256'] = self.docker('exec', worker, 'sha256sum', self.binary).split()[0]
        self.metadata.update(server_version=version, placements=placements, execution=EXECUTION)

    def run(self):
        args = self.args
        sources = (Path(__file__), Path(__file__).with_name('benchmark_oltp_workload.py'))
        self.metadata['benchmark_source_sha256'] = {source.name: hashlib.sha256(source.read_bytes()).hexdigest()
                                                    for source in sources}
        self.metadata['workspace_head'] = command('git', '-C', ROOT, 'rev-parse', 'HEAD')
        manifest = self.docker('info', '--format', '{{json .}}')
        host = json.loads(manifest)
        self.metadata['docker_environment'] = {key: host.get(key) for key in (
            'ServerVersion', 'OperatingSystem', 'Architecture', 'KernelVersion', 'NCPU', 'MemTotal', 'CgroupVersion')}
        self.metadata['allocations'] = {'worker_bytes': WORKER_MEMORY,
            'api_cpu': float(API_CPU), 'api_bytes': API_MEMORY, 'driver_cpu': float(DRIVER_CPU),
            'driver_bytes': DRIVER_MEMORY, 'total_cpu': 4, 'total_bytes': 7_900_000_000}
        allocations = self.metadata['allocations']
        if args.worker_cpus == DEFAULT_WORKER_CPUS:
            allocations['worker_cpu'] = 1.0
        else:
            allocations['worker_cpu_by_role'] = {worker: quota / NANO_CPUS
                                                for worker, quota in self.worker_nano_cpus.items()}
        write_json(self.directory / 'metadata.json', self.metadata)
        settings = ['--rates', ','.join(map(str, args.rates))]
        for field in ('seconds', 'warmup_seconds', 'repetitions', 'clients', 'rows', 'seed', 'timeout'):
            settings += ['--' + field.replace('_', '-'), str(getattr(args, field))]
        driver = self.launch('driver', DRIVER_CPU, DRIVER_MEMORY, 'python3',
            ['/source/scripts/benchmark_oltp_nabedra.py', '--driver', '--output', '/results', *settings])
        print(f'Running the OLTP workload in {driver}', flush=True)
        state = int(self.docker('wait', driver, timeout=len(args.rates) * (args.warmup_seconds
                    + args.repetitions * args.seconds + (args.repetitions + 1) * (args.timeout + 30)) + 120))
        self.docker('cp', f'{driver}:/results/.', str(args.output), timeout=120)
        return state

    def record_status_leader_hints(self):
        placement = self.metadata['status_seed_placement']
        workers = placement['workers_by_node_id']
        observations = {}
        for role in WORKERS:
            log_path = self.args.output / f'{self.prefix}-{role}.log'
            if not log_path.is_file():
                continue
            for timestamp, group_id, observer_id, leader_id in STATUS_LEADER_HINT.findall(
                    log_path.read_text(errors='replace')):
                leader = workers.get(int(leader_id))
                if group_id != placement['group_id'] or workers.get(int(observer_id)) != role or leader is None:
                    continue
                observation = observations.setdefault((role, leader), {
                    'observer': role, 'hinted_leader': leader, 'count': 0,
                    'first_timestamp': timestamp, 'last_timestamp': timestamp,
                })
                observation['count'] += 1
                observation['first_timestamp'] = min(observation['first_timestamp'], timestamp)
                observation['last_timestamp'] = max(observation['last_timestamp'], timestamp)
        placement['observed_follower_hints'] = list(observations.values())

    def finish(self):
        errors, runtime = [], {}
        for name in reversed(self.containers):
            try:
                item, = json.loads(self.docker('inspect', name))
                limits = item['HostConfig']
                runtime[name] = dict(state=item['State'], nano_cpus=limits['NanoCpus'],
                                     memory=limits['Memory'], memory_swap=limits['MemorySwap'])
                if item['State']['OOMKilled']:
                    errors.append(f'{name} was OOM-killed')
                if item['State']['Running']:
                    runtime[name]['cgroup_final'] = self.docker('exec', name, 'sh', '-c',
                        'for f in /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/io.stat '
                        '/sys/fs/cgroup/memory.current '
                        '/sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.events '
                        '/proc/net/dev /proc/net/snmp /proc/net/netstat; do '
                        'if [ -f "$f" ]; then echo "$f"; cat "$f"; fi; done')
                log_path = self.args.output / f'{name}.log'
                if self.args.rpc_trace and not name.endswith('-driver'):
                    with log_path.open('w+b') as log:
                        subprocess.run(['docker', 'logs', name], stdout=log, stderr=subprocess.STDOUT,
                                       timeout=20, check=True)
                        if log.tell() > TRACE_LOG_BYTES:
                            log.seek(-TRACE_LOG_BYTES, SEEK_END)
                            log.readline()
                            tail = log.read()
                            log.seek(0)
                            log.write(tail)
                            log.truncate()
                else:
                    logs = subprocess.run(['docker', 'logs', '--tail', '1000', name],
                                          capture_output=True, timeout=10, check=True)
                    log_path.write_bytes(logs.stdout + logs.stderr)
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
                errors.append(str(error))
            try:
                self.docker('rm', '-f', name)
            except (RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
        for volume in self.volumes:
            try:
                self.docker('volume', 'rm', volume)
            except (RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
        if self.network_created:
            try:
                self.docker('network', 'rm', self.prefix)
            except (RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
        limits = {worker: (cpu, WORKER_MEMORY) for worker, cpu in self.worker_nano_cpus.items()}
        limits.update(api=(350_000_000, API_MEMORY), driver=(650_000_000, DRIVER_MEMORY))
        cap_errors = []
        for name, record in runtime.items():
            role = name.rsplit('-', 1)[-1]
            cpu, memory = limits[role]
            if (record['nano_cpus'], record['memory'], record['memory_swap']) != (cpu, memory, memory):
                cap_errors.append(f'{name} did not have the requested CPU, memory and swap caps')
        total_cpu = sum(item['nano_cpus'] for item in runtime.values())
        total_memory = sum(item['memory'] for item in runtime.values())
        budget_verified = (not cap_errors and len(runtime) == len(WORKERS) + 2
                           and total_cpu <= 4_000_000_000 and total_memory <= 8_000_000_000)
        errors.extend(cap_errors)
        if not budget_verified:
            errors.append('could not verify the complete cluster and driver resource budget')
        if 'status_seed_placement' in self.metadata:
            try:
                self.record_status_leader_hints()
            except OSError as error:
                errors.append(f'could not read Status leader hints: {error}')
        write_json(self.args.output / 'container-runtime.json', {'containers': runtime, 'errors': errors,
                   'total_nano_cpus': total_cpu, 'total_memory_bytes': total_memory, 'budget_verified': budget_verified})
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', help='locally available Docker image containing Python 3.11 and nabedra-db')
    parser.add_argument('--binary', type=Path, help='use this release Linux binary with the image instead')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--worker-cpus', type=worker_cpu_quotas, default=DEFAULT_WORKER_CPUS,
                        help='comma-separated CPU quotas for w1, w2 and w3 (default: 1,1,1)')
    parser.add_argument('--status-seed-worker', choices=WORKERS,
                        help='select the first voter that initializes the fresh RF3 Status group (default: w3)')
    parser.add_argument('--rpc-trace', action='store_true', help='capture bounded native RPC logs for a diagnostic run')
    parser.add_argument('--tls-key-type', choices=('rsa', 'ed25519'), default='rsa',
                        help='key type for fixture certificates, including the CA (default: rsa)')
    parser.add_argument('--driver', action='store_true', help=argparse.SUPPRESS)
    add_workload_args(parser)
    args = parser.parse_args()
    try:
        validate_args(args)
    except ValueError as error:
        parser.error(str(error))
    if args.driver:
        auth = json.loads(Path('/fixture/auth.json').read_text())
        metadata = json.loads(Path('/fixture/metadata.json').read_text())
        if metadata['rpc_trace']:
            before = time.monotonic_ns()
            unix_ns = time.time_ns()
            after = time.monotonic_ns()
            metadata['trace_clock'] = {'unix_ns': unix_ns, 'monotonic_ns': (before + after) // 2,
                                       'sample_interval_ns': after - before}
        adapter = Nabedra(Path('/fixture/ca.pem'), auth, args.timeout, metadata)
        results = run_workload(args, adapter, args.output, metadata)
        return int(not results['correctness_verified'])
    if not args.image:
        parser.error('--image is required')
    if args.binary:
        args.binary = args.binary.resolve(strict=True)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('--output must be empty')
    with tempfile.TemporaryDirectory(prefix='.nabedra-oltp-benchmark-', dir=Path.home()) as directory:
        cluster = Cluster(args, Path(directory))
        try:
            cluster.start()
            result = cluster.run()
        finally:
            errors = cluster.finish()
            write_json(args.output / 'cluster-metadata.json', cluster.metadata)
    return result or int(bool(errors))


if __name__ == '__main__':
    raise SystemExit(main())
