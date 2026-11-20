"""Host-process OLTP topologies for the direct point path scenarios (T9).

`Rf3Cluster` runs three registered workers (`nabedra-db worker --bootstrap`) and enrolled
API gateways (`nabedra database serve`, remote topology) as plain processes on loopback.
Workers talk mTLS to each other and to the gateways, exactly as in a deployment; only the
addresses are loopback and there is no Docker, so the fixture can SIGKILL, SIGSTOP and
cold-start individual processes. `Rf1Serve` runs `nabedra-db serve`.

A worker can instead run as the library test child (`--test-executable`), which honours
the test-only barrier and budget variables (`NABEDRA_OLTP_TEST_BARRIER_DIR`,
`NABEDRA_OLTP_TEST_DIRECT_MAX_IN_FLIGHT`).
"""
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import ssl
import subprocess
import sys
import time
from urllib.parse import urlsplit
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
DATABASE = 'test'
MIB = 1 << 20
WORKERS = ('w1', 'w2', 'w3')
CHILD = 'oltp_runtime::recovery_tests::actual_runtime_child'
IDENTITY_FIELDS = ('worker_id', 'incarnation', 'enrollment_revision', 'certificate_sha256')
EXECUTION = dict(max_workers_per_request=1, max_pending_requests=128, max_queue_bytes_per_replica=8 * MIB,
                 max_retained_result_bytes_per_worker=8 * MIB, result_retention_ms=600_000,
                 queue_replication='all_ready_workers', queue_lifetime='live_cluster', partition_size='64 MiB')


def uid():
    return str(uuid4())


def write_json(path, value, mode=None):
    pending = path.with_suffix(path.suffix + '.tmp')
    pending.write_text(json.dumps(value, indent=2) + '\n')
    if mode is not None:
        pending.chmod(mode)
    pending.replace(path)


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def resources(memory, threads=2):
    return dict(max_threads=threads, memory_limit_bytes=memory, task_memory_limit_bytes=512 * MIB,
                control_memory_reserve_bytes=128 * MIB)


def child_pids(pid):
    try:
        out = subprocess.run(['pgrep', '-P', str(pid)], capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    return [int(line) for line in out.split()]


class Http:
    """One persistent, non-retrying HTTP(S) connection. Returns (status, json)."""

    def __init__(self, url, token, timeout=30, ca=None):
        target = urlsplit(url)
        if target.scheme == 'https':
            context = ssl.create_default_context(cafile=str(ca))
            self.connection = http.client.HTTPSConnection(target.hostname, target.port, timeout=timeout,
                                                          context=context)
        else:
            self.connection = http.client.HTTPConnection(target.hostname, target.port, timeout=timeout)
        self.headers = {'Authorization': f'Bearer {token}', 'X-Nabedra-Database': DATABASE,
                        'Content-Type': 'application/json'}

    def exchange(self, path, body=None):
        payload = None if body is None else json.dumps(body, separators=(',', ':')).encode()
        try:
            self.connection.request('GET' if body is None else 'POST', path, payload, self.headers)
            response = self.connection.getresponse()
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
        except BaseException:
            self.connection.close()
            raise

    def close(self):
        self.connection.close()


def call(url, path, body=None, *, token, ca=None, timeout=30):
    client = Http(url, token, timeout, ca)
    try:
        return client.exchange(path, body)
    finally:
        client.close()


def operation(operation, payload, request_id=None):
    return dict(request_id=request_id or uid(), operation=operation, payload=payload)


class Process:
    def __init__(self, name, command, env, log):
        self.name = name
        self.log = open(log, 'ab', buffering=0)
        self.popen = subprocess.Popen(command, env=env, stdout=subprocess.PIPE if name.startswith('api') else self.log,
                                      stderr=self.log, start_new_session=True)
        self.paused = False

    @property
    def pid(self):
        return self.popen.pid

    def alive(self):
        return self.popen.poll() is None

    def signal_group(self, sig):
        try:
            os.killpg(self.popen.pid, sig)
        except ProcessLookupError:
            pass

    def pause(self):
        self.signal_group(signal.SIGSTOP)
        self.paused = True

    def resume(self):
        self.signal_group(signal.SIGCONT)
        self.paused = False

    def kill(self):
        if self.paused:
            self.resume()
        self.signal_group(signal.SIGKILL)
        self.popen.wait(timeout=20)

    def stop(self):
        if self.alive():
            if self.paused:
                self.resume()
            self.signal_group(signal.SIGTERM)
            try:
                self.popen.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.signal_group(signal.SIGKILL)
                self.popen.wait(timeout=10)
        if self.popen.stdout:
            self.popen.stdout.close()
        self.log.close()


def announcement(process, timeout=60):
    import select
    data = bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ready, _, _ = select.select([process.popen.stdout], [], [], 0.1)
        if ready:
            chunk = os.read(process.popen.stdout.fileno(), 4096)
            if not chunk:
                raise AssertionError(f'{process.name} exited before its announcement')
            data.extend(chunk)
            if b'\n' in data:
                return json.loads(bytes(data).split(b'\n', 1)[0])
        if not process.alive():
            raise AssertionError(f'{process.name} exited before its announcement')
    raise AssertionError(f'{process.name} announcement timeout')


class Rf3Cluster:
    def __init__(self, binary, root, *, test_executable=None, direct=None, api_direct=(True,),
                 worker_env=None, key_type='ed25519', worker_memory=2048 * MIB):
        self.binary = str(Path(binary).resolve())
        self.test_executable = str(Path(test_executable).resolve()) if test_executable else None
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.direct = dict(direct or {name: True for name in WORKERS})
        self.api_direct = tuple(api_direct)
        self.worker_env = dict(worker_env or {})
        self.key_spec = {'rsa': 'rsa:2048', 'ed25519': 'ed25519'}[key_type]
        self.worker_memory = worker_memory
        self.cluster_id, self.run_id, self.bootstrap_id = uid(), uid(), uid()
        self.auth = {role + '_token': secrets.token_urlsafe(32) for role in ('admin', 'data', 'enrollment')}
        self.workers, self.apis = {}, {}
        self.config, self.urls, self.api_urls = {}, {}, {}
        self.enrollments = {}
        self.ca = self.root / 'ca.pem'

    # Certificates -----------------------------------------------------------------------
    def openssl(self, *args):
        subprocess.run(['openssl', *map(str, args)], check=True, capture_output=True, timeout=60)

    def certificate(self, name, node_port=None):
        key, csr, pem, ext = (self.root / f'{name}.{suffix}' for suffix in ('key', 'csr', 'pem', 'ext'))
        ext.write_text('subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n')
        self.openssl('req', '-new', '-newkey', self.key_spec, '-nodes', '-keyout', key, '-out', csr,
                     '-subj', f'/CN={name}')
        self.openssl('x509', '-req', '-in', csr, '-CA', self.ca, '-CAkey', self.root / 'ca.key',
                     '-set_serial', str(uuid4().int), '-out', pem, '-days', '1', '-extfile', ext)
        key.chmod(0o600)
        digest = hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem.read_text())).hexdigest()
        return dict(worker_id=uid(), incarnation=uid(), enrollment_revision=1, certificate_sha256=digest,
                    endpoint=f'127.0.0.1:{node_port or free_port()}', server_name='localhost')

    # Workers ----------------------------------------------------------------------------
    def start(self):
        self.openssl('req', '-x509', '-newkey', self.key_spec, '-nodes', '-days', '1', '-keyout',
                     self.root / 'ca.key', '-out', self.ca, '-subj', '/CN=Nabedra T9 direct fixture')
        write_json(self.root / 'auth.json', self.auth, 0o600)
        node_ports = {name: free_port() for name in WORKERS}
        enrollment = {name: self.certificate(name, node_ports[name]) for name in WORKERS}
        pool = {name: uid() for name in WORKERS}
        placements = {}
        for offset, (label, kind) in enumerate((('control', 'control'), ('timestamps', 'timestamps'),
                                                ('status', {'status': {'bucket': 0}}), ('catalog', 'catalog'))):
            order = WORKERS[offset:] + WORKERS[:offset]
            placements[label] = dict(
                identity=dict(cluster_id=self.cluster_id, database=DATABASE, group_id=uid(), kind=kind,
                              group_format_version=1),
                voters=[dict(node_id=index, worker_id=enrollment[name]['worker_id'], pool_id=pool[name])
                        for index, name in enumerate(order, start=1)])
        peers = sorted(enrollment.values(), key=lambda record: record['worker_id'])
        pins = sorted(record['certificate_sha256'] for record in peers)
        for index, name in enumerate(WORKERS, start=1):
            (self.root / name).mkdir(exist_ok=True)
            node = dict(format=1, directory=str(self.root / name / 'data'), pool_id=pool[name],
                        scheduler_node_id=index, enrollment=enrollment[name],
                        bind=f'127.0.0.1:{node_ports[name]}', certificate=str(self.root / f'{name}.pem'),
                        private_key=str(self.root / f'{name}.key'), authority_ca=str(self.ca),
                        bootstrap_id=self.bootstrap_id, initial_run=self.run_id, required_voters=len(WORKERS),
                        placements=placements, control_enrollments=peers, operator_pins=pins)
            self.config[name] = dict(version=1, bind=f'127.0.0.1:{free_port()}', auth_file=str(self.root / 'auth.json'),
                                     node=node, resources=resources(self.worker_memory),
                                     execution=EXECUTION | dict(direct_oltp=self.direct[name]))
            self.enrollments[name] = enrollment[name]
        for name in WORKERS:
            self.launch_worker(name)
        self.wait_workers()
        connection = self.admin('cluster.connection', {})
        assert connection['cluster_id'] == self.cluster_id and len(connection['control_peers']) == 3, connection
        self.connection = connection
        for index, direct in enumerate(self.api_direct, start=1):
            self.start_api(f'api{index}', direct)
        return self

    def launch_worker(self, name):
        config = self.config[name]
        path = self.root / f'{name}.json'
        write_json(path, config)
        self.urls[name] = 'http://' + config['bind']
        env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'NABEDRA_S3_'))}
        env.update(self.worker_env)
        if self.test_executable:
            barriers = self.root / 'barriers' / name
            barriers.mkdir(parents=True, exist_ok=True)
            env.update(NABEDRA_OLTP_TEST_REGISTERED_CONFIG=str(path), NABEDRA_OLTP_TEST_BARRIER_DIR=str(barriers))
            command = [self.test_executable, '--exact', CHILD, '--ignored', '--nocapture']
        else:
            command = [self.binary, 'worker', '--bootstrap', str(path)]
        self.workers[name] = Process(name, command, env, self.root / f'{name}.log')

    def barrier_dir(self, name):
        return self.root / 'barriers' / name

    def wait_workers(self, names=WORKERS, deadline_seconds=180):
        pending = set(names)
        deadline = time.monotonic() + deadline_seconds
        last = {}
        while pending:
            for name in sorted(pending):
                assert self.workers[name].alive(), f'{name} exited during startup:\n{self.tail(name)}'
                try:
                    status, ready = call(self.urls[name], '/health/ready', token=self.auth['data_token'], timeout=3)
                    last[name] = ready
                    if status == 200 and ready.get('replication_factor') == 3 and ready.get('ready_workers') == 3:
                        pending.discard(name)
                except (OSError, http.client.HTTPException, ValueError) as error:
                    last[name] = repr(error)
            if pending:
                assert time.monotonic() < deadline, f'workers not ready: {last}'
                time.sleep(0.25)
        return last

    def admin(self, name, payload, worker=None):
        targets = [worker] if worker else [w for w in WORKERS if self.workers[w].alive() and not self.workers[w].paused]
        error = None
        for target in targets:
            request = operation(name, payload)
            try:
                status, reply = call(self.urls[target], '/v1/cluster/operations', request,
                                     token=self.auth['admin_token'])
            except (OSError, http.client.HTTPException) as failure:
                error = failure
                continue
            if status == 200:
                return reply['data']
            error = (target, status, reply)
        raise AssertionError(f'admin {name} failed: {error}')

    # Gateways ---------------------------------------------------------------------------
    def start_api(self, name, direct=True):
        if name not in self.apis or not hasattr(self, f'_{name}_config'):
            record = dict(database=DATABASE, enrollment=self.certificate(name))
            self.admin('api.enroll', {'expected': None, 'next': record})
            port = free_port()
            config = dict(version=1, engine='oltp', bind=f'127.0.0.1:{port}', database=DATABASE,
                          auth_file=str(self.root / 'auth.json'),
                          tls=dict(cert_file=str(self.root / f'{name}.pem'), key_file=str(self.root / f'{name}.key')),
                          topology=dict(mode='remote', cluster_id=self.cluster_id,
                                        control_group_id=self.connection['control_group_id'],
                                        control_peers=self.connection['control_peers'], enrollment=record,
                                        direct_oltp=direct,
                                        worker_tls=dict(cert_file=str(self.root / f'{name}.pem'),
                                                        key_file=str(self.root / f'{name}.key'), ca_file=str(self.ca))),
                          worker=dict(resources=resources(2048 * MIB, 2), execution=EXECUTION, queue_size=128))
            setattr(self, f'_{name}_config', config)
        config = getattr(self, f'_{name}_config')
        config['topology']['control_peers'] = self.connection['control_peers']
        path = self.root / f'{name}.json'
        write_json(path, config)
        env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'NABEDRA_S3_'))}
        env.update(PYTHONPATH=str(ROOT / 'orm/python/src'), PYTHONDONTWRITEBYTECODE='1', NABEDRA_SERVER_BINARY=self.binary)
        process = Process(name, [sys.executable, '-m', 'nabedra.cli', 'database', 'serve', '--config', str(path)],
                          env, self.root / f'{name}.log')
        self.apis[name] = process
        message = announcement(process)
        assert message.get('event') == 'nabedra.serve.launched', message
        port = urlsplit(message['bound_url']).port
        self.api_urls[name] = f'https://localhost:{port}'
        deadline = time.monotonic() + 90
        while True:
            assert process.alive(), f'{name} exited:\n{self.tail(name)}'
            try:
                status, ready = call(self.api_urls[name], '/health/ready', token=self.auth['data_token'], ca=self.ca,
                                     timeout=3)
                if status == 200 and ready.get('topology') == 'api-gateway':
                    break
            except (OSError, http.client.HTTPException, ValueError):
                pass
            assert time.monotonic() < deadline, f'{name} never became ready'
            time.sleep(0.25)
        return self.api_urls[name]

    def kill_api(self, name):
        """SIGKILL the gateway's native process (the CLI supervisor then exits)."""
        process = self.apis[name]
        for pid in child_pids(process.pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.kill()

    # Observation ------------------------------------------------------------------------
    def stats(self, name):
        status, stats = call(self.urls[name], '/v1/runtime/stats', token=self.auth['data_token'], timeout=10)
        assert status == 200, (name, status, stats)
        return stats

    def all_stats(self, names=None):
        return {name: self.stats(name) for name in (names or self.live())}

    def live(self):
        return [name for name in WORKERS if name in self.workers and self.workers[name].alive()
                and not self.workers[name].paused]

    def wait_direct_active(self, deadline_seconds=120):
        deadline = time.monotonic() + deadline_seconds
        while True:
            capability = {name: (self.stats(name).get('direct_capability') or {}) for name in self.live()}
            if all(value.get('active') for value in capability.values()):
                return capability
            assert time.monotonic() < deadline, f'data.direct.v1 never activated: {capability}'
            time.sleep(0.5)

    def tail(self, name, size=6000):
        path = self.root / f'{name}.log'
        try:
            data = path.read_bytes()
        except OSError:
            return ''
        return data[-size:].decode(errors='replace')

    # Cold start (D09) -------------------------------------------------------------------
    def kill_all(self):
        for process in [*self.apis.values(), *self.workers.values()]:
            if process.alive():
                if process.name.startswith('api'):
                    self.kill_api(process.name)
                else:
                    process.kill()

    def cold_start(self):
        """Restart every worker as a fresh incarnation after a complete stop (R04/D09)."""
        assert not any(process.alive() for process in self.workers.values())
        profile = dict(previous_run=self.run_id, next_run=uid(), transition_id=uid())
        nexts = {}
        for name in WORKERS:
            previous = self.config[name]['node']['enrollment']
            port = int(previous['endpoint'].rsplit(':', 1)[1])
            fresh = self.certificate(name, port) | dict(worker_id=previous['worker_id'],
                                                        enrollment_revision=previous['enrollment_revision'] + 1)
            nexts[name] = fresh
        peers = sorted(nexts.values(), key=lambda record: record['worker_id'])
        pins = sorted(record['certificate_sha256'] for record in peers)
        for name in WORKERS:
            node = self.config[name]['node']
            node.update(cold=profile, enrollment=nexts[name], control_enrollments=peers, operator_pins=pins)
            self.enrollments[name] = nexts[name]
        self.run_id = profile['next_run']
        for name in WORKERS:
            self.launch_worker(name)
        self.wait_workers()
        self.connection = self.admin('cluster.connection', {})
        return profile

    def close(self):
        for process in [*self.apis.values(), *self.workers.values()]:
            try:
                if process.name.startswith('api') and process.alive():
                    self.kill_api(process.name)
                process.stop()
            except Exception as error:  # cleanup must reach every process
                print(f'cleanup {process.name}: {error!r}', file=sys.stderr)


class Rf1Serve:
    """`nabedra-db serve` (RF1, the U01 local topology)."""

    def __init__(self, binary, root, *, direct=True, test_executable=None):
        self.binary = str(Path(binary).resolve())
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.direct = direct
        self.token = secrets.token_urlsafe(32)
        self.process = None

    def start(self, cold_start=False):
        port = free_port()
        (self.root / 'data').mkdir(exist_ok=True)
        execution = self.root / 'execution.json'
        write_json(execution, EXECUTION | dict(direct_oltp=self.direct))
        env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'NABEDRA_S3_'))}
        env['NABEDRA_TOKEN'] = self.token
        command = [self.binary, 'serve', '--data-dir', str(self.root / 'data'), '--database', DATABASE,
                   '--host', '127.0.0.1', '--port', str(port), '--workers', '4', '--execution-config', str(execution)]
        if cold_start:
            command.append('--cold-start')
        self.process = Process('serve', command, env, self.root / 'serve.log')
        self.url = f'http://127.0.0.1:{port}'
        deadline = time.monotonic() + 90
        while True:
            assert self.process.alive(), (self.root / 'serve.log').read_text(errors='replace')[-4000:]
            try:
                status, ready = call(self.url, '/health/ready', token=self.token, timeout=3)
                if status == 200:
                    assert ready['replication_factor'] == 1 and ready['topology'] == 'typed-groups-rf1', ready
                    return self
            except (OSError, http.client.HTTPException, ValueError):
                pass
            assert time.monotonic() < deadline, 'serve never became ready'
            time.sleep(0.1)

    def stats(self):
        status, stats = call(self.url, '/v1/runtime/stats', token=self.token)
        assert status == 200, stats
        return stats

    def close(self):
        if self.process:
            self.process.stop()
