#!/usr/bin/env python3
"""LIVE3 provider-neutral acceptance on a dedicated, disposable kind cluster."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time

from csi import CSI_LIMIT, ZONES, ZONE_LABEL, documents, driver_for, manifests

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'server/tests'))
from oltp_direct_cluster import (CHILD, DATABASE, EXECUTION, IDENTITY_FIELDS, WORKERS, Http, Rf3Cluster,
                                 call, operation, resources, uid, write_json)
from oltp_recovery_operations import (TRANSPORT_ERRORS, create, get, initial_range, insert, send,
                                      success, wait_for)

NODE_IMAGE = 'kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d'
NS = 'live3'
RAWFILE = 'rawfile.csi.openebs.io'
PORT = 8080
POOL = '/pool'
EXTRA = '/extra'
CLUSTER_ADMIN = '/v1/cluster/operations'
RETRYABLE = (429, 503, 504)


def run(*args, input=None, timeout=45, check=True):
    with subprocess.Popen(args, text=True, stdin=subprocess.PIPE if input is not None else None,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True) as child:
        try:
            stdout, stderr = child.communicate(input, timeout=timeout)
        except BaseException:
            # kind invokes docker as a child. Reap the whole command group on timeout.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.communicate()
            raise
    result = subprocess.CompletedProcess(args, child.returncode, stdout, stderr)
    if check and result.returncode:
        raise RuntimeError(f'{args!r} exited {result.returncode}\n{stdout}{stderr}')
    return stdout if check else result


class Evidence:
    def __init__(self, output):
        self.output = output
        self.output.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def record(self, check, **details):
        item = dict(check=check, elapsed_seconds=round(time.monotonic() - START, 3), **details)
        line = json.dumps(item, sort_keys=True)
        with self.lock, (self.output / 'trace.jsonl').open('a') as trace:
            print(line, flush=True)
            print(line, file=trace)

    def file(self, name, data):
        (self.output / name).write_text(data)


@contextmanager
def lane(lock, evidence, label="LIVE3 kind (not BENCH)"):
    deadline = time.monotonic() + 3600
    while True:
        try:
            lock.mkdir()
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f'Docker lane remained occupied: {lock}')
            owner = lock / 'owner'
            print(f'Waiting for the Docker lane ({owner.read_text().strip() if owner.exists() else "owner updating"})',
                  flush=True)
            time.sleep(30)
    owner = lock / 'owner'
    try:
        owner.write_text(f'{label} {time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}\n')
        evidence.record('docker-lane', status='acquired')
        yield
    finally:
        owner.unlink(missing_ok=True)
        lock.rmdir()
        evidence.record('docker-lane', status='released')


class Kind:
    def __init__(self, root, evidence, rawfile=None):
        self.name = 'nabedra-live3-' + uid()[:8]
        self.root, self.evidence, self.rawfile = root, evidence, rawfile
        self.kubeconfig = root / 'kubeconfig'
        self.kubectl = ('kubectl', '--kubeconfig', str(self.kubeconfig), '--request-timeout=20s')
        self.control = self.name + '-control-plane'
        self.nodes = {}
        self.images = []
        self.peak = None

    def kube(self, *args, **kwargs):
        return run(*self.kubectl, *args, **kwargs)

    def in_namespace(self, *args, **kwargs):
        return self.kube('-n', NS, *args, **kwargs)

    def obj(self, *args, namespaced=True):
        invoke = self.in_namespace if namespaced else self.kube
        return json.loads(invoke('get', *args, '-o', 'json'))

    def apply(self, *definitions):
        self.kube('apply', '-f', '-', input=json.dumps(dict(apiVersion='v1', kind='List',
                                                           items=definitions)))

    def start(self, csi_source, node_image, archives, csi_images, *, preflight=False, zone_fault=False):
        for archive in archives:
            self.evidence.file(archive.name + '.load.txt', run('docker', 'load', '-i', str(archive), timeout=180))
        self.evidence.record('kind-create', cluster=self.name, node_image=node_image)
        result = run('kind', 'create', 'cluster', '--name', self.name, '--kubeconfig', str(self.kubeconfig),
                     '--config', str(HERE / 'kind.yaml'), '--image', node_image, '--wait', '150s',
                     '--retain',
                     timeout=420, check=False)
        self.evidence.file('kind-create.txt', result.stdout + result.stderr)
        if result.returncode:
            self.bootstrap_diagnostics()
            raise RuntimeError(f'kind create exited {result.returncode}, see kind-create.txt')
        self.kube('create', 'namespace', NS)
        nodes = self.obj('nodes', namespaced=False)['items']
        self.nodes = {node['metadata']['labels'][ZONE_LABEL]: node['metadata']['name'] for node in nodes
                      if ZONE_LABEL in node['metadata']['labels']}
        assert set(self.nodes) == set(ZONES) and len(nodes) == 4, self.nodes
        self.evidence.file('kind-nodes.txt', self.kube('get', 'nodes', '-o', 'wide', '-L', ZONE_LABEL))
        if zone_fault:
            # A data-zone outage must leave the Kubernetes DNS service available.
            system = ('-n', 'kube-system')
            dns = 'deployment/coredns'
            patch = {'spec': {'template': {'spec': {'nodeSelector': {'kubernetes.io/hostname': self.control}}}}}
            self.kube(*system, 'patch', dns, '--type=merge', '-p', json.dumps(patch))
            self.kube(*system, 'rollout', 'status', dns, '--timeout=145s', timeout=155)
            self.evidence.file('coredns-placement.txt',
                               self.kube(*system, 'get', 'pods', '-l', 'k8s-app=kube-dns', '-o', 'wide'))
        probe_node = self.nodes['a'] if self.rawfile and preflight else None
        for archive in csi_images:
            target = ('--nodes', probe_node) if probe_node else ()
            result = run('kind', 'load', 'image-archive', str(archive), '--name', self.name, *target,
                         check=False, timeout=300)
            self.evidence.file(archive.name + '.import.txt', result.stdout + result.stderr)
            assert result.returncode == 0, (archive, result.stderr)
        if self.rawfile:
            definitions = [item for item in documents(self.rawfile) if item]
            workloads = [item for item in definitions if item['kind'] in ('DaemonSet', 'Deployment')]
            if probe_node:
                for workload in workloads:
                    workload['spec']['template']['spec']['nodeSelector'] = {ZONE_LABEL: 'a'}
            self.apply(*definitions)
            targets = [f"{item['kind'].lower()}/{item['metadata']['name']}" for item in workloads]
        else:
            self.apply(*manifests(csi_source))
            targets = [f'statefulset/csi-hostpathplugin-{zone}' for zone in ZONES]
        for target in targets:
            self.kube('rollout', 'status', target, '--timeout=180s', timeout=200)
        if self.rawfile:
            storage_nodes = {probe_node} if probe_node else set(self.nodes.values())

            def rawfile_registered():
                return storage_nodes <= {node['metadata']['name']
                    for node in self.obj('csinode', namespaced=False)['items']
                    if any(driver['name'] == RAWFILE for driver in node['spec'].get('drivers') or ())}

            wait_for(rawfile_registered, 'Rawfile CSI registered on the storage nodes', 90)
        self.evidence.file('csi-ready.txt', self.kube('get', 'csidriver,csinode,storageclass', '-o', 'wide'))

    def load(self, engine, binary, test_executable=None, base_image='python:3.11-slim-trixie'):
        tag = f'{self.name}-{engine}:local'
        stage = self.root / engine
        stage.mkdir()
        (stage / 'nabedra-db').write_bytes(binary.read_bytes())
        if test_executable:
            (stage / 'oltp-test').write_bytes(test_executable.read_bytes())
        self.images.append(tag)
        self.evidence.file(f'{engine}-image.txt', run('docker', 'build', '--pull=false', '-t', tag,
            '--build-arg', 'RUNTIME_IMAGE=' + base_image, '-f', str(HERE / 'Dockerfile'), str(stage), timeout=120))
        run('kind', 'load', 'docker-image', '--name', self.name, tag, timeout=300)
        return tag

    def watch_memory(self, stop):
        while not stop.is_set():
            try:
                memory = run('docker', 'exec', self.control, 'cat', '/proc/meminfo', timeout=10, check=False)
                if memory.returncode:
                    stop.wait(3)
                    continue
                fields = dict((label, int(value)) for label, value in
                              re.findall(r'^(MemTotal|MemAvailable):\s+(\d+)', memory.stdout, re.M))
                used = (fields['MemTotal'] - fields['MemAvailable']) * 1024
                if self.peak is None or used > self.peak:
                    self.peak = used
                    stats = run('docker', 'stats', '--no-stream', '--format', '{{json .}}',
                                self.control, *self.nodes.values(), timeout=12)
                    self.evidence.file('peak-docker-stats.jsonl', stats)
                    self.evidence.record('vm-memory', used_bytes=used, total_bytes=fields['MemTotal'] * 1024)
                if used > 7 * (1 << 30):
                    os.kill(os.getpid(), signal.SIGUSR1)
                    return
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                self.evidence.record('memory-sample-error', detail=str(error))
            stop.wait(5)

    def diagnostics(self):
        for name, args in (
            ('pods.txt', ('get', 'pods', '-A', '-o', 'wide')),
            ('storage.txt', ('get', 'pv,pvc,volumeattachment', '-A', '-o', 'wide')),
            ('events.txt', ('get', 'events', '-A', '--sort-by=.lastTimestamp')),
        ):
            result = self.kube(*args, check=False, timeout=25)
            self.evidence.file(name, result.stdout + result.stderr)
        for name in (*[f'{worker}-0' for worker in WORKERS], 'api'):
            result = self.in_namespace('logs', name, '--all-containers=true', '--tail=120', check=False)
            self.evidence.file(f'{name}.log', result.stdout + result.stderr)

    def bootstrap_diagnostics(self):
        result = run('docker', 'exec', self.control, 'journalctl', '-u', 'kubelet',
                     '--no-pager', '-n', '100', timeout=20, check=False)
        self.evidence.file('kubelet-bootstrap.log', result.stdout + result.stderr)
        for component in ('etcd', 'kube-apiserver'):
            containers = run('docker', 'exec', self.control, 'crictl', 'ps', '-a', '-q',
                             '--name', component, timeout=15, check=False)
            if containers.returncode == 0 and containers.stdout.strip():
                container_id = containers.stdout.split()[0]
                result = run('docker', 'exec', self.control, 'crictl', 'logs', '--tail=80',
                             container_id, timeout=15, check=False)
                self.evidence.file(f'{component}-bootstrap.log', result.stdout + result.stderr)

    def close(self):
        if self.rawfile and self.kubeconfig.exists():
            for spec in ('statefulset,pod', 'pvc'):
                try:
                    result = self.in_namespace('delete', spec, '--all', '--timeout=55s',
                                               timeout=60, check=False)
                    output = result.stdout + result.stderr
                except subprocess.TimeoutExpired as error:
                    output = str(error)
                self.evidence.file(f'rawfile-delete-{spec}.txt', output)
        command = ('kind', 'delete', 'cluster', '--name', self.name, '--kubeconfig', str(self.kubeconfig))
        try:
            result = run(*command, timeout=100, check=False)
        except subprocess.TimeoutExpired as error:
            result = subprocess.CompletedProcess(command, 124, '', str(error))
        remaining = run('docker', 'ps', '-aq', '--filter', f'label=io.x-k8s.kind.cluster={self.name}').split()
        if remaining:
            run('docker', 'rm', '-f', '-v', *remaining)
        remaining = run('docker', 'ps', '-aq', '--filter', f'label=io.x-k8s.kind.cluster={self.name}').split()
        self.evidence.record('kind-delete', status='passed' if not remaining else 'failed',
                             kind_exit=result.returncode, remaining_nodes=remaining,
                             peak_vm_used_bytes=self.peak)
        self.evidence.file('kind-delete.txt', result.stdout + result.stderr)
        for tag in self.images:
            run('docker', 'image', 'rm', tag, check=False)
        assert not remaining, remaining


class Forward:
    def __init__(self, cluster, pod, path):
        self.log = path.open('w+')
        self.process = subprocess.Popen((*cluster.kubectl[:-1], '-n', NS, 'port-forward',
                                         f'pod/{pod}', f':{PORT}', '--address=127.0.0.1'),
                                        stdout=self.log, stderr=subprocess.STDOUT)
        try:
            self.port = wait_for(self.bound_port, f'forward for {pod}', timeout=20)
        except BaseException:
            self.close()
            raise

    def bound_port(self):
        self.log.seek(0)
        text = self.log.read()
        match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)', text)
        if match:
            return int(match[1])
        assert self.process.poll() is None, text
        return None

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=4)
        self.log.close()


def object_meta(kind, name, **extra):
    return dict(apiVersion='apps/v1' if kind in ('StatefulSet', 'Deployment') else 'v1',
                kind=kind, metadata=dict(name=name, namespace=NS), **extra)


class Oltp(Rf3Cluster):
    def __init__(self, cluster, binary, image, root, test_executable=None):
        super().__init__(binary, root, mounts=True, status_partitions=2, key_type='rsa',
                         test_executable=test_executable)
        self.gated = None
        self.cluster, self.image = cluster, image
        self.forwards = {}
        self.zone = dict(zip(WORKERS, ZONES))
        self.pvcs = {}
        self.volume_markers = {}
        self.growth_policy = None

    def certificate(self, name, node_port=None):
        # Retain REP1's pinned TLS identities. mTLS peers use StatefulSet DNS
        # and the gateway's host-side probe validates localhost over port-forward.
        host = f'{name}-0.{name}.{NS}.svc.cluster.local' if name in WORKERS else 'api'
        key, csr, pem, ext = (self.root / f'{name}.{suffix}' for suffix in ('key', 'csr', 'pem', 'ext'))
        ext.write_text(f'subjectAltName=DNS:{host},DNS:localhost,IP:127.0.0.1\n'
                       'extendedKeyUsage=serverAuth,clientAuth\n')
        self.openssl('req', '-new', '-newkey', self.key_spec, '-nodes', '-keyout', key, '-out', csr,
                     '-subj', f'/CN={name}')
        self.openssl('x509', '-req', '-in', csr, '-CA', self.ca, '-CAkey', self.root / 'ca.key',
                     '-set_serial', str(int(uid().replace('-', ''), 16)), '-out', pem, '-days', '1', '-extfile', ext)
        key.chmod(0o600)
        digest = run('openssl', 'x509', '-in', str(pem), '-noout', '-fingerprint', '-sha256')
        digest = digest.split('=', 1)[1].strip().replace(':', '').lower()
        return dict(worker_id=uid(), incarnation=uid(), enrollment_revision=1, certificate_sha256=digest,
                    endpoint=f'{host}:{9000 if name in WORKERS else PORT}', server_name=host)

    def mount_pool(self, name, pool_id, enrollment, path=POOL):
        claim = f'{name}-{"primary" if path == POOL else "extra"}'
        marker = uid()
        self.volume_markers[claim] = dict(format_version=1, volume_name=claim, volume_marker=marker)
        return dict(profile={'docker': {'volume_marker': marker}},
                    worker={field: enrollment[field] for field in IDENTITY_FIELDS}, generation=1,
                    identity=dict(engine='oltp', pool_format_version=2, cluster_id=self.cluster_id,
                                  worker_id=enrollment['worker_id'], pool_id=pool_id),
                    mount_path=path, provider_volume_id='docker-volume:' + claim, filesystem_uuid='')

    def paths(self, name):
        return dict(certificate=f'/config/{name}.pem', private_key=f'/config/{name}.key',
                    authority_ca='/config/ca.pem')

    def claim(self, name, zone, capacity):
        return object_meta('PersistentVolumeClaim', name,
                           spec=dict(accessModes=['ReadWriteOncePod'],
                                     storageClassName='rawfile-localpv' if self.cluster.rawfile else f'live3-{zone}',
                                     resources={'requests': {'storage': capacity}}))

    def secret(self, name, config, *, label_primary=False, label_extra=False):
        files = {path.name: path.read_text() for path in
                 (self.ca, self.root / 'auth.json', self.root / f'{name}.pem', self.root / f'{name}.key')}
        files[f'{name}.json'] = json.dumps(config)
        if name in WORKERS and self.gated:
            request_id = self.gated['request_id']
            files['gates.json'] = json.dumps({request_id: 'queued'})
            files['inspect.json'] = json.dumps([request_id])
            rolling = config['node'].get('rolling')
            if rolling:
                preparation = rolling['preparation_id']
                files['rolling-test-id'] = preparation
                files['gates.json'] = json.dumps({request_id: 'queued', preparation: 'rolling_copy_complete'})
        if name == 'api':
            files['supervised_api.py'] = (HERE / 'supervised_api.py').read_text()
        for label, active in [('primary', label_primary), ('extra', label_extra)]:
            if active:
                files[f'{label}.marker'] = json.dumps(self.volume_markers[f'{name}-{label}'])
        # Secrets contain generated keys. Keep definitions on stdin and out of evidence.
        self.cluster.apply(object_meta('Secret', name, stringData=files))

    def pod_spec(self, name, *, extra=False):
        volumes = [dict(name='input', secret={'secretName': name, 'defaultMode': 0o400}),
                   dict(name='config', emptyDir={})]
        config_mount = dict(name='config', mountPath='/config')
        mounts = [config_mount]
        init = 'umask 077\ncp -L /input/* /config/\nchmod 600 /config/*\n'
        if name in WORKERS:
            for label, path in [('primary', POOL), *(([('extra', EXTRA)]) if extra else [])]:
                volumes.append(dict(name=label, persistentVolumeClaim={'claimName': f'{name}-{label}'}))
                mounts.append(dict(name=label, mountPath=path))
                init += (f'if [ -f /config/{label}.marker ] && [ ! -e {path}/.nabedra-docker-volume.json ]; then\n'
                         f'  cp /config/{label}.marker {path}/.nabedra-docker-volume.json\nfi\n')
        args = (['worker', '--bootstrap', f'/config/{name}.json'] if name in WORKERS else
                ['python3', '/config/supervised_api.py'])
        command = ['nabedra-db', *args] if name in WORKERS else args
        # The worker pod must remain a stable DNS peer while its native runtime starts.
        pod = dict(automountServiceAccountToken=False, volumes=volumes,
                   initContainers=[dict(name='prepare', image=self.image, command=['sh', '-ec', init],
                                        volumeMounts=[*mounts, dict(name='input', mountPath='/input', readOnly=True)])],
                   containers=[dict(name=name, image=self.image, command=command,
                       volumeMounts=mounts, ports=[dict(containerPort=PORT)],
                       resources=dict(requests={'cpu':'100m', 'memory':'128Mi'},
                                      limits={'memory':'1152Mi'}),
                       readinessProbe=dict(tcpSocket={'port': PORT}, periodSeconds=2))])
        if name in WORKERS and self.test_executable:
            barrier = dict(name='barrier', mountPath='/barrier')
            volumes.append(dict(name='barrier', emptyDir={}))
            pod['initContainers'][0]['volumeMounts'].append(barrier)
            pod['initContainers'][0]['command'][2] += (
                'if [ -f /config/gates.json ]; then cp /config/gates.json /config/inspect.json /barrier/; fi\n'
                'if [ -f /config/rolling-test-id ]; then cp /config/rolling-test-id /barrier/; fi\n')
            pod['containers'][0].update(command=['oltp-test', '--exact', CHILD, '--ignored', '--nocapture'],
                env=[dict(name='NABEDRA_OLTP_TEST_REGISTERED_CONFIG', value=f'/config/{name}.json'),
                     dict(name='NABEDRA_OLTP_TEST_BARRIER_DIR', value='/barrier')])
            mounts.append(barrier)
        if name in WORKERS:
            pod['nodeSelector'] = {ZONE_LABEL: self.zone[name]}
        else:
            pod['tolerations'] = [{'key': 'node-role.kubernetes.io/control-plane', 'operator': 'Exists'}]
            pod['nodeSelector'] = {'kubernetes.io/hostname': self.cluster.control}
        return pod

    def launch_worker(self, name):
        cfg = self.config[name]
        node = cfg['node']
        node.update(self.paths(name), directory=POOL, bind='0.0.0.0:9000')
        cfg.update(auth_file='/config/auth.json', bind=f'0.0.0.0:{PORT}')
        new = name not in self.pvcs
        extra = f'{name}-extra' in self.volume_markers
        if new:
            self.cluster.apply(self.claim(f'{name}-primary', self.zone[name], '1Gi'))
        self.secret(name, cfg, label_primary=new, label_extra=extra)
        selector = {'app': name}
        service = object_meta('Service', name, spec=dict(clusterIP='None', publishNotReadyAddresses=True,
                              selector=selector, ports=[dict(port=9000, name='raft'), dict(port=PORT, name='http')]))
        workload = object_meta('StatefulSet', name, spec=dict(serviceName=name, replicas=1,
            selector={'matchLabels': selector}, template=dict(metadata={'labels': selector}, spec=self.pod_spec(name, extra=extra)),
            updateStrategy={'type': 'OnDelete'}))
        self.cluster.apply(service, workload)
        self.pvcs[name] = f'{name}-primary'

    def connect(self, name):
        if name not in self.forwards:
            pod = f'{name}-0' if name in WORKERS else name
            forward = Forward(self.cluster, pod, self.root / f'{name}-forward.log')
            self.forwards[name] = forward
            if name in WORKERS:
                self.urls[name] = f'http://localhost:{forward.port}'
            else:
                self.api_urls['api1'] = f'https://localhost:{forward.port}'
        return self.urls[name] if name in WORKERS else self.api_urls['api1']

    def disconnect(self, name):
        forward = self.forwards.pop(name, None)
        if forward:
            forward.close()
        self.urls.pop(name, None)

    def wait_workers(self, names=WORKERS, deadline_seconds=180):
        for name in names:
            self.wait_pod(name)
            self.connect(name)

        def ready():
            views = {}
            for name in names:
                status, reply = self.worker(name, '/health/ready')
                if status == 200 and reply.get('ready_workers') == reply.get('replication_factor') == 3:
                    assert reply['run_id'] == self.run_id, reply
                    views[name] = reply
            return views if len(views) == len(names) else None

        return wait_for(ready, 'RF3 same-run worker readiness', deadline_seconds)

    def wait_pod(self, name):
        def exists():
            result = self.cluster.in_namespace('get', f'pod/{name}-0', '--ignore-not-found')
            return bool(result.strip())
        wait_for(exists, f'{name} Pod created', 30)
        self.cluster.in_namespace('wait', f'pod/{name}-0', '--for=condition=Ready',
                                  '--timeout=145s', timeout=155)

    def worker(self, name, path, body=None, admin=False, timeout=5):
        token = self.auth['admin_token' if admin else 'data_token']
        return call(self.connect(name), path, body, token=token, timeout=timeout)

    def admin(self, name, payload, worker=None, request_id=None):
        targets = [worker] if worker else list(self.urls)
        last = None
        body = operation(name, payload, request_id)
        for target in targets:
            try:
                last = self.worker(target, CLUSTER_ADMIN, body, admin=True, timeout=10)
                if last[0] == 200:
                    return success(*last)
            except TRANSPORT_ERRORS as error:
                last = repr(error)
        raise AssertionError(f'{name}: {last!r}')

    def live(self):
        return list(self.urls)

    def start_api(self, _name, direct=True):
        name = 'api'
        enrollment = self.certificate(name)
        # The native gateway binds its enrolled peer endpoint separately from HTTP.
        enrollment.update(endpoint='127.0.0.1:9000', server_name='localhost')
        record = dict(database=DATABASE, enrollment=enrollment)
        self.admin('api.enroll', {'expected': None, 'next': record})
        config = dict(version=1, engine='oltp', bind=f'0.0.0.0:{PORT}', database=DATABASE,
                      auth_file='/config/auth.json', tls=dict(cert_file='/config/api.pem', key_file='/config/api.key'),
                      topology=dict(mode='remote', cluster_id=self.cluster_id, control_group_id=self.connection['control_group_id'],
                                    control_peers=self.connection['control_peers'], enrollment=record, direct_oltp=direct,
                                    worker_tls=dict(cert_file='/config/api.pem', key_file='/config/api.key', ca_file='/config/ca.pem')),
                      worker=dict(resources=resources(2048 << 20), execution=EXECUTION, queue_size=128))
        self.secret(name, config)
        self.cluster.apply(object_meta('Pod', name, spec=self.pod_spec(name)))
        self.cluster.in_namespace('wait', 'pod/api', '--for=condition=Ready', '--timeout=90s', timeout=100)
        self.connect(name)
        def healthy():
            status, body = self.exchange('/health/ready')
            return body if status == 200 else None

        ready = wait_for(healthy, 'separate API readiness', timeout=45)
        assert ready['topology'] == 'api-gateway', ready

    def exchange(self, path, body=None, timeout=10):
        return send(self.api_urls['api1'], self.auth['data_token'], body, path=path, ca=self.ca, timeout=timeout)

    def stats(self, name):
        return self.worker(name, '/v1/runtime/stats', timeout=10)[1]

    def close(self):
        for name in list(self.forwards):
            self.disconnect(name)

    def identity(self, name):
        return {key: self.enrollments[name][key] for key in IDENTITY_FIELDS}

    def pod(self, name):
        return self.cluster.obj('pod', f'{name}-0')

    def attachments(self, claim):
        volume = claim['spec']['volumeName']
        return [item for item in self.cluster.obj('volumeattachment', namespaced=False)['items']
                if item['spec']['source'].get('persistentVolumeName') == volume]

    def verify_volume(self, name, label='primary'):
        claim_name = f'{name}-{label}'
        claim = self.cluster.obj('pvc', claim_name)
        volume = self.cluster.obj('pv', claim['spec']['volumeName'], namespaced=False)
        pod = self.pod(name)
        node = self.cluster.nodes[self.zone[name]]
        assert pod['spec']['nodeName'] == node, pod['spec']['nodeName']
        driver = RAWFILE if self.cluster.rawfile else driver_for(self.zone[name])
        assert volume['spec']['csi']['driver'] == driver, volume['spec']['csi']
        assert volume['spec']['claimRef']['uid'] == claim['metadata']['uid'], volume['spec']['claimRef']
        attachments = self.attachments(claim)
        attachment = None
        if self.cluster.rawfile:
            assert not attachments, attachments
        else:
            assert len(attachments) == 1 and attachments[0]['status']['attached'], attachments
            assert attachments[0]['spec']['nodeName'] == node, attachments
            attachment = attachments[0]['metadata']['uid']
        endpoint = POOL if label == 'primary' else EXTRA
        mapping = next(line for line in self.exec(name, 'cat', '/proc/self/mountinfo').splitlines()
                       if line.split()[4] == endpoint)
        return dict(claim=claim_name, pvc_uid=claim['metadata']['uid'], pod_uid=pod['metadata']['uid'],
                    pv=volume['metadata']['name'], handle=volume['spec']['csi']['volumeHandle'],
                    node=node, zone=self.zone[name], mount=mapping,
                    attachment_uid=attachment, driver=driver)

    def exec(self, name, *args):
        return self.cluster.in_namespace('exec', f'{name}-0', '-c', name, '--', *args)

    def wait_pod_removed(self, name, timeout=80):
        wait_for(lambda: not self.cluster.in_namespace('get', f'pod/{name}-0',
                  '--ignore-not-found').strip(), f'{name} Pod removed', timeout)

    def remove(self, name, claim):
        self.disconnect(name)
        self.cluster.in_namespace('scale', f'statefulset/{name}', '--replicas=0')
        self.wait_pod_removed(name)
        wait_for(lambda: not self.attachments(claim), f'{name} CSI detach', 75)

    def reprepare(self, name):
        survivors = [other for other in WORKERS if other != name]

        def ready():
            for other in survivors:
                status, body = self.worker(other, '/health/ready')
                if not body or body.get('ready_workers') != 2:
                    return None
                assert body['run_id'] == self.run_id, (status, body)
            return True

        wait_for(ready, 'Control published two surviving workers', 90)
        peers = self.admin('cluster.connection', {}, worker=survivors[0])['control_peers']
        previous = self.enrollments[name]
        fresh = dict(previous, incarnation=uid(), enrollment_revision=previous['enrollment_revision'] + 1)
        cfg = deepcopy(self.config[name])
        node = cfg['node']
        plan = dict(id=uid(), run_id=self.run_id, previous=self.identity(name), next=fresh,
                    pool_id=node['pool_id'], mount_generation=node['mount']['generation'] + 1)
        request = operation('worker.restart.prepare', plan, plan['id'])

        def prepare(targets):
            for other in targets:
                status, reply = self.worker(other, CLUSTER_ADMIN, request, admin=True, timeout=10)
                if status == 200:
                    return other, success(status, reply)
                assert status in (409, *RETRYABLE), (status, reply)
            return None

        operator, result = wait_for(lambda: prepare(survivors), 'retained restart preparation', 90)
        _, replay = wait_for(lambda: prepare([operator]), 'retained restart preparation replay', 35)
        expected = {field: fresh[field] for field in IDENTITY_FIELDS}
        assert result['identity'] == replay['identity'] == expected, (result, replay)
        self.cluster.evidence.record('O18 retained restart preparation replay', operator=operator,
                                     preparation_id=plan['id'], original=result, replay=replay)
        node.update(enrollment=fresh, control_enrollments=peers,
                    rolling=dict(preparation_id=plan['id'], run_id=self.run_id))
        node['mount'].update(worker=result['identity'], generation=plan['mount_generation'])
        self.config[name], self.enrollments[name] = cfg, fresh
        return result['identity']

    def write_barrier(self, name, file, value):
        self.cluster.in_namespace('exec', '-i', f'{name}-0', '-c', name, '--',
                                  'tee', f'/barrier/{file}', input=json.dumps(value))

    def accepted_copy(self, name, *, at_join=False):
        request_id = self.gated['request_id']
        if not at_join:
            self.stats(name)  # The test instrumentation reads the live RAM replica.
        captured = json.loads(self.exec(name, 'cat', f'/barrier/record-{request_id}.json'))
        assert captured['accepted'] and json.loads(captured['body']) == self.gated, captured
        assert captured['run_id'] == self.run_id and captured['worker']['incarnation'] == self.identity(name)['incarnation']
        return {key: captured[key] for key in ('state', 'body_hash', 'context_hash', 'worker')}

    def hold_request(self, traffic):
        self.gated = insert(traffic.table, -100000, -300000)
        request_id = self.gated['request_id']
        for name in WORKERS:
            self.write_barrier(name, 'gates.json', {request_id: 'queued'})
            self.write_barrier(name, 'inspect.json', [request_id])
        status, reply = self.exchange('/v1/requests', self.gated)
        assert status == 202 and reply['run_id'] == self.run_id, (status, reply)
        traffic.receipts.append((self.gated, status))

        def owner():
            return next((name for name in WORKERS if
                self.cluster.in_namespace('exec', f'{name}-0', '-c', name, '--',
                    'test', '-f', f'/barrier/reached-{request_id}', check=False).returncode == 0), None)

        name = wait_for(owner, 'held write reached the queue owner', 30)
        self.cluster.evidence.record('R03/Q03 accepted complete replicas before fault', owner=name,
                                     copies={worker: self.accepted_copy(worker) for worker in WORKERS})
        return name

    def joined_before_ready(self, name):
        preparation = self.config[name]['node']['rolling']['preparation_id']

        def checkpoint():
            return self.cluster.in_namespace('exec', f'pod/{name}-0', '-c', name, '--',
                'test', '-f', f'/barrier/reached-{preparation}', check=False).returncode == 0

        wait_for(checkpoint, 'replacement reached its complete queue copy before publishing readiness', 115)
        copy = self.accepted_copy(name, at_join=True)
        assert copy['state'] == 'queued', copy
        proof = json.loads(self.exec(name, 'cat', '/barrier/rolling-copy.json'))
        assert proof['statement']['ticket']['target'] == self.identity(name), proof
        for survivor in WORKERS:
            if survivor != name:
                _, ready = self.worker(survivor, '/health/ready')
                assert ready['ready_workers'] == 2 and ready['run_id'] == self.run_id, ready
        self.cluster.evidence.record('R03/Q03 complete queue copy before readiness', worker=name, copy=copy,
                                     statement=proof['statement'])
        self.exec(name, 'touch', f'/barrier/release-{preparation}')

    def release_request(self):
        request_id = self.gated['request_id']
        for name in WORKERS:
            self.exec(name, 'touch', f'/barrier/release-{request_id}')
        self.gated = None

    def verify_replacement(self, name, old, installed):
        self.wait_workers()
        volume = self.verify_volume(name)
        assert old['pod_uid'] != volume['pod_uid'], (old, volume)
        assert all(volume[key] == old[key] for key in ('pvc_uid', 'pv', 'handle', 'zone', 'node')), (old, volume)
        peers = self.admin('cluster.connection', {})['control_peers']
        assert next(peer for peer in peers if peer['worker_id'] == installed['worker_id']) == self.enrollments[name]
        self.cluster.evidence.record('O18 retained replacement', worker=name, old=old, new=volume,
                                     identity=installed)

    def applied_voters(self, name, groups, placements):
        worker = self.identity(name)['worker_id']
        probes = [(group, next(member['node_id'] for member in placement['members']
                                if member['worker_id'] == worker))
                  for group, placement in zip(groups, placements)]
        self.write_barrier(name, 'inspect-data-voters.json', probes)
        expected = [sorted(item['node_id'] for item in placement['members']) for placement in placements]

        def applied():
            self.stats(name)
            observations = json.loads(self.exec(name, 'cat', '/barrier/data-voters.json'))
            if any(item is None for item in observations):
                return None
            assert [sorted(voters) for voters, _ in observations] == expected, observations
            return observations

        return wait_for(applied, f'{name} applied both RF3 Data voter sets', 35)

    def replace_worker(self, name, *, delete_pod=False):
        old = self.verify_volume(name)
        claim = self.cluster.obj('pvc', old['claim'])
        if delete_pod:
            self.disconnect(name)
            self.cluster.in_namespace('delete', f'pod/{name}-0', '--wait=true', '--timeout=70s', timeout=80)

            def rescheduled():
                result = self.cluster.in_namespace('get', f'pod/{name}-0', '-o', 'json', '--ignore-not-found')
                if result:
                    pod = json.loads(result)
                    if pod['metadata']['uid'] != old['pod_uid']:
                        return pod

            new = wait_for(rescheduled, 'StatefulSet controller rescheduled deleted Pod', 40)
            self.cluster.evidence.record('O18 automatic Pod replacement', old_uid=old['pod_uid'],
                                         recreated_uid=new['metadata']['uid'])
        self.remove(name, claim)
        identity = self.reprepare(name)
        self.launch_worker(name)
        self.verify_replacement(name, old, identity)


class Traffic:
    def __init__(self, topology):
        self.topology = topology
        self.evidence = topology.cluster.evidence
        self.table = create(self.op, 'live3_writes')
        self.probe_table = create(self.op, 'live3_probe')
        assert success(*self.op(insert(self.probe_table, 0, 777))) is None
        self.stop = threading.Event()
        self.writes = []
        self.receipts = []
        self.read_times = []

    def op(self, body):
        return self.topology.exchange('/v1/operations', body)

    def invoke(self, client, kind, path, body):
        started = time.monotonic()
        try:
            status, reply = client.exchange(path, body)
        except TRANSPORT_ERRORS as error:
            status, reply = None, {'transport': repr(error)}
        finished = time.monotonic()
        self.evidence.record(kind, request=body, status=status, response=reply,
                             started_seconds=round(started - START, 6),
                             latency_ms=round((finished - started) * 1000, 3))
        assert status in (200, 202, None, *RETRYABLE), (status, reply)
        return finished, status, reply

    def write_load(self, client):
        for key in range(1, 20000):
            if self.stop.is_set():
                return
            body = insert(self.table, key, key * 3)
            started = time.monotonic()
            _, status, reply = self.invoke(client, 'write', '/v1/operations', body)
            if status == 200:
                assert reply == {'request_id': body['request_id'], 'data': None}, reply
            self.writes.append((body, reply if status == 200 else None, started))
            self.stop.wait(0.2)

    def read_load(self, client):
        while not self.stop.is_set():
            end, status, reply = self.invoke(client, 'read', '/v1/operations', get(self.probe_table, 0))
            if status == 200:
                assert reply['data'] == {'id': 0, 'value': 777}, reply
                self.read_times.append(end)
            self.stop.wait(0.25)

    def queue_load(self, client):
        for key in range(-1, -2000, -1):
            if self.stop.is_set():
                return
            body = insert(self.table, key, key * 3)
            _, status, reply = self.invoke(client, 'queued-write', '/v1/requests', body)
            if status == 202:
                assert reply == dict(request_id=body['request_id'], run_id=self.topology.run_id, state='queued'), reply
            self.receipts.append((body, status))
            self.stop.wait(0.8)

    @contextmanager
    def load(self):
        def persistent(action):
            client = Http(self.topology.api_urls['api1'], self.topology.auth['data_token'],
                          timeout=7, ca=self.topology.ca)
            try:
                action(client)
            finally:
                client.close()

        with ThreadPoolExecutor(3) as pool:
            tasks = [pool.submit(persistent, action) for action in (self.write_load, self.read_load, self.queue_load)]
            try:
                wait_for(lambda: self.writes and self.read_times and any(status == 202 for _, status in self.receipts),
                         'write, independent read and receipt before lifecycle fault', 30)
                yield
            finally:
                self.stop.set()
                for task in tasks:
                    task.result()

    def finish(self):
        expected = []
        for body, reply, _ in self.writes:
            row = body['payload']['row']
            if reply is not None:
                # Check persistence before retrying so replay cannot repair a lost acknowledgment.
                assert success(*self.op(get(self.table, row['id']))) == row, row
            status, retried = self.op(body)
            assert status == 200 and retried == {'request_id': body['request_id'], 'data': None}, retried
            expected.append(row)
        accepted = 0
        for body, original in self.receipts:
            identity = body['request_id']
            path = f'/v1/requests/{self.topology.run_id}/{identity}'
            if original != 202:
                status, response = self.topology.exchange('/v1/requests', body)
                assert status == 202 and response['run_id'] == self.topology.run_id, (status, response)
            else:
                accepted += 1

            def receipt():
                status, reply = self.topology.exchange(path)
                if status in RETRYABLE:
                    return None
                assert status == 200 and reply['state'] not in ('failed', 'abandoned'), (status, reply)
                assert reply['run_id'] == self.topology.run_id and reply['request_id'] == identity, reply
                return reply if reply['state'] == 'succeeded' else None

            wait_for(receipt, f'original receipt {identity}', 35)
            expected.append(body['payload']['row'])
        query = dict(table_id=self.table, query=dict(select=None, filter=None, limit=None,
                                                     order_by=dict(column='id', desc=False)))
        rows = success(*self.op(operation('query.collect', query)))
        assert rows == sorted(expected, key=lambda row: row['id']), (len(rows), len(expected))
        assert success(*self.op(operation('query.count', query))) == len(expected)
        self.evidence.record('O17/O18 row and receipt oracle', exact_rows=len(rows),
                             acknowledged_direct=sum(reply is not None for _, reply, _ in self.writes),
                             originally_accepted_receipts=accepted, retried_receipts=len(self.receipts) - accepted)


def zone_loss(topology, traffic, name='w1'):
    cluster = topology.cluster
    old = topology.verify_volume(name)
    node = old['node']
    traffic.evidence.record('O18 zone-stop-issued', zone=old['zone'], node=node)
    started = time.monotonic()
    run('docker', 'stop', '--time=0', node, timeout=30)
    down = time.monotonic()
    traffic.evidence.record('O18 zone-stopped', zone=old['zone'], node=node)
    topology.disconnect(name)
    try:
        wait_for(lambda: next((sample for sample in traffic.read_times if sample > down), None),
                 'verified independent read while zone down', 65)
        wait_for(lambda: any(reply is not None and attempt_started > down
                             for _, reply, attempt_started in traffic.writes),
                 'acknowledged write while zone down', 65)
        first = next(sample for sample in traffic.read_times if sample > down)
        last = max(sample for sample in traffic.read_times if sample < started)
        traffic.evidence.record('O18 two-zone quorum', zone=old['zone'],
                                 stop_seconds=round(down - started, 3),
                                 first_read_after_stop_seconds=round(first - down, 3),
                                 read_completion_gap_seconds=round(first - last, 3))
        if topology.gated:
            traffic.evidence.record('R03/Q03 complete copies while zone down',
                                    copies={other: topology.accepted_copy(other) for other in WORKERS if other != name})
        cluster.in_namespace('scale', f'statefulset/{name}', '--replicas=0')
        identity = topology.reprepare(name)
    finally:
        run('docker', 'start', node, timeout=40)
        traffic.evidence.record('O18 zone-restored', zone=old['zone'], node=node)
    cluster.kube('wait', f'node/{node}', '--for=condition=Ready', '--timeout=120s', timeout=130)
    topology.wait_pod_removed(name, timeout=100)
    topology.launch_worker(name)
    if topology.gated:
        topology.joined_before_ready(name)
    topology.verify_replacement(name, old, identity)
    if topology.gated:
        topology.release_request()


def refuse_second_writer(topology, name='w1'):
    cluster = topology.cluster
    volume = topology.verify_volume(name)
    claim = volume['claim']
    probe = object_meta('Pod', 'second-writer', spec=dict(
        restartPolicy='Never', nodeSelector={ZONE_LABEL: volume['zone']},
        containers=[dict(name='writer', image=topology.image, command=['sh','-ec','echo exclusive > /probe/fail'],
                         volumeMounts=[dict(name='pvc', mountPath='/probe')])],
        volumes=[dict(name='pvc', persistentVolumeClaim={'claimName': claim})]))
    cluster.apply(probe)
    try:
        pod = cluster.obj('pod', 'second-writer')

        def denied():
            events = cluster.obj('events')['items']
            return next((item for item in events if item['reason'] == 'FailedScheduling'
                         and item['involvedObject'].get('uid') == pod['metadata']['uid']
                         and 'ReadWriteOncePod' in item.get('message', '')
                         and 'in-use' in item.get('message', '')), None)

        event = wait_for(denied, 'ReadWriteOncePod rejected a second writer', 25)
        assert cluster.obj('pod', 'second-writer')['status']['phase'] == 'Pending'
        cluster.evidence.record('O18 one-writer PVC', primary=volume, denial=event['message'])
    finally:
        cluster.in_namespace('delete', 'pod/second-writer', '--wait=false')


def growth_claim(topology, name, action, pool_id, generation, provider_volume, *, worker=None):
    identity = worker or topology.identity(name)
    claim_id = uid()
    if topology.growth_policy is None:
        topology.growth_policy = dict(policy_id=uid(), volume_cost_per_gib=0, worker_cost=0,
            initial_monthly_spend=0, monthly_spend_ceiling=0, max_attempts=2, cooldown_seconds=1,
            max_volume_bytes=1 << 40, zones=list(ZONES),
            initial_replica_zones={topology.enrollments[worker]['worker_id']: topology.zone[worker]
                                   for worker in WORKERS})
        topology.admin('storage.growth.configure', {'policy': topology.growth_policy}, worker=name)
    intent = dict(policy_id=topology.growth_policy['policy_id'], claim_id=claim_id, worker=identity,
                  pool_id=pool_id, pool_generation=generation, quoted_monthly_cost=0,
                  monthly_spend_ceiling=0, max_attempts=2, cooldown_seconds=1, action=action)
    claimed = topology.admin('storage.growth.claim', {'intent': intent}, worker=name, request_id=claim_id)
    assert claimed['phase'] == 'claimed' and claimed['mounted_total_bytes'] is None, claimed
    topology.admin('storage.growth.attempt', {'claim_id': claim_id}, worker=name)
    provisioned = {'claim_id': claim_id, 'resource_id': provider_volume}
    reported = topology.admin('storage.growth.provisioned', provisioned, worker=name)
    assert reported['phase'] == 'awaiting_mount' and reported['mounted_total_bytes'] is None, reported
    return claim_id


def pvc_resize(topology, traffic, name='w2'):
    cluster = topology.cluster
    before = topology.verify_volume(name)
    path = 'pvc/' + before['claim']

    def df():
        return topology.exec(name, 'df', '-kP', POOL).splitlines()[-1]

    def capacity():
        return cluster.obj('pvc', before['claim'])['status']['capacity']['storage']

    def set_size(size):
        cluster.in_namespace('patch', path, '--type=merge', '-p',
                             json.dumps({'spec': {'resources': {'requests': {'storage': size}}}}))

    initial_df = df()
    initial_pod = before['pod_uid']
    assert capacity() == '1Gi'
    primary = topology.config[name]['node']['mount']
    pool_id = primary['identity']['pool_id']
    worker = topology.identity(name)
    inventory = topology.admin('storage.growth.inventory', {}, worker=name)
    observed = next(item for item in inventory['pools'] if item['pool_id'] == pool_id)
    requested = observed['observed_total_bytes'] + (512 << 20)
    claim_id = growth_claim(topology, name, dict(kind='expand', worker=worker['worker_id'],
        pool=pool_id, target_bytes=requested), pool_id, primary['generation'], primary['provider_volume_id'])
    request = operation('storage.growth.mounted', {'claim_id': claim_id})
    status, body = topology.worker(name, CLUSTER_ADMIN, request, admin=True)
    assert status in (409, 503), (status, body)
    cluster.evidence.record('O27 premature growth is refused', status=status, response=body,
                            claim=claim_id, requested_bytes=requested,
                            observed_total_bytes=observed['observed_total_bytes'])
    # Discard the first accepted provider reply and retry the exact mutation.
    set_size('2Gi')
    set_size('2Gi')
    wait_for(lambda: capacity() == '2Gi', 'CSI controller and node expand PVC status', 120)
    after = topology.verify_volume(name)
    for key in ('pod_uid', 'pvc_uid', 'pv', 'handle', 'node', 'attachment_uid'):
        assert after[key] == before[key], (key, before, after)
    resized_df = df()
    physical_change = int(resized_df.split()[1]) - int(initial_df.split()[1])
    cluster.evidence.record('O27 PVC resize and replay', pvc=before['claim'], pod_uid=initial_pod,
                            pvc_uid=before['pvc_uid'], initial_capacity='1Gi', resized_capacity='2Gi',
                            initial_df=initial_df, resized_df=resized_df, filesystem_delta_kib=physical_change)

    if cluster.rawfile:
        assert physical_change > 900_000, (initial_df, resized_df)
        request_id = uid()
        payload = {'claim_id': claim_id}
        finished = topology.admin('storage.growth.mounted', payload, worker=name, request_id=request_id)
        assert finished['phase'] == 'complete' and finished['mounted_total_bytes'] >= requested, finished
        assert topology.admin('storage.growth.mounted', payload, worker=name, request_id=request_id) == finished
        cluster.evidence.record('O27 runtime expansion completed and replayed', claim=finished)
        return
    assert physical_change == 0, (initial_df, resized_df)
    refused = topology.worker(name, CLUSTER_ADMIN,
        operation('storage.growth.mounted', {'claim_id': claim_id}), admin=True)
    assert refused[0] in (409, 503), refused
    record = topology.admin('storage.growth.status', {'claim_id': claim_id}, worker=name)
    assert record['phase'] == 'awaiting_mount' and record['mounted_total_bytes'] is None, record
    cluster.evidence.record('O27 CSI metadata did not advertise runtime growth', claim=record)
    set_size('11Gi')

    def failed_event():
        events = cluster.obj('events')['items']
        return next((event['message'] for event in events
                     if event['involvedObject'].get('uid') == before['pvc_uid']
                     and 'maximum allowed' in event.get('message', '')), None)

    error = wait_for(failed_event, 'CSI rejects resize above driver capacity', 90)
    assert capacity() == '2Gi'
    assert int(df().split()[1]) == int(resized_df.split()[1])
    assert success(*traffic.op(get(traffic.probe_table, 0))) == {'id': 0, 'value': 777}
    cluster.evidence.record('O27 CSI resize failure', pvc=before['claim'], capacity='2Gi', message=error)


def provision_failure(topology, name='w3'):
    cluster = topology.cluster
    claim = 'oversized'
    cluster.apply(topology.claim(claim, topology.zone[name], '11Gi'))
    pod = object_meta('Pod', 'provision-failure', spec=dict(
        restartPolicy='Never', nodeSelector={ZONE_LABEL: topology.zone[name]},
        containers=[dict(name='mount', image=topology.image, command=['true'],
                         volumeMounts=[dict(name='oversized', mountPath='/probe')])],
        volumes=[dict(name='oversized', persistentVolumeClaim={'claimName': claim})]))
    cluster.apply(pod)
    try:
        pvc = cluster.obj('pvc', claim)
        identity = pvc['metadata']['uid']

        def failed_event():
            return next((event['message'] for event in cluster.obj('events')['items']
                         if event['involvedObject'].get('uid') == identity
                         and event['reason'] == 'ProvisioningFailed'
                         and ('maximum' in event['message'] or 'exceeds' in event['message'])), None)

        error = wait_for(failed_event, 'CSI rejects oversize provisioning', 90)
        updated = cluster.obj('pvc', claim)
        assert updated['status']['phase'] == 'Pending' and not updated['spec'].get('volumeName'), updated
        cluster.evidence.record('O27 CSI provision failure', pvc=claim, pvc_uid=identity, message=error,
                                driver_limit_bytes=int(CSI_LIMIT))
    finally:
        cluster.in_namespace('delete', 'pod/provision-failure', f'pvc/{claim}', '--wait=false')


def additional_pool(topology, name='w3'):
    cluster = topology.cluster
    old = topology.verify_volume(name)
    claim_name = f'{name}-extra'
    claim = topology.claim(claim_name, topology.zone[name], '1Gi')
    # Simulate losing the creation response. Apply the same claim and compare UID.
    cluster.apply(claim)
    first = cluster.obj('pvc', claim_name)['metadata']['uid']
    cluster.apply(claim)
    assert cluster.obj('pvc', claim_name)['metadata']['uid'] == first
    topology.remove(name, cluster.obj('pvc', old['claim']))
    identity = topology.reprepare(name)
    pool_id = uid()
    mount = topology.mount_pool(name, pool_id, topology.enrollments[name], EXTRA)
    topology.launch_worker(name)
    topology.verify_replacement(name, old, identity)
    attached = topology.verify_volume(name, 'extra')
    assert attached['pvc_uid'] == first and attached['handle'] != old['handle']
    assert attached['node'] == old['node']
    cluster.evidence.record('O27 secondary PVC attached', volume=attached, primary=old)

    wanted = 896 << 20
    action = dict(kind='add_pool', worker=identity['worker_id'], zone=topology.zone[name], bytes=wanted)
    claim_id = growth_claim(topology, name, action, pool_id, 1, mount['provider_volume_id'])
    before = topology.admin('storage.growth.inventory', {}, worker=name)
    assert all(pool['pool_id'] != pool_id for pool in before['pools']), before
    body = {'claim_id': claim_id, 'mount': mount}
    request_id = uid()
    ready = topology.admin('storage.growth.mounted', body, worker=name, request_id=request_id)
    assert ready['phase'] == 'complete' and ready['mounted_total_bytes'] >= wanted, ready
    assert topology.admin('storage.growth.mounted', body, worker=name, request_id=request_id) == ready
    after = topology.admin('storage.growth.inventory', {}, worker=name)
    assert sum(pool['pool_id'] == pool_id for pool in after['pools']) == 1, after
    assert len(before['backing_filesystems']) == 1, before
    assert len(after['backing_filesystems']) == (2 if cluster.rawfile else 1), after
    assert after['total_available_bytes'] == sum(item['available_bytes'] for item in after['backing_filesystems'])
    cluster.evidence.record('O27 runtime pool readiness and lost reply', pool=pool_id, claim=ready,
                            shared_backing_filesystem=not cluster.rawfile,
                            before_inventory=before, after_inventory=after)
    if cluster.rawfile:
        exhaust_new_pool(topology, name, pool_id)


def exhaust_new_pool(topology, name, pool_id):
    def capacity():
        inventory = topology.admin('storage.growth.inventory', {}, worker=name)
        return next(pool['observed_available_bytes'] for pool in inventory['pools'] if pool['pool_id'] == pool_id)

    initial = capacity()
    # Only the isolated, newly opened pool is filled. It holds no Data replicas.
    assert 0 < initial < 1 << 30, initial
    path = f'{EXTRA}/live3-filler'
    free_kib = int(topology.exec(name, 'df', '-kP', EXTRA).splitlines()[-1].split()[3])
    try:
        topology.exec(name, 'fallocate', '-l', str((free_kib - 64) * 1024), path)
        remaining = capacity()
        assert remaining < 2 << 20, remaining
        status = topology.cluster.in_namespace('exec', f'{name}-0', '-c', name, '--',
            'fallocate', '-l', '128MiB', path + '-overflow', check=False)
        assert status.returncode != 0 and 'No space left on device' in status.stderr, status.stderr
        topology.cluster.evidence.record('O27 mounted pool exhausted', pool=pool_id,
                                         initial_bytes=initial, remaining_bytes=remaining,
                                         write_error=status.stderr.strip())
    finally:
        topology.exec(name, 'rm', '-f', path, path + '-overflow')
    assert capacity() >= initial - (2 << 20)


def oltp(cluster, binary, image, scenario, test_executable):
    with ExitStack() as stack:
        topology = Oltp(cluster, binary, image, cluster.root / 'fixture', test_executable)
        stack.callback(topology.close)
        topology.start()
        topology.wait_direct_active()
        for name in WORKERS:
            cluster.evidence.record('O18 initial PVC', volume=topology.verify_volume(name))
        traffic = Traffic(topology)
        groups = [initial_range(topology, table) for table in (traffic.table, traffic.probe_table)]
        placements = [topology.admin('storage.replica.placement', {'group': group}) for group in groups]
        with traffic.load():
            if scenario == 'scale':
                from oltp_scale import scale
                scale(topology, growth_claim, run, traffic)
            if scenario in ('all', 'lifecycle', 'rolling', 'zone'):
                refuse_second_writer(topology)
            if scenario in ('all', 'lifecycle', 'rolling'):
                for name in WORKERS:
                    topology.replace_worker(name, delete_pod=name == 'w1')
            if scenario in ('all', 'lifecycle', 'zone'):
                victim = topology.hold_request(traffic) if test_executable else 'w1'
                zone_loss(topology, traffic, victim)
            if scenario in ('all', 'storage'):
                additional_pool(topology)
                pvc_resize(topology, traffic)
                if not cluster.rawfile:
                    provision_failure(topology)
        traffic.finish()
        if test_executable:
            for name in WORKERS:
                cluster.evidence.record('O18 Data votes applied', worker=name,
                                        groups=topology.applied_voters(name, groups, placements))
        for group, prior in zip(groups, placements):
            after = topology.admin('storage.replica.placement', {'group': group})
            assert after == prior and len(after['members']) == after['record']['replica_count'] == 3, after
            cluster.evidence.record('O18 Data placement RF3', group=group, placement=after)


def preflight_filesystem(cluster, image):
    # This provider probe needs no Nabedra binary. It checks the kernel and CSI
    # prerequisites before an isolated filesystem is used for the storage run.
    run('kind', 'load', 'docker-image', '--name', cluster.name, '--nodes', cluster.nodes['a'], image, timeout=300)
    name = 'ext4-preflight'
    claim = object_meta('PersistentVolumeClaim', name, spec=dict(
        accessModes=['ReadWriteOncePod'], storageClassName='rawfile-localpv',
        resources={'requests': {'storage': '256Mi'}}))
    pod = object_meta('Pod', name, spec=dict(restartPolicy='Never', nodeSelector={ZONE_LABEL: 'a'},
        containers=[dict(name='probe', image=image, command=['sleep', '600'],
                         volumeMounts=[dict(name='volume', mountPath='/probe')])],
        volumes=[dict(name='volume', persistentVolumeClaim={'claimName': name})]))
    cluster.apply(claim, pod)
    cluster.in_namespace('wait', f'pod/{name}', '--for=condition=Ready', '--timeout=100s', timeout=110)

    def exec_probe(*args, **options):
        return cluster.in_namespace('exec', name, '--', *args, **options)

    def size():
        return int(exec_probe('df', '-kP', '/probe').splitlines()[-1].split()[1])

    original = cluster.obj('pod', name)['metadata']['uid']
    before = size()
    exec_probe('sh', '-ec', 'echo live3-ext4 > /probe/sentinel && sync /probe/sentinel')
    patch = json.dumps({'spec': {'resources': {'requests': {'storage': '512Mi'}}}})
    for _ in range(2):
        cluster.in_namespace('patch', f'pvc/{name}', '--type=merge', '-p', patch)
    wait_for(lambda: cluster.obj('pvc', name)['status']['capacity']['storage'] == '512Mi',
             'CSI published the expanded capacity', 110)
    after = size()
    assert after - before > 240_000 and cluster.obj('pod', name)['metadata']['uid'] == original
    assert exec_probe('cat', '/probe/sentinel').strip() == 'live3-ext4'
    free = int(exec_probe('df', '-kP', '/probe').splitlines()[-1].split()[3])
    exec_probe('fallocate', '-l', str((free - 64) * 1024), '/probe/fill')
    overflow = exec_probe('fallocate', '-l', '64MiB', '/probe/overflow', check=False)
    assert overflow.returncode != 0 and 'No space left on device' in overflow.stderr, overflow.stderr
    assert exec_probe('cat', '/probe/sentinel').strip() == 'live3-ext4'
    exec_probe('rm', '-f', '/probe/fill', '/probe/overflow')
    cluster.evidence.record('O27 rawfile provider filesystem probe', status='passed', pod_uid=original,
                            filesystem_before_kib=before, filesystem_after_kib=after,
                            pvc_before='256Mi', pvc_after='512Mi', overflow=overflow.stderr.strip())


def interrupted(signum, _frame):
    if signum == signal.SIGUSR1:
        raise RuntimeError('Docker VM topped the 7 GiB budget')
    raise KeyboardInterrupt(f'session stopped by signal {signum}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--test-executable', type=Path)
    parser.add_argument('--revision', help='Source SHA for the CI release binary')
    parser.add_argument('--node-image', default=NODE_IMAGE)
    parser.add_argument('--base-image', default='python:3.11-slim-trixie')
    parser.add_argument('--load-archive', action='append', type=Path, default=[])
    parser.add_argument('--csi-archive-dir', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    driver = parser.add_mutually_exclusive_group(required=True)
    driver.add_argument('--csi-source', type=Path)
    driver.add_argument('--rawfile-manifest', type=Path)
    parser.add_argument('--lane', required=True, type=Path)
    parser.add_argument('--scenario',
                        choices=('all', 'lifecycle', 'rolling', 'zone', 'storage', 'scale', 'preflight'),
                        default='all')
    args = parser.parse_args()
    if args.scenario != 'preflight' and not args.binary:
        parser.error('--binary is required for OLTP scenarios')
    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGUSR1, signal.SIGALRM):
        signal.signal(signum, interrupted)
    evidence = Evidence(args.output)
    assert Path('/Volumes/ZX20/Docker/vms/0/data/Docker.raw').is_file(), '/Volumes/ZX20 Docker disk missing'
    assert run('docker', 'context', 'show').strip() == 'desktop-linux'
    binary_sha = None
    if args.binary:
        with args.binary.open('rb') as binary:
            binary_sha = hashlib.file_digest(binary, 'sha256').hexdigest()
    test_sha = None
    if args.test_executable:
        with args.test_executable.open('rb') as binary:
            test_sha = hashlib.file_digest(binary, 'sha256').hexdigest()
    evidence.record('host', context='desktop-linux', docker_disk='/Volumes/ZX20/Docker/vms/0/data/Docker.raw',
                    kernel=run('uname', '-a').strip(), docker=run('docker', 'version', '--format', '{{.Server.Version}}').strip(),
                    kind=run('kind', 'version').strip(), source=run('git', 'rev-parse', 'HEAD').strip(),
                    server_source=args.revision, server_sha256=binary_sha, test_executable_sha256=test_sha,
                    scenario=args.scenario, csi='openebs-rawfile' if args.rawfile_manifest else 'hostpath')
    with tempfile.TemporaryDirectory(prefix='nabedra-live3-') as scratch:
        cluster = Kind(Path(scratch), evidence, args.rawfile_manifest)
        with lane(args.lane, evidence):
            signal.alarm(50 * 60)
            stop = threading.Event()
            observer = threading.Thread(target=cluster.watch_memory, args=(stop,))
            observer.start()
            try:
                csi_images = sorted(args.csi_archive_dir.glob('*.tar')) if args.csi_archive_dir else ()
                cluster.start(args.csi_source, args.node_image, args.load_archive, csi_images,
                              preflight=args.scenario == 'preflight',
                              zone_fault=args.scenario in ('all', 'lifecycle', 'zone'))
                if args.scenario == 'preflight':
                    evidence.record('kind CSI preflight', status='passed', zones=cluster.nodes)
                    if cluster.rawfile:
                        preflight_filesystem(cluster, args.base_image)
                    return
                image = cluster.load('oltp', args.binary, args.test_executable, args.base_image)
                oltp(cluster, args.binary, image, args.scenario, args.test_executable)
            except Exception as error:
                evidence.record('session-failed', type=type(error).__name__, message=str(error))
                raise
            finally:
                signal.alarm(0)
                stop.set()
                observer.join(timeout=25)
                try:
                    if cluster.kubeconfig.exists():
                        cluster.diagnostics()
                finally:
                    cluster.close()


if __name__ == '__main__':
    START = time.monotonic()
    main()
