"""Docker inventory and explicitly configured data-only launch slots."""
from copy import copy
from dataclasses import asdict, dataclass
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import socket
import ssl
import tempfile
from types import MappingProxyType

from ..client import _unique_object
from ..providers import ProviderError
from .docker import DockerWorker, PREFIX, digest, identity, private_bytes
from .live import GrowthTarget, HostObservation, ProviderObservation, _worker

_OPERATIONS_PATH = '/v1/cluster/operations'
_GIB = 1 << 30


@dataclass(frozen=True)
class DockerWorkerTemplate:
    """One operator-prepared worker and pool identity on a local Docker host."""

    zone: str
    pool_id: str
    bootstrap_file: str
    mount_seed_file: str
    host_port: int
    cpu_millicores: int
    memory_bytes: int
    max_request_bytes: int
    monthly_cost: int


@dataclass(frozen=True)
class _LaunchInputs:
    bootstrap: dict
    worker: dict
    alias: str
    http_port: int
    admin_token: str
    public_key: str
    mounts: tuple
    fingerprint: str


def _launch_inputs(template):
    path = Path(template.bootstrap_file)
    bootstrap_raw = private_bytes(path, 65536)
    try:
        if path.parent.stat().st_mode & 0o077:
            raise ValueError
        bootstrap = json.loads(bootstrap_raw, object_pairs_hook=_unique_object)
        node = bootstrap['node']
        enrollment = node['intent']['enrollment']
        worker = _worker(enrollment)
        identity(worker['worker_id'])
        identity(worker['incarnation'])
        identity(template.pool_id)
        identity(node['control']['identity']['cluster_id'])
        alias, _, peer_port = enrollment['endpoint'].partition(':')
        http_port = int(bootstrap['bind'].removeprefix('0.0.0.0:'))
        if (set(bootstrap) != {'version', 'bind', 'auth_file', 'node', 'resources', 'execution'}
                or set(node) != {'intent', 'bind', 'certificate', 'private_key', 'authority_ca',
                                 'scheduler_node_id', 'control', 'control_enrollments'}
                or type(bootstrap['version']) is not int or bootstrap['version'] != 1
                or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', alias)
                or enrollment['server_name'] != alias or not re.fullmatch(r'[1-9][0-9]{0,4}', peer_port)
                or not 1 <= int(peer_port) <= 65535 or node['bind'] != '0.0.0.0:' + peer_port
                or bootstrap['bind'] != f'0.0.0.0:{http_port}' or not 1 <= http_port <= 65535
                or http_port == int(peer_port)
                or not re.fullmatch(r'[a-f0-9]{64}', worker['certificate_sha256'])
                or type(worker['enrollment_revision']) is not int or worker['enrollment_revision'] != 1
                or set(node['intent']) != {'enrollment'}
                or type(node['scheduler_node_id']) is not int or not 0 < node['scheduler_node_id'] < 2**64):
            raise ValueError
        resources = bootstrap['resources']
        if (type(resources['max_threads']) is not int
                or not 1 <= resources['max_threads'] * 1000 <= template.cpu_millicores
                or type(resources['memory_limit_bytes']) is not int
                or not 1 <= resources['memory_limit_bytes'] <= template.memory_bytes):
            raise ValueError
        destinations = (bootstrap['auth_file'], node['certificate'], node['private_key'], node['authority_ca'])
        if len(set(destinations)) != 4 or any(
                not re.fullmatch(r'/config/[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}', entry) for entry in destinations):
            raise ValueError
        hosts = [path.parent / Path(entry).name for entry in destinations]
        if path in hosts or Path(template.mount_seed_file) in hosts:
            raise ValueError
        inputs = [private_bytes(file, limit) for file, limit in zip(hosts, (16384, 65536, 65536, 65536))]
        auth = json.loads(inputs[0], object_pairs_hook=_unique_object)
        if set(auth) != {'admin_token', 'data_token'} or any(
                not isinstance(token, str) or not re.fullmatch(r'[!-~]{43,4096}', token)
                for token in auth.values()) or auth['admin_token'] == auth['data_token']:
            raise ValueError
        pem = re.search(rb'-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----', inputs[1], re.S)
        der = ssl.PEM_cert_to_DER_cert(pem.group().decode('ascii'))
        if hashlib.sha256(der).hexdigest() != worker['certificate_sha256']:
            raise ValueError
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_cert_chain(hosts[1], hosts[2], password=lambda: b'')
        context.load_verify_locations(cadata=inputs[3].decode('ascii'))
        seed = private_bytes(template.mount_seed_file, 32)
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
        public = Ed25519PrivateKey.from_private_bytes(seed).public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    except (AttributeError, ImportError, KeyError, OSError, TypeError, ValueError):
        raise ProviderError('Docker data worker template has invalid private launch configuration') from None
    fingerprint = digest([asdict(template), *[hashlib.sha256(value).hexdigest()
                                              for value in (bootstrap_raw, *inputs, seed)]])
    mounts = tuple((str(source), target) for source, target in zip(hosts, destinations))
    return _LaunchInputs(bootstrap, worker, alias, http_port, auth['admin_token'], public.hex(), mounts, fingerprint)


def _write_bootstrap(template, claim_id, bootstrap):
    destination = Path(template.bootstrap_file).with_name('launch-' + claim_id + '.json')
    body = json.dumps(bootstrap, sort_keys=True, separators=(',', ':')).encode()
    if len(body) > 65536:
        raise ProviderError('Docker launch bootstrap exceeds the worker limit')
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent) as temporary:
            temporary.write(body)
            temporary.flush()
            os.fsync(temporary.fileno())
            try:
                os.link(temporary.name, destination)
            except FileExistsError:
                pass
        if private_bytes(destination, 65536) != body:
            raise ProviderError('Docker launch bootstrap differs from the retained claim')
        fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        raise ProviderError('Cannot retain the private Docker launch bootstrap') from None
    return str(destination)


def _port_available(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(('127.0.0.1', port))
        return True
    except OSError:
        return False


def _price(policy, size):
    return policy['worker_cost'] + ((size + _GIB - 1) // _GIB) * policy['volume_cost_per_gib']


class DockerGrowthAdapter:
    """Observe exact Docker worker/mount bindings and retained containers.

    Named volumes lack an expandable size and can share a backing filesystem.
    A live container's mount set is immutable. This adapter therefore reports
    zero automatic attachment slots. Worker launch requires a configured
    template with an exact authenticated loopback endpoint.
    """

    def __init__(self, *, provider, adapter_id, workers, monthly_spend_floor, templates=()):
        self.provider = provider
        self.cluster_id = identity(provider.cluster_id)
        self.adapter_id = identity(adapter_id)
        self.workers = MappingProxyType({identity(key): resource for key, resource in workers.items()})
        if type(monthly_spend_floor) is not int or not 0 <= monthly_spend_floor < 2**64:
            raise ProviderError('Explicit retained Docker resource costs are required')
        self.monthly_spend_floor = monthly_spend_floor
        self._slots = {}
        self._workers_by_slot = {}
        self._fingerprints = {}
        self._endpoints = {}
        pool_ids, aliases, ports, scheduler_ids = set(), set(), set(), set()
        for template in templates:
            if (type(template) is not DockerWorkerTemplate or len(self._slots) >= 16
                    or not isinstance(template.zone, str) or not 0 < len(template.zone) <= 128
                    or template.zone in self._slots
                    or type(template.cpu_millicores) is not int or not 1 <= template.cpu_millicores <= 64000
                    or type(template.memory_bytes) is not int or not 64 << 20 <= template.memory_bytes <= 1 << 40
                    or type(template.host_port) is not int or not 1 <= template.host_port <= 65535
                    or type(template.max_request_bytes) is not int or not 0 < template.max_request_bytes < 2**64
                    or type(template.monthly_cost) is not int or not 0 <= template.monthly_cost < 2**64
                    or any(not isinstance(path, str) or ',' in path or '\0' in path
                           for path in (template.bootstrap_file, template.mount_seed_file))
                    or not re.fullmatch(r'sha256:[a-f0-9]{64}', self.provider.image)):
                raise ProviderError('Invalid bounded Docker data worker template')
            inputs = _launch_inputs(template)
            worker_id = inputs.worker['worker_id']
            scheduler_id = inputs.bootstrap['node']['scheduler_node_id']
            control_id = inputs.bootstrap['node']['control']['identity']['cluster_id']
            if (worker_id in self.workers or worker_id in self._workers_by_slot
                    or template.pool_id in pool_ids or inputs.alias in aliases or template.host_port in ports
                    or scheduler_id in scheduler_ids or control_id != self.cluster_id):
                raise ProviderError('Docker launch template reuses an identity, endpoint or slot')
            self._slots[template.zone] = template
            self._workers_by_slot[worker_id] = template
            self._fingerprints[template.zone] = self._fingerprint(inputs)
            pool_ids.add(template.pool_id)
            aliases.add(inputs.alias)
            ports.add(template.host_port)
            scheduler_ids.add(scheduler_id)

    def _fingerprint(self, inputs):
        return digest((self.cluster_id, self.adapter_id, self.provider.image, self.provider.network, inputs.fingerprint))

    def _inputs(self, template):
        inputs = _launch_inputs(template)
        if self._fingerprint(inputs) != self._fingerprints[template.zone]:
            raise ProviderError('Protected Docker launch template changed')
        return inputs

    def bind_endpoints(self, endpoints):
        bound = copy(self)
        matched = {}
        for worker_id, template in self._workers_by_slot.items():
            endpoint = endpoints.get(worker_id)
            if endpoint is None:
                continue
            url = f'http://127.0.0.1:{template.host_port}' + _OPERATIONS_PATH
            token = self._inputs(template).admin_token
            if (endpoint._transport._url == url
                    and hmac.compare_digest(endpoint._transport._token, token)):
                matched[worker_id] = endpoint
        bound._endpoints = MappingProxyType(matched)
        return bound

    def observe(self, inventories, *, policy, deadline, claim_id=None):
        hosts = {}
        observed_ids = set()
        retained = self.provider.worker_ids(deadline=deadline)
        launch_records = {}
        if self._slots:
            for container in retained - {item.removeprefix('docker-container:') for item in self.workers.values()}:
                record = self.provider.inspect_worker('docker-container:' + container, deadline=deadline)
                worker_id = record['Config']['Labels'].get(PREFIX + 'worker')
                if worker_id in self._workers_by_slot:
                    if worker_id in launch_records:
                        raise ProviderError('Several Docker containers use one launch slot')
                    launch_records[worker_id] = record
        for worker_id, sample in inventories.items():
            worker = sample['inventory']['worker']
            if worker_id in self.workers:
                record = self.provider.inspect_worker(self.workers[worker_id], deadline=deadline)
            else:
                record = launch_records.get(worker_id)
                template = self._workers_by_slot.get(worker_id)
                if record is None or template is None:
                    raise ProviderError('Ready worker has no exact Docker container binding')
                self._check_launched_binding(template, worker, record, sample)
            labels = record['Config']['Labels']
            container = record['Id']
            zone = labels.get(PREFIX + 'zone')
            if (container not in retained or container in observed_ids
                    or record['State']['Running'] is not True or not zone
                    or labels.get(PREFIX + 'worker') != worker_id
                    or labels.get(PREFIX + 'incarnation') != worker['incarnation']):
                raise ProviderError('Docker provider inventory differs from the current worker')
            mounts = {(mount['Name'], mount['Destination']) for mount in record.get('Mounts', [])
                      if mount['Type'] == 'volume' and mount['RW']}
            for row in sample['inventory']['pools']:
                expected = row['expected']
                resource = expected['provider_volume_id']
                volume = resource.removeprefix('docker-volume:')
                if resource == volume or (volume, expected['mount_path']) not in mounts:
                    raise ProviderError('Docker container no longer mounts the exact observed volume')
            resources = record['HostConfig']
            cpu, memory = resources.get('NanoCpus'), resources.get('Memory')
            if type(cpu) is not int or cpu < 10**6 or type(memory) is not int or memory <= 0:
                raise ProviderError('Docker worker requires finite enforced CPU and memory')
            hosts[worker_id] = HostObservation(worker, zone, 0, cpu_limit_millicores=cpu // 10**6,
                                              memory_limit_bytes=memory)
            observed_ids.add(container)
        zones = set()
        # A co-located named volume cannot satisfy an exact cross-worker split deficit.
        if claim_id is None and any('split_deficit' in sample for sample in inventories.values()):
            return ProviderObservation(hosts, len(retained - observed_ids), self.monthly_spend_floor)
        for worker_id, template in self._workers_by_slot.items():
            if (worker_id not in self._endpoints or worker_id in inventories or worker_id in launch_records
                    or template.zone not in policy['zones']):
                continue
            size = policy['automatic']['pool_increment_bytes']
            if (size > template.max_request_bytes or _price(policy, size) < template.monthly_cost
                    or not _port_available(template.host_port)):
                continue
            self._inputs(template)
            current_volumes = self.provider.worker_pool_names(worker_id, deadline=deadline)
            if current_volumes and (claim_id is None
                                    or current_volumes != {self.provider.pool_name(claim_id)}):
                continue
            zones.add(template.zone)
        if zones:
            self.provider.inspect_launch_environment(deadline=deadline)
        return ProviderObservation(hosts, len(retained - observed_ids), self.monthly_spend_floor, frozenset(zones))

    def _check_launched_binding(self, template, worker, record, sample):
        inputs = self._inputs(template)
        labels = record['Config']['Labels']
        claim_id = identity(labels.get(PREFIX + 'claim'))
        primary = [entry for entry in sample['inventory']['pools'] if entry['primary']]
        configured = record['HostConfig']
        if (worker != inputs.worker or labels.get(PREFIX + 'zone') != template.zone
                or labels.get(PREFIX + 'certificate') != worker['certificate_sha256']
                or labels.get(PREFIX + 'enrollment-revision') != str(worker['enrollment_revision'])
                or record.get('Name') != '/nabedra-worker-' + claim_id
                or record.get('Image') != self.provider.image
                or configured.get('NanoCpus') != template.cpu_millicores * 1000000
                or configured.get('Memory') != template.memory_bytes
                or len(primary) != 1 or primary[0]['pool_id'] != template.pool_id
                or primary[0]['growth_claim'] != claim_id
                or primary[0]['expected']['provider_volume_id'] != 'docker-volume:' + self.provider.pool_name(claim_id)
                or inputs.alias not in (record.get('NetworkSettings', {}).get('Networks', {})
                                        .get(self.provider.network, {}).get('Aliases') or ())):
            raise ProviderError('Docker data worker does not match the enrolled launch slot')

    def target(self, claim_id, action, inventories):
        template = self._slots.get(action.get('zone'))
        if (action['kind'] != 'add_worker' or template is None
                or not 0 < action['bytes'] <= template.max_request_bytes):
            return None
        worker = self._inputs(template).worker
        worker_id = worker['worker_id']
        if worker_id in inventories or worker_id not in self._endpoints:
            return None
        return GrowthTarget(worker, template.pool_id, 1, 'docker-volume:' + self.provider.pool_name(claim_id))

    def _volume(self, intent, target, *, existing_only, deadline):
        template = self._slots.get(intent['action'].get('zone'))
        if (template is None or self.target(intent['claim_id'], intent['action'], {}) != target
                or (intent['worker'], intent['pool_id'], intent['pool_generation'])
                != (target.worker, target.pool_id, target.pool_generation)
                or not template.monthly_cost <= intent['quoted_monthly_cost'] <= intent['monthly_spend_ceiling']):
            raise ProviderError('Docker launch template differs from the exact priced claim')
        observed = self.provider.ensure_pool(claim_id=intent['claim_id'], pool_id=target.pool_id,
            worker_id=target.worker['worker_id'], zone=template.zone, mount_seed=template.mount_seed_file,
            template_digest=self._fingerprints[template.zone], existing_only=existing_only, deadline=deadline)
        if (observed['resource_id'] != target.resource_id or observed['volume_marker'] != target.pool_id
                or observed['observed_free_bytes'] < intent['action']['bytes']):
            raise ProviderError('Docker retained volume identity or shared filesystem headroom differs')
        return template, observed

    def provision(self, intent, target, *, deadline):
        _, observed = self._volume(intent, target, existing_only=False, deadline=deadline)
        return observed

    def register(self, intent, target, record, *, controller, deadline):
        raise ProviderError('Live Docker filesystem provisioning is unavailable')

    def register_worker(self, intent, target, record, *, controller, pin, deadline):
        claim_id = intent['claim_id']
        before = controller._read('storage.growth.snapshot', {}, deadline=deadline)
        if (before['pin'] != pin or before['cluster_id'] != self.cluster_id or not before['active_automatic']
                or before['policy'].get('automatic', {}).get('adapter_id') != self.adapter_id
                or before['active'] != record or record['intent'] != intent
                or record['phase'] != 'awaiting_mount' or record['provider_resource'] != target.resource_id):
            raise ProviderError('The current Control claim does not authorize a Docker launch')
        template, volume = self._volume(intent, target, existing_only=True, deadline=deadline)
        inputs = self._inputs(template)
        bootstrap, worker = inputs.bootstrap, inputs.worker
        enrollment = bootstrap['node']['intent']['enrollment']
        run_id = identity(before['pin']['ready']['view']['run_id'])
        mount = dict(profile={'docker': {'volume_marker': target.pool_id}},
                     identity=dict(engine='oltp', pool_format_version=1, cluster_id=self.cluster_id,
                                   worker_id=worker['worker_id'], pool_id=target.pool_id),
                     worker=worker, generation=target.pool_generation, mount_path='/data',
                     provider_volume_id=target.resource_id, filesystem_uuid='')
        preparation = dict(claim_id=claim_id, run_id=run_id, enrollment=enrollment,
                           mount=mount, mount_public_key=inputs.public_key)
        bootstrap['node']['intent'] = preparation
        config = _write_bootstrap(template, claim_id, bootstrap)
        launch = DockerWorker(worker['worker_id'], worker['incarnation'], config, inputs.alias,
                              template.cpu_millicores, template.memory_bytes, ((volume['volume_name'], '/data'),),
                              zone=template.zone, http_ports=(template.host_port, inputs.http_port))
        options = dict(claim_id=claim_id, worker=launch, additional_mounts=inputs.mounts, deadline=deadline)
        existing = self.provider.ensure_worker(**options, inspect_only=True)
        if existing is not None and existing['state'] != 'created':
            return self._launch_result(existing)
        if not _port_available(template.host_port):
            raise ProviderError('Docker data worker loopback port is occupied')
        if controller._read('storage.growth.snapshot', {}, deadline=deadline) != before:
            raise ProviderError('Control changed before Docker data worker preparation')
        prepared = controller._operation(claim_id, 'prepare-worker', 'storage.growth.prepare_worker',
                                         preparation, deadline=deadline)
        after = controller._read('storage.growth.snapshot', {}, deadline=deadline)
        if (prepared != {'identity': worker} or after['active'] != record or not after['active_automatic']
                or after['cluster_id'] != self.cluster_id or after['policy'] != before['policy']
                or any(after['pin'][key] != value for key, value in before['pin'].items()
                       if key != 'enrollment_revision')
                or after['pin']['enrollment_revision'] not in (before['pin']['enrollment_revision'],
                                                               before['pin']['enrollment_revision'] + 1)):
            raise ProviderError('Control changed before Docker data worker launch')
        return self._launch_result(self.provider.ensure_worker(**options))

    @staticmethod
    def _launch_result(observed):
        if observed['state'] != 'running':
            raise ProviderError('Docker data worker did not stay running; its resources were retained')
        return {'state': 'launched' if observed['started'] else 'awaiting_ready',
                'container_id': observed['resource_id']}
