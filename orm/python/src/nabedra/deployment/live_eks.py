"""PVC expansion and bounded worker staging through exact EKS inventory."""
from copy import copy
from dataclasses import asdict, dataclass
import hmac
import json
from pathlib import Path
import re
from types import MappingProxyType
from urllib.parse import urlsplit

from ..client import _unique_object
from ..providers import ProviderError
from .cloud_control import _native_target, ec2_worker_tags
from .docker import digest, identity, private_bytes
from .eks import _ZONE
from .live import GrowthTarget, HostObservation, ProviderObservation, _worker as _enrolled_worker

_GIB = 1 << 30
_ADMIN_PATH = '/v1/cluster/operations'


@dataclass(frozen=True)
class EksWorkerTemplate:
    """One protected data-worker enrollment and retained PVC slot in a zone."""

    zone: str
    pool_id: str
    mount_path: str
    cpu_millicores: int
    memory_bytes: int
    worker_monthly_cost: int
    max_request_bytes: int
    filesystem_reserve_bytes: int
    mount_public_key: str
    enrollment_file: str
    admin_url: str
    admin_token_file: str
    handoff_directory: str


def _private_path(path):
    try:
        return (path.is_absolute() and path.resolve(strict=True) == path and path.is_dir()
                and path.stat().st_mode & 0o077 == 0)
    except (OSError, RuntimeError):
        return False


def _private_input(filename, maximum):
    path = Path(filename)
    if not _private_path(path.parent):
        raise ProviderError('EKS staging inputs require an isolated private directory')
    try:
        if path.stat().st_uid == path.parent.stat().st_uid:
            return private_bytes(path, maximum)
    except (OSError, ProviderError):
        pass
    raise ProviderError('EKS staging input must be a bounded private regular file')


def _inputs(template, namespace):
    try:
        enrollment = json.loads(_private_input(template.enrollment_file, 4096), object_pairs_hook=_unique_object)
        token = _private_input(template.admin_token_file, 4097).decode('ascii').removesuffix('\n')
        ec2_worker_tags(enrollment)
        name = 'nabedra-worker-' + enrollment['worker_id']
        peer = f'{name}-0.{name}.{namespace}.svc.'
        admin = urlsplit(template.admin_url)
        hostname = admin.hostname
        if (set(enrollment) != {'worker_id', 'incarnation', 'enrollment_revision', 'certificate_sha256',
                               'endpoint', 'server_name'} or enrollment['enrollment_revision'] != 1
                or not enrollment['server_name'].startswith(peer)
                or not re.fullmatch(r'[a-z0-9-]+(?:\.[a-z0-9-]+)*', enrollment['server_name'][len(peer):])
                or len(enrollment['server_name']) > 253 or enrollment['endpoint'] != enrollment['server_name'] + ':9000'
                or not hostname or not re.fullmatch(r'[a-z0-9-]+(?:\.[a-z0-9-]+)+', hostname)
                or template.admin_url != f'https://{hostname}' + (f':{admin.port}' if admin.port is not None else '')
                or not 43 <= len(token) <= 4096 or not re.fullmatch(r'[A-Za-z0-9._~+/-]+=*', token)
                or template.enrollment_file == template.admin_token_file):
            raise ValueError
    except (AttributeError, KeyError, TypeError, ValueError):
        raise ProviderError('EKS staging requires exact worker enrollment and protected admin credentials') from None
    return enrollment, token


class EksGrowthAdapter:
    """Grow exact fixed PVCs and reconcile opt-in data-worker launch slots."""

    def __init__(self, *, provider, adapter_id, workers, pvcs, max_volume_gib, templates=()):
        if type(max_volume_gib) is not int or not 1 <= max_volume_gib <= 65536:
            raise ProviderError('Explicit EKS volume ceiling is required')
        self.provider = provider
        self.cluster_id = identity(provider.cluster_id)
        self.adapter_id = identity(adapter_id)
        self.workers = MappingProxyType({identity(key): tuple(binding) for key, binding in workers.items()})
        if any(len(binding) != 2 or not all(isinstance(value, str) and value for value in binding)
               for binding in self.workers.values()):
            raise ProviderError('EKS workers require exact StatefulSet identity and Pod UID')
        self.pvcs = MappingProxyType(dict(pvcs))
        if len(set(pvcs.values())) != len(pvcs):
            raise ProviderError('EKS growth PVC bindings must be distinct')
        self.max_volume_bytes = max_volume_gib * _GIB
        self._templates, self._slots, self._fingerprints = {}, {}, {}
        self._endpoints = MappingProxyType({})
        pools, sources = set(), set()
        identities = [set() for _ in range(4)]
        for template in templates:
            if (type(template) is not EksWorkerTemplate or len(self._slots) >= 64
                    or provider._staging_ec2 is None or not re.fullmatch(r'\S+@sha256:[a-f0-9]{64}', provider._image)
                    or not isinstance(template.zone, str)
                    or not re.fullmatch(re.escape(provider._staging_region) + r'[a-z]', template.zone)
                    or template.zone in self._templates or identity(template.pool_id) in pools
                    or any(provider._resource_parts(binding)[1] == 'nabedra-pool-' + template.pool_id
                           for binding in self.pvcs.values())
                    or not isinstance(template.mount_path, str) or len(template.mount_path.encode()) > 1024
                    or not re.fullmatch(r'(?:/[A-Za-z0-9_][A-Za-z0-9_.-]*)+', template.mount_path)
                    or template.mount_path.split('/')[1] in ('dev', 'proc', 'sys', 'nabedra')
                    or type(template.cpu_millicores) is not int or not 1000 <= template.cpu_millicores < 2**32
                    or type(template.memory_bytes) is not int or not 0 < template.memory_bytes < 2**63
                    or type(template.worker_monthly_cost) is not int or not 0 < template.worker_monthly_cost < 2**64
                    or type(template.max_request_bytes) is not int
                    or not 0 < template.max_request_bytes <= self.max_volume_bytes
                    or type(template.filesystem_reserve_bytes) is not int or not 0 < template.filesystem_reserve_bytes < _GIB
                    or not isinstance(template.mount_public_key, str)
                    or not re.fullmatch(r'[a-f0-9]{64}', template.mount_public_key)
                    or template.mount_public_key == '0' * 64 or not isinstance(template.handoff_directory, str)
                    or not _private_path(Path(template.handoff_directory))):
                raise ProviderError('EKS requires a bounded protected worker template with a distinct zone and pool')
            enrollment, token = _inputs(template, provider.namespace)
            worker_id = enrollment['worker_id']
            inputs = {template.enrollment_file, template.admin_token_file}
            values = (enrollment['incarnation'], enrollment['certificate_sha256'], template.admin_url,
                      template.mount_public_key)
            if worker_id in self.workers or worker_id in self._slots or sources & inputs or any(
                    value in known for value, known in zip(values, identities)):
                raise ProviderError('EKS staging requires distinct enrollment, keys and private inputs')
            self._templates[template.zone] = template
            self._slots[worker_id] = template
            self._fingerprints[worker_id] = self._fingerprint(template, enrollment, token)
            pools.add(template.pool_id)
            sources.update(inputs)
            for value, known in zip(values, identities):
                known.add(value)

    def _fingerprint(self, template, enrollment, token):
        spec = asdict(template)
        for key in ('enrollment_file', 'admin_token_file', 'handoff_directory'):
            del spec[key]
        return digest([self.cluster_id, self.adapter_id, self.provider._origin, self.provider.namespace,
                       self.provider.storage_class, self.provider._image, spec, enrollment, digest(token)])

    def _inputs(self, worker_id, template):
        enrollment, token = _inputs(template, self.provider.namespace)
        if self._fingerprint(template, enrollment, token) != self._fingerprints[worker_id]:
            raise ProviderError('Protected EKS launch inputs changed')
        return enrollment, token

    def bind_endpoints(self, endpoints):
        bound = copy(self)
        matched = {}
        urls = [endpoint._transport._url for endpoint in endpoints.values()]
        for worker_id, template in self._slots.items():
            endpoint = endpoints.get(worker_id)
            if endpoint is not None:
                _, token = self._inputs(worker_id, template)
                transport = endpoint._transport
                url = template.admin_url + _ADMIN_PATH
                if transport._url == url and urls.count(url) == 1 and hmac.compare_digest(transport._token, token):
                    matched[worker_id] = endpoint
        bound._endpoints = MappingProxyType(matched)
        return bound

    def _valid_size(self, template, requested):
        return (type(requested) is int and 0 < requested <= template.max_request_bytes
                and self._gib(requested) * _GIB - requested >= template.filesystem_reserve_bytes)

    @staticmethod
    def _gib(size):
        return (size + _GIB - 1) // _GIB

    @staticmethod
    def _slot_record(records, worker_id, name, claim_id=None):
        matches = []
        for record in records.values():
            metadata = record['metadata']
            labels = metadata['labels']
            if (metadata.get('name') == name or labels.get('nabedra.io/worker') == worker_id
                    or (claim_id is not None and labels.get('nabedra.io/claim') == claim_id)):
                matches.append(record)
        if len(matches) > 1:
            raise ProviderError('Several EKS resources occupy one protected worker or pool slot')
        return matches[0] if matches else None

    def _staged_pvc(self, template, worker_id, claim_id, size, *, existing_only, authorize=None):
        return self.provider.ensure_pool(claim_id=claim_id, pool_id=template.pool_id, worker_id=worker_id,
                                         zone=template.zone, bytes=size, _staging=self._fingerprints[worker_id],
                                         _authorize=authorize, _existing_only=existing_only)

    def _staged_storage(self, resource, template, worker_id, claim_id, size, *, expected=None):
        provider = self.provider
        _, pvc = provider._pvc(resource, worker_id=worker_id, zone=template.zone, pool_id=template.pool_id)
        meta = pvc['metadata']
        capacity = pvc.get('status', {}).get('capacity', {}).get('storage')
        if (meta['labels'].get('nabedra.io/claim') != claim_id or capacity is None
                or provider._quantity_bytes(pvc['spec']['resources']['requests']['storage']) != size
                or not size <= provider._quantity_bytes(capacity) <= self._gib(size) * _GIB):
            raise ProviderError('Bound EKS staging PVC capacity or claim differs')
        pv = provider._bound_volume(pvc, zone=template.zone, expected=expected)
        volume_id = pv['spec']['csi']['volumeHandle']
        identity(meta['uid'])
        identity(pv['metadata']['uid'])
        return pvc, pv, volume_id

    def _handoff(self, template, claim_id, pvc, pv, volume_id):
        path = Path(template.handoff_directory) / (identity(claim_id) + '.json')
        if not path.exists() and not path.is_symlink():
            return None
        try:
            handoff = json.loads(_private_input(path, 4096), object_pairs_hook=_unique_object)
            required = {'filesystem_uuid', 'pvc_uid', 'pv_uid', 'volume_id'}
            complete = required | {'statefulset_uid', 'pod_uid'}
            if (set(handoff) not in (required, complete) or handoff['volume_id'] != volume_id
                    or handoff['pvc_uid'] != pvc['metadata']['uid'] or handoff['pv_uid'] != pv['metadata']['uid']):
                raise ValueError
            for key in set(handoff) - {'volume_id'}:
                identity(handoff[key])
            return handoff
        except (KeyError, TypeError, ValueError):
            raise ProviderError('Protected EKS handoff differs from the exact claim storage or Pod') from None

    @staticmethod
    def _secret_name(claim_id):
        return 'nabedra-bootstrap-' + claim_id

    def _staged_worker(self, template, enrollment, claim_id, resource, handoff, *, existing_only, authorize=None):
        worker_id = enrollment['worker_id']
        return self.provider._ensure_worker(claim_id=claim_id, worker_id=worker_id, incarnation=enrollment['incarnation'],
            zone=template.zone, bootstrap_secret=self._secret_name(claim_id), enrollment=enrollment, replacement=None,
            pools=[dict(resource_id=resource, mount_path=template.mount_path, filesystem_uuid=handoff['filesystem_uuid'])],
            cpu_millicores=template.cpu_millicores, memory_bytes=template.memory_bytes,
            staging=self._fingerprints[worker_id], authorize=authorize, existing_only=existing_only)

    def _native_target(self, handoff):
        if 'pod_uid' not in handoff:
            return None
        return _native_target({'kind': 'eks', 'namespace': self.provider.namespace,
                **{key: handoff[key] for key in ('pod_uid', 'statefulset_uid', 'pvc_uid', 'pv_uid')}})

    def _records(self, template, worker_id, stateful, pvcs, *, claim_id=None):
        compute = self._slot_record(stateful, worker_id, 'nabedra-worker-' + worker_id, claim_id)
        storage = self._slot_record(pvcs, worker_id, 'nabedra-pool-' + template.pool_id, claim_id)
        return compute, storage

    def _spend(self, stateful, pvcs, policy, *, reserved_worker=None, reserved_pvc=None):
        gib = 0
        for uid, pvc in pvcs.items():
            if uid == reserved_pvc:
                continue
            requested = self.provider._quantity_bytes(pvc['spec']['resources']['requests']['storage'])
            capacity = pvc.get('status', {}).get('capacity', {}).get('storage')
            size = max(requested, self.provider._quantity_bytes(capacity)) if capacity else requested
            gib += self._gib(size)
        return ((len(stateful) - int(reserved_worker in stateful)) * policy['worker_cost']
                + gib * policy['volume_cost_per_gib'])

    def _ready_staged(self, worker_id, inventory, stateful, pvcs):
        worker = inventory['worker']
        template = self._slots.get(worker_id)
        if template is None or worker_id not in self._endpoints or len(inventory['pools']) != 1:
            raise ProviderError('Ready EKS worker has no protected launch slot and native pool')
        enrollment, _ = self._inputs(worker_id, template)
        row = inventory['pools'][0]
        claim_id = identity(row['growth_claim'])
        compute, storage = self._records(template, worker_id, stateful, pvcs, claim_id=claim_id)
        if compute is None or storage is None or _enrolled_worker(enrollment) != worker:
            raise ProviderError('Ready EKS worker no longer matches the enrolled slot')
        size = self.provider._quantity_bytes(storage['spec']['resources']['requests']['storage'])
        if not self._valid_size(template, size):
            raise ProviderError('Ready EKS staging PVC changed its priced size')
        resource = self._staged_pvc(template, worker_id, claim_id, size, existing_only=True)['resource_id']
        pvc, pv, volume_id = self._staged_storage(resource, template, worker_id, claim_id, size,
                                                  expected=row['expected']['provider_volume_id'])
        handoff = self._handoff(template, claim_id, pvc, pv, volume_id)
        if (handoff is None or 'pod_uid' not in handoff or row['pool_id'] != template.pool_id or row['primary'] is not True
                or row['expected'] != self._mount(template, worker, volume_id, handoff['filesystem_uuid'])
                or not size <= row['observed_total_bytes'] <= self._gib(size) * _GIB
                or pvc['metadata']['uid'] != storage['metadata']['uid']):
            raise ProviderError('Ready EKS worker differs from its pinned mount and handoff')
        staged = self._staged_worker(template, enrollment, claim_id, resource, handoff, existing_only=True)
        if (staged['stateful']['metadata']['uid'] != compute['metadata']['uid']
                or self.provider._staged_pod(staged, zone=template.zone, pvc=pvc, pv=pv, size_gib=self._gib(size),
                                             expected=self._native_target(handoff), ready=True) is None):
            raise ProviderError('Ready EKS worker no longer owns its exact Pod and EBS attachment')
        return template.zone, compute['metadata']['uid']

    def observe(self, inventories, *, policy, deadline, claim_id=None):
        with self.provider._limit(deadline):
            stateful, pvcs = self.provider._retained_inventory()
            hosts, ready_uids = {}, set()
            for worker_id, sample in inventories.items():
                worker = sample['inventory']['worker']
                if worker_id not in self.workers:
                    zone, uid = self._ready_staged(worker_id, sample['inventory'], stateful, pvcs)
                    if uid in ready_uids:
                        raise ProviderError('Ready EKS worker shares a retained StatefulSet')
                    ready_uids.add(uid)
                    template = self._slots[worker_id]
                    hosts[worker_id] = HostObservation(worker, zone, 0, cpu_limit_millicores=template.cpu_millicores,
                                                       memory_limit_bytes=template.memory_bytes)
                    continue
                zone, owner, volumes, mounts = self._worker(worker)
                uid = owner['metadata']['uid']
                if uid not in stateful or uid in ready_uids:
                    raise ProviderError('Ready EKS worker is missing from retained inventory')
                ready_uids.add(uid)
                expandable = set()
                for row in sample['inventory']['pools']:
                    expected = row['expected']
                    resource = expected['provider_volume_id']
                    pvc = self._mounted_pvc(expected, worker, zone, volumes, mounts)
                    if pvc['metadata']['uid'] not in pvcs:
                        raise ProviderError('Mounted EKS PVC is missing from retained inventory')
                    capacity = self.provider._quantity_bytes(pvc['status']['capacity']['storage'])
                    if row['observed_total_bytes'] > capacity:
                        raise ProviderError('Mounted bytes exceed observed EKS volume capacity')
                    if row['observed_total_bytes'] < self.max_volume_bytes:
                        expandable.add(resource)
                hosts[worker_id] = HostObservation(worker, zone, 0, frozenset(expandable))
            zones, reserved_pvc = set(), None
            for worker_id, template in self._slots.items():
                if (worker_id not in self._endpoints or worker_id in inventories or template.zone not in policy['zones']
                        or template.worker_monthly_cost > policy['worker_cost']):
                    continue
                self._inputs(worker_id, template)
                compute, storage = self._records(template, worker_id, stateful, pvcs)
                if compute is not None:
                    continue
                if storage is not None:
                    meta = storage['metadata']
                    if claim_id is None or meta['labels'].get('nabedra.io/claim') != claim_id:
                        continue
                    size = self.provider._quantity_bytes(storage['spec']['resources']['requests']['storage'])
                    exact = self._staged_pvc(template, worker_id, claim_id, size, existing_only=True)['resource_id']
                    if (reserved_pvc is not None or not self._valid_size(template, size)
                            or self.provider._resource_parts(exact)[2] != meta['uid']):
                        raise ProviderError('Reserved EKS PVC differs from its exact priced slot')
                    capacity = storage.get('status', {}).get('capacity', {}).get('storage')
                    if capacity is not None and self.provider._quantity_bytes(capacity) > self._gib(size) * _GIB:
                        raise ProviderError('Reserved EKS capacity exceeds its priced slot')
                    reserved_pvc = meta['uid']
                zones.add(template.zone)

            floor = self._spend(stateful, pvcs, policy, reserved_pvc=reserved_pvc)
        return ProviderObservation(hosts, len(set(stateful) - ready_uids), floor, frozenset(zones))

    def target(self, claim_id, action, inventories):
        if action['kind'] == 'add_worker':
            template = self._templates.get(action['zone'])
            if template is None or not self._valid_size(template, action['bytes']):
                return None
            for worker_id, slot in self._slots.items():
                if slot is template and worker_id in self._endpoints and worker_id not in inventories:
                    enrollment, _ = self._inputs(worker_id, template)
                    return GrowthTarget(_enrolled_worker(enrollment), template.pool_id, 1)
            return None
        if action['kind'] != 'expand' or action['target_bytes'] > self.max_volume_bytes:
            return None
        inventory = inventories[action['worker']]['inventory']
        for row in inventory['pools']:
            if row['pool_id'] == action['pool'] and row['expected'].get('profile') == 'ebs':
                expected = row['expected']
                resource = expected['provider_volume_id']
                if resource in self.pvcs:
                    return GrowthTarget(inventory['worker'], row['pool_id'], expected['generation'], resource)
        return None

    def _worker_intent(self, intent, target):
        claim_id = identity(intent['claim_id'])
        action = intent['action']
        worker_id = target.worker['worker_id']
        template = self._slots.get(worker_id)
        if (action['kind'] != 'add_worker' or template is None or self.target(claim_id, action, {}) != target
                or (intent['worker'], intent['pool_id'], intent['pool_generation'])
                    != (target.worker, target.pool_id, target.pool_generation)
                or not template.worker_monthly_cost <= intent['quoted_monthly_cost'] <= intent['monthly_spend_ceiling']):
            raise ProviderError('EKS staging target differs from the exact priced worker claim')
        enrollment, _ = self._inputs(worker_id, template)
        return template, enrollment, action['bytes']

    def provision_worker(self, intent, target, *, authorize, deadline):
        template, enrollment, size = self._worker_intent(intent, target)
        worker_id, claim_id = enrollment['worker_id'], intent['claim_id']
        with self.provider._limit(deadline):
            stateful, pvcs = self.provider._retained_inventory()
            compute, retained = self._records(template, worker_id, stateful, pvcs, claim_id=claim_id)
            if compute is not None:
                raise ProviderError('EKS compute exists before this claim proved physical storage')
            staged = self._staged_pvc(template, worker_id, claim_id, size, existing_only=retained is not None,
                                      authorize=authorize)
            resource = staged['resource_id']
            if retained is not None and self.provider._resource_parts(resource)[2] != retained['metadata']['uid']:
                raise ProviderError('EKS staging PVC changed during observation')
            if staged['state'] != 'Bound':
                raise ProviderError('EKS WaitForFirstConsumer PVC is awaiting the protected storage handoff')
            _, _, volume_id = self._staged_storage(resource, template, worker_id, claim_id, size)
            volume = self.provider._staged_ebs(volume_id, template.zone, self._gib(size))
            if volume['State'] != 'available' or volume['Attachments']:
                raise ProviderError('EKS staging awaits exclusive ownership after the first consumer')
            authorize()
        return {'resource_id': volume_id, 'state': 'awaiting_worker_staging'}

    def _mount(self, template, worker, volume_id, filesystem_uuid):
        return dict(profile='ebs', worker=worker, generation=1, mount_path=template.mount_path,
                    provider_volume_id=volume_id, filesystem_uuid=filesystem_uuid,
                    identity=dict(engine='oltp', pool_format_version=1, cluster_id=self.cluster_id,
                                  worker_id=worker['worker_id'], pool_id=template.pool_id))

    def register_worker(self, intent, target, record, *, controller, pin, deadline):
        template, enrollment, size = self._worker_intent(intent, target)
        worker_id, claim_id = enrollment['worker_id'], intent['claim_id']
        volume_id = record['provider_resource']

        def snapshot():
            return controller._read('storage.growth.snapshot', {}, deadline=deadline)

        before = snapshot()
        policy = before['policy']
        auto = policy.get('automatic') or {}
        quote = policy['worker_cost'] + self._gib(size) * policy['volume_cost_per_gib']
        if (before['pin'] != pin or before['cluster_id'] != self.cluster_id or not before['active_automatic']
                or auto.get('adapter_id') != self.adapter_id or policy['policy_id'] != intent['policy_id']
                or template.zone not in policy['zones'] or policy['worker_cost'] < template.worker_monthly_cost
                or quote != intent['quoted_monthly_cost'] or before['active'] != record
                or record['phase'] != 'awaiting_mount' or record['intent'] != intent):
            raise ProviderError('The current Control claim does not authorize EKS staging')

        def observe():
            stateful, pvcs = self.provider._retained_inventory()
            compute, storage = self._records(template, worker_id, stateful, pvcs, claim_id=claim_id)
            if storage is None:
                raise ProviderError('The durable EKS staging PVC disappeared')
            pvc_uid = storage['metadata']['uid']
            compute_uid = None if compute is None else compute['metadata']['uid']
            floor = self._spend(stateful, pvcs, policy, reserved_worker=compute_uid, reserved_pvc=pvc_uid)
            extra = max(0, floor - policy['initial_monthly_spend'])
            count = max(len(stateful) + int(compute is None), pin['ready']['ready_count'] + 1)
            if count > auto['max_workers'] or pin['monthly_spend'] + extra > policy['monthly_spend_ceiling']:
                raise ProviderError('Retained EKS resources exceed the current growth ceiling')
            staged = self._staged_pvc(template, worker_id, claim_id, size, existing_only=True)
            resource = staged['resource_id']
            if self.provider._resource_parts(resource)[2] != pvc_uid:
                raise ProviderError('EKS staging PVC changed during observation')
            bound = self._staged_storage(resource, template, worker_id, claim_id, size, expected=volume_id)
            return compute_uid, resource, bound

        with self.provider._limit(deadline):
            compute_uid, resource, (pvc, pv, _) = observe()
            handoff = self._handoff(template, claim_id, pvc, pv, volume_id)
            stage = dict(zone=template.zone, pvc_resource_id=resource, pv_uid=pv['metadata']['uid'],
                         volume_id=volume_id, bootstrap_secret=self._secret_name(claim_id))
            if handoff is None:
                return {'state': 'awaiting_filesystem_identity', 'stage': stage}
            expected = self._native_target(handoff)

            def authorize():
                current_compute, current_resource, (current_pvc, current_pv, _) = observe()
                if (current_resource != resource or current_pv['metadata']['uid'] != stage['pv_uid']
                        or self._handoff(template, claim_id, current_pvc, current_pv, volume_id) != handoff
                        or current_compute != compute_uid
                        or snapshot() != before):
                    raise ProviderError('Control or the exact EKS staging resources changed')

            if compute_uid is None:
                volume = self.provider._staged_ebs(volume_id, template.zone, self._gib(size))
                if volume['State'] != 'available' or volume['Attachments']:
                    raise ProviderError('EKS can create the exact worker only after first-consumer detachment')
            staged = self._staged_worker(template, enrollment, claim_id, resource, handoff,
                                         existing_only=compute_uid is not None or expected is not None, authorize=authorize)
            compute_uid = staged['stateful']['metadata']['uid']
            if expected is not None:
                authorize()
            observed = self.provider._staged_pod(staged, zone=template.zone, pvc=pvc, pv=pv, size_gib=self._gib(size),
                                                 expected=expected)
            stage['statefulset_resource_id'] = staged['resource_id']
            if observed is None:
                return {'state': 'awaiting_pod_attachment', 'stage': stage}
            stage['pod_uid'] = observed['pod_uid']
            if expected is None:
                return {'state': 'awaiting_pod_identity', 'stage': stage}

            preparation = dict(claim_id=claim_id, run_id=identity(pin['ready']['view']['run_id']), enrollment=enrollment,
                               mount=self._mount(template, target.worker, volume_id, handoff['filesystem_uuid']),
                               mount_public_key=template.mount_public_key, native_target=observed)
            if snapshot() != before:
                raise ProviderError('Control changed before EKS worker preparation')
            prepared = controller._operation(claim_id, 'prepare-worker', 'storage.growth.prepare_worker',
                                             preparation, deadline=deadline)
            after = snapshot()
            if (prepared != {'identity': target.worker} or after['active'] != record or not after['active_automatic']
                    or after['cluster_id'] != self.cluster_id or after['policy'] != policy
                    or any(after['pin'][key] != value for key, value in pin.items() if key != 'enrollment_revision')
                    or after['pin']['enrollment_revision'] not in (pin['enrollment_revision'], pin['enrollment_revision'] + 1)):
                raise ProviderError('Control changed before the EKS bootstrap handoff')
        return {'state': 'awaiting_bootstrap', 'stage': stage | {'bootstrap_intent': preparation}}

    def provision(self, intent, target, *, deadline):
        with self.provider._limit(deadline):
            zone, _, volumes, mounts = self._worker(target.worker)
            pvc_id = self._pvc_id(target.resource_id)
            _, name, _ = self.provider._resource_parts(pvc_id)
            if name not in volumes.values() or not any(volumes.get(key) == name for key in mounts.values()):
                raise ProviderError('EKS worker no longer mounts the expansion target PVC')
            target_bytes = intent['action']['target_bytes']
            if target_bytes > self.max_volume_bytes:
                raise ProviderError('EKS growth exceeds the configured volume ceiling')
            return self.provider.expand_pool(resource_id=pvc_id, target_bytes=target_bytes,
                            worker=target.worker, pool_id=target.pool_id, zone=zone, volume_id=target.resource_id)

    def register(self, intent, target, record, *, controller, deadline):
        inventory = controller._read('storage.growth.inventory', {}, deadline=deadline)
        row = next((row for row in inventory['pools'] if row['pool_id'] == target.pool_id), None)
        if (inventory['worker'] != target.worker or inventory['ready'] is not True or row is None
                or row['expected']['generation'] != target.pool_generation
                or row['expected']['provider_volume_id'] != target.resource_id):
            raise ProviderError('EKS growth target differs from the current native mount')
        with self.provider._limit(deadline):
            zone, _, volumes, mounts = self._worker(target.worker)
            self._mounted_pvc(row['expected'], target.worker, zone, volumes, mounts)
        # The native worker still verifies filesystem capacity after CSI resize.
        return None

    def _worker(self, worker):
        binding = self.workers.get(worker['worker_id'])
        if binding is None:
            raise ProviderError('Ready EKS worker has no fixed StatefulSet/Pod binding')
        stateful, pod = self.provider._inspect_live_worker(resource_id=binding[0], pod_uid=binding[1], worker=worker)
        spec = pod['spec']
        workers = [container for container in spec.get('containers', []) if container.get('name') == 'worker']
        if len(workers) != 1:
            raise ProviderError('EKS Pod has no exact worker container')
        volumes = {entry['name']: entry['persistentVolumeClaim']['claimName'] for entry in spec.get('volumes', [])
                   if 'persistentVolumeClaim' in entry and entry['persistentVolumeClaim'].get('readOnly') is not True}
        mounts = {entry['mountPath']: entry['name'] for entry in workers[0].get('volumeMounts', [])
                  if entry.get('readOnly') is not True and not entry.get('subPath') and not entry.get('subPathExpr')}
        return spec['nodeSelector'][_ZONE], stateful, volumes, mounts

    def _pvc_id(self, resource):
        pvc_id = self.pvcs.get(resource)
        if pvc_id is None:
            raise ProviderError('Native EBS mount has no fixed PVC UID binding')
        return pvc_id

    def _mounted_pvc(self, expected, worker, zone, volumes, mounts):
        resource = expected['provider_volume_id']
        if expected.get('profile') != 'ebs' or expected['worker'] != worker:
            raise ProviderError('EKS inventory contains a mount from another worker or provider')
        pvc_id = self._pvc_id(resource)
        _, name, _ = self.provider._resource_parts(pvc_id)
        if volumes.get(mounts.get(expected['mount_path'])) != name:
            raise ProviderError('EKS Pod no longer mounts the exact PVC at the registered path')
        _, pvc = self.provider._pvc(pvc_id, worker_id=worker['worker_id'], zone=zone,
                                   pool_id=expected['identity']['pool_id'])
        self.provider._bound_volume(pvc, zone=zone, expected=resource)
        return pvc
