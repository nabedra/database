"""EBS growth and opt-in EC2 data worker staging over exact provider inventory."""
from copy import copy
from dataclasses import asdict, dataclass
import hmac
import json
from pathlib import Path, PurePosixPath
import re
from types import MappingProxyType
from typing import TypedDict
from urllib.parse import urlsplit

from ..client import _unique_object
from ..providers import ProviderError
from .cloud_control import _native_target, ec2_worker_tags
from .docker import digest, identity, private_bytes
from .live import GrowthTarget, HostObservation, ProviderObservation, _worker as _enrollment_worker

_GIB = 1 << 30
_ADMIN_PATH = '/v1/cluster/operations'


def _worker(record, worker):
    tags = _tags(record)
    if (any(tags.get(key) != value for key, value in ec2_worker_tags(worker).items())
            or record.get('State', {}).get('Name') != 'running'):
        raise ProviderError('EC2 instance identity differs from the current worker')
    return record['Placement']['AvailabilityZone']


def _tags(record):
    return {tag['Key']: tag['Value'] for tag in record.get('Tags', ())}


@dataclass(frozen=True)
class Ec2PoolSlot:
    pool_id: str
    device: str
    mount_path: str
    filesystem_uuid: str
    kms_key_id: str

    def __post_init__(self):
        identity(self.pool_id)
        identity(self.filesystem_uuid)
        path = PurePosixPath(self.mount_path)
        if (not isinstance(self.device, str) or not re.fullmatch(r'/dev/sd[f-p]', self.device)
                or not path.is_absolute() or str(path) != self.mount_path or path == PurePosixPath('/')
                or '..' in path.parts or not isinstance(self.kms_key_id, str) or not self.kms_key_id):
            raise ProviderError('A reserved EC2 secondary device, mount path and KMS key are required')


@dataclass(frozen=True)
class Ec2WorkerTemplate:
    """One protected enrollment and exclusive EBS slot in a private subnet."""

    zone: str
    subnet_id: str
    pool: Ec2PoolSlot
    enrollment_file: str
    admin_url: str
    admin_token_file: str
    mount_public_key: str
    filesystem_reserve_bytes: int
    max_request_bytes: int
    monthly_cost: int


class Ec2WorkerStage(TypedDict):
    """Exact provider handoff for preparing and starting a native data worker."""

    instance_id: str
    zone: str
    subnet_id: str
    device: str
    admin_url: str
    bootstrap_intent: dict


def _private_file(filename, limit):
    try:
        path = Path(filename)
        if path.parent.stat().st_mode & 0o077 or path.stat().st_uid != path.parent.stat().st_uid:
            raise ValueError
        return private_bytes(path, limit)
    except (OSError, TypeError, ValueError, ProviderError):
        raise ProviderError('EC2 staging requires isolated private enrollment and admin credentials') from None


def _inputs(template):
    try:
        enrollment = json.loads(_private_file(template.enrollment_file, 4096), object_pairs_hook=_unique_object)
        token = _private_file(template.admin_token_file, 4097).decode('ascii').removesuffix('\n')
        ec2_worker_tags(enrollment)
        name, _, port = enrollment['endpoint'].rpartition(':')
        url = urlsplit(template.admin_url)
        if (set(enrollment) != {'worker_id', 'incarnation', 'enrollment_revision', 'certificate_sha256',
                               'endpoint', 'server_name'} or enrollment['enrollment_revision'] != 1
                or not re.fullmatch(r'(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+'
                                    r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', name)
                or enrollment['server_name'] != name or not re.fullmatch(r'[1-9][0-9]{0,4}', port)
                or not 1 <= int(port) <= 65535 or url.scheme != 'https' or url.hostname != name
                or url.port == int(port) or url.path or url.query or url.fragment
                or template.admin_url != f'https://{name}' + (f':{url.port}' if url.port is not None else '')
                or not 43 <= len(token) <= 4096 or not re.fullmatch(r'[A-Za-z0-9._~+/-]+=*', token)
                or Path(template.enrollment_file) == Path(template.admin_token_file)):
            raise ValueError
    except (AttributeError, KeyError, TypeError, ValueError):
        raise ProviderError('EC2 data worker slot has an invalid enrollment or protected admin endpoint') from None
    return enrollment, token


class Ec2GrowthAdapter:
    """Reconcile EBS size requests and an explicitly reserved secondary slot.

    A new disk needs an operator-prepared filesystem and mount. An existing
    disk can use optional host maintenance. Native observation sets capacity.
    """

    def __init__(self, *, provider, adapter_id, instances, pool_slots=None, templates=()):
        self.provider = provider
        self.cluster_id = identity(provider.cluster_id)
        self.adapter_id = identity(adapter_id)
        self.instances = MappingProxyType({identity(key): resource for key, resource in instances.items()})
        slots = dict(pool_slots or {})
        if (not set(slots).issubset(self.instances) or any(type(slot) is not Ec2PoolSlot for slot in slots.values())
                or len({slot.pool_id for slot in slots.values()}) != len(slots)):
            raise ProviderError('EC2 pool slots must belong to fixed instances and have distinct pool identities')
        self.pool_slots = MappingProxyType(slots)
        self._templates = {}
        self._by_worker = {}
        self._fingerprints = {}
        self._endpoints = MappingProxyType({})
        pools = {slot.pool_id for slot in slots.values()}
        files, names, certificates, incarnations, mount_keys = set(), set(), set(), set(), set()
        for template in templates:
            if (type(template) is not Ec2WorkerTemplate or template.zone not in provider.zones
                    or template.zone in self._templates or type(template.pool) is not Ec2PoolSlot
                    or template.pool.pool_id in pools or len(template.pool.mount_path.encode()) > 1024
                    or '\0' in template.pool.mount_path or not isinstance(template.subnet_id, str)
                    or not re.fullmatch(r'subnet-(?:[a-f0-9]{8}|[a-f0-9]{17})', template.subnet_id)
                    or not isinstance(template.mount_public_key, str)
                    or not re.fullmatch(r'[a-f0-9]{64}', template.mount_public_key)
                    or template.mount_public_key == '0' * 64 or template.mount_public_key in mount_keys
                    or type(template.filesystem_reserve_bytes) is not int
                    or not 0 < template.filesystem_reserve_bytes < _GIB
                    or type(template.max_request_bytes) is not int
                    or not 0 < template.max_request_bytes <= provider.max_volume_gib * _GIB
                    or type(template.monthly_cost) is not int or not 0 < template.monthly_cost < 2**64):
                raise ProviderError('EC2 requires one bounded prospective worker slot per configured zone')
            region = getattr(provider.client.meta, 'region_name', None)
            kms = template.pool.kms_key_id
            if (not region or not re.fullmatch(r'arn:aws(?:-[a-z]+)*:kms:' + re.escape(region)
                                              + r':[0-9]{12}:key/(?:mrk-[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-'
                                                r'[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', kms)):
                raise ProviderError('EC2 staging requires a KMS key ARN in the configured client region')
            enrollment, token = _inputs(template)
            worker_id = enrollment['worker_id']
            name = enrollment['server_name']
            certificate = enrollment['certificate_sha256']
            incarnation = enrollment['incarnation']
            sources = {template.enrollment_file, template.admin_token_file}
            if (worker_id in self.instances or worker_id in self._by_worker or names & {name}
                    or certificates & {certificate} or incarnations & {incarnation} or files & sources):
                raise ProviderError('EC2 staging templates must have distinct worker identities and private inputs')
            self._templates[template.zone] = template
            self._by_worker[worker_id] = template
            self._fingerprints[template.zone] = self._fingerprint(template, enrollment, token)
            pools.add(template.pool.pool_id)
            names.add(name)
            certificates.add(certificate)
            incarnations.add(incarnation)
            files.update(sources)
            mount_keys.add(template.mount_public_key)

    def _fingerprint(self, template, enrollment, token):
        spec = asdict(template)
        del spec['enrollment_file'], spec['admin_token_file']
        return digest((self.cluster_id, self.adapter_id, dict(self.provider.launch_template),
                       spec, enrollment, digest(token)))

    def _inputs(self, template):
        enrollment, token = _inputs(template)
        if self._fingerprint(template, enrollment, token) != self._fingerprints[template.zone]:
            raise ProviderError('Protected EC2 launch inputs changed')
        return enrollment, token

    def bind_endpoints(self, endpoints):
        bound = copy(self)
        matched = {}
        urls = [endpoint._transport._url for endpoint in endpoints.values()]
        for worker_id, template in self._by_worker.items():
            endpoint = endpoints.get(worker_id)
            if endpoint is not None:
                _, token = self._inputs(template)
                transport = endpoint._transport
                if (transport._url == template.admin_url + _ADMIN_PATH and urls.count(transport._url) == 1
                        and hmac.compare_digest(transport._token, token)):
                    matched[worker_id] = endpoint
        bound._endpoints = MappingProxyType(matched)
        return bound

    def _template_data(self, template, deadline):
        return self.provider.staging_template(zone=template.zone, subnet_id=template.subnet_id,
                                              device=template.pool.device, deadline=deadline)

    @staticmethod
    def _slot_records(records, worker_id, *, pool_id=None, claim_id=None):
        matches = []
        for record in records.values():
            tags = _tags(record)
            if (tags.get('nabedra:worker') == worker_id
                    or (pool_id is not None and tags.get('nabedra:pool') == pool_id)
                    or (claim_id is not None and tags.get('nabedra:claim') == claim_id)):
                matches.append(record)
        if len(matches) > 1:
            raise ProviderError('Several EC2 resources match one reserved worker or pool')
        return matches[0] if matches else None

    def _volume_claim(self, template, worker_id, claim_id, size):
        return dict(claim_id=claim_id, worker_id=worker_id, pool_id=template.pool.pool_id,
                    zone=template.zone, kms_key_id=template.pool.kms_key_id, size=size,
                    staging_digest=self._fingerprints[template.zone])

    def _staged_instance_volume(self, workers, volumes, template, worker_id, claim_id, size):
        instance = self._slot_records(workers, worker_id, claim_id=claim_id)
        volume = self._slot_records(volumes, worker_id, pool_id=template.pool.pool_id, claim_id=claim_id)
        if volume is not None:
            self.provider._staged_volume(volume, **self._volume_claim(template, worker_id, claim_id, size))
            if volume['Size'] != size or volume.get('State') not in ('creating', 'available', 'in-use'):
                raise ProviderError('EBS staging volume changed size or is unavailable')
            if instance is None and (volume.get('Attachments') or volume['State'] == 'in-use'):
                raise ProviderError('EBS staging volume is attached without the exact retained worker')
        elif instance is not None:
            raise ProviderError('EC2 staging worker has no exact retained volume')
        return instance, volume

    def _instance_claim(self, template, enrollment, claim_id, volume_id, size, deadline):
        return dict(claim_id=claim_id, enrollment=enrollment, zone=template.zone, subnet_id=template.subnet_id,
                    volume_id=volume_id, size=size, staging_digest=self._fingerprints[template.zone],
                    template=self._template_data(template, deadline))

    def _mount(self, template, worker, volume_id):
        pool = template.pool
        return dict(profile='ebs', worker=worker, generation=1, mount_path=pool.mount_path,
                    provider_volume_id=volume_id, filesystem_uuid=pool.filesystem_uuid,
                    identity=dict(engine='oltp', pool_format_version=1, cluster_id=self.cluster_id,
                                  worker_id=worker['worker_id'], pool_id=pool.pool_id))

    def _staged_ready(self, template, worker, record, volumes, inventory, deadline):
        enrollment, _ = self._inputs(template)
        tags = _tags(record)
        claim_id = identity(tags.get('nabedra:claim'))
        original_size = tags.get('nabedra:volume-gib', '')
        volume_id = tags.get('nabedra:volume')
        primary = [row for row in inventory['pools'] if row['primary']]
        if (worker != _enrollment_worker(enrollment) or not isinstance(original_size, str)
                or not re.fullmatch(r'[1-9][0-9]{0,4}', original_size)
                or volume_id not in volumes or len(primary) != 1 or primary[0]['growth_claim'] != claim_id
                or primary[0]['pool_id'] != template.pool.pool_id
                or primary[0]['expected'] != self._mount(template, worker, volume_id)):
            raise ProviderError('Ready EC2 worker does not match its staged claim and native primary mount')
        size = int(original_size)
        self.provider._staged_volume(volumes[volume_id], **self._volume_claim(template, worker['worker_id'], claim_id, size))
        self.provider._staged_worker(record, **self._instance_claim(template, enrollment, claim_id, volume_id, size, deadline))
        if not self._staged_attachment(template, record, volumes[volume_id]):
            raise ProviderError('Ready EC2 worker lost the exact retained mount device')

    def _staged_attachment(self, template, instance, volume):
        attachment = self.provider._attachment(volume, instance, device=template.pool.device)
        mappings = [row.get('Ebs', {}) for row in instance.get('BlockDeviceMappings', ())
                    if row.get('DeviceName') == template.pool.device]
        if (len(mappings) > 1 or (mappings and (mappings[0].get('VolumeId') != volume['VolumeId']
                                               or mappings[0].get('DeleteOnTermination') is not False))):
            raise ProviderError('EC2 instance maps the staging device to a different or unretained volume')
        return bool(attachment and attachment.get('State') == 'attached' and len(mappings) == 1
                    and mappings[0].get('Status') == 'attached' and volume.get('State') == 'in-use')

    @staticmethod
    def _floor(workers, volumes, policy, *, reserved_worker=None, reserved_volume=None):
        sizes = [volume.get('Size') for resource, volume in volumes.items() if resource != reserved_volume]
        if any(type(gib) is not int or gib <= 0 for gib in sizes):
            raise ProviderError('EBS inventory has an invalid retained volume size')
        return ((len(workers) - int(reserved_worker in workers)) * policy['worker_cost']
                + sum(sizes) * policy['volume_cost_per_gib'])

    def _valid_size(self, template, size):
        return (type(size) is int and 0 < size <= template.max_request_bytes
                and self.provider._size(size) * _GIB - size >= template.filesystem_reserve_bytes)

    def observe(self, inventories, *, policy, deadline, claim_id=None):
        workers, volumes = self.provider.retained_inventory(deadline=deadline)
        hosts = {}
        ready_instances = set()
        for worker_id, sample in inventories.items():
            inventory = sample['inventory']
            worker = inventory['worker']
            instance_id = self.instances.get(worker_id)
            if instance_id is None and worker_id in self._endpoints:
                record = self._slot_records(workers, worker_id)
                if record is not None:
                    self._staged_ready(self._by_worker[worker_id], worker, record, volumes, inventory, deadline)
                    instance_id = record['InstanceId']
            if instance_id not in workers or instance_id in ready_instances:
                raise ProviderError('Ready worker has no exact EC2 provider instance')
            record = workers[instance_id]
            zone = _worker(record, worker)
            cpu = record.get('CpuOptions', {})
            cores, threads = cpu.get('CoreCount'), cpu.get('ThreadsPerCore')
            if type(cores) is not int or type(threads) is not int or min(cores, threads) <= 0:
                raise ProviderError('EC2 did not report the current worker CPU limit')
            expandable = set()
            for row in sample['inventory']['pools']:
                resource = row['expected']['provider_volume_id']
                if resource not in volumes:
                    raise ProviderError('Mounted EBS volume is missing from retained inventory')
                volume = volumes[resource]
                self._attached(volume, record, row['expected'])
                if volume['Size'] * _GIB < row['observed_total_bytes']:
                    raise ProviderError('Mounted bytes exceed the observed provider volume size')
                if (worker_id in self.instances
                        and row['observed_total_bytes'] < self.provider.max_volume_gib * _GIB):
                    expandable.add(resource)
            slot = self.pool_slots.get(worker_id)
            available = (slot is not None
                         and all(entry.get('DeviceName') != slot.device for entry in record.get('BlockDeviceMappings', []))
                         and all(row['pool_id'] != slot.pool_id and row['expected']['mount_path'] != slot.mount_path
                                 for row in sample['inventory']['pools']))
            hosts[worker_id] = HostObservation(worker, zone, int(available), frozenset(expandable),
                                              cpu_limit_millicores=cores * threads * 1000)
            ready_instances.add(instance_id)
        zones = set()
        reserved_worker = reserved_volume = None
        for worker_id, template in self._by_worker.items():
            if worker_id not in self._endpoints or worker_id in inventories or template.zone not in policy['zones']:
                continue
            enrollment, _ = self._inputs(template)
            instance = self._slot_records(workers, worker_id)
            volume = self._slot_records(volumes, worker_id, pool_id=template.pool.pool_id)
            if instance is not None and (claim_id is None or _tags(instance).get('nabedra:claim') != claim_id):
                continue
            if volume is not None:
                if claim_id is None or _tags(volume).get('nabedra:claim') != claim_id:
                    continue
                volume_size = volume.get('Size')
                instance, volume = self._staged_instance_volume(workers, volumes, template, worker_id,
                                                                claim_id, volume_size)
                if reserved_volume is not None:
                    raise ProviderError('More than one staging volume uses the current EC2 claim')
                reserved_volume = volume['VolumeId']
                if instance is not None:
                    self.provider._staged_worker(instance, **self._instance_claim(
                        template, enrollment, claim_id, reserved_volume, volume_size, deadline))
                    reserved_worker = instance['InstanceId']
            elif instance is not None:
                continue
            baseline = policy['automatic']['pool_increment_bytes']
            if not 0 < baseline <= template.max_request_bytes:
                continue
            if policy['worker_cost'] + self.provider._size(baseline) * policy['volume_cost_per_gib'] < template.monthly_cost:
                continue
            self._template_data(template, deadline)
            zones.add(template.zone)
        floor = self._floor(workers, volumes, policy, reserved_worker=reserved_worker, reserved_volume=reserved_volume)
        retained = len(set(workers) - ready_instances - {reserved_worker})
        return ProviderObservation(hosts, retained, floor, frozenset(zones))

    def target(self, claim_id, action, inventories):
        if action['kind'] == 'add_worker':
            template = self._templates.get(action['zone'])
            if template is None or not self._valid_size(template, action['bytes']):
                return None
            enrollment, _ = self._inputs(template)
            worker = _enrollment_worker(enrollment)
            worker_id = worker['worker_id']
            if (worker_id not in self._endpoints or worker_id in inventories
                    or any(sample['inventory']['worker'][key] == worker[key] for sample in inventories.values()
                           for key in ('incarnation', 'certificate_sha256'))):
                return None
            return GrowthTarget(worker, template.pool.pool_id, 1)
        if action['kind'] == 'add_pool':
            slot = self.pool_slots.get(action['worker'])
            if slot is None or action['bytes'] > self.provider.max_volume_gib * _GIB:
                return None
            return GrowthTarget(inventories[action['worker']]['inventory']['worker'], slot.pool_id, 1)
        if (action['kind'] != 'expand' or action['worker'] not in self.instances
                or action['target_bytes'] > self.provider.max_volume_gib * _GIB):
            return None
        inventory = inventories[action['worker']]['inventory']
        row = next((row for row in inventory['pools'] if row['pool_id'] == action['pool']), None)
        if row is None or row['expected'].get('profile') != 'ebs':
            return None
        expected = row['expected']
        return GrowthTarget(inventory['worker'], row['pool_id'], expected['generation'],
                            expected['provider_volume_id'])

    def provision(self, intent, target, *, deadline):
        if intent['action']['kind'] == 'add_worker':
            raise ProviderError('EC2 worker provisioning requires a current automatic Control attempt')
        instance_id = self.instances[target.worker['worker_id']]
        worker = self.provider.inspect_worker(instance_id, deadline=deadline)
        zone = _worker(worker, target.worker)
        action = intent['action']
        if action['kind'] == 'add_pool':
            slot = self.pool_slots[target.worker['worker_id']]
            if action['zone'] != zone or target.pool_id != slot.pool_id:
                raise ProviderError('Reserved EBS slot and growth claim differ')
            return self.provider.ensure_pool(claim_id=intent['claim_id'], pool_id=target.pool_id,
                worker_id=target.worker['worker_id'], zone=zone, bytes=action['bytes'],
                kms_key_id=slot.kms_key_id, deadline=deadline)
        volume = self.provider.inspect_volume(target.resource_id, deadline=deadline)
        self._attached(volume, worker, {'identity': {'pool_id': target.pool_id}, 'worker': target.worker})
        return self.provider.expand_pool(volume_id=target.resource_id,
                                         target_bytes=intent['action']['target_bytes'], deadline=deadline)

    def _worker_intent(self, intent, target):
        claim_id = identity(intent['claim_id'])
        action = intent['action']
        template = self._templates.get(action['zone'])
        if (template is None or self.target(claim_id, action, {}) != target
                or (intent['worker'], intent['pool_id'], intent['pool_generation'])
                    != (target.worker, target.pool_id, target.pool_generation)
                or not template.monthly_cost <= intent['quoted_monthly_cost'] <= intent['monthly_spend_ceiling']):
            raise ProviderError('EC2 staging template differs from the exact priced claim')
        enrollment, _ = self._inputs(template)
        size = self.provider._size(action['bytes'])
        return template, enrollment, self._volume_claim(template, enrollment['worker_id'], claim_id, size)

    def provision_worker(self, intent, target, *, authorize, deadline):
        template, enrollment, volume_claim = self._worker_intent(intent, target)
        claim_id, worker_id, size = intent['claim_id'], enrollment['worker_id'], volume_claim['size']
        workers, volumes = self.provider.retained_inventory(deadline=deadline)
        instance, volume = self._staged_instance_volume(workers, volumes, template, worker_id, claim_id, size)
        if volume is None:
            options = {key: value for key, value in volume_claim.items() if key != 'size'}
            created = self.provider.ensure_pool(**options, bytes=intent['action']['bytes'],
                                                authorize=authorize, deadline=deadline)
            volume = self.provider.inspect_volume(created['resource_id'], deadline=deadline)
            self.provider._staged_volume(volume, **volume_claim)
            if volume['Size'] != size or volume.get('Attachments'):
                raise ProviderError('EBS staged volume size or attachment changed')
        if volume.get('State') == 'creating':
            raise ProviderError('The exact encrypted EBS volume is still being created')
        if instance is None and volume.get('State') != 'available':
            raise ProviderError('The exact encrypted EBS volume is unavailable for an instance')
        volume_id = volume['VolumeId']
        options = self._instance_claim(template, enrollment, claim_id, volume_id, size, deadline)
        if instance is None:
            instance = self.provider.stage_worker(**options, authorize=authorize, deadline=deadline)
        else:
            self.provider._staged_worker(instance, **options)
        if instance.get('State', {}).get('Name') != 'running':
            raise ProviderError('The exact staged EC2 instance is pending or unavailable')
        instance_id = instance['InstanceId']
        if not self._staged_attachment(template, instance, volume):
            self.provider.attach_pool(claim_id=claim_id, pool_id=target.pool_id, worker=target.worker,
                                      volume_id=volume_id, instance_id=instance_id, device=template.pool.device,
                                      authorize=authorize, deadline=deadline)
            instance = self.provider.inspect_worker(instance_id, deadline=deadline)
            volume = self.provider.inspect_volume(volume_id, deadline=deadline)
            self.provider._staged_worker(instance, **options)
            self.provider._staged_volume(volume, **volume_claim)
        if not self._staged_attachment(template, instance, volume):
            raise ProviderError('The exact EC2 and EBS attachment is pending or unavailable')
        return {'resource_id': volume_id, 'state': 'attached'}

    def register_worker(self, intent, target, record, *, controller, pin, deadline):
        template, enrollment, volume_claim = self._worker_intent(intent, target)
        claim_id = intent['claim_id']
        worker_id = enrollment['worker_id']
        volume_id = record['provider_resource']

        def snapshot():
            return controller._read('storage.growth.snapshot', {}, deadline=deadline)

        before = snapshot()
        policy = before['policy']
        auto = policy.get('automatic') or {}
        size = volume_claim['size']
        quote = policy['worker_cost'] + size * policy['volume_cost_per_gib']
        if (before['pin'] != pin or before['cluster_id'] != self.cluster_id or not before['active_automatic']
                or auto.get('adapter_id') != self.adapter_id or policy['policy_id'] != intent['policy_id']
                or template.zone not in policy['zones'] or quote != intent['quoted_monthly_cost']
                or before['active'] != record or record['phase'] != 'awaiting_mount' or record['intent'] != intent):
            raise ProviderError('The current Control claim does not authorize EC2 staging')

        def authorize():
            if snapshot() != before:
                raise ProviderError('Control changed during EC2 staging')

        workers, volumes = self.provider.retained_inventory(deadline=deadline)
        instance, volume = self._staged_instance_volume(workers, volumes, template, worker_id, claim_id, size)
        if instance is None or volume is None or volume['VolumeId'] != volume_id:
            raise ProviderError('The durable EC2 staging resources are absent or changed')
        options = self._instance_claim(template, enrollment, claim_id, volume_id, size, deadline)
        self.provider._staged_worker(instance, **options)
        instance_id = instance['InstanceId']
        floor = self._floor(workers, volumes, policy, reserved_worker=instance_id, reserved_volume=volume_id)
        extra = max(0, floor - policy['initial_monthly_spend'])
        if (pin['monthly_spend'] + extra > policy['monthly_spend_ceiling']
                or max(len(workers), pin['ready']['ready_count']) > auto['max_workers']):
            raise ProviderError('Retained EC2 resources exceed the current growth ceiling')
        if instance.get('State', {}).get('Name') != 'running' or not self._staged_attachment(template, instance, volume):
            raise ProviderError('EC2 bootstrap requires the running instance and exact retained EBS attachment')

        preparation = dict(claim_id=claim_id, run_id=identity(pin['ready']['view']['run_id']), enrollment=enrollment,
                           mount=self._mount(template, target.worker, volume_id), mount_public_key=template.mount_public_key,
                           native_target=_native_target({'kind': 'ec2', 'instance_id': instance_id}))
        authorize()
        prepared = controller._operation(claim_id, 'prepare-worker', 'storage.growth.prepare_worker',
                                         preparation, deadline=deadline)
        after = snapshot()
        if (prepared != {'identity': target.worker} or after['active'] != record or not after['active_automatic']
                or after['cluster_id'] != self.cluster_id or after['policy'] != policy
                or any(after['pin'][key] != value for key, value in pin.items() if key != 'enrollment_revision')
                or after['pin']['enrollment_revision'] not in (pin['enrollment_revision'], pin['enrollment_revision'] + 1)):
            raise ProviderError('Control changed before the EC2 bootstrap handoff')
        stage: Ec2WorkerStage = dict(instance_id=instance_id, zone=template.zone, subnet_id=template.subnet_id,
                                     device=template.pool.device, admin_url=template.admin_url,
                                     bootstrap_intent=preparation)
        return {'state': 'awaiting_bootstrap', 'stage': stage}

    def register(self, intent, target, record, *, controller, deadline):
        instance_id = self.instances[target.worker['worker_id']]
        if intent['action']['kind'] == 'add_pool':
            slot = self.pool_slots[target.worker['worker_id']]
            volume_id = record['provider_resource']
            attachment = self.provider.attach_pool(claim_id=intent['claim_id'], pool_id=target.pool_id,
                worker=target.worker, volume_id=volume_id, instance_id=instance_id, device=slot.device,
                deadline=deadline)
            if attachment['state'] != 'attached':
                return None
            return {'worker': target.worker, 'profile': 'ebs', 'generation': target.pool_generation,
                    'provider_volume_id': volume_id, 'filesystem_uuid': slot.filesystem_uuid,
                    'mount_path': slot.mount_path, 'identity': {'engine': 'oltp', 'pool_format_version': 1,
                    'cluster_id': self.cluster_id, 'worker_id': target.worker['worker_id'], 'pool_id': target.pool_id}}
        if self.provider.local_host is not None and self.provider.local_host.instance_id == instance_id:
            inventory = controller._read('storage.growth.inventory', {}, deadline=deadline)
            mounted = next((row for row in inventory['pools'] if row['pool_id'] == target.pool_id), None)
            if (inventory['worker'] != target.worker or inventory['ready'] is not True or mounted is None
                    or mounted['expected']['generation'] != target.pool_generation
                    or mounted['expected']['provider_volume_id'] != target.resource_id
                    or mounted['expected']['profile'] != 'ebs'):
                raise ProviderError('EBS grow target no longer matches the verified mounted pool')
            if mounted['observed_total_bytes'] < intent['action']['target_bytes']:
                self.provider.grow_filesystem(instance_id=instance_id, expected=mounted['expected'],
                    target_bytes=intent['action']['target_bytes'], deadline=deadline)
        # Only the worker's native mount observation can complete the claim.
        return None

    def _attached(self, volume, worker, expected):
        self.provider._owned(volume, {'nabedra:pool': expected['identity']['pool_id'],
                                     'nabedra:worker': expected['worker']['worker_id']})
        attachment = self.provider._attachment(volume, worker)
        if attachment is None or attachment.get('State') != 'attached':
            raise ProviderError('EBS does not exclusively belong to the current worker')
