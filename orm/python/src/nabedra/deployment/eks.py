"""One StatefulSet per storage worker with retained PVCs.

Uses the existing bounded authenticated Kubernetes HTTP transport. API success,
PVC Bound and Pod Running do not publish database readiness or mounted bytes.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
import re
import time
from urllib.parse import quote, urlencode

from ..providers import KubernetesProvider, ProviderError, _bounded_call
from .cloud_control import CloudLifecycleControl, _closed_worker_mounts, _native_target, ec2_worker_tags
from .docker import digest, identity
from .eks_replacement import EksReplacementVerifier, _SERVICE_ACCOUNT, _TOKEN_PATH, audience, reconcile

_WRITER = ['ReadWriteOncePod']
_DRIVER = 'ebs.csi.aws.com'
_ZONE = 'topology.kubernetes.io/zone'
_ZONE_KEYS = {_ZONE, 'topology.ebs.csi.aws.com/zone'}


def _matches(actual, expected):
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        if set(expected) == {'cpu', 'memory'}:
            try:
                return (set(actual) == set(expected) and _millicores(actual['cpu']) == _millicores(expected['cpu'])
                        and EksProvider._quantity_bytes(actual['memory']) == EksProvider._quantity_bytes(expected['memory']))
            except (AttributeError, ArithmeticError, ProviderError):
                return False
        return all(_matches(actual.get(key), value) for key, value in expected.items())
    if isinstance(expected, list):
        return (isinstance(actual, list) and len(actual) == len(expected)
                and all(_matches(a, e) for a, e in zip(actual, expected)))
    return type(actual) is type(expected) and actual == expected


def _millicores(value):
    return Decimal(value[:-1]) if value.endswith('m') else Decimal(value) * 1000


def _staging_spec(actual, expected):
    if (not _matches(actual, expected) or any(actual.get(key) for key in (
            'ephemeralContainers', 'hostNetwork', 'hostPID', 'hostIPC', 'hostAliases', 'shareProcessNamespace'))):
        return False
    for kind in ('containers', 'initContainers'):
        for container, template in zip(actual[kind], expected[kind]):
            if (any(container.get(key) for key in ('args', 'env', 'envFrom', 'volumeDevices', 'lifecycle'))
                    or container.get('workingDir') or container.get('securityContext') != template['securityContext']
                    or any(mount.get('subPath') or mount.get('subPathExpr')
                           or mount.get('mountPropagation') not in (None, 'None')
                           or bool(mount.get('readOnly')) != bool(wanted.get('readOnly'))
                           for mount, wanted in zip(container['volumeMounts'], template['volumeMounts']))):
                return False
    return (actual.get('securityContext') == expected['securityContext']
            and actual.get('serviceAccountName', 'default') == 'default'
            and all(not volume.get('secret', {}).get('optional') and not volume.get('secret', {}).get('items')
                    and not volume.get('persistentVolumeClaim', {}).get('readOnly') for volume in actual['volumes'])
            and all(set(volume) == set(template) for volume, template in zip(actual['volumes'], expected['volumes'])))


def _enrollment_labels(worker):
    tags = ec2_worker_tags(worker)
    return {'nabedra.io/worker': tags['nabedra:worker'], 'nabedra.io/incarnation': tags['nabedra:incarnation'],
            'nabedra.io/enrollment-revision': tags['nabedra:enrollment-revision']}


_PRIVATE_CONFIG = r'''import os,shutil,uuid
source='/secret';target='/private'
os.chown(target,1000,1000);os.chmod(target,0o700)
names=[name for name in os.listdir(source) if not name.startswith('.')]
assert 0<len(names)<=64,'bounded bootstrap Secret required'
for name in names:
    assert '/' not in name and name not in ('.','..')
    path=source+'/'+name
    assert os.path.isfile(path) and os.stat(path).st_size<=1048576,'oversized bootstrap item'
    temporary=target+'/.copy-'+str(uuid.uuid4())
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    os.fchown(fd,1000,1000)
    with open(path,'rb') as incoming,os.fdopen(fd,'wb') as outgoing:
        shutil.copyfileobj(incoming,outgoing,65536)
    os.replace(temporary,target+'/'+name)
'''


class EksProvider(KubernetesProvider):
    def __init__(self, *, storage_class, lifecycle_authority=None, replacement_verifier=None,
                 staging_ec2_client=None, **config):
        super().__init__(**config)
        if not isinstance(storage_class, str) or not storage_class or len(storage_class) > 253:
            raise ProviderError('Explicit retained expandable EBS CSI StorageClass is required')
        if lifecycle_authority is not None and type(lifecycle_authority) is not CloudLifecycleControl:
            raise ProviderError('EKS worker lifecycle requires authenticated Control')
        if replacement_verifier is not None:
            if (type(replacement_verifier) is not EksReplacementVerifier or lifecycle_authority is None
                    or (replacement_verifier._kube.cluster_id, replacement_verifier._kube.namespace, replacement_verifier._kube._origin)
                    != (self.cluster_id, self.namespace, self._origin)):
                raise ProviderError('EKS replacement verifier must use the same explicit Kubernetes deployment')
        self.storage_class = storage_class
        self.lifecycle_authority = lifecycle_authority
        self.replacement_verifier = replacement_verifier
        self._apps = '/apis/apps/v1/namespaces/' + quote(self.namespace, safe='')
        self._staging_ec2 = staging_ec2_client
        if staging_ec2_client is not None:
            aws = getattr(getattr(staging_ec2_client, 'meta', None), 'config', None)
            connect, read = getattr(aws, 'connect_timeout', None), getattr(aws, 'read_timeout', None)
            retries = getattr(aws, 'retries', None) or {}
            region = getattr(aws, 'region_name', '') or ''
            if (type(connect) not in (int, float) or not 0 < connect <= 10
                    or type(read) not in (int, float) or not 0 < read <= 20
                    or retries.get('mode') != 'standard' or retries.get('total_max_attempts') != 1
                    or not isinstance(region, str) or not re.fullmatch(r'[a-z][a-z0-9-]+-[1-9][0-9]*', region)):
                raise ProviderError('EKS staging requires a region-pinned EC2 client with a single bounded attempt')
            self._staging_region, self._staging_aws_window = region, connect + read

    @contextmanager
    def _limit(self, deadline):
        previous = getattr(self._calls, 'until', None)
        self._calls.until = min(previous, deadline) if previous is not None else deadline
        try:
            yield
        finally:
            self._calls.until = previous

    def _stateful_labels(self, claim_id, content):
        return {'nabedra.io/cluster': self.cluster_id, 'nabedra.io/claim': identity(claim_id),
                'nabedra.io/content': base64.b32encode(bytes.fromhex(digest(content))).decode().rstrip('=').lower()}

    def _storage_class(self):
        storage = self._request('GET', '/apis/storage.k8s.io/v1/storageclasses/' + quote(self.storage_class, safe=''))
        if (storage.get('provisioner') != _DRIVER or storage.get('reclaimPolicy') != 'Retain'
                or storage.get('volumeBindingMode') != 'WaitForFirstConsumer'
                or storage.get('allowVolumeExpansion') is not True
                or storage.get('parameters', {}).get('encrypted') != 'true'):
            raise ProviderError('EKS storage requires retained expandable standard EBS CSI volumes and topology binding')

    def _stateful_owned(self, record, labels):
        meta = record.get('metadata', {})
        if meta.get('namespace') != self.namespace or any(meta.get('labels', {}).get(k) != v for k, v in labels.items()):
            raise ProviderError('EKS stateful ownership or immutable claim differs')
        if not meta.get('uid') or not meta.get('resourceVersion'):
            raise ProviderError('EKS stateful resource has no exact identity')
        return record

    @_bounded_call
    def ensure_pool(self, *, claim_id, pool_id, worker_id, zone, bytes,
                    _staging=None, _authorize=None, _existing_only=False):
        if type(bytes) is not int or bytes <= 0 or not isinstance(zone, str) or not zone:
            raise ProviderError('Explicit positive EKS volume size and zone are required')
        self._storage_class()
        name = 'nabedra-pool-' + identity(pool_id)
        content = dict(pool_id=pool_id, worker_id=identity(worker_id),
                       zone=zone, bytes=bytes, storage_class=self.storage_class)
        if _staging is not None:
            content['staging'] = _staging
        labels = self._stateful_labels(claim_id, content)
        labels.update({'nabedra.io/worker': worker_id, 'nabedra.io/pool': pool_id})
        path = self._base + '/persistentvolumeclaims/' + name
        record = self._request('GET', path, missing=True)
        if record is None:
            if _existing_only:
                raise ProviderError('The retained EKS staging PVC disappeared')
            # WaitForFirstConsumer plus the worker's required zone affinity
            # binds the disk to its physical AZ; never create a shared RWX pool.
            if _authorize is not None:
                _authorize()
            self._request('POST', self._base + '/persistentvolumeclaims', {
                'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim',
                'metadata': {'name': name, 'namespace': self.namespace, 'labels': labels,
                             'annotations': {'nabedra.io/retain': 'true', 'nabedra.io/zone': zone}},
                'spec': {'accessModes': _WRITER, 'volumeMode': 'Filesystem',
                         'storageClassName': self.storage_class, 'resources': {'requests': {'storage': str(bytes)}}}}, conflict=True)
            record = self._request('GET', path)
        self._stateful_owned(record, labels)
        self._retained_pvc(record, name=name, worker_id=worker_id, zone=zone, pool_id=pool_id)
        if (_staging is not None and self._quantity_bytes(record['spec']['resources']['requests']['storage']) != bytes):
            raise ProviderError('EKS staging PVC size differs from the durable claim')
        return dict(resource_id=self.namespace + '/' + name + '/' + record['metadata']['uid'],
                    pvc_name=name, state=record.get('status', {}).get('phase', 'Pending'))

    @_bounded_call
    def expand_pool(self, *, resource_id, target_bytes, worker, pool_id, zone, volume_id):
        if type(target_bytes) is not int or target_bytes <= 0:
            raise ProviderError('Positive EKS target bytes are required')
        self._storage_class()
        path, record = self._pvc(resource_id, worker_id=worker['worker_id'], zone=zone, pool_id=pool_id)
        self._bound_volume(record, zone=zone, expected=volume_id)
        previous = self._quantity_bytes(record['spec']['resources']['requests']['storage'])
        if target_bytes > previous:
            # UID and resourceVersion from the authenticated read are included
            # in the update. A conflict or missing reply resumes by observing
            # this immutable claim and PVC again, never by blind retry.
            record['spec']['resources']['requests']['storage'] = str(target_bytes)
            self._request('PUT', path, record)
        return dict(resource_id=volume_id, state='awaiting_filesystem')

    def _retained_pvc(self, record, *, name, worker_id=None, zone=None, pool_id=None):
        labels = {'nabedra.io/cluster': self.cluster_id}
        if worker_id is not None:
            labels['nabedra.io/worker'] = identity(worker_id)
        if pool_id is not None:
            labels['nabedra.io/pool'] = identity(pool_id)
        self._stateful_owned(record, labels)
        meta, spec = record['metadata'], record.get('spec', {})
        annotations = meta.get('annotations', {})
        if (meta.get('name') != name or meta.get('deletionTimestamp') or meta.get('ownerReferences')
                or annotations.get('nabedra.io/retain') != 'true'
                or (zone is not None and annotations.get('nabedra.io/zone') != zone)
                or spec.get('storageClassName') != self.storage_class or spec.get('accessModes') != _WRITER
                or spec.get('volumeMode', 'Filesystem') != 'Filesystem'):
            raise ProviderError('EKS PVC does not satisfy retained single-Pod storage ownership')
        return record

    def _pvc(self, resource_id, *, worker_id, zone, pool_id=None):
        _, name, uid = self._resource_parts(resource_id)
        if not name.startswith('nabedra-pool-') or (pool_id is not None and name != 'nabedra-pool-' + pool_id):
            raise ProviderError('EKS PVC name differs from the registered pool')
        path = self._base + '/persistentvolumeclaims/' + name
        record = self._request('GET', path)
        self._exact(record, uid)
        return path, self._retained_pvc(record, name=name, worker_id=worker_id, zone=zone, pool_id=pool_id)

    def _bound_volume(self, pvc, *, zone, expected=None):
        name = pvc.get('spec', {}).get('volumeName')
        if (pvc.get('status', {}).get('phase') != 'Bound' or not isinstance(name, str)
                or not re.fullmatch(r'[a-z0-9][.a-z0-9-]{0,252}', name)):
            raise ProviderError('Exact bound EKS PVC is required')
        volume = self._request('GET', '/api/v1/persistentvolumes/' + name)
        meta, spec = volume.get('metadata', {}), volume.get('spec', {})
        ref, csi = spec.get('claimRef', {}), spec.get('csi', {})
        if expected is None:
            expected = csi.get('volumeHandle')
        claim = pvc['metadata']
        zones = spec.get('nodeAffinity', {}).get('required', {}).get('nodeSelectorTerms', [])
        zone_bound = zones and all(any(expr.get('key') in _ZONE_KEYS and expr.get('operator') == 'In'
            and expr.get('values') == [zone] for expr in term.get('matchExpressions', [])) for term in zones)
        if (meta.get('name') != name or not meta.get('uid') or meta.get('deletionTimestamp')
                or any(ref.get(key) != claim[key] for key in ('namespace', 'name', 'uid'))
                or volume.get('status', {}).get('phase') != 'Bound'
                or spec.get('storageClassName') != self.storage_class or spec.get('accessModes') != _WRITER
                or spec.get('volumeMode', 'Filesystem') != 'Filesystem'
                or spec.get('persistentVolumeReclaimPolicy') != 'Retain' or not zone_bound
                or not isinstance(expected, str) or not re.fullmatch(r'vol-(?:[a-f0-9]{8}|[a-f0-9]{17})', expected)
                or csi.get('driver') != _DRIVER or csi.get('volumeHandle') != expected or csi.get('readOnly') is True):
            raise ProviderError('EKS PV no longer binds the exact retained same-zone EBS volume')
        return volume

    @staticmethod
    def _quantity_bytes(value):
        match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)([KMGTPE]i?|m)?', str(value))
        if not match:
            raise ProviderError('Unsupported PVC storage quantity')
        suffix = match[2] or ''
        power = {'': 1, 'm': Decimal('.001')}
        power.update({unit: 1000 ** (i + 1) for i, unit in enumerate('KMGTPE')})
        power.update({unit + 'i': 1024 ** (i + 1) for i, unit in enumerate('KMGTPE')})
        try:
            amount = Decimal(match[1]) * power[suffix]
        except (InvalidOperation, KeyError):
            raise ProviderError('Unsupported PVC storage quantity') from None
        if amount != int(amount) or not 0 < amount <= 2**63 - 1:
            raise ProviderError('PVC storage quantity is outside its byte range')
        return int(amount)

    def _resource_parts(self, value):
        try:
            namespace, name, uid = value.split('/')
        except (AttributeError, ValueError):
            raise ProviderError('Exact namespace/name/UID is required') from None
        if namespace != self.namespace or not re.fullmatch(r'nabedra-[a-z0-9][-a-z0-9]{0,240}', name) or not uid:
            raise ProviderError('EKS stateful resource identity differs')
        return namespace, name, uid

    def _exact(self, record, uid):
        meta = record.get('metadata', {})
        if (meta.get('namespace') != self.namespace or meta.get('uid') != uid
                or not meta.get('resourceVersion') or meta.get('deletionTimestamp')
                or meta.get('labels', {}).get('nabedra.io/cluster') != self.cluster_id):
            raise ProviderError('EKS stateful UID or cluster changed')

    @_bounded_call
    def ensure_worker(self, *, claim_id, worker_id, incarnation, zone, bootstrap_secret, pools,
                      cpu_millicores, memory_bytes, enrollment=None):
        return self._ensure_worker(claim_id=claim_id, worker_id=worker_id, incarnation=incarnation, zone=zone,
                                   bootstrap_secret=bootstrap_secret, pools=pools, cpu_millicores=cpu_millicores,
                                   memory_bytes=memory_bytes, enrollment=enrollment, replacement=None)

    def _ensure_worker(self, *, claim_id, worker_id, incarnation, zone, bootstrap_secret, pools,
                       cpu_millicores, memory_bytes, enrollment, replacement,
                       staging=None, authorize=None, existing_only=False):
        identity(worker_id)
        identity(incarnation)
        if (not isinstance(pools, list) or not 1 <= len(pools) <= 64
                or type(cpu_millicores) is not int or cpu_millicores <= 0
                or type(memory_bytes) is not int or memory_bytes <= 0
                or not isinstance(bootstrap_secret, str) or not re.fullmatch(r'[a-z0-9][.a-z0-9-]{0,252}', bootstrap_secret)
                or not isinstance(zone, str) or not zone):
            raise ProviderError('Bounded worker pools, resources and bootstrap Secret are required')
        name = 'nabedra-worker-' + worker_id
        content = dict(worker_id=worker_id, incarnation=incarnation,
            zone=zone, bootstrap_secret=bootstrap_secret, pools=pools, cpu=cpu_millicores, memory=memory_bytes,image=self._image)
        if replacement is not None:
            content['replacement'] = identity(replacement)
        if staging is not None:
            content['staging'] = staging
        labels = self._stateful_labels(claim_id, content)
        labels['nabedra.io/worker'] = worker_id
        labels['nabedra.io/incarnation'] = incarnation
        annotations = {}
        if enrollment is not None:
            enrollment_labels = _enrollment_labels(enrollment)
            if enrollment_labels['nabedra.io/worker'] != worker_id or enrollment_labels['nabedra.io/incarnation'] != incarnation:
                raise ProviderError('EKS bootstrap and enrollment worker identities differ')
            labels.update(enrollment_labels)
            annotations['nabedra.io/certificate'] = enrollment['certificate_sha256']
        volumes = [{'name': 'bootstrap-source', 'secret': {'secretName': bootstrap_secret, 'defaultMode': 256}},
                   {'name': 'bootstrap', 'emptyDir': {'medium': 'Memory', 'sizeLimit': '16Mi'}}]
        mounts = [{'name': 'bootstrap', 'mountPath': '/nabedra', 'readOnly': True}]
        if replacement is not None:
            annotations['nabedra.io/replacement'] = replacement
            volumes.append({'name': 'replacement-identity', 'projected': {'defaultMode': 0o440,
                'sources': [{'serviceAccountToken': {'audience': audience(self.cluster_id),
                            'expirationSeconds': 600, 'path': 'token'}}]}})
            mounts.append({'name': 'replacement-identity', 'mountPath': _TOKEN_PATH.rpartition('/')[0], 'readOnly': True})
        paths, claims, uuids = set(), set(), set()
        for index, pool in enumerate(pools):
            try:
                if set(pool) != {'resource_id', 'mount_path', 'filesystem_uuid'}:
                    raise ValueError
                _, pvc_name, _ = self._resource_parts(pool['resource_id'])
                mount_path = pool['mount_path']
                uuid = identity(pool['filesystem_uuid'])
                if (pvc_name in claims or uuid in uuids or not mount_path.startswith('/') or mount_path == '/'
                        or any(part in ('.', '..', '') for part in mount_path[1:].split('/'))
                        or mount_path.split('/')[1] in ('dev', 'proc', 'sys', 'nabedra')
                        or mount_path in paths):
                    raise ValueError
            except (KeyError, TypeError, AttributeError, ValueError):
                raise ProviderError('EKS requires distinct retained PVC UIDs, filesystem UUIDs and mount paths') from None
            self._pvc(pool['resource_id'], worker_id=worker_id, zone=zone)
            paths.add(mount_path)
            claims.add(pvc_name)
            uuids.add(uuid)
            storage_name = 'pool-' + str(index)
            device_name = 'pool-device-' + str(index)
            device_path = '/dev/disk/by-uuid/' + uuid
            volumes.extend([{'name': storage_name, 'persistentVolumeClaim': {'claimName': pvc_name}},
                            {'name': device_name, 'hostPath': {'path': device_path, 'type': 'BlockDevice'}}])
            mounts.extend([{'name': storage_name, 'mountPath': mount_path},
                           {'name': device_name, 'mountPath': device_path, 'readOnly': True}])
        path = self._apps + '/statefulsets/' + name
        record = self._request('GET', path, missing=True)
        if record is None and existing_only:
            raise ProviderError('The retained EKS staging StatefulSet disappeared')
        if record is None or staging is not None:
            service_path=self._base+'/services/'+name
            service=self._request('GET',service_path,missing=True)
            if service is None:
                if record is not None:
                    raise ProviderError('The retained EKS staging peer Service disappeared')
                if authorize is not None:
                    authorize()
                self._request('POST',self._base+'/services',{'apiVersion':'v1','kind':'Service',
                    'metadata':{'name':name,'namespace':self.namespace,'labels':labels},
                    'spec':{'clusterIP':'None','publishNotReadyAddresses':True,
                        'selector':{'nabedra.io/worker':worker_id},'ports':[{'name':'peer','port':9000,'targetPort':9000}]}},conflict=True)
                service=self._request('GET',service_path)
            if replacement is None:
                self._stateful_owned(service, labels)
                if staging is not None and (service['metadata'].get('name') != name
                        or service['metadata'].get('deletionTimestamp') or service['metadata'].get('ownerReferences')
                        or not _matches(service.get('spec'), {'clusterIP': 'None', 'publishNotReadyAddresses': True,
                            'ports': [{'name': 'peer', 'port': 9000, 'targetPort': 9000}]})
                        or service['spec'].get('selector') != {'nabedra.io/worker': worker_id}):
                    raise ProviderError('EKS staging requires the exact independent peer Service')
            else:
                self._stateful_owned(service, {key: labels[key] for key in
                                               ('nabedra.io/cluster', 'nabedra.io/claim', 'nabedra.io/worker')})
                service_spec = service.get('spec', {})
                if (service['metadata'].get('name') != name or service['metadata'].get('deletionTimestamp')
                        or service['metadata'].get('ownerReferences') or service_spec.get('clusterIP') != 'None'
                        or service_spec.get('selector') != {'nabedra.io/worker': worker_id}
                        or service_spec.get('publishNotReadyAddresses') is not True):
                    raise ProviderError('EKS replacement requires the exact retained peer Service')
        spec = {'replicas': 1, 'serviceName': name, 'updateStrategy': {'type': 'OnDelete'},
            'persistentVolumeClaimRetentionPolicy': {'whenDeleted': 'Retain', 'whenScaled': 'Retain'},
            'selector': {'matchLabels': {'nabedra.io/worker': worker_id}},
            'template': {'metadata': {'labels': labels, 'annotations': annotations}, 'spec': {
                'terminationGracePeriodSeconds': 120, 'automountServiceAccountToken': False,
                'securityContext':{'runAsUser':1000,'runAsGroup':1000,'fsGroup':1000},
                'initContainers':[{'name':'private-bootstrap','image':self._image,
                    'command':['python3','-c',_PRIVATE_CONFIG],
                    'securityContext':{'runAsUser':0,'runAsGroup':0},
                    'resources':{'requests':{'cpu':'100m','memory':'32Mi'},'limits':{'cpu':'250m','memory':'64Mi'}},
                    'volumeMounts':[{'name':'bootstrap-source','mountPath':'/secret','readOnly':True},
                                    {'name':'bootstrap','mountPath':'/private'}]}],
                'nodeSelector': {'topology.kubernetes.io/zone': zone},
                'affinity': {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [{
                    'labelSelector': {'matchLabels': {'nabedra.io/cluster': self.cluster_id}},
                    'topologyKey': 'kubernetes.io/hostname'}]}},
                'volumes': volumes, 'containers': [{'name': 'worker', 'image': self._image,
                    'command': ['nabedra-db', 'data-worker' if staging is not None else 'worker',
                                '--bootstrap', '/nabedra/bootstrap.json'],
                    'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
                    'volumeMounts': mounts, 'resources': {
                        'requests': {'cpu': str(cpu_millicores) + 'm', 'memory': str(memory_bytes)},
                        'limits': {'cpu': str(cpu_millicores) + 'm', 'memory': str(memory_bytes)}}}]}}}
        if replacement is not None:
            spec['template']['spec']['serviceAccountName'] = _SERVICE_ACCOUNT
        if record is None:
            if authorize is not None:
                authorize()
            self._request('POST', self._apps + '/statefulsets', {'apiVersion': 'apps/v1', 'kind': 'StatefulSet',
                'metadata': {'name': name, 'namespace': self.namespace, 'labels': labels,
                             'annotations': annotations}, 'spec': spec}, conflict=True)
            record = self._request('GET', path)
        self._stateful_owned(record, labels)
        self._worker_spec(record, name, labels, annotations)
        result = dict(resource_id=self.namespace + '/' + name + '/' + record['metadata']['uid'],
                      state='awaiting_database_ready')
        if staging is not None:
            actual = record['spec']
            if (record['metadata'].get('ownerReferences') or actual.get('ordinals', {}).get('start', 0) != 0
                    or actual.get('selector') != spec['selector'] or not _matches(actual, spec)
                    or not _staging_spec(actual['template']['spec'], spec['template']['spec'])):
                raise ProviderError('EKS staged StatefulSet differs from the exact worker template')
            result.update(stateful=record, labels=labels, annotations=annotations, pod_spec=spec['template']['spec'])
        return result

    def _worker_spec(self, record, name, labels, annotations, *, deleting=False):
        spec = record.get('spec', {})
        metadata = spec.get('template', {}).get('metadata', {})
        if (record['metadata'].get('name') != name or (record['metadata'].get('deletionTimestamp') and not deleting)
                or spec.get('updateStrategy', {}).get('type') != 'OnDelete' or spec.get('replicas') != 1
                or spec.get('persistentVolumeClaimRetentionPolicy') != {'whenDeleted': 'Retain', 'whenScaled': 'Retain'}
                or spec.get('volumeClaimTemplates')
                or any(metadata.get('labels', {}).get(k) != v for k, v in labels.items())
                or any(record['metadata'].get('annotations', {}).get(k) != v
                       or metadata.get('annotations', {}).get(k) != v for k, v in annotations.items())):
            raise ProviderError('EKS worker identity or fixed retained StatefulSet configuration changed')

    def _staged_ebs(self, volume_id, zone, size_gib):
        if (self._staging_ec2 is None or not re.fullmatch(re.escape(self._staging_region) + r'[a-z]', zone)
                or self._calls.until - time.monotonic() < self._staging_aws_window):
            raise ProviderError('EKS staging requires live EBS observation in the configured region')
        try:
            reply = self._staging_ec2.describe_volumes(VolumeIds=[volume_id])
            volume, = reply['Volumes']
            if (reply.get('NextToken') or volume['VolumeId'] != volume_id or volume['AvailabilityZone'] != zone
                    or volume['Encrypted'] is not True or volume.get('MultiAttachEnabled', False) is not False
                    or type(volume['Size']) is not int or volume['Size'] != size_gib
                    or volume['State'] not in ('available', 'in-use') or not isinstance(volume['Attachments'], list)
                    or any(not isinstance(item, dict) for item in volume['Attachments'])
                    or time.monotonic() >= self._calls.until):
                raise ValueError
        except Exception:
            raise ProviderError('EKS staging could not observe the exact encrypted EBS volume and size') from None
        return volume

    def _staged_pod(self, staged, *, zone, pvc, pv, size_gib, expected=None, ready=False):
        stateful = staged['stateful']
        owner = stateful['metadata']
        name, uid = owner['name'], identity(owner['uid'])
        pod_path = self._base + '/pods/' + name + '-0'
        pod = self._request('GET', pod_path, missing=True)
        if pod is None:
            return None
        meta = self._stateful_owned(pod, staged['labels'])['metadata']
        pod_uid = identity(meta['uid'])
        owners = meta.get('ownerReferences', [])
        reference = {'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': name, 'uid': uid, 'controller': True}
        spec = pod.get('spec', {})
        if (meta.get('name') != name + '-0' or meta.get('deletionTimestamp') or not _matches(owners, [reference])
                or not _matches(meta.get('annotations'), staged['annotations'])
                or not _staging_spec(spec, staged['pod_spec'])):
            raise ProviderError('EKS candidate Pod differs from the exact staged worker')
        observed = {'kind': 'eks', 'namespace': self.namespace, 'statefulset_uid': uid, 'pod_uid': pod_uid,
                    'pvc_uid': identity(pvc['metadata']['uid']), 'pv_uid': identity(pv['metadata']['uid'])}
        if expected is not None and observed != expected:
            raise ProviderError('EKS candidate Pod or storage UID changed')
        phase = pod.get('status', {}).get('phase')
        node_name = spec.get('nodeName')
        if phase not in ('Pending', 'Running') or (ready and phase != 'Running'):
            if phase in (None, 'Pending'):
                return None
            raise ProviderError('EKS candidate Pod is unavailable')
        if not node_name:
            return None
        if not isinstance(node_name, str) or not re.fullmatch(r'[a-z0-9][.a-z0-9-]{0,252}', node_name):
            raise ProviderError('EKS candidate Pod has an invalid Node name')
        node_path = '/api/v1/nodes/' + quote(node_name, safe='')
        node = self._request('GET', node_path)
        node_meta = node.get('metadata', {})
        provider_id = node.get('spec', {}).get('providerID')
        instance = isinstance(provider_id, str) and re.fullmatch(
            r'aws:///' + re.escape(zone) + r'/(i-(?:[a-f0-9]{8}|[a-f0-9]{17}))', provider_id)
        if (node_meta.get('name') != node_name or node_meta.get('deletionTimestamp')
                or node_meta.get('labels', {}).get(_ZONE) != zone or not instance):
            raise ProviderError('EKS candidate is not on the exact same-zone EC2 Node')
        identity(node_meta.get('uid'))
        volume_id = pv['spec']['csi']['volumeHandle']
        volume = self._staged_ebs(volume_id, zone, size_gib)
        attachments = volume['Attachments']
        if not attachments:
            return None
        if (len(attachments) != 1 or attachments[0].get('InstanceId') != instance[1]
                or attachments[0].get('VolumeId') != volume_id):
            raise ProviderError('EKS candidate EBS volume is attached outside the exact Node')
        if volume['State'] != 'in-use' or attachments[0].get('State') != 'attached':
            return None
        if attachments[0].get('DeleteOnTermination') is not False:
            raise ProviderError('EKS candidate EBS volume is not retained on Node termination')
        confirmations = ((pod_path, meta), (self._apps + '/statefulsets/' + name, owner), (node_path, node_meta))
        for path, previous in confirmations:
            final = self._request('GET', path, missing=True)
            current = final.get('metadata', {}) if final is not None else {}
            if (not previous.get('resourceVersion') or current.get('uid') != previous['uid']
                    or current.get('resourceVersion') != previous['resourceVersion'] or current.get('deletionTimestamp')):
                raise ProviderError('EKS Pod, StatefulSet or Node changed during EBS observation')
        return _native_target(observed)

    @staticmethod
    def _stop_storage(spec, expected, pvc_name, zone):
        volumes = spec.get('volumes', [])
        claims = [volume for volume in volumes if 'persistentVolumeClaim' in volume]
        workers = spec.get('containers', [])
        grace = spec.get('terminationGracePeriodSeconds')
        if (len(claims) != 1 or len(workers) != 1 or workers[0].get('name') != 'worker'
                or spec.get('ephemeralContainers') or type(grace) is not int or grace <= 0
                or spec.get('nodeSelector', {}).get(_ZONE) != zone
                or any('ephemeral' in volume for volume in volumes)
                or claims[0]['persistentVolumeClaim'].get('claimName') != pvc_name
                or claims[0]['persistentVolumeClaim'].get('readOnly') is True):
            raise ProviderError('EKS worker does not have the exact single retained claim')
        volume_name = claims[0].get('name')
        device_path = '/dev/disk/by-uuid/' + expected['filesystem_uuid']
        devices = [volume for volume in volumes if 'hostPath' in volume]
        if len(devices) != 1 or devices[0]['hostPath'] != {'path': device_path, 'type': 'BlockDevice'}:
            raise ProviderError('EKS Pod does not have the exact filesystem projection')
        device_name = devices[0].get('name')
        mounts = workers[0].get('volumeMounts', [])
        mounted = [mount for mount in mounts if mount.get('name') == volume_name
                   or mount.get('mountPath') == expected['mount_path']]
        device_mounts = [mount for mount in mounts if mount.get('name') == device_name
                         or mount.get('mountPath') == device_path]
        if (not volume_name or not device_name or len(mounted) != 1 or mounted[0].get('name') != volume_name
                or mounted[0].get('mountPath') != expected['mount_path']
                or any(mounted[0].get(key) for key in ('readOnly', 'subPath', 'subPathExpr'))
                or mounted[0].get('mountPropagation') not in (None, 'None')
                or len(device_mounts) != 1 or device_mounts[0].get('name') != device_name
                or device_mounts[0].get('mountPath') != device_path or device_mounts[0].get('readOnly') is not True
                or workers[0].get('volumeDevices')
                or any(init.get('volumeDevices') or any(mount.get('name') in (volume_name, device_name)
                    for mount in init.get('volumeMounts', [])) for init in spec.get('initContainers', []))):
            raise ProviderError('EKS Pod mount and filesystem projection differ from the closed mount')

    @staticmethod
    def _delete_exact(record):
        metadata = record['metadata']
        return {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'propagationPolicy': 'Orphan',
                'preconditions': {'uid': metadata['uid'], 'resourceVersion': metadata['resourceVersion']}}

    @classmethod
    def _stop_storage_set(cls, spec, retained, zone):
        if len(retained) == 1:
            expected, pvc_name = retained[0]
            cls._stop_storage(spec, expected, pvc_name, zone)
            return
        volumes = spec.get('volumes', [])
        claims = [volume for volume in volumes if 'persistentVolumeClaim' in volume]
        devices = [volume for volume in volumes if 'hostPath' in volume]
        if (len(claims) != len(retained) or len(devices) != len(retained)
                or len({volume.get('name') for volume in volumes}) != len(volumes)):
            raise ProviderError('EKS Pod does not have the exact retained PVC and filesystem set')
        other = [volume for volume in volumes if volume not in claims and volume not in devices]
        for expected, pvc_name in retained:
            path = '/dev/disk/by-uuid/' + expected['filesystem_uuid']
            claim = [volume for volume in claims if volume['persistentVolumeClaim'].get('claimName') == pvc_name]
            device = [volume for volume in devices if volume['hostPath'].get('path') == path]
            if len(claim) != 1 or len(device) != 1:
                raise ProviderError('EKS Pod does not have the exact retained claim and filesystem projection')
            cls._stop_storage(spec | {'volumes': other + claim + device}, expected, pvc_name, zone)

    @_bounded_call
    def stop_worker(self, *, claim_id, resource_id, pod_uid):
        """Remove the exact drained StatefulSet and then its exact orphaned Pod."""
        if self.lifecycle_authority is None:
            raise ProviderError('Authenticated Control is required before stopping an EKS worker')
        record, zone = self.lifecycle_authority._closed_worker(
            claim_id, self.cluster_id, 'eks', deadline=self._calls.until)
        intent = record['intent']
        worker, target = intent['worker'], intent['native_target']
        namespace, name, uid = self._resource_parts(resource_id)
        if (namespace != target['namespace'] or uid != target['statefulset_uid']
                or name != 'nabedra-worker-' + worker['worker_id'] or pod_uid != target['pod_uid']):
            raise ProviderError('EKS stop permit names a different StatefulSet or Pod')
        pools = _closed_worker_mounts(record, self.cluster_id, target)
        storage = [(expected, 'nabedra-pool-' + expected['identity']['pool_id'])
                   for expected, _, _, _ in pools]
        labels = _enrollment_labels(worker) | {'nabedra.io/cluster': self.cluster_id,
                                               'nabedra.io/claim': record['growth_claim']}
        annotations = {'nabedra.io/certificate': worker['certificate_sha256']}
        owner = {'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': name, 'uid': uid,
                 'controller': True}
        stateful_path = self._apps + '/statefulsets/' + name
        pod_name = name + '-0'
        pod_path = self._base + '/pods/' + pod_name

        self._storage_class()

        def check_retained():
            for (expected, claim, pvc_uid, pv_uid), (_, pvc_name) in zip(pools, storage):
                _, pvc = self._pvc('/'.join((namespace, pvc_name, pvc_uid)),
                                   worker_id=worker['worker_id'], zone=zone, pool_id=expected['identity']['pool_id'])
                pv = self._bound_volume(pvc, zone=zone, expected=expected['provider_volume_id'])
                if pvc['metadata'].get('labels', {}).get('nabedra.io/claim') != claim or pv['metadata']['uid'] != pv_uid:
                    raise ProviderError('EKS stop permit differs from the exact retained PVC or PV')

        check_retained()
        stateful = self._request('GET', stateful_path, missing=True)
        pod = self._request('GET', pod_path, missing=True)
        if stateful is not None:
            metadata = self._stateful_owned(stateful, labels)['metadata']
            self._worker_spec(stateful, name, labels, annotations, deleting=True)
            spec = stateful['spec']
            if (metadata['uid'] != uid or metadata.get('ownerReferences')
                    or spec.get('ordinals', {}).get('start', 0) != 0
                    or spec.get('selector') != {'matchLabels': {'nabedra.io/worker': worker['worker_id']}}):
                raise ProviderError('EKS stop requires the exact independent StatefulSet')
            self._stop_storage_set(spec['template']['spec'], storage, zone)
        pod_owners = []
        if pod is not None:
            metadata = self._stateful_owned(pod, labels)['metadata']
            pod_owners = metadata.get('ownerReferences', [])
            if (metadata['uid'] != pod_uid or metadata.get('name') != pod_name
                    or metadata.get('annotations', {}).get('nabedra.io/certificate') != annotations['nabedra.io/certificate']
                    or (stateful is not None and not pod_owners and not stateful['metadata'].get('deletionTimestamp'))
                    or (pod_owners and (len(pod_owners) != 1
                        or any(pod_owners[0].get(key) != value for key, value in owner.items())))
                    or (pod.get('status', {}).get('phase') == 'Running' and not pod.get('spec', {}).get('nodeName'))):
                raise ProviderError('EKS stop requires the exact enrolled StatefulSet Pod')
            self._stop_storage_set(pod.get('spec', {}), storage, zone)

        state = 'stopping'
        if stateful is not None:
            if not stateful['metadata'].get('deletionTimestamp'):
                # Orphan before deleting the Pod by UID. Cascading or scaling
                # could stop a new Pod created after our identity check.
                self._request('DELETE', stateful_path, self._delete_exact(stateful))
        elif pod is not None:
            if not pod_owners and not pod['metadata'].get('deletionTimestamp'):
                self._request('DELETE', pod_path, self._delete_exact(pod))
        else:
            state = 'stopped'
        check_retained()
        result = {'resource_id': resource_id, 'pod_uid': pod_uid, 'state': state,
                  'retained_volume_id': pools[0][0]['provider_volume_id']}
        if len(pools) > 1:
            result['retained_volume_ids'] = [mount['expected']['provider_volume_id']
                                             for mount in record['phase']['closed']['native_mounts']]
        return result

    @_bounded_call
    def _inspect_live_worker(self, *, resource_id, pod_uid, worker):
        _, name, uid = self._resource_parts(resource_id)
        labels = _enrollment_labels(worker) | {'nabedra.io/cluster': self.cluster_id}
        annotations = {'nabedra.io/certificate': worker['certificate_sha256']}
        if name != 'nabedra-worker-' + worker['worker_id'] or not pod_uid:
            raise ProviderError('EKS requires exact StatefulSet and Pod UIDs for the current worker')
        stateful = self._request('GET', self._apps + '/statefulsets/' + name)
        self._exact(stateful, uid)
        self._stateful_owned(stateful, labels)
        self._worker_spec(stateful, name, labels, annotations)
        pod_name = name + '-0'
        pod = self._request('GET', self._base + '/pods/' + pod_name)
        self._exact(pod, pod_uid)
        self._stateful_owned(pod, labels)
        meta, spec = pod['metadata'], pod.get('spec', {})
        owners = [owner for owner in meta.get('ownerReferences', []) if owner.get('controller') is True]
        if (meta.get('name') != pod_name or len(owners) != 1
                or any(owners[0].get(key) != value for key, value in {
                       'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': name, 'uid': uid}.items())
                or meta.get('annotations', {}).get('nabedra.io/certificate') != annotations['nabedra.io/certificate']
                or pod.get('status', {}).get('phase') != 'Running'
                or not spec.get('nodeName') or not spec.get('nodeSelector', {}).get(_ZONE)
                or spec['nodeSelector'][_ZONE] != stateful['spec']['template']['spec'].get('nodeSelector', {}).get(_ZONE)):
            raise ProviderError('EKS Pod no longer binds the expected worker generation and zone')
        return stateful, pod

    @_bounded_call
    def _retained_inventory(self):
        def collect(path):
            records, continuations = {}, set()
            query = {'labelSelector': 'nabedra.io/cluster=' + self.cluster_id, 'limit': '128'}
            for _ in range(32):
                page = self._request('GET', path + '?' + urlencode(query))
                for record in page.get('items', []):
                    self._stateful_owned(record, {'nabedra.io/cluster': self.cluster_id})
                    uid = record['metadata']['uid']
                    if uid in records or len(records) >= 4096:
                        raise ProviderError('EKS retained inventory changed during paging')
                    records[uid] = record
                cursor = page.get('metadata', {}).get('continue')
                if not cursor:
                    return records
                if cursor in continuations or len(cursor) > 4096:
                    raise ProviderError('EKS retained inventory cursor is invalid')
                query['continue'] = cursor
                continuations.add(cursor)
            raise ProviderError('EKS retained inventory page limit reached')

        self._storage_class()
        return collect(self._apps + '/statefulsets'), collect(self._base + '/persistentvolumeclaims')

    @_bounded_call
    def replace_worker_mounts(self, *, claim_id, resource_id, expected_pod_uid, template, projected_token=None):
        """Reconcile the closed worker, durable exclusion and exact projected replacement Pod."""
        return reconcile(self, claim_id=claim_id, resource_id=resource_id, expected_pod_uid=expected_pod_uid,
                         template=template, projected_token=projected_token)

    def start_node(self, payload):
        raise ProviderError('OLTP EKS requires ensure_worker with retained explicit pools and a database growth claim')

    def stop_node(self, resource_id):
        raise ProviderError('OLTP EKS worker removal requires the database drain controller and exact Pod UID')

    def list_nodes(self):
        raise ProviderError('OLTP EKS worker state requires exact StatefulSet and enrolled Pod inventory')
