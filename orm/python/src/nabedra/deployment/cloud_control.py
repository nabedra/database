"""Authenticated Control reads used by the cloud adapters."""
from ipaddress import ip_address
import re
import time
from urllib.parse import urlsplit

from .._control_transport import _ControlTransport
from ..providers import ProviderError
from .docker import identity


class _LifecycleTransport(_ControlTransport):
    _mutations = frozenset({'worker.drain', 'worker.replacement.apply'})
    _operations = _mutations | {'storage.pool.detach_permit', 'worker.drain.status', 'worker.stop_permit',
                                'worker.replacement.check', 'worker.replacement.status', 'worker.replacement.challenge',
                                'storage.growth.status'}


class CloudLifecycleControl:
    """Request cloud drains and read their capabilities from protected Control."""

    def __init__(self, *, api_url, admin_token, ca_file=None, timeout=20):
        url = urlsplit(api_url)
        if url.scheme != 'https':
            try:
                if url.scheme != 'http' or not ip_address(url.hostname).is_loopback:
                    raise ValueError
            except (TypeError, ValueError):
                raise ProviderError('Cloud lifecycle requires authenticated HTTPS or numeric loopback') from None
        self._transport = _LifecycleTransport(api_url, admin_token, ca_file=ca_file, timeout=timeout)
        self._timeout = timeout

    def begin_worker_drain(self, drain_id, *, worker, run_id, native_target, replacement_growth_claim=None,
                           retained_replacement_drain=None):
        """Request the exact target previously bound to the worker's mount enrollment."""
        ec2_worker_tags(worker)
        target = _native_target(native_target)
        payload = {'worker': worker, 'run_id': identity(run_id), 'native_target': target}
        if replacement_growth_claim is not None:
            if target['kind'] != 'eks':
                raise ProviderError('A replacement growth claim requires an EKS worker')
            payload['replacement_growth_claim'] = identity(replacement_growth_claim)
        if retained_replacement_drain is not None:
            if target['kind'] != 'eks' or replacement_growth_claim is not None:
                raise ProviderError('A retained replacement drain requires an EKS stop without an AddPool claim')
            payload['retained_replacement_drain'] = identity(retained_replacement_drain)
        _, record = self._transport.request('worker.drain', payload, request_id=identity(drain_id),
                                            deadline=time.monotonic() + self._timeout)
        return record

    def worker_drain_status(self, drain_id):
        return self._read_replacement('worker.drain.status', drain_id, {}, time.monotonic() + self._timeout)

    def check_worker_replacement(self, drain_id, *, next_intent):
        """Compare the next enrollment and retained mount without authorizing startup."""
        _, record = self._transport.request('worker.replacement.check', next_intent, request_id=identity(drain_id),
                                            deadline=time.monotonic() + self._timeout)
        return record

    def worker_replacement_status(self, drain_id):
        return self._eks_replacement_status(drain_id, deadline=time.monotonic() + self._timeout)

    def _read_replacement(self, operation, request_id, payload, deadline):
        _, record = self._transport.request(operation, payload, request_id=identity(request_id), deadline=deadline)
        return record

    def _eks_replacement_status(self, drain_id, *, deadline):
        reply = self._read_replacement('worker.replacement.status', drain_id, {}, deadline)
        if set(reply) != {'replacement'}:
            raise ProviderError('Control returned an invalid EKS replacement record')
        return reply['replacement']

    def _apply_eks_replacement(self, operation_id, action, signer, *, deadline):
        challenge = self._read_replacement('worker.replacement.challenge', operation_id, action, deadline)
        try:
            digest = challenge['digest']
            if challenge['verifier_key'] != signer.public_key or not re.fullmatch(r'[a-f0-9]{64}', digest):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ProviderError('Control challenge differs from the enrolled EKS verifier key') from None
        signed = {'action': action, 'signature': signer._sign(bytes.fromhex(digest))}
        _, record = self._transport.request('worker.replacement.apply', signed, request_id=identity(operation_id), deadline=deadline)
        return record

    def _closed_worker(self, drain_id, cluster_id, kind, *, deadline):
        drain_id = identity(drain_id)
        _, reply = self._transport.request('worker.stop_permit', {}, request_id=drain_id, deadline=deadline)
        try:
            record, zone = reply['record'], reply['native_zone']
            intent, phase = record['intent'], record['phase']
            target = _native_target(intent['native_target'])
            ready = phase['closed']['ready']
            if (record['id'] != drain_id or set(phase) != {'closed'} or target['kind'] != kind
                    or 'docker_container_id' in intent or not isinstance(zone, str) or not 0 < len(zone) <= 128
                    or ready['view']['run_id'] != identity(intent['run_id'])
                    or type(ready['ready_count']) is not int or ready['ready_count'] < 3):
                raise ValueError
            _closed_worker_mounts(record, cluster_id, target)
        except (KeyError, TypeError, ValueError, ProviderError):
            raise ProviderError('Control returned no exact closed cloud worker permit') from None
        return record, zone

    def _closed_pool(self, drain_id, cluster_id, volume_id, *, deadline):
        drain_id = identity(drain_id)
        _, record = self._transport.request('storage.pool.detach_permit', {},
                                            request_id=drain_id, deadline=deadline)
        try:
            expected = _ebs_mount(record['mount'], cluster_id)
            if (record['id'] != drain_id or record['phase'] != 'closed'
                    or expected['provider_volume_id'] != volume_id):
                raise ValueError
            identity(record['growth_claim'])
        except (KeyError, TypeError, ValueError, ProviderError):
            raise ProviderError('Control returned no exact closed EBS pool permit') from None
        return record


def _closed_worker_mounts(record, cluster_id, target):
    """Decode the complete native mount set before an adapter uses a stop permit."""
    closed, intent = record['phase']['closed'], record['intent']
    primary = closed['mount']
    primary_mount = _ebs_mount(primary, cluster_id, intent['worker'])
    claim = identity(record['growth_claim'])
    primary_pool = (primary_mount, claim, target.get('pvc_uid'), target.get('pv_uid'))
    if ('native_mounts' not in closed and 'retained_eks_pool' not in record
            and 'retained_replacement_drain' not in intent):
        return [primary_pool]

    mounts = closed['native_mounts']
    retained = record['retained_eks_pool']
    pool = retained['pool']
    if (target['kind'] != 'eks' or 'replacement_growth_claim' in intent
            or identity(intent['retained_replacement_drain']) == record['id']
            or type(mounts) is not list or len(mounts) != 2 or mounts.count(primary) != 1
            or set(retained) != {'claim_id', 'pool'} or set(pool) != {'mount', 'pvc_uid', 'pv_uid'}
            or identity(retained['claim_id']) == claim):
        raise ValueError
    first, second = [_ebs_mount(mount, cluster_id, intent['worker']) for mount in mounts]
    if (first['identity']['pool_id'] >= second['identity']['pool_id']
            or mounts[0]['mount_id'] == mounts[1]['mount_id']
            or (mounts[0]['device_major'], mounts[0]['device_minor'])
                == (mounts[1]['device_major'], mounts[1]['device_minor'])
            or any(first[key] == second[key] for key in ('provider_volume_id', 'filesystem_uuid', 'mount_path'))
            or first['mount_path'].startswith(second['mount_path'] + '/')
            or second['mount_path'].startswith(first['mount_path'] + '/')):
        raise ValueError
    added = second if mounts[0] == primary else first
    uids = [identity(pool['pvc_uid']), identity(pool['pv_uid']), target['pvc_uid'], target['pv_uid'],
            target['statefulset_uid'], target['pod_uid']]
    if added != pool['mount'] or len(set(uids)) != len(uids):
        raise ValueError
    return [primary_pool, (added, retained['claim_id'], pool['pvc_uid'], pool['pv_uid'])]


def _ebs_mount(mount, cluster_id, worker=None):
    expected = mount['expected']
    pool = expected['identity']
    mounted_worker = expected['worker']
    path = expected['mount_path']
    if (expected['profile'] != 'ebs' or pool['cluster_id'] != cluster_id or pool['engine'] != 'oltp'
            or pool['pool_format_version'] != 1 or pool['worker_id'] != mounted_worker['worker_id']
            or (worker is not None and worker != mounted_worker)
            or not re.fullmatch(r'vol-(?:[a-f0-9]{8}|[a-f0-9]{17})', expected['provider_volume_id'])
            or not isinstance(path, str) or not path.startswith('/') or '\x00' in path
            or any(part in ('', '.', '..') for part in path[1:].split('/'))
            or type(expected['generation']) is not int or expected['generation'] <= 0
            or any(type(mount[key]) is not int or mount[key] < 0
                   for key in ('mount_id', 'device_major', 'device_minor', 'root_inode'))
            or not mount['mount_id'] or not mount['root_inode']):
        raise ValueError
    identity(pool['pool_id'])
    identity(expected['filesystem_uuid'])
    ec2_worker_tags(mounted_worker)
    return expected


def _native_target(target):
    try:
        if target['kind'] == 'ec2':
            if set(target) != {'kind', 'instance_id'} or not re.fullmatch(
                    r'i-(?:[a-f0-9]{8}|[a-f0-9]{17})', target['instance_id']):
                raise ValueError
        elif target['kind'] == 'eks':
            uid_fields = {'statefulset_uid', 'pod_uid', 'pvc_uid', 'pv_uid'}
            if (set(target) != uid_fields | {'kind', 'namespace'}
                    or not re.fullmatch(r'[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?', target['namespace'])
                    or len({identity(target[field]) for field in uid_fields}) != len(uid_fields)):
                raise ValueError
        else:
            raise ValueError
    except (KeyError, TypeError, ValueError, ProviderError):
        raise ProviderError('An exact native EC2 instance or EKS StatefulSet, Pod, PVC and PV is required') from None
    return target


def ec2_worker_tags(worker):
    """Require the complete enrolled identity on an EC2 instance."""
    try:
        revision = worker['enrollment_revision']
        certificate = worker['certificate_sha256']
        if type(revision) is not int or revision < 1 or not re.fullmatch(r'[a-f0-9]{64}', certificate):
            raise ValueError
        return {'nabedra:worker': identity(worker['worker_id']),
                'nabedra:incarnation': identity(worker['incarnation']),
                'nabedra:enrollment-revision': str(revision), 'nabedra:certificate': certificate}
    except (KeyError, TypeError, ValueError, ProviderError):
        raise ProviderError('Exact cloud worker enrollment is required') from None
