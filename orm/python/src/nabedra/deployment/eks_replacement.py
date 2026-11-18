"""Dedicated EKS/EC2 observation and Ed25519 proof for a closed worker."""
from __future__ import annotations

import base64
import json
import os
from hashlib import sha256
import re
import stat
import time
from urllib.parse import quote, urlencode
from uuid import UUID, uuid5

from ..providers import KubernetesProvider, ProviderError
from .cloud_control import _ebs_mount, _native_target, ec2_worker_tags
from .docker import digest, identity

_SERVICE_ACCOUNT = 'nabedra-oltp-worker-replacement'
_TOKEN_PATH = '/var/run/nabedra-replacement/token'
_ZONE = 'topology.kubernetes.io/zone'


def audience(cluster_id):
    return 'nabedra:' + identity(cluster_id) + ':eks-replacement'


def _protected_seed(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            metadata = os.fstat(stream.fileno())
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                    or metadata.st_mode & 0o077 or metadata.st_nlink != 1 or metadata.st_size != 32):
                raise OSError
            seed = stream.read(33)
            if len(seed) != 32:
                raise OSError
            return seed
    except (OSError, TypeError):
        raise ProviderError('EKS replacement signing seed must be a private 32-byte regular file') from None


class EksReplacementVerifier:
    """Reads Kubernetes and AWS using dedicated credentials trusted by enrolled workers."""

    def __init__(self, *, api_url, token, namespace, cluster_id, ec2_client,
                 signing_seed_file, ca_file=None, timeout=10):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

        self._kube = KubernetesProvider(api_url=api_url, token=token, namespace=namespace,
                                        cluster_id=cluster_id, image='unused', ca_file=ca_file, timeout=timeout)
        self._aws = ec2_client
        config = getattr(getattr(ec2_client, 'meta', None), 'config', None)
        connect, read = getattr(config, 'connect_timeout', None), getattr(config, 'read_timeout', None)
        retries = getattr(config, 'retries', None) or {}
        if (type(connect) not in (int, float) or not 0 < connect <= 10
                or type(read) not in (int, float) or not 0 < read <= 20
                or retries.get('mode') != 'standard' or retries.get('total_max_attempts') != 1
                or not re.fullmatch(r'[a-z][a-z0-9-]+-[1-9][0-9]*', getattr(config, 'region_name', '') or '')):
            raise ProviderError('EKS verifier requires a pinned EC2 region and bounded SDK attempts')
        self._region = config.region_name
        self._aws_window = connect + read
        self._key = Ed25519PrivateKey.from_private_bytes(_protected_seed(signing_seed_file))
        self.public_key = self._key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()

    def _sign(self, message):
        signature = self._key.sign(message)
        return [signature[:32].hex(), signature[32:].hex()]

    def _request(self, method, path, *, deadline, body=None, missing=False):
        previous = getattr(self._kube._calls, 'until', None)
        self._kube._calls.until = min(deadline, time.monotonic() + self._kube._timeout)
        try:
            return self._kube._request(method, path, body, missing=missing)
        finally:
            self._kube._calls.until = previous

    def _attachments_absent(self, *, pv_name, deadline):
        revision, cursor, seen = None, None, set()
        for _ in range(64):
            query = {'limit': '128'}
            if cursor:
                query['continue'] = cursor
            result = self._request('GET', '/apis/storage.k8s.io/v1/volumeattachments?' + urlencode(query),
                                   deadline=deadline)
            try:
                metadata = result['metadata']
                current = _revision(metadata['resourceVersion'])
                if revision is not None and current != revision:
                    raise ValueError
                revision = current
                items = result['items']
                if not isinstance(items, list) or len(items) > 128:
                    raise ValueError
                for attachment in items:
                    source = attachment['spec']['source']
                    if not isinstance(source, dict) or bool(source.get('persistentVolumeName')) == bool(source.get('inlineVolumeSpec')):
                        raise ValueError
                    if source.get('persistentVolumeName') == pv_name:
                        return None
                cursor = metadata.get('continue')
                if not cursor:
                    return revision
                if not isinstance(cursor, str) or len(cursor) > 4096 or cursor in seen:
                    raise ValueError
                seen.add(cursor)
            except (KeyError, TypeError, ValueError):
                raise ProviderError('Kubernetes attachment inventory changed or is unreadable') from None
        raise ProviderError('Kubernetes attachment inventory exceeds the reconciliation bound')

    def _exact_ebs(self, volume_id, zone, *, deadline):
        if not zone.startswith(self._region) or deadline - time.monotonic() < self._aws_window:
            raise ProviderError('EKS EBS observation requires the exact region and a live deadline')
        try:
            response = self._aws.describe_volumes(VolumeIds=[volume_id])
            rows = response['Volumes']
            if len(rows) != 1 or response.get('NextToken'):
                raise ValueError
            volume = rows[0]
            if (volume['VolumeId'] != volume_id or volume['AvailabilityZone'] != zone
                    or volume['Encrypted'] is not True or volume.get('MultiAttachEnabled', False) is not False
                    or not isinstance(volume['Attachments'], list)):
                raise ValueError
            request_id = identity(response['ResponseMetadata']['RequestId'])
        except Exception:
            raise ProviderError('EKS verifier could not observe the exact encrypted EBS volume') from None
        return volume, request_id

    def _exclusion(self, provider, resource_id, pod_uid, retained, *, deadline):
        namespace, stateful_name, stateful_uid = provider._resource_parts(resource_id)
        if namespace != self._kube.namespace or retained['target']['statefulset_uid'] != stateful_uid:
            raise ProviderError('EKS verifier received another worker lifecycle')
        pod_name = stateful_name + '-0'
        paths = [provider._apps + '/statefulsets/' + stateful_name, provider._base + '/pods/' + pod_name]
        for path, uid in zip(paths, [stateful_uid, pod_uid], strict=True):
            record = self._request('GET', path, missing=True, deadline=deadline)
            if record is not None:
                if record.get('metadata', {}).get('uid') != uid:
                    raise ProviderError('Another EKS compute identity occupies the retired worker name')
                return None
        pv = _exact_pv(provider, retained['pvc'], retained['expected'], retained['target']['pv_uid'],
                       retained['zone'], retained['claim'])
        pv_name = pv['metadata']['name']
        revision = self._attachments_absent(pv_name=pv_name, deadline=deadline)
        if revision is None:
            return None
        volume, request_id = self._exact_ebs(retained['expected']['provider_volume_id'], retained['zone'], deadline=deadline)
        if volume.get('State') != 'available' or volume['Attachments']:
            return None
        # Recheck both compute identities and the retained binding after the attachment read.
        if any(self._request('GET', path, missing=True, deadline=deadline) is not None for path in paths):
            raise ProviderError('Retired EKS compute identity changed during attachment reconciliation')
        confirmed = _exact_pv(provider, retained['pvc'], retained['expected'], retained['target']['pv_uid'],
                              retained['zone'], retained['claim'])
        if confirmed['metadata']['name'] != pv_name:
            raise ProviderError('Retained EKS persistent volume binding changed')
        return {'attachment_revision': revision, 'describe_volumes_request_id': request_id}

    def _candidate(self, provider, resource_id, projected_token, expected, *, deadline):
        try:
            if (not isinstance(projected_token, str) or not 32 <= len(projected_token) <= 8192
                    or not re.fullmatch(r'[A-Za-z0-9_.-]+', projected_token)):
                raise ValueError
            namespace, name, stateful_uid = provider._resource_parts(resource_id)
            token_audience = audience(provider.cluster_id)
            pod_name = name + '-0'
            pod_path = provider._base + '/pods/' + pod_name
            pod = self._request('GET', pod_path, deadline=deadline)
            pod_uid = identity(pod['metadata']['uid'])
            stateful, verified_pod = provider._inspect_live_worker(resource_id=resource_id, pod_uid=pod_uid,
                                                                    worker=expected['worker'])
            if pod['metadata']['resourceVersion'] != verified_pod['metadata']['resourceVersion']:
                raise ValueError
            for spec in [pod['spec'], stateful['spec']['template']['spec']]:
                _check_candidate_mounts(provider, spec, expected)
            _check_projection(pod['spec'], token_audience)
            _check_projection(stateful['spec']['template']['spec'], token_audience)
            status = self._request('POST', '/apis/authentication.k8s.io/v1/tokenreviews', deadline=deadline,
                                   body={'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenReview',
                                         'spec': {'token': projected_token, 'audiences': [token_audience]}})['status']
            user = status['user']
            if (status.get('authenticated') is not True or status.get('error') or status.get('audiences') != [token_audience]
                    or user['username'] != f'system:serviceaccount:{namespace}:{_SERVICE_ACCOUNT}'):
                raise ValueError
            extra = user['extra']
            node_name = pod['spec']['nodeName']
            if (extra.get('authentication.kubernetes.io/pod-name') != [pod_name]
                    or extra.get('authentication.kubernetes.io/pod-uid') != [pod_uid]
                    or extra.get('authentication.kubernetes.io/node-name') != [node_name]
                    or not re.fullmatch(r'[a-z0-9][.a-z0-9-]{0,252}', node_name)):
                raise ValueError
            node = self._request('GET', '/api/v1/nodes/' + quote(node_name, safe=''), deadline=deadline)
            node_uid = identity(node['metadata']['uid'])
            zone = expected['zone']
            provider_id = node['spec']['providerID']
            instance = re.fullmatch(r'aws:///' + re.escape(zone) + r'/(i-[a-f0-9]{8}|i-[a-f0-9]{17})', provider_id)
            if (extra.get('authentication.kubernetes.io/node-uid') != [node_uid]
                    or node['metadata']['name'] != node_name or node['metadata'].get('deletionTimestamp')
                    or node['metadata']['labels'].get(_ZONE) != zone or instance is None):
                raise ValueError
            for exact_pvc, mount, pv_uid, claim in expected['pools']:
                _exact_pv(provider, exact_pvc, mount, pv_uid, zone, claim)
                volume, _ = self._exact_ebs(mount['provider_volume_id'], zone, deadline=deadline)
                attachments = volume['Attachments']
                if (volume['State'] != 'in-use' or len(attachments) != 1 or attachments[0]['State'] != 'attached'
                        or attachments[0]['InstanceId'] != instance[1]
                        or attachments[0]['VolumeId'] != mount['provider_volume_id']
                        or attachments[0]['DeleteOnTermination'] is not False):
                    raise ValueError
            final = self._request('GET', pod_path, deadline=deadline)['metadata']
            if final.get('uid') != pod_uid or final.get('deletionTimestamp') or final.get('resourceVersion') != pod['metadata']['resourceVersion']:
                raise ValueError
            # Read the expiry of the exact token that the API server authenticated above.
            _, claims, _ = projected_token.split('.')
            expiry = json.loads(base64.urlsafe_b64decode(claims + '=' * (-len(claims) % 4)))['exp']
            if type(expiry) is not int or expiry <= int(time.time()) or expiry >= 1 << 64:
                raise ValueError
            return ({'namespace': namespace, 'statefulset_uid': stateful_uid, 'pod_uid': pod_uid},
                    {'pod_revision': _revision(final['resourceVersion']), 'service_account_uid': identity(user['uid']),
                     'node_uid': node_uid, 'projected_token_sha256': sha256(projected_token.encode('ascii')).hexdigest(),
                     'projected_token_expires_at': expiry})
        except (AttributeError, KeyError, TypeError, IndexError, ValueError):
            raise ProviderError('Projected EKS identity, retained pools or native attachment differs') from None


def _revision(value):
    if not isinstance(value, str) or not re.fullmatch(r'[!-~]{1,128}', value):
        raise ValueError
    return value


def _exact_pv(provider, pvc_resource, mount, pv_uid, zone, claim):
    _, pvc = provider._pvc(pvc_resource, worker_id=mount['worker']['worker_id'], zone=zone,
                           pool_id=mount['identity']['pool_id'])
    pv = provider._bound_volume(pvc, zone=zone, expected=mount['provider_volume_id'])
    if pvc['metadata'].get('labels', {}).get('nabedra.io/claim') != claim or pv['metadata']['uid'] != pv_uid:
        raise ProviderError('EKS replacement PVC/PV identity or original growth claim changed')
    return pv


def _check_projection(spec, token_audience):
    expected = {'sources': [{'serviceAccountToken': {'audience': token_audience, 'expirationSeconds': 600, 'path': 'token'}}],
                'defaultMode': 0o440}
    volume = [item for item in spec['volumes'] if item.get('name') == 'replacement-identity']
    mounts = spec['containers'][0]['volumeMounts']
    projected = [item for item in mounts if item.get('name') == 'replacement-identity'
                 or item.get('mountPath') == _TOKEN_PATH.rpartition('/')[0]]
    if (spec.get('serviceAccountName') != _SERVICE_ACCOUNT or spec.get('automountServiceAccountToken') is not False
            or volume != [{'name': 'replacement-identity', 'projected': expected}]
            or projected != [{'name': 'replacement-identity', 'mountPath': _TOKEN_PATH.rpartition('/')[0], 'readOnly': True}]):
        raise ProviderError('EKS candidate lacks the dedicated projected Pod credential')


def _check_candidate_mounts(provider, spec, expected):
    retained = []
    for resource, mount, _, _ in expected['pools']:
        _, pvc_name, _ = provider._resource_parts(resource)
        retained.append((mount, pvc_name))
    provider._stop_storage_set(spec, retained, expected['zone'])


def reconcile(provider, *, claim_id, resource_id, expected_pod_uid, template, projected_token):
    control, verifier, deadline = provider.lifecycle_authority, provider.replacement_verifier, provider._calls.until
    if control is None or verifier is None:
        raise ProviderError('Controlled EKS replacement requires protected Control and a dedicated verifier')
    drain_id = identity(claim_id)
    record = control._read_replacement('worker.drain.status', drain_id, {}, deadline)
    durable = control._eks_replacement_status(drain_id, deadline=deadline)
    try:
        if set(template) != {'next_intent', 'added', 'bootstrap_secret', 'cpu_millicores', 'memory_bytes'}:
            raise ValueError
        launch_digest = digest(template)
        phase, drain = record['phase'], record['intent']
        old_worker, target = drain['worker'], _native_target(drain['native_target'])
        mount = phase['closed']['mount']
        previous = _ebs_mount(mount, provider.cluster_id, old_worker)
        growth_id, original = identity(drain['replacement_growth_claim']), identity(record['growth_claim'])
        namespace, name, old_stateful_uid = provider._resource_parts(resource_id)
        next_intent, added = template['next_intent'], template['added']
        worker = {key: next_intent['enrollment'][key] for key in old_worker}
        ec2_worker_tags(worker)
        if (record['id'] != drain_id or target['kind'] != 'eks' or target['namespace'] != namespace
                or name != 'nabedra-worker-' + old_worker['worker_id'] or old_stateful_uid != target['statefulset_uid']
                or identity(expected_pod_uid) != target['pod_uid'] or next_intent.get('native_target') is not None
                or next_intent['claim_id'] != original or next_intent['run_id'] != drain['run_id']
                or next_intent['eks_replacement_key'] != verifier.public_key or next_intent['mount']['worker'] != worker
                or worker['worker_id'] != old_worker['worker_id'] or worker['incarnation'] == old_worker['incarnation']
                or worker['certificate_sha256'] == old_worker['certificate_sha256']
                or worker['enrollment_revision'] <= old_worker['enrollment_revision']
                or next_intent['mount'] != previous | {'worker': worker, 'generation': previous['generation'] + 1}
                or added['mount']['worker'] != worker or added['mount']['profile'] != 'ebs'
                or added['mount']['identity']['cluster_id'] != provider.cluster_id
                or added['mount']['identity']['worker_id'] != worker['worker_id']
                or added['mount']['identity']['pool_id'] == previous['identity']['pool_id']
                or added['mount']['filesystem_uuid'] == previous['filesystem_uuid']
                or added['mount']['provider_volume_id'] == previous['provider_volume_id']
                or identity(added['pvc_uid']) == target['pvc_uid'] or identity(added['pv_uid']) == target['pv_uid']
                or (durable is not None and (durable['drain_id'] != drain_id or durable['add_pool_claim'] != growth_id
                    or durable['exclusion']['launch_digest'] != launch_digest))):
            raise ValueError
        pool = lambda mount, uid: '/'.join((namespace, 'nabedra-pool-' + identity(mount['identity']['pool_id']), identity(uid)))
        old_pvc, added_pvc = pool(previous, target['pvc_uid']), pool(added['mount'], added['pvc_uid'])
    except (AttributeError, KeyError, TypeError, ValueError):
        raise ProviderError('EKS replacement launch differs from the closed worker or durable operation') from None

    if durable is not None and durable['authorization'] is not None:
        return durable

    growth = control._read_replacement('storage.growth.status', growth_id, {'claim_id': growth_id}, deadline)
    try:
        zone = growth['intent']['action']['zone']
        if (growth['phase'] != 'awaiting_mount' or growth['intent']['action']['kind'] != 'add_pool'
                or growth['intent']['claim_id'] != growth_id or growth['intent']['worker'] != old_worker
                or growth['intent']['pool_id'] != added['mount']['identity']['pool_id']
                or growth['intent']['pool_generation'] != added['mount']['generation']
                or growth['provider_resource'] != added['mount']['provider_volume_id']):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ProviderError('EKS added mount differs from the original pending growth claim') from None
    provider._storage_class()
    _exact_pv(provider, added_pvc, added['mount'], added['pv_uid'], zone, growth_id)
    if durable is None:
        stopped = provider.stop_worker(claim_id=drain_id, resource_id=resource_id, pod_uid=expected_pod_uid)
        if stopped['state'] != 'stopped':
            return {'state': 'stopping'}
        proof = verifier._exclusion(provider, resource_id, expected_pod_uid, {
            'expected': previous, 'target': target, 'pvc': old_pvc, 'zone': zone, 'claim': original,
        }, deadline=deadline)
        if proof is None:
            return {'state': 'awaiting_provider_exclusion'}
        operation = str(uuid5(UUID(drain_id), 'eks-provider-exclusion-v1'))
        durable = control._apply_eks_replacement(operation, {'action': 'exclude', 'evidence': proof | {
            'drain_id': drain_id, 'launch_digest': launch_digest,
        }}, verifier, deadline=deadline)
    if (durable['drain_id'] != drain_id or durable['exclusion']['launch_digest'] != launch_digest
            or durable['zone'] != zone or durable['add_pool_claim'] != growth_id):
        raise ProviderError('EKS replacement has no matching committed exclusion')
    _exact_pv(provider, old_pvc, previous, target['pv_uid'], zone, original)
    mounts = [(old_pvc, previous, target['pv_uid'], original), (added_pvc, added['mount'], added['pv_uid'], growth_id)]
    pools = [{'resource_id': pvc, 'mount_path': mount['mount_path'], 'filesystem_uuid': mount['filesystem_uuid']}
             for pvc, mount, _, _ in mounts]
    staged = provider._ensure_worker(claim_id=original, worker_id=worker['worker_id'], incarnation=worker['incarnation'],
                                    zone=zone, bootstrap_secret=template['bootstrap_secret'], pools=pools,
                                    cpu_millicores=template['cpu_millicores'], memory_bytes=template['memory_bytes'],
                                    enrollment=worker, replacement=durable['exclusion_operation'])
    if projected_token is None:
        return {'state': 'awaiting_projected_pod_identity', 'resource_id': staged['resource_id']}
    new_target, proof = verifier._candidate(provider, staged['resource_id'], projected_token,
                                           {'worker': worker, 'zone': zone, 'pools': mounts}, deadline=deadline)
    if new_target['statefulset_uid'] == old_stateful_uid or new_target['pod_uid'] == expected_pod_uid:
        raise ProviderError('EKS candidate still has the retired compute identity')
    native_target = new_target | {'kind': 'eks', 'pvc_uid': target['pvc_uid'], 'pv_uid': target['pv_uid']}
    operation = str(uuid5(UUID(drain_id), 'eks-replacement-authorization-v1'))
    return control._apply_eks_replacement(operation, {'action': 'authorize', 'evidence': proof | {
        'drain_id': drain_id, 'exclusion_operation': durable['exclusion_operation'], 'launch_digest': launch_digest,
        'next': next_intent | {'native_target': native_target}, 'added': added,
    }}, verifier, deadline=deadline)
