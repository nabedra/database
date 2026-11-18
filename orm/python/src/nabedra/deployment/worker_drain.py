"""Consume a fresh Control stop permit against one retained Docker container."""
from __future__ import annotations

import ipaddress
import re
import time
from urllib.parse import urlsplit

from .._control_transport import _ControlTransport
from ..providers import ProviderError
from .docker import DockerProvider, PREFIX, identity


class _WorkerDrainTransport(_ControlTransport):
    _mutations = frozenset({'worker.drain'})
    _operations = _mutations | {'worker.drain.status', 'worker.stop_permit'}


class DockerWorkerDrain:
    def __init__(self, *, provider: DockerProvider, api_url, admin_token, ca_file=None):
        try:
            url = urlsplit(api_url)
            if url.scheme != 'https' and (url.scheme != 'http' or not ipaddress.ip_address(url.hostname).is_loopback):
                raise ValueError
        except (TypeError, ValueError):
            raise ProviderError('Docker stop authority requires authenticated HTTPS or a numeric loopback address') from None
        self._provider = provider
        self._control = _WorkerDrainTransport(api_url, admin_token,
                                             timeout=provider.timeout, ca_file=ca_file)

    def begin(self, drain_id, *, worker, run_id, resource_id):
        """Register a fixed intent after inspecting the full owned container ID."""
        drain_id = identity(drain_id)
        self._inspect(worker, resource_id, time.monotonic() + self._provider.timeout)
        intent = {'worker': worker, 'run_id': identity(run_id),
                  'docker_container_id': resource_id.split(':', 1)[1]}
        return self._read('worker.drain', drain_id, intent)

    def status(self, drain_id):
        return self._read('worker.drain.status', identity(drain_id), {})

    def stop(self, drain_id):
        """Stop the exact container, or observe the same stop after a lost reply."""
        drain_id = identity(drain_id)
        cutoff = time.monotonic() + self._provider.timeout
        permit = self._read('worker.stop_permit', drain_id, {}, deadline=cutoff)
        try:
            record = permit['record']
            if record['id'] != drain_id or set(record['phase']) != {'closed'}:
                raise ValueError
            intent = record['intent']
            worker = intent['worker']
            resource_id = 'docker-container:' + intent['docker_container_id']
            mount = record['phase']['closed']['mount']['expected']
            pool = mount['identity']
            if (pool['cluster_id'] != self._provider.cluster_id or pool['worker_id'] != worker['worker_id']
                    or mount['worker'] != worker or not mount['provider_volume_id'].startswith('docker-volume:')
                    or mount['profile']['docker']['volume_marker'] != pool['pool_id']):
                raise ValueError
            volume = mount['provider_volume_id'].split(':', 1)[1]
            if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}', volume):
                raise ValueError
            claim_id, pool_id = identity(record['growth_claim']), identity(pool['pool_id'])
            destination = mount['mount_path']
        except (TypeError, KeyError, ValueError, AttributeError):
            raise ProviderError('Control returned no exact retained Docker stop permit') from None
        container = self._inspect(worker, resource_id, cutoff)
        identifier = container['Id']
        if (container['Config']['Labels'].get(PREFIX + 'claim') != claim_id
                or container.get('HostConfig', {}).get('RestartPolicy', {}).get('Name') not in ('', 'no')):
            raise ProviderError('Docker launch claim or fixed restart policy differs')
        all_mounts = container.get('Mounts', [])
        mounts = [entry for entry in all_mounts if entry.get('Destination') == destination]
        if (any(entry.get('Type') == 'volume' and entry.get('RW') is True
                and entry.get('Destination') != destination for entry in all_mounts)
                or len(mounts) != 1 or mounts[0].get('Type') != 'volume'
                or mounts[0].get('Name') != volume or mounts[0].get('RW') is not True):
            raise ProviderError('Docker worker no longer owns the retained mount from the permit')
        self._retained_volume(volume, claim_id, worker['worker_id'], pool_id, cutoff)
        status = container.get('State', {}).get('Status')
        if status == 'running':
            self._provider._run('container', 'stop', '--time', '5', identifier, deadline=cutoff)
            status = self._inspect(worker, resource_id, cutoff).get('State', {}).get('Status')
        if status != 'exited':
            raise ProviderError('Exact Docker container stop is unresolved; reconcile the same drain')
        self._retained_volume(volume, claim_id, worker['worker_id'], pool_id, cutoff)
        return {'resource_id': resource_id, 'state': 'stopped', 'retained_volume_id': mount['provider_volume_id']}

    def _retained_volume(self, name, claim, worker, pool, deadline):
        records = self._provider._run('volume', 'inspect', name, json_result=True, deadline=deadline)
        if len(records) != 1 or records[0].get('Name') != name:
            raise ProviderError('Docker volume inventory changed')
        self._provider._owned(records[0].get('Labels') or {}, {
            PREFIX + 'cluster': self._provider.cluster_id, PREFIX + 'claim': claim,
            PREFIX + 'worker': worker, PREFIX + 'pool': pool,
        })

    def _inspect(self, worker, resource_id, deadline):
        try:
            revision = worker['enrollment_revision']
            certificate = worker['certificate_sha256']
            if (type(revision) is not int or revision < 1
                    or not re.fullmatch(r'[a-f0-9]{64}', certificate)):
                raise ValueError
            labels = {PREFIX + 'worker': identity(worker['worker_id']),
                      PREFIX + 'incarnation': identity(worker['incarnation']),
                      PREFIX + 'enrollment-revision': str(revision),
                      PREFIX + 'certificate': certificate}
        except (TypeError, KeyError, ValueError):
            raise ProviderError('Exact Docker worker enrollment is required') from None
        container = self._provider.inspect_worker(resource_id, deadline=deadline)
        self._provider._owned(container.get('Config', {}).get('Labels') or {}, labels)
        return container

    def _read(self, operation, drain_id, payload, *, deadline=None):
        if deadline is None:
            deadline = time.monotonic() + self._provider.timeout
        _, record = self._control.request(operation, payload, request_id=drain_id, deadline=deadline)
        if type(record) is not dict:
            raise ProviderError('Control worker drain reply is not a metadata record')
        return record
