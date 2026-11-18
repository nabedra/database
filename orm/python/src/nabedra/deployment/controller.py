"""Resume one database-owned growth claim through a bounded provider callback."""
from __future__ import annotations

import time
from uuid import UUID, uuid5

from .._control_transport import _ControlTransport
from ..errors import StorageError
from ..providers import ProviderError
from .docker import UnsupportedVolumeExpansion, identity


class _GrowthTransport(_ControlTransport):
    _mutations = frozenset('storage.growth.' + name for name in (
        'configure', 'claim', 'claim_automatic', 'attempt', 'provisioned', 'mounted', 'prepare_worker')
    ) | {'storage.replica.move'}
    _operations = _mutations | {'storage.growth.' + name for name in (
        'status', 'inventory', 'snapshot', 'workers', 'sample')}


class GrowthController:
    """Provider failure leaves the replicated claim and spend reservation intact.

    The provider callback receives the immutable intent and same claim UUID on
    every retry. Mounted completion is requested from the owning worker, which
    inspects its live store. No capacity value or proof is sent by this client.
    """

    def __init__(self, *, api_url, admin_token, timeout=20, ca_file=None):
        self._transport = _GrowthTransport(api_url, admin_token, timeout=timeout, ca_file=ca_file)

    def _operation(self, claim_id, step, operation, payload, *, deadline=None):
        request_id = str(uuid5(UUID(identity(claim_id)), step))
        return self._read(operation, payload, request_id=request_id, deadline=deadline)

    def _read(self, operation, payload, *, request_id=None, deadline=None):
        _, value = self._transport.request(operation, payload, request_id=request_id, deadline=deadline)
        if type(value) is not dict:
            raise ProviderError('Database growth reply is not a metadata record')
        return value

    def status(self, claim_id, *, deadline=None):
        return self._operation(claim_id, 'status', 'storage.growth.status', {'claim_id': claim_id}, deadline=deadline)

    def inventory(self):
        """Observe the contacted worker's bounded live mounted-pool inventory."""
        _, value = self._transport.request('storage.growth.inventory', {})
        if type(value) is not dict:
            raise ProviderError('Database pool inventory is not a metadata record')
        return value

    def configure(self, policy):
        """Explicit operator policy update, including existing retained costs."""
        policy_id = identity(policy['policy_id'])
        return self._operation(policy_id, 'configure', 'storage.growth.configure', {'policy': policy})

    def reconcile(self, *, intent, provision, mount=None):
        if not callable(provision):
            raise ProviderError('Provisioning is unavailable without a configured stateful adapter')
        claim_id = identity(intent['claim_id'])
        self._operation(claim_id, 'claim', 'storage.growth.claim', {'intent': intent})
        return self._advance(intent, provision, mount=mount)

    def _advance(self, intent, provision, *, mount=None, register=None, authorize=None, deadline=None):
        claim_id = identity(intent['claim_id'])
        # A replayed write receipt contains its original reply. Read current
        # replicated state before deciding which side effect remains necessary.
        record = self.status(claim_id, deadline=deadline)
        if record.get('intent') != intent or record.get('phase') not in ('claimed', 'provisioning', 'awaiting_mount', 'complete'):
            raise ProviderError('Database growth reply conflicts with its immutable intent')
        if record['phase'] == 'complete':
            return record
        if record['phase'] != 'awaiting_mount':
            now = int(time.time())
            previous = record.get('last_attempt')
            if previous is not None and now - previous < intent['cooldown_seconds']:
                return record
            attempt = record.get('attempts')
            if type(attempt) is not int or not 0 <= attempt < intent['max_attempts']:
                raise ProviderError('Growth retry ceiling reached; existing claim and resources remain retained')
            # If this reply is lost, query the same claim before another attempt.
            self._operation(claim_id, 'attempt-' + str(attempt + 1), 'storage.growth.attempt',
                            {'claim_id': claim_id}, deadline=deadline)
            if authorize is not None:
                authorize('provisioning', attempt + 1)
            try:
                observed = provision(intent)
            except UnsupportedVolumeExpansion as exc:
                if intent['action']['kind'] != 'expand':
                    raise
                return self._operation(claim_id, 'provisioned', 'storage.growth.provisioned',
                    {'claim_id': claim_id, 'resource_id': exc.resource_id}, deadline=deadline)
            if type(observed) is not dict or not isinstance(observed.get('resource_id'), str):
                raise ProviderError('Provider returned no exact resource identity')
            record = self._operation(claim_id, 'provisioned', 'storage.growth.provisioned',
                {'claim_id': claim_id, 'resource_id': observed['resource_id']}, deadline=deadline)
        if register is not None:
            authorize('awaiting_mount', record['attempts'])
            mount = register(intent, record)
            if mount is None and intent['action']['kind'] != 'expand':
                return self.status(claim_id, deadline=deadline)
        # The optional mount supplies an expectation. The worker opens the
        # actual store under an exclusive lock and measures the filesystem.
        try:
            return self.mounted(claim_id, mount=mount, deadline=deadline)
        except StorageError:
            current = self.status(claim_id, deadline=deadline)
            if (current.get('intent') == intent and current.get('phase') == 'awaiting_mount'
                    and current.get('mounted_total_bytes') is None):
                return current
            raise

    def mounted(self, claim_id, *, mount=None, deadline=None):
        payload = {'claim_id': claim_id}
        if mount is not None:
            payload['mount'] = mount
        return self._operation(claim_id, 'mounted', 'storage.growth.mounted', payload, deadline=deadline)
