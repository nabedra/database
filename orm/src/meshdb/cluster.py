"""Cluster policy acceptance, validated status and bounded readiness waits."""
from __future__ import annotations

import math
import re
import unicodedata
from time import monotonic as _monotonic, sleep as _sleep
from uuid import UUID, uuid4

from ._control_transport import _ControlTransport
from .errors import (
    ConflictError, MeshDBError, ProtocolError, TransportError,
    UnknownOutcomeError, ValidationError,
)
from .validation import validate_identifier

_POLICY_KEYS = {'revision', 'mode', 'desired', 'min_nodes', 'max_nodes'}
_STATUS_KEYS = _POLICY_KEYS | {
    'version', 'cluster_id', 'name', 'capabilities', 'controller', 'counts',
    'nodes', 'intents', 'metrics_fresh',
}
_NODE_KEYS = {
    'node_id', 'incarnation', 'resource_id', 'ownership', 'state',
    'operation_slots', 'partition_slots', 'running_operations', 'running_partitions',
    'queued_operations', 'queued_partitions', 'heartbeat_age_seconds',
}
_INTENT_KEYS = {'request_id', 'node_id', 'resource_id', 'action', 'state', 'attempts', 'error_code'}
# Error codes are bounded machine tokens, never free-form provider messages.
_ERROR_CODE = re.compile(r'[A-Za-z0-9_.:-]{1,256}')


def _record(value: object, keys: set[str], label: str) -> dict:
    if type(value) is not dict or value.keys() != keys:
        raise ValidationError(f'{label} must have exactly the specified fields')
    return value.copy()


def _integer(value: object, label: str, low: int = 0, high: int | None = None) -> int:
    if type(value) is not int or value < low or (high is not None and value > high):
        raise ValidationError(f'{label} is outside its integer range')
    return value


def _boolean(value: object, label: str) -> None:
    if type(value) is not bool:
        raise ValidationError(f'{label} must be a boolean')


def _choice(value: object, choices: tuple[str, ...], label: str) -> None:
    if type(value) is not str or value not in choices:
        raise ValidationError(f'{label} is unsupported')


def _uuid(value: object, label: str) -> None:
    if type(value) is not str:
        raise ValidationError(f'{label} must be a canonical UUID')
    try:
        valid = str(UUID(value)) == value
    except ValueError:
        valid = False
    if not valid:
        raise ValidationError(f'{label} must be a canonical UUID')


def _resource(value: object) -> None:
    if value is None:
        return
    if type(value) is not str or not value:
        raise ValidationError('Resource ID must be a nonempty string or null')
    try:
        valid = len(value.encode('utf-8')) <= 256
    except UnicodeError:
        valid = False
    if not valid or any(unicodedata.category(c) == 'Cc' for c in value):
        raise ValidationError('Resource ID exceeds its bound or contains controls')


def _policy_fields(data: dict) -> None:
    _integer(data['revision'], 'Revision', 1)
    _choice(data['mode'], ('fixed', 'autoscale'), 'Policy mode')
    low = 0 if data['mode'] == 'fixed' else 1
    for key in ('desired', 'min_nodes', 'max_nodes'):
        _integer(data[key], key, low, 1024)
    if not data['min_nodes'] <= data['desired'] <= data['max_nodes']:
        raise ValidationError('Desired count must be within policy bounds')
    if data['mode'] == 'fixed' and not data['min_nodes'] == data['desired'] == data['max_nodes']:
        raise ValidationError('Fixed policy bounds must equal desired count')


def _node(value: object) -> dict:
    data = _record(value, _NODE_KEYS, 'Node')
    _uuid(data['node_id'], 'Node ID')
    if data['incarnation'] is not None:
        _uuid(data['incarnation'], 'Incarnation')
    _resource(data['resource_id'])
    _choice(data['ownership'], ('managed', 'external'), 'Ownership')
    _choice(data['state'], ('starting', 'ready', 'draining', 'lost', 'stopped'), 'Node state')
    for kind in ('operations', 'partitions'):
        slot_key = 'operation_slots' if kind == 'operations' else 'partition_slots'
        slots = _integer(data[slot_key], slot_key, 1, 256)
        _integer(data['running_' + kind], 'Running ' + kind, 0, slots)
        _integer(data['queued_' + kind], 'Queued ' + kind, 0, 4096)
    age = data['heartbeat_age_seconds']
    # Integers are finite without converting arbitrarily large JSON integers to float.
    if age is not None and (type(age) not in (int, float) or age < 0
                            or (type(age) is float and not math.isfinite(age))):
        raise ValidationError('Heartbeat age must be finite nonnegative seconds or null')
    return data


def _intent(value: object) -> dict:
    data = _record(value, _INTENT_KEYS, 'Intent')
    _uuid(data['request_id'], 'Intent request ID')
    _uuid(data['node_id'], 'Intent node ID')
    _resource(data['resource_id'])
    _choice(data['action'], ('start', 'stop'), 'Intent action')
    _choice(data['state'], ('pending', 'in_progress', 'reconciling', 'completed', 'failed'),
            'Intent state')
    _integer(data['attempts'], 'Intent attempts')
    code = data['error_code']
    if code is not None and (type(code) is not str or _ERROR_CODE.fullmatch(code) is None):
        raise ValidationError('Intent error code must be a bounded machine token or null')
    return data


def _unique(records: list[dict], key: str) -> None:
    identities = [record[key] for record in records if record[key] is not None]
    if len(set(identities)) != len(identities):
        raise ValidationError(f'Duplicate {key} in status records')


def _status(value: object) -> dict:
    data = _record(value, _STATUS_KEYS, 'Status')
    if type(data['version']) is not int or data['version'] != 1:
        raise ValidationError('Unsupported status version')
    _uuid(data['cluster_id'], 'Cluster ID')
    validate_identifier(data['name'])
    _policy_fields(data)
    capabilities = _record(data['capabilities'], {'provision', 'terminate', 'autoscale'}, 'Capabilities')
    for key, value in capabilities.items():
        _boolean(value, key)
    data['capabilities'] = capabilities
    controller = _record(data['controller'], {'epoch', 'active', 'degraded'}, 'Controller')
    _integer(controller['epoch'], 'Controller epoch')
    for key in ('active', 'degraded'):
        _boolean(controller[key], key)
    data['controller'] = controller
    counts = _record(data['counts'], {'ready', 'starting', 'draining', 'lost'}, 'Counts')
    for key, value in counts.items():
        _integer(value, key)
    _boolean(data['metrics_fresh'], 'Metrics freshness')
    if type(data['nodes']) is not list or type(data['intents']) is not list:
        raise ValidationError('Nodes and intents must be arrays')
    if len(data['nodes']) + len(data['intents']) > 4096:
        raise ValidationError('Combined node/intent count exceeds 4096')
    nodes = [_node(value) for value in data['nodes']]
    intents = [_intent(value) for value in data['intents']]
    for key in ('node_id', 'incarnation', 'resource_id'):
        _unique(nodes, key)
    _unique(intents, 'request_id')
    # A node may have multiple retained lifecycle intents. Pending intent node IDs
    # need not be present in membership, and stopped nodes do not enter counts.
    actual = dict.fromkeys(counts, 0)
    for node in nodes:
        if node['state'] in actual:
            actual[node['state']] += 1
    if counts != actual:
        raise ValidationError('Counts do not match node membership states')
    data['counts'] = counts
    data['nodes'] = sorted(nodes, key=lambda node: node['node_id'])
    data['intents'] = sorted(intents, key=lambda intent: intent['request_id'])
    return data


class Cluster:
    """Submit cluster policies, optionally wait for readiness, or read status.

    Policy methods return persisted acceptance, which does not imply readiness.
    Invalid success replies to writes raise UnknownOutcomeError because the
    policy may already have applied. Mutations are never retried implicitly.
    """

    def __init__(self, api_url: str, admin_token: str, *, timeout: float = 30.0,
                 ca_file: str | None = None) -> None:
        self._transport = _ControlTransport(api_url, admin_token, timeout=timeout, ca_file=ca_file)

    def scale(self, nodes: int, *, wait: bool = False, timeout: float = 300.0) -> dict:
        """Accept a fixed target in 0..1024, optionally waiting for convergence.

        The wait budget includes submission. Expiration after acceptance leaves
        the policy in effect; supersession and expiration retain its request ID.
        """
        _integer(nodes, 'Nodes', 0, 1024)
        _boolean(wait, 'Wait')
        try:
            if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                raise ValueError
        except (ValueError, OverflowError):
            raise ValidationError('Timeout must be finite positive seconds') from None
        deadline = _monotonic() + timeout if wait else None
        request_id = str(uuid4())
        accepted = self._submit('cluster.scale', {'nodes': nodes},
                                {'mode': 'fixed', 'desired': nodes},
                                request_id=request_id, deadline=deadline)
        if wait:
            self._wait(accepted, request_id, deadline)
        return accepted

    @staticmethod
    def _remaining(deadline: float, request_id: str) -> float:
        remaining = deadline - _monotonic()
        if remaining <= 0:
            raise TransportError('Accepted policy did not converge before the deadline',
                                 code='wait_timeout', request_id=request_id, outcome=None)
        return remaining

    def _wait(self, accepted: dict, request_id: str, deadline: float) -> None:
        while True:
            self._remaining(deadline, request_id)
            try:
                current = self._read_status(deadline=deadline)
            except MeshDBError:
                # An expired read is a timeout of this accepted policy wait.
                # Other read failures keep their original diagnostics.
                self._remaining(deadline, request_id)
                raise
            self._remaining(deadline, request_id)
            if current['revision'] != accepted['revision']:
                error = ConflictError('Accepted policy was superseded',
                                      code='policy_superseded', request_id=request_id)
                # ConflictError defaults to a rejected write; this is a local
                # observation after acceptance, with no HTTP rejection.
                error.outcome = None
                raise error
            if (current['counts']['ready'] == accepted['desired']
                    and not any(intent['state'] in ('pending', 'in_progress', 'reconciling')
                                for intent in current['intents'])):
                return
            _sleep(min(0.1, self._remaining(deadline, request_id)))

    def autoscale(self, *, min_nodes: int, max_nodes: int) -> dict:
        """Accept autoscaling bounds in 1..1024, inclusive."""
        _integer(min_nodes, 'Minimum nodes', 1, 1024)
        _integer(max_nodes, 'Maximum nodes', 1, 1024)
        if min_nodes > max_nodes:
            raise ValidationError('Minimum nodes must not exceed maximum nodes')
        bounds = {'min_nodes': min_nodes, 'max_nodes': max_nodes}
        return self._submit('cluster.autoscale', bounds, {'mode': 'autoscale', **bounds})

    def _submit(self, operation: str, payload: dict, expected: dict, *,
                request_id: str | None = None, deadline: float | None = None) -> dict:
        request_id, value = self._transport.request(operation, payload,
                                                    request_id=request_id, deadline=deadline)
        try:
            data = _record(value, _POLICY_KEYS, 'Policy acceptance')
            _policy_fields(data)
            if any(data[key] != value for key, value in expected.items()):
                raise ValidationError('Policy acceptance does not match requested policy')
        except ValidationError as error:
            raise UnknownOutcomeError(str(error), request_id=request_id, status_code=200) from None
        return data

    def status(self) -> dict:
        """Read validated status with node/intent arrays sorted by identity."""
        return self._read_status()

    def _read_status(self, *, deadline: float | None = None) -> dict:
        request_id, value = self._transport.request('cluster.status', {}, deadline=deadline)
        try:
            return _status(value)
        except ValidationError as error:
            raise ProtocolError(str(error), request_id=request_id, status_code=200) from None
