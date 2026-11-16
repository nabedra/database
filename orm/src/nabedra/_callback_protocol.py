"""Private callback wire validation and single-attempt ownership observation.

No callback execution, admission, cache or runtime ownership lives here.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import math
import re
import socket
import time
import types
import unicodedata

from .errors import NabedraError, ValidationError
from .validation import validate_identifier

_MAX_BODY = 1024 * 1024
_MAX_HEADERS = 8192
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
_URL = re.compile(r'http://127\.0\.0\.1:([1-9][0-9]{0,4})/private/v1/authorize')
_MESSAGES = {'invalid_command': 'Invalid callback command',
             'invalid_callback_result': 'Invalid callback result',
             'limit_exceeded': 'Callback protocol limit exceeded',
             'authorization_denied': 'Callback authorization denied',
             'unauthorized': 'Callback authorization rejected',
             'ownership_changed': 'Callback ownership changed',
             'ownership_unavailable': 'Callback ownership unavailable'}
_AUTH_DENIALS = {400: 'invalid_command', 401: 'unauthorized',
                 409: 'ownership_changed', 413: 'limit_exceeded',
                 503: 'ownership_unavailable'}


class _CallbackProtocolError(NabedraError):
    def __init__(self, code, *, status_code=None):
        super().__init__(_MESSAGES[code], code=code, status_code=status_code)


@dataclass(frozen=True, repr=False)
class _Command:
    # Both fields are private protocol data. Never log or repr the dictionary.
    original_bytes: bytes
    value: dict


def _require(condition, code='invalid_command'):
    if not condition:
        raise _CallbackProtocolError(code)


def _fields(value, names, code='invalid_command'):
    _require(type(value) is dict and all(type(key) is str for key in value), code)
    _require(value.keys() == set(names.split()), code)


def _uuid(value, code='invalid_command'):
    _require(type(value) is str and _UUID.fullmatch(value) is not None, code)


def _text(value, code='invalid_command'):
    _require(type(value) is str and bool(value), code)


def _version(value):
    _require(type(value) is int and value == 1)


def _resource(value, code):
    _require(type(value) is str and 0 < len(value) <= 256, code)
    try:
        _require(len(value.encode('utf-8')) <= 256 and
                 all(unicodedata.category(c) != 'Cc' for c in value), code)
    except UnicodeError:
        raise _CallbackProtocolError(code) from None


def _unique(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _constant(_):
    raise _CallbackProtocolError('invalid_command')


def _json_tree(value, depth=1):
    _require(depth <= 32)
    if type(value) is dict:
        for key, item in value.items():
            key.encode('utf-8')
            _json_tree(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _json_tree(item, depth + 1)
    elif type(value) is str:
        value.encode('utf-8')
    elif type(value) is float:
        _require(math.isfinite(value))


def _load(raw):
    _require(type(raw) is bytes)
    _require(len(raw) <= _MAX_BODY, 'limit_exceeded')
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique,
                           parse_constant=_constant)
        _require(type(value) is dict)
        _json_tree(value)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise _CallbackProtocolError('invalid_command') from None


def _bootstrap(value, payload):
    _require(type(value) is dict)
    optional = set(value) & {'resources', 'execution'}
    _fields(value, 'version cluster_id node_id request_id api_urls listen advertise_url '
                   'storage enrollment_token tls operation_slots partition_slots queue_size partition_size '
                   + ' '.join(sorted(optional)))
    if 'resources' in value:
        r = value['resources']
        _fields(r, 'max_threads memory_limit_bytes task_memory_limit_bytes control_memory_reserve_bytes')
        _require(all(type(v) is int and 0 < v <= 2**63 - 1 for v in r.values()))
        _require(r['max_threads'] <= 256 and r['control_memory_reserve_bytes'] < r['memory_limit_bytes']
                 and r['task_memory_limit_bytes'] <= r['memory_limit_bytes'] - r['control_memory_reserve_bytes'])
    if 'execution' in value:
        e = value['execution']
        _fields(e, 'max_workers_per_request max_pending_requests max_queue_bytes_per_replica '
                   'max_retained_result_bytes_per_worker result_retention_ms queue_replication queue_lifetime partition_size')
        _require(all(type(e[k]) is int and 0 < e[k] <= 2**63 - 1 for k in (
            'max_workers_per_request', 'max_pending_requests', 'max_queue_bytes_per_replica',
            'max_retained_result_bytes_per_worker', 'result_retention_ms')))
        _require(e['queue_replication'] == 'all_ready_workers' and e['queue_lifetime'] == 'live_cluster')
        _text(e['partition_size'])
    _version(value['version'])
    for key in ('cluster_id', 'node_id', 'request_id'):
        _require(value[key] == payload[key])
    _require(value['advertise_url'] is None)
    urls = value['api_urls']
    _require(type(urls) is list and 1 <= len(urls) <= 16)
    for url in urls:
        _text(url)  # Rust owns address syntax, including API base URLs.
    for key in ('listen', 'partition_size', 'enrollment_token'):
        _text(value[key])
    for key, low, high in (('operation_slots', 1, 256), ('partition_slots', 1, 256),
                           ('queue_size', 0, 4096)):
        _require(type(value[key]) is int and low <= value[key] <= high)
    storage = value['storage']
    _fields(storage, 'driver options')
    _text(storage['driver'])
    _require(type(storage['options']) is dict)
    tls = value['tls']
    if tls is not None:
        _fields(tls, 'cert_file key_file ca_file')
        for path in tls.values():
            _text(path)


def _decode_command(original_command_bytes: bytes) -> _Command:
    """Parse once; retain the exact input bytes and a private decoded dictionary."""
    value = _load(original_command_bytes)
    _fields(value, 'version invocation_id cluster_id owner_id epoch intent_id action target payload')
    _version(value['version'])
    for key in ('invocation_id', 'cluster_id', 'owner_id'):
        _uuid(value[key])
    _require(type(value['epoch']) is int and 1 <= value['epoch'] <= 2**64 - 1)
    action, target, payload = value['action'], value['target'], value['payload']
    _require(type(action) is str and action in ('start', 'stop', 'inventory'))
    if action == 'inventory':
        _require(value['intent_id'] is None and target is None)
        _fields(payload, '')
    else:
        _uuid(value['intent_id'])
        _fields(target, 'node_id request_id incarnation_id')
        for key in ('node_id', 'request_id'):
            _uuid(target[key])
        if target['incarnation_id'] is not None:
            _uuid(target['incarnation_id'])
        if action == 'start':
            _require(value['intent_id'] == target['request_id'] and target['incarnation_id'] is None)
            _fields(payload, 'version cluster_id cluster_name request_id node_id bootstrap')
            _version(payload['version'])
            _require(payload['cluster_id'] == value['cluster_id'])
            for key in ('request_id', 'node_id'):
                _require(payload[key] == target[key])
            try:
                validate_identifier(payload['cluster_name'])
            except ValidationError:
                raise _CallbackProtocolError('invalid_command') from None
            _bootstrap(payload['bootstrap'], payload)
        else:
            _require(value['intent_id'] != target['request_id'])
            _fields(payload, 'resource_id')
            _resource(payload['resource_id'], 'invalid_command')
    return _Command(original_command_bytes, value)


def _bounded_json(value):
    """Encode only validated builtins, limiting allocation even within strings."""
    output = bytearray()

    def emit(chunk):
        _require(len(output) + len(chunk) <= _MAX_BODY, 'limit_exceeded')
        output.extend(chunk)

    def string(text):
        emit(b'"')
        for offset in range(0, len(text), 2048):
            emit(json.dumps(text[offset:offset+2048], ensure_ascii=False)[1:-1].encode('utf-8'))
        emit(b'"')

    def write(item, depth=1):
        _require(depth <= 32, 'invalid_callback_result')
        if type(item) is str:
            string(item)
        elif type(item) is dict:
            emit(b'{')
            for index, (key, child) in enumerate(item.items()):
                if index:
                    emit(b',')
                string(key)
                emit(b':')
                write(child, depth+1)
            emit(b'}')
        elif type(item) is list:
            emit(b'[')
            for index, child in enumerate(item):
                if index:
                    emit(b',')
                write(child, depth+1)
            emit(b']')
        elif item is None:
            emit(b'null')
        elif type(item) is int:
            emit(str(item).encode('ascii'))
        else:
            raise _CallbackProtocolError('invalid_callback_result')
    write(value)
    return bytes(output)


def _encode_result(command: _Command, result: object) -> bytes:
    """Validate public callback output and return the complete bounded success envelope."""
    code = 'invalid_callback_result'
    if type(result) is types.CoroutineType:
        result.close()
        raise _CallbackProtocolError(code)
    value = command.value
    action = value['action']
    if action == 'start':
        _resource(result, code)
    elif action == 'stop':
        _require(result is None, code)
    else:
        _require(type(result) is list, code)
        _require(len(result) <= 4096, 'limit_exceeded')
        resources, nodes, requests = set(), set(), set()
        for record in result:
            _fields(record, 'resource_id cluster_id node_id request_id state', code)
            _resource(record['resource_id'], code)
            for key in ('cluster_id', 'node_id', 'request_id'):
                _uuid(record[key], code)
            _require(record['cluster_id'] == value['cluster_id'], code)
            state = record['state']
            _require(type(state) is str and state in ('starting', 'running', 'stopping', 'terminated'), code)
            resource = record['resource_id']
            _require(resource not in resources, code)
            resources.add(resource)
            if state != 'terminated':
                node, request = record['node_id'], record['request_id']
                _require(node not in nodes and request not in requests, code)
                nodes.add(node)
                requests.add(request)
    return _bounded_json(dict(version=1, invocation_id=value['invocation_id'], result=result))


def _exchange(port, token, body, deadline):
    """One numeric plaintext connection; recompute budget before every socket operation."""
    def budget(sock):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        sock.settimeout(remaining)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        budget(sock)
        sock.connect(('127.0.0.1', port))
        header = (f'POST /private/v1/authorize HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n'
                  f'Authorization: Bearer {token}\r\nContent-Type: application/json\r\n'
                  f'Content-Length: {len(body)}\r\nConnection: close\r\n\r\n').encode('ascii')
        for part in (header, body):
            view = memoryview(part)
            while view:
                budget(sock)
                count = sock.send(view[:65536])
                if not count:
                    raise ValueError
                view = view[count:]
        received = bytearray()
        while b'\r\n\r\n' not in received:
            budget(sock)
            chunk = sock.recv(min(4096, _MAX_HEADERS + 1 - len(received)))
            if not chunk:
                raise ValueError
            received.extend(chunk)
            if len(received) > _MAX_HEADERS and b'\r\n\r\n' not in received:
                raise ValueError
        head, initial = bytes(received).split(b'\r\n\r\n', 1)
        if len(head) + 4 > _MAX_HEADERS:
            raise ValueError
        lines = head.split(b'\r\n')
        if re.fullmatch(rb'HTTP/1\.[01] [0-9]{3} [\x20-\x7e]*', lines[0]) is None:
            raise ValueError
        status = int(lines[0].split(b' ')[1])
        headers = {}
        for line in lines[1:]:
            key, sep, val = line.partition(b':')
            if not sep or re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key) is None:
                raise ValueError
            key = key.lower()
            if key in headers or any(c < 32 and c != 9 or c == 127 for c in val):
                raise ValueError
            headers[key] = val.strip(b' \t')
        if b'transfer-encoding' in headers or b'content-encoding' in headers:
            raise ValueError
        if headers.get(b'content-type', b'').lower() != b'application/json':
            raise ValueError
        length = headers.get(b'content-length', b'')
        if re.fullmatch(rb'0|[1-9][0-9]{0,6}', length) is None:
            raise ValueError
        length = int(length)
        if not 0 < length <= _MAX_BODY or len(initial) > length:
            raise ValueError
        response = bytearray(initial)
        while len(response) < length:
            budget(sock)
            chunk = sock.recv(min(65536, length-len(response)))
            if not chunk:
                raise ValueError
            response.extend(chunk)
        # A single-use connection must end, with no second body or trailing bytes.
        budget(sock)
        if sock.recv(1):
            raise ValueError
        return status, bytes(response)


def _authorize(url: str, token: str, original_command_bytes: bytes, *,
               invocation_id: str, deadline: float | None = None) -> None:
    """Fresh observation only. Caller supplies parsed original bytes and invocation UUID.

    Optional deadline is an absolute monotonic callback observation deadline.
    Never retries or invokes callbacks. Every failure is sanitized and denies entry.
    """
    started = time.monotonic()
    denial = None
    try:
        _require(type(url) is str and len(url) <= 80)
        match = _URL.fullmatch(url)
        _require(match is not None and int(match[1]) <= 65535)
        _require(type(token) is str and re.fullmatch(r'[A-Za-z0-9_-]{43}', token) is not None)
        decoded = base64.urlsafe_b64decode(token + '=')
        _require(len(decoded) == 32 and base64.urlsafe_b64encode(decoded).decode().rstrip('=') == token)
        _require(type(original_command_bytes) is bytes and 0 < len(original_command_bytes) <= _MAX_BODY)
        _uuid(invocation_id)
        end = started + 2.0
        if deadline is not None:
            _require(type(deadline) in (int, float) and math.isfinite(deadline))
            end = min(end, deadline)
        status, raw = _exchange(int(match[1]), token, original_command_bytes, end)
        response = _load(raw)
        if status == 200:
            _fields(response, 'version invocation_id command_sha256 authorized')
            _version(response['version'])
            _require(response['invocation_id'] == invocation_id)
            _require(type(response['command_sha256']) is str and
                     response['command_sha256'] == hashlib.sha256(original_command_bytes).hexdigest())
            _require(response['authorized'] is True)
        else:
            code = _AUTH_DENIALS.get(status)
            _require(code is not None)
            _fields(response, 'version invocation_id error')
            _version(response['version'])
            _require(response['invocation_id'] == invocation_id or
                     (response['invocation_id'] is None and status in (400, 401, 413)))
            _fields(response['error'], 'code')
            _require(response['error']['code'] == code)
            denial = _CallbackProtocolError(code, status_code=status)
        _require(time.monotonic() < end)
    except (OSError, ValueError, OverflowError, _CallbackProtocolError):
        raise _CallbackProtocolError('authorization_denied') from None
    if denial is not None:
        raise denial from None
